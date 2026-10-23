package compose

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// Caps of the read ops. The command runner caps a child's output at 64 KiB
// too; these are the caps this package applies on top, so a fake runner and a
// real one behave the same.
const (
	maxCommandOutput = 64 << 10
	maxPSContainers  = 64
	maxInspectCount  = 5
	maxInspectEnv    = 256
	maxConfigBody    = 48 << 10
	maxStackServices = 16
)

// dockerFormatJSON is the one Go template every read passes to a docker
// --format flag: the CLI prints its own JSON, so nothing is scraped from a
// table.
const dockerFormatJSON = "{{json .}}"

// StackParams are the typed params of the compose reads and writes: the
// rendered file and an optional service subset. No field carries free text
// compose would treat as configuration.
type StackParams struct {
	// File is the project-relative compose file name; "" means DefaultFile.
	File string `json:"file,omitempty"`
	// Services limits the command to these services; empty means the whole
	// project.
	Services []string `json:"services,omitempty"`
}

// decodeStack decodes and validates compose params without I/O.
func decodeStack(params json.RawMessage) (StackParams, error) {
	var p StackParams
	if err := decodeParams(params, &p); err != nil {
		return p, err
	}
	if p.File == "" {
		p.File = DefaultFile
	}
	if err := ops.CheckName(p.File); err != nil {
		return p, fmt.Errorf("%w: file: %v", ErrPath, err)
	}
	if !isComposeName(p.File) {
		return p, fmt.Errorf("%w: file %s is not a compose file name", ErrParams, q(p.File))
	}
	if len(p.Services) > maxStackServices {
		return p, fmt.Errorf("%w: %d services (at most %d)", ErrParams, len(p.Services), maxStackServices)
	}
	seen := make(map[string]bool, len(p.Services))
	names := make([]string, 0, len(p.Services))
	for _, name := range p.Services {
		if !serviceNameRE.MatchString(name) {
			return p, fmt.Errorf("%w: service %s is not a service name", ErrParams, q(name))
		}
		if seen[name] {
			continue
		}
		seen[name] = true
		names = append(names, name)
	}
	sort.Strings(names)
	p.Services = names
	return p, nil
}

// projectRoot opens the project directory as an os.Root: the same confinement
// every file op uses, without reaching into the ops package's Confiner. The
// caller closes it.
func projectRoot(env ops.Env) (*os.Root, error) {
	if env.Confine == nil {
		return nil, errors.New("compose: the project root is not open")
	}
	root, err := os.OpenRoot(env.Confine.Dir())
	if err != nil {
		return nil, fmt.Errorf("compose: opening the project root: %w", err)
	}
	return root, nil
}

// stackFiles checks the two files the pinned argv always names: the rendered
// compose file and the .env properties_write renders. Compose refuses a
// missing --env-file, so the op reports the missing step instead of docker's
// text. It returns the clean compose file name.
func stackFiles(env ops.Env, file string) (string, error) {
	if env.Confine == nil {
		return "", errors.New("compose: the project root is not open")
	}
	clean, err := env.Confine.Check(file)
	if err != nil {
		return "", err
	}
	root, err := projectRoot(env)
	if err != nil {
		return "", err
	}
	defer root.Close()
	check := func(name, clean string, what string) error {
		fi, err := root.Lstat(clean)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return fmt.Errorf("%w: %s is not written yet; %s first", ErrStack, q(name), what)
		case err != nil:
			return fmt.Errorf("compose: stat %s: %w", q(name), err)
		case !fi.Mode().IsRegular():
			return fmt.Errorf("%w: %s is not a regular file", ErrStack, q(name))
		}
		return nil
	}
	if err := check(file, clean, "run compose_write"); err != nil {
		return "", err
	}
	if err := check(EnvFileName, EnvFileName, "run properties_write"); err != nil {
		return "", err
	}
	return clean, nil
}

// stackSnapshot reads the on-disk compose file once, validates exactly those
// bytes against the S1 rules (validateDoc) and writes them to a fresh private
// file that docker reads instead of the project's. compose_write validated the
// bytes it wrote, but the file a later up, restart or down names is whatever
// is on disk NOW: a file replaced since the write (another tool, a person
// editing the project) would otherwise run with every privilege it asks for.
// Handing docker the project path after the check would leave a window in
// which the file can be swapped between the read and docker's own open; the
// snapshot closes it. The pinned argv's --project-directory keeps relative
// binds and the default project name anchored at the project.
//
// The bind sources the document mounts are checked on disk too: a component
// that is a symlink would make a lexically local ./x bind any host path.
//
// It also returns the document's project name (DefaultProject when it names
// none), which the caller passes to docker with -p.
//
// The caller runs docker with the returned path and then calls cleanup.
func stackSnapshot(env ops.Env, clean, class string) (string, string, func(), error) {
	root, err := projectRoot(env)
	if err != nil {
		return "", "", nil, err
	}
	defer root.Close()
	fi, err := root.Lstat(clean)
	if err != nil {
		return "", "", nil, fmt.Errorf("compose: stat %s: %w", q(clean), err)
	}
	if !fi.Mode().IsRegular() {
		return "", "", nil, fmt.Errorf("%w: %s is not a regular file", ErrStack, q(clean))
	}
	f, err := root.Open(clean)
	if err != nil {
		return "", "", nil, fmt.Errorf("compose: opening %s: %w", q(clean), err)
	}
	doc, err := io.ReadAll(io.LimitReader(f, maxComposeBytes+1))
	f.Close()
	if err != nil {
		return "", "", nil, fmt.Errorf("compose: reading %s: %w", q(clean), err)
	}
	binds, err := validateDoc(doc, class)
	if err == nil {
		err = checkBindsOnDisk(root, binds)
	}
	if err != nil {
		return "", "", nil, fmt.Errorf("%w: %s on disk no longer passes validation (run compose_write again): %w", ErrStack, q(clean), err)
	}
	project, err := docProjectName(doc)
	if err != nil {
		return "", "", nil, fmt.Errorf("%w: %s on disk no longer passes validation (run compose_write again): %w", ErrStack, q(clean), err)
	}
	dir, err := os.MkdirTemp("", "piperoom-compose-")
	if err != nil {
		return "", "", nil, fmt.Errorf("compose: creating the compose snapshot: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	snap := filepath.Join(dir, path.Base(clean))
	out, err := os.OpenFile(snap, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err == nil {
		_, err = out.Write(doc)
		if cerr := out.Close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		cleanup()
		return "", "", nil, fmt.Errorf("compose: writing the compose snapshot: %w", err)
	}
	return snap, project, cleanup, nil
}

// checkBindsOnDisk refuses a bind source any component of which is a symlink
// (or anything but a directory or regular file at the leaf): the lexical
// check keeps ./x under the project, and this keeps the path docker resolves
// there too. A component that does not exist yet is fine: docker creates a
// missing bind source as a directory at that path.
func checkBindsOnDisk(root *os.Root, binds []string) error {
	for _, src := range binds {
		parts := strings.Split(strings.TrimPrefix(src, "./"), "/")
		for i := range parts {
			p := strings.Join(parts[:i+1], "/")
			fi, err := root.Lstat(p)
			if errors.Is(err, fs.ErrNotExist) {
				break
			}
			if err != nil {
				return ruleError(RuleMount, "", "bind source %s: %s", q(src), clip(err.Error()))
			}
			if fi.Mode()&fs.ModeSymlink != 0 {
				return ruleError(RuleMount, "", "bind source %s goes through the symlink %s", q(src), q(p))
			}
			if !fi.IsDir() && !(i == len(parts)-1 && fi.Mode().IsRegular()) {
				return ruleError(RuleMount, "", "bind source %s is not a directory or a regular file", q(src))
			}
		}
	}
	return nil
}

// snapshotArgv points the pinned argv's -f at the validated snapshot and
// names the project with -p. -p outranks both COMPOSE_PROJECT_NAME (which
// compose also reads from the project's .env) and the directory name, so the
// project docker acts on is exactly the validated document's piperoom-
// project, never another stack on the host.
func snapshotArgv(argv []string, snap, project string) ([]string, error) {
	for i := 0; i+1 < len(argv); i++ {
		if argv[i] == "-f" {
			out := make([]string, 0, len(argv)+2)
			out = append(out, argv[:i]...)
			out = append(out, "-f", snap, "-p", project)
			return append(out, argv[i+2:]...), nil
		}
	}
	return nil, errors.New("compose: the pinned argv names no compose file")
}

// pinnedStackArgv validates the on-disk compose file, snapshots it, and
// builds the pinned argv for sub against the snapshot. The caller defers
// cleanup.
func pinnedStackArgv(env ops.Env, file, clean, class, sub string, flags, services []string) ([]string, func(), error) {
	argv, err := stackArgv(env.Confine.Dir(), file, sub, flags, services)
	if err != nil {
		return nil, nil, err
	}
	snap, project, cleanup, err := stackSnapshot(env, clean, class)
	if err != nil {
		return nil, nil, err
	}
	argv, err = snapshotArgv(argv, snap, project)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return argv, cleanup, nil
}

// commandResult is one docker invocation: the capped output, the exit code,
// and whether the cap cut the output.
type commandResult struct {
	out       []byte
	code      int
	truncated bool
}

// dockerCommand runs one literal docker argv through the injected runner. It
// does not treat a non-zero exit as an error: docker_info needs the output of
// a failing `docker version`, and each caller decides what the code means. A
// runner start failure is an error wrapping ops.ErrExec.
func dockerCommand(ctx context.Context, env ops.Env, argv []string) (commandResult, error) {
	if len(argv) < 2 || argv[0] != "docker" {
		return commandResult{}, errors.New("compose: refusing a command that is not docker")
	}
	if env.Exec == nil {
		return commandResult{}, errors.New("compose: no command runner configured")
	}
	raw, code, err := env.Exec(ctx, argv[0], argv[1:]...)
	res := commandResult{out: raw, code: code}
	if len(raw) > maxCommandOutput {
		res.out = raw[:maxCommandOutput]
		res.truncated = true
	}
	if err != nil {
		return res, fmt.Errorf("%w: running docker: %w", ops.ErrExec, err)
	}
	return res, nil
}

// dockerInfo reads the client, daemon and Compose plugin versions and the
// endpoint a container would use to reach the host.
type dockerInfoOp struct{ d *Driver }

// dockerVersionJSON is the shape `docker version --format {{json .}}` prints.
// The fields are the CLI's own; an absent Server block means the daemon was
// not reachable.
type dockerVersionJSON struct {
	Client struct {
		Version    string `json:"Version"`
		APIVersion string `json:"ApiVersion"`
		OS         string `json:"Os"`
		Arch       string `json:"Arch"`
		Context    string `json:"Context"`
	} `json:"Client"`
	Server struct {
		Version    string `json:"Version"`
		APIVersion string `json:"ApiVersion"`
		OS         string `json:"Os"`
		Arch       string `json:"Arch"`
	} `json:"Server"`
}

type dockerInfoData struct {
	ClientVersion    string `json:"client_version,omitempty"`
	ClientAPIVersion string `json:"client_api_version,omitempty"`
	ServerVersion    string `json:"server_version,omitempty"`
	ServerAPIVersion string `json:"server_api_version,omitempty"`
	OSType           string `json:"os_type,omitempty"`
	Arch             string `json:"arch,omitempty"`
	Context          string `json:"context,omitempty"`
	ComposeVersion   string `json:"compose_version,omitempty"`
	ComposeOK        bool   `json:"compose_ok"`
	DaemonOK         bool   `json:"daemon_ok"`
	SocketPath       string `json:"socket_path"`
	HostNetworking   bool   `json:"host_networking"`
	ContainerHost    string `json:"container_host"`
	ComposeError     string `json:"compose_error,omitempty"`
	Error            string `json:"error,omitempty"`
}

func (dockerInfoOp) Name() string   { return "docker_info" }
func (dockerInfoOp) Target() string { return targetKind }
func (dockerInfoOp) Tier() string   { return protocol.TierT0 }
func (dockerInfoOp) Layer() string  { return layerName }
func (dockerInfoOp) Describe() string {
	return "read the Docker client, daemon and Compose plugin versions and the daemon endpoint"
}

func (dockerInfoOp) ParamsSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false}
}

func (dockerInfoOp) Validate(params json.RawMessage, env ops.Env) error {
	var p struct{}
	return decodeParams(params, &p)
}

func (o dockerInfoOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	var p struct{}
	if err := decodeParams(params, &p); err != nil {
		return ops.Result{}, err
	}
	start := now(env)
	versionArgv := []string{"docker", "version", "--format", dockerFormatJSON}
	verRes, err := dockerCommand(ctx, env, versionArgv)
	if err != nil {
		return ops.Result{}, err
	}
	var v dockerVersionJSON
	info := dockerInfoData{
		SocketPath:     dockerSocketPath(o.d.runnerPlatform(), o.d.lookupEnv()),
		HostNetworking: hostNetworking(o.d.runnerPlatform()),
		ContainerHost:  containerHost(o.d.runnerPlatform()),
	}
	if jerr := json.Unmarshal(verRes.out, &v); jerr == nil {
		info.ClientVersion = clip(v.Client.Version)
		info.ClientAPIVersion = clip(v.Client.APIVersion)
		info.Context = clip(v.Client.Context)
		info.OSType = clip(v.Server.OS)
		if info.OSType == "" {
			info.OSType = clip(v.Client.OS)
		}
		info.Arch = clip(v.Server.Arch)
		if info.Arch == "" {
			info.Arch = clip(v.Client.Arch)
		}
	}
	info.DaemonOK = verRes.code == 0 && v.Server.Version != ""
	if info.DaemonOK {
		info.ServerVersion = clip(v.Server.Version)
		info.ServerAPIVersion = clip(v.Server.APIVersion)
	} else {
		// A daemon that is down is a finding, not an op failure: the model
		// reads it and proposes starting Docker Desktop.
		info.Error = clipTail(verRes.out, 400)
	}
	composeArgv := []string{"docker", "compose", "version", "--format", "json"}
	composeRes, cerr := dockerCommand(ctx, env, composeArgv)
	switch {
	case cerr != nil:
		info.ComposeError = clip(cerr.Error())
	case composeRes.code != 0:
		info.ComposeError = clipTail(composeRes.out, 300)
	default:
		info.ComposeVersion, info.ComposeOK = parseComposeVersion(composeRes.out)
		if !info.ComposeOK {
			info.ComposeError = "the compose plugin did not print a version"
		}
	}

	summary := "docker CLI present; the daemon is unreachable"
	if info.DaemonOK {
		summary = fmt.Sprintf("docker %s (api %s), daemon up on %s", info.ServerVersion, info.ServerAPIVersion, info.SocketPath)
	} else if info.ClientVersion != "" {
		summary = fmt.Sprintf("docker client %s; the daemon is unreachable on %s", info.ClientVersion, info.SocketPath)
	}
	if info.ComposeOK {
		summary += ", compose " + info.ComposeVersion
	} else {
		summary += ", compose plugin not available"
	}
	return ops.Result{
		Summary:   summary,
		Data:      info,
		Truncated: verRes.truncated || composeRes.truncated,
		Receipt:   events.SetupReceipt{Kind: "exec", Argv: versionArgv, Target: "docker", At: start},
	}, nil
}

// parseComposeVersion reads `docker compose version --format json` and falls
// back to a bare version line from an older plugin.
func parseComposeVersion(out []byte) (string, bool) {
	var v struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(out, &v); err == nil && v.Version != "" {
		return clip(strings.TrimPrefix(v.Version, "v")), true
	}
	line := strings.TrimSpace(string(out))
	if line == "" || len(line) > 40 || strings.ContainsAny(line, " \t\r\n") {
		return "", false
	}
	return clip(strings.TrimPrefix(line, "v")), true
}

// composePS lists the project's containers.
type composePSOp struct{ d *Driver }

type psContainer struct {
	ID       string `json:"id"`
	Name     string `json:"name,omitempty"`
	Service  string `json:"service,omitempty"`
	State    string `json:"state,omitempty"`
	Health   string `json:"health,omitempty"`
	Image    string `json:"image,omitempty"`
	ExitCode int    `json:"exit_code,omitempty"`
}

// psContainerJSON is the shape `docker compose ps --format json` prints. The
// output struct cannot double as the decode target: Go's case-insensitive
// field match never bridges exit_code to Docker's ExitCode (the underscore
// survives the comparison), so the exit code of an exited container was
// silently decoded as 0.
type psContainerJSON struct {
	ID       string `json:"ID"`
	Name     string `json:"Name"`
	Service  string `json:"Service"`
	State    string `json:"State"`
	Health   string `json:"Health"`
	Image    string `json:"Image"`
	ExitCode int    `json:"ExitCode"`
}

// container is the raw row as the whitelisted output shape.
func (r psContainerJSON) container() psContainer {
	return psContainer{
		ID:       clip(shortID(r.ID)),
		Name:     clip(r.Name),
		Service:  clip(r.Service),
		State:    clip(r.State),
		Health:   clip(r.Health),
		Image:    clip(r.Image),
		ExitCode: r.ExitCode,
	}
}

type composePSData struct {
	File       string        `json:"file"`
	Containers []psContainer `json:"containers"`
	Count      int           `json:"count"`
}

func (composePSOp) Name() string     { return "compose_ps" }
func (composePSOp) Target() string   { return targetKind }
func (composePSOp) Tier() string     { return protocol.TierT0 }
func (composePSOp) Layer() string    { return layerName }
func (composePSOp) Describe() string { return "list the rendered stack's containers and their state" }

func (composePSOp) ParamsSchema() map[string]any { return stackSchema() }

func (composePSOp) Validate(params json.RawMessage, env ops.Env) error {
	_, err := decodeStack(params)
	return err
}

func (o composePSOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	p, err := decodeStack(params)
	if err != nil {
		return ops.Result{}, err
	}
	clean, err := stackFiles(env, p.File)
	if err != nil {
		return ops.Result{}, err
	}
	argv, cleanup, err := pinnedStackArgv(env, p.File, clean, o.d.classOrDefault(), "ps", []string{"--format", "json"}, p.Services)
	if err != nil {
		return ops.Result{}, err
	}
	defer cleanup()
	start := now(env)
	res, err := dockerCommand(ctx, env, argv)
	if err != nil {
		return ops.Result{}, err
	}
	data := composePSData{File: clean, Containers: []psContainer{}}
	if res.code != 0 {
		return ops.Result{
			Summary:   failureSummary("compose ps", res),
			Data:      map[string]any{"file": clean, "argv": argv, "exit_code": res.code, "output": tailText(res.out, maxFailureOutput)},
			Truncated: res.truncated,
			Receipt:   events.SetupReceipt{Kind: "exec", Argv: argv, Target: clean, At: start, ExitCode: res.code},
		}, fmt.Errorf("%w: compose ps exited %d", ops.ErrExec, res.code)
	}
	containers, truncated, perr := parseComposePS(res.out)
	if perr != nil {
		return ops.Result{}, perr
	}
	data.Containers = containers
	data.Count = len(containers)
	return ops.Result{
		Summary:   summarizePS(containers),
		Data:      data,
		Truncated: res.truncated || truncated,
		Receipt:   events.SetupReceipt{Kind: "exec", Argv: argv, Target: clean, At: start},
	}, nil
}

// parseComposePS accepts both shapes `docker compose ps --format json` has
// printed: one JSON array (current plugins) and one object per line (older
// ones).
func parseComposePS(raw []byte) ([]psContainer, bool, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, false, nil
	}
	var raws []psContainerJSON
	if trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &raws); err != nil {
			return nil, false, fmt.Errorf("compose: parsing compose ps output: %w", err)
		}
	} else {
		for _, line := range bytes.Split(trimmed, []byte("\n")) {
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			var one psContainerJSON
			if err := json.Unmarshal(line, &one); err != nil {
				return nil, false, fmt.Errorf("compose: parsing compose ps output: %w", err)
			}
			raws = append(raws, one)
		}
	}
	truncated := len(raws) > maxPSContainers
	if truncated {
		raws = raws[:maxPSContainers]
	}
	rows := make([]psContainer, len(raws))
	for i, r := range raws {
		rows[i] = r.container()
	}
	return rows, truncated, nil
}

// summarizePS is the one-line story: how many containers, and what each
// service is doing.
func summarizePS(rows []psContainer) string {
	if len(rows) == 0 {
		return "no containers: the stack is not running"
	}
	parts := make([]string, 0, len(rows))
	for _, row := range rows {
		desc := row.Service
		if desc == "" {
			desc = row.Name
		}
		state := row.State
		if row.Health != "" {
			state += " (" + row.Health + ")"
		}
		parts = append(parts, strings.TrimSpace(desc+" "+state))
	}
	return fmt.Sprintf("%d containers: %s", len(rows), strings.Join(parts, ", "))
}

// composeConfig renders the stack as compose sees it, with credential values
// masked.
type composeConfigOp struct{ d *Driver }

type composeConfigData struct {
	File       string   `json:"file"`
	Body       string   `json:"body"`
	Bytes      int      `json:"bytes"`
	MaskedKeys []string `json:"masked_keys,omitempty"`
	Truncated  bool     `json:"truncated,omitempty"`
}

func (composeConfigOp) Name() string   { return "compose_config" }
func (composeConfigOp) Target() string { return targetKind }
func (composeConfigOp) Tier() string   { return protocol.TierT0 }
func (composeConfigOp) Layer() string  { return layerName }
func (composeConfigOp) Describe() string {
	return "render the stack as compose resolves it, with credential values masked"
}

func (composeConfigOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{"file": map[string]any{"type": "string"}},
	}
}

func (composeConfigOp) Validate(params json.RawMessage, env ops.Env) error {
	p, err := decodeStack(params)
	if err != nil {
		return err
	}
	if len(p.Services) > 0 {
		return fmt.Errorf("%w: compose config renders the whole project; do not name services", ErrParams)
	}
	return nil
}

func (o composeConfigOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	p, err := decodeStack(params)
	if err != nil {
		return ops.Result{}, err
	}
	if len(p.Services) > 0 {
		return ops.Result{}, fmt.Errorf("%w: compose config renders the whole project; do not name services", ErrParams)
	}
	clean, err := stackFiles(env, p.File)
	if err != nil {
		return ops.Result{}, err
	}
	// --no-interpolate keeps the ${KEY} references instead of resolving them
	// from the environment; the mask then hides the values of credential-named
	// keys, so the preview never carries a secret the person typed.
	argv, cleanup, err := pinnedStackArgv(env, p.File, clean, o.d.classOrDefault(), "config", []string{"--no-interpolate"}, p.Services)
	if err != nil {
		return ops.Result{}, err
	}
	defer cleanup()
	start := now(env)
	res, err := dockerCommand(ctx, env, argv)
	if err != nil {
		return ops.Result{}, err
	}
	if res.code != 0 {
		return ops.Result{
			Summary:   failureSummary("compose config", res),
			Data:      map[string]any{"file": clean, "argv": argv, "exit_code": res.code, "output": tailText(res.out, maxFailureOutput)},
			Truncated: res.truncated,
			Receipt:   events.SetupReceipt{Kind: "exec", Argv: argv, Target: clean, At: start, ExitCode: res.code},
		}, fmt.Errorf("%w: compose config exited %d", ops.ErrExec, res.code)
	}
	masked, keys, err := maskComposeConfig(res.out)
	if err != nil {
		return ops.Result{}, err
	}
	body := string(masked)
	truncated := res.truncated
	if len(body) > maxConfigBody {
		body = body[:maxConfigBody]
		truncated = true
	}
	summary := fmt.Sprintf("rendered %s (%d bytes)", clean, len(body))
	if len(keys) > 0 {
		summary += fmt.Sprintf(", masked %d credential value(s): %s", len(keys), strings.Join(keys, ", "))
	}
	return ops.Result{
		Summary: summary,
		Data: composeConfigData{
			File: clean, Body: body, Bytes: len(res.out), MaskedKeys: keys, Truncated: truncated,
		},
		Truncated: truncated,
		Receipt:   events.SetupReceipt{Kind: "exec", Argv: argv, Target: clean, At: start},
	}, nil
}

// maskMarker replaces a credential value in a config render.
const maskMarker = "***"

// maskComposeConfig parses a `docker compose config` render and replaces the
// value of every credential-named key with maskMarker, returning the keys it
// masked. A ${secret:name} reference is already a reference, not a value, and
// is kept. A document that does not parse is an error, never a body that
// might still carry the value.
func maskComposeConfig(doc []byte) ([]byte, []string, error) {
	dec := yaml.NewDecoder(bytes.NewReader(doc))
	var root map[string]any
	if err := dec.Decode(&root); err != nil {
		return nil, nil, fmt.Errorf("compose: parsing the config render: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, nil, errors.New("compose: the config render has more than one YAML document")
	}
	keys := map[string]bool{}
	maskNode("", root, keys)
	out, err := yaml.Marshal(root)
	if err != nil {
		return nil, nil, fmt.Errorf("compose: encoding the masked render: %w", err)
	}
	return out, sortedKeys(keys), nil
}

// maskNode walks the render. environment accepts both forms compose prints:
// a mapping, and a list of KEY=VALUE strings.
func maskNode(key string, v any, masked map[string]bool) {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if looksCredential(k) && isScalar(val) {
				t[k] = maskedValue(val)
				masked[k] = true
				continue
			}
			maskNode(k, val, masked)
		}
	case []any:
		for i, val := range t {
			if s, ok := val.(string); ok && key == "environment" {
				name, _, found := strings.Cut(s, "=")
				if found && looksCredential(name) {
					t[i] = name + "=" + maskMarker
					masked[name] = true
					continue
				}
			}
			maskNode(key, val, masked)
		}
	}
}

func isScalar(v any) bool {
	switch v.(type) {
	case map[string]any, []any:
		return false
	}
	return true
}

// maskedValue keeps a ${secret:name} reference (it names a secret, it is not
// one) and masks everything else.
func maskedValue(v any) any {
	if s, ok := v.(string); ok && secretRefOnlyRE.MatchString(s) {
		return s
	}
	return maskMarker
}

// containerInspect reads one service's container, dropping every environment
// value and every label but the compose ones.
type containerInspectOp struct{ d *Driver }

// InspectParams names the service to inspect and the compose file it belongs
// to.
type InspectParams struct {
	Service string `json:"service"`
	File    string `json:"file,omitempty"`
}

// inspectContainer is the whitelist this op reads from `docker inspect`. A
// field that is not here (Cmd, Args, Hostname, full labels, network IPs) never
// reaches Data.
type inspectContainer struct {
	ID     string `json:"Id"`
	Name   string `json:"Name"`
	Image  string `json:"Image"`
	Config *struct {
		Image  string            `json:"Image"`
		Env    []string          `json:"Env"`
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	State *struct {
		Status   string `json:"Status"`
		Running  bool   `json:"Running"`
		ExitCode int    `json:"ExitCode"`
		Health   *struct {
			Status string `json:"Status"`
		} `json:"Health"`
	} `json:"State"`
	HostConfig *struct {
		NetworkMode string `json:"NetworkMode"`
	} `json:"HostConfig"`
	NetworkSettings *struct {
		Networks map[string]json.RawMessage `json:"Networks"`
	} `json:"NetworkSettings"`
	Mounts []struct {
		Type        string `json:"Type"`
		Name        string `json:"Name"`
		Source      string `json:"Source"`
		Destination string `json:"Destination"`
		RW          bool   `json:"RW"`
	} `json:"Mounts"`
}

// inspectMount is one mount with the host source dropped unless it is inside
// the project directory.
type inspectMount struct {
	Type        string `json:"type"`
	Name        string `json:"name,omitempty"`
	Source      string `json:"source,omitempty"`
	Destination string `json:"destination"`
	ReadOnly    bool   `json:"read_only,omitempty"`
}

type inspectData struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Service     string            `json:"service,omitempty"`
	Image       string            `json:"image,omitempty"`
	State       string            `json:"state"`
	Running     bool              `json:"running"`
	ExitCode    int               `json:"exit_code"`
	Health      string            `json:"health,omitempty"`
	EnvNames    []string          `json:"env_names"`
	Labels      map[string]string `json:"labels,omitempty"`
	NetworkMode string            `json:"network_mode,omitempty"`
	Networks    []string          `json:"networks,omitempty"`
	Mounts      []inspectMount    `json:"mounts,omitempty"`
}

type containerInspectData struct {
	Service    string        `json:"service"`
	File       string        `json:"file"`
	Containers []inspectData `json:"containers"`
	Count      int           `json:"count"`
	Truncated  bool          `json:"truncated,omitempty"`
}

func (containerInspectOp) Name() string   { return "container_inspect" }
func (containerInspectOp) Target() string { return targetKind }
func (containerInspectOp) Tier() string   { return protocol.TierT0 }
func (containerInspectOp) Layer() string  { return layerName }
func (containerInspectOp) Describe() string {
	return "inspect one service's containers: state, mounts and environment variable names, never values"
}

func (containerInspectOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"service"},
		"properties": map[string]any{
			"service": map[string]any{"type": "string", "minLength": 1, "maxLength": 63},
			"file":    map[string]any{"type": "string"},
		},
	}
}

func (containerInspectOp) Validate(params json.RawMessage, env ops.Env) error {
	_, _, err := decodeInspect(params)
	return err
}

func (o containerInspectOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	p, _, err := decodeInspect(params)
	if err != nil {
		return ops.Result{}, err
	}
	clean, err := stackFiles(env, p.File)
	if err != nil {
		return ops.Result{}, err
	}
	start := now(env)
	// The ids come from this project's compose file, so a service name can
	// never name a container outside it.
	psArgv, cleanup, err := pinnedStackArgv(env, p.File, clean, o.d.classOrDefault(), "ps", []string{"-q"}, []string{p.Service})
	if err != nil {
		return ops.Result{}, err
	}
	defer cleanup()
	psRes, err := dockerCommand(ctx, env, psArgv)
	if err != nil {
		return ops.Result{}, err
	}
	if psRes.code != 0 {
		return ops.Result{
			Summary:   failureSummary("compose ps -q "+p.Service, psRes),
			Data:      map[string]any{"service": p.Service, "argv": psArgv, "exit_code": psRes.code, "output": tailText(psRes.out, maxFailureOutput)},
			Truncated: psRes.truncated,
			Receipt:   events.SetupReceipt{Kind: "exec", Argv: psArgv, Target: clean, At: start, ExitCode: psRes.code},
		}, fmt.Errorf("%w: compose ps exited %d", ops.ErrExec, psRes.code)
	}
	ids, extra := parseContainerIDs(psRes.out)
	if len(ids) == 0 {
		return ops.Result{}, fmt.Errorf("%w: service %s has no container in %s; run compose_up first", ErrStack, q(p.Service), q(clean))
	}
	truncated := psRes.truncated || extra
	data := containerInspectData{Service: p.Service, File: clean, Containers: []inspectData{}, Truncated: truncated}
	for _, id := range ids {
		argv := []string{"docker", "inspect", "--type", "container", "--format", dockerFormatJSON, id}
		res, err := dockerCommand(ctx, env, argv)
		if err != nil {
			return ops.Result{}, err
		}
		if res.code != 0 {
			return ops.Result{
				Summary:   failureSummary("docker inspect", res),
				Data:      map[string]any{"service": p.Service, "argv": argv, "exit_code": res.code, "output": tailText(res.out, maxFailureOutput)},
				Truncated: res.truncated,
				Receipt:   events.SetupReceipt{Kind: "exec", Argv: argv, Target: clean, At: start, ExitCode: res.code},
			}, fmt.Errorf("%w: docker inspect exited %d", ops.ErrExec, res.code)
		}
		var c inspectContainer
		if err := json.Unmarshal(res.out, &c); err != nil {
			return ops.Result{}, fmt.Errorf("compose: parsing docker inspect output: %w", err)
		}
		data.Containers = append(data.Containers, summarizeInspect(c, p.Service, env.Confine.Dir()))
	}
	data.Count = len(data.Containers)
	return ops.Result{
		Summary:   summarizeInspectAll(data),
		Data:      data,
		Truncated: data.Truncated,
		Receipt:   events.SetupReceipt{Kind: "exec", Argv: psArgv, Target: clean, At: start},
	}, nil
}

// decodeInspect decodes and validates container_inspect params.
func decodeInspect(params json.RawMessage) (InspectParams, string, error) {
	var p InspectParams
	if err := decodeParams(params, &p); err != nil {
		return p, "", err
	}
	if !serviceNameRE.MatchString(p.Service) {
		return p, "", fmt.Errorf("%w: service %s is not a service name", ErrParams, q(p.Service))
	}
	if p.File == "" {
		p.File = DefaultFile
	}
	if err := ops.CheckName(p.File); err != nil {
		return p, "", fmt.Errorf("%w: file: %v", ErrPath, err)
	}
	if !isComposeName(p.File) {
		return p, "", fmt.Errorf("%w: file %s is not a compose file name", ErrParams, q(p.File))
	}
	return p, p.File, nil
}

// containerIDRE is what `docker compose ps -q` prints: a short or full
// lowercase container id. Anything else is not passed to `docker inspect`.
var containerIDRE = regexp.MustCompile(`^[0-9a-f]{12,64}$`)

// parseContainerIDs reads ids from `compose ps -q`, caps them, and reports
// whether the cap or an unreadable line cut the list.
func parseContainerIDs(raw []byte) ([]string, bool) {
	var ids []string
	truncated := false
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !containerIDRE.MatchString(line) {
			truncated = true
			continue
		}
		ids = append(ids, line)
	}
	if len(ids) > maxInspectCount {
		ids = ids[:maxInspectCount]
		truncated = true
	}
	return ids, truncated
}

// summarizeInspect turns one docker inspect object into the whitelisted shape.
func summarizeInspect(c inspectContainer, service, projectDir string) inspectData {
	out := inspectData{
		ID:       shortID(c.ID),
		Name:     clip(strings.TrimPrefix(c.Name, "/")),
		Service:  service,
		Image:    clip(c.Image),
		EnvNames: []string{},
	}
	if c.Config != nil {
		if c.Config.Image != "" {
			out.Image = clip(c.Config.Image)
		}
		out.EnvNames = envNames(c.Config.Env)
		out.Labels = composeLabels(c.Config.Labels)
	}
	if c.State != nil {
		out.State = clip(c.State.Status)
		out.Running = c.State.Running
		out.ExitCode = c.State.ExitCode
		if c.State.Health != nil {
			out.Health = clip(c.State.Health.Status)
		}
	}
	if c.HostConfig != nil {
		out.NetworkMode = clip(c.HostConfig.NetworkMode)
	}
	if c.NetworkSettings != nil {
		out.Networks = sortedKeys(c.NetworkSettings.Networks)
	}
	for _, m := range c.Mounts {
		mount := inspectMount{
			Type:        clip(m.Type),
			Name:        clip(m.Name),
			Destination: clip(m.Destination),
			ReadOnly:    !m.RW,
		}
		// A host path outside the project says where the runner lives; the
		// person does not need it to read a mount, so only a project-relative
		// source is kept. A named volume keeps its name.
		if m.Type == "bind" {
			mount.Source = projectRelSource(m.Source, projectDir)
		}
		out.Mounts = append(out.Mounts, mount)
	}
	return out
}

// envNames keeps the names of Docker's KEY=VALUE environment entries and
// drops every value. A name is clipped like any peer text.
func envNames(env []string) []string {
	seen := make(map[string]bool, len(env))
	out := make([]string, 0, len(env))
	for _, entry := range env {
		name, _, _ := strings.Cut(entry, "=")
		name = clip(strings.TrimSpace(name))
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
		if len(out) == maxInspectEnv {
			break
		}
	}
	sort.Strings(out)
	return out
}

// composeLabels keeps the labels that say which project and service the
// container belongs to; every other label can carry anything.
func composeLabels(labels map[string]string) map[string]string {
	out := make(map[string]string, 2)
	for _, key := range []string{"com.docker.compose.project", "com.docker.compose.service"} {
		if v, ok := labels[key]; ok {
			out[key] = clip(v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// projectRelSource turns a host path under the project into a slash path,
// and returns "" for anything else.
func projectRelSource(source, projectDir string) string {
	if source == "" || projectDir == "" {
		return ""
	}
	rel, err := filepath.Rel(projectDir, source)
	if err != nil || rel == "." || filepath.IsAbs(rel) ||
		rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return ""
	}
	return clip(filepath.ToSlash(rel))
}

// shortID is Docker's conventional 12-character id.
func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// summarizeInspectAll is the one-line story of one service's containers.
func summarizeInspectAll(d containerInspectData) string {
	if len(d.Containers) == 0 {
		return "no containers for " + d.Service
	}
	parts := make([]string, 0, len(d.Containers))
	for _, c := range d.Containers {
		state := c.State
		if c.Health != "" {
			state += " (" + c.Health + ")"
		}
		parts = append(parts, fmt.Sprintf("%s %s, %d env names", c.ID, state, len(c.EnvNames)))
	}
	return fmt.Sprintf("%s: %s", d.Service, strings.Join(parts, "; "))
}

// stackSchema is the schema compose_ps and compose_config share.
func stackSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{
			"file": map[string]any{"type": "string"},
			"services": map[string]any{
				"type": "array", "maxItems": maxStackServices,
				"items": map[string]any{"type": "string"},
			},
		},
	}
}

// failureSummary is the first non-empty output line of a failed command plus
// its exit code; the full tail is in Data.
func failureSummary(what string, res commandResult) string {
	line := firstLine(res.out)
	if line == "" {
		return fmt.Sprintf("%s failed (exit %d)", what, res.code)
	}
	return fmt.Sprintf("%s failed (exit %d): %s", what, res.code, line)
}

// firstLine is the first non-empty line of out, clipped.
func firstLine(out []byte) string {
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			return clip(line)
		}
	}
	return ""
}

// maxFailureOutput bounds the command tail a failed op puts in Data; the
// registry's Data cap is the second bound.
const maxFailureOutput = 4 << 10

// tailText is the last max bytes of out as text, so a failure's real cause
// (docker prints warnings before the error) survives.
func tailText(out []byte, max int) string {
	if len(out) <= max {
		return string(out)
	}
	start := len(out) - max
	for start < len(out) && !utf8.RuneStart(out[start]) {
		start++
	}
	return "…" + string(out[start:])
}

// clipTail is tailText clipped to a short summary-sized string.
func clipTail(out []byte, max int) string {
	return clip(tailText(out, max))
}
