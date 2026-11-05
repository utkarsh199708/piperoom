package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/compose"
)

// Bounds of the status read: the command output is the host's text and the
// properties are a fixed list, so nothing here grows with a peer's input.
const (
	maxCommandOutput = 64 << 10
	maxTailOutput    = 4 << 10
	// maxPSRows bounds the parsed `compose ps` rows; a service one project
	// renders has one container.
	maxPSRows = 16
)

// showProperties are the systemd properties server_status reads, in this
// order. Nothing else from `systemctl show` reaches Data.
var showProperties = []string{
	"LoadState", "ActiveState", "SubState", "UnitFileState", "Result",
	"MainPID", "ExecMainStatus", "NRestarts", "FragmentPath",
}

// unitSegmentRE is a service name segment for piperoom-<segment>.service.
var unitSegmentRE = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?$`)

// StatusParams name what server_status reads.
type StatusParams struct {
	// Name is the Compose service in compose mode (default "dbzserver") or
	// the piperoom unit segment in service mode (default "dbzserver").
	Name string `json:"name,omitempty"`
}

// statusName resolves the params' name for a mode.
func statusName(mode, name string) (string, error) {
	if name == "" {
		return DefaultService, nil
	}
	switch mode {
	case ModeCompose:
		if !serviceNameRE.MatchString(name) {
			return "", fmt.Errorf("%w: service %s is not a compose service name", ErrParams, q(name))
		}
	case ModeService:
		if !unitSegmentRE.MatchString(name) {
			return "", fmt.Errorf("%w: unit name %s is not a piperoom unit name segment", ErrParams, q(name))
		}
	}
	return name, nil
}

// composeStatus reads the rendered stack's service state through
// `docker compose ps`, after validating the compose file on disk: a file that
// no longer passes the validator is never handed to docker.
func (o serverStatusOp) composeStatus(ctx context.Context, env ops.Env, name string) (ops.Result, error) {
	if env.Confine == nil {
		return ops.Result{}, errors.New("server: the project root is not open")
	}
	dir := env.Confine.Dir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		return ops.Result{}, fmt.Errorf("server: opening the project root: %w", err)
	}
	defer root.Close()
	fi, err := root.Lstat(ComposeFile)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return ops.Result{
			Summary: ComposeFile + " is not written yet (run server_compose_write)",
			Data:    map[string]any{"mode": ModeCompose, "file": ComposeFile, "service": name, "installed": false, "running": false},
			Receipt: events.SetupReceipt{Kind: "file", Target: ComposeFile},
		}, nil
	case err != nil:
		return ops.Result{}, fmt.Errorf("server: stat %s: %w", q(ComposeFile), err)
	case !fi.Mode().IsRegular():
		return ops.Result{}, fmt.Errorf("%w: %s is not a regular file", ErrStatus, q(ComposeFile))
	}
	doc, err := root.ReadFile(ComposeFile)
	if err != nil {
		return ops.Result{}, fmt.Errorf("server: reading %s: %w", q(ComposeFile), err)
	}
	if err := compose.ValidateYAML(doc, o.d.classOrDefault()); err != nil {
		return ops.Result{}, fmt.Errorf("%w: %s on disk no longer passes the Compose validator: %w", ErrStatus, q(ComposeFile), err)
	}
	_, envErr := root.Lstat(EnvFileName)
	argv, err := composeArgv(dir, envErr == nil, name)
	if err != nil {
		return ops.Result{}, err
	}
	start := now(env)
	res, err := runCommand(ctx, env, argv)
	if err != nil {
		return ops.Result{}, err
	}
	receipt := events.SetupReceipt{Kind: "exec", Argv: argv, Target: ComposeFile, At: start, ExitCode: res.code}
	if res.code != 0 {
		return failed("docker compose ps", res, ComposeFile), fmt.Errorf("%w: docker compose ps exited %d", ops.ErrExec, res.code)
	}
	rows, perr := parseComposePS(res.out)
	if perr != nil {
		return ops.Result{}, fmt.Errorf("%w: %s", ErrStatus, perr)
	}
	data := map[string]any{
		"mode": ModeCompose, "file": ComposeFile, "service": name, "installed": true,
		"running": false, "containers": len(rows),
	}
	summary := name + " has no container: the stack is not running"
	if len(rows) > 0 {
		row := rows[0]
		data["running"] = row.State == "running"
		data["state"] = row.State
		data["health"] = row.Health
		data["exit_code"] = row.ExitCode
		data["container_id"] = row.ID
		data["image"] = row.Image
		summary = fmt.Sprintf("%s: %s", name, row.State)
		if row.Health != "" {
			summary += " (" + row.Health + ")"
		}
		if row.ExitCode != 0 {
			summary += fmt.Sprintf(", exit code %d", row.ExitCode)
		}
		if len(rows) > 1 {
			summary += fmt.Sprintf(", %d containers", len(rows))
		}
	}
	return ops.Result{Summary: summary, Data: data, Truncated: res.truncated, Receipt: receipt}, nil
}

// composeStatusRow is one `docker compose ps --format json` row: the fields
// the CLI prints, clipped before they become Data.
type composeStatusRow struct {
	ID       string `json:"ID"`
	Name     string `json:"Name"`
	Service  string `json:"Service"`
	State    string `json:"State"`
	Health   string `json:"Health"`
	Image    string `json:"Image"`
	ExitCode int    `json:"ExitCode"`
}

// parseComposePS accepts both shapes `docker compose ps --format json` has
// printed: one JSON array (current plugins) and one object per line (older
// ones). IDs are shortened, as in the Compose driver's compose_ps.
func parseComposePS(raw []byte) ([]composeStatusRow, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, nil
	}
	var rows []composeStatusRow
	if trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &rows); err != nil {
			return nil, fmt.Errorf("server: parsing compose ps output: %w", err)
		}
	} else {
		for _, line := range bytes.Split(trimmed, []byte("\n")) {
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			var one composeStatusRow
			if err := json.Unmarshal(line, &one); err != nil {
				return nil, fmt.Errorf("server: parsing compose ps output: %w", err)
			}
			rows = append(rows, one)
		}
	}
	for i := range rows {
		rows[i].ID = shortID(rows[i].ID)
		rows[i].Name = clipText(rows[i].Name, 128)
		rows[i].Service = clipText(rows[i].Service, 64)
		rows[i].State = clipText(rows[i].State, 32)
		rows[i].Health = clipText(rows[i].Health, 32)
		rows[i].Image = clipText(rows[i].Image, 256)
	}
	if len(rows) > maxPSRows {
		rows = rows[:maxPSRows]
	}
	return rows, nil
}

// shortID keeps the 12 hex characters docker prints short.
func shortID(id string) string {
	id = clipText(id, 64)
	if i := strings.IndexByte(id, ':'); i > 0 && len(id) > 12 {
		id = id[:i]
	}
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// serviceStatus reads the user-level unit's state through `systemctl --user
// show`, with the same property list and parsing the host target uses for its
// worker unit.
func (o serverStatusOp) serviceStatus(ctx context.Context, env ops.Env, name string) (ops.Result, error) {
	if o.d.goos != "linux" {
		return ops.Result{}, fmt.Errorf("%w (%s: the service mode is systemd on Linux)", ErrUnsupported, o.d.goos)
	}
	unit := "piperoom-" + name + ".service"
	argv := []string{"systemctl", "--user", "show", "--no-pager", "--property=" + strings.Join(showProperties, ","), unit}
	start := now(env)
	res, err := runCommand(ctx, env, argv)
	if err != nil {
		return ops.Result{}, err
	}
	receipt := events.SetupReceipt{Kind: "exec", Argv: argv, Target: unit, At: start, ExitCode: res.code}
	if res.code != 0 {
		return failed("systemctl show", res, unit), fmt.Errorf("%w: systemctl show exited %d", ops.ErrExec, res.code)
	}
	st := parseShow(res.out)
	data := st.data(unit)
	// Unit names are per OS user, not per project: another project's
	// service_install may own this name. Say so rather than report its server
	// as this project's.
	own, want := o.d.ownFragment(env, unit, st)
	data["this_project"] = own
	summary := statusSummary(unit, st)
	if !own && st["FragmentPath"] != "" {
		summary += fmt.Sprintf("; loaded from %s, not this project's %s", q(st["FragmentPath"]), q(want))
	}
	return ops.Result{Summary: summary, Data: data, Truncated: res.truncated, Receipt: receipt}, nil
}

// ownFragment reports whether systemd loads unit from this project's unit
// file, and returns that file's absolute path. A FragmentPath that is a
// symlink to it (a linked unit seen through the user's search path) counts.
func (d *Driver) ownFragment(env ops.Env, unit string, st unitState) (bool, string) {
	if env.Confine == nil {
		return false, ""
	}
	want := env.Confine.Dir() + "/" + UnitDir + "/" + unit
	got := st["FragmentPath"]
	if got == "" {
		return false, want
	}
	if got == want {
		return true, want
	}
	if d.resolve == nil {
		return false, want
	}
	a, aerr := d.resolve(got)
	b, berr := d.resolve(want)
	return aerr == nil && berr == nil && a == b, want
}

// unitState is the parsed `systemctl show` output.
type unitState map[string]string

// parseShow reads KEY=value lines; keys not in showProperties are ignored and
// values are clipped, since the output is the host's text.
func parseShow(out []byte) unitState {
	want := make(map[string]bool, len(showProperties))
	for _, p := range showProperties {
		want[p] = true
	}
	st := unitState{}
	for _, line := range strings.Split(string(out), "\n") {
		k, v, ok := strings.Cut(strings.TrimRight(line, "\r"), "=")
		if ok && want[k] {
			st[k] = clipText(v, 256)
		}
	}
	return st
}

// data is the unit state in the shape server_status returns.
func (st unitState) data(unit string) map[string]any {
	return map[string]any{
		"mode": ModeService, "unit": unit, "scope": "user",
		"installed":        st["LoadState"] == "loaded",
		"running":          st["ActiveState"] == "active" && st["SubState"] == "running",
		"load_state":       st["LoadState"],
		"active_state":     st["ActiveState"],
		"sub_state":        st["SubState"],
		"unit_file_state":  st["UnitFileState"],
		"result":           st["Result"],
		"main_pid":         st["MainPID"],
		"exec_main_status": st["ExecMainStatus"],
		"restarts":         st["NRestarts"],
		"fragment_path":    st["FragmentPath"],
	}
}

// statusSummary is the one-line unit story.
func statusSummary(unit string, st unitState) string {
	if st["LoadState"] == "not-found" || st["LoadState"] == "" {
		return unit + " is not installed"
	}
	s := fmt.Sprintf("%s: %s (%s)", unit, st["ActiveState"], st["SubState"])
	if st["UnitFileState"] != "" {
		s += ", " + st["UnitFileState"]
	}
	if r := st["Result"]; r != "" && r != "success" {
		s += ", last result " + r + ", exit status " + st["ExecMainStatus"]
	}
	return s
}

// commandResult is one external command: the capped output, the exit code and
// whether the cap cut the output.
type commandResult struct {
	out       []byte
	code      int
	truncated bool
}

// runCommand runs one literal argv through the catalogue's injectable
// command runner (argv slice, no shell, the request's deadline). It refuses
// anything but the two programs this driver reads state with.
func runCommand(ctx context.Context, env ops.Env, argv []string) (commandResult, error) {
	if len(argv) < 2 || (argv[0] != "docker" && argv[0] != "systemctl") {
		return commandResult{}, errors.New("server: refusing a command that is not docker or systemctl")
	}
	if env.Exec == nil {
		return commandResult{}, errors.New("server: no command runner configured")
	}
	raw, code, err := env.Exec(ctx, argv[0], argv[1:]...)
	res := commandResult{out: raw, code: code}
	if len(raw) > maxCommandOutput {
		res.out = raw[:maxCommandOutput]
		res.truncated = true
	}
	if err != nil {
		return res, fmt.Errorf("%w: running %s: %w", ops.ErrExec, argv[0], err)
	}
	return res, nil
}

// failed turns a non-zero exit into the op's result: the first output line in
// the summary and the tail in Data, both clipped.
func failed(what string, res commandResult, target string) ops.Result {
	summary := fmt.Sprintf("%s failed (exit %d)", what, res.code)
	if line := firstLine(res.out); line != "" {
		summary += ": " + line
	}
	return ops.Result{
		Summary:   summary,
		Data:      map[string]any{"target": target, "exit_code": res.code, "output": tail(res.out)},
		Truncated: res.truncated,
		Receipt:   events.SetupReceipt{Kind: "exec", Target: target, ExitCode: res.code},
	}
}

// firstLine is the first non-empty output line, clipped.
func firstLine(out []byte) string {
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			return clipText(line, maxQuoted)
		}
	}
	return ""
}

// tail is the last maxTailOutput bytes of out, on a rune boundary.
func tail(out []byte) string {
	if len(out) <= maxTailOutput {
		return strings.ToValidUTF8(string(out), "?")
	}
	start := len(out) - maxTailOutput
	for start < len(out) && (out[start]&0xC0) == 0x80 {
		start++
	}
	return "…" + strings.ToValidUTF8(string(out[start:]), "?")
}

// ---- server_status (T0) ----

type serverStatusOp struct{ d *Driver }

func (serverStatusOp) Name() string   { return "server_status" }
func (serverStatusOp) Target() string { return targetKind }
func (serverStatusOp) Tier() string   { return protocol.TierT0 }
func (serverStatusOp) Layer() string  { return layerName }
func (serverStatusOp) Describe() string {
	return "report the Debezium Server's state: the Compose service's container or the user-level unit"
}

func (serverStatusOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{"name": map[string]any{"type": "string"}},
	}
}

func (o serverStatusOp) Validate(params json.RawMessage, _ ops.Env) error {
	var p StatusParams
	if err := decodeStrict(params, &p); err != nil {
		return err
	}
	if _, err := statusName(o.d.mode, p.Name); err != nil {
		return err
	}
	if o.d.mode == ModeService && o.d.goos != "linux" {
		return fmt.Errorf("%w (%s: the service mode is systemd on Linux)", ErrUnsupported, o.d.goos)
	}
	return nil
}

func (o serverStatusOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	var p StatusParams
	if err := decodeStrict(params, &p); err != nil {
		return ops.Result{}, err
	}
	name, err := statusName(o.d.mode, p.Name)
	if err != nil {
		return ops.Result{}, err
	}
	if o.d.mode == ModeService {
		return o.serviceStatus(ctx, env, name)
	}
	return o.composeStatus(ctx, env, name)
}
