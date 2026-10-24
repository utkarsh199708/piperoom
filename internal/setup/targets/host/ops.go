package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// UnitParams name one installed service: service_status and service_restart.
type UnitParams struct {
	// Name is the service name of piperoom-<name>.service; default "connect".
	Name string `json:"name,omitempty"`
}

// PropertiesParams are properties_write's typed params.
type PropertiesParams struct {
	// File is connect-distributed.properties or
	// connect-standalone.properties at the project root (T1), or an
	// absolute path outside the project directory with one of those base
	// names (T2: PropertiesWriteTier). The base name picks the worker mode
	// and its key allow-list.
	File string `json:"file"`
	// Entries are allow-listed keys and their values.
	Entries map[string]string `json:"entries"`
}

// PropertiesReadParams name the worker file properties_read reads.
type PropertiesReadParams struct {
	File string `json:"file"`
}

func nameSchema() map[string]any {
	return map[string]any{"type": "string", "pattern": serviceNameRE.String()}
}

// ---- service_status (T0) ----

type serviceStatusOp struct{ d *Driver }

func (serviceStatusOp) Name() string   { return "service_status" }
func (serviceStatusOp) Target() string { return hostTarget }
func (serviceStatusOp) Tier() string   { return protocol.TierT0 }
func (serviceStatusOp) Layer() string  { return hostLayer }
func (o serviceStatusOp) Describe() string {
	if o.d.goos == "windows" {
		return "report the state of a piperoom service on this host (the service control manager's query)"
	}
	return "report the state of a piperoom service on this host (systemctl --user show; systemctl show where the driver installs system units)"
}

func (serviceStatusOp) ParamsSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false,
		"properties": map[string]any{"name": nameSchema()}}
}

func (o serviceStatusOp) Validate(params json.RawMessage, _ ops.Env) error {
	_, _, err := o.d.decodeUnit(params)
	return err
}

func (o serviceStatusOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	unit, mgr, err := o.d.decodeUnit(params)
	if err != nil {
		return ops.Result{}, err
	}
	return mgr.status(ctx, env, unit)
}

// ---- service_restart (T2) ----

type serviceRestartOp struct{ d *Driver }

func (serviceRestartOp) Name() string   { return "service_restart" }
func (serviceRestartOp) Target() string { return hostTarget }
func (serviceRestartOp) Tier() string   { return protocol.TierT2 }
func (serviceRestartOp) Layer() string  { return hostLayer }
func (o serviceRestartOp) Describe() string {
	if o.d.goos == "windows" {
		return "start or restart a piperoom service this project installed (service control manager stop and start)"
	}
	return "start or restart a piperoom service this project installed (systemctl --user restart; systemctl restart for a system unit)"
}

func (serviceRestartOp) ParamsSchema() map[string]any { return serviceStatusOp{}.ParamsSchema() }

func (o serviceRestartOp) Validate(params json.RawMessage, _ ops.Env) error {
	_, _, err := o.d.decodeUnit(params)
	return err
}

func (o serviceRestartOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	unit, mgr, err := o.d.decodeUnit(params)
	if err != nil {
		return ops.Result{}, err
	}
	return mgr.restart(ctx, env, unit)
}

// decodeUnit decodes UnitParams and returns the service name and the manager.
func (d *Driver) decodeUnit(params json.RawMessage) (string, serviceManager, error) {
	var p UnitParams
	if err := decodeStrict(params, &p); err != nil {
		return "", nil, err
	}
	unit, err := d.serviceName(p.Name)
	if err != nil {
		return "", nil, err
	}
	mgr, err := d.manager()
	return unit, mgr, err
}

// ---- service_install (T2) ----

type serviceInstallOp struct{ d *Driver }

func (serviceInstallOp) Name() string   { return "service_install" }
func (serviceInstallOp) Target() string { return hostTarget }
func (serviceInstallOp) Tier() string   { return protocol.TierT2 }
func (serviceInstallOp) Layer() string  { return hostLayer }
func (serviceInstallOp) Describe() string {
	return "render the Kafka Connect worker unit from the template and enable it (a user unit, systemctl --user enable, by default; a system unit in /etc/systemd/system when the runner is root or the target is production)"
}

func (serviceInstallOp) ParamsSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false,
		"properties": map[string]any{
			"name": nameSchema(),
			"mode": map[string]any{"type": "string", "enum": []string{ModeDistributed, ModeStandalone}},
		}}
}

func (o serviceInstallOp) Validate(params json.RawMessage, _ ops.Env) error {
	var p ServiceParams
	if err := decodeStrict(params, &p); err != nil {
		return err
	}
	if _, err := p.normalize(); err != nil {
		return err
	}
	_, err := o.d.manager()
	return err
}

func (o serviceInstallOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	s, mgr, err := o.prepare(params, env)
	if err != nil {
		return ops.Result{}, err
	}
	return mgr.install(ctx, env, s)
}

// Preview is the unit file as a diff against what is on disk now (design
// §2.5, plan §3.4: a templated file shows its rendered diff). It has the
// signature of the design's Op.Preview, so the catalogue can call it once
// ops gains the optional interface.
func (o serviceInstallOp) Preview(ctx context.Context, params json.RawMessage, env ops.Env) (events.PreviewResult, error) {
	s, mgr, err := o.prepare(params, env)
	if err != nil {
		return events.PreviewResult{}, err
	}
	text, err := mgr.artifact(s)
	if err != nil {
		return events.PreviewResult{}, err
	}
	return o.d.diffPreview(ctx, env, s.UnitFile, string(text), installEffect(s))
}

// installEffect is the sentence a service_install preview ends with: what the
// step runs, in the shape the platform's artifact has. A Windows service has no
// ExecStart to read (the SCM runs the copied runner with the recorded
// arguments), so the unit's line would read "runs ." there.
func installEffect(s serviceSpec) string {
	if s.ExePath == "" {
		return "the unit " + s.Unit + " is enabled and runs " + strings.Join(s.Rendered.ExecStart, " ")
	}
	return "the service " + s.Unit + " is installed (automatic start) and runs " + s.ExePath + " " + strings.Join(s.Args, " ")
}

func (o serviceInstallOp) prepare(params json.RawMessage, env ops.Env) (serviceSpec, serviceManager, error) {
	var p ServiceParams
	if err := decodeStrict(params, &p); err != nil {
		return serviceSpec{}, nil, err
	}
	mgr, err := o.d.manager()
	if err != nil {
		return serviceSpec{}, nil, err
	}
	s, err := o.d.spec(p, env)
	if err != nil {
		return serviceSpec{}, nil, err
	}
	s, err = mgr.complete(env, s)
	return s, mgr, err
}

// ---- properties_write (T1 under the project, T2 outside it) ----

type propertiesWriteOp struct{ d *Driver }

func (propertiesWriteOp) Name() string   { return "properties_write" }
func (propertiesWriteOp) Target() string { return hostTarget }

// Tier is the op's floor, T1: a worker file at the project root. A file
// outside the project directory raises the step to T2 (TierFor), which the
// catalogue and the server must read for the step to run (Options.TierRouted).
func (propertiesWriteOp) Tier() string  { return protocol.TierT1 }
func (propertiesWriteOp) Layer() string { return hostLayer }
func (propertiesWriteOp) Describe() string {
	return "write a Kafka Connect worker properties file (distributed or standalone) from allow-listed keys: at the project root (T1), or an absolute path outside the project directory (T2, the driver's click)"
}

// TierFor is the tier one properties_write step needs, from its params alone
// (PropertiesWriteTier).
func (propertiesWriteOp) TierFor(params json.RawMessage) (string, error) {
	return PropertiesWriteTier(params)
}

// PropertiesWriteTier is the lead ruling on U8a as a function of the params,
// with no I/O and no knowledge of the runner's OS, so the server can compute
// it when it builds the step: a worker file named by its project-root name is
// T1; an absolute path (necessarily outside the project directory: the
// runner refuses an absolute path under it) is T2 and needs the driver's
// click. Params that name neither are an error.
func PropertiesWriteTier(params json.RawMessage) (string, error) {
	var p PropertiesParams
	if err := decodeStrict(params, &p); err != nil {
		return "", err
	}
	if _, ok := workerFiles[p.File]; ok {
		return protocol.TierT1, nil
	}
	if absoluteShape(p.File) {
		return protocol.TierT2, nil
	}
	return "", fmt.Errorf("%w: file %s is neither a worker file at the project root nor an absolute path", ErrProperties, q(p.File))
}

func (propertiesWriteOp) ParamsSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false,
		"required": []string{"file", "entries"},
		"properties": map[string]any{
			// One of the project-root worker files, or an absolute path
			// outside the project whose base name is one of them (T2).
			"file": map[string]any{"type": "string", "minLength": 1, "maxLength": maxTemplatePath},
			"entries": map[string]any{"type": "object", "minProperties": 1, "maxProperties": maxEntries,
				"additionalProperties": map[string]any{"type": "string"}},
		}}
}

func (o propertiesWriteOp) Validate(params json.RawMessage, env ops.Env) error {
	_, _, err := o.prepare(params, env)
	return err
}

func (o propertiesWriteOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	file, wp, err := o.prepare(params, env)
	if err != nil {
		return ops.Result{}, err
	}
	// Every plugin.path entry is PluginDir or under it, a mount since
	// Register, so the worker never loads from a directory file_write fills.
	text := wp.render()
	var (
		clean   string
		changed bool
	)
	if file.outside {
		clean = file.name
		changed, err = writeOutside(file.dir, file.base, env.Confine.Dir(), text, 0o600)
	} else {
		clean, changed, err = writeConfined(env, file.name, text, 0o600)
	}
	if err != nil {
		return ops.Result{}, err
	}
	state := "wrote"
	if !changed {
		state = "unchanged"
	}
	summary := fmt.Sprintf("%s %s (%s worker, %d keys)", state, clean, wp.mode, len(wp.keys))
	if file.outside {
		summary += ", outside the project directory"
	}
	if len(wp.defaults) > 0 {
		summary += "; defaults added: " + strings.Join(wp.defaults, ", ")
	}
	return ops.Result{
		Summary: summary,
		Data: map[string]any{"file": clean, "mode": wp.mode, "keys": wp.keys, "defaults_added": wp.defaults,
			"changed": changed, "bytes": len(text), "outside_project": file.outside, "tier": file.tier},
		Receipt: events.SetupReceipt{Kind: "file", Target: clean},
	}, nil
}

// Preview diffs the file properties_write would write against the one on
// disk.
func (o propertiesWriteOp) Preview(ctx context.Context, params json.RawMessage, env ops.Env) (events.PreviewResult, error) {
	file, wp, err := o.prepare(params, env)
	if err != nil {
		return events.PreviewResult{}, err
	}
	effect := "the " + wp.mode + " worker reads it at its next restart"
	if file.outside {
		effect = "a file outside the project directory is replaced (T2); the " + wp.mode + " worker that reads it picks it up at its next restart"
	}
	return o.d.diffPreview(ctx, env, file.name, string(wp.render()), effect)
}

// propsFile is the file a properties_write step names, resolved.
type propsFile struct {
	name    string // the project-root name, or the absolute path outside
	outside bool
	dir     string // outside only: the parent directory, native form
	base    string // outside only: the base name
	mode    string // the worker mode the base name picks
	tier    string // PropertiesWriteTier
}

// resolvePropsFile classifies p: a project-root worker file (T1), or an
// absolute path outside the project directory whose base name is a worker
// file (T2). The mode, and so the key allow-list, comes from the base name in
// both cases.
func (o propertiesWriteOp) resolvePropsFile(name string, env ops.Env) (propsFile, error) {
	if mode, ok := workerFiles[name]; ok {
		return propsFile{name: name, mode: mode, tier: protocol.TierT1}, nil
	}
	if !absoluteShape(name) {
		return propsFile{}, fmt.Errorf("%w: file %s is not one of %s (the worker files at the project root) nor an absolute path outside the project directory",
			ErrProperties, q(name), strings.Join(sortedKeys(workerFiles), ", "))
	}
	dir, base, err := checkOutsidePath(name)
	if err != nil {
		return propsFile{}, err
	}
	mode, ok := workerFiles[base]
	if !ok {
		return propsFile{}, fmt.Errorf("%w: %s outside the project directory must be named %s",
			ErrProperties, q(name), strings.Join(sortedKeys(workerFiles), " or "))
	}
	// Under the project by spelling: name it by its root name, so the
	// project's own rule (and T1) applies. A path that reaches the project by
	// identity (a bind mount, a case-insensitive volume) is refused at write
	// time by openOutsideDir.
	if _, rerr := relUnder(strings.ReplaceAll(env.Confine.Dir(), "\\", "/"), strings.ReplaceAll(name, "\\", "/")); rerr == nil {
		return propsFile{}, fmt.Errorf("%w: %s is under the project directory; name it %s", ErrProperties, q(name), base)
	}
	return propsFile{name: name, outside: true, dir: dir, base: base, mode: mode, tier: protocol.TierT2}, nil
}

func (o propertiesWriteOp) prepare(params json.RawMessage, env ops.Env) (propsFile, workerProps, error) {
	var p PropertiesParams
	if err := decodeStrict(params, &p); err != nil {
		return propsFile{}, workerProps{}, err
	}
	if env.Confine == nil {
		return propsFile{}, workerProps{}, errors.New("host: the project root is not open")
	}
	file, err := o.resolvePropsFile(p.File, env)
	if err != nil {
		return propsFile{}, workerProps{}, err
	}
	// The same allow-list and validators on both sides of the project
	// boundary: path values (plugin.path, the offset file) still name the
	// project's own directories.
	wp, err := prepareProps(file.mode, p.Entries, env.Confine.Dir(), o.d.classOrDefault())
	if err != nil {
		return propsFile{}, workerProps{}, err
	}
	if file.outside && !o.d.tierRouted {
		return propsFile{}, workerProps{}, fmt.Errorf("%w: %s is outside the project directory, which needs a T2 step, and this catalogue does not yet route properties_write by its params' tier; nothing outside the project is written",
			ErrTierRouting, q(p.File))
	}
	return file, wp, nil
}

// ---- properties_read (T0) ----

type propertiesReadOp struct{ d *Driver }

func (propertiesReadOp) Name() string   { return "properties_read" }
func (propertiesReadOp) Target() string { return hostTarget }
func (propertiesReadOp) Tier() string   { return protocol.TierT0 }
func (propertiesReadOp) Layer() string  { return hostLayer }
func (propertiesReadOp) Describe() string {
	return "read a Kafka Connect worker properties file and report missing and wrong-mode keys"
}

func (propertiesReadOp) ParamsSchema() map[string]any {
	names := append(sortedKeys(workerFiles), sortedKeys(sampleFiles)...)
	return map[string]any{"type": "object", "additionalProperties": false, "required": []string{"file"},
		"properties": map[string]any{"file": map[string]any{"type": "string", "enum": names}}}
}

func (propertiesReadOp) Validate(params json.RawMessage, _ ops.Env) error {
	_, _, err := decodeRead(params)
	return err
}

func (o propertiesReadOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	file, mode, err := decodeRead(params)
	if err != nil {
		return ops.Result{}, err
	}
	content, exists, err := o.d.readCurrent(ctx, env, file)
	if err != nil {
		return ops.Result{}, err
	}
	if !exists {
		return ops.Result{
			Summary: file + " does not exist",
			Data:    map[string]any{"file": file, "mode": mode, "exists": false},
			Receipt: events.SetupReceipt{Kind: "file", Target: file},
		}, nil
	}
	entries, order := parseProperties(content)
	shown := make(map[string]string, len(entries))
	var missing, otherMode []string
	for _, key := range order {
		_, allowed, other := ruleFor(mode, key)
		if other {
			otherMode = append(otherMode, key)
		}
		// file_read already redacted the content; a key that names a
		// credential and is not one of the allow-listed worker keys
		// (key.converter is not a key) is masked whole as well.
		if !allowed && !other && actions.LooksSecret(key) {
			shown[key] = "[masked]"
			continue
		}
		shown[key] = entries[key]
	}
	for _, key := range requiredKeys[mode] {
		if _, ok := entries[key]; !ok {
			missing = append(missing, key)
		}
	}
	summary := fmt.Sprintf("%s (%s worker): %d keys", file, mode, len(order))
	if len(missing) > 0 {
		summary += "; missing " + strings.Join(missing, ", ")
	}
	if _, ok := entries["plugin.path"]; !ok {
		summary += "; plugin.path unset"
	}
	if len(otherMode) > 0 {
		summary += "; keys of the other mode: " + strings.Join(otherMode, ", ")
	}
	return ops.Result{
		Summary: summary,
		Data: map[string]any{"file": file, "mode": mode, "exists": true, "entries": shown, "keys": order,
			"missing": missing, "other_mode_keys": otherMode},
		Receipt: events.SetupReceipt{Kind: "file", Target: file},
	}, nil
}

func decodeRead(params json.RawMessage) (string, string, error) {
	var p PropertiesReadParams
	if err := decodeStrict(params, &p); err != nil {
		return "", "", err
	}
	if mode, ok := workerFiles[p.File]; ok {
		return p.File, mode, nil
	}
	if mode, ok := sampleFiles[p.File]; ok {
		return p.File, mode, nil
	}
	return "", "", fmt.Errorf("%w: file %s is not a worker properties file this target reads", ErrProperties, q(p.File))
}

// ---- shared ----

// diffPreview is a PreviewResult whose Expected carries the line diff of the
// file now → after the step.
func (d *Driver) diffPreview(ctx context.Context, env ops.Env, file, after, effect string) (events.PreviewResult, error) {
	before, exists, err := d.readAny(ctx, env, file)
	if err != nil {
		return events.PreviewResult{}, err
	}
	current := file + " does not exist"
	if exists {
		current = fmt.Sprintf("%s has %d lines", file, len(splitLines(before)))
	}
	diff := lineDiff(before, after)
	if exists && before == after {
		diff = "(no change)"
	}
	return events.PreviewResult{
		Target:   file,
		Current:  current,
		Expected: "after this step " + effect + ". Diff of " + file + " (- now, + after):\n" + diff,
	}, nil
}

// readAny reads a project file through file_read, or an absolute file outside
// the project (a system unit, an outside worker file) through readOutside.
func (d *Driver) readAny(ctx context.Context, env ops.Env, file string) (string, bool, error) {
	if !absoluteShape(file) {
		return d.readCurrent(ctx, env, file)
	}
	if env.Confine == nil {
		return "", false, errors.New("host: the project root is not open")
	}
	dir, base, err := checkOutsidePath(file)
	if err != nil {
		return "", false, err
	}
	return readOutside(dir, base, env.Confine.Dir())
}

// decodeStrict fills dst from params, refusing unknown fields and trailing
// JSON. Empty params are the empty object.
func decodeStrict(raw json.RawMessage, dst any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("%w: %v", ErrParams, err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing JSON", ErrParams)
	}
	return nil
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
