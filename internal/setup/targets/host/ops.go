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
	// connect-standalone.properties at the project root; the name picks the
	// worker mode and its key allow-list.
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
	return "report the state of a piperoom service on this host (systemctl --user show)"
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
	return "start or restart a piperoom service this project installed (systemctl --user restart)"
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
	return "render the Kafka Connect worker unit from the template and enable it (systemctl --user enable)"
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

// ---- properties_write (T1) ----

type propertiesWriteOp struct{ d *Driver }

func (propertiesWriteOp) Name() string   { return "properties_write" }
func (propertiesWriteOp) Target() string { return hostTarget }
func (propertiesWriteOp) Tier() string   { return protocol.TierT1 }
func (propertiesWriteOp) Layer() string  { return hostLayer }
func (propertiesWriteOp) Describe() string {
	return "write a Kafka Connect worker properties file (distributed or standalone) from allow-listed keys"
}

func (propertiesWriteOp) ParamsSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false,
		"required": []string{"file", "entries"},
		"properties": map[string]any{
			"file": map[string]any{"type": "string", "enum": sortedKeys(workerFiles)},
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
	clean, changed, err := writeConfined(env, file, text, 0o600)
	if err != nil {
		return ops.Result{}, err
	}
	state := "wrote"
	if !changed {
		state = "unchanged"
	}
	summary := fmt.Sprintf("%s %s (%s worker, %d keys)", state, clean, wp.mode, len(wp.keys))
	if len(wp.defaults) > 0 {
		summary += "; defaults added: " + strings.Join(wp.defaults, ", ")
	}
	return ops.Result{
		Summary: summary,
		Data: map[string]any{"file": clean, "mode": wp.mode, "keys": wp.keys, "defaults_added": wp.defaults,
			"changed": changed, "bytes": len(text)},
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
	return o.d.diffPreview(ctx, env, file, string(wp.render()), "the "+wp.mode+" worker reads it at its next restart")
}

func (o propertiesWriteOp) prepare(params json.RawMessage, env ops.Env) (string, workerProps, error) {
	var p PropertiesParams
	if err := decodeStrict(params, &p); err != nil {
		return "", workerProps{}, err
	}
	mode, ok := workerFiles[p.File]
	if !ok {
		return "", workerProps{}, fmt.Errorf("%w: file %s is not one of %s (the worker files at the project root; nothing outside the project directory is written)",
			ErrProperties, q(p.File), strings.Join(sortedKeys(workerFiles), ", "))
	}
	if env.Confine == nil {
		return "", workerProps{}, errors.New("host: the project root is not open")
	}
	wp, err := prepareProps(mode, p.Entries, env.Confine.Dir(), o.d.classOrDefault())
	return p.File, wp, err
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
	before, exists, err := d.readCurrent(ctx, env, file)
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
