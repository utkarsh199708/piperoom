package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"piperoom/internal/clients/connect"
	"piperoom/internal/redact"
)

// Compile-time checks that every write action can preview itself (F4).
var (
	_ Preview = (*restartTask)(nil)
	_ Preview = (*restartConnector)(nil)
	_ Preview = (*resumeConnector)(nil)
	_ Preview = (*updateConnectorConfig)(nil)
	_ Preview = (*pauseConnector)(nil)
	_ Preview = (*setCompatibility)(nil)
	_ Preview = (*registerSchema)(nil)
)

// configValidator and compatibilityChecker are optional adapters for the two
// dry-run endpoints. They stay off the contract Reader interfaces so fakes,
// demo and evals clients keep compiling; a client without them gets a
// "skipped" dry-run.
type (
	configValidator interface {
		ValidateConfig(ctx context.Context, class string, cfg map[string]string) (connect.ValidateResult, error)
	}
	compatibilityChecker interface {
		CheckCompatibility(ctx context.Context, subject, schema string) (ok bool, messages []string, err error)
	}
)

const (
	sourceCompat   = "registry compatibility check"
	sourceValidate = "connect config validate"
	// maxDryRunText caps DryRunResult.Text (contract: 2 KiB).
	maxDryRunText = 2048
	noDryRun      = "client has no dry-run"
)

// previewErr wraps a read failure of Preview the same way for every action.
func previewErr(action string, err error) error {
	return fmt.Errorf("actions: preview %s: %w", action, err)
}

// rollbackText renders an undo as "<action> {json params}".
func rollbackText(action string, params map[string]string) string {
	b, _ := json.Marshal(params) // map[string]string cannot fail to marshal
	return action + " " + string(b)
}

// dryRunText redacts and caps a dry-run message.
func dryRunText(s string) string {
	s, _ = redact.Text(s)
	if len(s) > maxDryRunText {
		s = strings.ToValidUTF8(s[:maxDryRunText], "")
	}
	return s
}

func (a *restartTask) Preview(ctx context.Context, raw json.RawMessage) (PreviewResult, error) {
	var p restartTaskParams
	if err := json.Unmarshal(raw, &p); err != nil || p.Connector == "" || p.Task < 0 {
		return PreviewResult{}, previewErr(a.Name(), errors.New("connector and non-negative task are required"))
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	st, err := a.d.Connect.Status(ctx, p.Connector)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	cur := fmt.Sprintf("task %d is not listed in the status (GET /connectors/%s/status)", p.Task, p.Connector)
	for _, t := range st.Tasks {
		if t.ID == p.Task {
			worker := t.WorkerID
			if worker == "" {
				worker = st.WorkerID
			}
			cur = fmt.Sprintf("%s on %s (GET /connectors/%s/status)", orUnknown(t.State), orUnknown(worker), p.Connector)
			break
		}
	}
	return PreviewResult{
		Target:     fmt.Sprintf("connector %s task %d", p.Connector, p.Task),
		Current:    cur,
		Expected:   "RUNNING within ~20 s; the task fails again if its cause is still there",
		Reversible: false,
	}, nil
}

func (a *restartConnector) Preview(ctx context.Context, raw json.RawMessage) (PreviewResult, error) {
	var p restartConnectorParams
	if err := json.Unmarshal(raw, &p); err != nil || p.Connector == "" {
		return PreviewResult{}, previewErr(a.Name(), errors.New("connector is required"))
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	st, err := a.d.Connect.Status(ctx, p.Connector)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	desc, _, _ := connectorAndTasks(st)
	if desc == "RUNNING" {
		desc = "connector and all tasks RUNNING"
	}
	return PreviewResult{
		Target:     "connector " + p.Connector,
		Current:    fmt.Sprintf("%s (GET /connectors/%s/status)", desc, p.Connector),
		Expected:   "connector and its failed tasks RUNNING",
		Reversible: false,
	}, nil
}

func (a *resumeConnector) Preview(ctx context.Context, raw json.RawMessage) (PreviewResult, error) {
	var p resumeConnectorParams
	if err := json.Unmarshal(raw, &p); err != nil || p.Connector == "" {
		return PreviewResult{}, previewErr(a.Name(), errors.New("connector is required"))
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	st, err := a.d.Connect.Status(ctx, p.Connector)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	return PreviewResult{
		Target:     "connector " + p.Connector,
		Current:    fmt.Sprintf("%s (GET /connectors/%s/status)", orUnknown(st.State), p.Connector),
		Expected:   "RUNNING (tasks assigned)",
		Reversible: false, // re-pausing is its own proposal (connect_pause_connector), not offered as an undo
	}, nil
}

func (a *updateConnectorConfig) Preview(ctx context.Context, raw json.RawMessage) (PreviewResult, error) {
	set, err := a.parseParams(raw)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	if err := ctxConnectorScope(ctx, set.connector); err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cfg, err := a.d.Connect.Config(ctx, set.connector)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	keys := set.keys()
	todo, same := set.plan(cfg)
	noun := "key"
	if len(keys) > 1 {
		noun = "keys"
	}
	cur := make([]string, 0, len(keys))
	for _, k := range keys {
		cur = append(cur, fmt.Sprintf("%s = %s", k, configText(cfg, k)))
	}
	res := PreviewResult{
		Target:  fmt.Sprintf("connector %s %s %s", set.connector, noun, strings.Join(keys, ", ")),
		Current: fmt.Sprintf("%s (GET /connectors/%s/config)", strings.Join(cur, "; "), set.connector),
	}
	sameText := make([]string, 0, len(same))
	for _, k := range same {
		sameText = append(sameText, fmt.Sprintf("%s is already %s", k, set.changes[k]))
	}
	if len(todo) == 0 {
		// Execute writes nothing then; the preview must not promise a change.
		res.Expected = fmt.Sprintf("no change: %s; no task restart", strings.Join(sameText, ", "))
		return res, nil
	}
	todoKeys := sortedStringKeys(todo)
	exp := make([]string, 0, len(todoKeys))
	for _, k := range todoKeys {
		exp = append(exp, fmt.Sprintf("%s = %s", k, todo[k]))
	}
	res.Expected = strings.Join(exp, "; ") + "; Connect restarts the tasks"
	if len(sameText) > 0 {
		res.Expected += " (unchanged: " + strings.Join(sameText, ", ") + ")"
	}
	// Undo needs, for every key written, a stored value the allow-list would
	// accept again; an unset key (or a value we truncated or do not offer)
	// cannot be restored by an action, and a partial undo is not offered.
	prev := make(map[string]string, len(todoKeys))
	for _, k := range todoKeys {
		v, set := cfg[k]
		if !set || !a.allowList()[k][v] {
			prev = nil
			break
		}
		prev[k] = v
	}
	if prev != nil {
		res.Reversible = true
		if set.legacy {
			k := todoKeys[0]
			res.Rollback = rollbackText(a.Name(), map[string]string{"connector": set.connector, "key": k, "value": prev[k]})
		} else {
			b, _ := json.Marshal(map[string]any{"connector": set.connector, "changes": prev}) // strings only: cannot fail
			res.Rollback = a.Name() + " " + string(b)
		}
	}
	res.DryRun = a.dryRun(ctx, cfg, todo)
	return res, nil
}

// dryRun sends the connector's config with the changed keys merged in to
// Connect's validate endpoint. It never reads or returns a value other than
// through Connect's error messages, and it sends nothing when the config holds
// a credential-looking key: those values would leave this process for a call
// that exists only for the preview.
func (a *updateConnectorConfig) dryRun(ctx context.Context, cfg, changes map[string]string) *DryRunResult {
	for k := range cfg {
		if LooksSecret(k) {
			return &DryRunResult{Source: sourceValidate, Skipped: "the connector config holds secret keys; not sent to validate"}
		}
	}
	v, ok := a.d.Connect.(configValidator)
	if !ok {
		return &DryRunResult{Source: sourceValidate, Skipped: noDryRun}
	}
	class := cfg["connector.class"]
	if class == "" {
		return &DryRunResult{Source: sourceValidate, Skipped: "the connector config has no connector.class"}
	}
	next := make(map[string]string, len(cfg)+len(changes))
	for k, val := range cfg {
		next[k] = val
	}
	for k, val := range changes {
		next[k] = val
	}
	vr, err := v.ValidateConfig(ctx, class, next)
	if err != nil {
		return &DryRunResult{Source: sourceValidate, Skipped: dryRunText("validate failed: " + err.Error())}
	}
	return &DryRunResult{OK: vr.ErrorCount == 0, Source: sourceValidate, Text: validateText(vr)}
}

// validateText renders "0 errors" or "2 errors: key: msg; key2: msg".
func validateText(vr connect.ValidateResult) string {
	if vr.ErrorCount == 0 {
		return "0 errors"
	}
	keys := make([]string, 0, len(vr.Errors))
	for k := range vr.Errors {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, k+": "+strings.Join(vr.Errors[k], ", "))
	}
	s := fmt.Sprintf("%d errors", vr.ErrorCount)
	if vr.ErrorCount == 1 {
		s = "1 error"
	}
	if len(parts) > 0 {
		s += ": " + strings.Join(parts, "; ")
	}
	return dryRunText(s)
}

func (a *setCompatibility) Preview(ctx context.Context, raw json.RawMessage) (PreviewResult, error) {
	var p setCompatibilityParams
	if err := json.Unmarshal(raw, &p); err != nil || p.Subject == "" || !compatLevels[p.Level] {
		return PreviewResult{}, previewErr(a.Name(), errors.New("subject and a valid level are required"))
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	level, subjectLevel, err := a.d.Registry.Compatibility(ctx, p.Subject)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	res := PreviewResult{
		Target:   "subject " + p.Subject,
		Current:  fmt.Sprintf("%s (GET /config/%s)", levelText(level, subjectLevel), p.Subject),
		Expected: p.Level,
	}
	if subjectLevel {
		res.Reversible = true
		res.Rollback = rollbackText(a.Name(), map[string]string{"subject": p.Subject, "level": level})
	} else {
		res.Rollback = "delete the subject level (no action exists; needs a lead)"
	}
	return res, nil
}

func (a *registerSchema) Preview(ctx context.Context, raw json.RawMessage) (PreviewResult, error) {
	var p registerSchemaParams
	if err := json.Unmarshal(raw, &p); err != nil || p.Subject == "" || p.Schema == "" {
		return PreviewResult{}, previewErr(a.Name(), errors.New("subject and schema are required"))
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	sv, err := a.d.Registry.Schema(ctx, p.Subject, -1)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	res := PreviewResult{
		Target:     "subject " + p.Subject,
		Current:    fmt.Sprintf("latest version %d (id %d)", sv.Version, sv.ID),
		Expected:   fmt.Sprintf("a new version %d", sv.Version+1),
		Reversible: false, // soft-delete is not an action
	}
	res.DryRun = a.dryRun(ctx, p)
	return res, nil
}

func (a *registerSchema) dryRun(ctx context.Context, p registerSchemaParams) *DryRunResult {
	c, ok := a.d.Registry.(compatibilityChecker)
	if !ok {
		return &DryRunResult{Source: sourceCompat, Skipped: noDryRun}
	}
	compatible, msgs, err := c.CheckCompatibility(ctx, p.Subject, p.Schema)
	if err != nil {
		return &DryRunResult{Source: sourceCompat, Skipped: dryRunText("compatibility check failed: " + err.Error())}
	}
	text := fmt.Sprintf("compatible: %t", compatible)
	switch {
	case len(msgs) > 0:
		text += "; " + strings.Join(msgs, "; ")
	case !compatible:
		text += " (no detail from this Registry version)"
	}
	return &DryRunResult{OK: compatible, Source: sourceCompat, Text: dryRunText(text)}
}
