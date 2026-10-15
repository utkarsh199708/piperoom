// Package changes is the read-only tool what_changed: it lists what changed on
// the room's pipeline shortly before the alert (DDL on the source, connector
// config and state changes, schema versions registered or deleted), from the
// change log the watcher fills plus a live read of the DDL audit table.
//
// Everything it returns came from customer systems and was stored by Piperoom;
// it is untrusted text for the model like any other tool output, and the agent
// loop wraps it as such. Credential values were masked when the rows were
// written (clients.PipelineChange), and summaries are cut and cleaned again
// here, so a hostile DDL statement cannot add result lines.
package changes

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// ToolName is the name the model calls.
const ToolName = "what_changed"

// Limits of one answer.
const (
	// DefaultWindow is how far back the tool looks when window is not given.
	DefaultWindow = 6 * time.Hour
	// MinWindow is the shortest window the tool accepts.
	MinWindow = time.Second
	// MaxWindow is the longest window the tool accepts.
	MaxWindow = 7 * 24 * time.Hour
	// callTimeout bounds each read, as for every tool.
	callTimeout = 10 * time.Second

	listLimit    = 200 // rows read from the change log and from the audit table
	maxShown     = 40  // lines in the summary and entries in Data.changes
	maxLineRunes = 300 // one summary line, after the timestamp and object
)

// Kinds lists every change kind the log can hold, for the input schema and the
// "recorded kinds" note.
var Kinds = []string{clients.ChangeDDL, clients.ChangeConnectorConfig, clients.ChangeConnectorState, clients.ChangeSchemaVersion}

// Deps are the tool's dependencies.
type Deps struct {
	// Changes is the change log. Nil or failing is allowed: the tool then
	// answers from the live DDL read alone and says "change log unavailable".
	Changes clients.ChangeReader
	// Src is the source Postgres reader for the live DDL read; nil skips it.
	Src clients.SourceReader
	// Owners resolves the room's pipeline (tools.ScopeFor).
	Owners *owners.Owners
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// Now defaults to time.Now.
	Now func() time.Time
}

// Tool is what_changed. It is safe for concurrent use.
type Tool struct{ d Deps }

var _ tools.Tool = (*Tool)(nil)

// New returns the tool.
func New(d Deps) *Tool {
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	return &Tool{d: d}
}

// Register adds what_changed to reg.
func Register(reg *tools.Registry, d Deps) error {
	if err := reg.Register(New(d)); err != nil {
		return fmt.Errorf("changes: register: %w", err)
	}
	return nil
}

// Name implements tools.Tool.
func (t *Tool) Name() string { return ToolName }

// Kind implements tools.Tool: the tool only reads.
func (t *Tool) Kind() tools.Kind { return tools.KindRead }

// Description implements tools.Tool.
func (t *Tool) Description() string {
	return "Lists what changed on this pipeline recently, newest first: DDL on the source tables (with the user), " +
		"connector config key changes (credential values masked), connector and task state transitions, and " +
		"Schema Registry versions registered or deleted. Use it as your second read: a change just before the " +
		"alert is a suspect to verify with the other tools, not proof of the cause. Config changes are noticed " +
		"when the watcher polls, so their time is the poll time, not the edit time."
}

// InputSchema implements tools.Tool. Limits are enforced in Run (strict tool
// use rejects bounds).
func (t *Tool) InputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"window": map[string]any{"type": "string", "description": "How far back, like \"6h\" or \"2d\". Default 6h, from 1s to 7d."},
			"kinds": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string", "enum": Kinds},
				"description": "Only these kinds. Default: all.",
			},
			"object": map[string]any{"type": "string", "description": "Only this \"schema.table\", connector or schema subject of the pipeline."},
		},
		"additionalProperties": false,
	}
}

// Data is Result.Data.
type Data struct {
	Window  string                   `json:"window"`
	Changes []clients.PipelineChange `json:"changes"`
	// Omitted counts changes found but not listed in Changes.
	Omitted int `json:"omitted"`
}

// parseWindow reads "6h", "90m", "2d"; empty is DefaultWindow.
func parseWindow(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return DefaultWindow, nil
	}
	var d time.Duration
	if days, ok := strings.CutSuffix(s, "d"); ok {
		var n int
		if _, err := fmt.Sscanf(days, "%d", &n); err != nil || fmt.Sprint(n) != days {
			return 0, fmt.Errorf("%w: window %q must look like 6h, 90m or 2d", tools.ErrBadInput, s)
		}
		if n > int(MaxWindow/(24*time.Hour)) {
			return 0, fmt.Errorf("%w: window %q is longer than 7d", tools.ErrBadInput, s)
		}
		// Refuse before multiplying: a large negative count wraps the Duration,
		// and some counts wrap it to a small positive one that passes the range
		// check below.
		if n < 1 {
			return 0, fmt.Errorf("%w: window %q must be between 1s and 7d", tools.ErrBadInput, s)
		}
		d = time.Duration(n) * 24 * time.Hour
	} else {
		var err error
		if d, err = time.ParseDuration(s); err != nil {
			return 0, fmt.Errorf("%w: window %q must look like 6h, 90m or 2d", tools.ErrBadInput, s)
		}
	}
	if d < MinWindow || d > MaxWindow {
		return 0, fmt.Errorf("%w: window %q must be between 1s and 7d", tools.ErrBadInput, s)
	}
	return d, nil
}

// Run implements tools.Tool.
func (t *Tool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	if err := ctx.Err(); err != nil {
		return tools.Result{}, err
	}
	var p struct {
		Window string   `json:"window"`
		Kinds  []string `json:"kinds"`
		Object string   `json:"object"`
	}
	if len(bytes.TrimSpace(input)) == 0 {
		input = json.RawMessage(`{}`)
	}
	if !utf8.Valid(input) {
		return tools.Result{}, fmt.Errorf("%w: input is not valid UTF-8", tools.ErrBadInput)
	}
	dec := json.NewDecoder(bytes.NewReader(input))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return tools.Result{}, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	window, err := parseWindow(p.Window)
	if err != nil {
		return tools.Result{}, err
	}
	for _, k := range p.Kinds {
		if !slices.Contains(Kinds, k) {
			return tools.Result{}, fmt.Errorf("%w: unknown kind %q (kinds: %s)", tools.ErrBadInput, k, strings.Join(Kinds, ", "))
		}
	}
	if len(p.Object) > 200 || strings.ContainsFunc(p.Object, unicode.IsControl) {
		return tools.Result{}, fmt.Errorf("%w: object is not a table, connector or subject name", tools.ErrBadInput)
	}

	scope, err := tools.ScopeFor(ctx, t.d.Owners)
	if err != nil {
		return tools.Result{}, err
	}
	pipeline := scope.Pipeline().ID
	if scope.Unscoped() || pipeline == "" {
		return tools.Result{}, fmt.Errorf("%w: what_changed reads the room's own pipeline only", tools.ErrNoScope)
	}
	if p.Object != "" && !objectInScope(scope, p.Object) {
		return tools.Result{}, fmt.Errorf("%w: %w: object %q is not a source table, connector or subject of pipeline %s", tools.ErrBadInput, tools.ErrOutOfScope, p.Object, pipeline)
	}

	now := t.d.Now()
	since := now.Add(-window)
	var notes []string
	var all []clients.PipelineChange

	logOK := false
	if t.d.Changes != nil {
		cctx, cancel := context.WithTimeout(ctx, callTimeout)
		rows, err := t.d.Changes.ListChanges(cctx, pipeline, since, now, p.Kinds, listLimit)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return tools.Result{}, ctx.Err()
			}
			t.d.Logger.Warn("what_changed: change log read failed", "pipeline", pipeline, "err", err)
		} else {
			logOK, all = true, rows
		}
	}
	if !logOK {
		notes = append(notes, "change log unavailable")
	}

	liveOK := false
	if t.d.Src != nil && (len(p.Kinds) == 0 || slices.Contains(p.Kinds, clients.ChangeDDL)) {
		cctx, cancel := context.WithTimeout(ctx, callTimeout)
		rows, err := t.d.Src.RecentDDL(cctx, since, listLimit)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return tools.Result{}, ctx.Err()
			}
			t.d.Logger.Warn("what_changed: live DDL read failed", "pipeline", pipeline, "err", err)
			notes = append(notes, "ddl: live read failed")
		} else {
			liveOK = true
			for _, r := range rows {
				if scope.SourceTableInScope(r.ObjectIdentity) && !r.At.Before(since) {
					all = append(all, ddlChange(pipeline, r))
				}
			}
		}
	}
	if !logOK && !liveOK {
		return tools.Result{}, errors.New("changes: neither the change log nor the live DDL read is available")
	}

	all = mergeAndFilter(all, p.Object, p.Kinds, since, now)
	shown := all[:min(len(all), maxShown)]
	omitted := len(all) - len(shown)

	var sb strings.Builder
	if len(shown) == 0 {
		fmt.Fprintf(&sb, "no recorded change on %s in the last %s (recorded kinds: %s)", pipeline, windowLabel(p.Window, window), strings.Join(Kinds, ", "))
	}
	for i, c := range shown {
		if i > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(line(c))
	}
	if omitted > 0 {
		fmt.Fprintf(&sb, "\n%d more %s not shown", omitted, tools.Plural(omitted, "change", "changes"))
	}
	for _, n := range notes {
		sb.WriteString("\n(" + n + ")")
	}
	s, tr := tools.Truncate(sb.String())
	return tools.Result{
		Summary:   s,
		Data:      Data{Window: windowLabel(p.Window, window), Changes: nonNil(shown), Omitted: omitted},
		Truncated: tr || omitted > 0,
	}, nil
}

func nonNil(c []clients.PipelineChange) []clients.PipelineChange {
	if c == nil {
		return []clients.PipelineChange{}
	}
	return c
}

func windowLabel(given string, d time.Duration) string {
	if g := strings.TrimSpace(given); g != "" {
		return g
	}
	return d.String()
}

// objectInScope reports whether name is a source table, connector (or one of
// its tasks, "name/0") or schema subject of the pipeline.
func objectInScope(sc tools.Scope, name string) bool {
	if sc.CheckSourceTable(name) == nil || sc.CheckConnector(name) == nil || sc.CheckSubject(name) == nil {
		return true
	}
	if conn, _, ok := strings.Cut(name, "/"); ok {
		return sc.CheckConnector(conn) == nil
	}
	return false
}

// objectMatches reports whether a change is about object: the object itself
// or, for a connector, one of its tasks.
func objectMatches(c clients.PipelineChange, object string) bool {
	return c.Object == object || strings.HasPrefix(c.Object, object+"/")
}

// ddlChange converts an audit row to the change form. The fingerprint follows
// the watcher's rule for ddl rows (sha256 of at, object identity, query) so the
// two readings of one statement merge.
func ddlChange(pipeline string, r clients.DDLChange) clients.PipelineChange {
	return clients.PipelineChange{
		PipelineID:  pipeline,
		Kind:        clients.ChangeDDL,
		Object:      r.ObjectIdentity,
		At:          r.At.UTC(),
		Actor:       r.Username,
		Source:      "ddl_audit",
		Fingerprint: ddlFingerprint(r),
		Summary:     cleanLine(r.CommandTag+" "+r.ObjectIdentity+" | "+r.Query, maxLineRunes),
	}
}

func ddlFingerprint(r clients.DDLChange) string {
	h := sha256.Sum256([]byte(r.At.UTC().Format(time.RFC3339Nano) + "|" + r.ObjectIdentity + "|" + r.Query))
	return hex.EncodeToString(h[:])
}

// mergeAndFilter merges the log and the live read, newest first. A ddl row is
// the same change when object and time (to the microsecond, the precision of
// the audit column) agree; the live read wins on text. Other kinds merge by
// (kind, object, fingerprint).
func mergeAndFilter(in []clients.PipelineChange, object string, kinds []string, since, until time.Time) []clients.PipelineChange {
	type key struct {
		kind, object, fp string
		at               int64
	}
	keyOf := func(c clients.PipelineChange) key {
		if c.Kind == clients.ChangeDDL {
			return key{kind: c.Kind, object: c.Object, at: c.At.UnixMicro()}
		}
		return key{kind: c.Kind, object: c.Object, fp: c.Fingerprint}
	}
	byKey := map[key]clients.PipelineChange{}
	var order []key
	for _, c := range in {
		if c.At.Before(since) || c.At.After(until.Add(time.Minute)) {
			continue
		}
		if len(kinds) > 0 && !slices.Contains(kinds, c.Kind) {
			continue
		}
		if object != "" && !objectMatches(c, object) {
			continue
		}
		k := keyOf(c)
		if old, ok := byKey[k]; ok {
			// The live audit row (Source ddl_audit, no id) carries the text.
			if c.ID == 0 && old.ID != 0 {
				c.ID, c.Fingerprint = old.ID, old.Fingerprint
				byKey[k] = c
			}
			continue
		}
		byKey[k] = c
		order = append(order, k)
	}
	out := make([]clients.PipelineChange, 0, len(order))
	for _, k := range order {
		out = append(out, byKey[k])
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].At.After(out[j].At) })
	return out
}

// line renders one change as one summary line. Stored text came from customer
// systems, so it is masked, cleaned and cut again here.
func line(c clients.PipelineChange) string {
	var sb strings.Builder
	sb.WriteString(c.At.UTC().Format(time.RFC3339))
	sb.WriteByte(' ')
	sb.WriteString(cleanLine(c.Kind, 40))
	sb.WriteByte(' ')
	sb.WriteString(cleanLine(c.Object, 120))
	if c.Actor != "" {
		sb.WriteString(" by " + cleanLine(c.Actor, 60))
	}
	sb.WriteString(": ")
	sb.WriteString(cleanLine(describe(c), maxLineRunes))
	return sb.String()
}

// describe is the text after the colon: for a state change the transition from
// before/after, otherwise the stored summary.
func describe(c clients.PipelineChange) string {
	if c.Kind == clients.ChangeConnectorState {
		var b, a struct {
			State string `json:"state"`
			Trace string `json:"trace"`
		}
		if json.Unmarshal(c.Before, &b) == nil && json.Unmarshal(c.After, &a) == nil && (b.State != "" || a.State != "") {
			s := b.State + " → " + a.State
			if a.Trace != "" {
				s += " (" + a.Trace + ")"
			}
			return s
		}
	}
	return c.Summary
}

// cleanLine masks secrets, folds whitespace and control characters to single
// spaces and cuts to n runes, so no stored text can start a new line.
func cleanLine(s string, n int) string {
	s, _ = redact.Text(s)
	s = strings.Join(strings.FieldsFunc(s, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }), " ")
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n]) + "…"
}
