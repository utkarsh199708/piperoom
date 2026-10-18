// Package tools defines what an investigation tool is and keeps the set of
// tools the agent may call.
//
// CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// MaxSummaryBytes caps Result.Summary. The summary is what the model reads,
// so it must stay small; put bulky detail in Result.Data for the UI.
const MaxSummaryBytes = 8000

// ErrBadInput wraps input validation failures. The agent loop reports these
// back to the model as tool errors instead of failing the investigation.
var ErrBadInput = errors.New("bad tool input")

// ErrNotReadOnly wraps the error Register returns when a read-only registry
// (NewReadOnlyRegistry) is given a tool whose Kind is not KindRead.
var ErrNotReadOnly = errors.New("tool is not read-only")

// Kind says what a tool is allowed to touch. A tool declares itself read-only
// by returning KindRead. The declaration is checked by type as well: a KindRead
// tool's constructor takes only the clients.*Reader interfaces
// (internal/clients/clients.go), so it holds no client method that writes.
// Register refuses any value other than the two below.
type Kind string

const (
	// KindRead tools only read from customer systems. They never change them.
	KindRead Kind = "read"
	// KindRoom tools only write to the incident room log (hypothesis, proposal).
	KindRoom Kind = "room"
)

// Tool is one capability the investigation agent can call.
//
// Rules for every implementation:
//   - Name is snake_case and unique, e.g. "connect_status".
//   - Description says what the tool returns and when to use it. The model reads it.
//   - InputSchema is a JSON Schema object with "type":"object",
//     "additionalProperties": false and a "required" list.
//   - Run must respect ctx (deadline and cancel) and never panic.
//   - Run returns an error wrapping ErrBadInput for bad input; other errors
//     mean the target system failed.
//   - Run never changes customer systems. Write operations are actions
//     (package actions), run only after human approval.
type Tool interface {
	Name() string
	Description() string
	InputSchema() map[string]any
	Kind() Kind
	Run(ctx context.Context, input json.RawMessage) (Result, error)
}

// Result is what a tool returns.
type Result struct {
	// Summary is plain text for the model, at most MaxSummaryBytes.
	Summary string `json:"summary"`
	// Data is structured detail for the UI. It must marshal to JSON.
	Data any `json:"data,omitempty"`
	// Truncated is true when Summary or Data was cut to fit limits.
	Truncated bool `json:"truncated,omitempty"`
	// Provenance lists the reads behind this result (see Call). A tool may
	// set it; the agent also merges the calls recorded under its context
	// (WithRecorder), tool-set calls first, and applies the caps. It is
	// stored on agent.tool_result.provenance, outside Data and its cap.
	Provenance []Call `json:"provenance,omitempty"`
}

// Call kinds for Call.Kind.
const (
	CallSQL      = "sql"      // a SQL statement with $1/? placeholders
	CallHTTP     = "http"     // a Kafka Connect REST request
	CallKafka    = "kafka"    // a kadm/kgo admin or fetch request
	CallRegistry = "registry" // a Schema Registry REST request
	CallFile     = "file"     // a local file read, e.g. dbt artifacts from DBT_ARTIFACTS_DIR
)

// Call is one read a tool made against a customer system: the receipt behind
// the tool's summary. Text is what a person would re-run: SQL with its
// placeholders (never values), "GET /connectors/x/status", "kadm.Lag(group)".
// ParamsRedacted holds the arguments as "$1=value" / "name=value" after the
// client's redaction; a record key or value is never in it. Target names the
// system (host, database, brokers), never credentials.
type Call struct {
	Kind           string    `json:"kind"`
	Target         string    `json:"target"`
	Text           string    `json:"text"`
	ParamsRedacted []string  `json:"params_redacted,omitempty"`
	At             time.Time `json:"at"`
	TookMS         int64     `json:"took_ms"`
}

// Provenance caps, applied by the agent before a call reaches the room log.
const (
	MaxProvenanceCalls = 16   // calls per Result; the first 16 are kept
	MaxCallTextBytes   = 2048 // Call.Text; cut with Truncate's marker on a rune boundary
	MaxCallParamBytes  = 200  // each ParamsRedacted item
)

// Recorder collects the Calls made under a context. A client records with
// Record(ctx, call); a caller that wants receipts installs one with
// WithRecorder and reads Calls after the tool ran. Safe for concurrent use.
type Recorder struct {
	mu    sync.Mutex
	calls []Call
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder { return &Recorder{} }

// Record appends c. A nil Recorder ignores it.
func (r *Recorder) Record(c Call) {
	if r == nil {
		return
	}
	c.ParamsRedacted = append([]string(nil), c.ParamsRedacted...)
	r.mu.Lock()
	r.calls = append(r.calls, c)
	r.mu.Unlock()
}

// Calls returns a copy of the recorded calls, in call order.
func (r *Recorder) Calls() []Call {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Call, len(r.calls))
	for i, c := range r.calls {
		c.ParamsRedacted = append([]string(nil), c.ParamsRedacted...)
		out[i] = c
	}
	return out
}

type recorderKey struct{}

// WithRecorder returns ctx carrying r. RecorderFrom returns it.
func WithRecorder(ctx context.Context, r *Recorder) context.Context {
	return context.WithValue(ctx, recorderKey{}, r)
}

// RecorderFrom returns the Recorder installed by WithRecorder, if any; a nil
// Recorder counts as none.
func RecorderFrom(ctx context.Context) (*Recorder, bool) {
	r, ok := ctx.Value(recorderKey{}).(*Recorder)
	return r, ok && r != nil
}

// Record adds c to the recorder in ctx; without one it does nothing. Clients
// call it after every request, on success and on error. It never blocks.
func Record(ctx context.Context, c Call) {
	if r, ok := RecorderFrom(ctx); ok {
		r.Record(c)
	}
}

// CapProvenance applies the caps above and reports whether anything was cut.
// It returns a new slice; calls is not modified.
func CapProvenance(calls []Call) ([]Call, bool) {
	if len(calls) == 0 {
		return nil, false
	}
	cut := false
	n := len(calls)
	if n > MaxProvenanceCalls {
		n, cut = MaxProvenanceCalls, true
	}
	out := make([]Call, n)
	for i, c := range calls[:n] {
		var c1 bool
		c.Text, c1 = truncateTo(c.Text, MaxCallTextBytes)
		cut = cut || c1
		if len(c.ParamsRedacted) > 0 {
			ps := make([]string, len(c.ParamsRedacted))
			for j, p := range c.ParamsRedacted {
				var c2 bool
				ps[j], c2 = truncateTo(p, MaxCallParamBytes)
				cut = cut || c2
			}
			c.ParamsRedacted = ps
		}
		out[i] = c
	}
	return out, cut
}

// Truncate cuts s to MaxSummaryBytes and reports whether it cut anything.
func Truncate(s string) (string, bool) {
	return truncateTo(s, MaxSummaryBytes)
}

// truncateMarker ends every string cut by Truncate or CapProvenance.
const truncateMarker = "\n…[truncated]"

// truncateTo cuts s to at most limit bytes, marker included, on a rune boundary.
func truncateTo(s string, limit int) (string, bool) {
	if len(s) <= limit {
		return s, false
	}
	cut := limit - len(truncateMarker)
	if cut < 0 {
		cut = 0
	}
	// Do not split a UTF-8 rune.
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return s[:cut] + truncateMarker, true
}

// Registry holds tools in registration order. The order is stable so the
// tool list sent to the model stays byte-identical (prompt caching).
type Registry struct {
	mu       sync.RWMutex
	byKey    map[string]Tool
	order    []Tool
	readOnly bool
}

// NewRegistry returns an empty registry that takes KindRead and KindRoom tools.
func NewRegistry() *Registry {
	return &Registry{byKey: map[string]Tool{}}
}

// NewReadOnlyRegistry returns an empty registry that takes KindRead tools
// only; Register refuses anything else with an error wrapping ErrNotReadOnly.
// agent.Deps.Tools (the tools from A2, A3 and the other tool packages) must be
// built with it. The agent's own KindRoom tools (hypothesis, message,
// propose_fix) are added by the agent package outside this registry.
func NewReadOnlyRegistry() *Registry {
	return &Registry{byKey: map[string]Tool{}, readOnly: true}
}

// ReadOnly reports whether r was made by NewReadOnlyRegistry.
func (r *Registry) ReadOnly() bool { return r.readOnly }

// Register adds t. It fails on an empty or duplicate name; in a read-only
// registry on any Kind other than KindRead (wrapping ErrNotReadOnly, unknown
// kinds included); elsewhere on a Kind other than KindRead or KindRoom. Kind
// is read once, here.
func (r *Registry) Register(t Tool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	name := t.Name()
	if name == "" {
		return errors.New("tools: empty tool name")
	}
	switch k := t.Kind(); {
	case r.readOnly && k != KindRead:
		return fmt.Errorf("tools: tool %q has kind %q in a read-only registry: %w", name, k, ErrNotReadOnly)
	case k != KindRead && k != KindRoom:
		return fmt.Errorf("tools: tool %q has unknown kind %q", name, k)
	}
	if _, dup := r.byKey[name]; dup {
		return fmt.Errorf("tools: duplicate tool %q", name)
	}
	r.byKey[name] = t
	r.order = append(r.order, t)
	return nil
}

// Get returns the tool with the given name.
func (r *Registry) Get(name string) (Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.byKey[name]
	return t, ok
}

// All returns every tool in registration order.
func (r *Registry) All() []Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]Tool(nil), r.order...)
}
