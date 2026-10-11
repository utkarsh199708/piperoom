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
)

// MaxSummaryBytes caps Result.Summary. The summary is what the model reads,
// so it must stay small; put bulky detail in Result.Data for the UI.
const MaxSummaryBytes = 8000

// ErrBadInput wraps input validation failures. The agent loop reports these
// back to the model as tool errors instead of failing the investigation.
var ErrBadInput = errors.New("bad tool input")

// Kind says what a tool is allowed to touch.
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
}

// Truncate cuts s to MaxSummaryBytes and reports whether it cut anything.
func Truncate(s string) (string, bool) {
	if len(s) <= MaxSummaryBytes {
		return s, false
	}
	const marker = "\n…[truncated]"
	cut := MaxSummaryBytes - len(marker)
	// Do not split a UTF-8 rune.
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return s[:cut] + marker, true
}

// Registry holds tools in registration order. The order is stable so the
// tool list sent to the model stays byte-identical (prompt caching).
type Registry struct {
	mu    sync.RWMutex
	byKey map[string]Tool
	order []Tool
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{byKey: map[string]Tool{}}
}

// Register adds t. It fails on an empty or duplicate name.
func (r *Registry) Register(t Tool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	name := t.Name()
	if name == "" {
		return errors.New("tools: empty tool name")
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
