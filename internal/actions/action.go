// Package actions holds write operations on customer systems. An action runs
// only as a step of a proposal that every required layer owner approved.
// The agent can propose actions but never runs them.
//
// CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// Action is one write operation.
type Action interface {
	// Name is snake_case and unique, e.g. "connect_restart_task".
	Name() string
	// Layer is the pipeline layer whose owner must approve (see events.Layers).
	Layer() string
	// Describe explains the action for the model and the approval UI.
	Describe() string
	// ParamsSchema is a JSON Schema object for Params.
	ParamsSchema() map[string]any
	// Validate checks params without side effects. Called when a proposal is
	// created and again right before Execute. It may read from the target, so
	// it takes a context: the implementation bounds it with its own deadline
	// and stops when ctx ends.
	Validate(ctx context.Context, params json.RawMessage) error
	// Execute performs the write and returns a short human-readable result.
	Execute(ctx context.Context, params json.RawMessage) (string, error)
}

// Scoped is implemented by every action that writes to a named resource
// (connector, schema subject, table, consumer group). Approvers in a room only
// own and watch that room's pipeline, so a step aimed at another pipeline's
// resource must be refused even when the resource exists (security finding
// F10). The room service calls CheckScope with the room's pipeline after
// Validate, both when a proposal is created and right before Execute; an
// action that does not implement Scoped is refused.
type Scoped interface {
	// CheckScope returns an error when params target anything outside p.
	// It must not do I/O: decide from params and p alone.
	CheckScope(p owners.Pipeline, params json.RawMessage) error
}

// Preview is implemented by an action that can say, without writing, what
// Execute would do to the target: its current state, the expected state after,
// whether it can be undone, and the target system's own dry-run verdict where
// one exists. The room service calls it once per step after proposal.created
// (10 s, read-only, the pipeline scope in ctx) and stores the result on
// proposal.previewed; a Preview error is stored as the step's error and never
// blocks the proposal. Preview must call only *Reader client methods plus the
// documented dry-run endpoints; it never changes a system. (F4)
type Preview interface {
	Preview(ctx context.Context, params json.RawMessage) (PreviewResult, error)
}

// PreviewResult and DryRunResult are the event types, so one definition serves
// the action, the room log and the API.
type (
	PreviewResult = events.PreviewResult
	DryRunResult  = events.DryRunResult
)

// Receipted is implemented by an action whose Execute can hand back a receipt
// of what was done: the setup actions, which forward one typed op to the
// paired runner and get its events.SetupReceipt back. The executor prefers
// ExecuteReceipt over Execute when an action implements it and stores the
// receipt on action.executed (ActionExecutedPayload.Receipt). The string is
// the same short human-readable result Execute returns. A receipt never
// carries a secret value or raw output (docs/design/setup-agent.md §2.2). (U)
type Receipted interface {
	ExecuteReceipt(ctx context.Context, params json.RawMessage) (string, events.SetupReceipt, error)
}

// Tiered is implemented by an action whose approval route depends on where
// it writes (the setup actions and, from U1, the Q actions when they run in a
// setup room). Tier returns the step's tier, protocol.TierT0 to TierT3: the
// op's floor raised to class, the class of the endpoint the room service
// resolved from params (never a label the model supplied). It must not do
// I/O. An action that is not Tiered is T2 in a setup room. The room service
// routes by the result: T0 is never a proposal, T1 is scheduled under
// PolicySetupLocal, T2 and T3 wait for the approver of design §3.6, T3 also
// for a maintenance window where the guard table says so. (U)
type Tiered interface {
	Tier(params json.RawMessage, class string) (string, error)
}

// secretKeySubstrings are the fragments that mark a config key as a
// credential. The rule is shared, not configurable: a runner op, a validator
// and a connector-config action must agree on what is secret.
var secretKeySubstrings = []string{"password", "passwd", "secret", "token", "credential", "jaas", "apikey", "private"}

// LooksSecret reports whether a config key probably names a credential: the
// key contains one of password, passwd, secret, token, credential, jaas,
// apikey or private (any case), or has a bare "key" segment (api.key,
// ssl.key.password, key.converter credentials). It is the one rule the
// connector-config actions, the config validator (Q4a) and the runner's ops
// and artifact validators share, so a value for such a key is refused in
// plain text everywhere and allowed only as a ${secret:name} or
// ${file:...:key} reference. It is a second line of defence behind each
// caller's allow-list, so it errs towards true. (U; A2 switches its private
// copy to it in U1-23)
func LooksSecret(key string) bool {
	k := strings.ToLower(key)
	for _, s := range secretKeySubstrings {
		if strings.Contains(k, s) {
			return true
		}
	}
	for _, seg := range strings.FieldsFunc(k, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	}) {
		if seg == "key" {
			return true
		}
	}
	return false
}

// Registry holds actions in registration order.
type Registry struct {
	mu    sync.RWMutex
	byKey map[string]Action
	order []Action
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{byKey: map[string]Action{}}
}

// Register adds a. It fails on an empty or duplicate name.
func (r *Registry) Register(a Action) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	name := a.Name()
	if name == "" {
		return errors.New("actions: empty action name")
	}
	if _, dup := r.byKey[name]; dup {
		return fmt.Errorf("actions: duplicate action %q", name)
	}
	r.byKey[name] = a
	r.order = append(r.order, a)
	return nil
}

// Get returns the action with the given name.
func (r *Registry) Get(name string) (Action, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.byKey[name]
	return a, ok
}

// All returns every action in registration order.
func (r *Registry) All() []Action {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]Action(nil), r.order...)
}
