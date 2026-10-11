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
	"sync"
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
	// created and again right before Execute.
	Validate(params json.RawMessage) error
	// Execute performs the write and returns a short human-readable result.
	Execute(ctx context.Context, params json.RawMessage) (string, error)
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
