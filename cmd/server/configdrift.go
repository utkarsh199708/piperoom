package main

// Wiring of config_drift (X3b): the room service's action.executed hook moves
// a connector's approved config in the watcher, and POST
// /api/pipelines/{pid}/connectors/{name}/accept-config reaches the watcher
// through the same holder the room-closed hook uses (roomClosedHook), so both
// follow the watcher of the current leadership term.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/watch"
)

// connectorApprover is the part of *watch.Watcher the action hook calls.
type connectorApprover interface {
	ApproveConnectorChangeBy(pipelineID, connector, proposalID string)
}

// configAccepter is the part of *watch.Watcher accept-config calls.
type configAccepter interface {
	AcceptConfig(ctx context.Context, pipelineID, connector string) (oldHash, newHash string, err error)
}

var (
	_ httpapi.ConfigAccepter = (*roomClosedHook)(nil)
	_ connectorApprover      = (*watch.Watcher)(nil)
	_ configAccepter         = (*watch.Watcher)(nil)
)

// stepConnector is the connector a proposal step acts on: its "connector"
// param, the one name every connector action uses; "" when it has none.
func stepConnector(params json.RawMessage) string {
	var p struct {
		Connector string `json:"connector"`
	}
	if json.Unmarshal(params, &p) != nil {
		return ""
	}
	return p.Connector
}

// onActionExecuted matches room.Deps.OnActionExecuted: a step that names a
// connector moves that connector's approved config in the watcher (if this
// replica runs it), and the config version it produces names the proposal
// (X5c). It does no I/O.
func (h *roomClosedHook) onActionExecuted(_ context.Context, pipelineID, proposalID string, step events.ProposalStep) {
	c := stepConnector(step.Params)
	if c == "" {
		return
	}
	h.mu.RLock()
	w := h.watcher
	h.mu.RUnlock()
	if a, ok := w.(connectorApprover); ok {
		a.ApproveConnectorChangeBy(pipelineID, c, proposalID)
	}
}

// AcceptConfig implements httpapi.ConfigAccepter over the current term's
// watcher. No watcher here (mode off, or another replica leads the watcher)
// and a connector the watcher does not look at are httpapi.ErrWatchOff; a
// Connect read failure is httpapi.ErrConnectUnavailable.
func (h *roomClosedHook) AcceptConfig(ctx context.Context, pipelineID, connector string) (string, string, error) {
	h.mu.RLock()
	w := h.watcher
	h.mu.RUnlock()
	a, ok := w.(configAccepter)
	if !ok {
		return "", "", fmt.Errorf("server: accept config: no watcher runs on this replica: %w", httpapi.ErrWatchOff)
	}
	oldHash, newHash, err := a.AcceptConfig(ctx, pipelineID, connector)
	switch {
	case errors.Is(err, watch.ErrNotWatched):
		return "", "", fmt.Errorf("server: accept config: %w: %w", httpapi.ErrWatchOff, err)
	case errors.Is(err, watch.ErrConnectUnavailable):
		return "", "", fmt.Errorf("server: accept config: %w: %w", httpapi.ErrConnectUnavailable, err)
	case err != nil:
		return "", "", fmt.Errorf("server: accept config: %w", err)
	}
	return oldHash, newHash, nil
}
