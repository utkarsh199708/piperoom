package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// ---- connect_pause_connector ----

// pauseConnector stops the bleeding while people decide: the connector and
// its tasks stop, nothing is dropped, and connect_resume_connector undoes it.
type pauseConnector struct {
	d Deps
	// pollInterval overrides resumePollInterval in tests; zero means the default.
	pollInterval time.Duration
}

var _ Scoped = (*pauseConnector)(nil)

func (a *pauseConnector) Name() string  { return "connect_pause_connector" }
func (a *pauseConnector) Layer() string { return "kafka-connect" }
func (a *pauseConnector) Describe() string {
	return "Pauses a Kafka Connect connector and its tasks (PUT /connectors/{name}/pause) to stop the damage while people decide. No data is lost: a source connector resumes from its committed offsets (a Postgres replication slot keeps the WAL meanwhile, so it grows), a sink resumes from its consumer group's offsets. Consumers see lag grow while it is paused. Undo with connect_resume_connector."
}
func (a *pauseConnector) ParamsSchema() map[string]any {
	return objectSchema(map[string]any{"connector": strProp("connector name")}, "connector")
}

type pauseConnectorParams struct {
	Connector string `json:"connector"`
}

// maxConnectorNameLen caps a connector name before any I/O or echo.
const maxConnectorNameLen = 249

func (a *pauseConnector) parseParams(raw json.RawMessage) (pauseConnectorParams, error) {
	var p pauseConnectorParams
	if err := json.Unmarshal(raw, &p); err != nil || p.Connector == "" {
		return p, errors.New("actions: connector is required")
	}
	if len(p.Connector) > maxConnectorNameLen {
		return p, fmt.Errorf("actions: connector name is longer than %d bytes", maxConnectorNameLen)
	}
	return p, nil
}

// Validate refuses a connector that does not exist or whose status cannot be
// read. An already PAUSED connector is not refused: Execute then writes
// nothing and says so, as the config action does for a value already set. It
// never needs a write client, so the agent can propose with write mode off.
func (a *pauseConnector) Validate(ctx context.Context, raw json.RawMessage) error {
	p, err := a.parseParams(raw)
	if err != nil {
		return err
	}
	if err := ctxConnectorScope(ctx, p.Connector); err != nil {
		return err
	}
	ctx, cancel := a.d.validateContext(ctx)
	defer cancel()
	if err := a.d.connectorExists(ctx, p.Connector); err != nil {
		return err
	}
	if _, err := a.d.Connect.Status(ctx, p.Connector); err != nil {
		return fmt.Errorf("actions: pause %s: read status: %w", p.Connector, err)
	}
	return nil
}

func (a *pauseConnector) CheckScope(p owners.Pipeline, raw json.RawMessage) error {
	var params pauseConnectorParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return fmt.Errorf("actions: bad params: %w", err)
	}
	return inScopeConnector(p, params.Connector)
}

// Preview reads the status only (GET /connectors/{name}/status).
func (a *pauseConnector) Preview(ctx context.Context, raw json.RawMessage) (PreviewResult, error) {
	p, err := a.parseParams(raw)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	if err := ctxConnectorScope(ctx, p.Connector); err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	st, err := a.d.Connect.Status(ctx, p.Connector)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	if st.State == "PAUSED" {
		// Execute writes nothing then; the preview must not promise a change.
		return PreviewResult{
			Target:   "connector " + p.Connector,
			Current:  fmt.Sprintf("PAUSED (GET /connectors/%s/status)", p.Connector),
			Expected: "no change: the connector is already PAUSED",
		}, nil
	}
	desc, n, _ := connectorAndTasks(st)
	if desc == "RUNNING" {
		desc = fmt.Sprintf("connector and %d tasks RUNNING", n)
	}
	expected := fmt.Sprintf("PAUSED: the connector and its %d tasks stop; consumers see lag grow while it is paused; no data is lost (it resumes from its committed offsets)", n)
	if st.Type == "source" {
		expected += "; the source keeps the change log meanwhile (a Postgres replication slot retains WAL, so disk use grows)"
	}
	return PreviewResult{
		Target:     "connector " + p.Connector,
		Current:    fmt.Sprintf("%s (GET /connectors/%s/status)", desc, p.Connector),
		Expected:   expected,
		Reversible: true,
		Rollback:   rollbackText("connect_resume_connector", map[string]string{"connector": p.Connector}),
	}, nil
}

func (a *pauseConnector) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, err := a.parseParams(raw)
	if err != nil {
		return "", err
	}
	if err := ctxConnectorScope(ctx, p.Connector); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// Asserted here only: with write mode off the client is read-only, and the
	// agent must still be able to propose (Validate) and preview.
	fw, ok := a.d.Connect.(clients.ConnectFixWriter)
	if !ok {
		return "", fmt.Errorf("actions: pause %s: connect client does not support PauseConnector (clients.ConnectFixWriter); is write mode on?", p.Connector)
	}
	// A pause of a PAUSED connector would change nothing; say so instead.
	// A failed read is not a reason to skip the write.
	if st, err := a.d.Connect.Status(ctx, p.Connector); err == nil && st.State == "PAUSED" {
		return fmt.Sprintf("%s is already PAUSED; no change made", p.Connector), nil
	}
	if err := fw.PauseConnector(ctx, p.Connector); err != nil {
		return "", fmt.Errorf("actions: pause %s: %w", p.Connector, err)
	}
	// Connect applies a pause asynchronously. Once the call has succeeded the
	// write is done, so every later problem is a non-error result: an error
	// would tell the agent the pause failed and invite a retry.
	state, paused, err := awaitState(ctx, pollEvery(a.pollInterval), "PAUSED", func(ctx context.Context) (string, error) {
		st, err := a.d.Connect.Status(ctx, p.Connector)
		if err != nil {
			return "", err
		}
		return st.State, nil
	})
	switch {
	case err != nil:
		return fmt.Sprintf("pause accepted for %s; status read failed: %v", p.Connector, err), nil
	case paused:
		return fmt.Sprintf("paused %s (PAUSED); undo with connect_resume_connector", p.Connector), nil
	}
	return fmt.Sprintf("pause accepted for %s; state now %s (Connect applies a pause asynchronously)", p.Connector, orUnknown(state)), nil
}
