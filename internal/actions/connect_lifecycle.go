package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/mysql"
)

// Q5b and Q5c: the Kafka Connect lifecycle actions. Wiring registers them in
// both write modes (Q5d, P0 ruling #606); with writes off they hold a
// read-only Connect value, so Execute refuses before any I/O:
//
//   - connect_stop_connector stops a connector and its tasks (KIP-875);
//   - connect_reset_source_offsets clears a stopped source connector's stored
//     offsets or replaces one partition's position (KIP-875);
//   - connect_create_connector registers a connector the pipeline's
//     owners.yaml already names, after Connect's own config validation;
//   - connect_delete_connector deletes a connector whose stored config can be
//     given back (no plaintext credential), leaving its offsets under the name.
//
// Every action is an ordinary proposal step: Validate and Preview read only,
// Execute writes through connect.ConnectLifecycleWriter after the approvals.
const (
	// StopConnectorName is the registered name of connect_stop_connector.
	StopConnectorName = "connect_stop_connector"
	// ResetSourceOffsetsName is the registered name of
	// connect_reset_source_offsets.
	ResetSourceOffsetsName = "connect_reset_source_offsets"
	// CreateConnectorName is the registered name of
	// connect_create_connector.
	CreateConnectorName = "connect_create_connector"
	// DeleteConnectorName is the registered name of
	// connect_delete_connector.
	DeleteConnectorName = "connect_delete_connector"
)

// ErrNoLifecycleWriter is the Execute refusal of the four lifecycle actions
// when their Connect client is not a connect.ConnectLifecycleWriter: write
// mode is off (wiring hands them the read-only client), so the step can be
// proposed and previewed but not run. It is returned before any I/O and
// worded like the source actions' refusal.
var ErrNoLifecycleWriter = errors.New("no write credentials: the Connect lifecycle writer is not configured (PIPEROOM_WRITE_MODE=on is needed)")

// LifecycleDeps is what the Q5b/Q5c actions read and write through. Connect
// must be the whole client when write mode is on: the actions assert for
// connect.ConnectLifecycleWriter and connect.SourceOffsetReader at use time,
// like the other optional writer interfaces.
type LifecycleDeps struct {
	Connect clients.ConnectClient
	// Source is optional and read-only. Only connect_reset_source_offsets'
	// Preview uses it, through the tableRowEstimator assertion (the Postgres
	// client's planner estimate), to say how large a following snapshot is.
	Source clients.SourceReader
	// Binlog is optional and read-only: the MySQL source's binlog state, which
	// connect_reset_source_offsets checks a MySQL position against (Q8e). Nil
	// falls back to Source when that is a mysql.BinlogReader (the MySQL
	// client); with neither, a MySQL position is refused (clients.ErrNotSupported).
	Binlog mysql.BinlogReader
	// AllowedHosts are operator-configured endpoints (host[:port] or a
	// comma-separated broker list) a created connector may point at besides
	// the pipeline's connections and registered connectors: the Kafka
	// bootstrap as Connect's workers reach it, say. Empty adds none.
	AllowedHosts []string
	// Timeout overrides the 10 s bound of every call; tests only.
	Timeout time.Duration
	// PollInterval overrides the wait between stop status reads; tests only.
	PollInterval time.Duration
}

// RegisterLifecycle adds the four Q5b/Q5c actions to reg. Wiring calls it in
// both write modes (Q5d); with writes off d.Connect has no
// connect.ConnectLifecycleWriter, so the agent can propose and preview a step
// and Execute refuses it before any I/O.
func RegisterLifecycle(reg *Registry, d LifecycleDeps) error {
	for _, a := range []Action{
		&stopConnector{d: d},
		&resetSourceOffsets{d: d},
		&createConnector{d: d},
		&deleteConnector{d: d},
	} {
		if err := reg.Register(a); err != nil {
			return err
		}
	}
	return nil
}

// timeout is the per-call deadline in force.
func (d LifecycleDeps) timeout() time.Duration {
	if d.Timeout > 0 {
		return d.Timeout
	}
	return timeout
}

// ctx bounds one call with the action's deadline.
func (d LifecycleDeps) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d.timeout())
}

// writeCtx bounds an Execute write. Creating or deleting a connector can wait
// on a Connect worker rebalance, so it gets more than the 10 s read bound but
// stays under the room executor's 30 s step timeout.
func (d LifecycleDeps) writeCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	t := 25 * time.Second
	if d.Timeout > 0 {
		t = d.Timeout
	}
	return context.WithTimeout(ctx, t)
}

// poll is how often Execute re-reads a status while it waits.
func (d LifecycleDeps) poll() time.Duration { return pollEvery(d.PollInterval) }

// connectorExists checks, read-only, that Connect lists name.
func (d LifecycleDeps) connectorExists(ctx context.Context, name string) error {
	present, err := d.listed(ctx, name)
	if err != nil {
		return err
	}
	if present {
		return nil
	}
	return fmt.Errorf("actions: connector %q not found", name)
}

// listed reports, read-only, whether Connect lists name.
func (d LifecycleDeps) listed(ctx context.Context, name string) (bool, error) {
	if d.Connect == nil {
		return false, fmt.Errorf("actions: no Connect client")
	}
	names, err := d.Connect.ListConnectors(ctx)
	if err != nil {
		return false, fmt.Errorf("actions: list connectors: %w", err)
	}
	for _, n := range names {
		if n == name {
			return true, nil
		}
	}
	return false, nil
}

// isTimeoutErr reports whether err is a request that ran out of time. Connect
// can hold a create or delete through a worker rebalance longer than the HTTP
// client's 10 s bound; the caller then checks whether the write landed and
// retries when it did not.
func isTimeoutErr(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// connectorParam is the shared {connector} params shape of stop/delete/reset.
type connectorParam struct {
	Connector string `json:"connector"`
}

// parseConnectorParam decodes and caps a connector name without I/O.
func parseConnectorParam(raw json.RawMessage) (string, error) {
	var p connectorParam
	if err := json.Unmarshal(raw, &p); err != nil || p.Connector == "" {
		return "", fmt.Errorf("actions: connector is required")
	}
	if len(p.Connector) > maxConnectorNameLen {
		return "", fmt.Errorf("actions: connector name is longer than %d bytes", maxConnectorNameLen)
	}
	return p.Connector, nil
}
