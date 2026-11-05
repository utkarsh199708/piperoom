package connect

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

var _ clients.ConnectFixWriter = (*Client)(nil)

// Caps on what UpdateConnectorConfigKeys sends. The action that calls it
// allows far less (an allow-list of a few keys with fixed values); these only
// keep a bad caller from sending a huge body.
const (
	maxChangeKeys   = 64
	maxChangeKeyLen = 255
	maxChangeValLen = 8 << 10
)

// writeTimeout bounds a write when ctx has no deadline of its own.
const writeTimeout = 10 * time.Second

// checkWriteScope is the fail-closed pipeline scope of the P3 writes
// (P0 item 7), the same rule as the Kafka writer's:
//   - ctx carries the room's whole pipeline (tools.WithPipeline): the
//     connector must be one of its connectors, else tools.ErrOutOfScope;
//   - ctx carries the explicit opt-out (tools.WithUnscoped; CLI, integration
//     tests): nothing is checked;
//   - anything else (no scope, or only a pipeline id, which this client has
//     no owners config to resolve): tools.ErrNoScope.
//
// The older writes (RestartTask, RestartConnector, Resume, UpdateConfig) are
// scoped by the actions' CheckScope only; they are not changed here.
func checkWriteScope(ctx context.Context, connector string) error {
	if _, ok := tools.PipelineFrom(ctx); !ok {
		sc, err := tools.ScopeFor(ctx, nil)
		switch {
		case err == nil && sc.Unscoped():
			return nil
		case err == nil || errors.Is(err, tools.ErrNoScope):
			return fmt.Errorf("connect: %w: a write needs the room's pipeline in ctx (tools.WithPipeline) or tools.WithUnscoped", tools.ErrNoScope)
		default:
			return fmt.Errorf("connect: %w: ctx names the room's pipeline only by id; a write needs the whole pipeline (tools.WithPipeline)", tools.ErrNoScope)
		}
	}
	sc, err := tools.ScopeFor(ctx, nil) // a whole pipeline needs no owners config
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if err := sc.CheckConnector(connector); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	return nil
}

// withWriteDeadline gives ctx the default write deadline when it has none.
func withWriteDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, writeTimeout)
}

// PauseConnector pauses the connector and its tasks
// (PUT /connectors/{name}/pause; Connect answers 202 and applies it
// asynchronously). connect_resume_connector undoes it. It refuses a connector
// outside the room's pipeline and a ctx with no scope before any I/O.
func (c *Client) PauseConnector(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("connect: pause: empty connector name")
	}
	if err := checkWriteScope(ctx, name); err != nil {
		return err
	}
	ctx, cancel := withWriteDeadline(ctx)
	defer cancel()
	path := "/connectors/" + url.PathEscape(name) + "/pause"
	return c.do(ctx, http.MethodPut, path, nil)
}

// UpdateConnectorConfigKeys sets every key of changes in one
// PATCH /connectors/{name}/config (KIP-477, Kafka Connect 3.5+) and leaves the
// other keys alone. PATCH and not GET+PUT: the server merges, so the rest of
// the config (which can hold plaintext passwords) never passes through this
// process, there is no read-modify-write window in which another change is
// lost, and a connector deleted meanwhile answers 404 instead of being
// re-created by a PUT. There is no PUT fallback: an older Connect answers 405,
// reported as an error. Connect restarts the connector's tasks on a change.
// It refuses a connector outside the room's pipeline and a ctx with no scope
// before any I/O. Values never appear in its errors.
func (c *Client) UpdateConnectorConfigKeys(ctx context.Context, name string, changes map[string]string) error {
	if name == "" {
		return fmt.Errorf("connect: update config: empty connector name")
	}
	if err := checkChanges(changes); err != nil {
		return fmt.Errorf("connect: update config %s: %w", name, err)
	}
	if err := checkWriteScope(ctx, name); err != nil {
		return err
	}
	ctx, cancel := withWriteDeadline(ctx)
	defer cancel()
	path := "/connectors/" + url.PathEscape(name) + "/config"
	err := c.doBody(ctx, http.MethodPatch, path, changes, nil)
	var se *statusError
	if errors.As(err, &se) && se.Code == http.StatusMethodNotAllowed {
		return fmt.Errorf("connect: update config %s: Connect does not support PATCH config; needs Kafka Connect 3.5+: %w", name, err)
	}
	return err
}

// checkChanges refuses an empty change set, an empty or oversized key, a key
// with a byte outside printable ASCII, and an oversized value.
func checkChanges(changes map[string]string) error {
	if len(changes) == 0 {
		return errors.New("no keys to change")
	}
	if len(changes) > maxChangeKeys {
		return fmt.Errorf("%d keys; at most %d per request", len(changes), maxChangeKeys)
	}
	for k, v := range changes {
		if k == "" || len(k) > maxChangeKeyLen {
			return fmt.Errorf("config key must be 1..%d bytes", maxChangeKeyLen)
		}
		for i := 0; i < len(k); i++ {
			if k[i] <= ' ' || k[i] > '~' {
				return errors.New("a config key has a byte outside printable ASCII")
			}
		}
		if len(v) > maxChangeValLen {
			return fmt.Errorf("value of %s is longer than %d bytes", k, maxChangeValLen)
		}
	}
	return nil
}
