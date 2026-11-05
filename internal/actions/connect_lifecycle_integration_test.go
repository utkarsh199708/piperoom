//go:build integration

package actions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/owners"
	"piperoom/internal/testenv"
	"piperoom/internal/tools"
)

// devOrdersConnector is the Debezium source the dev stack runs.
const devOrdersConnector = "pg-orders-connector"

// TestConnectLifecycleActionsIntegration runs connect_stop_connector and
// connect_reset_source_offsets against the dev stack's Debezium connector:
// empty reset, reset to the position the connector stored, and the refusal of
// a running connector without the stop step. The connector's original offsets
// are put back and the connector resumed on every exit path.
func TestConnectLifecycleActionsIntegration(t *testing.T) {
	cfg := testenv.Require(t)
	// GET /connectors/{name}/offsets reads the offsets topic through the
	// worker's herder and waits out a rebalance; on the shared CI worker
	// (other packages create and delete connectors in parallel) one such
	// request has hung past any bound while /status answered in ms. Each
	// request gets 30 s and retryBusyConnect re-issues a hung or
	// rebalance-refused one within connectBusyBudget.
	c := connect.New(cfg.ConnectURL, &http.Client{Timeout: 30 * time.Second})
	bg := tools.WithUnscoped(context.Background())

	startRunning(t, c, devOrdersConnector)
	var original []connect.SourceOffset
	err := retryBusyConnect(t, "read stored offsets", func() (err error) {
		original, err = c.SourceOffsets(bg, devOrdersConnector)
		return err
	})
	if err != nil {
		if errors.Is(err, clients.ErrNotSupported) {
			t.Skipf("this dev Connect has no KIP-875 offsets endpoint: %v", err)
		}
		t.Fatalf("read stored offsets: %v", err)
	}
	if len(original) == 0 {
		t.Skip("the dev connector stores no offsets yet; nothing to reset")
	}
	t.Cleanup(func() { restoreConnector(t, c, devOrdersConnector, original) })

	var connCfg map[string]string
	err = retryBusyConnect(t, "read config", func() (err error) {
		connCfg, err = c.Config(bg, devOrdersConnector)
		return err
	})
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	pl := owners.Pipeline{
		ID:           "orders",
		TopicPrefix:  connCfg["topic.prefix"],
		Connectors:   []string{devOrdersConnector},
		SourceTables: splitList(connCfg["table.include.list"]),
	}
	ctx := tools.WithPipeline(context.Background(), pl)

	reg := NewRegistry()
	if err := RegisterLifecycle(reg, LifecycleDeps{Connect: c}); err != nil {
		t.Fatal(err)
	}
	stop := mustGet(t, reg, StopConnectorName)
	reset := mustGet(t, reg, ResetSourceOffsetsName)

	// The connector is RUNNING: the reset refuses without the stop step.
	err = retryBusyConnect(t, "reset Validate while RUNNING", func() error {
		return reset.Validate(ctx, json.RawMessage(`{"connector":"`+devOrdersConnector+`"}`))
	})
	if !errors.Is(err, ErrStopStepRequired) {
		t.Fatalf("reset while RUNNING: %v, want ErrStopStepRequired", err)
	}

	// Stop it (the first step of the proposal).
	if err := retryBusyConnect(t, "stop Validate", func() error {
		return stop.Validate(ctx, json.RawMessage(`{"connector":"`+devOrdersConnector+`"}`))
	}); err != nil {
		t.Fatalf("stop Validate: %v", err)
	}
	out, err := stop.Execute(ctx, json.RawMessage(`{"connector":"`+devOrdersConnector+`"}`))
	if err != nil {
		t.Fatalf("stop Execute: %v", err)
	}
	t.Logf("stop: %s", out)
	var st clients.ConnectorStatus
	err = retryBusyConnect(t, "status after stop", func() (err error) {
		st, err = c.Status(bg, devOrdersConnector)
		return err
	})
	if err != nil || st.State != "STOPPED" {
		t.Fatalf("after stop: state=%v err=%v", st.State, err)
	}

	// Preview shows the offsets now and what a restart does.
	var res PreviewResult
	err = retryBusyConnect(t, "reset Preview", func() (err error) {
		res, err = reset.(Preview).Preview(ctx, json.RawMessage(`{"connector":"`+devOrdersConnector+`"}`))
		return err
	})
	if err != nil {
		t.Fatalf("reset Preview: %v", err)
	}
	t.Logf("preview current: %s", res.Current)
	t.Logf("preview expected: %s", res.Expected)
	if !strings.Contains(res.Current, "lsn") && !strings.Contains(res.Current, "file") && !strings.Contains(res.Current, "offset") {
		t.Fatalf("preview does not show a stored position: %q", res.Current)
	}
	if !strings.Contains(res.Expected, "snapshot.mode") {
		t.Fatalf("preview does not explain snapshot.mode: %q", res.Expected)
	}
	if !res.Reversible || !strings.Contains(res.Rollback, ResetSourceOffsetsName) {
		t.Fatalf("preview rollback = %q", res.Rollback)
	}

	// Empty reset: the connector stores nothing afterwards.
	if err := retryBusyConnect(t, "reset Validate", func() error {
		return reset.Validate(ctx, json.RawMessage(`{"connector":"`+devOrdersConnector+`"}`))
	}); err != nil {
		t.Fatalf("reset Validate: %v", err)
	}
	out, err = reset.Execute(ctx, json.RawMessage(`{"connector":"`+devOrdersConnector+`"}`))
	if err != nil {
		t.Fatalf("reset Execute: %v", err)
	}
	t.Logf("empty reset: %s", out)
	var offs []connect.SourceOffset
	if err := retryBusyConnect(t, "read offsets after the empty reset", func() (err error) {
		offs, err = c.SourceOffsets(bg, devOrdersConnector)
		return err
	}); err != nil {
		t.Fatalf("read offsets after the empty reset: %v", err)
	}
	if len(offs) != 0 {
		t.Fatalf("after an empty reset the connector still stores %d offsets", len(offs))
	}

	// Reset to the position it had: params carry the exact stored shape.
	pos, _ := json.Marshal(map[string]any{
		"connector":  devOrdersConnector,
		"stop_first": true,
		"position":   map[string]any{"partition": original[0].Partition, "offset": original[0].Offset},
	})
	if err := retryBusyConnect(t, "position Validate", func() error { return reset.Validate(ctx, pos) }); err != nil {
		t.Fatalf("position Validate: %v", err)
	}
	out, err = reset.Execute(ctx, pos)
	if err != nil {
		t.Fatalf("position Execute: %v", err)
	}
	t.Logf("position reset: %s", out)
	var got []connect.SourceOffset
	err = retryBusyConnect(t, "re-read offsets", func() (err error) {
		got, err = c.SourceOffsets(bg, devOrdersConnector)
		return err
	})
	if err != nil {
		t.Fatalf("re-read offsets: %v", err)
	}
	if len(got) != 1 || !sameJSON(got[0].Partition, original[0].Partition) || !sameJSON(got[0].Offset, original[0].Offset) {
		t.Fatalf("after the position reset got %s, want %s", formatSourceOffsets(got), formatSourceOffsets(original))
	}
}

// startRunning resumes a stopped or paused connector and waits for RUNNING.
func startRunning(t *testing.T, c *connect.Client, name string) {
	t.Helper()
	bg := tools.WithUnscoped(context.Background())
	var st clients.ConnectorStatus
	err := retryBusyConnect(t, "status", func() (err error) {
		st, err = c.Status(bg, name)
		return err
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.State == "RUNNING" {
		return
	}
	if st.State == "FAILED" {
		t.Skipf("the dev connector %s is FAILED; skipping the lifecycle test", name)
	}
	if err := retryBusyConnect(t, "resume", func() error { return c.Resume(bg, name) }); err != nil {
		t.Fatalf("resume: %v", err)
	}
	testenv.WaitFor(t, 90*time.Second, func() (bool, error) {
		st, err := c.Status(bg, name)
		return err == nil && st.State == "RUNNING", nil
	})
}

// restoreConnector stops the connector if needed, puts the original offsets
// back and resumes it. Each call gets the busy-worker retry: a cleanup that
// gives up on one hung request leaves the shared dev connector STOPPED or at
// the wrong position for every later test.
func restoreConnector(t *testing.T, c *connect.Client, name string, original []connect.SourceOffset) {
	t.Helper()
	ctx := tools.WithUnscoped(context.Background())
	var st clients.ConnectorStatus
	err := retryBusyConnect(t, "cleanup status", func() (err error) {
		st, err = c.Status(ctx, name)
		return err
	})
	if err == nil && st.State != "STOPPED" {
		if err := retryBusyConnect(t, "cleanup stop", func() error { return c.StopConnector(ctx, name) }); err != nil {
			t.Logf("cleanup stop %s: %v", name, err)
		}
	}
	var got []connect.SourceOffset
	if err := retryBusyConnect(t, "cleanup read offsets", func() (err error) {
		got, err = c.SourceOffsets(ctx, name)
		return err
	}); err != nil {
		t.Logf("cleanup read offsets %s: %v", name, err)
		return
	}
	if !sameOffsetSet(got, original) {
		if err := retryBusyConnect(t, "cleanup restore offsets", func() error { return c.AlterSourceOffsets(ctx, name, original) }); err != nil {
			t.Logf("cleanup restore offsets %s: %v", name, err)
		}
	}
	if err := retryBusyConnect(t, "cleanup resume", func() error { return c.Resume(ctx, name) }); err != nil {
		t.Logf("cleanup resume %s: %v", name, err)
	}
}

// connectBusyBudget is how long retryBusyConnect keeps re-issuing a call that
// a busy shared dev Connect worker held or refused. A rebalance triggered by
// another package's connector create or delete settles well inside it.
const connectBusyBudget = 90 * time.Second

// retryBusyConnect runs call until it succeeds, fails with an error that is
// not a busy worker (connectBusy), or connectBusyBudget runs out, pausing with
// a doubling backoff between attempts. It returns the last error unchanged,
// so the caller's assertions see exactly what Connect answered.
func retryBusyConnect(t *testing.T, what string, call func() error) error {
	t.Helper()
	deadline := time.Now().Add(connectBusyBudget)
	pause := 500 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := call()
		if err == nil || !connectBusy(err) || time.Now().Add(pause).After(deadline) {
			return err
		}
		t.Logf("%s: Connect worker busy (attempt %d), retrying in %s: %v", what, attempt, pause, err)
		time.Sleep(pause)
		pause = min(2*pause, 8*time.Second)
	}
}

// connectBusy reports a transient answer from a busy Connect worker: a
// request that outlasted the HTTP client's timeout (the worker held it while
// committing offsets or rebalancing), or Connect's 409/500 that names a
// rebalance (connect.IsRebalancing matches the typed status and the decoded
// error message, not the raw body).
func connectBusy(err error) bool {
	return isTimeoutErr(err) || connect.IsRebalancing(err)
}

// sameOffsetSet compares two stored offset sets by their JSON encoding.
func sameOffsetSet(a, b []connect.SourceOffset) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return bytes.Equal(ab, bb)
}

// splitList splits a comma-separated Debezium list, trimming blanks.
func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
