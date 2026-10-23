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
	c := connect.New(cfg.ConnectURL, &http.Client{Timeout: 10 * time.Second})
	bg := tools.WithUnscoped(context.Background())

	startRunning(t, c, devOrdersConnector)
	original, err := c.SourceOffsets(bg, devOrdersConnector)
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

	connCfg, err := c.Config(bg, devOrdersConnector)
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
	if err := reset.Validate(ctx, json.RawMessage(`{"connector":"`+devOrdersConnector+`"}`)); !errors.Is(err, ErrStopStepRequired) {
		t.Fatalf("reset while RUNNING: %v, want ErrStopStepRequired", err)
	}

	// Stop it (the first step of the proposal).
	if err := stop.Validate(ctx, json.RawMessage(`{"connector":"`+devOrdersConnector+`"}`)); err != nil {
		t.Fatalf("stop Validate: %v", err)
	}
	out, err := stop.Execute(ctx, json.RawMessage(`{"connector":"`+devOrdersConnector+`"}`))
	if err != nil {
		t.Fatalf("stop Execute: %v", err)
	}
	t.Logf("stop: %s", out)
	if st, err := c.Status(bg, devOrdersConnector); err != nil || st.State != "STOPPED" {
		t.Fatalf("after stop: state=%v err=%v", st.State, err)
	}

	// Preview shows the offsets now and what a restart does.
	res, err := reset.(Preview).Preview(ctx, json.RawMessage(`{"connector":"`+devOrdersConnector+`"}`))
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
	if err := reset.Validate(ctx, json.RawMessage(`{"connector":"`+devOrdersConnector+`"}`)); err != nil {
		t.Fatalf("reset Validate: %v", err)
	}
	out, err = reset.Execute(ctx, json.RawMessage(`{"connector":"`+devOrdersConnector+`"}`))
	if err != nil {
		t.Fatalf("reset Execute: %v", err)
	}
	t.Logf("empty reset: %s", out)
	if offs, _ := c.SourceOffsets(bg, devOrdersConnector); len(offs) != 0 {
		t.Fatalf("after an empty reset the connector still stores %d offsets", len(offs))
	}

	// Reset to the position it had: params carry the exact stored shape.
	pos, _ := json.Marshal(map[string]any{
		"connector":  devOrdersConnector,
		"stop_first": true,
		"position":   map[string]any{"partition": original[0].Partition, "offset": original[0].Offset},
	})
	if err := reset.Validate(ctx, pos); err != nil {
		t.Fatalf("position Validate: %v", err)
	}
	out, err = reset.Execute(ctx, pos)
	if err != nil {
		t.Fatalf("position Execute: %v", err)
	}
	t.Logf("position reset: %s", out)
	got, err := c.SourceOffsets(bg, devOrdersConnector)
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
	st, err := c.Status(bg, name)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.State == "RUNNING" {
		return
	}
	if st.State == "FAILED" {
		t.Skipf("the dev connector %s is FAILED; skipping the lifecycle test", name)
	}
	if err := c.Resume(bg, name); err != nil {
		t.Fatalf("resume: %v", err)
	}
	testenv.WaitFor(t, 90*time.Second, func() (bool, error) {
		st, err := c.Status(bg, name)
		return err == nil && st.State == "RUNNING", nil
	})
}

// restoreConnector stops the connector if needed, puts the original offsets
// back and resumes it.
func restoreConnector(t *testing.T, c *connect.Client, name string, original []connect.SourceOffset) {
	t.Helper()
	ctx, cancel := context.WithTimeout(tools.WithUnscoped(context.Background()), 30*time.Second)
	defer cancel()
	st, err := c.Status(ctx, name)
	if err == nil && st.State != "STOPPED" {
		if err := c.StopConnector(ctx, name); err != nil {
			t.Logf("cleanup stop %s: %v", name, err)
		}
	}
	got, err := c.SourceOffsets(ctx, name)
	if err != nil {
		t.Logf("cleanup read offsets %s: %v", name, err)
		return
	}
	if !sameOffsetSet(got, original) {
		if err := c.AlterSourceOffsets(ctx, name, original); err != nil {
			t.Logf("cleanup restore offsets %s: %v", name, err)
		}
	}
	if err := c.Resume(ctx, name); err != nil {
		t.Logf("cleanup resume %s: %v", name, err)
	}
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
