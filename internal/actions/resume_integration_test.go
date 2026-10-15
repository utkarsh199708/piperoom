//go:build integration

package actions

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients/connect"
	"piperoom/internal/testenv"
)

const (
	resumeConnectorName = "pg-orders-connector"
	resumeSlotName      = "debezium_orders"
)

// slotActive reports whether the Debezium replication slot has a WAL sender.
// It reads the source Postgres directly (pgx is an existing dependency).
func slotActive(t *testing.T, dsn string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect source postgres: %v", err)
	}
	defer conn.Close(context.Background())
	var active bool
	if err := conn.QueryRow(ctx,
		"SELECT active FROM pg_replication_slots WHERE slot_name = $1", resumeSlotName).Scan(&active); err != nil {
		t.Fatalf("read slot %s: %v", resumeSlotName, err)
	}
	return active
}

// stopConnector mirrors `devctl break slot-inactive`'s Connect call
// (PUT /connectors/{name}/stop) without pulling cmd/devctl into this package.
func stopConnector(t *testing.T, connectURL string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, connectURL+"/connectors/"+resumeConnectorName+"/stop", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hc := &http.Client{Timeout: 10 * time.Second}
	resp, err := hc.Do(req.WithContext(ctx))
	if err != nil {
		t.Fatalf("stop connector: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("stop connector: status %d", resp.StatusCode)
	}
}

// TestResumeConnectorIntegration breaks slot-inactive (stop the Debezium
// connector), resumes it with connect_resume_connector, and checks the
// connector is RUNNING and the replication slot is active again. It runs under
// the shared stack lock: set PIPEROOM_STACK_OWNER to the lock holder.
func TestResumeConnectorIntegration(t *testing.T) {
	cfg := testenv.Require(t)
	c := connect.New(cfg.ConnectURL, &http.Client{Timeout: 10 * time.Second})

	stopConnector(t, cfg.ConnectURL)
	// The shared pg-orders-connector is used instead of an it_ connector: a
	// Debezium replication slot per test would be too heavy. If the test fails
	// before the action resumes it, this cleanup resumes it so the next run
	// starts from a RUNNING connector. Resume is idempotent, so a second resume
	// after success is harmless.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, cfg.ConnectURL+"/connectors/"+resumeConnectorName+"/resume", nil)
		if err != nil {
			t.Logf("cleanup resume %s: %v", resumeConnectorName, err)
			return
		}
		hc := &http.Client{Timeout: 10 * time.Second}
		resp, err := hc.Do(req)
		if err != nil {
			t.Logf("cleanup resume %s: %v", resumeConnectorName, err)
			return
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		t.Logf("cleanup resume %s: status %d", resumeConnectorName, resp.StatusCode)
	})

	testenv.WaitFor(t, 60*time.Second, func() (bool, error) {
		st, err := c.Status(context.Background(), resumeConnectorName)
		if err != nil {
			return false, err
		}
		return st.State == "STOPPED", nil
	})
	// The slot goes inactive asynchronously after the stop, so poll for it
	// rather than reading it once.
	testenv.WaitFor(t, 60*time.Second, func() (bool, error) {
		return !slotActive(t, cfg.SourceDBURL), nil
	})
	t.Logf("before resume: connector STOPPED, slot %s inactive", resumeSlotName)

	reg := NewRegistry()
	if err := Register(reg, Deps{Connect: c, Registry: nil}); err != nil {
		t.Fatal(err)
	}
	a, ok := reg.Get("connect_resume_connector")
	if !ok {
		t.Fatal("connect_resume_connector not registered")
	}
	if err := a.Validate(t.Context(), json.RawMessage(`{"connector":"pg-orders-connector"}`)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	out, err := a.Execute(context.Background(), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	t.Logf("Execute: %s", out)

	testenv.WaitFor(t, 60*time.Second, func() (bool, error) {
		st, err := c.Status(context.Background(), resumeConnectorName)
		if err != nil {
			return false, err
		}
		return st.State == "RUNNING", nil
	})
	testenv.WaitFor(t, 60*time.Second, func() (bool, error) {
		return slotActive(t, cfg.SourceDBURL), nil
	})
	afterActive := slotActive(t, cfg.SourceDBURL)
	t.Logf("after resume: connector RUNNING, slot %s active=%v", resumeSlotName, afterActive)
	if !afterActive {
		t.Fatalf("slot %s not active after resume", resumeSlotName)
	}
}
