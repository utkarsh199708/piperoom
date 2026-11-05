//go:build integration

package oracle

import (
	"context"
	"os"
	"testing"

	"piperoom/internal/tools"
)

// newIntegrationWriter opens the write pool against ORACLE_DSN (the same
// variable the read-only integration test uses; the write pool needs its own
// write-capable role in the DSN). It skips when the variable is unset.
func newIntegrationWriter(t *testing.T) *Writer {
	t.Helper()
	dsn := os.Getenv(oracleDSN)
	if dsn == "" {
		t.Skipf("oracle integration skipped: set %s to a go-ora URL (oracle://user:pass@host:port/service)", oracleDSN)
	}
	w, err := NewWriter(context.Background(), dsn)
	if err != nil {
		t.Fatalf("oracle writer unreachable (%s): %v", targetOf(dsn), err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

// TestWriterRefusesIntegration runs the fail-closed paths that do no I/O
// against a real connection: a non-allow-listed setting and bad identifiers
// must be refused without reaching the database, so they are safe to run
// against any Oracle. The write-path round trips (create canary, insert,
// create role, enable archive log mode) are covered by the oracle compose
// profile (U9c) integration once that lands.
func TestWriterRefusesIntegration(t *testing.T) {
	w := newIntegrationWriter(t)

	if _, err := w.AlterSystem(tools.WithUnscoped(context.Background()), "processes", "100"); err == nil {
		t.Fatal("AlterSystem(processes) succeeded, want refusal")
	}
	if err := w.CreatePublication(tools.WithUnscoped(context.Background()), "dbz", []string{`a;DROP`}); err == nil {
		t.Fatal("CreatePublication with a bad identifier succeeded, want refusal")
	}
}
