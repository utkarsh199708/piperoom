//go:build integration

package oracle

import (
	"context"
	"os"
	"testing"
)

// oracleDSN names the environment variable that carries the go-ora URL for the
// "oracle" compose profile (U9c). The test skips when it is unset, so the
// default unit run stays database-free.
const oracleDSN = "ORACLE_DSN"

func newIntegrationClient(t *testing.T) *Client {
	t.Helper()
	dsn := os.Getenv(oracleDSN)
	if dsn == "" {
		t.Skipf("oracle integration skipped: set %s to a go-ora URL (oracle://user:pass@host:port/service)", oracleDSN)
	}
	c, err := New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("oracle unreachable (%s): %v", targetOf(dsn), err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestDatabaseIntegration(t *testing.T) {
	c := newIntegrationClient(t)

	info, err := c.Database(context.Background())
	if err != nil {
		t.Fatalf("Database: %v", err)
	}
	if info.Version == "" {
		t.Fatal("empty version banner")
	}
	switch info.LogMode {
	case "ARCHIVELOG", "NOARCHIVELOG":
	default:
		t.Fatalf("unexpected LOG_MODE %q", info.LogMode)
	}
	if info.IsCDB && len(info.PDBs) == 0 {
		t.Fatal("CDB reports no PDBs")
	}
}
