//go:build integration

package sqlserver

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// Setup-writer integration tests run against the SQL Server named by
// PIPEROOM_MSSQL_DSN (the dev compose profile "mssql", U4c, which has not
// landed yet, so they skip; the skip is stated in the PR). The DSN must be a
// db_owner (or sysadmin) login: sp_cdc_enable_db and sp_cdc_enable_table
// require it, and the writer is a separate write pool, never the read-only
// DSN. Objects are named it_<hex> and dropped afterwards.

func uniqueSuffix(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b[:])
}

func dropTable(t *testing.T, dsn, table string) {
	t.Helper()
	schema, tbl, err := splitTable(table)
	if err != nil {
		return
	}
	qs, _ := quoteIdent(schema)
	qt, _ := quoteIdent(tbl)
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+qs+"."+qt)
}

func TestSetupWriterCanaryIntegration(t *testing.T) {
	dsn := mssqlDSN(t)
	ctx := tools.WithUnscoped(context.Background())
	w, err := NewSetupWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("sqlserver writer unreachable (%s): %v", hostPort(dsn), err)
	}
	t.Cleanup(func() { _ = w.Close() })

	table := "dbo.it_" + uniqueSuffix(t)
	t.Cleanup(func() { dropTable(t, dsn, table) })

	if err := w.CreateCanaryTable(ctx, table); err != nil {
		t.Fatal(err)
	}
	if err := w.InsertCanary(ctx, table, "deadbeefdeadbeefdeadbeefdeadbeef"); err != nil {
		t.Fatal(err)
	}
}

func TestAlterSystemNotSupportedIntegration(t *testing.T) {
	dsn := mssqlDSN(t)
	w, err := NewSetupWriter(context.Background(), dsn)
	if err != nil {
		t.Fatalf("sqlserver writer unreachable (%s): %v", hostPort(dsn), err)
	}
	t.Cleanup(func() { _ = w.Close() })
	if _, err := w.AlterSystem(context.Background(), "wal_level", "logical"); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("err = %v, want clients.ErrNotSupported", err)
	}
}
