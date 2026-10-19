//go:build databricks_integration

// A tag of its own, not `integration`: these tests need a real Databricks
// workspace, which the full-stack CI job never has, and that job fails on
// any skipped test (.github/scripts/fail-on-skips.sh).

package databricks

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// Integration tests run against a real SQL warehouse and are skipped unless
// the credentials are set:
//
//	PIPEROOM_IT_DATABRICKS_DSN        token:<pat>@<host>/<http-path>?catalog=<catalog>
//	PIPEROOM_IT_DATABRICKS_TABLE      optional "schema.table" with rows
//	PIPEROOM_IT_DATABRICKS_TS_COLUMN  optional TIMESTAMP column of that table
//
// The credential is meant to be the read-only one README.md describes, so
// the tests create nothing: they read an existing table and probe names
// that do not exist (it_<random>).
func itDB(t *testing.T) (*DB, string) {
	t.Helper()
	dsn := os.Getenv("PIPEROOM_IT_DATABRICKS_DSN")
	if dsn == "" {
		t.Skip("set PIPEROOM_IT_DATABRICKS_DSN to run Databricks integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, dsn
}

func itName(t *testing.T) string {
	t.Helper()
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return "it_" + hex.EncodeToString(b)
}

func TestIntegrationMissingTableIsNotFound(t *testing.T) {
	db, _ := itDB(t)
	ctx := context.Background()
	missing := itName(t) + "." + itName(t)
	if _, err := db.Freshness(ctx, missing, "loaded_at"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("Freshness of missing table: %v", err)
	}
	if _, _, err := db.Row(ctx, missing, "id", "1"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("Row of missing table: %v", err)
	}
	if _, err := db.PrimaryKeyColumn(ctx, missing); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("PrimaryKeyColumn of missing table: %v", err)
	}
}

func TestIntegrationBadTokenIsPermissionDenied(t *testing.T) {
	_, dsn := itDB(t)
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	bad := "dapi" + strings.Repeat("0", 32)
	_, err = New(context.Background(), strings.Replace(dsn, cfg.Token, bad, 1))
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("bad token: %v", err)
	}
	if strings.Contains(err.Error(), bad) {
		t.Fatalf("token in error: %v", err)
	}
}

func TestIntegrationFreshnessRowCount(t *testing.T) {
	db, _ := itDB(t)
	table, ts := os.Getenv("PIPEROOM_IT_DATABRICKS_TABLE"), os.Getenv("PIPEROOM_IT_DATABRICKS_TS_COLUMN")
	if table == "" || ts == "" {
		t.Skip("set PIPEROOM_IT_DATABRICKS_TABLE and PIPEROOM_IT_DATABRICKS_TS_COLUMN")
	}
	ctx := context.Background()
	newest, err := db.Freshness(ctx, table, ts)
	if err != nil {
		t.Fatalf("Freshness: %v", err)
	}
	t.Logf("newest %s = %v", ts, newest)

	if !newest.IsZero() {
		until := newest.Add(time.Second)
		counts, err := db.CountByBucket(ctx, table, ts, until.Add(-time.Hour), until, 5*time.Minute)
		if err != nil {
			t.Fatalf("CountByBucket: %v", err)
		}
		if len(counts) == 0 {
			t.Fatal("CountByBucket: no bucket holds the newest row")
		}
		t.Logf("%d buckets", len(counts))
	}

	pk, err := db.PrimaryKeyColumn(ctx, table)
	switch {
	case errors.Is(err, clients.ErrNotFound), errors.Is(err, clients.ErrNotSupported):
		t.Logf("no single-column primary key declared: %v", err)
	case err != nil:
		t.Fatalf("PrimaryKeyColumn: %v", err)
	default:
		if _, found, err := db.Row(ctx, table, pk, itName(t)+"' OR '1'='1"); err != nil || found {
			t.Fatalf("Row of a key that does not exist: found=%v err=%v", found, err)
		}
	}
}
