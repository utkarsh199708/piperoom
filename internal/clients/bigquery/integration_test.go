//go:build integration

package bigquery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// Integration tests run against a real BigQuery project and are skipped
// unless the credentials are set:
//
//	GOOGLE_APPLICATION_CREDENTIALS  service account key (or other ADC source)
//	PIPEROOM_IT_BIGQUERY_DSN        bigquery://<project>/<location>
//	PIPEROOM_IT_BIGQUERY_TABLE      optional "dataset.table" with rows
//	PIPEROOM_IT_BIGQUERY_TS_COLUMN  optional TIMESTAMP column of that table
//
// The credential is meant to be the read-only one README.md describes, so
// the tests create nothing: they read an existing table and probe names that
// do not exist (it_<random>).
func itDB(t *testing.T) *DB {
	t.Helper()
	dsn := os.Getenv("PIPEROOM_IT_BIGQUERY_DSN")
	if dsn == "" || os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		t.Skip("set GOOGLE_APPLICATION_CREDENTIALS and PIPEROOM_IT_BIGQUERY_DSN to run BigQuery integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
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
	db := itDB(t)
	ctx := context.Background()
	missing := itName(t) + "." + itName(t)
	if _, err := db.Freshness(ctx, missing, "loaded_at"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("Freshness of missing table: %v", err)
	}
	if _, _, err := db.Row(ctx, missing, "id", "1"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("Row of missing table: %v", err)
	}
}

func TestIntegrationFreshnessAndRow(t *testing.T) {
	db := itDB(t)
	table, ts := os.Getenv("PIPEROOM_IT_BIGQUERY_TABLE"), os.Getenv("PIPEROOM_IT_BIGQUERY_TS_COLUMN")
	if table == "" || ts == "" {
		t.Skip("set PIPEROOM_IT_BIGQUERY_TABLE and PIPEROOM_IT_BIGQUERY_TS_COLUMN")
	}
	ctx := context.Background()
	got, err := db.Freshness(ctx, table, ts)
	if err != nil {
		t.Fatalf("Freshness: %v", err)
	}
	t.Logf("newest %s = %v", ts, got)

	pk, err := db.PrimaryKeyColumn(ctx, table)
	switch {
	case errors.Is(err, clients.ErrNotFound), errors.Is(err, clients.ErrNotSupported):
		t.Logf("no single-column primary key declared: %v", err)
	case err != nil:
		t.Fatalf("PrimaryKeyColumn: %v", err)
	default:
		if _, found, err := db.Row(ctx, table, pk, itName(t)); err != nil || found {
			t.Fatalf("Row of a key that does not exist: found=%v err=%v", found, err)
		}
	}
}

func TestIntegrationLoadErrors(t *testing.T) {
	db := itDB(t)
	table := os.Getenv("PIPEROOM_IT_BIGQUERY_TABLE")
	if table == "" {
		t.Skip("set PIPEROOM_IT_BIGQUERY_TABLE")
	}
	got, err := db.LoadErrors(context.Background(), table, time.Now().Add(-7*24*time.Hour), 5)
	if errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("LoadErrors needs roles/bigquery.resourceViewer (bigquery.jobs.listAll): %v", err)
	}
	if err != nil {
		t.Fatalf("LoadErrors: %v", err)
	}
	t.Logf("%d failed or partial jobs", len(got))
}
