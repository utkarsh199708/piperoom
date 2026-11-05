//go:build integration

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"piperoom/internal/testenv"
)

// TestWindowKeysIntegration checks the distinct-key read end to end: one key
// is one row however often it was updated, rows outside the window are
// excluded, and a bounded read still reports the true total.
func TestWindowKeysIntegration(t *testing.T) {
	db := sourceDB(t)
	conn := writeConn(t, testenv.Require(t).SourceDBURL)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	table := testenv.Name("keys")
	if _, err := conn.Exec(ctx, "CREATE TABLE public."+table+" (id bigint primary key, updated_at timestamptz)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(c, "DROP TABLE IF EXISTS public."+table)
	})

	since := time.Now().Add(-time.Hour).UTC()
	until := time.Now().Add(-time.Minute).UTC()
	// Three keys in the window; id 1 is updated twice (three events, one row).
	for _, id := range []int64{1, 2, 3} {
		if _, err := conn.Exec(ctx, "INSERT INTO public."+table+" (id, updated_at) VALUES ($1, $2)", id, since.Add(time.Duration(id)*time.Minute)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if _, err := conn.Exec(ctx, "UPDATE public."+table+" SET updated_at = $1 WHERE id = 1", since.Add(30*time.Minute)); err != nil {
		t.Fatalf("update: %v", err)
	}
	// Two rows outside the window.
	if _, err := conn.Exec(ctx, "INSERT INTO public."+table+" (id, updated_at) VALUES (4, $1), (5, $2)", since.Add(-time.Minute), until.Add(time.Minute)); err != nil {
		t.Fatalf("insert outside: %v", err)
	}

	keys, total, err := db.WindowKeys(ctx, "public."+table, "id", "updated_at", since, until, 100)
	if err != nil {
		t.Fatalf("WindowKeys: %v", err)
	}
	if total != 3 || strings.Join(keys, ",") != "1,2,3" {
		t.Fatalf("keys = %v, total = %d; want 1,2,3 / 3", keys, total)
	}

	// A bounded read returns its limit and the true total, so the caller can
	// refuse to diff a cut read.
	keys, total, err = db.WindowKeys(ctx, "public."+table, "id", "updated_at", since, until, 2)
	if err != nil {
		t.Fatalf("WindowKeys (bounded): %v", err)
	}
	if total != 3 || len(keys) != 2 {
		t.Fatalf("bounded keys = %v, total = %d; want 2 keys and total 3", keys, total)
	}

	if _, _, err := db.WindowKeys(ctx, "public."+table+"; drop", "id", "updated_at", since, until, 10); err == nil {
		t.Fatal("a bad table name was accepted")
	}
}
