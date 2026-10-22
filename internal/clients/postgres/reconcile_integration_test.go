//go:build integration

package postgres

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"piperoom/internal/owners"
	"piperoom/internal/testenv"
	"piperoom/internal/tools"
)

// TestReconcilerIntegration (X1a TC6): 100 rows in a window are counted,
// none outside it; the newest keys come first; RowTimes returns the exact
// timestamps and never the email column. The stack being down skips the
// test (testenv.Require), it does not fail it (TC8).
func TestReconcilerIntegration(t *testing.T) {
	db := sourceDB(t)
	conn := writeConn(t, testenv.Require(t).SourceDBURL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	table := testenv.Name("reconcile")
	if _, err := conn.Exec(ctx, "CREATE TABLE public."+table+" (id bigint primary key, email text, updated_at timestamptz, created_on timestamp)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(c, "DROP TABLE IF EXISTS public."+table)
	})

	from := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Microsecond)
	to := from.Add(time.Hour)
	if _, err := conn.Exec(ctx, `INSERT INTO public.`+table+` (id, email, updated_at, created_on)
		SELECT g, 'user' || g || '@example.com', $1::timestamptz + g * interval '30 seconds', ($1::timestamptz AT TIME ZONE 'UTC') + g * interval '1 second'
		FROM generate_series(1, 100) g`, from); err != nil {
		t.Fatalf("insert: %v", err)
	}

	sctx := tools.WithPipeline(ctx, owners.Pipeline{ID: "it", SourceTables: []string{"public." + table}})
	if n, err := db.CountBetween(sctx, "public."+table, "updated_at", from, to); err != nil || n != 100 {
		t.Fatalf("CountBetween(window) = %d, %v; want 100", n, err)
	}
	if n, err := db.CountBetween(sctx, "public."+table, "updated_at", to, to.Add(time.Hour)); err != nil || n != 0 {
		t.Fatalf("CountBetween(after) = %d, %v; want 0", n, err)
	}

	keys, err := db.SampleKeys(sctx, "public."+table, "id", "updated_at", from, 5)
	if err != nil || strings.Join(keys, ",") != "100,99,98,97,96" {
		t.Fatalf("SampleKeys = %v, %v; want the newest five", keys, err)
	}

	got, err := db.RowTimes(sctx, "public."+table, "id", []string{"1", "100", "424242"}, []string{"updated_at", "created_on"})
	if err != nil {
		t.Fatalf("RowTimes: %v", err)
	}
	if _, ok := got["424242"]; ok || len(got) != 2 {
		t.Fatalf("RowTimes keys = %v; want 1 and 100 only", got)
	}
	for _, id := range []int{1, 100} {
		row := got[strconv.Itoa(id)]
		wantUpd := from.Add(time.Duration(id) * 30 * time.Second)
		wantCreated := from.Add(time.Duration(id) * time.Second)
		if !row["updated_at"].Equal(wantUpd) || !row["created_on"].Equal(wantCreated) || len(row) != 2 {
			t.Errorf("row %d = %v; want updated_at %s, created_on %s", id, row, wantUpd, wantCreated)
		}
	}
	if _, err := db.RowTimes(sctx, "public."+table, "id", []string{"1"}, []string{"email"}); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("RowTimes(email) err = %v; want ErrBadInput", err)
	}
	if _, err := db.CountBetween(sctx, "public.orders", "updated_at", from, to); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("out-of-scope table err = %v", err)
	}
}

// TestReconcilerDeadlineIntegration (X1a TC7): a 12 s read behind a view is
// cut at the 10 s deadline with context.DeadlineExceeded wrapped.
func TestReconcilerDeadlineIntegration(t *testing.T) {
	db := sourceDB(t)
	conn := writeConn(t, testenv.Require(t).SourceDBURL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	view := testenv.Name("slow")
	if _, err := conn.Exec(ctx, "CREATE VIEW public."+view+" AS SELECT now() AS ts FROM pg_sleep(12)"); err != nil {
		t.Fatalf("create view: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(c, "DROP VIEW IF EXISTS public."+view)
	})

	sctx := tools.WithPipeline(ctx, owners.Pipeline{ID: "it", SourceTables: []string{"public." + view}})
	start := time.Now()
	_, err := db.CountBetween(sctx, "public."+view, "ts", time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	took := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v; want DeadlineExceeded", err)
	}
	if took > 11*time.Second {
		t.Fatalf("took %s; the 10 s deadline did not cut the read", took)
	}
}
