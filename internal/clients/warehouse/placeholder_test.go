package warehouse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

func sinkCtx() context.Context {
	return tools.WithPipeline(context.Background(), owners.Pipeline{ID: "orders", SinkTables: []string{"analytics.orders"}})
}

func TestCountPlaceholderSQL(t *testing.T) {
	q := &fakeQuerier{}
	until := time.Date(2026, 10, 22, 12, 0, 0, 0, time.FixedZone("X", 3600))
	since := until.Add(-24 * time.Hour)
	if _, err := testDB(q).CountPlaceholder(sinkCtx(), "analytics.orders", "notes", "__debezium_unavailable_value", "updated_at", since, until); err != nil {
		t.Fatal(err)
	}
	if len(q.sqls) != 1 {
		t.Fatalf("%d statements", len(q.sqls))
	}
	want := `SELECT count(*) FROM "analytics"."orders" WHERE "notes"::text = $1 AND "updated_at" >= $2 AND "updated_at" < $3`
	if q.sqls[0] != want {
		t.Errorf("sql = %s", q.sqls[0])
	}
	a := q.args[0]
	if a[0] != "__debezium_unavailable_value" || !a[1].(time.Time).Equal(since) || a[2].(time.Time).Location() != time.UTC {
		t.Errorf("args = %#v", a)
	}
}

func TestCountPlaceholderRefusesBeforeIO(t *testing.T) {
	now := time.Now()
	day := now.Add(-24 * time.Hour)
	for _, c := range []struct {
		name               string
		ctx                context.Context
		table, col, ph, ts string
		since, until       time.Time
		want               error
	}{
		{"bad table", sinkCtx(), "analytics.orders; drop", "notes", "p", "updated_at", day, now, tools.ErrBadInput},
		{"bad column", sinkCtx(), "analytics.orders", "notes\"", "p", "updated_at", day, now, tools.ErrBadInput},
		{"bad ts column", sinkCtx(), "analytics.orders", "notes", "p", "updated at", day, now, tools.ErrBadInput},
		{"empty placeholder", sinkCtx(), "analytics.orders", "notes", "", "updated_at", day, now, tools.ErrBadInput},
		{"long placeholder", sinkCtx(), "analytics.orders", "notes", strings.Repeat("x", MaxPlaceholderBytes+1), "updated_at", day, now, tools.ErrBadInput},
		{"empty window", sinkCtx(), "analytics.orders", "notes", "p", "updated_at", now, now, tools.ErrBadInput},
		{"long window", sinkCtx(), "analytics.orders", "notes", "p", "updated_at", now.Add(-8 * 24 * time.Hour), now, tools.ErrBadInput},
		{"other table", sinkCtx(), "analytics.payments", "notes", "p", "updated_at", day, now, tools.ErrOutOfScope},
		{"no scope", context.Background(), "analytics.orders", "notes", "p", "updated_at", day, now, tools.ErrNoScope},
	} {
		t.Run(c.name, func(t *testing.T) {
			q := &fakeQuerier{}
			_, err := testDB(q).CountPlaceholder(c.ctx, c.table, c.col, c.ph, c.ts, c.since, c.until)
			if !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
			if len(q.sqls) != 0 {
				t.Fatalf("ran %d statements", len(q.sqls))
			}
		})
	}
}

func TestCountPlaceholderErrors(t *testing.T) {
	now := time.Now()
	for _, c := range []struct {
		err   error
		want  error
		quote string
	}{
		{&pgconn.PgError{Code: "42703", Message: `column "notes" does not exist`}, clients.ErrNotFound, ""},
		{&pgconn.PgError{Code: "22007", Message: `invalid input syntax for type timestamp: "alice@example.com"`}, tools.ErrBadInput, "alice"},
	} {
		_, err := testDB(&fakeQuerier{err: c.err}).CountPlaceholder(sinkCtx(), "analytics.orders", "notes", "p", "updated_at", now.Add(-time.Hour), now)
		if !errors.Is(err, c.want) {
			t.Errorf("err = %v, want %v", err, c.want)
		}
		if c.quote != "" && strings.Contains(err.Error(), c.quote) {
			t.Errorf("error quotes a value: %v", err)
		}
	}
}
