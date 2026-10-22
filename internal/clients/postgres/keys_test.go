package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// keyRows answers WindowKeys: one row per key, with the same total on every
// row, as count(*) OVER () does.
type keyRows struct {
	pgx.Rows // unused methods panic: the tests only drive Next/Scan/Close/Err
	keys     []string
	total    int64
	i        int
	scanErr  error
	rowsErr  error
}

func (r *keyRows) Next() bool {
	if r.i >= len(r.keys) {
		return false
	}
	r.i++
	return true
}

func (r *keyRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	*dest[0].(*string) = r.keys[r.i-1]
	*dest[1].(*int64) = r.total
	return nil
}

func (r *keyRows) Close()     {}
func (r *keyRows) Err() error { return r.rowsErr }

// keyQuerier records the statement and answers it with rows.
type keyQuerier struct {
	fakeQuerier
	rows *keyRows
}

func (q *keyQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if _, err := q.fakeQuerier.Query(ctx, sql, args...); err != nil {
		return nil, err
	}
	return q.rows, nil
}

var (
	keysSince = time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)
	keysUntil = keysSince.Add(30 * time.Minute)
)

func TestWindowKeys(t *testing.T) {
	q := &keyQuerier{rows: &keyRows{keys: []string{"1", "2", "3"}, total: 3}}
	keys, total, err := testDB(q).WindowKeys(context.Background(), "public.orders", "id", "updated_at", keysSince, keysUntil, 100)
	if err != nil {
		t.Fatalf("WindowKeys: %v", err)
	}
	if total != 3 || strings.Join(keys, ",") != "1,2,3" {
		t.Fatalf("keys = %v, total = %d", keys, total)
	}
	if len(q.sqls) != 1 {
		t.Fatalf("%d statements, want 1", len(q.sqls))
	}
	sql := q.sqls[0]
	for _, want := range []string{"DISTINCT", "::text", "count(*) OVER ()", "ORDER BY k LIMIT $3",
		`"public"."orders"`, `"id"`, `"updated_at"`, ">= $1", "< $2"} {
		if !strings.Contains(sql, want) {
			t.Errorf("query lacks %q: %s", want, sql)
		}
	}
	// Values are parameters, never interpolated.
	for _, secret := range []string{keysSince.Format("2006-01-02"), "LIMIT 100"} {
		if strings.Contains(sql, secret) {
			t.Errorf("query interpolates %q: %s", secret, sql)
		}
	}
	args := q.args[0]
	if len(args) != 3 || args[2] != 100 {
		t.Fatalf("args = %v", args)
	}
	if got := args[0].(time.Time); !got.Equal(keysSince) || got.Location() != time.UTC {
		t.Errorf("since arg = %v, want UTC %v", got, keysSince)
	}
}

// A total above the limit is reported: the caller refuses to diff a cut read.
func TestWindowKeysTotalAboveLimit(t *testing.T) {
	q := &keyQuerier{rows: &keyRows{keys: []string{"1", "2"}, total: 2500}}
	keys, total, err := testDB(q).WindowKeys(context.Background(), "public.orders", "id", "updated_at", keysSince, keysUntil, 2)
	if err != nil || total != 2500 || len(keys) != 2 {
		t.Fatalf("keys = %v, total = %d, err = %v", keys, total, err)
	}
}

func TestWindowKeysRejects(t *testing.T) {
	cases := []struct {
		name              string
		table, key, tsCol string
		since, until      time.Time
		limit             int
	}{
		{"bad table", "orders; drop", "id", "updated_at", keysSince, keysUntil, 10},
		{"three-part table", "db.public.orders", "id", "updated_at", keysSince, keysUntil, 10},
		{"bad key", "public.orders", "id; drop", "updated_at", keysSince, keysUntil, 10},
		{"bad ts", "public.orders", "id", "updated_at = 1", keysSince, keysUntil, 10},
		{"zero limit", "public.orders", "id", "updated_at", keysSince, keysUntil, 0},
		{"limit over the bound", "public.orders", "id", "updated_at", keysSince, keysUntil, maxWindowKeys + 1},
		{"empty window", "public.orders", "id", "updated_at", keysUntil, keysUntil, 10},
		{"reversed window", "public.orders", "id", "updated_at", keysUntil, keysSince, 10},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := &keyQuerier{rows: &keyRows{}}
			if _, _, err := testDB(q).WindowKeys(context.Background(), c.table, c.key, c.tsCol, c.since, c.until, c.limit); err == nil {
				t.Fatal("accepted")
			}
			if len(q.sqls) != 0 {
				t.Fatalf("ran a statement: %v", q.sqls)
			}
		})
	}
}

func TestWindowKeysErrors(t *testing.T) {
	ctx := context.Background()
	if _, _, err := testDB(&fakeQuerier{err: errors.New("connection refused")}).WindowKeys(ctx, "public.orders", "id", "updated_at", keysSince, keysUntil, 10); err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("query error = %v", err)
	}
	if _, _, err := testDB(&keyQuerier{rows: &keyRows{keys: []string{"1"}, scanErr: errors.New("bad cell")}}).WindowKeys(ctx, "public.orders", "id", "updated_at", keysSince, keysUntil, 10); err == nil || !strings.Contains(err.Error(), "scan") {
		t.Fatalf("scan error = %v", err)
	}
	if _, _, err := testDB(&keyQuerier{rows: &keyRows{rowsErr: errors.New("stream cut")}}).WindowKeys(ctx, "public.orders", "id", "updated_at", keysSince, keysUntil, 10); err == nil || !strings.Contains(err.Error(), "stream cut") {
		t.Fatalf("rows error = %v", err)
	}
}
