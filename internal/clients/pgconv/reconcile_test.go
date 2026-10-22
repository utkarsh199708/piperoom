package pgconv

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// recRows is a canned result: column OIDs and rows of Go values.
type recRows struct {
	pgx.Rows // unused methods panic
	oids     []uint32
	rows     [][]any
	i        int
	err      error
}

func (r *recRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *recRows) FieldDescriptions() []pgconn.FieldDescription {
	fds := make([]pgconn.FieldDescription, len(r.oids))
	for i, o := range r.oids {
		fds[i] = pgconn.FieldDescription{DataTypeOID: o}
	}
	return fds
}

func (r *recRows) Scan(dest ...any) error {
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("scan: %d dests for %d values", len(dest), len(row))
	}
	for i, v := range row {
		switch d := dest[i].(type) {
		case *string:
			*d = v.(string)
		case *int64:
			*d = v.(int64)
		case *bool:
			*d = v.(bool)
		case *pgtype.Timestamptz:
			*d = pgtype.Timestamptz{}
			if t, ok := v.(time.Time); ok {
				*d = pgtype.Timestamptz{Time: t, Valid: true}
			} else if m, ok := v.(pgtype.InfinityModifier); ok {
				*d = pgtype.Timestamptz{InfinityModifier: m, Valid: true}
			}
		case *pgtype.Timestamp:
			*d = pgtype.Timestamp{}
			if t, ok := v.(time.Time); ok {
				*d = pgtype.Timestamp{Time: t, Valid: true}
			}
		case *pgtype.Date:
			*d = pgtype.Date{}
			if t, ok := v.(time.Time); ok {
				*d = pgtype.Date{Time: t, Valid: true}
			}
		default:
			return fmt.Errorf("scan: unexpected dest %T", d)
		}
	}
	return nil
}

func (r *recRows) Close()     {}
func (r *recRows) Err() error { return r.err }

type recCall struct {
	sql      string
	args     []any
	receipt  []any
	deadline time.Time
}

// recDB answers statements in order from results and remembers them. A nil
// results list fails the test on any call: it proves "no I/O".
type recDB struct {
	t       *testing.T
	noIO    bool
	results []*recRows
	errs    []error
	calls   []recCall
}

func (f *recDB) query(ctx context.Context, receipt []any, sql string, args ...any) (pgx.Rows, error) {
	if f.noIO {
		f.t.Fatalf("I/O on a call that must be refused first: %s", sql)
	}
	dl, _ := ctx.Deadline()
	f.calls = append(f.calls, recCall{sql: sql, args: args, receipt: receipt, deadline: dl})
	i := len(f.calls) - 1
	if i < len(f.errs) && f.errs[i] != nil {
		return nil, f.errs[i]
	}
	if i < len(f.results) {
		return f.results[i], nil
	}
	return &recRows{}, nil
}

var srcPipeline = owners.Pipeline{ID: "orders", SourceTables: []string{"public.orders"}, SinkTables: []string{"analytics.orders"}}

func scoped() context.Context { return tools.WithPipeline(context.Background(), srcPipeline) }

func newRec(f *recDB, sink bool) Reconcile {
	return Reconcile{Name: "postgres", Sink: sink, Query: f.query}
}

var (
	winFrom = time.Date(2026, 10, 22, 9, 0, 0, 0, time.FixedZone("CEST", 2*3600))
	winTo   = winFrom.Add(time.Hour)
)

// TC1: the window count, identifiers quoted, bounds as parameters in UTC.
func TestCountBetweenQuery(t *testing.T) {
	if got, want := countBetweenSQL(pgx.Identifier{"s", "t"}, "ts"), `SELECT count(*) FROM "s"."t" WHERE "ts" >= $1 AND "ts" < $2`; got != want {
		t.Fatalf("sql = %s\nwant  %s", got, want)
	}
	f := &recDB{t: t, results: []*recRows{{oids: []uint32{pgtype.Int8OID}, rows: [][]any{{int64(100)}}}}}
	n, err := newRec(f, false).CountBetween(scoped(), "public.orders", "updated_at", winFrom, winTo)
	if err != nil || n != 100 {
		t.Fatalf("CountBetween = %d, %v", n, err)
	}
	c := f.calls[0]
	if c.sql != `SELECT count(*) FROM "public"."orders" WHERE "updated_at" >= $1 AND "updated_at" < $2` {
		t.Errorf("sql = %s", c.sql)
	}
	from, to := c.args[0].(time.Time), c.args[1].(time.Time)
	if !from.Equal(winFrom) || from.Location() != time.UTC || !to.Equal(winTo) || to.Location() != time.UTC {
		t.Errorf("args = %v, want the window in UTC", c.args)
	}
	if c.deadline.IsZero() || time.Until(c.deadline) > clients.ReconcileTimeout {
		t.Errorf("deadline %v: want at most %s from now", c.deadline, clients.ReconcileTimeout)
	}
}

func TestCountBetweenWindow(t *testing.T) {
	f := &recDB{t: t, noIO: true}
	if _, err := newRec(f, false).CountBetween(scoped(), "public.orders", "updated_at", winTo, winFrom); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("reversed window: err = %v, want ErrBadInput", err)
	}
	if n, err := newRec(f, false).CountBetween(scoped(), "public.orders", "updated_at", winFrom, winFrom); err != nil || n != 0 {
		t.Fatalf("empty window = %d, %v; want 0 with no I/O", n, err)
	}
}

// TC2: bad identifiers are ErrBadInput and nothing is sent.
func TestReconcileBadIdentifiers(t *testing.T) {
	long := strings.Repeat("c", 64)
	cases := []struct {
		name, table, pk, ts string
		pkOnly              bool // CountBetween takes no key column
	}{
		{"injection", "orders; drop", "id", "updated_at", false},
		{"schema injection", "public.orders; drop", "id", "updated_at", false},
		{"three parts", "a.b.c", "id", "updated_at", false},
		{"no schema", "orders", "id", "updated_at", false},
		{"empty table", "", "id", "updated_at", false},
		{"quoted", `"x"`, "id", "updated_at", false},
		{"quoted part", `public."x"`, "id", "updated_at", false},
		{"empty column", "public.orders", "id", "", false},
		{"bad ts", "public.orders", "id", "updated_at = 1", false},
		{"too long", "public.orders", "id", long, false},
		{"bad pk", "public.orders", "id; drop", "updated_at", true},
		{"empty pk", "public.orders", "", "updated_at", true},
		{"quoted pk", "public.orders", `"x"`, "updated_at", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newRec(&recDB{t: t, noIO: true}, false)
			ctx := tools.WithUnscoped(context.Background())
			if !c.pkOnly {
				if _, err := r.CountBetween(ctx, c.table, c.ts, winFrom, winTo); !errors.Is(err, tools.ErrBadInput) {
					t.Errorf("CountBetween err = %v, want ErrBadInput", err)
				}
			}
			if _, err := r.SampleKeys(ctx, c.table, c.pk, c.ts, winFrom, 10); !errors.Is(err, tools.ErrBadInput) {
				t.Errorf("SampleKeys err = %v, want ErrBadInput", err)
			}
			if _, err := r.RowTimes(ctx, c.table, c.pk, []string{"1"}, []string{c.ts}); !errors.Is(err, tools.ErrBadInput) {
				t.Errorf("RowTimes err = %v, want ErrBadInput", err)
			}
		})
	}
}

// TC3: SampleKeys cuts n to the cap; n <= 0 and too many keys are refused.
func TestReconcileCaps(t *testing.T) {
	f := &recDB{t: t, results: []*recRows{{oids: []uint32{pgtype.TextOID}, rows: [][]any{{"3"}, {"2"}}}}}
	keys, err := newRec(f, false).SampleKeys(scoped(), "public.orders", "id", "updated_at", winFrom, 500)
	if err != nil || strings.Join(keys, ",") != "3,2" {
		t.Fatalf("SampleKeys = %v, %v", keys, err)
	}
	c := f.calls[0]
	if c.args[1] != clients.ReconcileMaxSample {
		t.Errorf("limit = %v, want %d", c.args[1], clients.ReconcileMaxSample)
	}
	want := `SELECT "id"::text FROM "public"."orders" WHERE "updated_at" >= $1 AND "id" IS NOT NULL ORDER BY "updated_at" DESC, "id" DESC LIMIT $2`
	if c.sql != want {
		t.Errorf("sql = %s\nwant  %s", c.sql, want)
	}
	if since := c.args[0].(time.Time); since.Location() != time.UTC || !since.Equal(winFrom) {
		t.Errorf("since = %v, want UTC", since)
	}

	r := newRec(&recDB{t: t, noIO: true}, false)
	for _, n := range []int{0, -1} {
		if _, err := r.SampleKeys(scoped(), "public.orders", "id", "updated_at", winFrom, n); !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("SampleKeys(n=%d) err = %v, want ErrBadInput", n, err)
		}
	}
	keys1001 := make([]string, clients.ReconcileMaxKeys+1)
	if _, err := r.RowTimes(scoped(), "public.orders", "id", keys1001, []string{"updated_at"}); !errors.Is(err, tools.ErrBadInput) {
		t.Errorf("RowTimes(1001 keys) err = %v, want ErrBadInput", err)
	}
	if _, err := r.RowTimes(scoped(), "public.orders", "id", []string{"1"}, nil); !errors.Is(err, tools.ErrBadInput) {
		t.Errorf("RowTimes(no columns) err = %v, want ErrBadInput", err)
	}
	if got, err := r.RowTimes(scoped(), "public.orders", "id", nil, []string{"updated_at"}); err != nil || len(got) != 0 || got == nil {
		t.Errorf("RowTimes(no keys) = %v, %v; want an empty map, no I/O", got, err)
	}
}

// TC4: a table outside the pipeline, or no scope at all, is refused first.
func TestReconcileScope(t *testing.T) {
	cases := []struct {
		name  string
		ctx   context.Context
		sink  bool
		table string
		want  error
	}{
		{"other pipeline's table", scoped(), false, "public.customers", tools.ErrOutOfScope},
		{"sink table on the source side", scoped(), false, "analytics.orders", tools.ErrOutOfScope},
		{"source table on the sink side", scoped(), true, "public.orders", tools.ErrOutOfScope},
		{"no scope", context.Background(), false, "public.orders", tools.ErrNoScope},
		{"pipeline id only", tools.WithPipelineID(context.Background(), "orders"), false, "public.orders", tools.ErrNoScope},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newRec(&recDB{t: t, noIO: true}, c.sink)
			if _, err := r.CountBetween(c.ctx, c.table, "updated_at", winFrom, winTo); !errors.Is(err, c.want) {
				t.Errorf("CountBetween err = %v, want %v", err, c.want)
			}
			if _, err := r.SampleKeys(c.ctx, c.table, "id", "updated_at", winFrom, 10); !errors.Is(err, c.want) {
				t.Errorf("SampleKeys err = %v, want %v", err, c.want)
			}
			if _, err := r.RowTimes(c.ctx, c.table, "id", []string{"1"}, []string{"updated_at"}); !errors.Is(err, c.want) {
				t.Errorf("RowTimes err = %v, want %v", err, c.want)
			}
		})
	}
	// The sink side reads sink_tables.
	f := &recDB{t: t, results: []*recRows{{rows: [][]any{{int64(7)}}}}}
	if n, err := newRec(f, true).CountBetween(scoped(), "analytics.orders", "updated_at", winFrom, winTo); err != nil || n != 7 {
		t.Fatalf("sink CountBetween = %d, %v", n, err)
	}
}

// catalog answers columnTypesSQL for the named columns.
func catalog(cols ...[]any) *recRows {
	return &recRows{oids: []uint32{pgtype.NameOID, pgtype.Int8OID, pgtype.NameOID, pgtype.NameOID}, rows: cols}
}

func col(name string, oid uint32, typ string) []any {
	return []any{name, int64(oid), "pg_catalog", typ}
}

// TC5: RowTimes selects the key and the named timestamp columns only; the
// table's email column is never selected and never returned.
func TestRowTimesSelectsOnlyKeyAndTimes(t *testing.T) {
	created := time.Date(2026, 10, 22, 7, 0, 0, 0, time.FixedZone("X", 3600))
	updated := created.Add(time.Minute)
	f := &recDB{t: t, results: []*recRows{
		catalog(col("id", pgtype.Int8OID, "int8"), col("updated_at", pgtype.TimestamptzOID, "timestamptz"), col("created_on", pgtype.TimestampOID, "timestamp")),
		{
			oids: []uint32{pgtype.TextOID, pgtype.TimestamptzOID, pgtype.TimestampOID},
			rows: [][]any{
				{"1", updated, created},
				{"2", nil, created},         // NULL column: absent
				{"3", pgtype.Infinity, nil}, // infinity and NULL: an empty row map
			},
		},
	}}
	got, err := newRec(f, false).RowTimes(scoped(), "public.orders", "id", []string{"1", "2", "3", "4", "1"}, []string{"updated_at", "created_on", "updated_at"})
	if err != nil {
		t.Fatalf("RowTimes: %v", err)
	}
	if len(f.calls) != 2 {
		t.Fatalf("%d statements, want catalog + read", len(f.calls))
	}
	if cat := f.calls[0]; cat.sql != columnTypesSQL || strings.Join(cat.args[2].([]string), ",") != "id,updated_at,created_on" {
		t.Errorf("catalog call = %s %v", cat.sql, cat.args)
	}
	read := f.calls[1]
	want := `SELECT "id"::text, "updated_at", "created_on" FROM "public"."orders" WHERE "id" = ANY($1::text[]::"pg_catalog"."int8"[]) LIMIT $2`
	if read.sql != want {
		t.Errorf("sql = %s\nwant  %s", read.sql, want)
	}
	if strings.Contains(read.sql, "*") || strings.Contains(read.sql, "email") {
		t.Errorf("selects other columns: %s", read.sql)
	}
	if keys := read.args[0].([]string); strings.Join(keys, ",") != "1,2,3,4" || read.args[1] != 5 {
		t.Errorf("args = %v, want distinct keys and limit 5", read.args)
	}
	if fmt.Sprint(read.receipt) != "[<4 keys> 5]" {
		t.Errorf("receipt = %v, want the key count only", read.receipt)
	}
	if len(got) != 3 {
		t.Fatalf("got %d keys, want 3 (key 4 has no row): %v", len(got), got)
	}
	if r1 := got["1"]; len(r1) != 2 || !r1["updated_at"].Equal(updated) || r1["updated_at"].Location() != time.UTC || !r1["created_on"].Equal(created) {
		t.Errorf("row 1 = %v", r1)
	}
	if r2 := got["2"]; len(r2) != 1 || r2["updated_at"] != (time.Time{}) {
		t.Errorf("row 2 = %v, want only created_on", r2)
	}
	if r3, ok := got["3"]; !ok || len(r3) != 0 {
		t.Errorf("row 3 = %v, %v; want present and empty", r3, ok)
	}
	for k, row := range got {
		for c := range row {
			if c != "updated_at" && c != "created_on" {
				t.Errorf("key %s returns column %q", k, c)
			}
		}
	}
}

func TestRowTimesRefusesNonTimeColumn(t *testing.T) {
	f := &recDB{t: t, results: []*recRows{catalog(col("id", pgtype.Int8OID, "int8"), col("email", pgtype.TextOID, "text"))}}
	_, err := newRec(f, false).RowTimes(scoped(), "public.orders", "id", []string{"1"}, []string{"email"})
	if !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("err = %v, want ErrBadInput", err)
	}
	if len(f.calls) != 1 {
		t.Fatalf("read rows after refusing the column: %d statements", len(f.calls))
	}
}

func TestRowTimesTypeChangedDuringRead(t *testing.T) {
	f := &recDB{t: t, results: []*recRows{
		catalog(col("id", pgtype.Int8OID, "int8"), col("updated_at", pgtype.TimestamptzOID, "timestamptz")),
		{oids: []uint32{pgtype.TextOID, pgtype.TextOID}, rows: [][]any{{"1", "secret@example.com"}}},
	}}
	_, err := newRec(f, false).RowTimes(scoped(), "public.orders", "id", []string{"1"}, []string{"updated_at"})
	if !errors.Is(err, tools.ErrBadInput) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("err = %v, want ErrBadInput without the value", err)
	}
}

func TestRowTimesMissingColumnAndNonUniqueKey(t *testing.T) {
	f := &recDB{t: t, results: []*recRows{catalog(col("id", pgtype.Int8OID, "int8"))}}
	if _, err := newRec(f, false).RowTimes(scoped(), "public.orders", "id", []string{"1"}, []string{"gone_at"}); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing column: err = %v, want ErrNotFound", err)
	}

	ts := time.Date(2026, 10, 22, 7, 0, 0, 0, time.UTC)
	f = &recDB{t: t, results: []*recRows{
		catalog(col("tenant", pgtype.Int8OID, "int8"), col("updated_at", pgtype.TimestamptzOID, "timestamptz")),
		{oids: []uint32{pgtype.TextOID, pgtype.TimestamptzOID}, rows: [][]any{{"1", ts}, {"1", ts}}},
	}}
	if _, err := newRec(f, false).RowTimes(scoped(), "public.orders", "tenant", []string{"1"}, []string{"updated_at"}); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("non-unique key: err = %v, want ErrBadInput", err)
	}
}

// E5: backend errors are wrapped; a value in a data exception never shows.
func TestReconcileErrors(t *testing.T) {
	down := errors.New("dial tcp: connection refused")
	f := &recDB{t: t, errs: []error{down}}
	if _, err := newRec(f, false).CountBetween(scoped(), "public.orders", "updated_at", winFrom, winTo); !errors.Is(err, down) {
		t.Errorf("down: err = %v", err)
	}

	f = &recDB{t: t, errs: []error{&pgconn.PgError{Code: "42P01", Message: `relation "public.orders" does not exist`}}}
	if _, err := newRec(f, false).SampleKeys(scoped(), "public.orders", "id", "updated_at", winFrom, 5); !errors.Is(err, clients.ErrNotFound) {
		t.Errorf("missing table: err = %v, want ErrNotFound", err)
	}

	f = &recDB{t: t, results: []*recRows{catalog(col("id", pgtype.Int8OID, "int8"), col("updated_at", pgtype.TimestamptzOID, "timestamptz"))},
		errs: []error{nil, &pgconn.PgError{Code: "22P02", Message: `invalid input syntax for type bigint: "alice@example.com"`}}}
	_, err := newRec(f, false).RowTimes(scoped(), "public.orders", "id", []string{"alice@example.com"}, []string{"updated_at"})
	if !errors.Is(err, tools.ErrBadInput) || strings.Contains(err.Error(), "alice") {
		t.Errorf("bad key: err = %v, want ErrBadInput without the key", err)
	}

	f = &recDB{t: t, results: []*recRows{{rows: nil, err: errors.New("stream cut")}}}
	if _, err := newRec(f, false).CountBetween(scoped(), "public.orders", "updated_at", winFrom, winTo); err == nil || !strings.Contains(err.Error(), "stream cut") {
		t.Errorf("rows error = %v", err)
	}
}

// E4: a call cut by its deadline wraps context.DeadlineExceeded, even when
// the driver reports the server's cancel instead.
func TestReconcileDeadline(t *testing.T) {
	r := Reconcile{Name: "postgres", Query: func(ctx context.Context, _ []any, _ string, _ ...any) (pgx.Rows, error) {
		<-ctx.Done()
		return nil, &pgconn.PgError{Code: "57014", Message: "canceling statement due to user request"}
	}}
	ctx, cancel := context.WithTimeout(scoped(), 20*time.Millisecond)
	defer cancel()
	_, err := r.CountBetween(ctx, "public.orders", "updated_at", winFrom, winTo)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}
