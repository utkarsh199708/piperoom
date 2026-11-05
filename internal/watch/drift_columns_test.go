package watch

// X7a gave clients.ColumnReader real implementations (pgconv.Reconcile.Columns
// on the source Postgres and the dev Postgres warehouse). These tests run the
// watcher's row_drift column checks (X1b's timeColumns, X1b-fix's
// structurePart) through that real reader, over a fake pg_catalog answer
// spelled as format_type spells types, so the scope it checks, the type names
// it returns and the errors it wraps are the production ones.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
)

// catalogCol is one pg_attribute row as columnsSQL returns it.
type catalogCol struct {
	name, typ string
	nullable  bool
}

// fakeCatalog answers pgconv's catalog query for one table and records the
// relations it was asked for.
type fakeCatalog struct {
	mu     sync.Mutex
	schema string
	table  string
	cols   []catalogCol
	asked  []string // "schema.table" per query
}

func (c *fakeCatalog) query(ctx context.Context, _ []any, sql string, args ...any) (pgx.Rows, error) {
	if !strings.Contains(sql, "pg_catalog.pg_attribute") || len(args) != 3 {
		return nil, fmt.Errorf("fake catalog: unexpected query %q", sql)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rel := fmt.Sprint(args[0], ".", args[1])
	c.asked = append(c.asked, rel)
	r := &catalogRows{}
	if args[0] != c.schema || args[1] != c.table {
		return r, nil // no such relation: no row at all
	}
	for i, col := range c.cols {
		r.rows = append(r.rows, []any{col.name, col.typ, int64(i + 1), col.nullable})
	}
	return r, nil
}

func (c *fakeCatalog) drop(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cols = slices.DeleteFunc(c.cols, func(col catalogCol) bool { return col.name == name })
}

func (c *fakeCatalog) relations() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.asked)
}

// catalogRows is a pgx.Rows over fixed rows of (text, text, int8, bool), as
// pgconv's own fake: the methods Columns does not call panic.
type catalogRows struct {
	pgx.Rows
	rows [][]any
	i    int
}

func (r *catalogRows) Close()     {}
func (r *catalogRows) Err() error { return nil }
func (r *catalogRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *catalogRows) Scan(dest ...any) error {
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return errors.New("catalog rows: scan arity")
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = row[i].(string)
		case *int64:
			*p = row[i].(int64)
		case *bool:
			*p = row[i].(bool)
		default:
			return fmt.Errorf("catalog rows: scan into %T", d)
		}
	}
	return nil
}

// pgColsSource is the model source with the real catalog reader, recording
// the columns row_drift asks RowTimes for.
type pgColsSource struct {
	modelSource
	cat     pgconv.Reconcile
	mu      *sync.Mutex
	rowCols *[][]string
}

func (s pgColsSource) Columns(ctx context.Context, table string) ([]clients.ColumnInfo, error) {
	return s.cat.Columns(ctx, table)
}

func (s pgColsSource) RowTimes(ctx context.Context, table, pk string, keys, cols []string) (map[string]map[string]time.Time, error) {
	s.mu.Lock()
	*s.rowCols = append(*s.rowCols, slices.Clone(cols))
	s.mu.Unlock()
	return s.modelSource.RowTimes(ctx, table, pk, keys, cols)
}

// pgColsSink is the model sink with the real catalog reader.
type pgColsSink struct {
	*modelDB
	cat pgconv.Reconcile
}

func (s pgColsSink) Columns(ctx context.Context, table string) ([]clients.ColumnInfo, error) {
	return s.cat.Columns(ctx, table)
}

func TestRowDriftWithRealColumnReader(t *testing.T) {
	h := newModelHarness(t)
	src := &fakeCatalog{schema: "public", table: "orders", cols: []catalogCol{
		{"id", "bigint", false},
		{"customer_id", "bigint", true},
		{"amount", "numeric", true},
		{"status", "text", true},
		{mTS, "timestamp with time zone", false},
		{"created_at", "timestamp without time zone", true},
		{"shipped_on", "date", true},
		{"email_verified_at", "timestamp with time zone", true}, // looks like personal data
		{"Odd Name", "timestamp with time zone", true},          // not a plain identifier
		{"tags", "text[]", true},
	}}
	snk := &fakeCatalog{schema: "analytics", table: "orders", cols: slices.Concat(src.cols,
		[]catalogCol{{"loaded_at", "timestamp with time zone", true}})}
	var mu sync.Mutex
	var rowCols [][]string
	d := h.deps
	d.Source = pgColsSource{
		modelSource: d.Source.(modelSource),
		cat:         pgconv.Reconcile{Name: "postgres", Query: src.query},
		mu:          &mu, rowCols: &rowCols,
	}
	d.Warehouse = pgColsSink{modelDB: h.m.snk, cat: pgconv.Reconcile{Name: "warehouse", Sink: true, Query: snk.query}}
	w, err := New(d)
	if err != nil {
		t.Fatal(err)
	}
	h.w, h.deps = w, d

	h.run(2, true)
	h.wantQuiet()
	// The watcher's ctx passes the reader's own scope check: both tables were
	// read, each on its side, and nothing else.
	if got := src.relations(); len(got) == 0 || slices.ContainsFunc(got, func(r string) bool { return r != mSource }) {
		t.Fatalf("source catalog reads %v, want %s only", got, mSource)
	}
	if got := snk.relations(); len(got) == 0 || slices.ContainsFunc(got, func(r string) bool { return r != mSink }) {
		t.Fatalf("sink catalog reads %v, want %s only", got, mSink)
	}
	// X1b's compare reads the date/time columns both sides have, as
	// format_type names them, never a personal-data or odd name.
	mu.Lock()
	first := slices.Clone(rowCols[0])
	mu.Unlock()
	if want := []string{mTS, "created_at", "shipped_on"}; !slices.Equal(first, want) {
		t.Fatalf("RowTimes columns %v, want %v", first, want)
	}

	// X1b-fix: a column both sides had, gone from the sink, is a row_drift
	// naming it, with counts 0 and an empty window.
	snk.drop("status")
	h.step(15 * time.Minute)
	h.wantFired(rowID)
	ev := h.evidence(rowID)
	if d, _ := ev["detail"].(string); !strings.Contains(d, "columns missing on the sink: status") {
		t.Fatalf("detail %q", d)
	}
	win, _ := ev["window"].(map[string]any)
	if ev["missing"] != float64(0) || ev["mismatched"] != float64(0) || win == nil || win["from"] != win["to"] {
		t.Fatalf("structural evidence %v, want counts 0 and from = to", ev)
	}
}

// A sink table the catalog no longer has is an unknown reading (the reader's
// ErrNotFound), never a "column missing" breach.
func TestRowDriftRealColumnReaderMissingSinkTable(t *testing.T) {
	h := newModelHarness(t)
	src := &fakeCatalog{schema: "public", table: "orders", cols: []catalogCol{{"id", "bigint", false}, {mTS, "timestamp with time zone", false}}}
	snk := &fakeCatalog{schema: "analytics", table: "gone"}
	d := h.deps
	var mu sync.Mutex
	var rowCols [][]string
	d.Source = pgColsSource{modelSource: d.Source.(modelSource), cat: pgconv.Reconcile{Name: "postgres", Query: src.query}, mu: &mu, rowCols: &rowCols}
	d.Warehouse = pgColsSink{modelDB: h.m.snk, cat: pgconv.Reconcile{Name: "warehouse", Sink: true, Query: snk.query}}
	w, err := New(d)
	if err != nil {
		t.Fatal(err)
	}
	h.w, h.deps = w, d
	h.run(2, false)
	if slices.Contains(h.fired(), rowID) {
		t.Fatalf("a missing sink table fired row_drift\n%s", h.logs)
	}
	if h.logs.count("check failed, signal state unchanged") == 0 {
		t.Fatalf("want the unknown reading logged\n%s", h.logs)
	}
}

// structurePart quotes a catalog name that is not a plain identifier, so a
// name with a comma or a bidirectional control reads as one name; and a sink
// that folds names still has sink_ts_column.
func TestStructurePartNames(t *testing.T) {
	cols := func(names ...string) []clients.ColumnInfo {
		var out []clients.ColumnInfo
		for _, n := range names {
			out = append(out, clients.ColumnInfo{Name: n})
		}
		return out
	}
	pr := tablePair{mSource, mSink}
	mem := newDriftMem()
	odd := "a,b\u202eevil"
	structurePart(t0, mem, pr, mTS, &catalogs{src: cols("id", odd, mTS), snk: cols("id", odd, mTS)})
	p, tsGone := structurePart(t0, mem, pr, mTS, &catalogs{src: cols("id", odd, mTS), snk: cols("id", mTS)})
	if tsGone || p.v != vBreach || !strings.Contains(p.detail, `"a,b\u202eevil"`) || strings.ContainsRune(p.label, '\u202e') {
		t.Fatalf("odd name: %+v (tsGone %v)", p, tsGone)
	}

	p, tsGone = structurePart(t0, newDriftMem(), pr, mTS, &catalogs{src: cols("id", mTS), snk: cols("ID", strings.ToUpper(mTS))})
	if tsGone || p.v != vClear {
		t.Fatalf("folded sink names: %+v (tsGone %v), want clear", p, tsGone)
	}
	p, tsGone = structurePart(t0, newDriftMem(), pr, mTS, &catalogs{src: cols("id", mTS), snk: cols("id")})
	if !tsGone || p.v != vBreach || p.detail != "columns missing on the sink: "+mTS {
		t.Fatalf("sink_ts_column gone: %+v (tsGone %v)", p, tsGone)
	}
}
