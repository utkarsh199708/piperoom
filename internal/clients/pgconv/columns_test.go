package pgconv

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

func colsRows(rows ...[]any) *recRows { return &recRows{rows: rows} }

// X7a E1: the catalog read returns names, types, positions and nullability
// in ordinal order; the table goes in as parameters, the cap as $3, and the
// call has a deadline.
func TestColumns(t *testing.T) {
	f := &recDB{t: t, results: []*recRows{colsRows(
		[]any{"id", "bigint", int64(1), false},
		[]any{"customer_email", "text", int64(2), true},
		[]any{"updated_at", "timestamp with time zone", int64(4), false},
	)}}
	got, err := newRec(f, false).Columns(scoped(), "public.orders")
	if err != nil {
		t.Fatal(err)
	}
	want := []clients.ColumnInfo{
		{Name: "id", Type: "bigint", Position: 1},
		{Name: "customer_email", Type: "text", Position: 2, Nullable: true},
		{Name: "updated_at", Type: "timestamp with time zone", Position: 4},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Columns = %+v\nwant %+v", got, want)
	}
	c := f.calls[0]
	if c.sql != columnsSQL || !slices.Equal(c.args, []any{"public", "orders", clients.MaxTableColumns}) || c.deadline.IsZero() {
		t.Fatalf("call = %+v", c)
	}
	if time.Until(c.deadline) > clients.CatalogTimeout {
		t.Fatalf("deadline %v is past CatalogTimeout", time.Until(c.deadline))
	}
	// Catalog only: never a default, a comment or the table itself.
	for _, bad := range []string{"pg_attrdef", "adbin", "description", "information_schema", `"public"."orders"`} {
		if strings.Contains(columnsSQL, bad) {
			t.Fatalf("columnsSQL reads %q", bad)
		}
	}

	// The sink side checks sink_tables.
	f = &recDB{t: t, results: []*recRows{colsRows([]any{"id", "bigint", int64(1), false})}}
	if _, err := newRec(f, true).Columns(scoped(), "analytics.orders"); err != nil {
		t.Fatalf("sink: %v", err)
	}
}

// A table with no columns is an empty list (the LEFT JOIN's NULL row); no
// row at all is ErrNotFound.
func TestColumnsEmptyAndMissing(t *testing.T) {
	f := &recDB{t: t, results: []*recRows{colsRows([]any{"", "", int64(0), false})}}
	got, err := newRec(f, false).Columns(scoped(), "public.orders")
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("no columns: %+v, %v", got, err)
	}
	f = &recDB{t: t, results: []*recRows{{}}}
	if _, err := newRec(f, false).Columns(scoped(), "public.orders"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing table: %v", err)
	}
	f = &recDB{t: t, errs: []error{&pgconn.PgError{Code: "42P01", Message: "relation does not exist"}}}
	if _, err := newRec(f, false).Columns(scoped(), "public.orders"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("42P01: %v", err)
	}
}

// E2, E3: a bad name, no scope or another pipeline's table is refused before
// any I/O; the source side does not accept a sink table and the other way.
func TestColumnsRefusesBeforeIO(t *testing.T) {
	for name, tc := range map[string]struct {
		ctx   context.Context
		sink  bool
		table string
		want  error
	}{
		"bad table":         {scoped(), false, "orders", tools.ErrBadInput},
		"injection":         {scoped(), false, `public.orders"; drop table x; --`, tools.ErrBadInput},
		"long name":         {scoped(), false, "public." + strings.Repeat("a", 64), tools.ErrBadInput},
		"no scope":          {context.Background(), false, "public.orders", tools.ErrNoScope},
		"id only":           {tools.WithPipelineID(context.Background(), "orders"), false, "public.orders", tools.ErrNoScope},
		"other table":       {scoped(), false, "public.payroll", tools.ErrOutOfScope},
		"sink as source":    {scoped(), false, "analytics.orders", tools.ErrOutOfScope},
		"source as sink":    {scoped(), true, "public.orders", tools.ErrOutOfScope},
		"other sink tables": {scoped(), true, "analytics.payroll", tools.ErrOutOfScope},
	} {
		t.Run(name, func(t *testing.T) {
			f := &recDB{t: t, noIO: true}
			_, err := newRec(f, tc.sink).Columns(tc.ctx, tc.table)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
	// The explicit opt-out passes.
	f := &recDB{t: t, results: []*recRows{colsRows([]any{"id", "bigint", int64(1), false})}}
	if _, err := newRec(f, false).Columns(tools.WithUnscoped(context.Background()), "public.anything"); err != nil {
		t.Fatalf("unscoped: %v", err)
	}
}

// E4, E5: a cut call wraps DeadlineExceeded; a backend error is wrapped with
// the client's name, never swallowed.
func TestColumnsErrors(t *testing.T) {
	r := Reconcile{Name: "warehouse", Sink: true, Query: func(ctx context.Context, _ []any, _ string, _ ...any) (pgx.Rows, error) {
		<-ctx.Done()
		return nil, &pgconn.PgError{Code: "57014", Message: "canceling statement due to user request"}
	}}
	ctx, cancel := context.WithTimeout(scoped(), 20*time.Millisecond)
	defer cancel()
	if _, err := r.Columns(ctx, "analytics.orders"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	boom := errors.New("connection reset")
	f := &recDB{t: t, errs: []error{boom}}
	_, err := newRec(f, false).Columns(scoped(), "public.orders")
	if !errors.Is(err, boom) || !strings.HasPrefix(err.Error(), "postgres: columns of public.orders") {
		t.Fatalf("err = %v", err)
	}
	f = &recDB{t: t, results: []*recRows{{err: boom}}}
	if _, err := newRec(f, false).Columns(scoped(), "public.orders"); !errors.Is(err, boom) {
		t.Fatalf("rows err = %v", err)
	}
}

// X7c TC3: the catalog read behind the PII tags names only catalog relations
// after FROM and JOIN, and selects names, type names, positions and
// nullability: never a table of the pipeline, so no row value can be read.
func TestColumnsSQLCatalogOnly(t *testing.T) {
	words := strings.Fields(columnsSQL)
	var rels []string
	for i, w := range words[:len(words)-1] {
		if u := strings.ToUpper(w); u == "FROM" || u == "JOIN" {
			rels = append(rels, words[i+1])
		}
	}
	want := []string{"pg_catalog.pg_class", "pg_catalog.pg_namespace", "pg_catalog.pg_attribute"}
	if !slices.Equal(rels, want) {
		t.Fatalf("columnsSQL reads %v, want %v", rels, want)
	}
	sel, _, _ := strings.Cut(columnsSQL, "FROM")
	for _, col := range []string{"attname", "format_type", "attnum", "attnotnull"} {
		if !strings.Contains(sel, col) {
			t.Fatalf("select list lost %s: %s", col, sel)
		}
	}
	// Four output columns: three commas outside parentheses.
	depth, top := 0, 0
	for _, r := range sel {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				top++
			}
		}
	}
	if top != 3 {
		t.Fatalf("select list has %d columns, want 4: %s", top+1, sel)
	}
}
