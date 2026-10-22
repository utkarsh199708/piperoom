package pgconv

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

func colRow(schema, name string, oid uint32, pk bool) *recRows {
	return &recRows{rows: [][]any{{schema, name, int64(oid), pk}}}
}

func countRow(n int64) *recRows {
	return &recRows{oids: []uint32{pgtype.Int8OID}, rows: [][]any{{n}}}
}

func TestRangeKind(t *testing.T) {
	for oid, want := range map[uint32]string{
		pgtype.Int2OID: RangeKindInteger, pgtype.Int4OID: RangeKindInteger, pgtype.Int8OID: RangeKindInteger,
		pgtype.TextOID: RangeKindText, pgtype.VarcharOID: RangeKindText, pgtype.BPCharOID: RangeKindText,
		pgtype.UUIDOID: RangeKindUUID, pgtype.DateOID: RangeKindTime, pgtype.TimestampOID: RangeKindTime,
		pgtype.TimestamptzOID: RangeKindTime, pgtype.NumericOID: "", pgtype.JSONBOID: "", pgtype.BoolOID: "",
	} {
		if got := rangeKind(oid); got != want {
			t.Errorf("rangeKind(%d) = %q, want %q", oid, got, want)
		}
	}
}

func TestRangeColumn(t *testing.T) {
	f := &recDB{t: t, results: []*recRows{colRow("pg_catalog", "int8", pgtype.Int8OID, true)}}
	rc, err := newRec(f, false).RangeColumn(scoped(), "public.orders", "id")
	if err != nil {
		t.Fatal(err)
	}
	if rc != (RangeColumn{Type: "int8", Kind: RangeKindInteger, PrimaryKey: true}) {
		t.Fatalf("rc = %+v", rc)
	}
	if c := f.calls[0]; !slices.Equal(c.args, []any{"public", "orders", "id"}) || c.deadline.IsZero() {
		t.Fatalf("call = %+v", c)
	}

	// No row: the column (or table) does not exist.
	f = &recDB{t: t, results: []*recRows{{}}}
	if _, err := newRec(f, false).RangeColumn(scoped(), "public.orders", "nope"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing column: %v", err)
	}
}

func TestRangeColumnRefusesBeforeIO(t *testing.T) {
	for name, tc := range map[string]struct {
		ctx           context.Context
		table, column string
		want          error
	}{
		"bad table":    {scoped(), "orders", "id", tools.ErrBadInput},
		"bad column":   {scoped(), "public.orders", `id"; drop`, tools.ErrBadInput},
		"no scope":     {context.Background(), "public.orders", "id", tools.ErrNoScope},
		"out of scope": {scoped(), "public.refunds", "id", tools.ErrOutOfScope},
	} {
		t.Run(name, func(t *testing.T) {
			f := &recDB{t: t, noIO: true}
			if _, err := newRec(f, false).RangeColumn(tc.ctx, tc.table, tc.column); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if _, err := newRec(f, false).CountRange(tc.ctx, tc.table, tc.column, "1", "2"); !errors.Is(err, tc.want) {
				t.Fatalf("CountRange err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestCountRangeQuery(t *testing.T) {
	if got, want := countRangeSQL(pgx.Identifier{"public", "orders"}, "id", pgx.Identifier{"pg_catalog", "int8"}, true),
		`SELECT count(*) FROM "public"."orders" WHERE "id" >= $1::text::"pg_catalog"."int8" AND "id" <= $2::text::"pg_catalog"."int8"`; got != want {
		t.Fatalf("sql = %s\nwant  %s", got, want)
	}
	f := &recDB{t: t, results: []*recRows{colRow("pg_catalog", "int8", pgtype.Int8OID, true), countRow(412)}}
	n, err := newRec(f, false).CountRange(scoped(), "public.orders", "id", "100", "511")
	if err != nil || n != 412 {
		t.Fatalf("n, err = %d, %v", n, err)
	}
	c := f.calls[1]
	if !slices.Equal(c.args, []any{"100", "511"}) || !slices.Equal(c.receipt, []any{"<bound>", "<bound>"}) || c.deadline.IsZero() {
		t.Fatalf("count call = %+v: the receipt must never show a bound", c)
	}

	// No upper bound: one parameter.
	f = &recDB{t: t, results: []*recRows{colRow("pg_catalog", "timestamptz", pgtype.TimestamptzOID, false), countRow(7)}}
	n, err = newRec(f, true).CountRange(tools.WithPipeline(context.Background(), srcPipeline), "analytics.orders", "updated_at", "2026-10-22T09:00:00Z", "")
	if err != nil || n != 7 {
		t.Fatalf("since: n, err = %d, %v", n, err)
	}
	if c := f.calls[1]; len(c.args) != 1 || strings.Contains(c.sql, "<=") {
		t.Fatalf("since call = %+v", c)
	}
}

func TestCountRangeRefusals(t *testing.T) {
	// A type no condition is built on.
	f := &recDB{t: t, results: []*recRows{colRow("pg_catalog", "jsonb", pgtype.JSONBOID, false)}}
	if _, err := newRec(f, false).CountRange(scoped(), "public.orders", "doc", "1", ""); !errors.Is(err, tools.ErrBadInput) || len(f.calls) != 1 {
		t.Fatalf("jsonb column: %v (%d calls)", err, len(f.calls))
	}
	// No lower bound: refused before I/O.
	if _, err := newRec(&recDB{t: t, noIO: true}, false).CountRange(scoped(), "public.orders", "id", "", "2"); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("no lower bound: %v", err)
	}
	// A bound that is not a value of the type: a fixed phrase, never the value.
	f = &recDB{t: t, results: []*recRows{colRow("pg_catalog", "int8", pgtype.Int8OID, true)},
		errs: []error{nil, &pgconn.PgError{Code: "22P02", Message: `invalid input syntax for type bigint: "secret-1"`}}}
	_, err := newRec(f, false).CountRange(scoped(), "public.orders", "id", "secret-1", "")
	if !errors.Is(err, tools.ErrBadInput) || strings.Contains(err.Error(), "secret-1") {
		t.Fatalf("bad bound: %v", err)
	}
	// A missing column.
	f = &recDB{t: t, results: []*recRows{{}}}
	if _, err := newRec(f, false).CountRange(scoped(), "public.orders", "nope", "1", ""); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing column: %v", err)
	}
}
