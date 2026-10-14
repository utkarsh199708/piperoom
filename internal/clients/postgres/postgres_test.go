package postgres

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var testCols = []column{{name: "id"}, {name: "note", varlena: true}, {name: "updated_at"}}

func TestRowQuery(t *testing.T) {
	q, err := rowQuery("public.orders", "id", testCols)
	if err != nil {
		t.Fatalf("rowQuery: %v", err)
	}
	for _, want := range []string{
		`"public"."orders"`, `"id" = $1`,
		// Columns by name, never SELECT *; the varlena column is capped.
		`SELECT "id", CASE WHEN octet_length("note"::text) <= 8192 THEN "note" END AS "note", "updated_at", ARRAY[COALESCE(octet_length("note"::text) > 8192, false)] FROM`,
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("query %q does not contain %q", q, want)
		}
	}
	if strings.Contains(q, "*") {
		t.Fatalf("query selects *: %q", q)
	}
}

func TestRowQueryQuotesCatalogNames(t *testing.T) {
	// Column names come from the catalog and can hold any character.
	q, err := rowQuery("public.orders", "id", []column{{name: `we"ird`, varlena: true}})
	if err != nil {
		t.Fatalf("rowQuery: %v", err)
	}
	if !strings.Contains(q, `"we""ird"`) {
		t.Fatalf("column not quoted: %q", q)
	}
}

func TestRowQueryRejectsInjection(t *testing.T) {
	cases := []struct{ table, col string }{
		{"public.orders; DROP TABLE x", "id"},
		{"public.orders", "id; DROP TABLE x"},
		{"public.orders", "1=1"},
		{"orders", "id"}, // missing schema
		{"a.b.c", "id"},
		{"public.orders", ""},
	}
	for _, tc := range cases {
		if _, err := rowQuery(tc.table, tc.col, testCols); err == nil {
			t.Fatalf("expected error for table=%q col=%q", tc.table, tc.col)
		}
	}
	if _, err := rowQuery("public.orders", "id", nil); err == nil {
		t.Fatal("expected error for a table without columns")
	}
}

func TestRowValues(t *testing.T) {
	fds := []pgconn.FieldDescription{
		{Name: "id", DataTypeOID: pgtype.Int8OID},
		{Name: "note", DataTypeOID: pgtype.TextOID},
		{Name: "updated_at", DataTypeOID: pgtype.TimestampOID},
		{Name: "array", DataTypeOID: pgtype.BoolArrayOID},
	}
	tests := []struct {
		name     string
		vals     []any
		wantNote any
	}{
		{"small value kept", []any{int64(7), "hi", nil, []any{false}}, "hi"},
		{"oversized value omitted", []any{int64(7), nil, nil, []any{true}}, OmittedValue},
		{"null flag is not oversized", []any{int64(7), nil, nil, []any{nil}}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, types := rowValues(testCols, fds, tt.vals)
			if m["note"] != tt.wantNote {
				t.Fatalf("note = %#v, want %#v", m["note"], tt.wantNote)
			}
			if m["id"] != "7" || types["updated_at"] != pgtype.TimestampOID || len(m) != 3 {
				t.Fatalf("row = %#v types = %#v", m, types)
			}
		})
	}
}
