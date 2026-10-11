package postgres

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestRowQuery(t *testing.T) {
	q, err := rowQuery("public.orders", "id")
	if err != nil {
		t.Fatalf("rowQuery: %v", err)
	}
	if !strings.Contains(q, `"public"."orders"`) || !strings.Contains(q, `"id" = $1`) {
		t.Fatalf("unexpected query: %q", q)
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
		if _, err := rowQuery(tc.table, tc.col); err == nil {
			t.Fatalf("expected error for table=%q col=%q", tc.table, tc.col)
		}
	}
}

func TestNumericString(t *testing.T) {
	cases := []struct {
		n    pgtype.Numeric
		want string
	}{
		{pgtype.Numeric{Int: big.NewInt(1234), Exp: -2, Valid: true}, "12.34"},
		{pgtype.Numeric{Int: big.NewInt(-5), Exp: -2, Valid: true}, "-0.05"},
		{pgtype.Numeric{Int: big.NewInt(42), Exp: 0, Valid: true}, "42"},
		{pgtype.Numeric{Int: big.NewInt(7), Exp: 2, Valid: true}, "700"},
		{pgtype.Numeric{NaN: true, Valid: true}, "NaN"},
	}
	for _, tc := range cases {
		if got := numericString(tc.n); got != tc.want {
			t.Fatalf("numericString(%+v) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

func TestToJSON(t *testing.T) {
	ts := time.Date(2026, 10, 11, 10, 59, 3, 0, time.UTC)
	cases := []struct {
		in   any
		want any
	}{
		{int64(123), "123"},
		{int32(7), "7"},
		{float64(3.5), "3.5"},
		{ts, ts.Format(time.RFC3339Nano)},
		{"hello", "hello"},
		{true, true},
		{nil, nil},
		{[]byte("abc"), "abc"},
	}
	for _, tc := range cases {
		if got := toJSON(tc.in); got != tc.want {
			t.Fatalf("toJSON(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
