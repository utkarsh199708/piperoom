package postgres

import (
	"strings"
	"testing"
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
