package warehouse

import (
	"strings"
	"testing"
)

func TestValidateTableColumn(t *testing.T) {
	schema, tbl, col, err := validateTableColumn("analytics.orders", "updated_at")
	if err != nil {
		t.Fatalf("validateTableColumn: %v", err)
	}
	if schema != "analytics" || tbl != "orders" || col != "updated_at" {
		t.Fatalf("got (%q,%q,%q)", schema, tbl, col)
	}
}

func TestValidateTableColumnRejectsInjection(t *testing.T) {
	cases := []struct{ table, col string }{
		{"analytics.orders; DROP TABLE x", "id"},
		{"analytics.orders", "id; DROP TABLE x"},
		{"orders", "id"},
		{"analytics.orders", ""},
		{"analytics.orders", "1=1"},
	}
	for _, tc := range cases {
		if _, _, _, err := validateTableColumn(tc.table, tc.col); err == nil {
			t.Fatalf("expected error for table=%q col=%q", tc.table, tc.col)
		} else if !strings.Contains(err.Error(), "warehouse") {
			t.Fatalf("error not wrapped: %v", err)
		}
	}
}
