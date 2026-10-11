package snowflake

import (
	"strings"
	"testing"
)

func TestValidateTable(t *testing.T) {
	cases := []struct {
		table   string
		want    string
		wantErr bool
	}{
		{table: "public.orders", want: "PUBLIC.ORDERS"},
		{table: "analytics.orders", want: "ANALYTICS.ORDERS"},
		{table: "analytics_db.public.orders", want: "ANALYTICS_DB.PUBLIC.ORDERS"},
		{table: "orders", wantErr: true},
		{table: "a.b.c.d", wantErr: true},
		{table: "public.orders; DROP TABLE x", wantErr: true},
		{table: "public.\"orders\"", wantErr: true},
		{table: "public.", wantErr: true},
		{table: "", wantErr: true},
	}
	for _, tc := range cases {
		got, err := validateTable(tc.table)
		if tc.wantErr {
			if err == nil {
				t.Errorf("validateTable(%q): expected error, got %q", tc.table, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("validateTable(%q): unexpected error: %v", tc.table, err)
			continue
		}
		if got != tc.want {
			t.Errorf("validateTable(%q) = %q, want %q", tc.table, got, tc.want)
		}
	}
}

// TestValidateTableCaseFoldsLikeSnowflakeUnquoted pins down the fix for
// LEAD-ARCH #1 on PR #4: dev/owners.yaml names tables in lower case (e.g.
// "analytics.orders"), the way they're normally created (unquoted); a
// quoted "analytics"."orders" would be case-sensitive and never match the
// actual, upper-cased ORDERS table.
func TestValidateTableCaseFoldsLikeSnowflakeUnquoted(t *testing.T) {
	got, err := validateTable("analytics.orders")
	if err != nil {
		t.Fatalf("validateTable: %v", err)
	}
	if got != "ANALYTICS.ORDERS" {
		t.Fatalf("validateTable(%q) = %q, want unquoted, upper-cased %q", "analytics.orders", got, "ANALYTICS.ORDERS")
	}
	if strings.Contains(got, `"`) {
		t.Fatalf("validateTable(%q) = %q, must not quote the identifier (quoting makes it case-sensitive)", "analytics.orders", got)
	}
}

func TestValidateIdent(t *testing.T) {
	cases := []struct {
		name    string
		want    string
		wantErr bool
	}{
		{name: "updated_at", want: "UPDATED_AT"},
		{name: "_col$1", want: "_COL$1"},
		{name: "1col", wantErr: true},
		{name: "col; DROP TABLE x", wantErr: true},
		{name: "col with space", wantErr: true},
		{name: "col\"quote", wantErr: true},
		{name: "", wantErr: true},
	}
	for _, tc := range cases {
		got, err := validateIdent(tc.name)
		if tc.wantErr {
			if err == nil {
				t.Errorf("validateIdent(%q): expected error, got %q", tc.name, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("validateIdent(%q): unexpected error: %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("validateIdent(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}
}
