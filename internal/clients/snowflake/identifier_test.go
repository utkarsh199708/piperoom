package snowflake

import "testing"

// TestQuoteTableCaseFoldsLikeSnowflakeUnquoted pins down the fix for
// LEAD-ARCH #1 on PR #4: dev/owners.yaml names tables in lower case (e.g.
// "analytics.orders"), the way they're normally created (unquoted, so stored
// upper-case). A quoted lower-case "analytics"."orders" would be
// case-sensitive and never match, so the name is folded to upper case first
// and only then quoted.
func TestQuoteTableCaseFoldsLikeSnowflakeUnquoted(t *testing.T) {
	cases := []struct {
		table   string
		want    string
		wantErr bool
	}{
		{table: "analytics.orders", want: `"ANALYTICS"."ORDERS"`},
		{table: "Analytics.Orders", want: `"ANALYTICS"."ORDERS"`},
		{table: "db1.analytics.orders", want: `"DB1"."ANALYTICS"."ORDERS"`},
		{table: "analytics.order$1", want: `"ANALYTICS"."ORDER$1"`},
		{table: "orders", wantErr: true},
		{table: "a.b.c.d", wantErr: true},
		{table: `analytics."orders"`, wantErr: true},
		{table: `analytics.orders" OR 1=1 --`, wantErr: true},
		{table: "analytics.orders;DROP TABLE x", wantErr: true},
		{table: "analytics.orders\n", wantErr: true},
		{table: "analytics.ord ers", wantErr: true},
		{table: "analytics.", wantErr: true},
		{table: ".orders", wantErr: true},
		{table: "", wantErr: true},
	}
	for _, tc := range cases {
		got, err := quoteTable(tc.table)
		if tc.wantErr {
			if err == nil {
				t.Errorf("quoteTable(%q): expected error, got %q", tc.table, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("quoteTable(%q): unexpected error: %v", tc.table, err)
			continue
		}
		if got != tc.want {
			t.Errorf("quoteTable(%q) = %q, want %q", tc.table, got, tc.want)
		}
	}
}

func TestQuoteIdent(t *testing.T) {
	cases := []struct {
		name    string
		want    string
		wantErr bool
	}{
		{name: "updated_at", want: `"UPDATED_AT"`},
		{name: "ORDER", want: `"ORDER"`}, // reserved word: only works because it is quoted
		{name: "Mixed_Case", want: `"MIXED_CASE"`},
		{name: `x" FROM y; --`, wantErr: true},
		{name: `"x"`, wantErr: true},
		{name: "x,y", wantErr: true},
		{name: "xé", wantErr: true},
		{name: "", wantErr: true},
	}
	for _, tc := range cases {
		got, err := quoteIdent(tc.name)
		if tc.wantErr {
			if err == nil {
				t.Errorf("quoteIdent(%q): expected error, got %q", tc.name, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("quoteIdent(%q) = %q, %v; want %q", tc.name, got, err, tc.want)
		}
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
