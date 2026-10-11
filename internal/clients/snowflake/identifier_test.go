package snowflake

import "testing"

func TestValidateTable(t *testing.T) {
	cases := []struct {
		table   string
		want    string
		wantErr bool
	}{
		{table: "public.orders", want: `"public"."orders"`},
		{table: "analytics_db.public.orders", want: `"analytics_db"."public"."orders"`},
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

func TestValidateIdent(t *testing.T) {
	cases := []struct {
		name    string
		want    string
		wantErr bool
	}{
		{name: "updated_at", want: `"updated_at"`},
		{name: "_col$1", want: `"_col$1"`},
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
