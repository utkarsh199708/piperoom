package owners_test

import (
	"strings"
	"testing"
)

// A table name containing '.' or '"' is spelled quoted in owners.yaml; the
// plain form is ambiguous and refused with a hint instead of matching nothing
// in what_changed (#461).
func TestLoadSourceTableSpelling(t *testing.T) {
	const head = "pipelines:\n  - id: orders\n    topic_prefix: pg\n    connectors: [c1]\n    source_tables: "
	for _, tc := range []struct {
		name, tables, wantErr string // wantErr "" = accepted
	}{
		{"plain", `[public.orders]`, ""},
		{"mixed case", `[public.Orders]`, ""},
		{"quoted dot", `['public."a.b"']`, ""},
		{"quoted quote", `['public."a""b"']`, ""},
		{"both quoted", `['"my.schema"."a.b"']`, ""},
		{"unqualified", `[orders]`, ""},
		{"plain dotted name", `[public.a.b]`, `public."a.b"`},
		{"plain quote in name", `['public.a"b']`, "unbalanced quote"},
		{"empty part", `['public.']`, "empty name part"},
		{"unclosed quote", `['public."a.b']`, "unbalanced quote"},
	} {
		_, err := loadOwners(t, head+tc.tables+"\n")
		switch {
		case tc.wantErr == "" && err != nil:
			t.Errorf("%s: %v", tc.name, err)
		case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
			t.Errorf("%s: err = %v, want it to contain %q", tc.name, err, tc.wantErr)
		}
	}
}
