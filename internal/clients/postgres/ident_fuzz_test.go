package postgres

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// seedIdents is the fuzz seed corpus: valid identifiers plus the malformed and
// injection inputs from TestRowQueryRejectsInjection and other edge cases. Each
// is accepted (or refused) exactly as identRe and splitTable define, so they run
// as a deterministic unit test on every `go test`.
var seedIdents = []string{
	"orders",
	"_x",
	"a1",
	"",
	"1a",
	"a-b",
	"a b",
	"a;drop",
	"a\"b",
	"a.b",
	"a\n",
	"orders\n", // trailing newline on a valid identifier
	"\x00",     // NUL byte
	"\u00e4",   // ä (latin small letter a with diaeresis)
	"\uff41",   // fullwidth latin small letter a
	strings.Repeat("a", 1000),
	// TestRowQueryRejectsInjection table/column inputs.
	"public.orders; DROP TABLE x",
	"public.orders",
	"a.b.c",
	"id",
	"id; DROP TABLE x",
	"1=1",
}

func FuzzValidateIdent(f *testing.F) {
	for _, s := range seedIdents {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		checkIdent(t, s)
		checkSplitTable(t, s)
	})
}

// checkIdent asserts validateIdent accepts exactly the strings matching
// identRe, that it passes the value through unchanged, and that an accepted
// identifier sanitizes to exactly one double-quoted identifier — no NUL or
// quote byte can survive into SQL.
func checkIdent(t *testing.T, s string) {
	t.Helper()
	got, err := validateIdent(s)
	if err != nil {
		if identRe.MatchString(s) {
			t.Fatalf("validateIdent(%q) refused a string matching identRe", s)
		}
		// Refused: the caller (rowQuery) returns before any SQL is built.
		return
	}
	if !identRe.MatchString(s) {
		t.Fatalf("validateIdent(%q) accepted a string not matching identRe", s)
	}
	if got != s {
		t.Fatalf("validateIdent(%q) returned %q, want identity", s, got)
	}
	q := pgx.Identifier{s}.Sanitize()
	if q != `"`+s+`"` {
		t.Fatalf("pgx.Identifier{%q}.Sanitize() = %q, want exactly one quoted identifier", s, q)
	}
}

// checkSplitTable asserts splitTable accepts exactly "schema.table" with both
// parts valid identifiers, and that it returns the raw parts unchanged.
func checkSplitTable(t *testing.T, s string) {
	t.Helper()
	schema, tbl, err := splitTable(s)
	parts := strings.Split(s, ".")
	valid := len(parts) == 2 && identRe.MatchString(parts[0]) && identRe.MatchString(parts[1])
	if err != nil {
		if valid {
			t.Fatalf("splitTable(%q) refused a valid schema.table", s)
		}
		return
	}
	if !valid {
		t.Fatalf("splitTable(%q) accepted an invalid table", s)
	}
	if schema != parts[0] || tbl != parts[1] {
		t.Fatalf("splitTable(%q) = (%q, %q), want (%q, %q)", s, schema, tbl, parts[0], parts[1])
	}
}
