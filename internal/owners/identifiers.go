package owners

import (
	"fmt"
	"strings"
)

// identParts counts the dot-separated parts of a SQL identifier, treating a
// dot inside double quotes as part of the name ("" inside quotes is one
// quote). ok is false for an unbalanced quote or an empty part.
func identParts(s string) (n int, ok bool) {
	inQ, have := false, false
	rs := []rune(s)
	for i := 0; i < len(rs); i++ {
		switch r := rs[i]; {
		case r == '"' && inQ && i+1 < len(rs) && rs[i+1] == '"':
			i++
		case r == '"':
			inQ, have = !inQ, true
		case r == '.' && !inQ:
			if !have {
				return 0, false
			}
			n++
			have = false
		default:
			have = true
		}
	}
	if inQ || !have {
		return 0, false
	}
	return n + 1, true
}

// validateSourceTable checks the spelling of a source_tables entry. A name
// whose identifier contains '.' or '"' must be quoted (public."a.b",
// public."a""b"); the plain form public.a.b is read as three parts and would
// silently match no change, so it is refused here instead. A plain name with
// a lone '"' (public.a"b) is not detectable as wrong and stays a known gap.
func validateSourceTable(name string) error {
	// A malformed entry (unbalanced quote, empty part) is left alone: the
	// watcher still matches it by exact text, and refusing it would break
	// files that load today. Only the unambiguous 3+ part form is refused.
	if n, ok := identParts(name); ok && n > 2 {
		return fmt.Errorf("source table %q has %d dot-separated parts, want schema.table; if the table name itself contains a '.', quote it: %s",
			name, n, suggestQuoted(name))
	}
	return nil
}

// suggestQuoted proposes the quoted spelling for a plain schema.rest name.
func suggestQuoted(name string) string {
	schema, rest, _ := strings.Cut(name, ".")
	return schema + `."` + strings.ReplaceAll(rest, `"`, `""`) + `"`
}
