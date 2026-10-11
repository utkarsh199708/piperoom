package snowflake

import (
	"fmt"
	"regexp"
	"strings"
)

// identRe matches one unquoted Snowflake identifier part: a letter or
// underscore followed by letters, digits, underscores or dollar signs.
var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

// validateIdent validates a single identifier (a column name, or one part of
// a qualified table name) and returns it quoted with Snowflake double-quote
// rules, safe to interpolate into SQL text. Values are never accepted here —
// only bind parameters carry values.
func validateIdent(name string) (string, error) {
	if !identRe.MatchString(name) {
		return "", fmt.Errorf("snowflake: invalid identifier %q", name)
	}
	return quoteIdent(name), nil
}

// quoteIdent double-quotes an identifier already known to match identRe (so
// it contains no quote characters to escape).
func quoteIdent(name string) string {
	return `"` + name + `"`
}

// validateTable validates a "schema.table" or "db.schema.table" name and
// returns it as a quoted, dot-joined qualified name ready to interpolate
// into SQL text.
func validateTable(table string) (string, error) {
	parts := strings.Split(table, ".")
	if len(parts) != 2 && len(parts) != 3 {
		return "", fmt.Errorf("snowflake: table must be \"schema.table\" or \"db.schema.table\", got %q", table)
	}
	quoted := make([]string, len(parts))
	for i, p := range parts {
		q, err := validateIdent(p)
		if err != nil {
			return "", fmt.Errorf("snowflake: invalid table %q: %w", table, err)
		}
		quoted[i] = q
	}
	return strings.Join(quoted, "."), nil
}
