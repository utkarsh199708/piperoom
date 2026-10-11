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
// a qualified table name) and returns it ready to interpolate into SQL text,
// unquoted and upper-cased. Values are never accepted here — only bind
// parameters carry values.
//
// Snowflake folds an unquoted identifier to upper case when it resolves a
// table or column, and treats a double-quoted one as case-sensitive instead.
// A table created the normal way (CREATE TABLE orders, no quotes — what
// dev/owners.yaml's lower-case names like "analytics.orders" assume) is
// stored as ORDERS and is only found by an unquoted, case-folded reference;
// quoting the name as typed would instead look for a table literally named
// "orders" and never match. Upper-casing here makes that explicit rather
// than relying on Snowflake to fold it implicitly. A table or column created
// WITH quotes, preserving mixed/lower case, cannot be referenced through
// this client — there would be no way to tell that case apart from the
// normal unquoted one from the input string alone.
func validateIdent(name string) (string, error) {
	if !identRe.MatchString(name) {
		return "", fmt.Errorf("snowflake: invalid identifier %q", name)
	}
	return strings.ToUpper(name), nil
}

// validateTable validates a "schema.table" or "db.schema.table" name and
// returns it as an unquoted, upper-cased, dot-joined qualified name ready to
// interpolate into SQL text (see validateIdent for why it is upper-cased).
func validateTable(table string) (string, error) {
	parts := strings.Split(table, ".")
	if len(parts) != 2 && len(parts) != 3 {
		return "", fmt.Errorf("snowflake: table must be \"schema.table\" or \"db.schema.table\", got %q", table)
	}
	valid := make([]string, len(parts))
	for i, p := range parts {
		v, err := validateIdent(p)
		if err != nil {
			return "", fmt.Errorf("snowflake: invalid table %q: %w", table, err)
		}
		valid[i] = v
	}
	return strings.Join(valid, "."), nil
}
