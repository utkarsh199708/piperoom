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
// a qualified table name) and returns it upper-cased, not yet quoted. Values
// are never accepted here: only bind parameters carry values.
//
// Snowflake folds an unquoted identifier to upper case when it resolves a
// table or column, and treats a double-quoted one as case-sensitive instead.
// A table created the normal way (CREATE TABLE orders, no quotes, which is
// what owners.yaml's lower-case names like "analytics.orders" assume) is
// stored as ORDERS. Folding to upper case here and then quoting (see
// quoteIdent) reaches exactly that object, and also keeps names that are
// reserved words (ORDER, TIMESTAMP, DATE) working. A table or column created
// WITH quotes, preserving mixed or lower case, cannot be reached through this
// client: the input string alone cannot tell it from the normal unquoted one.
func validateIdent(name string) (string, error) {
	if !identRe.MatchString(name) {
		return "", fmt.Errorf("snowflake: invalid identifier %q", name)
	}
	return strings.ToUpper(name), nil
}

// quoteIdent validates name and returns it as a double-quoted, upper-cased
// Snowflake identifier ready to interpolate into SQL text.
func quoteIdent(name string) (string, error) {
	v, err := validateIdent(name)
	if err != nil {
		return "", err
	}
	return quote(v), nil
}

// quote wraps an already validated identifier part in double quotes. The
// regex check rules out any double quote in the input, so the doubling is
// defence in depth only.
func quote(part string) string {
	return `"` + strings.ReplaceAll(part, `"`, `""`) + `"`
}

// quoteTable validates a "schema.table" or "db.schema.table" name and returns
// it as a dot-joined, quoted, upper-cased qualified name, for example
// "ANALYTICS"."ORDERS".
func quoteTable(table string) (string, error) {
	parts, err := tableParts(table)
	if err != nil {
		return "", err
	}
	q := make([]string, len(parts))
	for i, p := range parts {
		q[i] = quote(p)
	}
	return strings.Join(q, "."), nil
}

func tableParts(table string) ([]string, error) {
	parts := strings.Split(table, ".")
	if len(parts) != 2 && len(parts) != 3 {
		return nil, fmt.Errorf("snowflake: table must be \"schema.table\" or \"db.schema.table\", got %q", table)
	}
	valid := make([]string, len(parts))
	for i, p := range parts {
		v, err := validateIdent(p)
		if err != nil {
			return nil, fmt.Errorf("snowflake: invalid table %q: %w", table, err)
		}
		valid[i] = v
	}
	return valid, nil
}
