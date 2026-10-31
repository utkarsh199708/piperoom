package oracle

import (
	"fmt"
	"regexp"
	"strings"
)

// oracleIdentRe is the identifier shape the contract rule 6 check allows for
// Oracle: the leading character set plus digits, '_', '$' and '#' (Oracle
// permits '$' and '#' in unquoted identifiers). A name that does not match is
// refused before it can reach a statement.
var oracleIdentRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$#]*$`)

// validateIdent applies the rule 6 check to name and returns it unchanged for
// use as a bind value. Anything that does not match is refused.
func validateIdent(name string) (string, error) {
	if !oracleIdentRe.MatchString(name) {
		return "", fmt.Errorf("oracle: invalid identifier %q", name)
	}
	return name, nil
}

// quoteIdent validates name against the rule 6 check and returns it quoted with
// double quotes, the way Oracle quotes identifiers. Quoting is applied even for
// plain names so the statement is never built from an untrusted fragment; note
// that a quoted identifier is case-sensitive, so callers pass names exactly as
// the dictionary stores them (uppercase for identifiers created without
// quotes). The validation regexp admits no double quote, so the quoting
// cannot be closed early.
//
// The quoted text is copied out of identChars byte by byte rather than from
// name: the statement is built only from this package's constants, and a
// byte of name that oracleIdentRe admitted but identChars lacks (none can)
// would be refused. Static analysis (CodeQL's go/sql-injection) does not
// count a regexp match as a sanitiser, so before this it traced the
// caller's string into ExecContext; the copy cuts that trace for real.
func quoteIdent(name string) (string, error) {
	if !oracleIdentRe.MatchString(name) {
		return "", fmt.Errorf("oracle: invalid identifier %q", name)
	}
	var b strings.Builder
	b.Grow(len(name) + 2)
	b.WriteByte('"')
	for i := 0; i < len(name); i++ {
		j := strings.IndexByte(identChars, name[i])
		if j < 0 {
			return "", fmt.Errorf("oracle: invalid identifier %q", name)
		}
		b.WriteByte(identChars[j])
	}
	b.WriteByte('"')
	return b.String(), nil
}

// identChars is every byte oracleIdentRe admits.
const identChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_$#"

// quoteTable quotes an "owner.table" name into two quoted identifiers. Every
// part must pass the rule 6 check; anything else is refused.
func quoteTable(table string) (string, error) {
	owner, name, err := quoteTableParts(table)
	if err != nil {
		return "", err
	}
	return owner + "." + name, nil
}

// tableParts validates an "owner.table" name and returns its two parts
// unchanged for use as bind values.
func tableParts(table string) (owner, name string, err error) {
	parts := strings.Split(table, ".")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("oracle: table must be owner.table, got %q", table)
	}
	owner, err = validateIdent(parts[0])
	if err != nil {
		return "", "", fmt.Errorf("oracle: table %q: %w", table, err)
	}
	name, err = validateIdent(parts[1])
	if err != nil {
		return "", "", fmt.Errorf("oracle: table %q: %w", table, err)
	}
	return owner, name, nil
}

// quoteTableParts returns the two quoted identifiers of an "owner.table" name,
// so a caller can splice them into a WHERE clause without re-splitting a
// quoted string.
func quoteTableParts(table string) (owner, name string, err error) {
	owner, name, err = tableParts(table)
	if err != nil {
		return "", "", err
	}
	owner, err = quoteIdent(owner)
	if err != nil {
		return "", "", err
	}
	name, err = quoteIdent(name)
	if err != nil {
		return "", "", err
	}
	return owner, name, nil
}
