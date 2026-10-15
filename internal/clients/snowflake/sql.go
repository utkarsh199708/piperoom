package snowflake

import "fmt"

// freshnessSQL builds the freshness query. Identifiers are validated, folded
// to upper case and double-quoted; the query has no values, so there is
// nothing to bind.
func freshnessSQL(table, tsColumn string) (string, error) {
	qualified, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	col, err := quoteIdent(tsColumn)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SELECT MAX(%s) FROM %s", col, qualified), nil
}

// rowSQL builds the single-row lookup. pkColumn is quoted exactly as given
// (it is the column's stored name, see DB.resolveKeyColumn), not upper-cased:
// a quoted "id" and an unquoted ID are different columns. The primary key
// value is a "?" bind parameter, never part of the text.
func rowSQL(table, pkColumn string) (string, error) {
	qualified, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	if !identRe.MatchString(pkColumn) {
		return "", fmt.Errorf("snowflake: invalid identifier %q", pkColumn)
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s = ? LIMIT 1", qualified, quote(pkColumn)), nil
}
