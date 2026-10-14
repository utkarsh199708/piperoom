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

// rowSizeSQL builds the probe Row runs first: the size of the row's JSON form
// (OBJECT_CONSTRUCT(*) leaves NULL columns out), so a huge row is refused
// before any of its values are fetched. The key is a "?" bind parameter.
func rowSizeSQL(table, pkColumn string) (string, error) {
	qualified, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	col, err := quoteIdent(pkColumn)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SELECT LENGTH(TO_JSON(OBJECT_CONSTRUCT(*))) FROM %s WHERE %s = ? LIMIT 1", qualified, col), nil
}

// rowSQL builds the single-row lookup. The primary key value is a "?" bind
// parameter, never part of the text.
func rowSQL(table, pkColumn string) (string, error) {
	qualified, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	col, err := quoteIdent(pkColumn)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s = ? LIMIT 1", qualified, col), nil
}
