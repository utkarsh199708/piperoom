package clickhouse

import (
	"fmt"
	"strings"
)

func freshnessSQL(table, tsColumn string) (string, error) {
	qualified, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	column, err := quoteIdent(tsColumn)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SELECT maxOrNull(%s) FROM %s", column, qualified), nil
}

// Row uses FINAL so ReplacingMergeTree returns the deduplicated version even
// before background merges have completed. Sink tables therefore need a
// MergeTree-family engine.
func rowSQL(table, pkColumn string) (string, error) {
	return rowQuerySQL(table, pkColumn, true)
}

func rowFallbackSQL(table, pkColumn string) (string, error) {
	return rowQuerySQL(table, pkColumn, false)
}

func rowQuerySQL(table, pkColumn string, final bool) (string, error) {
	qualified, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	column, err := quoteIdent(pkColumn)
	if err != nil {
		return "", err
	}
	finalClause := ""
	if final {
		finalClause = " FINAL"
	}
	return fmt.Sprintf("SELECT * FROM %s%s WHERE %s = ? LIMIT 1", qualified, finalClause, column), nil
}

func countSQL(table, tsColumn string) (string, error) {
	qualified, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	column, err := quoteIdent(tsColumn)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"SELECT toStartOfInterval(toDateTime64(%[1]s, 9) - INTERVAL ? NANOSECOND, INTERVAL ? SECOND) + INTERVAL ? NANOSECOND AS bucket, count() FROM %[2]s WHERE %[1]s >= fromUnixTimestamp64Nano(?) AND %[1]s < fromUnixTimestamp64Nano(?) GROUP BY bucket ORDER BY bucket",
		column, qualified,
	), nil
}

func sqlText(query string) string {
	return strings.Join(strings.Fields(query), " ")
}
