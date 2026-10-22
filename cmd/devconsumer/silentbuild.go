package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// The two silent bad builds (X1g): neither crashes, rejects or lags, so
// nothing raises a symptom. Only a source-to-sink comparison sees them.

// skipWhere is SKIP_WHERE: a release whose filter drops every order matching
// column=value without loading it, yet commits its offset (sink-filtered-rows).
// Only a few columns are accepted and the predicate is compared in Go, so it
// never reaches SQL. The zero value drops nothing.
type skipWhere struct {
	column, value string
}

// skipWhereColumns are the order columns a predicate may test.
var skipWhereColumns = []string{"status", "customer_id", "id"}

// parseSkipWhere reads SKIP_WHERE, e.g. "status=backfill". Empty means off.
func parseSkipWhere(raw string) (skipWhere, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return skipWhere{}, nil
	}
	col, val, ok := strings.Cut(raw, "=")
	col, val = strings.TrimSpace(col), strings.TrimSpace(val)
	if !ok || val == "" || !slices.Contains(skipWhereColumns, col) {
		return skipWhere{}, fmt.Errorf("bad SKIP_WHERE %q: want <column>=<value> with column one of %s", raw, strings.Join(skipWhereColumns, ", "))
	}
	if col != "status" {
		if _, err := strconv.ParseInt(val, 10, 64); err != nil {
			return skipWhere{}, fmt.Errorf("bad SKIP_WHERE %q: %s is a number", raw, col)
		}
	}
	return skipWhere{column: col, value: val}, nil
}

// String prints the predicate for the start log line; empty when off.
func (w skipWhere) String() string {
	if w.column == "" {
		return ""
	}
	return w.column + "=" + w.value
}

// matches reports whether the change is an order the filter drops. A delete
// carries no row, so it always passes.
func (w skipWhere) matches(c change) bool {
	if w.column == "" || c.After == nil {
		return false
	}
	switch w.column {
	case "status":
		return c.After.Status == w.value
	case "customer_id":
		return strconv.FormatInt(c.After.CustomerID, 10) == w.value
	case "id":
		return strconv.FormatInt(c.After.ID, 10) == w.value
	}
	return false
}

// The analytics.orders columns the upsert writes besides id and loaded_at.
const (
	colCustomerID = "customer_id"
	colAmount     = "amount"
	colStatus     = "status"
	colUpdatedAt  = "updated_at"
)

// orderColumns is the upsert's column order.
var orderColumns = []string{colCustomerID, colAmount, colStatus, colUpdatedAt}

// parseDropColumns reads DROP_COLUMNS, e.g. "updated_at". Empty means none.
func parseDropColumns(raw string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, f := range strings.Split(raw, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if !slices.Contains(orderColumns, f) {
			return nil, fmt.Errorf("bad DROP_COLUMNS entry %q: want one of %s", f, strings.Join(orderColumns, ", "))
		}
		out[f] = true
	}
	return out, nil
}

// joinColumns prints the dropped columns in upsert order for the start log line.
func joinColumns(cols map[string]bool) string {
	var out []string
	for _, c := range orderColumns {
		if cols[c] {
			out = append(out, c)
		}
	}
	return strings.Join(out, ",")
}

// upsertSQL builds the warehouse upsert without the dropped columns and
// returns the columns it writes, in argument order ($1 is id, then one per
// column). A dropped column is left out of both the INSERT and the DO UPDATE
// SET list, so a new row has it NULL and an updated row keeps its old value,
// the way a refactor that forgot the column behaves. Column names come from
// orderColumns, never from input.
func upsertSQL(dropped map[string]bool) (query string, cols []string) {
	names := []string{"id"}
	holders := []string{"$1"}
	var sets []string
	for _, c := range orderColumns {
		if dropped[c] {
			continue
		}
		cols = append(cols, c)
		names = append(names, c)
		holders = append(holders, "$"+strconv.Itoa(len(names)))
		sets = append(sets, c+" = EXCLUDED."+c)
	}
	names = append(names, "loaded_at")
	holders = append(holders, "now()")
	sets = append(sets, "loaded_at = now()")
	query = "INSERT INTO analytics.orders (" + strings.Join(names, ", ") + ")\n" +
		"VALUES (" + strings.Join(holders, ", ") + ")\n" +
		"ON CONFLICT (id) DO UPDATE SET " + strings.Join(sets, ", ")
	return query, cols
}
