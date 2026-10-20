package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"

	"piperoom/internal/clients"
)

const (
	defaultLoadLimit = 20
	maxLoadLimit     = 100
)

// loadErrorsSQL matches failed INSERTs into one table. An INSERT that fails
// before it starts (an unknown column, a bad INSERT ... SELECT: the classic
// schema-drift sink failure) is logged with an empty tables array, so those
// rows are matched on the statement's target instead: the qualified name, or
// the bare name when the session's current database is the table's. The query
// text is only matched, never selected.
//
// event_date is the server's local date while since is UTC, so the partition
// filter starts a day early and event_time keeps the exact bound. toDate32
// keeps the day before 1970-01-01 from wrapping around to 2149.
const loadErrorsSQL = "SELECT event_time, exception_code, written_rows\n" +
	"FROM system.query_log\n" +
	"WHERE type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')\n" +
	"  AND query_kind = 'Insert'\n" +
	"  AND event_date >= toDate32(?) - 1\n" +
	"  AND event_time >= ?\n" +
	"  AND (has(tables, ?) OR (empty(tables) AND (match(query, ?) OR (current_database = ? AND match(query, ?)))))\n" +
	"ORDER BY event_time DESC\n" +
	"LIMIT ?"

const kafkaConsumerErrorsSQL = "SELECT arrayJoin(arrayFilter(at -> at >= ?, exceptions.time)) AS at\n" +
	"FROM system.kafka_consumers\n" +
	"WHERE database = ? AND table = ?\n" +
	"ORDER BY at DESC\n" +
	"LIMIT ?"

// insertTargetPattern matches the start of an INSERT whose target is the given
// table, with optional backticks and the TABLE keyword, case-insensitively. It
// is an RE2 pattern for ClickHouse's match(). The names are already validated
// identifiers; QuoteMeta still quotes them so a looser validator later cannot
// turn a name into a pattern. An empty database matches the bare table name.
func insertTargetPattern(database, table string) string {
	const tick = "`?"
	target := tick + regexp.QuoteMeta(table) + tick
	if database != "" {
		target = tick + regexp.QuoteMeta(database) + tick + `\.` + target
	}
	return `(?i)^\s*INSERT\s+INTO\s+(TABLE\s+)?` + target + `([\s(]|$)`
}

// LoadErrors returns failed or partial INSERTs since the given time. It reads
// only exception codes and write counts from query_log; raw query and exception
// text can contain record values, so neither is returned. Kafka-engine error
// text is likewise omitted; only its exception timestamps are surfaced.
func (c *Client) LoadErrors(ctx context.Context, table string, since time.Time, limit int) ([]clients.LoadError, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	parts, err := tableParts(table)
	if err != nil {
		return nil, err
	}
	database, tableName := c.defaultDatabase, parts[0]
	if len(parts) == 2 {
		database, tableName = parts[0], parts[1]
	}
	if database == "" {
		return nil, fmt.Errorf("clickhouse: load history for %q requires a database in the DSN or db.table input", table)
	}
	if limit <= 0 {
		limit = defaultLoadLimit
	}
	if limit > maxLoadLimit {
		limit = maxLoadLimit
	}

	rows, err := c.query(ctx, loadErrorsSQL,
		since.UTC(), since.UTC(),
		database+"."+tableName,
		insertTargetPattern(database, tableName),
		database, insertTargetPattern("", tableName),
		limit)
	if err != nil {
		if missingObject(err) {
			return nil, fmt.Errorf("clickhouse: load history: %w", clients.ErrNotSupported)
		}
		return nil, c.queryError("load history of", table, err)
	}

	out := make([]clients.LoadError, 0)
	for rows.Next() {
		var atRaw, codeRaw, writtenRaw any
		if err := rows.Scan(&atRaw, &codeRaw, &writtenRaw); err != nil {
			_ = rows.Close()
			return nil, c.queryError("decode load history of", table, err)
		}
		at, ok, err := timestamp(atRaw)
		if err != nil || !ok {
			_ = rows.Close()
			if err == nil {
				err = errors.New("null query-log event time")
			}
			return nil, c.queryError("decode load history of", table, err)
		}
		rowsWritten := asInt64(writtenRaw)
		status := "LOAD_FAILED"
		if rowsWritten > 0 {
			status = "PARTIALLY_LOADED"
		}
		out = append(out, clients.LoadError{
			Table:      table,
			At:         at,
			Status:     status,
			Rows:       rowsWritten,
			Errors:     1,
			FirstError: fmt.Sprintf("ClickHouse INSERT exception (code %d)", asInt64(codeRaw)),
		})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, c.queryError("load history of", table, err)
	}
	if err := rows.Close(); err != nil {
		return nil, c.queryError("close load history of", table, err)
	}

	kafkaRows, err := c.query(ctx, kafkaConsumerErrorsSQL, since.UTC(), database, tableName, limit)
	if err != nil {
		// system.kafka_consumers is optional on builds without Kafka support.
		// A missing grant should not discard query-log evidence already read.
		if !missingObject(err) && !accessDenied(err) {
			return nil, c.queryError("read Kafka consumer errors of", table, err)
		}
	} else {
		for kafkaRows.Next() {
			var atRaw any
			if err := kafkaRows.Scan(&atRaw); err != nil {
				_ = kafkaRows.Close()
				return nil, c.queryError("decode Kafka consumer errors of", table, err)
			}
			at, ok, err := timestamp(atRaw)
			if err != nil || !ok {
				_ = kafkaRows.Close()
				if err == nil {
					err = errors.New("null Kafka consumer exception time")
				}
				return nil, c.queryError("decode Kafka consumer errors of", table, err)
			}
			out = append(out, clients.LoadError{
				Table:      table,
				At:         at,
				Status:     "LOAD_FAILED",
				Errors:     1,
				FirstError: "ClickHouse Kafka engine consumer reported an exception",
			})
		}
		if err := kafkaRows.Err(); err != nil {
			_ = kafkaRows.Close()
			return nil, c.queryError("read Kafka consumer errors of", table, err)
		}
		if err := kafkaRows.Close(); err != nil {
			return nil, c.queryError("close Kafka consumer errors of", table, err)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].At.After(out[j].At) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

var _ clients.LoadErrorReader = (*Client)(nil)
