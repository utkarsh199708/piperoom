package snowflake

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/redact"
)

const (
	// copyHistoryWindow is how far back INFORMATION_SCHEMA.COPY_HISTORY reads.
	copyHistoryWindow = 14 * 24 * time.Hour
	// maxLoadErrorRunes caps LoadError.FirstError (clients.LoadError contract).
	maxLoadErrorRunes = 300
	defaultLoadLimit  = 20
	maxLoadLimit      = 100
)

// loadErrorsSQL lists the loads of one table that did not fully succeed. The
// table name and the cut-off are bind parameters; START_TIME goes through
// TO_TIMESTAMP_LTZ(epoch seconds) so the instant does not depend on the
// session time zone.
const loadErrorsSQL = `SELECT TABLE_NAME, LAST_LOAD_TIME, STATUS, ROW_COUNT, ERROR_COUNT, FIRST_ERROR_MESSAGE, FILE_NAME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME => ?, START_TIME => TO_TIMESTAMP_LTZ(?)))
WHERE STATUS <> 'LOADED'
ORDER BY LAST_LOAD_TIME DESC
LIMIT ?`

// LoadErrors reads the failed and partial loads of table ("schema.table" or
// "db.schema.table") since a time, newest first. COPY_HISTORY keeps 14 days, so
// an older since is raised to that. It lists COPY INTO loads only: Snowpipe
// Streaming writes no COPY_HISTORY rows, so an empty result does not prove a
// healthy sink. Error text is redacted and cut; file names lose their path.
func (d *DB) LoadErrors(ctx context.Context, table string, since time.Time, limit int) ([]clients.LoadError, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	parts, err := tableParts(table)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = defaultLoadLimit
	}
	if limit > maxLoadLimit {
		limit = maxLoadLimit
	}
	if floor := time.Now().Add(-copyHistoryWindow); since.Before(floor) {
		since = floor
	}

	rows, err := d.query(ctx, loadErrorsSQL, strings.Join(parts, "."), since.Unix(), int64(limit))
	if err != nil {
		return nil, wrapQueryErr("load history of", table, err)
	}
	defer rows.Close()

	var out []clients.LoadError
	for rows.Next() {
		var name, at, status, nrows, nerr, msg, file any
		if err := rows.Scan(&name, &at, &status, &nrows, &nerr, &msg, &file); err != nil {
			return nil, fmt.Errorf("snowflake: load history of %s: decode: %w", table, err)
		}
		le := clients.LoadError{
			Table:      table,
			Status:     asString(status),
			Rows:       asInt(nrows),
			Errors:     asInt(nerr),
			FirstError: cleanLoadError(asString(msg)),
			File:       baseName(asString(file)),
		}
		switch t := at.(type) {
		case time.Time:
			le.At = t.UTC()
		case *time.Time:
			if t != nil {
				le.At = t.UTC()
			}
		}
		out = append(out, le)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapQueryErr("load history of", table, err)
	}
	return out, nil
}

// cleanLoadError masks secrets, folds whitespace (the text may carry a record
// excerpt on several lines) and cuts to maxLoadErrorRunes.
func cleanLoadError(s string) string {
	s, _ = redact.Text(s)
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) > maxLoadErrorRunes {
		s = string([]rune(s)[:maxLoadErrorRunes-1]) + "…"
	}
	return s
}

// baseName keeps the last path element of a stage file name, so no bucket,
// prefix or presigned URL query leaves the client.
func baseName(s string) string {
	if i := strings.IndexAny(s, "?#"); i >= 0 {
		s = s[:i]
	}
	if i := strings.LastIndexAny(s, `/\`); i >= 0 {
		s = s[i+1:]
	}
	return s
}

func asString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}

func asInt(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case float64:
		return int64(t)
	case string:
		var n int64
		if _, err := fmt.Sscanf(t, "%d", &n); err == nil {
			return n
		}
	}
	return 0
}

var _ clients.LoadErrorReader = (*DB)(nil)
