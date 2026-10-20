package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"time"
	"unicode/utf8"

	"piperoom/internal/tools"
)

// sqlRows narrows *sql.Rows so unit tests can provide deterministic results.
type sqlRows interface {
	Next() bool
	Scan(dest ...any) error
	Columns() ([]string, error)
	Err() error
	Close() error
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (sqlRows, error)
}

type dbQuerier struct {
	db *sql.DB
}

func (q dbQuerier) QueryContext(ctx context.Context, query string, args ...any) (sqlRows, error) {
	// clickhouse-go safely binds the '?' values client-side before sending SQL.
	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (c *Client) query(ctx context.Context, query string, args ...any) (sqlRows, error) {
	start := time.Now()
	rows, err := c.q.QueryContext(ctx, query, args...)
	tools.Record(ctx, tools.Call{
		Kind:           tools.CallSQL,
		Target:         c.target,
		Text:           sqlText(query),
		ParamsRedacted: redactArgs(args),
		At:             start,
		TookMS:         time.Since(start).Milliseconds(),
	})
	if err != nil {
		return nil, Redact(err, c.dsn)
	}
	return rows, nil
}

// maxParamBytes cuts a long string argument in a receipt.
const maxParamBytes = 64

// redactArgs renders bind arguments as "?n=value" for a provenance receipt, as
// the Snowflake client does. Names, keys and bounds are shown as they are (the
// Row key also appears in the tool summary); a long string is cut and bytes
// are never shown.
func redactArgs(args []any) []string {
	if len(args) == 0 {
		return nil
	}
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = fmt.Sprintf("?%d=%s", i+1, renderArg(a))
	}
	return out
}

func renderArg(a any) string {
	switch v := a.(type) {
	case nil:
		return "NULL"
	case string:
		return cutParam(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	case time.Duration:
		return v.String()
	case []byte:
		return fmt.Sprintf("<%d bytes>", len(v))
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		return fmt.Sprint(v)
	default:
		return cutParam(fmt.Sprintf("%v", v))
	}
}

// cutParam shortens s to maxParamBytes on a rune boundary.
func cutParam(s string) string {
	if len(s) <= maxParamBytes {
		return s
	}
	cut := maxParamBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}
