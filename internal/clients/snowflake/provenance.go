package snowflake

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/snowflakedb/gosnowflake/v2"

	"piperoom/internal/tools"
)

// maxParamBytes cuts a long string argument in a receipt.
const maxParamBytes = 64

// targetFromDSN names the account and database for a receipt. It reads only
// those two fields: the user, password and token in the DSN never reach it,
// and a parse failure is dropped without its message (it can echo the DSN).
func targetFromDSN(dsn string) string {
	cfg, err := gosnowflake.ParseDSN(dsn)
	if err != nil || cfg == nil {
		return "snowflake"
	}
	return "snowflake " + cfg.Account + "/" + cfg.Database
}

// query runs the statement and records it as a provenance call, on success and
// on error. Text keeps the "?" placeholders; values only appear as redacted
// parameters.
func (d *DB) query(ctx context.Context, sql string, args ...any) (sqlRows, error) {
	start := time.Now()
	rows, err := d.q.QueryContext(ctx, sql, args...)
	tools.Record(ctx, tools.Call{
		Kind:           tools.CallSQL,
		Target:         d.target,
		Text:           sqlText(sql),
		ParamsRedacted: redactArgs(args),
		At:             start,
		TookMS:         time.Since(start).Milliseconds(),
	})
	return rows, err
}

// sqlText collapses whitespace runs to one space and keeps placeholders.
func sqlText(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}

// redactArgs renders bind arguments as "?n=value". Names and keys are shown as
// they are; a long string is cut; bytes are never shown. The primary key of
// Row stays, like in the tool summary.
func redactArgs(args []any) []string {
	var out []string
	for i, a := range args {
		out = append(out, fmt.Sprintf("?%d=%s", i+1, renderArg(a)))
	}
	return out
}

func renderArg(a any) string {
	switch v := a.(type) {
	case nil:
		return "NULL"
	case string:
		return cutParam(v)
	case []string:
		parts := make([]string, len(v))
		for i, s := range v {
			parts[i] = cutParam(s)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case time.Time:
		return v.UTC().Format(time.RFC3339)
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
