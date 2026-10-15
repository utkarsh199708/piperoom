// Package pgconv shares helpers used by the read-only Postgres-backed clients
// (postgres and warehouse): converting pgx-decoded values into JSON-friendly
// values, rendering numerics without float rounding, and applying a per-call
// deadline.
package pgconv

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// ToJSON converts a decoded pgx value into a JSON-friendly one: numbers become
// strings, times become RFC 3339 strings, UUIDs become their canonical form,
// and maps/slices become JSON.
func ToJSON(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	case int64:
		return strconv.FormatInt(t, 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int16:
		return strconv.FormatInt(int64(t), 10)
	case int8:
		return strconv.FormatInt(int64(t), 10)
	case int:
		return strconv.Itoa(t)
	case uint64:
		return strconv.FormatUint(t, 10)
	case uint32:
		return strconv.FormatUint(uint64(t), 10)
	case uint16:
		return strconv.FormatUint(uint64(t), 10)
	case uint8:
		return strconv.FormatUint(uint64(t), 10)
	case uint:
		return strconv.FormatUint(uint64(t), 10)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32)
	case pgtype.Numeric:
		return NumericString(t)
	case pgtype.InfinityModifier:
		// pgx decodes an infinite timestamp, timestamptz or date to this
		// type; JSON-marshalled it would read as the number 1 or -1.
		switch t {
		case pgtype.Infinity:
			return "infinity"
		case pgtype.NegativeInfinity:
			return "-infinity"
		}
		return ""
	case []byte:
		return string(t)
	case string:
		return t
	case bool:
		return t
	case [16]byte:
		return formatUUID(t)
	case pgtype.UUID:
		return formatUUID(t.Bytes)
	default:
		if b, err := json.Marshal(t); err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", t)
	}
}

// NumericString renders a PostgreSQL numeric without float rounding.
func NumericString(n pgtype.Numeric) string {
	if !n.Valid {
		return ""
	}
	if n.NaN {
		return "NaN"
	}
	switch n.InfinityModifier {
	case pgtype.Infinity:
		return "Infinity"
	case pgtype.NegativeInfinity:
		return "-Infinity"
	}
	if n.Int == nil {
		return "0"
	}
	neg := n.Int.Sign() < 0
	abs := new(big.Int).Abs(n.Int)
	s := abs.String()
	if n.Exp > 0 && abs.Sign() != 0 {
		e := int(n.Exp)
		if e > 100000 {
			return s + "e+" + strconv.Itoa(e)
		}
		s += strings.Repeat("0", e)
	} else if n.Exp < 0 {
		e := -int(n.Exp)
		if e > 100000 {
			return s + "e" + strconv.Itoa(int(n.Exp))
		}
		point := len(s) - e
		if point <= 0 {
			s = "0." + strings.Repeat("0", -point) + s
		} else {
			s = s[:point] + "." + s[point:]
		}
	}
	if neg {
		s = "-" + s
	}
	return s
}

func formatUUID(u [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

// WithDeadline applies d as a timeout, capped at the parent's deadline when it
// is earlier. Unlike returning the parent context unchanged, this never lets a
// call run longer than d.
func WithDeadline(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}
