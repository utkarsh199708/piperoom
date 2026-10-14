// Package pgconv shares helpers used by the read-only Postgres-backed clients
// (postgres and warehouse): converting pgx-decoded values into JSON-friendly
// values, rendering numerics without float rounding, and applying a per-call
// deadline.
package pgconv

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// ToJSON converts a decoded pgx value into a JSON-friendly one: numbers become
// strings, times become RFC 3339 strings, UUIDs become their canonical form,
// and maps/slices become JSON. It does not know the column type, so a
// timestamp without time zone is rendered like a timestamptz (UTC, "Z");
// callers that have the column's type OID use ValueToJSON instead.
func ToJSON(v any) any {
	return ValueToJSON(0, v)
}

// ValueToJSON is ToJSON for a value whose Postgres type OID is known (from
// pgconn.FieldDescription.DataTypeOID; 0 means unknown). The OID decides what
// the Go value alone cannot tell apart:
//   - timestamp (without time zone) is "2006-01-02T15:04:05.999999999", with
//     no "Z", because it is not a UTC instant; timestamptz stays RFC 3339 UTC;
//   - date is "2006-01-02";
//   - inet renders a host address without its /32 or /128, like Postgres; cidr
//     always keeps the mask.
//
// Every other non-JSON type is rendered in its Postgres text form, never as a
// Go struct: ±infinity times are "infinity"/"-infinity", bytea is Postgres's
// hex format (`\x0102ff`), and time, interval, point, bit strings, ranges and
// multiranges read as Postgres prints them.
func ValueToJSON(oid uint32, v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case time.Time:
		return formatTime(oid, t)
	case pgtype.InfinityModifier:
		return infinityString(t)
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
	case []byte:
		// bytea: Postgres's hex output format. A raw string(t) would put
		// arbitrary (often invalid UTF-8) bytes into JSON.
		return `\x` + hex.EncodeToString(t)
	case string:
		return t
	case bool:
		return t
	case [16]byte:
		return formatUUID(t)
	case pgtype.UUID:
		return formatUUID(t.Bytes)
	case netip.Prefix:
		return formatPrefix(oid, t)
	case pgtype.Time:
		return formatTimeOfDay(t)
	case pgtype.Interval:
		return formatInterval(t)
	case pgtype.Point:
		return "(" + formatFloat8(t.P.X) + "," + formatFloat8(t.P.Y) + ")"
	case pgtype.Range[any]:
		return formatRange(rangeElemOID(oid), t)
	case pgtype.Multirange[pgtype.Range[any]]:
		parts := make([]string, len(t))
		for i, r := range t {
			parts[i] = formatRange(rangeElemOID(multirangeRangeOID(oid)), r)
		}
		return "{" + strings.Join(parts, ",") + "}"
	case map[string]any:
		return marshalOrFmt(normalizeJSON(t))
	case []any:
		return marshalOrFmt(normalizeJSON(t))
	}
	// pgtype values with a text encoding (bits, box, circle, line, lseg,
	// path, polygon, tid, hstore, tsvector): their driver.Value is the
	// Postgres text form.
	if vr, ok := v.(driver.Valuer); ok {
		if dv, err := vr.Value(); err == nil {
			if s, ok := dv.(string); ok {
				return s
			}
		}
	}
	// net.HardwareAddr (macaddr) and similar text types.
	if s, ok := v.(fmt.Stringer); ok {
		return s.String()
	}
	return marshalOrFmt(v)
}

// marshalOrFmt JSON-encodes v, falling back to fmt so a row never fails on one
// odd value.
func marshalOrFmt(v any) any {
	if b, err := json.Marshal(v); err == nil {
		return string(b)
	}
	return fmt.Sprintf("%v", v)
}

// normalizeJSON converts the scalars inside a decoded json/jsonb document or a
// Postgres array that json.Marshal would render lossily or as a Go struct
// (numeric, ±infinity, bytea, intervals, ...) to their text form. json.Number
// (from a codec registered with RegisterJSONNumber) is kept, so jsonb numbers
// keep every digit.
func normalizeJSON(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = normalizeJSON(e)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = normalizeJSON(e)
		}
		return out
	case nil, string, bool, json.Number, float64, float32,
		int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return t
	default:
		if _, err := json.Marshal(t); err != nil {
			// Leave it: the whole document then takes the fmt fallback.
			return t
		}
		return ValueToJSON(0, t)
	}
}

func infinityString(m pgtype.InfinityModifier) string {
	switch m {
	case pgtype.Infinity:
		return "infinity"
	case pgtype.NegativeInfinity:
		return "-infinity"
	}
	return ""
}

func formatTime(oid uint32, t time.Time) string {
	switch oid {
	case pgtype.TimestampOID:
		// pgx decodes a timestamp without time zone into a UTC time.Time; the
		// wall clock is the value, there is no zone to print.
		return t.Format("2006-01-02T15:04:05.999999999")
	case pgtype.DateOID:
		return t.Format("2006-01-02")
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// formatPrefix renders inet like Postgres: a host (full-length mask) without
// the mask. cidr keeps it always.
func formatPrefix(oid uint32, p netip.Prefix) string {
	if oid != pgtype.CIDROID && p.Bits() == p.Addr().BitLen() {
		return p.Addr().String()
	}
	return p.String()
}

// formatTimeOfDay renders a time like Postgres: HH:MM:SS plus the fraction
// without trailing zeros.
func formatTimeOfDay(t pgtype.Time) string {
	us := t.Microseconds
	h := us / 3_600_000_000
	m := (us % 3_600_000_000) / 60_000_000
	return fmt.Sprintf("%02d:%02d:", h, m) + formatSeconds(us%60_000_000)
}

// formatSeconds renders |us| microseconds as SS[.ffffff] with the trailing
// zeros of the fraction trimmed (Postgres AppendSeconds).
func formatSeconds(us int64) string {
	if us < 0 {
		us = -us
	}
	s := fmt.Sprintf("%02d", us/1_000_000)
	if frac := us % 1_000_000; frac != 0 {
		s += "." + strings.TrimRight(fmt.Sprintf("%06d", frac), "0")
	}
	return s
}

// formatInterval renders an interval in Postgres's default IntervalStyle
// ("postgres"), e.g. "1 year 2 mons -3 days +04:05:06.5".
func formatInterval(iv pgtype.Interval) string {
	var sb strings.Builder
	isZero, isBefore := true, false
	part := func(v int64, unit string) {
		if v == 0 {
			return
		}
		if !isZero {
			sb.WriteByte(' ')
		}
		if isBefore && v > 0 {
			sb.WriteByte('+')
		}
		sb.WriteString(strconv.FormatInt(v, 10) + " " + unit)
		if v != 1 {
			sb.WriteByte('s')
		}
		isBefore = v < 0
		isZero = false
	}
	part(int64(iv.Months/12), "year")
	part(int64(iv.Months%12), "mon")
	part(int64(iv.Days), "day")
	us := iv.Microseconds
	if isZero || us != 0 {
		if !isZero {
			sb.WriteByte(' ')
		}
		switch {
		case us < 0:
			sb.WriteByte('-')
		case isBefore:
			sb.WriteByte('+')
		}
		abs := us
		if abs < 0 {
			abs = -abs
		}
		h := abs / 3_600_000_000
		m := (abs % 3_600_000_000) / 60_000_000
		sb.WriteString(fmt.Sprintf("%02d:%02d:", h, m) + formatSeconds(abs%60_000_000))
	}
	return sb.String()
}

// formatFloat8 renders a float like Postgres float8out: shortest exact
// digits, exponent form below 1e-4 and from 1e15.
func formatFloat8(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	if a := math.Abs(f); a != 0 && (a < 1e-4 || a >= 1e15) {
		return strconv.FormatFloat(f, 'e', -1, 64)
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// rangeElemOID maps the built-in range types whose bounds are times to their
// element type, so a daterange bound reads as a date and a tsrange bound has
// no "Z". Other ranges need no OID.
func rangeElemOID(oid uint32) uint32 {
	switch oid {
	case pgtype.DaterangeOID:
		return pgtype.DateOID
	case pgtype.TsrangeOID:
		return pgtype.TimestampOID
	case pgtype.TstzrangeOID:
		return pgtype.TimestamptzOID
	}
	return 0
}

func multirangeRangeOID(oid uint32) uint32 {
	switch oid {
	case pgtype.DatemultirangeOID:
		return pgtype.DaterangeOID
	case pgtype.TsmultirangeOID:
		return pgtype.TsrangeOID
	case pgtype.TstzmultirangeOID:
		return pgtype.TstzrangeOID
	}
	return 0
}

// formatRange renders a range like Postgres: "[1,5)", "(,4)", "empty".
func formatRange(elemOID uint32, r pgtype.Range[any]) string {
	if r.LowerType == pgtype.Empty {
		return "empty"
	}
	var sb strings.Builder
	if r.LowerType == pgtype.Inclusive {
		sb.WriteByte('[')
	} else {
		sb.WriteByte('(')
	}
	if r.LowerType != pgtype.Unbounded {
		sb.WriteString(rangeBound(elemOID, r.Lower))
	}
	sb.WriteByte(',')
	if r.UpperType != pgtype.Unbounded {
		sb.WriteString(rangeBound(elemOID, r.Upper))
	}
	if r.UpperType == pgtype.Inclusive {
		sb.WriteByte(']')
	} else {
		sb.WriteByte(')')
	}
	return sb.String()
}

// rangeBound renders one bound, double-quoted when it holds a character that
// is special in range syntax (range_out's quoting rule).
func rangeBound(elemOID uint32, v any) string {
	s := fmt.Sprint(ValueToJSON(elemOID, v))
	if s != "" && !strings.ContainsAny(s, "\\\"()[], \t\n\r") {
		return s
	}
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return `"` + s + `"`
}

// RegisterJSONNumber makes json and jsonb columns (and their arrays) decode
// with json.Decoder.UseNumber, so a number such as 12345678901234567890.5
// keeps every digit instead of being rounded through float64. Call it from
// pgxpool.Config.AfterConnect with conn.TypeMap().
func RegisterJSONNumber(m *pgtype.Map) {
	jsonT := &pgtype.Type{Name: "json", OID: pgtype.JSONOID, Codec: &pgtype.JSONCodec{Marshal: json.Marshal, Unmarshal: unmarshalUseNumber}}
	jsonbT := &pgtype.Type{Name: "jsonb", OID: pgtype.JSONBOID, Codec: &pgtype.JSONBCodec{Marshal: json.Marshal, Unmarshal: unmarshalUseNumber}}
	m.RegisterType(jsonT)
	m.RegisterType(jsonbT)
	m.RegisterType(&pgtype.Type{Name: "_json", OID: pgtype.JSONArrayOID, Codec: &pgtype.ArrayCodec{ElementType: jsonT}})
	m.RegisterType(&pgtype.Type{Name: "_jsonb", OID: pgtype.JSONBArrayOID, Codec: &pgtype.ArrayCodec{ElementType: jsonbT}})
}

func unmarshalUseNumber(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("pgconv: decode json: %w", err)
	}
	if dec.More() {
		return fmt.Errorf("pgconv: decode json: trailing data after the value")
	}
	return nil
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
	if n.Int == nil || (n.Int.Sign() == 0 && n.Exp >= 0) {
		// A zero with a positive exponent is "0", not "000": appending the
		// exponent's zeros only makes sense for a non-zero digit string.
		return "0"
	}
	neg := n.Int.Sign() < 0
	abs := new(big.Int).Abs(n.Int)
	s := abs.String()
	if n.Exp > 0 {
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
