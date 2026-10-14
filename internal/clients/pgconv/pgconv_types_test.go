package pgconv

import (
	"encoding/json"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// The expected strings are what PostgreSQL 17 prints for the same values
// (psql, default DateStyle/IntervalStyle), so a row read through pgx reads like
// the row in psql.
func TestValueToJSONPostgresTextForms(t *testing.T) {
	ts := time.Date(2026, 10, 11, 10, 59, 3, 500000000, time.UTC)
	hour := int64(3_600_000_000)
	tests := []struct {
		name string
		oid  uint32
		in   any
		want any
	}{
		// ±infinity (timestamp, timestamptz and date decode to InfinityModifier).
		{"timestamptz infinity", pgtype.TimestamptzOID, pgtype.Infinity, "infinity"},
		{"timestamptz -infinity", pgtype.TimestamptzOID, pgtype.NegativeInfinity, "-infinity"},
		{"date infinity, unknown oid", 0, pgtype.Infinity, "infinity"},

		// Time zones.
		{"timestamptz is UTC with Z", pgtype.TimestamptzOID, ts, "2026-10-11T10:59:03.5Z"},
		{"timestamp without tz has no Z", pgtype.TimestampOID, ts, "2026-10-11T10:59:03.5"},
		{"date", pgtype.DateOID, time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC), "2026-10-11"},

		// bytea.
		{"bytea hex", pgtype.ByteaOID, []byte{0x01, 0x02, 0xff}, `\x0102ff`},

		// inet / cidr.
		{"inet host drops /32", pgtype.InetOID, netip.MustParsePrefix("10.0.0.1/32"), "10.0.0.1"},
		{"inet with mask", pgtype.InetOID, netip.MustParsePrefix("10.0.0.1/24"), "10.0.0.1/24"},
		{"inet v6 host", pgtype.InetOID, netip.MustParsePrefix("::1/128"), "::1"},
		{"cidr keeps /32", pgtype.CIDROID, netip.MustParsePrefix("10.0.0.1/32"), "10.0.0.1/32"},
		{"cidr net", pgtype.CIDROID, netip.MustParsePrefix("10.0.0.0/8"), "10.0.0.0/8"},
		{"macaddr", 0, net.HardwareAddr{0x08, 0, 0x2b, 1, 2, 3}, "08:00:2b:01:02:03"},

		// time.
		{"time fraction trimmed", pgtype.TimeOID, pgtype.Time{Microseconds: 12*hour + 34*60_000_000 + 56_500_000, Valid: true}, "12:34:56.5"},
		{"time midnight", pgtype.TimeOID, pgtype.Time{Valid: true}, "00:00:00"},
		{"time 24:00", pgtype.TimeOID, pgtype.Time{Microseconds: 24 * hour, Valid: true}, "24:00:00"},
		{"time one microsecond", pgtype.TimeOID, pgtype.Time{Microseconds: 12*hour + 1, Valid: true}, "12:00:00.000001"},

		// interval (IntervalStyle postgres).
		{"interval all parts", pgtype.IntervalOID, pgtype.Interval{Months: 14, Days: 3, Microseconds: 4*hour + 5*60_000_000 + 6_789_000, Valid: true}, "1 year 2 mons 3 days 04:05:06.789"},
		{"interval -1 day", pgtype.IntervalOID, pgtype.Interval{Days: -1, Valid: true}, "-1 days"},
		{"interval -1 day +1 hour", pgtype.IntervalOID, pgtype.Interval{Days: -1, Microseconds: hour, Valid: true}, "-1 days +01:00:00"},
		{"interval zero", pgtype.IntervalOID, pgtype.Interval{Valid: true}, "00:00:00"},
		{"interval 1 day -1 hour", pgtype.IntervalOID, pgtype.Interval{Days: 1, Microseconds: -hour, Valid: true}, "1 day -01:00:00"},
		{"interval all negative", pgtype.IntervalOID, pgtype.Interval{Months: -2, Days: -1, Microseconds: -1_000_000, Valid: true}, "-2 mons -1 days -00:00:01"},
		{"interval 14 mons", pgtype.IntervalOID, pgtype.Interval{Months: 14, Valid: true}, "1 year 2 mons"},
		{"interval -14 mons", pgtype.IntervalOID, pgtype.Interval{Months: -14, Valid: true}, "-1 years -2 mons"},
		{"interval 1 day", pgtype.IntervalOID, pgtype.Interval{Days: 1, Valid: true}, "1 day"},
		{"interval -1 hour", pgtype.IntervalOID, pgtype.Interval{Microseconds: -hour, Valid: true}, "-01:00:00"},
		{"interval 100 hours", pgtype.IntervalOID, pgtype.Interval{Microseconds: 100 * hour, Valid: true}, "100:00:00"},

		// point (float8out formatting).
		{"point", pgtype.PointOID, pgtype.Point{P: pgtype.Vec2{X: 1.5, Y: 2}, Valid: true}, "(1.5,2)"},
		{"point large", pgtype.PointOID, pgtype.Point{P: pgtype.Vec2{X: 123456789, Y: 1e20}, Valid: true}, "(123456789,1e+20)"},
		{"point small", pgtype.PointOID, pgtype.Point{P: pgtype.Vec2{X: 0.0001, Y: 0.00001}, Valid: true}, "(0.0001,1e-05)"},
		{"point 1e15 and 1e14", pgtype.PointOID, pgtype.Point{P: pgtype.Vec2{X: 1e15, Y: 1e14}, Valid: true}, "(1e+15,100000000000000)"},

		// bit strings.
		{"bit", pgtype.BitOID, pgtype.Bits{Bytes: []byte{0x50}, Len: 4, Valid: true}, "0101"},

		// ranges.
		{"int4range", pgtype.Int4rangeOID, pgtype.Range[any]{Lower: int32(1), Upper: int32(5), LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true}, "[1,5)"},
		{"empty range", pgtype.Int4rangeOID, pgtype.Range[any]{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Valid: true}, "empty"},
		{"unbounded lower", pgtype.Int4rangeOID, pgtype.Range[any]{Upper: int32(4), LowerType: pgtype.Unbounded, UpperType: pgtype.Exclusive, Valid: true}, "(,4)"},
		{"daterange", pgtype.DaterangeOID, pgtype.Range[any]{
			Lower: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Upper: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
			LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true}, "[2024-01-01,2024-02-01)"},
		{"tstzrange", pgtype.TstzrangeOID, pgtype.Range[any]{
			Lower: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), LowerType: pgtype.Inclusive, UpperType: pgtype.Unbounded, Valid: true}, "[2024-01-01T00:00:00Z,)"},
		{"multirange", pgtype.Int4multirangeOID, pgtype.Multirange[pgtype.Range[any]]{
			{Lower: int32(1), Upper: int32(3), LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
			{Lower: int32(5), Upper: int32(7), LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
		}, "{[1,3),[5,7)}"},
		{"textrange bound with a comma is quoted", 0, pgtype.Range[any]{Lower: "a,b", Upper: `q"`, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true}, `["a,b","q\"")`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValueToJSON(tt.oid, tt.in); got != tt.want {
				t.Fatalf("ValueToJSON(%d, %#v) = %#v, want %#v", tt.oid, tt.in, got, tt.want)
			}
		})
	}
}

func TestToJSONIsValueToJSONWithoutType(t *testing.T) {
	// Without an OID a time is a UTC instant, an infinity is still not "1".
	if got := ToJSON(pgtype.Infinity); got != "infinity" {
		t.Fatalf("ToJSON(Infinity) = %#v, want \"infinity\"", got)
	}
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if got := ToJSON(ts); got != "2026-01-02T03:04:05Z" {
		t.Fatalf("ToJSON(time) = %#v", got)
	}
}

func TestJSONNumbersKeepDigits(t *testing.T) {
	// What the jsonb codec registered by RegisterJSONNumber produces.
	var doc any
	if err := unmarshalUseNumber([]byte(`{"big":12345678901234567890.123,"n":[1e400,0.1]}`), &doc); err != nil {
		t.Fatalf("unmarshalUseNumber: %v", err)
	}
	got := ToJSON(doc)
	want := `{"big":12345678901234567890.123,"n":[1e400,0.1]}`
	if got != want {
		t.Fatalf("ToJSON(jsonb) = %v, want %s", got, want)
	}
	if err := unmarshalUseNumber([]byte(`{} {}`), &doc); err == nil {
		t.Fatal("trailing data must be an error")
	}
	if err := unmarshalUseNumber([]byte(`{`), &doc); err == nil {
		t.Fatal("bad JSON must be an error")
	}
}

func TestRegisterJSONNumberDecodesJSONB(t *testing.T) {
	m := pgtype.NewMap()
	RegisterJSONNumber(m)
	for _, oid := range []uint32{pgtype.JSONOID, pgtype.JSONBOID} {
		typ, ok := m.TypeForOID(oid)
		if !ok {
			t.Fatalf("oid %d not registered", oid)
		}
		src := []byte(`{"v":12345678901234567890}`)
		if oid == pgtype.JSONBOID {
			src = append([]byte{1}, src...) // jsonb binary format version byte
		}
		v, err := typ.Codec.DecodeValue(m, oid, pgtype.BinaryFormatCode, src)
		if err != nil {
			t.Fatalf("decode oid %d: %v", oid, err)
		}
		n, ok := v.(map[string]any)["v"].(json.Number)
		if !ok || n.String() != "12345678901234567890" {
			t.Fatalf("oid %d: v = %#v, want json.Number 12345678901234567890", oid, v)
		}
	}
}

func TestJSONArrayValuesNormalised(t *testing.T) {
	// A numeric[] decodes to []any of pgtype.Numeric: elements render exact.
	in := []any{pgtype.Numeric{Int: nil, Valid: true}, []byte{0xab}, pgtype.Infinity}
	if got := ToJSON(in); got != `["0","\\xab","infinity"]` {
		t.Fatalf("ToJSON(array) = %v", got)
	}
}
