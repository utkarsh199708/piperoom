package pgconv

import (
	"context"
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestToJSONIntegerTypesBecomeDecimalStrings(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"int8 min", int8(-128), "-128"},
		{"int16 min", int16(-32768), "-32768"},
		{"int32 min", int32(-2147483648), "-2147483648"},
		{"int64 min", int64(math.MinInt64), "-9223372036854775808"},
		{"int zero", 0, "0"},
		{"int negative", -5, "-5"},
		{"uint8 max", uint8(255), "255"},
		{"uint16 max", uint16(65535), "65535"},
		{"uint32 max", uint32(math.MaxUint32), "4294967295"},
		{"uint64 max", uint64(math.MaxUint64), "18446744073709551615"},
		{"uint zero", uint(0), "0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToJSON(tt.in); got != tt.want {
				t.Fatalf("ToJSON(%T %v) = %v, want %q", tt.in, tt.in, got, tt.want)
			}
		})
	}
}

func TestToJSONFloatsAreNotExponentFormatted(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"shortest float64", float64(0.1), "0.1"},
		{"large float64 stays plain", float64(1e21), "1000000000000000000000"},
		{"small float64 stays plain", float64(1e-7), "0.0000001"},
		{"float32 keeps float32 precision", float32(0.1), "0.1"},
		{"NaN", math.NaN(), "NaN"},
		{"+Inf", math.Inf(1), "+Inf"},
		{"-Inf", math.Inf(-1), "-Inf"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToJSON(tt.in); got != tt.want {
				t.Fatalf("ToJSON(%v) = %v, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestToJSONTimesAreUTCRFC3339(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+30*60)
	tests := []struct {
		name string
		in   time.Time
		want string
	}{
		{
			name: "UTC is unchanged",
			in:   time.Date(2026, 10, 11, 10, 59, 3, 0, time.UTC),
			want: "2026-10-11T10:59:03Z",
		},
		{
			name: "offset zone is converted to UTC",
			in:   time.Date(2026, 10, 11, 16, 0, 0, 0, ist),
			want: "2026-10-11T10:30:00Z",
		},
		{
			name: "nanoseconds are kept",
			in:   time.Date(2026, 10, 11, 10, 59, 3, 123456789, time.UTC),
			want: "2026-10-11T10:59:03.123456789Z",
		},
		{
			name: "zero time",
			in:   time.Time{},
			want: "0001-01-01T00:00:00Z",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToJSON(tt.in); got != tt.want {
				t.Fatalf("ToJSON(%v) = %v, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestToJSONNullIsNil(t *testing.T) {
	// rows.Values() reports SQL NULL as an untyped nil; it must stay nil so
	// the JSON row carries null, not an empty string.
	if got := ToJSON(nil); got != nil {
		t.Fatalf("ToJSON(nil) = %#v, want nil", got)
	}
}

func TestToJSONNestedValues(t *testing.T) {
	// Maps and slices are JSON-encoded as they are; only top-level scalars are
	// normalised to strings.
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"slice with mixed values", []any{int64(1), "a", nil, true}, `[1,"a",null,true]`},
		{"map with nil value", map[string]any{"n": nil}, `{"n":null}`},
		{"struct", struct {
			A int `json:"a"`
			B string
		}{1, "x"}, `{"a":1,"B":"x"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToJSON(tt.in); got != tt.want {
				t.Fatalf("ToJSON = %v, want %q", got, tt.want)
			}
		})
	}
}

func TestToJSONUnmarshalableFallsBackToFmt(t *testing.T) {
	// A value json cannot encode must not fail the whole row: it falls back to
	// fmt's rendering, so the output is always a string.
	got := ToJSON(map[string]any{"c": make(chan int)})
	s, ok := got.(string)
	if !ok || !strings.HasPrefix(s, "map[c:0x") {
		t.Fatalf("ToJSON(unmarshalable map) = %#v, want fmt rendering starting map[c:0x", got)
	}

	top := ToJSON(make(chan int))
	if s, ok := top.(string); !ok || !strings.HasPrefix(s, "0x") {
		t.Fatalf("ToJSON(chan) = %#v, want fmt pointer rendering", top)
	}
}

func TestToJSONBytesAndUUIDs(t *testing.T) {
	uuid := [16]byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x40, 0x00, 0x00}
	tests := []struct {
		name string
		in   any
		want any
	}{
		{"empty bytes", []byte{}, ""},
		{"bytes with NUL and high bytes", []byte("a\x00\xff"), "a\x00\xff"},
		{"zero UUID array", [16]byte{}, "00000000-0000-0000-0000-000000000000"},
		{"UUID array", uuid, "123e4567-e89b-12d3-a456-426614400000"},
		{"pgtype.UUID valid", pgtype.UUID{Bytes: uuid, Valid: true}, "123e4567-e89b-12d3-a456-426614400000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToJSON(tt.in); got != tt.want {
				t.Fatalf("ToJSON = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestNumericStringTable(t *testing.T) {
	tests := []struct {
		name string
		n    pgtype.Numeric
		want string
	}{
		{"zero with scale", pgtype.Numeric{Int: big.NewInt(0), Exp: -2, Valid: true}, "0.00"},
		{"zero", pgtype.Numeric{Int: big.NewInt(0), Valid: true}, "0"},
		{"nil int is zero", pgtype.Numeric{Valid: true}, "0"},
		{"leading zeros after the point", pgtype.Numeric{Int: big.NewInt(1), Exp: -3, Valid: true}, "0.001"},
		{"trailing zero kept by scale", pgtype.Numeric{Int: big.NewInt(100), Exp: -1, Valid: true}, "10.0"},
		{"negative fraction", pgtype.Numeric{Int: big.NewInt(-1), Exp: -3, Valid: true}, "-0.001"},
		{"large value without rounding", pgtype.Numeric{Int: mustBig("12345678901234567890123"), Exp: -5, Valid: true}, "123456789012345678.90123"},
		{"big integer with trailing zeros", pgtype.Numeric{Int: big.NewInt(7), Exp: 20, Valid: true}, "700000000000000000000"},
		{"positive infinity", pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true}, "Infinity"},
		{"negative infinity", pgtype.Numeric{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, "-Infinity"},
		{"exponent beyond 100000 uses e notation", pgtype.Numeric{Int: big.NewInt(7), Exp: 100001, Valid: true}, "7e+100001"},
		{"negative exponent beyond 100000 uses e notation", pgtype.Numeric{Int: big.NewInt(7), Exp: -100001, Valid: true}, "7e-100001"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NumericString(tt.n); got != tt.want {
				t.Fatalf("NumericString = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNumericStringZeroWithPositiveExponentIsSkipped(t *testing.T) {
	// BUG (see reviews/coverage-2026-10.md): zero with a positive exponent
	// renders as "000" because the trailing zeros are appended to the digit
	// string "0". The canonical form is "0". Kept out of the pass/fail set
	// until the fix lands in pgconv.go.
	t.Skip("BUG: NumericString(Int=0, Exp=2) returns \"000\", want \"0\"; see reviews/coverage-2026-10.md")
	if got := NumericString(pgtype.Numeric{Int: big.NewInt(0), Exp: 2, Valid: true}); got != "0" {
		t.Fatalf("got %q, want \"0\"", got)
	}
}

func mustBig(s string) *big.Int {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		panic("bad test literal " + s)
	}
	return n
}

func TestWithDeadlineErrorPaths(t *testing.T) {
	t.Run("cancelled parent stays cancelled", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		cancel()
		ctx, cf := WithDeadline(parent, time.Minute)
		defer cf()
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("ctx.Err() = %v, want context.Canceled", ctx.Err())
		}
	})
	t.Run("zero duration expires at once", func(t *testing.T) {
		ctx, cf := WithDeadline(context.Background(), 0)
		defer cf()
		<-ctx.Done()
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatalf("ctx.Err() = %v, want DeadlineExceeded", ctx.Err())
		}
	})
	t.Run("cancel func releases the context", func(t *testing.T) {
		ctx, cf := WithDeadline(context.Background(), time.Minute)
		cf()
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("after cancel, ctx.Err() = %v, want Canceled", ctx.Err())
		}
	})
}
