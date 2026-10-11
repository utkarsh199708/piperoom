package snowflake

import (
	"math/big"
	"testing"
	"time"
)

func TestToJSON(t *testing.T) {
	ts := time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		in   any
		want any
	}{
		{"nil", nil, nil},
		{"bool", true, true},
		{"string", "hi", "hi"},
		{"bytes", []byte("hi"), "hi"},
		{"time", ts, "2026-03-04T12:00:00Z"},
		{"int64", int64(42), "42"},
		{"float64 no exponent", float64(12345.5), "12345.5"},
		{"big.Int", big.NewInt(9223372036854775807), "9223372036854775807"},
		{"big.Float", newBigFloat("12345678901234567890.123"), "12345678901234567890.123"},
		{"nil big.Int", (*big.Int)(nil), nil},
		{"nil big.Float", (*big.Float)(nil), nil},
	}
	for _, tc := range cases {
		got := toJSON(tc.in)
		if got != tc.want {
			t.Errorf("%s: toJSON(%v) = %v (%T), want %v (%T)", tc.name, tc.in, got, got, tc.want, tc.want)
		}
	}
}

func newBigFloat(s string) *big.Float {
	f, _, err := big.ParseFloat(s, 10, 200, big.ToNearestEven)
	if err != nil {
		panic(err)
	}
	return f
}
