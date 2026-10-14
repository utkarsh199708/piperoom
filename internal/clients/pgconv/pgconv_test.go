package pgconv

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestNumericString(t *testing.T) {
	cases := []struct {
		n    pgtype.Numeric
		want string
	}{
		{pgtype.Numeric{Int: big.NewInt(1234), Exp: -2, Valid: true}, "12.34"},
		{pgtype.Numeric{Int: big.NewInt(-5), Exp: -2, Valid: true}, "-0.05"},
		{pgtype.Numeric{Int: big.NewInt(42), Exp: 0, Valid: true}, "42"},
		{pgtype.Numeric{Int: big.NewInt(7), Exp: 2, Valid: true}, "700"},
		{pgtype.Numeric{NaN: true, Valid: true}, "NaN"},
	}
	for _, tc := range cases {
		if got := NumericString(tc.n); got != tc.want {
			t.Fatalf("NumericString(%+v) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

func TestToJSON(t *testing.T) {
	ts := time.Date(2026, 10, 11, 10, 59, 3, 0, time.UTC)
	cases := []struct {
		in   any
		want any
	}{
		{int64(123), "123"},
		{int32(7), "7"},
		{float64(3.5), "3.5"},
		{ts, ts.Format(time.RFC3339Nano)},
		{"hello", "hello"},
		{true, true},
		{nil, nil},
		{[]byte("abc"), `\x616263`},
		{[16]byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x40, 0x00, 0x00}, "123e4567-e89b-12d3-a456-426614400000"},
		{pgtype.UUID{Bytes: [16]byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x40, 0x00, 0x00}, Valid: true}, "123e4567-e89b-12d3-a456-426614400000"},
		{map[string]any{"a": 1}, `{"a":1}`},
		{[]string{"x", "y"}, `["x","y"]`},
	}
	for _, tc := range cases {
		if got := ToJSON(tc.in); got != tc.want {
			t.Fatalf("ToJSON(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestWithDeadlineCapsLongerParent(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	ctx, cf := WithDeadline(parent, 10*time.Second)
	defer cf()
	dl, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected a deadline")
	}
	if d := time.Until(dl); d > 10*time.Second {
		t.Fatalf("deadline = %v, want <= 10s (must not extend beyond 10s)", d)
	}
}

func TestWithDeadlineRespectsEarlierParent(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx, cf := WithDeadline(parent, 10*time.Second)
	defer cf()
	dl, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected a deadline")
	}
	if d := time.Until(dl); d > 2*time.Second {
		t.Fatalf("deadline = %v, want <= 2s (parent is earlier)", d)
	}
}
