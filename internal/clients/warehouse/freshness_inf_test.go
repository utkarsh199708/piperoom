package warehouse

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// decodeRow scans a text-format timestamptz the way pgx does on the wire, so
// the test exercises the real "infinity" decoding of the destination type.
type decodeRow struct{ text *string }

func (r decodeRow) Scan(dest ...any) error {
	var src []byte
	if r.text != nil {
		src = []byte(*r.text)
	}
	return pgtype.NewMap().Scan(pgtype.TimestamptzOID, pgtype.TextFormatCode, src, dest[0])
}

type decodeQuerier struct {
	fakeQuerier
	text *string
}

func (q *decodeQuerier) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return decodeRow{text: q.text}
}

// An 'infinity' max used to fail the scan, so the sink read as unreachable.
func TestFreshnessInfinity(t *testing.T) {
	s := func(v string) *string { return &v }
	cases := []struct {
		name   string
		text   *string
		future bool
		zero   bool
	}{
		{"infinity is bad data in the future", s("infinity"), true, false},
		{"-infinity is no usable value", s("-infinity"), false, true},
		{"null is an empty table", nil, false, true},
		{"normal value", s("2026-10-14 03:00:00+00"), false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := testDB(&decodeQuerier{text: c.text})
			ts, err := d.Freshness(context.Background(), "analytics.orders", "loaded_at")
			if err != nil {
				t.Fatalf("Freshness: %v", err)
			}
			if ts.IsZero() != c.zero {
				t.Fatalf("zero = %v, want %v (ts %v)", ts.IsZero(), c.zero, ts)
			}
			if c.future && !ts.After(time.Now().Add(365*24*time.Hour)) {
				t.Fatalf("infinity must read as far in the future, got %v", ts)
			}
		})
	}
}
