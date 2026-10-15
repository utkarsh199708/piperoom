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
type decodeRow struct {
	text   *string
	binary []byte // used instead of text when set
}

func (r decodeRow) Scan(dest ...any) error {
	var src []byte
	if r.text != nil {
		src = []byte(*r.text)
	}
	if r.binary != nil {
		return pgtype.NewMap().Scan(pgtype.TimestamptzOID, pgtype.BinaryFormatCode, r.binary, dest[0])
	}
	return pgtype.NewMap().Scan(pgtype.TimestamptzOID, pgtype.TextFormatCode, src, dest[0])
}

type decodeQuerier struct {
	fakeQuerier
	text   *string
	binary []byte
}

func (q *decodeQuerier) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return decodeRow{text: q.text, binary: q.binary}
}

// An 'infinity' max used to fail the scan, so the sink read as unreachable.
func TestFreshnessInfinity(t *testing.T) {
	s := func(v string) *string { return &v }
	cases := []struct {
		name   string
		text   *string
		binary []byte // the wire format pgx uses by default: int64 microseconds
		future bool
		zero   bool
		want   time.Time // exact value when set
	}{
		{name: "infinity is bad data in the future", text: s("infinity"), future: true},
		{name: "-infinity is no usable value", text: s("-infinity"), zero: true},
		{name: "null is an empty table", text: nil, zero: true},
		{name: "normal value", text: s("2026-10-14 03:00:00+00"), want: time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)},
		{name: "binary infinity", binary: []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, future: true},
		{name: "binary -infinity", binary: []byte{0x80, 0, 0, 0, 0, 0, 0, 0}, zero: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := testDB(&decodeQuerier{text: c.text, binary: c.binary})
			ts, err := d.Freshness(context.Background(), "analytics.orders", "loaded_at")
			if err != nil {
				t.Fatalf("Freshness: %v", err)
			}
			if ts.IsZero() != c.zero {
				t.Fatalf("zero = %v, want %v (ts %v)", ts.IsZero(), c.zero, ts)
			}
			if !c.want.IsZero() && !ts.Equal(c.want) {
				t.Fatalf("ts = %v, want %v", ts, c.want)
			}
			if c.future && !ts.After(time.Now().Add(365*24*time.Hour)) {
				t.Fatalf("infinity must read as far in the future, got %v", ts)
			}
		})
	}
}
