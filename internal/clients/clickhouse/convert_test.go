package clickhouse

import (
	"testing"
	"time"
)

func TestTimestampParsesClickHouseDateTimes(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  time.Time
	}{
		{
			name:  "RFC3339 with offset and nanoseconds",
			input: "2026-10-19T12:00:01.123456789+05:30",
			want:  time.Date(2026, 10, 19, 6, 30, 1, 123456789, time.UTC),
		},
		{
			name:  "ClickHouse text with offset and fractional seconds",
			input: []byte("2026-10-19 12:00:01.123456 +05:30"),
			want:  time.Date(2026, 10, 19, 6, 30, 1, 123456000, time.UTC),
		},
		{
			name:  "ClickHouse text without offset",
			input: "2026-10-19 12:00:01.123456",
			want:  time.Date(2026, 10, 19, 12, 0, 1, 123456000, time.UTC),
		},
		{
			name:  "ClickHouse text without fraction",
			input: "2026-10-19 12:00:01",
			want:  time.Date(2026, 10, 19, 12, 0, 1, 0, time.UTC),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok, err := timestamp(tc.input)
			if err != nil || !ok || !got.Equal(tc.want) || got.Location() != time.UTC {
				t.Fatalf("timestamp(%v) = %s, %t, %v; want %s in UTC", tc.input, got.Format(time.RFC3339Nano), ok, err, tc.want.Format(time.RFC3339Nano))
			}
		})
	}
}

func TestTimestampRejectsInvalidTextAndHandlesNull(t *testing.T) {
	if got, ok, err := timestamp(nil); err != nil || ok || !got.IsZero() {
		t.Fatalf("timestamp(nil) = %s, %t, %v; want zero, false, nil", got, ok, err)
	}
	if _, ok, err := timestamp("not a ClickHouse timestamp"); err == nil || ok {
		t.Fatalf("timestamp(invalid) = %t, %v; want an error", ok, err)
	}
}
