package httpapi

import (
	"net/http/httptest"
	"testing"
)

// TestParseLimitDistinguishesAbsentFromZero: an absent (or empty) limit is the
// documented default 500, while an explicit 0 reaches the service as the empty
// page (KAN-395). Negatives and non-numbers are validation errors; a value
// over the cap is left for the service to clamp.
func TestParseLimitDistinguishesAbsentFromZero(t *testing.T) {
	cases := []struct {
		name    string
		query   string
		want    int
		wantErr bool
	}{
		{"absent", "", 500, false},
		{"empty", "?limit=", 500, false},
		{"zero", "?limit=0", 0, false},
		{"one", "?limit=1", 1, false},
		{"at the cap", "?limit=1000", 1000, false},
		{"over the cap", "?limit=1001", 1001, false},
		{"negative", "?limit=-1", 0, true},
		{"not a number", "?limit=abc", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/api/rooms/r1/events"+tc.query, nil)
			got, err := parseLimit(r)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseLimit(%q) = %d, want an error", tc.query, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseLimit(%q): %v", tc.query, err)
			}
			if got != tc.want {
				t.Fatalf("parseLimit(%q) = %d, want %d", tc.query, got, tc.want)
			}
		})
	}
}
