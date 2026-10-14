package events

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestTruncateData(t *testing.T) {
	long := json.RawMessage(`{"rows":"` + strings.Repeat("a", 200) + `"}`)
	multi := json.RawMessage(`"` + strings.Repeat("é", 100) + `"`)          // 2-byte runes
	bad := append(json.RawMessage(`"`+strings.Repeat("b", 90)), 0xe2, 0x82) // cut 3-byte rune
	tests := []struct {
		name string
		raw  json.RawMessage
		max  int
		cut  bool
	}{
		{"under cap", json.RawMessage(`{"a":1}`), 64, false},
		{"at cap", json.RawMessage(`{"a":1}`), 7, false},
		{"over cap", long, 100, true},
		{"max 0 never cuts", long, 0, false},
		{"negative max never cuts", long, -1, false},
		{"nil raw", nil, 10, false},
		{"multibyte boundary", multi, 101, true},
		{"invalid utf-8 tail", bad, 80, true},
		{"nothing fits", long, 10, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, cut := TruncateData(tc.raw, tc.max)
			if cut != tc.cut {
				t.Fatalf("cut = %v, want %v", cut, tc.cut)
			}
			if !tc.cut {
				if string(out) != string(tc.raw) {
					t.Fatalf("raw changed: %q", out)
				}
				return
			}
			var td TruncatedData
			if err := json.Unmarshal(out, &td); err != nil {
				t.Fatalf("output is not a TruncatedData: %v: %s", err, out)
			}
			if !td.Truncated || td.OriginalBytes != int64(len(tc.raw)) {
				t.Fatalf("got truncated=%v original_bytes=%d, want true and %d", td.Truncated, td.OriginalBytes, len(tc.raw))
			}
			if !utf8.ValidString(td.Preview) {
				t.Fatalf("preview is not valid UTF-8: %q", td.Preview)
			}
			if !strings.HasPrefix(string(tc.raw), td.Preview) && tc.name != "invalid utf-8 tail" {
				t.Fatalf("preview %q is not a prefix of the input", td.Preview)
			}
			// The whole object fits unless even an empty preview is too big.
			if len(out) > tc.max && td.Preview != "" {
				t.Fatalf("output %d bytes > max %d with a non-empty preview", len(out), tc.max)
			}
			if tc.name == "nothing fits" && td.Preview != "" {
				t.Fatalf("want empty preview when nothing fits, got %q", td.Preview)
			}
		})
	}
}

// The multibyte case must cut between runes, never inside one: the preview is
// the longest run of whole runes that fits.
func TestTruncateDataRuneBoundary(t *testing.T) {
	raw := json.RawMessage(strings.Repeat("é", 50)) // 100 bytes
	out, cut := TruncateData(raw, 60)
	if !cut {
		t.Fatal("want cut")
	}
	var td TruncatedData
	if err := json.Unmarshal(out, &td); err != nil {
		t.Fatal(err)
	}
	if td.Preview == "" || len(td.Preview)%2 != 0 || strings.Contains(td.Preview, "�") {
		t.Fatalf("preview split a rune: %q", td.Preview)
	}
}

// An invalid byte at the cut point walks back to the previous rune start, and
// a lone invalid byte inside the preview is encoded as U+FFFD by encoding/json
// rather than failing.
func TestTruncateDataInvalidUTF8(t *testing.T) {
	raw := append(json.RawMessage(strings.Repeat("x", 20)), 0xff)
	raw = append(raw, []byte(strings.Repeat("y", 20))...)
	out, cut := TruncateData(raw, 30)
	if !cut {
		t.Fatal("want cut")
	}
	if !json.Valid(out) {
		t.Fatalf("invalid JSON: %s", out)
	}
	var td TruncatedData
	if err := json.Unmarshal(out, &td); err != nil {
		t.Fatal(err)
	}
	if !utf8.ValidString(td.Preview) {
		t.Fatalf("preview not valid UTF-8: %q", td.Preview)
	}
}
