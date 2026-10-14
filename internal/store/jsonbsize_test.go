package store

import (
	"strings"
	"testing"
)

// The rendered lengths are what Postgres 17 prints for '<tok>'::jsonb::text.
func TestStoredSizeNumbers(t *testing.T) {
	cases := []struct {
		tok      string
		rendered int
	}{
		{"1", 1},
		{"123.4500", 8},
		{"1e308", 309},
		{"1.000e-5", 10},
		{"0.000e-5", 10},
		{"-1.5e2", 4},
		{"12.50e1", 5},
		{"1E+3", 4},
		{"1e-300", 302},
		{"5e0", 1},
		{"1e99999999999999999999", 0}, // Postgres refuses it; not counted
	}
	for _, c := range cases {
		raw := []byte(`{"n":` + c.tok + `}`)
		want := int64(len(raw))
		if c.rendered > len(c.tok) {
			want += int64(c.rendered - len(c.tok))
		}
		if got := StoredSize(raw); got != want {
			t.Errorf("%s: StoredSize = %d, want %d", c.tok, got, want)
		}
	}
}

func TestStoredSizeIgnoresStrings(t *testing.T) {
	raw := []byte(`{"s":"1e308 in a string","n":1e3}`)
	if got, want := StoredSize(raw), int64(len(raw))+1; got != want {
		t.Fatalf("StoredSize = %d, want %d", got, want)
	}
}

// QA sweep 2026-10-14: a 1 MiB body of 1e308 tokens is ~54 MB once stored.
func TestStoredSizeAmplification(t *testing.T) {
	raw := []byte("[" + strings.TrimSuffix(strings.Repeat("1e308,", 1<<20/6), ",") + "]")
	if got := StoredSize(raw); got < 50*int64(len(raw)) {
		t.Fatalf("StoredSize = %d for %d bytes, want the ~52x expansion", got, len(raw))
	}
}
