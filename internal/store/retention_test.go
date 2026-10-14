package store_test

import (
	"strings"
	"testing"

	"piperoom/internal/store"
)

func TestParseRetentionDays(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"", 0, false},
		{"0", 0, false},
		{"00", 0, false},
		{"1", 1, false},
		{"90", 90, false},
		{" 30 ", 30, false},
		{"36500", 36500, false},
		{"36501", 0, true},
		{"99999999999999999999", 0, true},
		{"-1", 0, true},
		{"-0", 0, true},
		{"+5", 0, true},
		{"1.5", 0, true},
		{"30d", 0, true},
		{"thirty", 0, true},
		{"3 0", 0, true},
		{"1e2", 0, true},
		{"0x10", 0, true},
		{"٣٠", 0, true}, // non-ASCII digits
	} {
		got, err := store.ParseRetentionDays(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("ParseRetentionDays(%q): err = %v, wantErr %v", tc.in, err, tc.wantErr)
			continue
		}
		if err == nil && got != tc.want {
			t.Errorf("ParseRetentionDays(%q) = %d, want %d", tc.in, got, tc.want)
		}
		if err != nil && !strings.Contains(err.Error(), "retention days") {
			t.Errorf("ParseRetentionDays(%q): error should name the setting: %v", tc.in, err)
		}
	}
}
