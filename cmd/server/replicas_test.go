package main

import "testing"

func TestParseResumeOnTakeover(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		want    bool
		wantErr bool
	}{
		{"", false, false},
		{"0", false, false},
		{" 1 ", true, false},
		{"1", true, false},
		{"true", false, true},
		{"yes", false, true},
		{"2", false, true},
	} {
		got, err := parseResumeOnTakeover(tc.raw)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("parseResumeOnTakeover(%q) = %v, %v; want %v, err %v", tc.raw, got, err, tc.want, tc.wantErr)
		}
	}
}
