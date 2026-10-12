package main

import (
	"strings"
	"testing"

	"piperoom/internal/room"
)

func TestParseMaxAgents(t *testing.T) {
	tests := []struct {
		in      string
		want    int
		wantErr string // substring of the error; "" means no error
	}{
		{"", room.DefaultMaxAgents, ""},
		{"  ", room.DefaultMaxAgents, ""},
		{"0", room.DefaultMaxAgents, ""},
		{"00", room.DefaultMaxAgents, ""},
		{"1", 1, ""},
		{" 7 ", 7, ""},
		{"007", 7, ""},
		{"-1", 0, "whole number"},
		{"-0", 0, "whole number"},
		{"+3", 0, "whole number"},
		{"abc", 0, "whole number"},
		{"2.5", 0, "whole number"},
		{"3x", 0, "whole number"},
		{"1e3", 0, "whole number"},
		{"99999999999999999999999999", 0, "too large"},
	}
	for _, tt := range tests {
		got, err := parseMaxAgents(tt.in)
		if tt.wantErr == "" {
			if err != nil {
				t.Errorf("parseMaxAgents(%q) unexpected error: %v", tt.in, err)
			} else if got != tt.want {
				t.Errorf("parseMaxAgents(%q) = %d, want %d", tt.in, got, tt.want)
			}
			continue
		}
		if err == nil {
			t.Errorf("parseMaxAgents(%q) = %d, want an error containing %q", tt.in, got, tt.wantErr)
		} else if !strings.Contains(err.Error(), tt.wantErr) {
			t.Errorf("parseMaxAgents(%q) error = %q, want it to contain %q", tt.in, err, tt.wantErr)
		}
	}
}
