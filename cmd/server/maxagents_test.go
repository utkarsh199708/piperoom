package main

import (
	"testing"

	"piperoom/internal/room"
)

func TestParseMaxAgents(t *testing.T) {
	tests := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"", room.DefaultMaxAgents, false},
		{"  ", room.DefaultMaxAgents, false},
		{"0", room.DefaultMaxAgents, false},
		{"1", 1, false},
		{" 7 ", 7, false},
		{"-1", 0, true},
		{"abc", 0, true},
		{"2.5", 0, true},
		{"3x", 0, true},
	}
	for _, tt := range tests {
		got, err := parseMaxAgents(tt.in)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseMaxAgents(%q) err = %v, wantErr %v", tt.in, err, tt.wantErr)
			continue
		}
		if err == nil && got != tt.want {
			t.Errorf("parseMaxAgents(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}
