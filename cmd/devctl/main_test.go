package main

import (
	"context"
	"strings"
	"testing"
)

// TestRunCommandArgErrors covers the argument-validation paths of runCommand,
// which need no Docker or network (the stack is never touched on these paths).
func TestRunCommandArgErrors(t *testing.T) {
	st := &stack{}
	tests := []struct {
		name string
		cmd  string
		args []string
		want string
	}{
		{"unknown command", "frobnicate", nil, `unknown command "frobnicate"`},
		{"break missing scenario", "break", nil, "usage: devctl break <scenario>"},
		{"break unknown scenario", "break", []string{"nope"}, `unknown scenario "nope"`},
		{"fix missing scenario", "fix", nil, "usage: devctl fix <scenario>"},
		{"fix unknown scenario", "fix", []string{"nope"}, `unknown scenario "nope"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runCommand(context.Background(), st, tt.cmd, tt.args)
			if err == nil {
				t.Fatalf("runCommand(%q, %v) = nil, want error containing %q", tt.cmd, tt.args, tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("runCommand(%q, %v) = %q, want containing %q", tt.cmd, tt.args, err, tt.want)
			}
		})
	}
}
