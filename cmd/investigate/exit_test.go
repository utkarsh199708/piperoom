package main

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"piperoom/internal/agent"
)

func TestExitCode(t *testing.T) {
	// The shape run returns when ep.Run stops on the budget.
	budget := fmt.Errorf("investigate: %w", fmt.Errorf("agent: stopped: %w", &agent.BudgetError{Used: 120, Limit: 100}))
	tests := []struct {
		name     string
		err      error
		want     int
		wantLine string // "" = nothing printed
	}{
		{"nil", nil, 0, ""},
		{"wrapped budget stop", budget, 0, "investigate: stopped: token budget reached (120 of 100)"},
		{"other error", errors.New("investigate: open the data stack: dial tcp: refused"), 1, "investigate: investigate: open the data stack"},
		{"escape codes are stripped", errors.New("bad \x1b[2J value"), 1, "investigate: bad [2J value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			if got := exitCode(tt.err, &out); got != tt.want {
				t.Fatalf("exitCode = %d, want %d", got, tt.want)
			}
			printed := out.String()
			if tt.wantLine == "" {
				if printed != "" {
					t.Fatalf("printed %q, want nothing", printed)
				}
				return
			}
			if strings.Count(printed, "\n") != 1 {
				t.Fatalf("printed %q, want exactly one line", printed)
			}
			if !strings.Contains(printed, tt.wantLine) {
				t.Fatalf("printed %q, want it to contain %q", printed, tt.wantLine)
			}
		})
	}
}
