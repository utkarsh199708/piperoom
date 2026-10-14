package room

import (
	"errors"
	"fmt"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

func TestIsUnretryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"validation", validation("bad"), true},
		{"room closed", roomClosed("closed"), true},
		{"wrapped validation", fmt.Errorf("agent: %w", validation("bad")), true},
		{"tools bad input", fmt.Errorf("x: %w", tools.ErrBadInput), true},
		{"topology bad input", fmt.Errorf("x: %w", topology.ErrBadInput), true},
		{"transient", errors.New("dial"), false},
		{"not found", notFound("gone"), false},
		{"budget stop", &agent.BudgetError{}, false},
	}
	for _, c := range cases {
		if got := isUnretryable(c.err); got != c.want {
			t.Errorf("%s: isUnretryable = %v, want %v", c.name, got, c.want)
		}
	}
}
