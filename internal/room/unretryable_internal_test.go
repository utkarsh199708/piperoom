package room

import (
	"errors"
	"fmt"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/agent/llm"
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
		// A model request the API refused (#370): a bad key or model id fails
		// every run; throttling and timeouts may pass on the next one.
		{"model 401", fmt.Errorf("agent: model turn: %w", modelErr(llm.ErrAuth, 401)), true},
		{"model 404", fmt.Errorf("agent: model turn: %w", modelErr(llm.ErrModelNotFound, 404)), true},
		{"model 400", fmt.Errorf("agent: model turn: %w", modelErr(llm.ErrBadRequest, 400)), true},
		{"model 408", fmt.Errorf("agent: model turn: %w", modelErr(llm.ErrNetwork, 408)), false},
		{"model 409", fmt.Errorf("agent: model turn: %w", modelErr(llm.ErrOverloaded, 409)), false},
		{"model 429", fmt.Errorf("agent: model turn: %w", modelErr(llm.ErrRateLimited, 429)), false},
		{"model 529", fmt.Errorf("agent: model turn: %w", modelErr(llm.ErrOverloaded, 529)), false},
		// One run's history is too long: a fresh run starts from one message.
		{"model 413", fmt.Errorf("agent: model turn: %w", modelErr(llm.ErrTooLong, 413)), false},
		{"model 400 prompt too long", fmt.Errorf("agent: model turn: %w", modelErr(llm.ErrTooLong, 400)), false},
		{"model billing", fmt.Errorf("agent: model turn: %w", modelErr(llm.ErrBilling, 400)), true},
		{"model truncated", fmt.Errorf("agent: model turn: %w", modelErr(llm.ErrTruncated, 0)), true},
		{"model network", fmt.Errorf("agent: model turn: %w", modelErr(llm.ErrNetwork, 0)), false},
		{"non-llm 401 text", errors.New("401 unauthorized"), false},
		{"model 403", fmt.Errorf("agent: model turn: %w", modelErr(llm.ErrAuth, 403)), true},
	}
	for _, c := range cases {
		if got := isUnretryable(c.err); got != c.want {
			t.Errorf("%s: isUnretryable = %v, want %v", c.name, got, c.want)
		}
	}
}

// modelErr is the adapter's error for a refused model request.
func modelErr(kind llm.ErrorKind, status int) *llm.APIError {
	return &llm.APIError{Provider: llm.ProviderAnthropic, Status: status, Kind: kind}
}
