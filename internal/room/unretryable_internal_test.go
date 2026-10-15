package room

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

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
		// A model request the API refused (#370): a bad key or model id fails
		// every run; throttling and timeouts may pass on the next one.
		{"model 401", fmt.Errorf("agent: model turn: %w", &anthropic.Error{StatusCode: http.StatusUnauthorized}), true},
		{"model 404", fmt.Errorf("agent: model turn: %w", &anthropic.Error{StatusCode: http.StatusNotFound}), true},
		{"model 400", fmt.Errorf("agent: model turn: %w", &anthropic.Error{StatusCode: http.StatusBadRequest}), true},
		{"model 408", fmt.Errorf("agent: model turn: %w", &anthropic.Error{StatusCode: http.StatusRequestTimeout}), false},
		{"model 409", fmt.Errorf("agent: model turn: %w", &anthropic.Error{StatusCode: http.StatusConflict}), false},
		{"model 429", fmt.Errorf("agent: model turn: %w", &anthropic.Error{StatusCode: http.StatusTooManyRequests}), false},
		{"model 529", fmt.Errorf("agent: model turn: %w", &anthropic.Error{StatusCode: 529}), false},
	}
	for _, c := range cases {
		if got := isUnretryable(c.err); got != c.want {
			t.Errorf("%s: isUnretryable = %v, want %v", c.name, got, c.want)
		}
	}
}
