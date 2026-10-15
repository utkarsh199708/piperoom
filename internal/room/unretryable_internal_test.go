package room

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
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
		{"model 401", fmt.Errorf("agent: model turn: %w", modelErr(http.StatusUnauthorized, "")), true},
		{"model 404", fmt.Errorf("agent: model turn: %w", modelErr(http.StatusNotFound, "")), true},
		{"model 400", fmt.Errorf("agent: model turn: %w", modelErr(http.StatusBadRequest, "")), true},
		{"model 408", fmt.Errorf("agent: model turn: %w", modelErr(http.StatusRequestTimeout, "")), false},
		{"model 409", fmt.Errorf("agent: model turn: %w", modelErr(http.StatusConflict, "")), false},
		{"model 429", fmt.Errorf("agent: model turn: %w", modelErr(http.StatusTooManyRequests, "")), false},
		{"model 529", fmt.Errorf("agent: model turn: %w", modelErr(529, "")), false},
		// One run's history is too long: a fresh run starts from one message.
		{"model 413", fmt.Errorf("agent: model turn: %w", modelErr(http.StatusRequestEntityTooLarge, "")), false},
		{"model 400 prompt too long", fmt.Errorf("agent: model turn: %w", modelErr(http.StatusBadRequest,
			`{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 1000001 tokens > 1000000 maximum"}}`)), false},
		{"model 403", fmt.Errorf("agent: model turn: %w", modelErr(http.StatusForbidden, "")), true},
	}
	for _, c := range cases {
		if got := isUnretryable(c.err); got != c.want {
			t.Errorf("%s: isUnretryable = %v, want %v", c.name, got, c.want)
		}
	}
}

// modelErr is the SDK's error for a refused model request (Error() reads the
// request and response, so both are set).
func modelErr(code int, body string) *anthropic.Error {
	e := &anthropic.Error{
		StatusCode: code,
		Request:    httptest.NewRequest(http.MethodPost, "https://api.anthropic.com/v1/messages", nil),
		Response:   &http.Response{StatusCode: code},
	}
	if body != "" {
		if err := e.UnmarshalJSON([]byte(body)); err != nil {
			panic(err)
		}
	}
	return e
}
