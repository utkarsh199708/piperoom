package llm

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestUnretryable(t *testing.T) {
	for _, tc := range []struct {
		kind ErrorKind
		want bool
	}{
		{ErrAuth, true}, {ErrBilling, true}, {ErrModelNotFound, true}, {ErrBadRequest, true}, {ErrTruncated, true},
		{ErrTooLong, false}, {ErrRateLimited, false}, {ErrOverloaded, false}, {ErrNetwork, false},
	} {
		err := fmt.Errorf("agent: turn: %w", &APIError{Provider: ProviderOpenAI, Kind: tc.kind})
		if got := Unretryable(err); got != tc.want {
			t.Errorf("Unretryable(%s) = %v, want %v", tc.kind, got, tc.want)
		}
	}
	if Unretryable(errors.New("plain")) || Unretryable(nil) {
		t.Error("a non-APIError must be retryable")
	}
}

func TestAPIErrorKeepsBodyOutAndUnwraps(t *testing.T) {
	inner := errors.New("sdk error")
	e := &APIError{Provider: ProviderGemini, Status: 400, Kind: ErrBadRequest, Body: "echoed prompt text", Err: inner}
	if s := e.Error(); strings.Contains(s, "echoed") || !strings.Contains(s, "bad_request") || !strings.Contains(s, "400") {
		t.Errorf("Error() = %q", s)
	}
	if !errors.Is(e, inner) {
		t.Error("errors.Is through Unwrap failed")
	}
	if s := (&APIError{Provider: ProviderOpenAI, Kind: ErrNetwork}).Error(); strings.Contains(s, "status") {
		t.Errorf("no-status Error() = %q", s)
	}
}
