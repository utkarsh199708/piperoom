package anthropic

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	sdk "github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/agent/llm"
)

// The decisions the agent and the room took from the raw *anthropic.Error
// before the switch-over, copied here verbatim as the reference the kinds
// must reproduce.

// oldPerRunMarkers is internal/agent's perRunMarkers before V1a.
var oldPerRunMarkers = []string{"prompt is too long", "too long", "credit balance"}

// oldModelRejection is internal/agent's modelRejection before V1a: the
// triage model goes off for every room.
func oldModelRejection(status int, body string) bool {
	switch status {
	case http.StatusNotFound:
		return true
	case http.StatusBadRequest, http.StatusForbidden:
	default:
		return false
	}
	b := strings.ToLower(body)
	for _, m := range oldPerRunMarkers {
		if strings.Contains(b, m) {
			return false
		}
	}
	for _, m := range modelRejectionMarkers {
		if strings.Contains(b, m) {
			return true
		}
	}
	return false
}

// oldModelRefused is internal/room's modelRefused before V1c: an error every
// run repeats, so the room does not restart the agent.
func oldModelRefused(status int, body string) bool {
	switch c := status; {
	case c < 400 || c >= 500:
		return false
	case c == http.StatusRequestTimeout, c == http.StatusConflict, c == http.StatusTooManyRequests, c == http.StatusRequestEntityTooLarge:
		return false
	case c == http.StatusBadRequest:
		return !strings.Contains(strings.ToLower(body), "too long")
	}
	return true
}

// errorBodies are API error bodies: one per marker of either list, the
// recorded cases of internal/agent's TestModelRejection, and bodies with no
// marker at all.
func errorBodies() []string {
	bodies := []string{
		``,
		`{}`,
		`{"type":"error","error":{"type":"not_found_error","message":"x"}}`,
		`{"error":{"message":"thinking.type.adaptive is not supported for this model"}}`,
		`{"error":{"message":"output_config.effort: Extra inputs are not permitted"}}`,
		`{"error":{"message":"eager_input_streaming: Extra inputs are not permitted"}}`,
		`{"error":{"message":"cache_control is not allowed at the top level"}}`,
		`{"message":"The provided model identifier is invalid."}`,
		`{"message":"You don't have access to the model with the specified model ID."}`,
		`{"error":{"status":"PERMISSION_DENIED","message":"Permission denied on resource model"}}`,
		`{"error":{"message":"prompt is too long: 250000 tokens > 200000 maximum"}}`,
		`{"error":{"message":"input is too long for this model"}}`,
		`{"error":{"type":"request_too_large","message":"Request exceeds the maximum size for this model"}}`,
		`{"error":{"message":"Your credit balance is too low to access the Anthropic API"}}`,
		`{"error":{"message":"Your credit balance is too low to access this model"}}`,
		`{"error":{"message":"messages: roles must alternate"}}`,
		`{"error":{"message":"forbidden"}}`,
		`{"error":{"message":"model"}}`,
		`{"type":"error","error":{"type":"invalid_request_error","message":"test server"}}`,
	}
	for _, m := range modelRejectionMarkers {
		bodies = append(bodies, fmt.Sprintf(`{"error":{"message":"%s"}}`, m))
		bodies = append(bodies, fmt.Sprintf(`{"error":{"message":"%s"}}`, strings.ToUpper(m)))
	}
	for _, m := range oldPerRunMarkers {
		bodies = append(bodies, fmt.Sprintf(`{"error":{"message":"%s"}}`, m))
		for _, mm := range modelRejectionMarkers {
			bodies = append(bodies, fmt.Sprintf(`{"error":{"message":"%s; %s"}}`, mm, m))
		}
	}
	return bodies
}

// TestKindsGiveTodaysDecisions checks every marker case, on every status the
// decisions distinguish, against the decisions taken from the raw error
// before the switch-over: the triage switch (modelRejection is now Kind ==
// model_not_found) and the room's restart rule (now llm.Unretryable).
func TestKindsGiveTodaysDecisions(t *testing.T) {
	statuses := []int{400, 401, 402, 403, 404, 405, 408, 409, 413, 415, 422, 429, 500, 502, 503, 529}
	for _, status := range statuses {
		for _, body := range errorBodies() {
			kind := Kind(status, body)
			err := &llm.APIError{Provider: llm.ProviderAnthropic, Status: status, Kind: kind}
			if got, want := kind == llm.ErrModelNotFound, oldModelRejection(status, body); got != want {
				t.Errorf("status %d body %s: kind %s turns triage off = %v, today %v", status, body, kind, got, want)
			}
			if got, want := llm.Unretryable(err), oldModelRefused(status, body); got != want {
				t.Errorf("status %d body %s: kind %s unretryable = %v, today the room says %v", status, body, kind, got, want)
			}
		}
	}
}

func TestKindTable(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		want   llm.ErrorKind
	}{
		{401, `{}`, llm.ErrAuth},
		{403, `{"error":{"message":"forbidden"}}`, llm.ErrAuth},
		{403, `{"message":"You don't have access to the model with the specified model ID."}`, llm.ErrModelNotFound},
		{404, `{}`, llm.ErrModelNotFound},
		{400, `{"error":{"message":"thinking.type.adaptive is not supported for this model"}}`, llm.ErrModelNotFound},
		{400, `{"error":{"message":"messages: roles must alternate"}}`, llm.ErrBadRequest},
		{422, `{"error":{"message":"model"}}`, llm.ErrBadRequest},
		{400, `{"error":{"message":"prompt is too long: 250000 tokens > 200000 maximum"}}`, llm.ErrTooLong},
		{413, `{}`, llm.ErrTooLong},
		{400, `{"error":{"message":"Your credit balance is too low to access the Anthropic API"}}`, llm.ErrBilling},
		{429, `{}`, llm.ErrRateLimited},
		{408, `{}`, llm.ErrOverloaded},
		{409, `{}`, llm.ErrOverloaded},
		{500, `{}`, llm.ErrOverloaded},
		{529, `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`, llm.ErrOverloaded},
	} {
		if got := Kind(tc.status, tc.body); got != tc.want {
			t.Errorf("Kind(%d, %s) = %s, want %s", tc.status, tc.body, got, tc.want)
		}
	}
}

func TestClassifyErrorKeepsTheSDKError(t *testing.T) {
	se := &sdk.Error{StatusCode: http.StatusUnauthorized}
	_ = se.UnmarshalJSON([]byte(`{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key sk-ant-api03-Ab3xAb3xAb3xAb3xAb3xAb3xAb3xAb3xAb3xAb3xAb3xAb3x9Q"}}`))
	se.Request, _ = http.NewRequest(http.MethodPost, "https://api.anthropic.com/v1/messages?beta=true", nil)
	se.Response = &http.Response{StatusCode: http.StatusUnauthorized}
	err := ClassifyError(llm.ProviderBedrock, se, nil)

	var ae *llm.APIError
	if !errors.As(err, &ae) {
		t.Fatalf("err = %T, want an *llm.APIError", err)
	}
	if ae.Provider != llm.ProviderBedrock || ae.Status != 401 || ae.Kind != llm.ErrAuth {
		t.Fatalf("APIError = %+v", ae)
	}
	var got *sdk.Error
	if !errors.As(err, &got) || got != se {
		t.Fatal("errors.As must still find the SDK error (room and evals read it until V1c and V9)")
	}
	if strings.Contains(ae.Error(), "invalid x-api-key") {
		t.Fatalf("the APIError's own text must not carry the body: %q", ae.Error())
	}
	if !strings.HasPrefix(err.Error(), `POST "https://api.anthropic.com/v1/messages?beta=true": 401 Unauthorized {`) || !strings.Contains(err.Error(), "invalid x-api-key") {
		t.Fatalf("the error reads as the SDK error did before the switch-over: %q", err.Error())
	}
	if strings.Contains(ae.Body, "Ab3xAb3xAb3xAb3x") || strings.Contains(err.Error(), "Ab3xAb3xAb3xAb3x") {
		t.Fatalf("the key must be redacted: body %q, text %q", ae.Body, err.Error())
	}
	if !llm.Unretryable(err) {
		t.Fatal("a 401 is unretryable")
	}
}

func TestClassifyErrorCapsTheBody(t *testing.T) {
	se := &sdk.Error{StatusCode: http.StatusBadRequest}
	_ = se.UnmarshalJSON([]byte(`{"error":{"message":"` + strings.Repeat("x", 3*maxErrorBody) + `"}}`))
	var ae *llm.APIError
	if !errors.As(ClassifyError(llm.ProviderAnthropic, se, nil), &ae) || len(ae.Body) > maxErrorBody {
		t.Fatalf("body is %d bytes, want at most %d", len(ae.Body), maxErrorBody)
	}
}

func TestClassifyErrorWithoutAnHTTPAnswerIsNetwork(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, context.Canceled, errors.New("dial tcp: connection refused")} {
		err := ClassifyError(llm.ProviderVertex, cause, nil)
		var ae *llm.APIError
		if !errors.As(err, &ae) || ae.Kind != llm.ErrNetwork || ae.Status != 0 || ae.Provider != llm.ProviderVertex {
			t.Fatalf("%v: err = %#v", cause, err)
		}
		if !errors.Is(err, cause) {
			t.Fatalf("%v: errors.Is must still find the cause", cause)
		}
		if !strings.Contains(err.Error(), cause.Error()) {
			t.Fatalf("%v: the text must keep the cause for the operator: %q", cause, err.Error())
		}
		if llm.Unretryable(err) {
			t.Fatalf("%v: a network error is retryable", cause)
		}
	}
	if ClassifyError(llm.ProviderAnthropic, nil, nil) != nil {
		t.Fatal("nil stays nil")
	}
	ae := fmt.Errorf("wrapped: %w", &llm.APIError{Provider: llm.ProviderAnthropic, Kind: llm.ErrAuth})
	if got := ClassifyError(llm.ProviderAnthropic, ae, nil); got != ae {
		t.Fatal("an *llm.APIError passes through unchanged")
	}
}
