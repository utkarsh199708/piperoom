package anthropic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
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

// oldModelRejectionMarkers is internal/agent's modelRejectionMarkers before
// V1a, frozen here so a change to the live list cannot move the reference.
var oldModelRejectionMarkers = []string{
	"model", "not supported", "does not support", "unsupported",
	"thinking", "effort", "adaptive", "context_management", "compact",
	"cache_control", "eager_input_streaming", "extra inputs",
	"access to the model", // Bedrock AccessDeniedException
}

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
	for _, m := range oldModelRejectionMarkers {
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

// errorBodies are API error bodies: one per marker of either list (the old
// and the live one), every pair of markers, the recorded cases of
// internal/agent's TestModelRejection, and bodies with no marker at all.
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
		`{"error":{"message":"Your credit balance is too low; prompt is too long"}}`,
		`{"error":{"message":"credit balance: the model input is too long"}}`,
	}
	var markers []string
	markers = append(markers, oldModelRejectionMarkers...)
	markers = append(markers, modelRejectionMarkers...)
	markers = append(markers, oldPerRunMarkers...)
	markers = append(markers, tooLongMarker, billingMarker)
	for _, m := range markers {
		bodies = append(bodies, fmt.Sprintf(`{"error":{"message":"%s"}}`, m))
		bodies = append(bodies, fmt.Sprintf(`{"error":{"message":"%s"}}`, strings.ToUpper(m)))
		for _, mm := range markers {
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
		// A 400 that says "too long" was retried by the room whatever else
		// it said, so it is too_long even next to "credit balance".
		{400, `{"error":{"message":"credit balance; prompt is too long"}}`, llm.ErrTooLong},
		{403, `{"error":{"message":"credit balance; prompt is too long"}}`, llm.ErrBilling},
		{402, `{"error":{"message":"Your credit balance is too low"}}`, llm.ErrBilling},
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
	if want := "llm: bedrock: auth (status 401): check the API key and its access (authentication_error)"; err.Error() != want {
		t.Fatalf("err.Error() = %q, want %q", err.Error(), want)
	}
	if strings.Contains(ae.Body, "Ab3xAb3xAb3xAb3x") || !strings.Contains(ae.Body, "invalid x-api-key") {
		t.Fatalf("the body keeps the provider's message with the key redacted: %q", ae.Body)
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
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	for _, cause := range []error{context.DeadlineExceeded, context.Canceled, refused} {
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

// promptEcho is error body text that echoes the request: an alert's text, a
// tool's output, a schema fragment. It must never reach err.Error().
const promptEcho = "orders-alert-echo-5d1c"

func TestClassifyErrorTextNeverCarriesTheBody(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		want   string
	}{
		{400, `{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low. ` + promptEcho + `"}}`,
			"llm: anthropic: billing (status 400): credit balance too low (invalid_request_error)"},
		{400, `{"type":"error","error":{"type":"invalid_request_error","message":"tools.0.input_schema: ` + promptEcho + ` is not valid"}}`,
			"llm: anthropic: bad_request (status 400): the provider rejected the request (invalid_request_error)"},
		{400, `{"error":{"message":"prompt is too long: ` + promptEcho + `"}}`,
			"llm: anthropic: too_long (status 400): the conversation is too long for the model"},
		{404, `{"type":"error","error":{"type":"not_found_error","message":"model: ` + promptEcho + `"}}`,
			"llm: anthropic: model_not_found (status 404): the model is unknown or does not take this request (not_found_error)"},
		{429, `{}`, "llm: anthropic: rate_limited (status 429): rate limited"},
		{529, `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`,
			"llm: anthropic: overloaded (status 529): the provider is overloaded or unavailable (overloaded_error)"},
		// An error type the API does not define is the body's text too.
		{400, `{"type":"error","error":{"type":"` + promptEcho + `","message":"x"}}`,
			"llm: anthropic: bad_request (status 400): the provider rejected the request"},
	} {
		se := &sdk.Error{StatusCode: tc.status}
		_ = se.UnmarshalJSON([]byte(tc.body))
		se.Request, _ = http.NewRequest(http.MethodPost, "https://api.anthropic.com/v1/messages?beta=true", nil)
		se.Response = &http.Response{StatusCode: tc.status}
		err := ClassifyError(llm.ProviderAnthropic, se, nil)
		if err.Error() != tc.want {
			t.Errorf("%d %s: err.Error() = %q, want %q", tc.status, tc.body, err.Error(), tc.want)
		}
		var ae *llm.APIError
		if !errors.As(err, &ae) {
			t.Fatalf("%d: no *llm.APIError", tc.status)
		}
		if strings.Contains(tc.body, promptEcho) && !strings.Contains(ae.Body, promptEcho) {
			t.Errorf("%d: APIError.Body keeps the body for the debug log: %q", tc.status, ae.Body)
		}
	}
}

// A stream that failed after a 200 answers no HTTP error: its text can be the
// error event's JSON, which can echo the request.
func TestClassifyErrorStreamFailureTextIsFixed(t *testing.T) {
	cause := fmt.Errorf("received error while streaming: %s", `{"type":"error","error":{"type":"invalid_request_error","message":"`+promptEcho+`"}}`)
	err := ClassifyError(llm.ProviderAnthropic, cause, nil)
	if want := "llm: anthropic: network: no answer from the provider"; err.Error() != want {
		t.Fatalf("err.Error() = %q, want %q", err.Error(), want)
	}
	if !errors.Is(err, cause) {
		t.Fatal("errors.Is must still find the cause")
	}
}

func TestClassifyErrorDebugLogIsRedactedAndCapped(t *testing.T) {
	const key = "sk-ant-api03-Ab3xAb3xAb3xAb3xAb3xAb3xAb3xAb3xAb3xAb3xAb3xAb3x9Q"
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// The echo marker sits past the cap: only the capped body is logged.
	tail := "tail-" + promptEcho
	body := `{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key ` + key + " " + strings.Repeat("y", 3*maxErrorBody) + tail + `"}}`
	se := &sdk.Error{StatusCode: http.StatusUnauthorized}
	_ = se.UnmarshalJSON([]byte(body))
	se.Request, _ = http.NewRequest(http.MethodPost, "https://api.anthropic.com/v1/messages?beta=true", nil)
	se.Response = &http.Response{StatusCode: http.StatusUnauthorized}
	err := ClassifyError(llm.ProviderAnthropic, se, log)

	// A stream failure is logged too, redacted and capped.
	streamErr := fmt.Errorf("received error while streaming: key %s %s%s", key, strings.Repeat("z", 3*maxErrorBody), tail)
	err2 := ClassifyError(llm.ProviderAnthropic, streamErr, log)

	out := buf.String()
	if strings.Count(out, "llm/anthropic: request failed") != 2 {
		t.Fatalf("want two debug lines:\n%s", out)
	}
	if strings.Contains(out, "Ab3xAb3xAb3xAb3x") {
		t.Fatalf("the debug log carries the API key:\n%s", out)
	}
	if strings.Contains(out, promptEcho) {
		t.Fatalf("the debug log carries text past the capped body:\n%s", out)
	}
	if !strings.Contains(out, "invalid x-api-key") || !strings.Contains(out, "received error while streaming") {
		t.Fatalf("the debug log lacks the redacted body:\n%s", out)
	}
	for _, e := range []error{err, err2} {
		if strings.Contains(e.Error(), "Ab3xAb3x") || strings.Contains(e.Error(), "invalid x-api-key") || strings.Contains(e.Error(), "streaming") {
			t.Fatalf("err.Error() carries provider text: %q", e.Error())
		}
	}
}
