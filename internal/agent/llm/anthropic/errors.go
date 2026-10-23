package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"unicode/utf8"

	sdk "github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/agent/llm"
	"piperoom/internal/redact"
)

// maxErrorBody caps APIError.Body.
const maxErrorBody = 2 << 10

// modelRejectionMarkers are lower-case fragments of an API error body that
// say the model, or the request shape for this model, is the problem: an id
// the account cannot use, or a parameter the model does not take (adaptive
// thinking, effort, compaction, top-level cache_control, eager input
// streaming).
var modelRejectionMarkers = []string{
	"model", "not supported", "does not support", "unsupported",
	"thinking", "effort", "adaptive", "context_management", "compact",
	"cache_control", "eager_input_streaming", "extra inputs",
	"access to the model", // Bedrock AccessDeniedException
}

// tooLongMarker is the fragment of a 400 that says one conversation outgrew
// the model's context window: "prompt is too long", "input is too long for
// this model". It concerns one run's history, never the model.
const tooLongMarker = "too long"

// billingMarker is the fragment of the error an empty credit balance gives.
const billingMarker = "credit balance"

// ClassifyError turns a failed request into an error that wraps an
// *llm.APIError (errors.As finds it; llm.Unretryable reads it). An HTTP error
// (*anthropic.Error, kept as APIError.Err so errors.As on it still works) is
// classified by its status and body; anything else had no HTTP answer and is
// ErrNetwork. log, when set, gets the redacted, capped body (or, without an
// HTTP answer, the redacted, capped error text) at debug level.
//
// The returned error's text is safe for a room event: the room puts it in the
// agent's status and stop message, and the evals in their run error. A
// provider's error body can echo prompt content (alert text, tool output,
// schema fragments; design 10.2), and redact.Text only masks credentials, so
// the text never carries the body or the SDK's error text. It is the
// APIError's (provider, kind, status), a fixed hint per kind ("credit balance
// too low"), and the provider's error type when it is one of the API's own
// (errorTypes). Without an HTTP answer it names the cause only when the cause
// is a context or transport error, whose text holds no response content.
//
// The kinds reproduce the decisions the agent and the room took from the raw
// error before the switch-over (TestKindsGiveTodaysDecisions):
//
//   - 401 is auth, 429 rate_limited, 408 and 409 overloaded (retried later),
//     413 and a 400 that says "too long" too_long (even when it says "credit
//     balance" too: the room retried any 400 that said "too long"), 5xx
//     overloaded;
//   - a 404 is model_not_found;
//   - "credit balance" in any other 4xx is billing;
//   - a 400 or 403 whose body names the model or a request shape this model
//     does not take (modelRejectionMarkers, unless "too long" or "credit
//     balance" is also there) is model_not_found too: the model cannot serve
//     this request, so the triage switch turns the triage model off;
//   - any other 403 is auth, any other 4xx bad_request.
func ClassifyError(provider string, err error, log *slog.Logger) error {
	if err == nil {
		return nil
	}
	var ae *llm.APIError
	if errors.As(err, &ae) {
		return err
	}
	var se *sdk.Error
	if !errors.As(err, &se) {
		out := &llm.APIError{Provider: provider, Kind: llm.ErrNetwork, Err: err}
		if log != nil {
			text, _ := redact.Text(err.Error())
			log.Debug("llm/anthropic: request failed", "provider", provider, "kind", out.Kind, "err", capText(text))
		}
		return &classified{api: out, text: out.Error() + ": " + networkHint(err)}
	}
	raw := se.RawJSON()
	body, _ := redact.Text(raw)
	out := &llm.APIError{
		Provider: provider,
		Status:   se.StatusCode,
		Kind:     Kind(se.StatusCode, raw),
		Body:     capText(body),
		Err:      err,
	}
	if log != nil {
		log.Debug("llm/anthropic: request failed", "provider", provider, "status", out.Status, "kind", out.Kind, "body", out.Body)
	}
	text := out.Error() + ": " + kindHint(out.Kind)
	if typ := errorType(raw); typ != "" {
		text += " (" + typ + ")"
	}
	return &classified{api: out, text: text}
}

// capText cuts s to maxErrorBody bytes, on a rune boundary.
func capText(s string) string {
	if len(s) <= maxErrorBody {
		return s
	}
	s = s[:maxErrorBody]
	for len(s) > 0 && !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}

// kindHint is the fixed, caller-visible explanation of a kind. It never comes
// from the provider's answer.
func kindHint(k llm.ErrorKind) string {
	switch k {
	case llm.ErrAuth:
		return "check the API key and its access"
	case llm.ErrBilling:
		return "credit balance too low"
	case llm.ErrModelNotFound:
		return "the model is unknown or does not take this request"
	case llm.ErrTooLong:
		return "the conversation is too long for the model"
	case llm.ErrRateLimited:
		return "rate limited"
	case llm.ErrOverloaded:
		return "the provider is overloaded or unavailable"
	case llm.ErrNetwork:
		return "no answer from the provider"
	}
	return "the provider rejected the request"
}

// networkHint names the cause of a failure without an HTTP answer when its
// text cannot hold response content: the caller's context, or a transport
// error (a *url.Error or other net.Error, raised before any response body is
// read: dial, TLS, a reset connection). Anything else, such as an error event
// in the stream (its text is the event's JSON) or a stream that could not be
// accumulated, gets the fixed hint.
func networkHint(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded.Error()
	case errors.Is(err, context.Canceled):
		return context.Canceled.Error()
	}
	var ne net.Error
	if errors.As(err, &ne) {
		text, _ := redact.Text(ne.Error())
		return capText(text)
	}
	return kindHint(llm.ErrNetwork)
}

// errorTypes are the error types the Messages API (and its Bedrock and Vertex
// front ends) names in an error body's error.type. Only these are shown: a
// type is a fixed token, never request content.
var errorTypes = map[string]bool{
	"invalid_request_error": true, "authentication_error": true, "billing_error": true,
	"permission_error": true, "not_found_error": true, "request_too_large": true,
	"rate_limit_error": true, "api_error": true, "overloaded_error": true, "timeout_error": true,
}

// errorType is the body's error.type when it is one of errorTypes, else "".
func errorType(raw string) string {
	var b struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(raw), &b) != nil || !errorTypes[b.Error.Type] {
		return ""
	}
	return b.Error.Type
}

// classified is a classified failure: errors.As finds its *llm.APIError (and,
// through it, the SDK error or the network cause). Its text is safe for a
// room event (see ClassifyError).
type classified struct {
	api  *llm.APIError
	text string
}

func (c *classified) Error() string { return c.text }

func (c *classified) Unwrap() error { return c.api }

// Kind classifies an HTTP error by its status and response body (see
// ClassifyError).
func Kind(status int, body string) llm.ErrorKind {
	b := strings.ToLower(body)
	switch {
	case status >= 500:
		return llm.ErrOverloaded
	case status < 400:
		// Not an error status; nothing a caller can act on.
		return llm.ErrNetwork
	case status == http.StatusUnauthorized:
		return llm.ErrAuth
	case status == http.StatusTooManyRequests:
		return llm.ErrRateLimited
	case status == http.StatusRequestTimeout, status == http.StatusConflict:
		return llm.ErrOverloaded
	case status == http.StatusRequestEntityTooLarge:
		return llm.ErrTooLong
	case status == http.StatusNotFound:
		return llm.ErrModelNotFound
	}
	perRun := strings.Contains(b, tooLongMarker)
	switch {
	case status == http.StatusBadRequest && perRun:
		// Before "credit balance": the room retried every 400 that said
		// "too long", whatever else it said.
		return llm.ErrTooLong
	case strings.Contains(b, billingMarker):
		return llm.ErrBilling
	}
	if (status == http.StatusBadRequest || status == http.StatusForbidden) && !perRun && namesModel(b) {
		return llm.ErrModelNotFound
	}
	if status == http.StatusForbidden {
		return llm.ErrAuth
	}
	return llm.ErrBadRequest
}

func namesModel(lowerBody string) bool {
	for _, m := range modelRejectionMarkers {
		if strings.Contains(lowerBody, m) {
			return true
		}
	}
	return false
}
