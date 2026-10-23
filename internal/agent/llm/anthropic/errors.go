package anthropic

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

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
// ErrNetwork. log, when set, gets the redacted body at debug level.
//
// The APIError's own text names only the provider, kind and status. The
// returned error reads as the underlying error, redacted, because the agent's
// model errors read so before the switch-over: the room's stop message and
// the evals' run error show it ("Your credit balance is too low"), and evals'
// TestRunExits3OnAPIRefusal checks it within the run error's 200 runes. The
// room and evals classify by kind from V1c and V9.
//
// The kinds reproduce the decisions the agent and the room took from the raw
// error before the switch-over (TestKindsGiveTodaysDecisions):
//
//   - 401 is auth, 429 rate_limited, 408 and 409 overloaded (retried later),
//     413 and a 400 that says "too long" too_long, 5xx overloaded;
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
	isHTTP := errors.As(err, &se)
	text, _ := redact.Text(errorText(err, se))
	if !isHTTP {
		return &classified{api: &llm.APIError{Provider: provider, Kind: llm.ErrNetwork, Err: err}, text: text}
	}
	raw := se.RawJSON()
	body, _ := redact.Text(raw)
	if len(body) > maxErrorBody {
		body = body[:maxErrorBody]
	}
	out := &llm.APIError{
		Provider: provider,
		Status:   se.StatusCode,
		Kind:     Kind(se.StatusCode, raw),
		Body:     body,
		Err:      err,
	}
	if log != nil {
		log.Debug("llm/anthropic: request failed", "provider", provider, "status", out.Status, "kind", out.Kind, "body", out.Body)
	}
	return &classified{api: out, text: text}
}

// errorText is err's text. An SDK error built in code without its request
// and response (a test's) cannot print itself, so it gets a stand-in.
func errorText(err error, se *sdk.Error) string {
	if se != nil && err == error(se) && (se.Request == nil || se.Response == nil) {
		return fmt.Sprintf("%d %s %s", se.StatusCode, http.StatusText(se.StatusCode), se.RawJSON())
	}
	return err.Error()
}

// classified is a classified failure: errors.As finds its *llm.APIError (and,
// through it, the SDK error or the network cause), and its text is the
// underlying error's, redacted.
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
	case strings.Contains(b, billingMarker):
		return llm.ErrBilling
	}
	perRun := strings.Contains(b, tooLongMarker)
	if status == http.StatusBadRequest && perRun {
		return llm.ErrTooLong
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
