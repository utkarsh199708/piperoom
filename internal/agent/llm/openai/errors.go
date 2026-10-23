package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"

	"piperoom/internal/agent/llm"
	"piperoom/internal/redact"
)

// maxErrorBody caps APIError.Body.
const maxErrorBody = 2 << 10

// wireError is an error body: OpenAI and Azure nest it under "error"; vLLM
// and some proxies put message, type and code at the top level.
type wireError struct {
	Error   *wireErrorDetail `json:"error"`
	Message string           `json:"message"`
	Type    string           `json:"type"`
	Code    json.RawMessage  `json:"code"`
}

type wireErrorDetail struct {
	Message    string          `json:"message"`
	Type       string          `json:"type"`
	Code       json.RawMessage `json:"code"`
	InnerError *struct {
		Code                string                      `json:"code"`
		ContentFilterResult map[string]wireFilterResult `json:"content_filter_result"`
	} `json:"innererror"`
}

// parsedError is the classification input of an error body.
type parsedError struct {
	message, typ, code, innerCode string
	filter                        map[string]wireFilterResult
}

func parseError(raw []byte) parsedError {
	var w wireError
	if json.Unmarshal(raw, &w) != nil {
		return parsedError{message: string(raw)}
	}
	p := parsedError{message: w.Message, typ: w.Type, code: codeString(w.Code)}
	if d := w.Error; d != nil {
		p.message, p.typ, p.code = d.Message, d.Type, codeString(d.Code)
		if d.InnerError != nil {
			p.innerCode, p.filter = d.InnerError.Code, d.InnerError.ContentFilterResult
		}
	}
	return p
}

// codeString reads a code that is a string on OpenAI and a number on vLLM.
func codeString(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return ""
}

// filteredPrompt answers Azure's 400 content_filter (the prompt tripped the
// content filter) as a refusal, not an error (design 3): the room reports
// it as the model refusing. Usage is estimated from the request: the
// estimate counts against the caps, the safe side.
func (m *Model) filteredPrompt(status int, raw []byte, reqBytes int) (llm.Response, bool) {
	if status != http.StatusBadRequest {
		return llm.Response{}, false
	}
	p := parseError(raw)
	if p.code != "content_filter" && p.innerCode != "ResponsibleAIPolicyViolation" {
		return llm.Response{}, false
	}
	return llm.Response{
		Message:      llm.Message{Role: llm.RoleAssistant},
		Stop:         llm.StopRefusal,
		StopDetail:   filterDetail(p.filter),
		ProviderStop: "content_filter",
		Usage:        llm.Usage{Input: estimateTokens(reqBytes), Estimated: true},
	}, true
}

// classify turns an error answer into an *llm.APIError and the error text a
// room may show: the kind, the status, a fixed hint and, when it is one of
// knownCodes, the provider's error code. The body (redacted, the key
// scrubbed, capped) goes only to APIError.Body and the debug log.
func (m *Model) classify(status int, raw []byte) (*llm.APIError, string) {
	p := parseError(raw)
	ae := &llm.APIError{
		Provider: llm.ProviderOpenAI,
		Status:   status,
		Kind:     Kind(status, p.code, p.typ, p.message),
		Body:     m.safeBody(string(raw)),
	}
	m.log.Debug("llm/openai: request failed", "flavor", m.flavor.name, "endpoint", m.origin,
		"status", ae.Status, "kind", ae.Kind, "body", ae.Body)
	text := ae.Error() + ": " + kindHint(ae.Kind)
	if c := strings.ToLower(p.code); knownCodes[c] {
		text += " (" + c + ")"
	}
	return ae, text
}

// knownCodes are error codes of the chat completions API shown in the error
// text: fixed tokens, never request content.
var knownCodes = map[string]bool{
	"insufficient_quota": true, "billing_hard_limit_reached": true, "rate_limit_exceeded": true,
	"context_length_exceeded": true, "model_not_found": true, "invalid_api_key": true,
	"unsupported_parameter": true, "unsupported_value": true, "deploymentnotfound": true,
	"string_above_max_length": true, "tokens_exceeded": true,
}

// Kind classifies an error answer by its status and its error code, type
// and message:
//
//   - 5xx (529 too), 408 and 409 are overloaded; 401 is auth;
//   - insufficient_quota and billing_hard_limit_reached (OpenAI answers them
//     with 429), "exceeded your current quota", and any 402 are billing:
//     they are never retried;
//   - any other 429 is rate_limited;
//   - 413, context_length_exceeded, and a 400 about the context length
//     ("maximum context length", "context window", "too long") are too_long;
//   - 404, model_not_found and Azure's DeploymentNotFound are
//     model_not_found;
//   - any other 403 is auth, any other 4xx bad_request.
func Kind(status int, code, typ, message string) llm.ErrorKind {
	c, t, msg := strings.ToLower(code), strings.ToLower(typ), strings.ToLower(message)
	switch {
	case status >= 500:
		return llm.ErrOverloaded
	case status < 400:
		// A redirect (never followed) or another non-error answer: nothing
		// the caller can act on but a later retry.
		return llm.ErrNetwork
	case status == http.StatusUnauthorized:
		return llm.ErrAuth
	case c == "insufficient_quota" || t == "insufficient_quota" || c == "billing_hard_limit_reached" ||
		strings.Contains(msg, "exceeded your current quota") || status == http.StatusPaymentRequired:
		return llm.ErrBilling
	case status == http.StatusTooManyRequests:
		return llm.ErrRateLimited
	case status == http.StatusRequestTimeout, status == http.StatusConflict:
		return llm.ErrOverloaded
	case status == http.StatusRequestEntityTooLarge, c == "context_length_exceeded", tooLongMessage(msg):
		return llm.ErrTooLong
	case status == http.StatusNotFound, c == "model_not_found", c == "deploymentnotfound":
		return llm.ErrModelNotFound
	case status == http.StatusForbidden:
		return llm.ErrAuth
	}
	return llm.ErrBadRequest
}

func tooLongMessage(msg string) bool {
	for _, m := range []string{"maximum context length", "context length", "context window", "too long", "prompt is too large"} {
		if strings.Contains(msg, m) {
			return true
		}
	}
	return false
}

// safeBody redacts secrets by pattern, removes the key literally and cuts
// to maxErrorBody: a body can echo a key (a proxy's 401) and, OpenAI and
// Azure 400s, prompt content.
func (m *Model) safeBody(s string) string {
	if k := m.key.Reveal(); k != "" {
		s = strings.ReplaceAll(s, k, "[redacted]")
	}
	s, _ = redact.Text(s)
	return capText(s)
}

// logFailure logs a failure without an HTTP error body at debug level.
func (m *Model) logFailure(ae *llm.APIError, err error) {
	m.log.Debug("llm/openai: request failed", "flavor", m.flavor.name, "endpoint", m.origin,
		"status", ae.Status, "kind", ae.Kind, "err", m.safeBody(withoutURL(err).Error()))
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

// kindHint is the fixed, caller-visible explanation of a kind.
func kindHint(k llm.ErrorKind) string {
	switch k {
	case llm.ErrAuth:
		return "check the API key and its access"
	case llm.ErrBilling:
		return "quota or credit exhausted"
	case llm.ErrModelNotFound:
		return "the model (or Azure deployment) is unknown"
	case llm.ErrTooLong:
		return "the conversation is too long for the model"
	case llm.ErrRateLimited:
		return "rate limited"
	case llm.ErrOverloaded:
		return "the provider is overloaded or unavailable"
	case llm.ErrNetwork:
		return "no answer from the provider"
	case llm.ErrTruncated:
		return "the model server truncated the prompt; raise its context length"
	}
	return "the provider rejected the request"
}

// networkHint names the cause of a failure without an HTTP error body when
// its text cannot hold response content: the caller's context, the rate
// limit wait, the dial guard, or a transport error. Anything else gets the
// fixed hint of kind.
func networkHint(err error, kind llm.ErrorKind) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded.Error()
	case errors.Is(err, context.Canceled):
		return context.Canceled.Error()
	case errors.Is(err, errDenied), errors.Is(err, errOtherHost):
		return redactedText(err)
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return redactedText(ne)
	}
	if kind == llm.ErrRateLimited {
		return "the endpoint's rate limit wait ends past the turn's deadline"
	}
	return kindHint(kind)
}

// redactedText is err's text without the request URL (a *url.Error names
// it, and a gateway's path can hold an account id or a token), secrets
// redacted, capped.
func redactedText(err error) string {
	s, _ := redact.Text(withoutURL(err).Error())
	return capText(s)
}

// withoutURL unwraps a *url.Error to its cause, so the text drops the URL.
func withoutURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) && ue.Err != nil {
		return ue.Err
	}
	return err
}

// classified is a classified failure: errors.As finds its *llm.APIError
// (and through it the cause). Its text is safe for a room event.
type classified struct {
	api  *llm.APIError
	text string
}

func (c *classified) Error() string { return c.text }

func (c *classified) Unwrap() error { return c.api }
