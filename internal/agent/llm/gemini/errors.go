package gemini

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"piperoom/internal/agent/llm"
)

// wireError is a Google API error body.
type wireError struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
		Details []struct {
			Type       string `json:"@type"`
			Reason     string `json:"reason"`
			RetryDelay string `json:"retryDelay"`
			Violations []struct {
				QuotaID     string `json:"quotaId"`
				QuotaMetric string `json:"quotaMetric"`
			} `json:"violations"`
		} `json:"details"`
	} `json:"error"`
}

// classifiedError is how one failed answer is treated.
type classifiedError struct {
	kind       llm.ErrorKind
	retryable  bool
	retryAfter time.Duration // the server's hint (RetryInfo.retryDelay); 0 = none
}

// retryStatuses are retried by the policy of design 7.4.
var retryStatuses = map[int]bool{408: true, 429: true, 500: true, 502: true, 503: true, 504: true, 529: true}

// parseError classifies a non-200 answer by its status and body:
//
//   - 401, 403 auth, except a 403 or 400 whose ErrorInfo reason is
//     BILLING_DISABLED (billing); a 400 with reason API_KEY_INVALID is auth
//     (the Gemini API answers a bad key with 400);
//   - 404 model_not_found; 413 too_long; a 400 about the token count too_long;
//   - 429 RESOURCE_EXHAUSTED is rate_limited and retried, unless it is a
//     quota the next minutes will not refill: a daily quota (a QuotaFailure
//     violation whose quota id or metric says PerDay), a tier with no quota
//     ("limit: 0") or billing; those are billing and never retried;
//   - 408 and 5xx overloaded (408, 500, 502, 503, 504 and 529 retried);
//   - any other 4xx bad_request.
func parseError(status int, body []byte) classifiedError {
	var w wireError
	_ = json.Unmarshal(body, &w)
	msg := strings.ToLower(w.Error.Message)
	reasons := map[string]bool{}
	daily := false
	var delay time.Duration
	for _, d := range w.Error.Details {
		if d.Reason != "" {
			reasons[d.Reason] = true
		}
		for _, v := range d.Violations {
			if strings.Contains(v.QuotaID, "PerDay") || strings.Contains(v.QuotaMetric, "per_day") || strings.Contains(v.QuotaMetric, "PerDay") {
				daily = true
			}
		}
		if strings.HasSuffix(d.Type, "google.rpc.RetryInfo") && d.RetryDelay != "" {
			if dd, err := time.ParseDuration(d.RetryDelay); err == nil && dd > 0 {
				delay = dd
			}
		}
	}
	c := classifiedError{retryable: retryStatuses[status], retryAfter: delay}
	switch {
	case reasons["BILLING_DISABLED"]:
		c.kind, c.retryable = llm.ErrBilling, false
	case status == http.StatusTooManyRequests:
		if daily || strings.Contains(msg, "limit: 0") || strings.Contains(msg, "billing account") || strings.Contains(msg, "free tier is not available") {
			c.kind, c.retryable = llm.ErrBilling, false
		} else {
			c.kind = llm.ErrRateLimited
		}
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		c.kind = llm.ErrAuth
	case status == http.StatusBadRequest && (reasons["API_KEY_INVALID"] || strings.Contains(msg, "api key not valid")):
		c.kind = llm.ErrAuth
	case status == http.StatusNotFound:
		c.kind = llm.ErrModelNotFound
	case status == http.StatusRequestEntityTooLarge:
		c.kind = llm.ErrTooLong
	case status == http.StatusBadRequest && (strings.Contains(msg, "token count") || strings.Contains(msg, "maximum number of tokens") || strings.Contains(msg, "too long")):
		c.kind = llm.ErrTooLong
	case status == http.StatusRequestTimeout || status >= 500:
		c.kind = llm.ErrOverloaded
	case status >= 400:
		c.kind = llm.ErrBadRequest
	default:
		// A 1xx or 3xx (a redirect is refused before this): nothing a
		// caller can act on.
		c.kind, c.retryable = llm.ErrNetwork, false
	}
	return c
}
