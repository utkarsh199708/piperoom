// Package gemini is the llm.Model adapter for Google Gemini: the Gemini API
// (generativelanguage.googleapis.com, an x-goog-api-key key) and Gemini on
// Vertex AI (PIPEROOM_GEMINI_BACKEND=vertex, Google application default
// credentials). Both speak generateContent; only the URL and the
// authorisation differ. Design docs/design/model-providers.md sections 3, 7
// and 10.3, board row V4.
//
// It talks plain net/http (no SDK), non-streaming (design 3.1). Requests leave
// through the model transport only: a dialer that refuses the metadata deny
// list (config.ModelAddrDenied) on every dialed address, no proxy unless
// PIPEROOM_MODEL_PROXY=on (and then only after the target is checked), no
// redirects, and a guard that sends nothing to a host other than the
// configured one. On Vertex the OAuth token source has its own client, the
// only one that may reach a metadata server; only its tokens cross over to
// the model transport.
package gemini

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"golang.org/x/oauth2"

	"piperoom/internal/agent/llm"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
)

// TurnTimeout bounds one model turn, retries included (design 7.4: never past
// the turn's 10 min deadline).
const TurnTimeout = 10 * time.Minute

// maxResponseBytes caps a response body (AGENTS.md rule 7); a turn is far
// smaller.
const maxResponseBytes = 4 << 20

// Capability table values (design section 3; the fixtures prove the wire).
const (
	// contextTokens is the context window of Gemini 2.5 and later.
	contextTokens = 1_048_576
	// legacyContextTokens is assumed for older or unknown models.
	legacyContextTokens = 128_000
	// maxTools is the most function declarations one request may carry.
	maxTools = 128
	// toolNameMax is the longest function name Gemini accepts (design 7.1).
	toolNameMax = 64
)

// Default endpoints.
const (
	defaultAPIBase = "https://generativelanguage.googleapis.com/v1beta"
	vertexScope    = "https://www.googleapis.com/auth/cloud-platform"
)

// Options configures the adapter. Keys and credentials come from the caller
// (V6 wiring reads them at construction); none is ever stored in config.
type Options struct {
	// Backend is config.GeminiBackendAPI (default) or config.GeminiBackendVertex.
	Backend string
	// BaseURL overrides the endpoint (PIPEROOM_MODEL_BASE_URL, validated by
	// config.ParseBaseURL); nil = the backend's default.
	BaseURL *config.BaseURL
	// APIKey is the Gemini API key (backend api). It is sent only as the
	// x-goog-api-key header, only to BaseURL's host.
	APIKey netsec.Secret
	// Project and Region are VERTEX_PROJECT_ID and VERTEX_REGION (backend
	// vertex). Region "global" uses the global endpoint.
	Project, Region string
	// TokenSource authorises Vertex requests; nil = Google application
	// default credentials looked up through CredentialClient.
	TokenSource oauth2.TokenSource
	// CredentialClient is the client the default credentials fetch tokens
	// with: the only client that may reach a metadata server. nil = a client
	// on a clone of http.DefaultTransport with a 30 s timeout.
	CredentialClient *http.Client
	// Proxy is PIPEROOM_MODEL_PROXY=on: model requests go through
	// HTTPS_PROXY after the target is checked against the deny list.
	Proxy bool
	// ContextTokens overrides the context window (PIPEROOM_MODEL_CONTEXT_TOKENS).
	ContextTokens int64
	// MaxConcurrency bounds requests in flight (PIPEROOM_MODEL_MAX_CONCURRENCY);
	// 0 = no bound.
	MaxConcurrency int64
	// TPM is a tokens-per-minute budget (PIPEROOM_MODEL_TPM); 0 = none.
	TPM int64
	// Log gets debug detail (redacted error bodies) and the long-wait note;
	// nil = slog.Default().
	Log *slog.Logger
}

// Model is the Gemini adapter. It is safe for concurrent use.
type Model struct {
	backend  string
	base     config.BaseURL
	project  string
	region   string
	ctxToks  int64
	client   *http.Client
	retry    retrier
	limit    limiter
	log      *slog.Logger
	endpoint string // for logs: scheme://host only
	// hideKey removes the key from error text. A closure, not the key: fmt's
	// %+v prints unexported fields without calling netsec.Secret's String.
	hideKey func(string) string
}

var _ llm.Model = (*Model)(nil)

var (
	regionRe  = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	projectRe = regexp.MustCompile(`^[a-z0-9][a-z0-9.:-]{0,127}$`)
	modelRe   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._@-]{0,127}$`)
)

// New builds the adapter. ctx is used for the credential lookup and kept
// (without its cancellation) by the token source for later refreshes. It
// makes no model request.
func New(ctx context.Context, o Options) (*Model, error) {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	m := &Model{
		backend: o.Backend,
		ctxToks: o.ContextTokens,
		log:     o.Log,
		hideKey: func(s string) string {
			if k := o.APIKey.Reveal(); k != "" {
				return strings.ReplaceAll(s, k, "[redacted]")
			}
			return s
		},
		retry: newBackoff(),
		limit: newEndpointLimiter(o.MaxConcurrency, o.TPM, o.Log),
	}
	if m.backend == "" {
		m.backend = config.GeminiBackendAPI
	}
	switch m.backend {
	case config.GeminiBackendAPI:
		if o.APIKey == "" {
			return nil, errors.New("llm/gemini: no API key (GEMINI_API_KEY or PIPEROOM_MODEL_API_KEY)")
		}
	case config.GeminiBackendVertex:
		if !projectRe.MatchString(o.Project) {
			return nil, errors.New("llm/gemini: VERTEX_PROJECT_ID is missing or not a project id")
		}
		if !regionRe.MatchString(o.Region) {
			return nil, errors.New("llm/gemini: VERTEX_REGION is missing or not a region")
		}
		m.project, m.region = o.Project, o.Region
	default:
		return nil, fmt.Errorf("llm/gemini: backend %q: want api or vertex", o.Backend)
	}
	if o.BaseURL != nil {
		m.base = *o.BaseURL
	} else {
		def := defaultAPIBase
		if m.backend == config.GeminiBackendVertex {
			def = vertexBase(m.region)
		}
		b, err := config.ParseBaseURL("PIPEROOM_MODEL_BASE_URL", def, false)
		if err != nil {
			return nil, fmt.Errorf("llm/gemini: default base URL: %w", err)
		}
		m.base = b
	}
	m.endpoint = m.base.Redacted()

	rt := modelTransport(m.base, o.Proxy, nil)
	var auth http.RoundTripper
	if m.backend == config.GeminiBackendAPI {
		auth = &apiKeyTransport{key: o.APIKey, base: rt}
	} else {
		ts := o.TokenSource
		if ts == nil {
			var err error
			if ts, err = defaultTokenSource(ctx, o.CredentialClient); err != nil {
				return nil, err
			}
		}
		// Only the token crosses over: the request itself goes out on the
		// deny-listed model transport (design 10.3).
		auth = &oauth2.Transport{Source: ts, Base: rt}
	}
	m.client = &http.Client{
		Timeout:       TurnTimeout,
		Transport:     &hostGuard{host: m.base.URL.Host, base: auth},
		CheckRedirect: refuseRedirect,
	}
	return m, nil
}

func vertexBase(region string) string {
	if region == "global" {
		return "https://aiplatform.googleapis.com/v1"
	}
	return "https://" + region + "-aiplatform.googleapis.com/v1"
}

// Provider reports llm.ProviderGemini.
func (m *Model) Provider() string { return llm.ProviderGemini }

// Capabilities reports the table for model; it never calls the network.
// Gemini 2.5 and later take a JSON Schema for tools and output, think with a
// level or budget, and cache implicitly; older or unknown models get the
// conservative row.
func (m *Model) Capabilities(model string) llm.Capabilities {
	c := llm.Capabilities{
		ToolCalls:         true,
		ParallelToolCalls: true,
		ContextTokens:     legacyContextTokens,
		MaxTools:          maxTools,
		ToolNameMax:       toolNameMax,
	}
	if modern(model) {
		c.StructuredOutput = true
		c.PromptCache = true
		c.Reasoning = true
		c.Effort = true
		c.ContextTokens = contextTokens
	}
	if m.ctxToks > 0 {
		c.ContextTokens = m.ctxToks
	}
	return c
}

// url is the generateContent URL of model.
func (m *Model) url(model string) string {
	base := strings.TrimSuffix(m.base.String(), "/")
	if m.backend == config.GeminiBackendVertex {
		return base + "/projects/" + m.project + "/locations/" + m.region + "/publishers/google/models/" + model + ":generateContent"
	}
	return base + "/models/" + model + ":generateContent"
}

// Turn sends req as one generateContent request (retried by the policy of
// design 7.4) and decodes the answer. Every failure is an *llm.APIError whose
// text never carries the provider's body; one that was never sent wraps
// llm.ErrNotSent.
func (m *Model) Turn(ctx context.Context, req llm.Request) (llm.Response, error) {
	model := strings.TrimPrefix(req.Model, "models/")
	if !modelRe.MatchString(model) {
		return llm.Response{}, notSent(errors.New("model id is not a Gemini model id"))
	}
	body, lower, err := encodeRequest(model, req)
	if err != nil {
		return llm.Response{}, notSent(err)
	}
	ctx, cancel := context.WithTimeout(ctx, TurnTimeout)
	defer cancel()

	release, err := m.limit.acquire(ctx, int64(len(body)/3)+req.MaxTokens)
	if err != nil {
		return llm.Response{}, &llm.APIError{Provider: llm.ProviderGemini, Kind: llm.ErrRateLimited, Err: fmt.Errorf("%w: %w", llm.ErrNotSent, err)}
	}
	defer release()

	var (
		status  int
		header  http.Header
		payload []byte
		sendErr error
	)
	url := m.url(model)
	waitErr := m.retry.do(ctx, func(ctx context.Context) attempt {
		status, header, payload, sendErr = m.send(ctx, url, body)
		if sendErr != nil {
			return attempt{retryable: retryableSendError(ctx, sendErr)}
		}
		if status == http.StatusOK {
			return attempt{status: status}
		}
		e := parseError(status, payload)
		return attempt{status: status, retryable: e.retryable, retryAfter: max(e.retryAfter, retryAfterHeader(header))}
	})
	if sendErr != nil {
		if waitErr != nil {
			sendErr = errors.Join(sendErr, waitErr)
		}
		m.log.Debug("llm/gemini: request failed", "endpoint", m.endpoint, "kind", llm.ErrNetwork, "err", m.scrub(sendErr.Error()))
		return llm.Response{}, &llm.APIError{Provider: llm.ProviderGemini, Kind: llm.ErrNetwork, Err: sendErr}
	}
	if status != http.StatusOK {
		e := parseError(status, payload)
		ae := &llm.APIError{Provider: llm.ProviderGemini, Status: status, Kind: e.kind, Body: m.scrub(string(payload)), Err: waitErr}
		m.log.Debug("llm/gemini: request failed", "endpoint", m.endpoint, "status", status, "kind", ae.Kind, "body", ae.Body)
		return llm.Response{}, ae
	}
	resp, err := decodeResponse(model, req, payload)
	if err != nil {
		return llm.Response{}, &llm.APIError{Provider: llm.ProviderGemini, Status: status, Kind: llm.ErrBadRequest, Err: err}
	}
	if resp.Usage.Estimated {
		resp.Usage = estimate(body, payload)
		resp.Usage.Estimated = true
	} else if truncated(resp.Usage, lower) {
		// The server cut the prompt to fit (design 6.2): the run must stop,
		// not continue on a prompt that lost its system rules.
		return resp, &llm.APIError{Provider: llm.ProviderGemini, Status: status, Kind: llm.ErrTruncated}
	}
	return resp, nil
}

// scrub removes the key literally and other secrets by pattern, and caps s.
func (m *Model) scrub(s string) string {
	return safeText(m.hideKey(s))
}

// send makes one HTTP attempt and reads the capped body.
func (m *Model) send(ctx context.Context, url string, body []byte) (int, http.Header, []byte, error) {
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, nil, fmt.Errorf("llm/gemini: build request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	resp, err := m.client.Do(hreq)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		// The response had started: never retried (design 7.4).
		return 0, nil, nil, &startedError{err: err}
	}
	if len(b) > maxResponseBytes {
		return 0, nil, nil, &startedError{err: errors.New("llm/gemini: response over 4 MiB")}
	}
	return resp.StatusCode, resp.Header, b, nil
}

// startedError is a failure after the response began; it is not retried.
type startedError struct{ err error }

func (e *startedError) Error() string { return "llm/gemini: read response: " + e.err.Error() }
func (e *startedError) Unwrap() error { return e.err }

// retryableSendError reports a failure with no response byte that a retry
// may fix: not the caller's context, not a policy refusal (deny list,
// redirect, wrong host), not a response that had started.
func retryableSendError(ctx context.Context, err error) bool {
	var se *startedError
	switch {
	case ctx.Err() != nil, errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, errDenied), errors.Is(err, errRedirect), errors.As(err, &se):
		return false
	}
	return true
}

func notSent(err error) error {
	return &llm.APIError{Provider: llm.ProviderGemini, Kind: llm.ErrBadRequest, Err: fmt.Errorf("%w: %w", llm.ErrNotSent, err)}
}

// encodeJSON marshals v without HTML escaping, so text arrives as written.
func encodeJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
