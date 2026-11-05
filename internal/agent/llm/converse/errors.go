package converse

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"unicode/utf8"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"

	"piperoom/internal/agent/llm"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/redact"
)

// maxErrorBody caps APIError.Body.
const maxErrorBody = 2 << 10

// apiError is the SDK's modelled error (smithy.APIError), matched by its
// methods so this package needs no direct smithy-go import.
type apiError interface {
	error
	ErrorCode() string
	ErrorMessage() string
}

// errorCodes are the error codes Bedrock Runtime and the AWS front end name.
// Only these are shown in an error's text: a code is a fixed token, never
// request content.
var errorCodes = map[string]bool{
	"AccessDeniedException": true, "ResourceNotFoundException": true, "ThrottlingException": true,
	"ServiceQuotaExceededException": true, "ValidationException": true, "ModelTimeoutException": true,
	"ModelNotReadyException": true, "ModelErrorException": true, "InternalServerException": true,
	"ServiceUnavailableException": true, "ConflictException": true, "UnrecognizedClientException": true,
	"ExpiredTokenException": true, "InvalidSignatureException": true, "IncompleteSignature": true,
	"MissingAuthenticationTokenException": true,
}

// authCodes are the AWS front end's codes for a bad, expired or missing
// credential, whatever the status.
var authCodes = map[string]bool{
	"UnrecognizedClientException": true, "ExpiredTokenException": true, "InvalidSignatureException": true,
	"IncompleteSignature": true, "MissingAuthenticationTokenException": true,
}

// tooLongMarkers are lower-case fragments of a ValidationException that say
// the conversation outgrew the model's context window.
var tooLongMarkers = []string{
	"too long", "too many input tokens", "too many tokens", "context length", "context window",
	"maximum context", "max context", "exceeds the maximum",
}

// modelAccessMarkers are lower-case fragments of an AccessDeniedException
// that say the account cannot use this model id (not enabled, not offered in
// the region), not that the credential is bad.
var modelAccessMarkers = []string{"access to the model", "model access", "specified model"}

// quotaMarkers are lower-case fragments of a 429 that mean a quota, not a
// rate: a retry within seconds cannot succeed (design 7.4, never retried).
var quotaMarkers = []string{"per day", "daily", "quota"}

// classifyError turns a failed Converse call into an error that wraps an
// *llm.APIError (errors.As finds it, and through it the SDK's error). Its
// text is safe for a room event: the APIError's provider, kind and status, a
// fixed hint per kind and the AWS error code when it is a known one, never
// the message, which can echo prompt content (design 10.2). log gets the
// redacted, capped message at debug level.
func classifyError(err error, key *netsec.Secret, log *slog.Logger) error {
	if err == nil {
		return nil
	}
	var ae *llm.APIError
	if errors.As(err, &ae) {
		return err
	}
	status := httpStatus(err)
	var code, msg string
	var me apiError
	if errors.As(err, &me) {
		code, msg = me.ErrorCode(), me.ErrorMessage()
	}
	if status == 0 && credentialFailure(err) {
		// The request was never signed, so never sent: a fresh run repeats
		// it until the credentials are fixed (unretryable auth).
		out := &llm.APIError{Provider: llm.ProviderBedrockConverse, Kind: llm.ErrAuth, Err: err}
		if log != nil {
			log.Debug("llm/converse: request not signed", "provider", out.Provider, "kind", out.Kind, "err", capText(scrub(err.Error(), key)))
		}
		return &classified{api: out, text: out.Error() + ": could not retrieve AWS credentials or sign the request"}
	}
	if status == 0 {
		out := &llm.APIError{Provider: llm.ProviderBedrockConverse, Kind: llm.ErrNetwork, Err: err}
		if log != nil {
			log.Debug("llm/converse: request failed", "provider", out.Provider, "kind", out.Kind, "err", capText(scrub(err.Error(), key)))
		}
		return &classified{api: out, text: out.Error() + ": " + scrub(networkHint(err), key)}
	}
	body := msg
	if code != "" {
		body = code + ": " + msg
	}
	body = scrub(body, key)
	out := &llm.APIError{
		Provider: llm.ProviderBedrockConverse,
		Status:   status,
		Kind:     Kind(status, code, msg),
		Body:     capText(body),
		Err:      err,
	}
	if log != nil {
		log.Debug("llm/converse: request failed", "provider", out.Provider, "status", out.Status, "kind", out.Kind, "body", out.Body)
	}
	text := out.Error() + ": " + kindHint(out.Kind)
	if errorCodes[code] {
		text += " (" + code + ")"
	}
	return &classified{api: out, text: text}
}

// scrub masks secrets in s: the Bedrock API key literally (it has no
// pattern redact knows), then every pattern redact knows.
func scrub(s string, key *netsec.Secret) string {
	if key != nil && key.Reveal() != "" {
		s = strings.ReplaceAll(s, key.Reveal(), "[redacted]")
	}
	s, _ = redact.Text(s)
	return s
}

// credentialFailure reports a request that failed before it was sent because
// the AWS credentials could not be retrieved (markedCredentials) or SigV4
// signing failed (*v4.SigningError).
func credentialFailure(err error) bool {
	var se *v4.SigningError
	var ce *credentialsError
	return errors.As(err, &se) || errors.As(err, &ce)
}

// httpStatus is the status of the HTTP answer err carries, 0 without one.
func httpStatus(err error) int {
	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.ResponseError != nil {
		return re.HTTPStatusCode()
	}
	return 0
}

// Kind classifies a Converse error by its HTTP status, its AWS error code
// and its message:
//
//   - ServiceQuotaExceededException, and a 429 whose message names a daily
//     limit or a quota, are billing (quota exhausted; never retried);
//   - ThrottlingException and any other 429 are rate_limited;
//   - ModelNotReady, ModelTimeout, ModelError, InternalServer,
//     ServiceUnavailable, 408 and 5xx are overloaded;
//   - ResourceNotFoundException and 404 are model_not_found, and so are an
//     AccessDeniedException about access to the model (not enabled for the
//     account) and a ValidationException about the model identifier;
//   - a credential code (UnrecognizedClient, ExpiredToken, signature
//     errors), 401 and any other 403 are auth;
//   - 413 and a 400 whose message says the input is too long are too_long;
//   - any other 4xx is bad_request.
func Kind(status int, code, message string) llm.ErrorKind {
	m := strings.ToLower(message)
	switch {
	case code == "ServiceQuotaExceededException":
		return llm.ErrBilling
	case status == http.StatusTooManyRequests && containsAny(m, quotaMarkers):
		return llm.ErrBilling
	case code == "ThrottlingException", status == http.StatusTooManyRequests:
		return llm.ErrRateLimited
	case code == "ModelNotReadyException", code == "ModelTimeoutException", code == "ModelErrorException",
		code == "InternalServerException", code == "ServiceUnavailableException",
		status == http.StatusRequestTimeout, status >= 500:
		return llm.ErrOverloaded
	case code == "ResourceNotFoundException", status == http.StatusNotFound:
		return llm.ErrModelNotFound
	case authCodes[code], status == http.StatusUnauthorized:
		return llm.ErrAuth
	case code == "AccessDeniedException" || status == http.StatusForbidden:
		if containsAny(m, modelAccessMarkers) {
			return llm.ErrModelNotFound
		}
		return llm.ErrAuth
	case status == http.StatusRequestEntityTooLarge, containsAny(m, tooLongMarkers):
		return llm.ErrTooLong
	case strings.Contains(m, "model identifier") || strings.Contains(m, "model id is invalid"):
		return llm.ErrModelNotFound
	case status < 400:
		// Not an error status; nothing a caller can act on.
		return llm.ErrNetwork
	}
	return llm.ErrBadRequest
}

func containsAny(s string, frags []string) bool {
	for _, f := range frags {
		if strings.Contains(s, f) {
			return true
		}
	}
	return false
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
		return "check the AWS credentials or the Bedrock API key and their access"
	case llm.ErrBilling:
		return "a Bedrock quota is exhausted"
	case llm.ErrModelNotFound:
		return "the model is unknown or not enabled for this account and region"
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
// text cannot hold response content: the caller's context or a transport
// error. Anything else gets the fixed hint.
func networkHint(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded.Error()
	case errors.Is(err, context.Canceled):
		return context.Canceled.Error()
	case errors.Is(err, errResponseTooLarge):
		return errResponseTooLarge.Error()
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return capText(ne.Error())
	}
	return kindHint(llm.ErrNetwork)
}

// classified is a classified failure: errors.As finds its *llm.APIError (and,
// through it, the SDK error or the network cause). Its text is safe for a
// room event (see classifyError).
type classified struct {
	api  *llm.APIError
	text string
}

func (c *classified) Error() string { return c.text }

func (c *classified) Unwrap() error { return c.api }
