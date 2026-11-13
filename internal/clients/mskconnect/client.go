// Package mskconnect is the AWS MSK Connect client of the managed setup
// target (row U13b1, docs/design/setup-managed.md §3.1) over the AWS SDK for
// Go v2 with the shared aws.Config (wiring.LoadAWS's pattern): reads of the
// caller identity, the MSK cluster and its bootstrap brokers, custom plugins,
// connectors, the network (subnets, security groups, route tables), the
// service execution role and the plugin archive in S3; and the setup writes
// (create a custom plugin, a worker configuration and a connector; delete a
// connector).
//
// It also implements clients.ConnectClient over MSK Connect, so the Q tools
// and the watcher read an MSK connector the way they read a REST one:
// ListConnectors and Status by name, Config redacted with
// actions.RedactConfig. RestartTask and RestartConnector answer
// clients.ErrNotSupported: MSK Connect has no restart API, and U13 adds no
// UpdateConnector op.
//
// Rules this package keeps, each a test over the cassettes in testdata/:
//   - every call has its own 10-second deadline (CallTimeout) under the
//     caller's context; the SDK retries throttling (TooManyRequestsException,
//     Throttling, 429) three times with its backoff, then the error is
//     ErrThrottled, never a loop;
//   - every SDK error maps to one of ErrBadRequest, ErrForbidden,
//     ErrNotFound, ErrConflict, ErrThrottled or ErrAWS, by its API error code
//     and HTTP status (errors.As on smithy.APIError, never string matching);
//   - a connector configuration leaves this package only through
//     actions.RedactConfig, so the materialised database password MSK
//     Connect returns from DescribeConnector (design §10 D6) is never in a
//     DTO; a caller identity's full ARN is kept only in Identity.Arn, for
//     Data, never in an error;
//   - every name, ARN, subnet and security group id is checked against a
//     strict pattern before it reaches a request (validate.go).
//
// Wiring (outside this package): the runner builds one aws.Config with
// awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region)) and passes it
// to New; the server's CONNECT_KIND=msk wiring does the same with its own
// identity (msk.md PR 2, A4's internal/wiring).
package mskconnect

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafkaconnect"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"

	"piperoom/internal/clients"
	"piperoom/internal/redact"
)

// CallTimeout bounds one SDK call (AGENTS.md rule 4), retries included.
const CallTimeout = 10 * time.Second

// RetryMaxAttempts is one call plus three retries of a throttled or
// retryable error (design §4), inside one call's deadline.
const RetryMaxAttempts = 4

// maxMessageBytes caps an AWS error message quoted in an error.
const maxMessageBytes = 240

// Errors callers branch on with errors.Is.
var (
	// ErrBadRequest: the request was refused as invalid (400,
	// BadRequestException, ValidationError, InvalidParameterValue).
	ErrBadRequest = errors.New("mskconnect: bad request")
	// ErrForbidden: the caller lacks a permission (403, ForbiddenException,
	// UnauthorizedException, AccessDenied, UnauthorizedOperation).
	ErrForbidden = errors.New("mskconnect: access denied")
	// ErrNotFound: the resource does not exist (404, NotFoundException,
	// NoSuchEntity, an EC2 *.NotFound code).
	ErrNotFound = errors.New("mskconnect: not found")
	// ErrConflict: the name is taken or the resource is mid-change (409,
	// ConflictException).
	ErrConflict = errors.New("mskconnect: conflict")
	// ErrThrottled: still throttled after the retries (429,
	// TooManyRequestsException, Throttling, RequestLimitExceeded).
	ErrThrottled = errors.New("mskconnect: throttled")
	// ErrAWS: any other AWS error (5xx, a cut body, a transport failure).
	ErrAWS = errors.New("mskconnect: aws error")
	// ErrInvalid: a name, ARN or id failed its pattern; nothing was sent.
	ErrInvalid = errors.New("mskconnect: invalid input")
	// ErrNotSupported wraps clients.ErrNotSupported for the restart calls.
	ErrNotSupported = fmt.Errorf("mskconnect: MSK Connect has no restart API: %w", clients.ErrNotSupported)
)

// Client holds one SDK client per service, all from one aws.Config.
type Client struct {
	kc      *kafkaconnect.Client
	kf      *kafka.Client
	ec2     *ec2.Client
	iam     *iam.Client
	s3      *s3.Client
	sts     *sts.Client
	timeout time.Duration
}

// New builds a Client from cfg. cfg.Region is required: MSK Connect is a
// regional API and a silent default region would read another account's
// view of the world.
func New(cfg aws.Config) (*Client, error) {
	if strings.TrimSpace(cfg.Region) == "" {
		return nil, fmt.Errorf("%w: the AWS config has no region", ErrInvalid)
	}
	if cfg.HTTPClient == nil {
		// The SDK's default client has no overall timeout; the per-call
		// context deadline is the bound, and this keeps one more.
		cfg.HTTPClient = &http.Client{Timeout: CallTimeout}
	}
	return &Client{
		kc:  kafkaconnect.NewFromConfig(cfg, func(o *kafkaconnect.Options) { o.RetryMaxAttempts = RetryMaxAttempts }),
		kf:  kafka.NewFromConfig(cfg, func(o *kafka.Options) { o.RetryMaxAttempts = RetryMaxAttempts }),
		ec2: ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.RetryMaxAttempts = RetryMaxAttempts }),
		iam: iam.NewFromConfig(cfg, func(o *iam.Options) { o.RetryMaxAttempts = RetryMaxAttempts }),
		s3:  s3.NewFromConfig(cfg, func(o *s3.Options) { o.RetryMaxAttempts = RetryMaxAttempts }),
		sts: sts.NewFromConfig(cfg, func(o *sts.Options) { o.RetryMaxAttempts = RetryMaxAttempts }),

		timeout: CallTimeout,
	}, nil
}

// call runs one SDK call under its own deadline.
func call[T any](ctx context.Context, c *Client, what string, fn func(context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	out, err := fn(ctx)
	if err != nil {
		var zero T
		return zero, mapError(what, err)
	}
	return out, nil
}

// Error codes by class. The SDKs name the same condition differently per
// protocol (REST-JSON exceptions for kafkaconnect and kafka, query-protocol
// codes for IAM, EC2, STS, S3's HEAD status).
var (
	badRequestCodes = []string{"BadRequestException", "ValidationError", "ValidationException", "InvalidParameterValue", "InvalidParameterCombination", "MalformedPolicyDocument", "InvalidInput"}
	forbiddenCodes  = []string{"ForbiddenException", "UnauthorizedException", "AccessDenied", "AccessDeniedException", "UnauthorizedOperation", "InvalidClientTokenId", "ExpiredToken", "Forbidden"}
	notFoundCodes   = []string{"NotFoundException", "NoSuchEntity", "NoSuchKey", "NoSuchBucket", "NotFound"}
	conflictCodes   = []string{"ConflictException", "EntityAlreadyExists"}
	throttleCodes   = []string{"TooManyRequestsException", "Throttling", "ThrottlingException", "RequestLimitExceeded", "SlowDown"}
)

// mapError classifies an SDK error. The message is the API's own text,
// redacted and clipped; the request id and the full SDK error string (which
// can quote ARNs and URLs) are not repeated.
func mapError(what string, err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: %s: %w", ErrAWS, what, err)
	}
	code, msg := "", ""
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code, msg = apiErr.ErrorCode(), apiErr.ErrorMessage()
	}
	status := 0
	var withStatus interface{ HTTPStatusCode() int }
	if errors.As(err, &withStatus) {
		status = withStatus.HTTPStatusCode()
	}
	class := ErrAWS
	switch {
	case inCodes(code, throttleCodes) || status == http.StatusTooManyRequests:
		class = ErrThrottled
	case inCodes(code, forbiddenCodes) || status == http.StatusForbidden || status == http.StatusUnauthorized:
		class = ErrForbidden
	case inCodes(code, notFoundCodes) || strings.HasSuffix(code, ".NotFound") || status == http.StatusNotFound:
		class = ErrNotFound
	case inCodes(code, conflictCodes) || status == http.StatusConflict:
		class = ErrConflict
	case inCodes(code, badRequestCodes) || strings.HasPrefix(code, "Invalid") || status == http.StatusBadRequest:
		class = ErrBadRequest
	}
	detail := code
	if detail == "" && status != 0 {
		detail = fmt.Sprintf("HTTP %d", status)
	}
	if detail == "" {
		// No API error and no status: a transport failure or a body that
		// did not decode. Its text can quote the request URL.
		detail = "request failed"
	}
	if msg != "" {
		m, _ := redact.Text(msg)
		detail += ": " + clip(m, maxMessageBytes)
	}
	return fmt.Errorf("%w: %s: %s", class, what, detail)
}

func inCodes(code string, set []string) bool {
	for _, c := range set {
		if code == c {
			return true
		}
	}
	return false
}

// clip cuts s to max bytes on a rune boundary, with no control characters.
func clip(s string, max int) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, strings.ToValidUTF8(s, "?"))
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return s[:cut] + "…"
}

// str dereferences an SDK string pointer.
func str(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
