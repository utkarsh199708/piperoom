package converse

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"

	"piperoom/internal/agent/llm"
)

// maxResponseBytes caps a response body (AGENTS.md rule 7); a model turn is
// far smaller.
const maxResponseBytes = 4 << 20

// errResponseTooLarge is the read error of a body past maxResponseBytes.
var errResponseTooLarge = fmt.Errorf("llm/converse: response body over %d bytes", maxResponseBytes)

// cappedClient caps every response body the SDK reads.
type cappedClient struct {
	next bedrockruntime.HTTPClient
}

func (c cappedClient) Do(r *http.Request) (*http.Response, error) {
	resp, err := c.next.Do(r)
	if err != nil || resp == nil || resp.Body == nil {
		return resp, err
	}
	resp.Body = &cappedBody{rc: resp.Body, left: maxResponseBytes}
	return resp, nil
}

// cappedBody fails the read that passes the cap instead of cutting the body
// short, so a cut answer is never decoded as a whole one.
type cappedBody struct {
	rc   io.ReadCloser
	left int64
}

func (b *cappedBody) Read(p []byte) (int, error) {
	// One byte past the cap is allowed through the limit so that reading it
	// proves the body is too large.
	if int64(len(p)) > b.left+1 {
		p = p[:b.left+1]
	}
	n, err := b.rc.Read(p)
	b.left -= int64(n)
	if b.left < 0 {
		return 0, errResponseTooLarge
	}
	return n, err
}

func (b *cappedBody) Close() error { return b.rc.Close() }

// wireSize is the size of one turn's request and response bodies on the wire
// (the last attempt's, when the SDK retried): the truncation check and the
// usage estimate read it.
type wireSize struct {
	req, resp atomic.Int64
}

func (s *wireSize) request() int64  { return s.req.Load() }
func (s *wireSize) response() int64 { return s.resp.Load() }

// recordingClient records one turn's sizes. It is set per call, so
// concurrent turns never share one.
type recordingClient struct {
	next bedrockruntime.HTTPClient
	size *wireSize
}

func (c recordingClient) Do(r *http.Request) (*http.Response, error) {
	if r.ContentLength > 0 {
		c.size.req.Store(r.ContentLength)
	}
	resp, err := c.next.Do(r)
	if err != nil || resp == nil || resp.Body == nil {
		return resp, err
	}
	c.size.resp.Store(0)
	resp.Body = &countingBody{rc: resp.Body, n: &c.size.resp}
	return resp, nil
}

type countingBody struct {
	rc io.ReadCloser
	n  *atomic.Int64
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	b.n.Add(int64(n))
	return n, err
}

func (b *countingBody) Close() error { return b.rc.Close() }

// truncationRatio: a reported prompt below half of the lower-bound estimate
// of the request just sent (request bytes / 4) means the server cut it
// (design 6.2).
const (
	bytesPerTokenLowerBound = 4
	truncationRatio         = 2
)

// truncated returns an ErrTruncated error when the reported prompt tokens
// are far below what the request's bytes must hold. An estimated usage and
// an unknown request size skip the check: there is nothing to compare.
func truncated(u llm.Usage, requestBytes int64) error {
	if u.Estimated || requestBytes <= 0 {
		return nil
	}
	prompt := u.Input + u.CacheRead + u.CacheWrite
	lower := requestBytes / bytesPerTokenLowerBound
	if prompt*truncationRatio >= lower {
		return nil
	}
	ae := &llm.APIError{
		Provider: llm.ProviderBedrockConverse,
		Kind:     llm.ErrTruncated,
		Err:      errors.New("llm/converse: the reported prompt is under half of the request's size"),
	}
	return &classified{api: ae, text: fmt.Sprintf("%s: the model server truncated the prompt (%d prompt tokens reported for a %d-byte request); raise its context length", ae.Error(), prompt, requestBytes)}
}
