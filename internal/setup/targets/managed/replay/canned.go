package replay

import (
	"fmt"
	"io"
	"net/http"
	"sync"
)

// Canned is an http.RoundTripper that answers each request with the next of
// its Responses, in order, whatever the request. The cassette generators of
// the client packages (`-tags cassettegen`) put it behind a Recorder: the
// responses are written by hand from the provider's documented shapes, and
// the recorded requests are exactly what the client sends. Such a cassette
// is saved with Recorded false; a sandbox run (`-tags manual`) replaces it.
type Canned struct {
	mu        sync.Mutex
	responses []Response
	next      int
}

// NewCanned returns a Canned over responses.
func NewCanned(responses ...Response) *Canned {
	return &Canned{responses: responses}
}

// RoundTrip answers with the next response, or an error when none is left.
func (c *Canned) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(req.Body, maxBodyBytes))
		_ = req.Body.Close()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.next >= len(c.responses) {
		return nil, fmt.Errorf("replay: canned: no response left for %s %s", req.Method, req.URL.Path)
	}
	r := c.responses[c.next]
	c.next++
	return response(req, r), nil
}

// Left counts the responses not served yet.
func (c *Canned) Left() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.responses) - c.next
}
