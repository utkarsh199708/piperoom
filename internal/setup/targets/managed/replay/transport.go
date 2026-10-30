package replay

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// maxBodyBytes caps a request or response body read by the harness.
const maxBodyBytes = 4 << 20

// Replayer is an http.RoundTripper that answers from a cassette. Each
// interaction is used once, in order among the ones that match, so a polled
// request (an async state sequence) walks its recorded responses. The live
// request is scrubbed with the same Scrubber before it is compared, so a
// test that sends a real-looking password matches the REDACTED body the
// recorder wrote.
type Replayer struct {
	cassette *Cassette
	scrub    Scrubber

	mu        sync.Mutex
	used      []bool
	unmatched []string
}

// NewReplayer returns a Replayer over c.
func NewReplayer(c *Cassette, s Scrubber) *Replayer {
	return &Replayer{cassette: c, scrub: s, used: make([]bool, len(c.Interactions))}
}

// Client returns an *http.Client over the replayer with a timeout, the
// shape every client package takes.
func (r *Replayer) Client() *http.Client {
	return &http.Client{Transport: r, Timeout: 10 * time.Second}
}

// RoundTrip answers req from the first unused matching interaction, or
// fails with ErrUnmatched naming the method and path.
func (r *Replayer) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := readBody(req)
	if err != nil {
		return nil, err
	}
	got := Request{
		Method: req.Method,
		Path:   r.scrub.Path(req.URL),
		Query:  r.scrub.Query(req.URL.RawQuery),
		Body:   r.scrub.Body(req.Header.Get("Content-Type"), body),
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, it := range r.cassette.Interactions {
		if r.used[i] || !matches(it.Request, got) {
			continue
		}
		r.used[i] = true
		return response(req, it.Response), nil
	}
	r.unmatched = append(r.unmatched, got.Method+" "+got.Path)
	return nil, fmt.Errorf("%w: %s %s (cassette %s)", ErrUnmatched, got.Method, got.Path, r.cassette.Name)
}

// Unmatched lists the requests no interaction answered.
func (r *Replayer) Unmatched() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.unmatched...)
}

// Remaining counts the interactions not used yet.
func (r *Replayer) Remaining() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, u := range r.used {
		if !u {
			n++
		}
	}
	return n
}

// matches compares a recorded request with a live one. Method and path
// always; query when the recording has one; the body by hash, or by its
// normalised text, when the recording has either.
func matches(rec, got Request) bool {
	if !strings.EqualFold(rec.Method, got.Method) || rec.Path != got.Path {
		return false
	}
	if rec.Query != "" && rec.Query != got.Query {
		return false
	}
	switch {
	case rec.BodySHA256 != "":
		return rec.BodySHA256 == bodyHash(got.Body)
	case rec.Body != "":
		return rec.Body == got.Body
	}
	return true
}

func bodyHash(b string) string {
	sum := sha256.Sum256([]byte(b))
	return hex.EncodeToString(sum[:])
}

func response(req *http.Request, rec Response) *http.Response {
	h := http.Header{}
	for k, v := range rec.Headers {
		h.Set(k, v)
	}
	if h.Get("Content-Type") == "" && looksJSON([]byte(rec.Body)) {
		h.Set("Content-Type", "application/json")
	}
	var body io.ReadCloser = io.NopCloser(strings.NewReader(rec.Body))
	length := int64(len(rec.Body))
	if rec.Cut {
		body = &cutBody{r: strings.NewReader(rec.Body)}
		// The advertised length is longer than what arrives, as with a
		// dropped connection.
		length = int64(len(rec.Body)) + 1024
	}
	return &http.Response{
		Status:        fmt.Sprintf("%d %s", rec.Status, http.StatusText(rec.Status)),
		StatusCode:    rec.Status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        h,
		Body:          body,
		ContentLength: length,
		Request:       req,
	}
}

// cutBody yields its text and then io.ErrUnexpectedEOF.
type cutBody struct{ r *strings.Reader }

func (c *cutBody) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if errors.Is(err, io.EOF) {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

func (c *cutBody) Close() error { return nil }

// readBody reads and restores a request body, capped.
func readBody(req *http.Request) ([]byte, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return nil, nil
	}
	raw, err := io.ReadAll(io.LimitReader(req.Body, maxBodyBytes+1))
	_ = req.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("replay: reading request body: %w", err)
	}
	if len(raw) > maxBodyBytes {
		return nil, fmt.Errorf("replay: request body over %d bytes", maxBodyBytes)
	}
	req.Body = io.NopCloser(bytes.NewReader(raw))
	return raw, nil
}

// Recorder is an http.RoundTripper that forwards to Next and writes each
// exchange, scrubbed, into a cassette. It is used only by the
// `//go:build manual` tests that run against a sandbox account.
type Recorder struct {
	Next  http.RoundTripper
	Scrub Scrubber

	mu       sync.Mutex
	cassette Cassette
}

// NewRecorder starts a cassette for provider and name.
func NewRecorder(next http.RoundTripper, s Scrubber, provider, name, source string, now time.Time) *Recorder {
	if next == nil {
		next = http.DefaultTransport
	}
	return &Recorder{Next: next, Scrub: s, cassette: Cassette{
		Provider: provider, Name: name, RecordedAt: now.UTC(), Recorded: true, Source: source,
	}}
}

// RoundTrip forwards req and records the scrubbed exchange. The caller gets
// the real response; only the cassette is scrubbed.
func (r *Recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := readBody(req)
	if err != nil {
		return nil, err
	}
	resp, err := r.Next.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	raw, rerr := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	_ = resp.Body.Close()
	cut := rerr != nil || len(raw) > maxBodyBytes
	if len(raw) > maxBodyBytes {
		raw = raw[:maxBodyBytes]
	}
	if rerr != nil {
		// The caller sees the same cut the recording keeps.
		resp.Body = &cutBody{r: strings.NewReader(string(raw))}
	} else {
		resp.Body = io.NopCloser(bytes.NewReader(raw))
	}
	scrubbed := r.Scrub.Body(req.Header.Get("Content-Type"), body)
	it := Interaction{
		Request: Request{
			Method:  req.Method,
			Path:    r.Scrub.Path(req.URL),
			Query:   r.Scrub.Query(req.URL.RawQuery),
			Body:    scrubbed,
			Headers: r.Scrub.RequestHeaders(req.Header),
		},
		Response: Response{
			Status:  resp.StatusCode,
			Headers: r.Scrub.ResponseHeaders(resp.Header),
			Body:    r.Scrub.Body(resp.Header.Get("Content-Type"), raw),
			Cut:     cut,
		},
	}
	if scrubbed != "" {
		it.Request.BodySHA256 = bodyHash(scrubbed)
	}
	r.mu.Lock()
	r.cassette.Interactions = append(r.cassette.Interactions, it)
	r.mu.Unlock()
	return resp, nil
}

// Cassette returns a copy of what was recorded so far.
func (r *Recorder) Cassette() *Cassette {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.cassette
	c.Interactions = append([]Interaction(nil), r.cassette.Interactions...)
	return &c
}
