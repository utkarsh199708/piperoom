// Package replay is the recorded-fakes harness of the managed setup targets
// (row U13a, docs/design/setup-managed.md §5): HTTP cassettes that the
// MSK Connect, Confluent Cloud and Aiven clients replay in `go test ./...`
// without network, a recorder that writes them from a sandbox run behind
// `//go:build manual`, and a loader that refuses a cassette with anything
// credential-shaped in it.
//
// Three rules, each a test:
//   - scrub at record time, requests and responses alike: identifiers
//     (account ids, ARNs, Confluent and Aiven ids, cloud hostnames, the names a
//     recording session lists) become fixed placeholders, and every value
//     under a secret-named JSON key, every Authorization or SigV4 header and
//     every URL userinfo becomes REDACTED (scrub.go);
//   - Load scans every string of a cassette with redact.Text's patterns plus
//     the AWS, Confluent and Aiven key shapes, URL userinfo and the secret key
//     table, and a hit fails it (ErrCredential), so a cassette that slipped
//     past the scrubber cannot be loaded by any test (check.go);
//   - an unmatched request fails the call with its method and path, never a
//     silent default response (transport.go).
//
// A cassette carries Recorded: false when it was written by hand from the
// provider's documented shapes rather than recorded on a sandbox account;
// the replay works the same, and the flag is what tells a reader the
// response shapes are not yet confirmed by a live run.
package replay

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// StaleAfter is the age after which a cassette is flagged as stale: a
// warning that the provider's API may have moved, never a failure.
const StaleAfter = 180 * 24 * time.Hour

// MaxCassetteBytes caps one cassette file (AGENTS.md rule 7's 4 MB).
const MaxCassetteBytes = 4 << 20

// Providers a cassette can belong to.
const (
	ProviderAWS       = "aws"
	ProviderConfluent = "confluent"
	ProviderAiven     = "aiven"
	// ProviderTest is the harness's own test cassettes.
	ProviderTest = "test"
)

// Errors callers branch on with errors.Is.
var (
	// ErrCredential: a cassette holds a credential-shaped string. The
	// string itself is in no error text; the location is.
	ErrCredential = errors.New("replay: cassette holds a credential-shaped value")
	// ErrCassette: the file does not read or decode as a cassette.
	ErrCassette = errors.New("replay: invalid cassette")
	// ErrUnmatched: a request matched no unused interaction.
	ErrUnmatched = errors.New("replay: no recorded interaction for request")
)

// Cassette is one recorded test: the interactions in the order they
// happened.
type Cassette struct {
	Provider string `json:"provider"`
	Name     string `json:"name"`
	// RecordedAt is when the sandbox run recorded it, or when it was
	// written by hand (Recorded false).
	RecordedAt time.Time `json:"recorded_at"`
	// Recorded is true only for a cassette the recorder wrote from a live
	// sandbox run.
	Recorded bool `json:"recorded"`
	// Source names where the shapes come from: the sandbox run, or the SDK
	// or API document a hand-written cassette follows.
	Source       string        `json:"source,omitempty"`
	Interactions []Interaction `json:"interactions"`
}

// Interaction is one request and its response.
type Interaction struct {
	Request  Request  `json:"request"`
	Response Response `json:"response"`
}

// Request is what the replayer matches on: method, the escaped path, the
// normalised query and the normalised, scrubbed body. An empty Query or an
// empty Body and BodySHA256 match any value (hand-written cassettes); the
// recorder always writes all three.
type Request struct {
	Method string `json:"method"`
	Path   string `json:"path"`
	Query  string `json:"query,omitempty"`
	// Body is the scrubbed, normalised request body, kept for the reader.
	Body string `json:"body,omitempty"`
	// BodySHA256 is the hex SHA-256 of Body; when set it is what matches.
	BodySHA256 string `json:"body_sha256,omitempty"`
	// Headers are the request headers kept for the reader (Content-Type,
	// X-Amz-Target); never Authorization or a SigV4 header.
	Headers map[string]string `json:"headers,omitempty"`
}

// Response is the replayed response. Cut makes the body end early with
// io.ErrUnexpectedEOF after Body, as a connection dropped mid-body would.
type Response struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body"`
	Cut     bool              `json:"cut,omitempty"`
}

// Load reads one cassette and runs Check on it. Any finding fails the load
// with ErrCredential, so a test that loads such a cassette fails.
func Load(path string) (*Cassette, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: opening %s: %v", ErrCassette, path, err)
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, MaxCassetteBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: reading %s: %v", ErrCassette, path, err)
	}
	if len(raw) > MaxCassetteBytes {
		return nil, fmt.Errorf("%w: %s is over %d bytes", ErrCassette, path, MaxCassetteBytes)
	}
	return Parse(raw, path)
}

// Parse decodes a cassette from raw and runs Check on it; name labels
// errors.
func Parse(raw []byte, name string) (*Cassette, error) {
	var c Cassette
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("%w: decoding %s: %v", ErrCassette, name, err)
	}
	if len(c.Interactions) == 0 {
		return nil, fmt.Errorf("%w: %s has no interactions", ErrCassette, name)
	}
	for i, it := range c.Interactions {
		if it.Request.Method == "" || it.Request.Path == "" {
			return nil, fmt.Errorf("%w: %s: interaction %d has no method or path", ErrCassette, name, i)
		}
		if it.Response.Status < 100 || it.Response.Status > 599 {
			return nil, fmt.Errorf("%w: %s: interaction %d has status %d", ErrCassette, name, i, it.Response.Status)
		}
	}
	if findings := Check(&c); len(findings) > 0 {
		return nil, fmt.Errorf("%w: %s: %s", ErrCredential, name, findings[0])
	}
	return &c, nil
}

// Save writes c to path after Check: a cassette with a finding is never
// written, so the recorder cannot produce one the loader would refuse.
func Save(c *Cassette, path string) error {
	if findings := Check(c); len(findings) > 0 {
		return fmt.Errorf("%w: refusing to write %s: %s", ErrCredential, path, findings[0])
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	// A reader compares cassettes with the provider's documents; <, > and &
	// stay as they are.
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(c); err != nil {
		return fmt.Errorf("replay: encoding %s: %w", path, err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("replay: writing %s: %w", path, err)
	}
	return nil
}

// Stale reports whether c is older than StaleAfter at now.
func (c *Cassette) Stale(now time.Time) bool {
	return !c.RecordedAt.IsZero() && now.Sub(c.RecordedAt) > StaleAfter
}
