// Package connect implements clients.ConnectClient against the Kafka Connect REST API.
package connect

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/netsec"
)

// Client is a thin Kafka Connect REST client.
type Client struct {
	baseURL string
	hc      *http.Client
}

var _ clients.ConnectClient = (*Client)(nil)

// New returns a Connect client for baseURL (e.g. http://localhost:8083).
func New(baseURL string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), hc: httpClient}
}

// NewSecure returns a Connect client for a real cluster: TLS when baseURL is
// https (custom CA and client certificate from sec.TLS) and HTTP basic auth
// from sec.Username/sec.Password. It refuses basic auth over plain http.
func NewSecure(baseURL string, sec netsec.HTTP) (*Client, error) {
	hc, err := sec.Client(baseURL, netsec.DefaultHTTPTimeout)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return New(baseURL, hc), nil
}

// ListConnectors returns the names of all connectors.
func (c *Client) ListConnectors(ctx context.Context) ([]string, error) {
	var out []string
	if err := c.do(ctx, http.MethodGet, "/connectors", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Status returns the flat connector status.
//
// A status body larger than 4 MiB (a connector with many tasks failing on
// huge traces) is read up to the cap: Status then returns the part it read,
// with TruncatedTrace in the trace that was cut, together with an error
// wrapping ErrResponseTooLarge. Callers that can use a partial status check
// for that error; the rest treat it like any other error.
func (c *Client) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	path := "/connectors/" + url.PathEscape(name) + "/status"
	var cs clients.ConnectorStatus
	err := c.doRaw(ctx, http.MethodGet, path, nil, func(r io.Reader) error {
		var derr error
		cs, derr = decodeStatus(r)
		return derr
	})
	if err != nil && !errors.Is(err, ErrResponseTooLarge) {
		return clients.ConnectorStatus{}, err
	}
	return cs, err
}

// TruncatedTrace replaces the rest of a task trace that did not fit in the
// response cap.
const TruncatedTrace = "\n[trace cut: Connect status response exceeds 4 MiB]"

// decodeStatus streams the status JSON token by token, so a body cut at the
// cap still yields the connector state and every task read before the cut.
func decodeStatus(r io.Reader) (clients.ConnectorStatus, error) {
	var cs clients.ConnectorStatus
	dec := json.NewDecoder(r)
	if err := expectDelim(dec, '{'); err != nil {
		return cs, err
	}
	for dec.More() {
		key, err := objectKey(dec)
		if err != nil {
			return cs, err
		}
		switch key {
		case "name":
			err = dec.Decode(&cs.Name)
		case "type":
			err = dec.Decode(&cs.Type)
		case "connector":
			var conn struct {
				State    string `json:"state"`
				WorkerID string `json:"worker_id"`
			}
			err = dec.Decode(&conn)
			cs.State, cs.WorkerID = conn.State, conn.WorkerID
		case "tasks":
			err = decodeTasks(dec, &cs)
		default:
			var skip json.RawMessage
			err = dec.Decode(&skip)
		}
		if err != nil {
			return cs, err
		}
	}
	return cs, nil
}

func decodeTasks(dec *json.Decoder, cs *clients.ConnectorStatus) error {
	if err := expectDelim(dec, '['); err != nil {
		return err
	}
	for dec.More() {
		if err := expectDelim(dec, '{'); err != nil {
			return err
		}
		var t clients.TaskStatus
		for dec.More() {
			key, err := objectKey(dec)
			if err == nil {
				switch key {
				case "id":
					err = dec.Decode(&t.ID)
				case "state":
					err = dec.Decode(&t.State)
				case "worker_id":
					err = dec.Decode(&t.WorkerID)
				case "trace":
					err = dec.Decode(&t.Trace)
					if errors.Is(err, ErrResponseTooLarge) {
						// The exception's first line is the useful part.
						t.Trace = firstLineOfPartialString(dec.Buffered())
					}
				default:
					var skip json.RawMessage
					err = dec.Decode(&skip)
				}
			}
			if err != nil {
				if errors.Is(err, ErrResponseTooLarge) && t.State != "" {
					t.Trace += TruncatedTrace
					cs.Tasks = append(cs.Tasks, t)
				}
				return err
			}
		}
		if err := expectDelim(dec, '}'); err != nil {
			return err
		}
		cs.Tasks = append(cs.Tasks, t)
	}
	return expectDelim(dec, ']')
}

// firstLineOfPartialString decodes the first line of a JSON string value
// whose end was cut off: r holds the unread rest of the body, starting at the
// value (json.Decoder.Buffered after a failed Decode). It returns "" when
// that is not a string.
func firstLineOfPartialString(r io.Reader) string {
	buf, _ := io.ReadAll(io.LimitReader(r, 16<<10))
	buf = bytes.TrimLeft(buf, " \t\r\n:")
	if len(buf) == 0 || buf[0] != '"' {
		return ""
	}
	body := buf[1:]
	if i := bytes.Index(body, []byte(`\n`)); i >= 0 {
		body = body[:i]
	} else if i := bytes.IndexByte(body, '"'); i >= 0 {
		body = body[:i]
	}
	if len(body) > 2048 {
		body = body[:2048]
	}
	// A cut may split an escape (é or \"): drop bytes until it decodes.
	for n := len(body); n >= 0 && n > len(body)-8; n-- {
		var s string
		if json.Unmarshal(append(append([]byte{'"'}, body[:n]...), '"'), &s) == nil {
			return s
		}
	}
	return ""
}

func expectDelim(dec *json.Decoder, want json.Delim) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if d, ok := tok.(json.Delim); !ok || d != want {
		return fmt.Errorf("unexpected token %v, want %v", tok, want)
	}
	return nil
}

func objectKey(dec *json.Decoder) (string, error) {
	tok, err := dec.Token()
	if err != nil {
		return "", err
	}
	key, ok := tok.(string)
	if !ok {
		return "", fmt.Errorf("unexpected token %v, want an object key", tok)
	}
	return key, nil
}

// Config returns the connector's full config.
func (c *Client) Config(ctx context.Context, name string) (map[string]string, error) {
	var out map[string]string
	path := "/connectors/" + url.PathEscape(name) + "/config"
	if err := c.do(ctx, http.MethodGet, path, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// RestartTask restarts one task of a connector.
func (c *Client) RestartTask(ctx context.Context, name string, task int) error {
	path := fmt.Sprintf("/connectors/%s/tasks/%d/restart", url.PathEscape(name), task)
	return c.do(ctx, http.MethodPost, path, nil)
}

// RestartConnector restarts only the failed tasks of a connector.
func (c *Client) RestartConnector(ctx context.Context, name string) error {
	path := "/connectors/" + url.PathEscape(name) + "/restart?includeTasks=true&onlyFailed=true"
	return c.do(ctx, http.MethodPost, path, nil)
}

// Resume resumes a paused or stopped connector (PUT /connectors/{name}/resume).
func (c *Client) Resume(ctx context.Context, name string) error {
	path := "/connectors/" + url.PathEscape(name) + "/resume"
	return c.do(ctx, http.MethodPut, path, nil)
}

// UpdateConfig sets one key in a connector's config with
// PATCH /connectors/{name}/config (KIP-477, Kafka Connect 3.5+). PATCH merges
// the one key on the server, so no other key (the config can hold passwords in
// plaintext) passes through this process, there is no read-modify-write window,
// and a deleted connector answers 404 instead of being re-created by a PUT.
// There is deliberately no PUT fallback: an older Connect answers 405, which
// is reported as an error. Connect restarts the connector's tasks on a config
// change. It is not on clients.ConnectClient yet (contract change request); the
// connect_update_connector_config action asserts for it.
func (c *Client) UpdateConfig(ctx context.Context, name string, key, value string) error {
	if key == "" {
		return fmt.Errorf("connect: update config %s: empty key", name)
	}
	path := "/connectors/" + url.PathEscape(name) + "/config"
	err := c.doBody(ctx, http.MethodPatch, path, map[string]string{key: value}, nil)
	var se *statusError
	if errors.As(err, &se) && se.Code == http.StatusMethodNotAllowed {
		return fmt.Errorf("connect: update config %s: Connect does not support PATCH config; needs Kafka Connect 3.5+: %w", name, err)
	}
	return err
}

// statusError is a non-2xx, non-404 answer from Connect.
type statusError struct {
	Method, Path string
	Code         int
	Body         string
}

func (e *statusError) Error() string {
	return fmt.Sprintf("connect: %s %s: status %d: %s", e.Method, e.Path, e.Code, e.Body)
}

// do performs a request without a body and decodes the JSON response into out;
// nil out skips decoding.
func (c *Client) do(ctx context.Context, method, path string, out any) error {
	return c.doBody(ctx, method, path, nil, out)
}

// doBody is do with an optional JSON request body (nil sends none).
func (c *Client) doBody(ctx context.Context, method, path string, in, out any) error {
	var handle func(io.Reader) error
	if out != nil {
		handle = func(r io.Reader) error { return json.NewDecoder(r).Decode(out) }
	}
	return c.doRaw(ctx, method, path, in, handle)
}

// maxResponseBytes caps every response body read (AGENTS.md rule 7).
const maxResponseBytes = 4 << 20

// maxErrorSnippet caps the error-body text kept in a statusError: a proxy's
// HTML error page is noise to the agent and to logs.
const maxErrorSnippet = 256

// ErrResponseTooLarge is wrapped when a response body exceeds 4 MiB.
var ErrResponseTooLarge = errors.New("response larger than 4 MiB")

// capReader reads at most n bytes and then fails with ErrResponseTooLarge
// rather than io.EOF, so a decoder cut at the cap reports why, not
// "unexpected EOF".
type capReader struct {
	r io.Reader
	n int64
}

func (c *capReader) Read(p []byte) (int, error) {
	if c.n <= 0 {
		// One more byte tells a body of exactly n bytes from a longer one.
		var one [1]byte
		k, err := c.r.Read(one[:])
		if k > 0 {
			return 0, ErrResponseTooLarge
		}
		return 0, err
	}
	if int64(len(p)) > c.n {
		p = p[:c.n]
	}
	k, err := c.r.Read(p)
	c.n -= int64(k)
	return k, err
}

// doRaw sends the request and hands a 2xx body, capped at maxResponseBytes,
// to handle (nil skips reading it).
func (c *Client) doRaw(ctx context.Context, method, path string, in any, handle func(io.Reader) error) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("connect: %s %s: encode body: %w", method, path, err)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return fmt.Errorf("connect: %s %s: %w", method, path, err)
	}
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("connect: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("connect: %s %s: %w", method, path, clients.ErrNotFound)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorSnippet))
		return &statusError{Method: method, Path: path, Code: resp.StatusCode, Body: snippet(b)}
	}
	if handle == nil {
		return nil
	}
	if err := handle(&capReader{r: resp.Body, n: maxResponseBytes}); err != nil {
		return fmt.Errorf("connect: %s %s: decode: %w", method, path, err)
	}
	return nil
}

// snippet makes an error body printable: valid UTF-8, trimmed, and marked
// when it was cut at maxErrorSnippet bytes.
// A rune split by the cut is invalid UTF-8 and dropped with the rest.
func snippet(b []byte) string {
	s := strings.TrimSpace(strings.ToValidUTF8(string(b), ""))
	if len(b) >= maxErrorSnippet {
		s += "…"
	}
	return s
}
