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
	"piperoom/internal/tools"
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
func (c *Client) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	var raw struct {
		Name      string `json:"name"`
		Type      string `json:"type"`
		Connector struct {
			State    string `json:"state"`
			WorkerID string `json:"worker_id"`
			Trace    string `json:"trace"`
		} `json:"connector"`
		Tasks []struct {
			ID       int    `json:"id"`
			State    string `json:"state"`
			WorkerID string `json:"worker_id"`
			Trace    string `json:"trace"`
		} `json:"tasks"`
	}
	path := "/connectors/" + url.PathEscape(name) + "/status"
	if err := c.do(ctx, http.MethodGet, path, &raw); err != nil {
		return clients.ConnectorStatus{}, err
	}
	cs := clients.ConnectorStatus{
		Name:     raw.Name,
		Type:     raw.Type,
		State:    raw.Connector.State,
		WorkerID: raw.Connector.WorkerID,
		Trace:    raw.Connector.Trace,
	}
	for _, t := range raw.Tasks {
		cs.Tasks = append(cs.Tasks, clients.TaskStatus{
			ID: t.ID, State: t.State, WorkerID: t.WorkerID, Trace: t.Trace,
		})
	}
	return cs, nil
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

// doBody is do with an optional JSON request body (nil sends none). It records
// one provenance call per request, on success and on error. The call holds the
// method and path only: a request body can carry a config value.
func (c *Client) doBody(ctx context.Context, method, path string, in, out any) error {
	// A refused path is never sent, so it gets no receipt (#446).
	if err := netsec.CheckPath(path); err != nil {
		return fmt.Errorf("connect: %s: %w", method, err)
	}
	start := time.Now()
	err := c.roundTrip(ctx, method, path, in, out)
	tools.Record(ctx, tools.Call{
		Kind:   tools.CallHTTP,
		Target: c.baseURL,
		Text:   method + " " + path,
		At:     start,
		TookMS: time.Since(start).Milliseconds(),
	})
	return err
}

func (c *Client) roundTrip(ctx context.Context, method, path string, in, out any) error {
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
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &statusError{Method: method, Path: path, Code: resp.StatusCode, Body: strings.TrimSpace(string(b))}
	}
	if out == nil {
		return nil
	}
	// Read one byte past the cap so an oversized answer is reported as
	// such, not as the JSON syntax error a cut body would give.
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("connect: %s %s: read: %w", method, path, err)
	}
	if len(b) > maxResponseBytes {
		return fmt.Errorf("connect: %s %s: %w", method, path, ErrResponseTooLarge)
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("connect: %s %s: decode: %w", method, path, err)
	}
	return nil
}

// maxResponseBytes caps every response body Connect sends (AGENTS.md rule 7).
const maxResponseBytes = 4 << 20

// ErrResponseTooLarge is wrapped when a response body is over the 4 MB cap.
var ErrResponseTooLarge = errors.New("response larger than the 4 MB cap")

// ValidateResult is Connect's verdict on a config. Errors maps a config key to
// its error messages; values are never kept, because they can be secrets.
type ValidateResult struct {
	ErrorCount int
	Errors     map[string][]string
}

// ValidateConfig asks Connect to validate cfg for the connector plugin class
// (PUT /connector-plugins/{class}/config/validate). The endpoint only
// validates; it creates nothing. A Connect without it answers 405 or 404.
// It is not on clients.ConnectReader; the actions package asserts for it.
func (c *Client) ValidateConfig(ctx context.Context, class string, cfg map[string]string) (ValidateResult, error) {
	if class == "" {
		return ValidateResult{}, fmt.Errorf("connect: validate config: empty connector.class")
	}
	var out struct {
		ErrorCount int `json:"error_count"`
		Configs    []struct {
			Value struct {
				Name   string   `json:"name"`
				Errors []string `json:"errors"`
			} `json:"value"`
		} `json:"configs"`
	}
	path := "/connector-plugins/" + url.PathEscape(class) + "/config/validate"
	if err := c.doBody(ctx, http.MethodPut, path, cfg, &out); err != nil {
		return ValidateResult{}, err
	}
	res := ValidateResult{ErrorCount: out.ErrorCount, Errors: map[string][]string{}}
	for _, e := range out.Configs {
		if len(e.Value.Errors) > 0 {
			res.Errors[e.Value.Name] = append(res.Errors[e.Value.Name], e.Value.Errors...)
		}
	}
	return res, nil
}
