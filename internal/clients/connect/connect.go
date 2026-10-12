// Package connect implements clients.ConnectClient against the Kafka Connect REST API.
package connect

import (
	"bytes"
	"context"
	"encoding/json"
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
func (c *Client) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	var raw struct {
		Name      string `json:"name"`
		Type      string `json:"type"`
		Connector struct {
			State    string `json:"state"`
			WorkerID string `json:"worker_id"`
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

// hiddenValue is what Connect prints in place of a secret it masks. Writing it
// back would overwrite the real secret, so UpdateConfig refuses to send it.
const hiddenValue = "[hidden]"

// UpdateConfig sets one key in a connector's config. Connect's PUT replaces the
// whole config, so it reads the current config, changes only key, and writes
// the full map back. Connect restarts the connector's tasks on a config change.
// It is not on clients.ConnectClient yet (contract change request); the
// connect_update_connector_config action asserts for it.
func (c *Client) UpdateConfig(ctx context.Context, name string, key, value string) error {
	if key == "" {
		return fmt.Errorf("connect: update config %s: empty key", name)
	}
	cfg, err := c.Config(ctx, name)
	if err != nil {
		return fmt.Errorf("connect: update config %s: read current config: %w", name, err)
	}
	if cfg == nil {
		cfg = map[string]string{}
	}
	for k, v := range cfg {
		if v == hiddenValue {
			return fmt.Errorf("connect: update config %s: key %q is masked by Connect; refusing to write the config back", name, k)
		}
	}
	cfg[key] = value
	path := "/connectors/" + url.PathEscape(name) + "/config"
	return c.doBody(ctx, http.MethodPut, path, cfg, nil)
}

// do performs a request without a body and decodes the JSON response into out;
// nil out skips decoding.
func (c *Client) do(ctx context.Context, method, path string, out any) error {
	return c.doBody(ctx, method, path, nil, out)
}

// doBody is do with an optional JSON request body (nil sends none).
func (c *Client) doBody(ctx context.Context, method, path string, in, out any) error {
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
		return fmt.Errorf("connect: %s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(out); err != nil {
		return fmt.Errorf("connect: %s %s: decode: %w", method, path, err)
	}
	return nil
}
