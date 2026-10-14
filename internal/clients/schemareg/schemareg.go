// Package schemareg implements clients.SchemaRegistryClient against the
// Confluent Schema Registry REST API.
package schemareg

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
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/clients/netsec"
)

const mediaType = "application/vnd.schemaregistry.v1+json"

// Error is the registry's structured error body {"error_code":..,"message":..}.
type Error struct {
	Code    int    `json:"error_code"`
	Message string `json:"message"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("schema registry error %d: %s", e.Code, e.Message)
}

// Client is a thin Schema Registry REST client.
type Client struct {
	baseURL string
	hc      *http.Client
}

var _ clients.SchemaRegistryClient = (*Client)(nil)

// New returns a Schema Registry client for baseURL (e.g. http://localhost:8081).
func New(baseURL string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), hc: httpClient}
}

// NewSecure returns a Schema Registry client for a real cluster: TLS when baseURL is
// https (custom CA and client certificate from sec.TLS) and HTTP basic auth
// from sec.Username/sec.Password. It refuses basic auth over plain http.
func NewSecure(baseURL string, sec netsec.HTTP) (*Client, error) {
	hc, err := sec.Client(baseURL, netsec.DefaultHTTPTimeout)
	if err != nil {
		return nil, fmt.Errorf("schemareg: %w", err)
	}
	return New(baseURL, hc), nil
}

// Subjects lists all registered subjects.
func (c *Client) Subjects(ctx context.Context) ([]string, error) {
	var out []string
	if err := c.do(ctx, http.MethodGet, "/subjects", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Versions lists the version numbers of a subject.
func (c *Client) Versions(ctx context.Context, subject string) ([]int, error) {
	var out []int
	path := "/subjects/" + url.PathEscape(subject) + "/versions"
	if err := c.do(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Schema returns one version of a subject; version -1 means latest.
func (c *Client) Schema(ctx context.Context, subject string, version int) (clients.SchemaVersion, error) {
	sv, _, err := c.SchemaTyped(ctx, subject, version)
	return sv, err
}

// SchemaTyped is Schema plus the version's schemaType: "AVRO", "JSON" or
// "PROTOBUF" (the registry omits it for Avro; "AVRO" is returned then). It is
// not on clients.SchemaRegistryClient yet (contract change request);
// schema_registry_diff asserts for it.
func (c *Client) SchemaTyped(ctx context.Context, subject string, version int) (clients.SchemaVersion, string, error) {
	v := "latest"
	if version >= 0 {
		v = fmt.Sprintf("%d", version)
	}
	var raw struct {
		Subject    string `json:"subject"`
		Version    int    `json:"version"`
		ID         int    `json:"id"`
		Schema     string `json:"schema"`
		SchemaType string `json:"schemaType"`
	}
	path := "/subjects/" + url.PathEscape(subject) + "/versions/" + v
	if err := c.do(ctx, http.MethodGet, path, nil, &raw); err != nil {
		return clients.SchemaVersion{}, "", err
	}
	typ := strings.ToUpper(raw.SchemaType)
	if typ == "" {
		typ = "AVRO"
	}
	return clients.SchemaVersion{Subject: raw.Subject, Version: raw.Version, ID: raw.ID, Schema: raw.Schema}, typ, nil
}

// SchemaByID returns the schema string for a global schema id (for Avro decoding).
func (c *Client) SchemaByID(ctx context.Context, id int) (string, error) {
	var raw struct {
		Schema string `json:"schema"`
	}
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/schemas/ids/%d", id), nil, &raw); err != nil {
		return "", err
	}
	return raw.Schema, nil
}

// Compatibility returns the subject compatibility level, falling back to the
// global level when the subject has none (registry error code 40408).
func (c *Client) Compatibility(ctx context.Context, subject string) (level string, subjectLevel bool, err error) {
	var cfg struct {
		CompatibilityLevel string `json:"compatibilityLevel"`
	}
	if err = c.do(ctx, http.MethodGet, "/config/"+url.PathEscape(subject), nil, &cfg); err == nil {
		return cfg.CompatibilityLevel, true, nil
	}
	var serr *Error
	if !errors.As(err, &serr) || serr.Code != 40408 {
		return "", false, err
	}
	if err := c.do(ctx, http.MethodGet, "/config", nil, &cfg); err != nil {
		return "", false, err
	}
	return cfg.CompatibilityLevel, false, nil
}

// SetCompatibility sets a subject's compatibility level.
func (c *Client) SetCompatibility(ctx context.Context, subject, level string) error {
	body, err := json.Marshal(map[string]string{"compatibility": level})
	if err != nil {
		return fmt.Errorf("schemareg: set compatibility: %w", err)
	}
	path := "/config/" + url.PathEscape(subject)
	return c.do(ctx, http.MethodPut, path, bytes.NewReader(body), nil)
}

// Register registers a schema under a subject and returns its id.
func (c *Client) Register(ctx context.Context, subject, schema string) (int, error) {
	body, err := json.Marshal(map[string]string{"schema": schema})
	if err != nil {
		return 0, fmt.Errorf("schemareg: register: %w", err)
	}
	var out struct {
		ID int `json:"id"`
	}
	path := "/subjects/" + url.PathEscape(subject) + "/versions"
	if err := c.do(ctx, http.MethodPost, path, bytes.NewReader(body), &out); err != nil {
		return 0, err
	}
	return out.ID, nil
}

// do performs a request and decodes JSON into out; nil out skips decoding.
// Non-2xx responses are parsed into a typed *Error; 404 additionally wraps ErrNotFound.
func (c *Client) do(ctx context.Context, method, path string, body io.Reader, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return fmt.Errorf("schemareg: %s %s: %w", method, path, err)
	}
	req.Header.Set("Accept", mediaType)
	if body != nil {
		req.Header.Set("Content-Type", mediaType)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("schemareg: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if out == nil {
			return nil
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(out); err != nil {
			return fmt.Errorf("schemareg: %s %s: decode: %w", method, path, err)
		}
		return nil
	}

	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode == http.StatusNotFound {
		var serr Error
		if err := json.Unmarshal(b, &serr); err == nil && serr.Code != 0 {
			serr.Message = snippet(serr.Message)
			return fmt.Errorf("schemareg: %s %s: %w: %w", method, path, clients.ErrNotFound, &serr)
		}
		return fmt.Errorf("schemareg: %s %s: %w", method, path, clients.ErrNotFound)
	}
	var serr Error
	if err := json.Unmarshal(b, &serr); err != nil || serr.Code == 0 {
		return fmt.Errorf("schemareg: %s %s: status %d: %s", method, path, resp.StatusCode, snippet(string(b)))
	}
	serr.Message = snippet(serr.Message)
	return fmt.Errorf("schemareg: %s %s: %w", method, path, &serr)
}

// maxErrorSnippet caps the error-body text kept in an error: a proxy's HTML
// error page is noise to the agent and to logs.
const maxErrorSnippet = 256

// snippet trims s and cuts it to maxErrorSnippet bytes on a rune boundary.
func snippet(s string) string {
	s = strings.TrimSpace(strings.ToValidUTF8(s, ""))
	if len(s) <= maxErrorSnippet {
		return s
	}
	n := maxErrorSnippet
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "…"
}
