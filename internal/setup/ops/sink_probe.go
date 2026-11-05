package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/tools"
)

const (
	// serverPropertiesPath is U11a's fixed runner-side path for the generated
	// Debezium Server configuration.
	serverPropertiesPath = "debezium/config/application.properties"
	// maxServerPropertiesBytes matches U11a's 32 KiB properties-file bound.
	maxServerPropertiesBytes = 32 << 10
	serverSinkTypeKey        = "debezium.sink.type"
	serverHTTPSinkURLKey     = "debezium.sink.http.url"
)

// ErrSinkProbeConfig means the generated Debezium Server HTTP sink
// configuration is missing, malformed, or outside this probe's supported
// scope. Its error text never includes a property value.
var ErrSinkProbeConfig = errors.New("ops: invalid sink_probe configuration")

// ErrSinkProbeFailed means the configured HTTP sink did not answer the
// read-only HEAD probe successfully.
var ErrSinkProbeFailed = errors.New("ops: sink_probe failed")

// sinkProbeOp checks the one sink with a safe non-writing probe available in
// the U11a profile: HTTP. It reads the runner's generated properties file,
// then sends an empty HEAD request to the configured destination.
type sinkProbeOp struct{}

type sinkProbeParams struct {
	// Target is the exact address of the registered host target that must match
	// the destination in the generated server properties.
	Target string `json:"target"`
}

type sinkProbeData struct {
	Sink       string `json:"sink"`
	Target     string `json:"target"`
	Method     string `json:"method"`
	Status     int    `json:"status,omitempty"`
	Reachable  bool   `json:"reachable"`
	Configured bool   `json:"configured"`
}

func (sinkProbeOp) Name() string   { return "sink_probe" }
func (sinkProbeOp) Target() string { return "" }
func (sinkProbeOp) Tier() string   { return protocol.TierT0 }
func (sinkProbeOp) Layer() string  { return "environment" }
func (sinkProbeOp) Describe() string {
	return "check the configured Debezium Server HTTP sink with an empty HEAD request"
}

func (sinkProbeOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"target"},
		"properties": map[string]any{
			"target": map[string]any{"type": "string", "minLength": 1, "maxLength": 512},
		},
	}
}

func (sinkProbeOp) Validate(params json.RawMessage, env Env) error {
	_, _, err := decodeSinkProbe(params, env)
	return err
}

func (sinkProbeOp) checkTarget(params json.RawMessage, env Env) error {
	_, _, err := decodeSinkProbe(params, env)
	return err
}

func (sinkProbeOp) ParamTarget(params json.RawMessage, env Env) string {
	_, target, err := decodeSinkProbe(params, env)
	if err != nil {
		return ""
	}
	return target.Kind
}

func (sinkProbeOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	_, target, err := decodeSinkProbe(params, env)
	if err != nil {
		return Result{}, err
	}
	started := opNow(env)
	base := sinkProbeData{Sink: "http", Target: clip(target.Address), Method: http.MethodHead}
	properties, err := readServerProperties(env)
	if err != nil {
		return sinkProbeResult("sink_probe http: server configuration is unavailable", base, started, serverPropertiesPath, env), err
	}
	if properties[serverSinkTypeKey] != "http" {
		return sinkProbeResult("sink_probe http: configured sink is unsupported", base, started, serverPropertiesPath, env),
			fmt.Errorf("%w: only the HTTP sink is supported", ErrSinkProbeConfig)
	}
	rawURL := properties[serverHTTPSinkURLKey]
	safeURL, address, err := configuredSinkURL(rawURL, target)
	if err != nil {
		return sinkProbeResult("sink_probe http: configured destination is invalid", base, started, serverPropertiesPath, env), err
	}
	if env.HTTP == nil {
		return sinkProbeResult("sink_probe http: no HTTP client is configured", base, started, address, env),
			fmt.Errorf("%w: no HTTP client is configured", ErrSinkProbeFailed)
	}

	// A fresh 10-second context and the cloned client cap this request even
	// when a caller supplies an HTTP client without a timeout. HEAD carries no
	// event body, and redirects are refused so the registered-target check
	// cannot be bypassed by the endpoint.
	probeCtx, cancel := context.WithTimeout(ctx, defaultOpTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodHead, safeURL.String(), nil)
	if err != nil {
		return sinkProbeResult("sink_probe http: configured destination is invalid", base, started, address, env),
			fmt.Errorf("%w: could not build the HEAD request", ErrSinkProbeConfig)
	}
	req.Header.Set("User-Agent", "piperoom-setup/"+CatalogueVersion)
	client := *env.HTTP
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return errRedirectRefused }
	if client.Timeout == 0 || client.Timeout > defaultOpTimeout {
		client.Timeout = defaultOpTimeout
	}
	resp, err := client.Do(req)
	if err != nil {
		data := base
		data.Configured = true
		result := sinkProbeResult("sink_probe http: HEAD request failed", data, started, address, env)
		switch {
		case errors.Is(err, context.DeadlineExceeded), errors.Is(probeCtx.Err(), context.DeadlineExceeded):
			return result, fmt.Errorf("%w: %w", ErrSinkProbeFailed, context.DeadlineExceeded)
		case errors.Is(err, context.Canceled), errors.Is(probeCtx.Err(), context.Canceled):
			return result, fmt.Errorf("%w: %w", ErrSinkProbeFailed, context.Canceled)
		default:
			// Do not include *url.Error.Error(): it embeds the full URL, whose
			// query may contain credentials.
			return result, fmt.Errorf("%w: HTTP request failed", ErrSinkProbeFailed)
		}
	}
	defer resp.Body.Close()

	data := base
	data.Status = resp.StatusCode
	data.Reachable = true
	data.Configured = resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
	result := sinkProbeResult(fmt.Sprintf("sink_probe http: HEAD returned HTTP %d", resp.StatusCode), data, started, address, env)
	if !data.Configured {
		return result, fmt.Errorf("%w: endpoint returned HTTP %d", ErrSinkProbeFailed, resp.StatusCode)
	}
	return result, nil
}

func decodeSinkProbe(params json.RawMessage, env Env) (sinkProbeParams, events.SetupTarget, error) {
	var p sinkProbeParams
	if err := decodeParams(params, &p); err != nil {
		return p, events.SetupTarget{}, err
	}
	p.Target = strings.TrimSpace(p.Target)
	if p.Target == "" || len(p.Target) > 512 {
		return p, events.SetupTarget{}, fmt.Errorf("%w: target address is required and at most 512 bytes", ErrParams)
	}
	for _, target := range env.Targets {
		if strings.EqualFold(target.Kind, "host") && target.Address == p.Target {
			return p, target, nil
		}
	}
	return p, events.SetupTarget{}, fmt.Errorf("%w: target address is not a registered host target", ErrTarget)
}

// readServerProperties opens U11a's generated properties file through the
// project Confiner, bounds it and parses only in-memory values. Values are
// never copied to Result, summaries, or errors.
func readServerProperties(env Env) (map[string]string, error) {
	if env.Confine == nil {
		return nil, fmt.Errorf("%w: project file access is unavailable", ErrSinkProbeConfig)
	}
	clean, denied, err := env.Confine.prepare(serverPropertiesPath)
	if err != nil {
		return nil, fmt.Errorf("%w: server properties file is not safe to open", ErrSinkProbeConfig)
	}
	f, err := env.Confine.root.Open(clean)
	if err != nil {
		return nil, fmt.Errorf("%w: server properties file cannot be opened", ErrSinkProbeConfig)
	}
	defer f.Close()
	if err := env.Confine.verifyHandle(f, denied); err != nil {
		return nil, fmt.Errorf("%w: server properties file is not a regular file", ErrSinkProbeConfig)
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxServerPropertiesBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: server properties file cannot be read", ErrSinkProbeConfig)
	}
	if len(raw) > maxServerPropertiesBytes || !utf8.Valid(raw) {
		return nil, fmt.Errorf("%w: server properties file is too large or malformed", ErrSinkProbeConfig)
	}
	properties, err := parseServerProperties(raw)
	if err != nil {
		return nil, err
	}
	return properties, nil
}

// parseServerProperties accepts the simple key=value format emitted by
// server_properties_write. Refusing other forms avoids guessing at Java
// property escaping and prevents duplicate keys from changing sink meaning.
func parseServerProperties(raw []byte) (map[string]string, error) {
	properties := make(map[string]string)
	for _, rawLine := range strings.Split(string(raw), "\n") {
		line := strings.TrimSpace(strings.TrimSuffix(rawLine, "\r"))
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" || strings.ContainsAny(key, " \t") {
			return nil, fmt.Errorf("%w: server properties file is malformed", ErrSinkProbeConfig)
		}
		if _, duplicate := properties[key]; duplicate {
			return nil, fmt.Errorf("%w: duplicate property in server configuration", ErrSinkProbeConfig)
		}
		properties[key] = strings.TrimSpace(value)
	}
	if len(properties) == 0 {
		return nil, fmt.Errorf("%w: server properties file is empty", ErrSinkProbeConfig)
	}
	return properties, nil
}

// configuredSinkURL checks the configured URL against one registered target,
// then rebuilds it with that target's host and port rather than trusting the
// file's host spelling. The guarded HTTP transport checks DNS-resolved
// addresses at dial time.
func configuredSinkURL(raw string, target events.SetupTarget) (*url.URL, string, error) {
	if raw == "" || len(raw) > maxURLLength {
		return nil, "", fmt.Errorf("%w: HTTP sink URL is missing or too long", ErrSinkProbeConfig)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, "", fmt.Errorf("%w: HTTP sink URL is malformed", ErrSinkProbeConfig)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" {
		return nil, "", fmt.Errorf("%w: HTTP sink URL must be http(s), without user info or fragment", ErrSinkProbeConfig)
	}
	host, port, ok := matchSinkTarget(target, u)
	if !ok {
		return nil, "", fmt.Errorf("%w: configured URL does not match the registered host target", ErrTarget)
	}
	if port == "" {
		port = schemeDefaultPort(u.Scheme)
	}
	if ip, parseErr := netip.ParseAddr(host); parseErr == nil && dialGuard.CheckAddr(ip) != nil {
		return nil, "", fmt.Errorf("%w: configured destination address is not allowed", ErrTarget)
	}
	hostPort := net.JoinHostPort(host, port)
	safe := &url.URL{
		Scheme:   u.Scheme,
		Host:     hostPort,
		Path:     u.Path,
		RawPath:  u.RawPath,
		RawQuery: u.RawQuery,
	}
	return safe, hostPort, nil
}

func matchSinkTarget(target events.SetupTarget, u *url.URL) (string, string, bool) {
	reqHost := strings.ToLower(u.Hostname())
	reqPort := u.Port()
	if reqPort == "" {
		reqPort = schemeDefaultPort(u.Scheme)
	}
	host, port := splitTargetAddress(target.Address)
	if host == "" || !strings.EqualFold(host, reqHost) {
		return "", "", false
	}
	if port == "" {
		port = schemeDefaultPort(u.Scheme)
	}
	if port != reqPort {
		return "", "", false
	}
	return host, port, true
}

func sinkProbeResult(summary string, data sinkProbeData, started time.Time, target string, env Env) Result {
	summary, truncated := tools.Truncate(summary)
	return Result{
		Summary:   summary,
		Data:      data,
		Truncated: truncated,
		Receipt:   events.SetupReceipt{Kind: sinkProbeReceiptKind(target), Target: target, At: started, TookMS: opNow(env).Sub(started).Milliseconds()},
	}
}

func sinkProbeReceiptKind(target string) string {
	if target == serverPropertiesPath {
		return "file"
	}
	return "http"
}

func opNow(env Env) time.Time {
	if env.Now != nil {
		return env.Now()
	}
	return time.Now()
}
