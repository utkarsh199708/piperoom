package evals

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/wiring"
)

// NewEndpointRunner builds the agent runner cmd/server uses: the Endpoint of
// wiring.NewAgentEndpoint (provider client, triage model, per-room token
// budgets), and returns its Run. It reads credentials but calls no model.
func NewEndpointRunner(ctx context.Context, cfg config.Config, log *slog.Logger) (agent.Runner, error) {
	ep, err := wiring.NewAgentEndpoint(ctx, cfg, log, nil)
	if err != nil {
		return nil, fmt.Errorf("evals: %w", err)
	}
	return ep.Run, nil
}

// ModelTap is a reverse proxy on 127.0.0.1 between the agent's Anthropic
// client and the API. It forwards every request and response unchanged
// (streaming included) and reads only the request body's model, system,
// tools and the room line of the first user message. From those it records,
// per room, the models the agent really called (the triage model too) and a
// hash of the prompt the model really saw: system plus tool definitions, as
// sent. Headers, the API key among them, are passed on and never read.
//
// The SDK takes its base URL from ANTHROPIC_BASE_URL when the client is
// built, so the tap must be installed (InstallModelTap) before the Endpoint
// is. Only the anthropic provider can be tapped; Bedrock and Vertex sign
// their own requests to their own hosts.
type ModelTap struct {
	srv *http.Server
	url string

	mu     sync.Mutex
	rooms  map[string]*tapRoom
	hashes []string // every prompt hash of an attributed request, in first-seen order
}

type tapRoom struct {
	models, hashes []string
	requests       int
}

// TapRoom is what the tap saw for one room.
type TapRoom struct {
	Models       []string
	PromptHashes []string
	Requests     int
}

// maxTapBody caps the request body the tap reads into memory; a model
// request is a few MB at most.
const maxTapBody = 64 << 20

// NewModelTap starts a tap that forwards to upstream (scheme and host, with
// an optional path prefix).
func NewModelTap(upstream string) (*ModelTap, error) {
	return newModelTap(upstream, maxTapBody)
}

// newModelTap is NewModelTap with a body cap: a body over maxBody is
// forwarded whole and not observed.
func newModelTap(upstream string, maxBody int64) (*ModelTap, error) {
	up, err := url.Parse(upstream)
	if err != nil || up.Scheme == "" || up.Host == "" {
		return nil, fmt.Errorf("evals: model tap: bad upstream %q", upstream)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("evals: model tap: listen: %w", err)
	}
	t := &ModelTap{url: "http://" + ln.Addr().String(), rooms: map[string]*tapRoom{}}
	proxy := &httputil.ReverseProxy{
		// Stream server-sent events through as they come.
		FlushInterval: -1,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(up)
			if pr.Out.Body == nil {
				return
			}
			body := pr.Out.Body
			b, err := io.ReadAll(io.LimitReader(body, maxBody+1))
			if err != nil || int64(len(b)) > maxBody {
				// Forward what was read and the rest unchanged, with the
				// original length: a cut body would break the promise to
				// forward every request unchanged. A read error reaches
				// the transport and fails the request.
				pr.Out.Body = struct {
					io.Reader
					io.Closer
				}{io.MultiReader(bytes.NewReader(b), body), body}
				pr.Out.GetBody = nil
				return
			}
			_ = body.Close()
			pr.Out.Body = io.NopCloser(bytes.NewReader(b))
			pr.Out.ContentLength = int64(len(b))
			pr.Out.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil }
			t.observe(b)
		},
	}
	t.srv = &http.Server{Handler: proxy, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = t.srv.Serve(ln) }()
	return t, nil
}

// URL is the base URL the SDK must use.
func (t *ModelTap) URL() string { return t.url }

// Close stops the tap.
func (t *ModelTap) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return t.srv.Shutdown(ctx)
}

// roomLine finds the room id the agent writes into its first user message
// (agent.firstUserMessage: "Room: <id>" on its own line, after the alert's
// title and pipeline, which cannot hold a newline).
var roomLine = regexp.MustCompile(`(?m)^Room: (\S+)$`)

// tapRequest is the part of a Messages request the tap reads.
type tapRequest struct {
	Model    string          `json:"model"`
	System   json.RawMessage `json:"system"`
	Tools    json.RawMessage `json:"tools"`
	Messages []struct {
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
}

func (t *ModelTap) observe(body []byte) {
	var req tapRequest
	if json.Unmarshal(body, &req) != nil || len(req.Messages) == 0 {
		return
	}
	roomID := firstRoomID(req.Messages[0].Content)
	if roomID == "" {
		return // not an investigation request (a hand-off, the judge)
	}
	hash := PromptHash(req.System, req.Tools)
	t.mu.Lock()
	defer t.mu.Unlock()
	r := t.rooms[roomID]
	if r == nil {
		r = &tapRoom{}
		t.rooms[roomID] = r
	}
	r.requests++
	if req.Model != "" && !slices.Contains(r.models, req.Model) {
		r.models = append(r.models, req.Model)
	}
	if !slices.Contains(r.hashes, hash) {
		r.hashes = append(r.hashes, hash)
	}
	if !slices.Contains(t.hashes, hash) {
		t.hashes = append(t.hashes, hash)
	}
}

// firstRoomID reads the room id from the first user message, whose content
// is a string or a list of blocks.
func firstRoomID(content json.RawMessage) string {
	var text string
	if json.Unmarshal(content, &text) != nil {
		var blocks []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal(content, &blocks) != nil {
			return ""
		}
		for _, b := range blocks {
			if b.Type == "text" {
				text = b.Text
				break
			}
		}
	}
	if m := roomLine.FindStringSubmatch(text); m != nil {
		return m[1]
	}
	return ""
}

// PromptHash is the hash of what the model is told before any incident data:
// the system prompt and the tool definitions, exactly as sent (compacted
// JSON). Any change to the prompt text, a tool description or schema, or the
// action list in propose_fix changes it.
func PromptHash(system, tools json.RawMessage) string {
	h := sha256.New()
	for _, part := range []json.RawMessage{system, tools} {
		var b bytes.Buffer
		if len(part) > 0 && json.Compact(&b, part) == nil {
			h.Write(b.Bytes())
		} else {
			h.Write(part)
		}
		h.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))[:16]
}

// Room returns what the tap saw for one room.
func (t *ModelTap) Room(roomID string) TapRoom {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := t.rooms[roomID]
	if r == nil {
		return TapRoom{}
	}
	return TapRoom{Models: slices.Clone(r.models), PromptHashes: slices.Clone(r.hashes), Requests: r.requests}
}

// PromptHashes are every prompt hash the tap saw in investigation requests.
func (t *ModelTap) PromptHashes() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.hashes)
}

// defaultAnthropicURL is the SDK's default base URL.
const defaultAnthropicURL = "https://api.anthropic.com"

// InstallModelTap starts a tap in front of the Anthropic API (or the
// ANTHROPIC_BASE_URL already set) and points ANTHROPIC_BASE_URL at it, so
// the next SDK client built in this process goes through it. It changes the
// process environment: call it from a command's main, before building the
// Endpoint, never from a library or a parallel test. A provider other than
// anthropic is left alone and returns (nil, nil).
func InstallModelTap(provider string) (*ModelTap, error) {
	if p := strings.ToLower(strings.TrimSpace(provider)); p != "" && p != agent.ProviderAnthropic {
		return nil, nil
	}
	upstream := strings.TrimSpace(os.Getenv("ANTHROPIC_BASE_URL"))
	if upstream == "" {
		upstream = defaultAnthropicURL
	}
	t, err := NewModelTap(upstream)
	if err != nil {
		return nil, err
	}
	if err := os.Setenv("ANTHROPIC_BASE_URL", t.URL()); err != nil {
		return nil, errors.Join(fmt.Errorf("evals: model tap: %w", err), t.Close())
	}
	return t, nil
}
