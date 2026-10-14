package loadtest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Room title markers. The agent puts the alert title in its opening user
// message, so the fake model reads the room's mode from there.
const (
	// ModeQuiet rooms get an immediate end_turn with no tool calls, so their
	// agent parks at once and the room only carries what the driver posts.
	ModeQuiet = "ltmode-quiet"
	// ModeFlood rooms never stop calling tools (stop_reason tool_use on every
	// turn) until the agent's turn guard pauses them.
	ModeFlood = "ltmode-flood"
	// ModePropose rooms call propose_fix once after their tool calls.
	ModePropose = "ltmode-propose"
)

// ModelConfig shapes the fake model's replies.
type ModelConfig struct {
	// Latency is how long each reply waits before it is written.
	Latency time.Duration
	// OutputTokens is the reported output token count of every turn; the
	// final turn's text is about four bytes per token.
	OutputTokens int
	// ToolCallsPerRun is the number of read-tool calls before the final
	// end_turn reply; ToolsPerTurn of them go in one turn (at least 1).
	ToolCallsPerRun int
	ToolsPerTurn    int
	// ToolName is the read tool to call; "" means ProbeToolName.
	ToolName string
	// FloodLatency and FloodToolsPerTurn apply to ModeFlood rooms.
	FloodLatency      time.Duration
	FloodToolsPerTurn int
	// ProposeAction is the action a ModePropose room proposes.
	ProposeAction string
}

func (c ModelConfig) withDefaults() ModelConfig {
	if c.ToolsPerTurn < 1 {
		c.ToolsPerTurn = 1
	}
	if c.FloodToolsPerTurn < 1 {
		c.FloodToolsPerTurn = 1
	}
	if c.OutputTokens < 1 {
		c.OutputTokens = 1
	}
	if c.ToolName == "" {
		c.ToolName = ProbeToolName
	}
	if c.ProposeAction == "" {
		c.ProposeAction = NoopActionName
	}
	return c
}

// RoomModelStats is what the fake model saw for one room.
type RoomModelStats struct {
	FirstRequest time.Time `json:"first_request"`
	Requests     int       `json:"requests"`
	// RunStarts counts requests whose history is only the opening user
	// message: one per agent run. More than one in a clean run means the
	// room's agent was started twice (a restart or a duplicate run).
	RunStarts int `json:"run_starts"`
	EndTurns  int `json:"end_turns"`
	ToolUses  int `json:"tool_uses"`
	Proposals int `json:"proposals"`
	// MaxInFlight is the most requests this room had open at once; above 1
	// means two agent runs called the model for the same room concurrently.
	MaxInFlight int `json:"max_in_flight"`
	inFlight    int
}

// ModelStats is a copy of the fake model's counters.
type ModelStats struct {
	Requests    int64                      `json:"requests"`
	BadRequests int64                      `json:"bad_requests"`
	Canceled    int64                      `json:"canceled"`
	MaxInFlight int                        `json:"max_in_flight"`
	First       time.Time                  `json:"first"`
	Last        time.Time                  `json:"last"`
	Rooms       map[string]*RoomModelStats `json:"rooms"`
}

// FakeModel is an http.Handler that speaks enough of the Anthropic Messages
// API (streaming, beta endpoint) for the real agent loop to run against it.
// Its reply to a request depends only on the config and the request body,
// never on timing or on other rooms, so runs are repeatable.
type FakeModel struct {
	mu       sync.Mutex
	cfg      ModelConfig
	stats    ModelStats
	inFlight int
	now      func() time.Time
}

// NewFakeModel returns a fake model with cfg.
func NewFakeModel(cfg ModelConfig) *FakeModel {
	m := &FakeModel{now: time.Now}
	m.Reset(cfg)
	return m
}

// Reset installs cfg and clears every counter. Requests in flight finish with
// the config they started with.
func (m *FakeModel) Reset(cfg ModelConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg = cfg.withDefaults()
	m.stats = ModelStats{Rooms: map[string]*RoomModelStats{}}
	m.inFlight = 0
}

// Stats returns a deep copy of the counters.
func (m *FakeModel) Stats() ModelStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.stats
	s.Rooms = make(map[string]*RoomModelStats, len(m.stats.Rooms))
	for k, v := range m.stats.Rooms {
		c := *v
		s.Rooms[k] = &c
	}
	return s
}

// modelRequest is the part of a Messages API request the fake reads.
type modelRequest struct {
	Messages []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
}

// replyKind is what a turn does.
type replyKind int

const (
	replyEnd replyKind = iota
	replyTools
	replyPropose
)

// plan is one computed reply.
type plan struct {
	room      string
	mode      string
	kind      replyKind
	turn      int // assistant turns already in the history
	toolCalls int
	latency   time.Duration
	runStart  bool
}

var roomLine = regexp.MustCompile(`(?m)^Room: (r_[0-9A-Za-z]+)$`)

// planFor decides the reply from the config and the request alone.
func planFor(cfg ModelConfig, req modelRequest) plan {
	first := ""
	if len(req.Messages) > 0 {
		first = contentText(req.Messages[0].Content)
	}
	p := plan{latency: cfg.Latency}
	if m := roomLine.FindStringSubmatch(first); m != nil {
		p.room = m[1]
	}
	for _, msg := range req.Messages {
		if msg.Role == "assistant" {
			p.turn++
		}
	}
	p.runStart = len(req.Messages) == 1
	switch {
	case strings.Contains(first, ModeQuiet):
		p.mode, p.kind, p.latency = ModeQuiet, replyEnd, 0
		return p
	case strings.Contains(first, ModeFlood):
		p.mode, p.kind, p.latency, p.toolCalls = ModeFlood, replyTools, cfg.FloodLatency, cfg.FloodToolsPerTurn
		return p
	case strings.Contains(first, ModePropose):
		p.mode = ModePropose
	}
	toolTurns := (cfg.ToolCallsPerRun + cfg.ToolsPerTurn - 1) / cfg.ToolsPerTurn
	switch {
	case p.turn < toolTurns:
		p.kind = replyTools
		p.toolCalls = min(cfg.ToolsPerTurn, cfg.ToolCallsPerRun-p.turn*cfg.ToolsPerTurn)
	case p.mode == ModePropose && p.turn == toolTurns:
		p.kind = replyPropose
	default:
		p.kind = replyEnd
	}
	return p
}

// contentText joins the text of a message content, which is either a string
// or an array of blocks.
func contentText(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) != nil {
		return ""
	}
	var b strings.Builder
	for _, bl := range blocks {
		if bl.Type == "text" {
			b.WriteString(bl.Text)
			b.WriteString("\n")
		}
	}
	return b.String()
}

// maxModelRequest caps a request body; the real API takes far more, but a
// load-test conversation is small.
const maxModelRequest = 16 << 20

// ServeHTTP implements the streaming Messages endpoint (POST /v1/messages).
func (m *FakeModel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/v1/messages") {
		writeModelError(w, http.StatusNotFound, "not_found_error", "fake model serves POST /v1/messages only")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxModelRequest+1))
	if err != nil || len(body) > maxModelRequest {
		m.count(func(s *ModelStats) { s.BadRequests++ })
		writeModelError(w, http.StatusBadRequest, "invalid_request_error", "unreadable or oversized body")
		return
	}
	var req modelRequest
	if err := json.Unmarshal(body, &req); err != nil || len(req.Messages) == 0 {
		m.count(func(s *ModelStats) { s.BadRequests++ })
		writeModelError(w, http.StatusBadRequest, "invalid_request_error", "want a JSON body with messages")
		return
	}

	m.mu.Lock()
	cfg := m.cfg
	p := planFor(cfg, req)
	now := m.now()
	m.stats.Requests++
	if m.stats.First.IsZero() {
		m.stats.First = now
	}
	m.inFlight++
	m.stats.MaxInFlight = max(m.stats.MaxInFlight, m.inFlight)
	rs := m.stats.Rooms[p.room]
	if rs == nil {
		rs = &RoomModelStats{FirstRequest: now}
		m.stats.Rooms[p.room] = rs
	}
	rs.Requests++
	if p.runStart {
		rs.RunStarts++
	}
	rs.inFlight++
	rs.MaxInFlight = max(rs.MaxInFlight, rs.inFlight)
	m.mu.Unlock()

	ok := sleepCtx(r.Context(), p.latency)
	out := renderReply(cfg, p, body)

	m.mu.Lock()
	m.inFlight--
	rs.inFlight--
	m.stats.Last = m.now()
	if !ok {
		m.stats.Canceled++
	} else {
		switch p.kind {
		case replyEnd:
			rs.EndTurns++
		case replyTools:
			rs.ToolUses += p.toolCalls
		case replyPropose:
			rs.Proposals++
		}
	}
	m.mu.Unlock()
	if !ok {
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

func (m *FakeModel) count(f func(*ModelStats)) {
	m.mu.Lock()
	f(&m.stats)
	m.mu.Unlock()
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func writeModelError(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":  "error",
		"error": map[string]string{"type": typ, "message": msg},
	})
}

// fillerText returns n bytes of deterministic text.
func fillerText(n int) string {
	const words = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor "
	if n <= 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(n)
	for b.Len() < n {
		b.WriteString(words)
	}
	return b.String()[:n]
}

// renderReply writes the whole SSE stream of one reply. It is a pure function
// of cfg, the plan and the request body: the message id is a hash of the
// body, tool_use ids are numbered by turn and position.
func renderReply(cfg ModelConfig, p plan, body []byte) []byte {
	sum := sha256.Sum256(body)
	msgID := "msg_lt_" + hex.EncodeToString(sum[:8])
	var b bytes.Buffer
	ev := func(name string, v any) {
		data, _ := json.Marshal(v) // plain maps and strings: cannot fail
		fmt.Fprintf(&b, "event: %s\ndata: %s\n\n", name, data)
	}
	ev("message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id": msgID, "type": "message", "role": "assistant", "model": "lt-fake-model",
			"content": []any{}, "stop_reason": nil, "stop_sequence": nil,
			"usage": map[string]any{"input_tokens": len(body)/4 + 1, "output_tokens": 1},
		},
	})
	idx := 0
	stop := "end_turn"
	toolUse := func(id, name string, input any) {
		in, _ := json.Marshal(input)
		ev("content_block_start", map[string]any{
			"type": "content_block_start", "index": idx,
			"content_block": map[string]any{"type": "tool_use", "id": id, "name": name, "input": map[string]any{}},
		})
		ev("content_block_delta", map[string]any{
			"type": "content_block_delta", "index": idx,
			"delta": map[string]any{"type": "input_json_delta", "partial_json": string(in)},
		})
		ev("content_block_stop", map[string]any{"type": "content_block_stop", "index": idx})
		idx++
	}
	switch p.kind {
	case replyTools:
		stop = "tool_use"
		for i := 0; i < p.toolCalls; i++ {
			n := p.turn*1000 + i
			toolUse(fmt.Sprintf("toolu_lt_%d_%d", p.turn, i), cfg.ToolName,
				map[string]any{"n": n, "why": fmt.Sprintf("lt probe %d", n)})
		}
	case replyPropose:
		stop = "tool_use"
		params, _ := json.Marshal(map[string]string{"key": p.room})
		toolUse(fmt.Sprintf("toolu_lt_%d_p", p.turn), "propose_fix", map[string]any{
			"title": "lt fix " + p.room,
			"steps": []map[string]string{{
				"action": cfg.ProposeAction, "params": string(params), "description": "no-op step of the load test",
			}},
			"risk":     "none: the load-test action changes nothing",
			"rollback": "nothing to undo: the step changes nothing",
		})
	default:
		text := "lt done. " + fillerText(4*cfg.OutputTokens)
		if p.mode == ModeQuiet {
			text = "lt quiet room: nothing to do."
		}
		ev("content_block_start", map[string]any{
			"type": "content_block_start", "index": idx,
			"content_block": map[string]any{"type": "text", "text": ""},
		})
		const chunk = 64
		for off := 0; off < len(text); off += chunk {
			ev("content_block_delta", map[string]any{
				"type": "content_block_delta", "index": idx,
				"delta": map[string]any{"type": "text_delta", "text": text[off:min(off+chunk, len(text))]},
			})
		}
		ev("content_block_stop", map[string]any{"type": "content_block_stop", "index": idx})
	}
	ev("message_delta", map[string]any{
		"type":  "message_delta",
		"delta": map[string]any{"stop_reason": stop, "stop_sequence": nil},
		"usage": map[string]any{"output_tokens": cfg.OutputTokens},
	})
	ev("message_stop", map[string]any{"type": "message_stop"})
	return b.Bytes()
}
