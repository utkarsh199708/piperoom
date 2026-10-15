package qamodel

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// PendingTurn is one entry of GET /qa/pending and /qa/wait.
type PendingTurn struct {
	ID string `json:"id"`
	// RoomHint is the room id and alert title from the agent's opening
	// message (or the first line of the request when it has none).
	RoomHint      string    `json:"room_hint"`
	Model         string    `json:"model"`
	ReceivedAt    time.Time `json:"received_at"`
	DeadlineAt    time.Time `json:"deadline_at"`
	SystemBytes   int       `json:"system_bytes"`
	MessagesCount int       `json:"messages_count"`
	Tools         []string  `json:"tools"`
	// Structured is true when the reply text must be JSON matching a schema
	// (hand-off, post-mortem, first-look verdict).
	Structured bool `json:"structured"`
}

func (t *turn) pendingView() PendingTurn {
	return PendingTurn{
		ID: t.id, RoomHint: t.roomHint, Model: t.model,
		ReceivedAt: t.received, DeadlineAt: t.deadline,
		SystemBytes: t.systemBytes, MessagesCount: t.messagesCount,
		Tools: t.tools, Structured: t.structured,
	}
}

// LogEntry is one entry of GET /qa/log: a turn and, once there is one, its reply.
type LogEntry struct {
	PendingTurn
	Seq   int   `json:"seq"`
	State State `json:"state"`
	// LastMessage is a short rendering of the request's final message, the
	// one the reply answers.
	LastMessage string     `json:"last_message"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	// Delivered is false when the reply was accepted but the agent's request
	// had already ended.
	Delivered *bool  `json:"delivered,omitempty"`
	Reply     *Reply `json:"reply,omitempty"`
}

func (t *turn) logEntry() LogEntry {
	e := LogEntry{PendingTurn: t.pendingView(), Seq: t.seq, State: t.state, LastMessage: t.lastMessage, Reply: t.reply}
	if t.state != StatePending {
		fin := t.finished
		e.FinishedAt = &fin
	}
	if t.reply != nil {
		d := t.delivered
		e.Delivered = &d
	}
	return e
}

// serveControl routes /qa/*. Every answer is JSON except the text rendering of
// a turn; errors are always {"error": "..."}.
func (p *Puppet) serveControl(w http.ResponseWriter, r *http.Request) {
	rest := strings.Trim(strings.TrimPrefix(r.URL.Path, "/qa"), "/")
	parts := strings.Split(rest, "/")
	switch {
	case rest == "":
		if requireMethod(w, r, http.MethodGet) {
			p.serveIndex(w)
		}
	case rest == "pending":
		if requireMethod(w, r, http.MethodGet) {
			writeJSON(w, http.StatusOK, p.pending())
		}
	case rest == "wait":
		if requireMethod(w, r, http.MethodGet) {
			p.serveWait(w, r)
		}
	case rest == "log":
		if requireMethod(w, r, http.MethodGet) {
			p.serveLog(w, r)
		}
	case len(parts) == 2 && parts[0] == "turns":
		if requireMethod(w, r, http.MethodGet) {
			p.serveTurn(w, r, parts[1])
		}
	case len(parts) == 3 && parts[0] == "turns" && parts[2] == "reply":
		if requireMethod(w, r, http.MethodPost) {
			p.serveReply(w, r, parts[1])
		}
	default:
		writeError(w, http.StatusNotFound, "no such endpoint; GET /qa/ lists them", nil)
	}
}

func (p *Puppet) serveIndex(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, map[string]any{
		"about": "qamodel puppet model: every model request waits here for a reply you post",
		"endpoints": []string{
			"GET  /qa/pending                 pending turns (JSON list)",
			"GET  /qa/wait?timeout=60s        block until a turn is pending, then the same list ([] on timeout)",
			"GET  /qa/turns/{id}              the request as text; ?system=0 skips the system prompt, ?tools=1 adds tool schemas, ?full=1 skips clipping, ?from=N skips messages before N, ?format=json is the raw request",
			"POST /qa/turns/{id}/reply        {\"text\":\"...\",\"tool_calls\":[{\"name\":\"...\",\"input\":{}}],\"stop\":\"end_turn|tool_use|max_tokens|pause_turn|refusal|model_context_window_exceeded\",\"thinking\":\"...\",\"usage\":{\"input_tokens\":0,\"output_tokens\":0},\"error\":{\"status\":529}}",
			"GET  /qa/log?n=50                the last turns (at most 200) with their replies",
		},
		"reply_timeout": p.cfg.ReplyTimeout.String(),
	})
}

func (p *Puppet) serveWait(w http.ResponseWriter, r *http.Request) {
	d := defaultWait
	if raw := r.URL.Query().Get("timeout"); raw != "" {
		var err error
		if d, err = parseWait(raw); err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), nil)
			return
		}
	}
	writeJSON(w, http.StatusOK, p.waitPending(r.Context(), d))
}

// parseWait reads a /qa/wait timeout: a Go duration ("60s") or bare seconds
// ("60"), at most MaxWait.
func parseWait(raw string) (time.Duration, error) {
	d, err := time.ParseDuration(raw)
	if err != nil {
		secs, serr := strconv.ParseFloat(raw, 64)
		if serr != nil {
			return 0, fmt.Errorf("timeout %q: want a duration such as 60s", raw)
		}
		d = time.Duration(secs * float64(time.Second))
	}
	if d <= 0 || d > MaxWait {
		return 0, fmt.Errorf("timeout %q: want more than 0 and at most %s", raw, MaxWait)
	}
	return d, nil
}

func (p *Puppet) serveLog(w http.ResponseWriter, r *http.Request) {
	n := -1 // all that is kept
	if raw := r.URL.Query().Get("n"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("n %q: want a positive integer", raw), nil)
			return
		}
		n = v
	}
	p.mu.Lock()
	// The newest n turns, oldest first. Finished turns are kept up to the
	// configured log size (200), so by default this is the last 200 turns
	// plus any that are still pending.
	start := 0
	if n >= 0 {
		start = max(len(p.turns)-n, 0)
	}
	out := make([]LogEntry, 0, len(p.turns)-start)
	for _, t := range p.turns[start:] {
		out = append(out, t.logEntry())
	}
	p.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

func (p *Puppet) serveTurn(w http.ResponseWriter, r *http.Request, id string) {
	q := r.URL.Query()
	format := q.Get("format")
	if format != "" && format != "text" && format != "json" {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("format %q: want text or json", format), nil)
		return
	}
	from := 0
	if raw := q.Get("from"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 0 {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("from %q: want a message index, 0 or more", raw), nil)
			return
		}
		from = v
	}
	opts := renderOpts{system: !flagOff(q.Get("system")), tools: flagOn(q.Get("tools")), full: flagOn(q.Get("full")), from: from}

	t, ok := p.lookup(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("no turn %q (unknown, or older than the last %d finished turns)", id, p.cfg.LogSize), nil)
		return
	}
	p.mu.Lock()
	state, req, body := t.state, t.req, t.body
	p.mu.Unlock()
	if state != StatePending {
		writeError(w, http.StatusGone, fmt.Sprintf("turn %s is %s; its request is released, see GET /qa/log", id, state),
			map[string]any{"state": state})
		return
	}
	if format == "json" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, renderTurn(t, req, opts))
}

func flagOn(v string) bool  { return v == "1" || v == "true" }
func flagOff(v string) bool { return v == "0" || v == "false" }

func (p *Puppet) serveReply(w http.ResponseWriter, r *http.Request, id string) {
	t, ok := p.lookup(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("no turn %q", id), nil)
		return
	}
	var rr ReplyRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxControlBytes))
	dec.DisallowUnknownFields() // a typo such as "toolcalls" must not become an empty reply
	err := dec.Decode(&rr)
	if err == nil {
		// Anything after the first document is a mistake worth saying so.
		if _, terr := dec.Token(); !errors.Is(terr, io.EOF) {
			err = errors.New("unexpected data after the JSON reply")
		}
	}
	if err != nil {
		if isTooLarge(err) {
			writeError(w, http.StatusRequestEntityTooLarge, "reply body is larger than 1 MiB", nil)
			return
		}
		writeError(w, http.StatusBadRequest, "bad reply JSON: "+err.Error(), map[string]any{
			"example": map[string]any{
				"text":       "...",
				"tool_calls": []map[string]any{{"name": "<tool>", "input": map[string]any{}}},
				"stop":       "tool_use",
			},
		})
		return
	}

	rep, notes, err := resolveReply(t, rr)
	if err != nil {
		var re *replyError
		if errors.As(err, &re) {
			writeError(w, http.StatusBadRequest, re.msg, re.extra)
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error(), nil)
		return
	}
	if state, ok := p.accept(t, rep); !ok {
		writeError(w, http.StatusConflict, fmt.Sprintf("turn %s is already %s", id, state), map[string]any{"state": state})
		return
	}
	p.log.Info("qamodel: reply accepted", "turn", id, "stop", rep.Stop, "tool_calls", len(rep.ToolCalls), "error", rep.Error != nil)
	if notes == nil {
		notes = []string{} // a list in the JSON, never null
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": id, "state": StateReplied, "reply": rep, "notes": notes,
	})
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	w.Header().Set("Allow", method)
	writeError(w, http.StatusMethodNotAllowed, fmt.Sprintf("%s %s: use %s", r.Method, r.URL.Path, method), nil)
	return false
}

// writeJSON writes v as the response body.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError writes {"error": msg} plus any extra fields.
func writeError(w http.ResponseWriter, status int, msg string, extra map[string]any) {
	body := map[string]any{"error": msg}
	for k, v := range extra {
		body[k] = v
	}
	writeJSON(w, status, body)
}
