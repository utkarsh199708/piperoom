// Package qamodel is a puppet Anthropic Messages endpoint for QA runs: the real
// Piperoom server and the real agent loop run on the real dev stack, but each
// model reply is decided by a tester (a person or another agent) through a small
// control API under /qa/, so no API key is needed.
//
// A model request is parked as a pending turn. The tester lists the pending
// turns, reads one, and posts the reply (text and/or tool calls). The parked
// request then completes with that reply as a streamed Messages response. When
// nobody replies in time the request completes with a plain end_turn text, so
// the agent parks and waits for a human instead of failing.
//
// The agent calls one model endpoint only, the streaming beta Messages API
// (POST /v1/messages?beta=true), for the investigation loop, the first-look
// pass, hand-offs and post-mortems alike. That is the only endpoint served.
package qamodel

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultReplyTimeout is how long a model request waits for a reply. It is
	// kept under the agent's own 10 minute model-turn timeout: at 10 minutes
	// the agent gives up first and reports "model request failed" instead of
	// receiving the fallback text.
	DefaultReplyTimeout = 9 * time.Minute
	// DefaultLogSize is how many finished turns /qa/log keeps.
	DefaultLogSize = 200
	// MaxModelRequestBytes caps a model request body; a long investigation
	// carries a large history, the real API takes far more.
	MaxModelRequestBytes = 16 << 20
	// MaxControlBytes caps a control API request body.
	MaxControlBytes = 1 << 20
	// MaxWait caps /qa/wait?timeout=.
	MaxWait = 10 * time.Minute
	// defaultWait is /qa/wait's timeout when none is given.
	defaultWait = 30 * time.Second

	// FallbackText is the reply a request gets when its deadline passes.
	FallbackText = "qamodel: no reply in time"
)

// State is where a turn is in its life.
type State string

// Turn states.
const (
	// StatePending means the request waits for a reply.
	StatePending State = "pending"
	// StateReplied means a reply was accepted.
	StateReplied State = "replied"
	// StateTimeout means the deadline passed and the fallback text was sent.
	StateTimeout State = "timeout"
	// StateCanceled means the agent's request ended (client gone, or the
	// puppet shut down) before any reply.
	StateCanceled State = "canceled"
)

// Config tunes a Puppet. The zero value works.
type Config struct {
	// ReplyTimeout is how long a request waits for a reply before it gets
	// FallbackText; 0 means DefaultReplyTimeout.
	ReplyTimeout time.Duration
	// LogSize is how many finished turns are kept for /qa/log; 0 means
	// DefaultLogSize. Pending turns are never dropped.
	LogSize int
	// Logger receives one line per turn event (never a prompt or a reply).
	// Nil means slog.Default().
	Logger *slog.Logger
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// Puppet is an http.Handler serving the Messages endpoint and the /qa/ control
// API. Construct it with New and call Close before shutting its server down, so
// requests that are still waiting for a reply return.
type Puppet struct {
	cfg Config
	log *slog.Logger

	mu  sync.Mutex
	seq int
	// turns holds every known turn in arrival order, pending and finished.
	turns []*turn
	byID  map[string]*turn
	// arrived is closed and replaced whenever a turn is enqueued; /qa/wait
	// waits on it.
	arrived chan struct{}

	closed    chan struct{}
	closeOnce sync.Once
}

// New returns a Puppet with cfg.
func New(cfg Config) *Puppet {
	if cfg.ReplyTimeout <= 0 {
		cfg.ReplyTimeout = DefaultReplyTimeout
	}
	if cfg.LogSize <= 0 {
		cfg.LogSize = DefaultLogSize
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Puppet{
		cfg:     cfg,
		log:     cfg.Logger,
		byID:    map[string]*turn{},
		arrived: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

// Close makes every waiting request return and every later /qa/wait return at
// once. It is safe to call more than once.
func (p *Puppet) Close() {
	p.closeOnce.Do(func() { close(p.closed) })
}

// ServeHTTP routes a Messages request to the puppet and /qa/ to the control API.
func (p *Puppet) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	// HasSuffix, not equality: ANTHROPIC_BASE_URL may carry a path prefix.
	case strings.HasSuffix(path, "/v1/messages"):
		p.serveMessages(w, r)
	case path == "/qa" || strings.HasPrefix(path, "/qa/"):
		p.serveControl(w, r)
	default:
		writeAPIError(w, http.StatusNotFound, "not_found_error",
			"qamodel serves POST /v1/messages and the /qa/ control API only")
	}
}

// turn is one parked model request.
type turn struct {
	// Set at enqueue and never changed, so readable without the lock.
	id            string
	seq           int
	received      time.Time
	deadline      time.Time
	model         string
	roomHint      string
	systemBytes   int
	messagesCount int
	tools         []string
	toolSet       map[string]bool
	required      map[string][]string // tool name -> required input fields
	structured    bool
	thinking      bool
	lastMessage   string

	// Guarded by Puppet.mu.
	state State
	// req and body are released when the turn finishes: 200 requests of a long
	// investigation would otherwise stay in memory.
	req       *apiRequest
	body      []byte
	reply     *Reply
	finished  time.Time
	delivered bool
	// done is closed when a reply is accepted.
	done chan struct{}
}

// enqueue parks a request and wakes /qa/wait callers.
func (p *Puppet) enqueue(req *apiRequest, body []byte) *turn {
	t := &turn{
		received:      p.cfg.Now(),
		model:         req.Model,
		roomHint:      roomHint(req),
		systemBytes:   len(req.systemText()),
		messagesCount: len(req.Messages),
		tools:         req.toolNames(),
		structured:    req.structured(),
		thinking:      req.thinkingOn(),
		lastMessage:   lastMessageExcerpt(req),
		state:         StatePending,
		req:           req,
		body:          body,
		done:          make(chan struct{}),
	}
	t.deadline = t.received.Add(p.cfg.ReplyTimeout)
	t.required = req.requiredFields()
	t.toolSet = make(map[string]bool, len(t.tools))
	for _, n := range t.tools {
		t.toolSet[n] = true
	}

	p.mu.Lock()
	p.seq++
	t.seq = p.seq
	t.id = "t" + strconv.Itoa(p.seq)
	p.turns = append(p.turns, t)
	p.byID[t.id] = t
	close(p.arrived)
	p.arrived = make(chan struct{})
	p.mu.Unlock()

	p.log.Info("qamodel: turn pending", "turn", t.id, "room", t.roomHint, "model", t.model,
		"messages", t.messagesCount, "tools", len(t.tools))
	return t
}

// finishLocked ends a turn in state st and drops its heavy fields. Callers hold
// p.mu and have checked t.state == StatePending.
func (p *Puppet) finishLocked(t *turn, st State) {
	t.state = st
	t.finished = p.cfg.Now()
	t.req, t.body = nil, nil
	p.trimLocked()
}

// trimLocked forgets the oldest finished turns beyond the log size.
func (p *Puppet) trimLocked() {
	finished := 0
	for _, t := range p.turns {
		if t.state != StatePending {
			finished++
		}
	}
	drop := finished - p.cfg.LogSize
	if drop <= 0 {
		return
	}
	kept := p.turns[:0]
	for _, t := range p.turns {
		if drop > 0 && t.state != StatePending {
			delete(p.byID, t.id)
			drop--
			continue
		}
		kept = append(kept, t)
	}
	clear(p.turns[len(kept):])
	p.turns = kept
}

// lookup returns the turn with id.
func (p *Puppet) lookup(id string) (*turn, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	t, ok := p.byID[id]
	return t, ok
}

// accept stores rep as the turn's reply and wakes its request. It reports the
// state the turn was already in when it was not pending.
func (p *Puppet) accept(t *turn, rep *Reply) (State, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if t.state != StatePending {
		return t.state, false
	}
	t.reply = rep
	p.finishLocked(t, StateReplied)
	close(t.done)
	return StateReplied, true
}

// settle ends a request that no reply reached: a deadline, a gone client, or a
// shutdown. It returns the reply the request must send (nil when it sends
// none). A reply accepted in the same instant wins, so no accepted reply is
// dropped silently.
func (p *Puppet) settle(t *turn, why State) *Reply {
	p.mu.Lock()
	defer p.mu.Unlock()
	if t.state == StateReplied {
		return t.reply
	}
	if why == StateTimeout {
		t.reply = &Reply{Text: FallbackText, Stop: StopEndTurn, Fallback: true, Usage: defaultUsage(t.messagesCount, &Reply{Text: FallbackText})}
	}
	p.finishLocked(t, why)
	if why == StateTimeout {
		return t.reply
	}
	return nil
}

// delivery records whether the HTTP response of a replied turn reached the
// client.
func (p *Puppet) delivery(t *turn, ok bool) {
	p.mu.Lock()
	t.delivered = ok
	p.mu.Unlock()
}

// pending lists the pending turns in arrival order.
func (p *Puppet) pending() []PendingTurn {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pendingLocked()
}

func (p *Puppet) pendingLocked() []PendingTurn {
	out := []PendingTurn{}
	for _, t := range p.turns {
		if t.state == StatePending {
			out = append(out, t.pendingView())
		}
	}
	return out
}

// waitPending blocks until a turn is pending, d passes, ctx ends or the puppet
// closes, then returns the pending turns (an empty list on a timeout).
func (p *Puppet) waitPending(ctx context.Context, d time.Duration) []PendingTurn {
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		p.mu.Lock()
		list := p.pendingLocked()
		arrived := p.arrived
		p.mu.Unlock()
		if len(list) > 0 {
			return list
		}
		select {
		case <-arrived:
		case <-timer.C:
			return list
		case <-ctx.Done():
			return list
		case <-p.closed:
			return list
		}
	}
}
