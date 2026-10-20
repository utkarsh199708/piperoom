package runner_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
)

// testClock is a manually advanced clock for the runner's token expiry.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock { return &testClock{now: time.Now()} }

func (c *testClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// fakeCatalogue is the runner's Catalogue for tests.
type fakeCatalogue struct {
	mu        sync.Mutex
	calls     []protocol.OpRequest
	onExecute func(context.Context, protocol.OpRequest) (protocol.OpResult, error)
	floor     map[string]string
	target    map[string]string
	guard     map[string]bool
}

func (f *fakeCatalogue) Execute(ctx context.Context, req protocol.OpRequest) (protocol.OpResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, req)
	f.mu.Unlock()
	if f.onExecute != nil {
		return f.onExecute(ctx, req)
	}
	return protocol.OpResult{ID: req.ID, OK: true, Summary: "ok"}, nil
}

func (f *fakeCatalogue) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeCatalogue) Floor(op string) (string, bool) {
	t, ok := f.floor[op]
	return t, ok
}

func (f *fakeCatalogue) Target(req protocol.OpRequest) string { return f.target[req.Op] }

func (f *fakeCatalogue) GuardForbidden(op, class string) bool { return f.guard[op+"|"+class] }

// testProposal builds a frozen proposal with one step and a stable shape.
func testProposal(proposalID string) events.ProposalCreatedPayload {
	return events.ProposalCreatedPayload{
		ProposalID:     proposalID,
		Title:          "fix the stack",
		Risk:           "low",
		Rollback:       "revert",
		RequiredLayers: []string{"environment"},
		Steps: []events.ProposalStep{
			{Action: "compose_up", Layer: "environment", Params: json.RawMessage(`{"service":"connect"}`), Description: "start connect"},
		},
	}
}

// proposalHash computes the runner's hash for a frozen proposal in room r1.
func proposalHash(t *testing.T, p events.ProposalCreatedPayload) string {
	t.Helper()
	h, err := runner.ComputeParamsHash("r1", p.ProposalID, p.Title, p.Risk, p.Rollback, p.RequiredLayers, p.Steps)
	if err != nil {
		t.Fatalf("ComputeParamsHash: %v", err)
	}
	return h
}

func proposalRaw(t *testing.T, p events.ProposalCreatedPayload) []byte {
	t.Helper()
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal proposal: %v", err)
	}
	return b
}

// opReq builds a T1+ OpRequest carrying a frozen proposal and its hash.
func opReq(id, op, tier, proposalID string, step int, hash string, frozen []byte) protocol.OpRequest {
	si := step
	return protocol.OpRequest{
		ID:         id,
		Op:         op,
		Params:     json.RawMessage(`{"service":"connect"}`),
		Tier:       tier,
		Class:      protocol.ClassLocal,
		ProposalID: proposalID,
		StepIndex:  &si,
		ParamsHash: hash,
		Proposal:   frozen,
	}
}

func readFrameJSON(t *testing.T, conn *websocket.Conn, ctx context.Context) protocol.Frame {
	t.Helper()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	var f protocol.Frame
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	return f
}

func writeFrameJSON(t *testing.T, conn *websocket.Conn, ctx context.Context, f protocol.Frame) {
	t.Helper()
	if err := conn.Write(ctx, websocket.MessageText, mustFrame(f)); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

// doHandshake plays the server side of a first pairing: Pair -> Welcome (with
// targets) -> Hello -> Token (expiring 30 min from the injected clock).
func doHandshake(t *testing.T, conn *websocket.Conn, ctx context.Context, token string, now func() time.Time, targets []events.SetupTarget) {
	t.Helper()
	f := readFrameJSON(t, conn, ctx)
	if f.Type != protocol.FramePair || f.Pair == nil {
		t.Fatalf("expected pair, got %+v", f)
	}
	writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameWelcome, Welcome: &protocol.Welcome{
		RoomID: "r1", Title: "orders", PipelineID: "orders",
		Creator:         events.Actor{Kind: events.ActorHuman, ID: "u1", Name: "Ada"},
		ProtocolVersion: 1, ProtocolMinor: 1,
		Targets: targets,
	}})
	f = readFrameJSON(t, conn, ctx)
	if f.Type != protocol.FrameHello || f.Hello == nil {
		t.Fatalf("expected hello, got %+v", f)
	}
	writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameToken, Token: &protocol.Token{
		Token: token, ExpiresAt: now().Add(30 * time.Minute),
	}})
}

// doReconnect plays the server side of a token reconnect: it reads Pair, checks
// the token, sends a fresh one, and returns the Resume the runner must send
// first.
func doReconnect(t *testing.T, conn *websocket.Conn, ctx context.Context, wantToken, newToken string, now func() time.Time) protocol.Resume {
	t.Helper()
	f := readFrameJSON(t, conn, ctx)
	if f.Type != protocol.FramePair || f.Pair == nil {
		t.Fatalf("expected pair, got %+v", f)
	}
	if f.Pair.Token != wantToken {
		t.Fatalf("reconnect pair token = %q, want %q", f.Pair.Token, wantToken)
	}
	writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameToken, Token: &protocol.Token{
		Token: newToken, ExpiresAt: now().Add(30 * time.Minute),
	}})
	f = readFrameJSON(t, conn, ctx)
	if f.Type != protocol.FrameResume || f.Resume == nil {
		t.Fatalf("expected resume first after reconnect, got %+v", f)
	}
	return *f.Resume
}

// runMultiServer serves the handlers in order, one per connection, and closes
// each connection normally when its handler returns.
func runMultiServer(handlers ...func(conn *websocket.Conn)) *httptest.Server {
	var mu sync.Mutex
	i := 0
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		idx := i
		i++
		mu.Unlock()
		defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()
		if idx < len(handlers) {
			handlers[idx](conn)
		}
	}))
}

// readOpLog returns the runner's op log for a project directory.
func readOpLog(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, ".piperoom", "ops.jsonl"))
	if err != nil {
		t.Fatalf("read op log: %v", err)
	}
	return string(b)
}
