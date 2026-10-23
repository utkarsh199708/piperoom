package remoteclients

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
)

// The tests in this file drive the remote clients through the real setup
// runner (runner.Runner over a WebSocket), not the fake: the runner's own
// checks decide, so a write whose params are not the frozen step's
// (params_hash), whose op is not the step's action, or that is sent twice for
// one step (duplicate_step) fails here exactly as it would in a room. Only the
// catalogue is a test double, and its StepAction is the identity the real Q
// catalogue entries use (ops.go).

// stepCatalogue is the runner's catalogue for these tests: every op in Ops()
// at its floor, answering with the fake runner's default data (or answers').
// fail makes an op answer OK=false with an error_code.
type stepCatalogue struct {
	mu      sync.Mutex
	calls   []protocol.OpRequest
	answers map[string]json.RawMessage
	fail    map[string]string
	extra   map[string]string // more op -> floor, for the controls
}

func (c *stepCatalogue) Execute(_ context.Context, req protocol.OpRequest) (protocol.OpResult, error) {
	c.mu.Lock()
	c.calls = append(c.calls, req)
	code := c.fail[req.Op]
	c.mu.Unlock()
	if code != "" {
		data, _ := json.Marshal(map[string]string{"error_code": code})
		return protocol.OpResult{ID: req.ID, OK: false, Summary: "connect refused: " + code, Data: data}, nil
	}
	data, ok := c.answers[req.Op]
	if !ok {
		data = defaultAnswers[req.Op]
	}
	return protocol.OpResult{ID: req.ID, OK: true, Summary: "ok", Data: data}, nil
}

func (c *stepCatalogue) Floor(op string) (string, bool) {
	for _, spec := range Ops() {
		if spec.Op == op {
			return spec.Tier, true
		}
	}
	tier, ok := c.extra[op]
	return tier, ok
}

func (c *stepCatalogue) Target(req protocol.OpRequest) string {
	for _, spec := range Ops() {
		if spec.Op == req.Op {
			return spec.Target
		}
	}
	return ""
}

func (c *stepCatalogue) GuardForbidden(string, string) bool { return false }

func (c *stepCatalogue) StepAction(op string) (string, bool) {
	_, ok := c.Floor(op)
	return op, ok
}

// executed returns the requests the catalogue ran for op.
func (c *stepCatalogue) executed(op string) []protocol.OpRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []protocol.OpRequest
	for _, r := range c.calls {
		if r.Op == op {
			out = append(out, r)
		}
	}
	return out
}

// wsExecutor is the server side of the room's runner socket: it sends one
// FrameOp and waits for that op's Result or Refused, answering pings. A
// refusal comes back as *protocol.RefusedError, as the room's executor
// returns it.
type wsExecutor struct {
	mu   sync.Mutex
	conn *websocket.Conn
}

func (e *wsExecutor) Execute(ctx context.Context, _ string, req protocol.OpRequest) (protocol.OpResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.write(ctx, protocol.Frame{Type: protocol.FrameOp, Op: &req}); err != nil {
		return protocol.OpResult{}, err
	}
	for {
		_, data, err := e.conn.Read(ctx)
		if err != nil {
			return protocol.OpResult{}, err
		}
		var f protocol.Frame
		if err := json.Unmarshal(data, &f); err != nil {
			return protocol.OpResult{}, err
		}
		switch {
		case f.Type == protocol.FrameResult && f.Result != nil && f.Result.ID == req.ID:
			return *f.Result, nil
		case f.Type == protocol.FrameRefused && f.Refused != nil && f.Refused.ID == req.ID:
			return protocol.OpResult{}, &protocol.RefusedError{Refusal: *f.Refused}
		case f.Type == protocol.FramePing:
			if err := e.write(ctx, protocol.Frame{Type: protocol.FramePong}); err != nil {
				return protocol.OpResult{}, err
			}
		}
	}
}

func (e *wsExecutor) write(ctx context.Context, f protocol.Frame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return e.conn.Write(ctx, websocket.MessageText, b)
}

// startRunner pairs a real runner with cat to a test server for room r_setup
// and returns the executor that reaches it. The runner stops at test end.
// allowT3 is the runner's AllowT3 (false is how cmd/setup builds it).
func startRunner(t *testing.T, cat runner.Catalogue, allowT3 bool) *wsExecutor {
	t.Helper()
	conns := make(chan *websocket.Conn, 1)
	finish := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()
		if err := serverHandshake(r.Context(), conn); err != nil {
			t.Errorf("handshake: %v", err)
			close(conns)
			return
		}
		conns <- conn
		<-finish
	}))
	// Every exit below, a FailNow included, stops the server; the runner's
	// stop is registered as soon as it runs.
	t.Cleanup(srv.Close)
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), AllowT3: allowT3,
		// Every T3 op asks on the terminal; the person says yes.
		Stdin:  strings.NewReader(strings.Repeat("y\n", 8)),
		Stdout: io.Discard, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Catalogue: cat,
	})
	if err != nil {
		t.Fatal(err)
	}
	ran := make(chan error, 1)
	go func() { ran <- r.Run(context.Background()) }()
	t.Cleanup(func() {
		close(finish)
		select {
		case <-ran:
		case <-time.After(10 * time.Second):
			t.Error("the runner did not stop")
		}
	})
	conn, ok := <-conns
	if !ok {
		t.FailNow()
	}
	return &wsExecutor{conn: conn}
}

// serverHandshake plays the server side of a first pairing.
func serverHandshake(ctx context.Context, conn *websocket.Conn) error {
	read := func(want protocol.FrameType) error {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		var f protocol.Frame
		if err := json.Unmarshal(data, &f); err != nil {
			return err
		}
		if f.Type != want {
			return fmt.Errorf("got frame %q, want %q", f.Type, want)
		}
		return nil
	}
	write := func(f protocol.Frame) error {
		b, err := json.Marshal(f)
		if err != nil {
			return err
		}
		return conn.Write(ctx, websocket.MessageText, b)
	}
	if err := read(protocol.FramePair); err != nil {
		return err
	}
	if err := write(protocol.Frame{Type: protocol.FrameWelcome, Welcome: &protocol.Welcome{
		RoomID: "r_setup", Title: "orders", PipelineID: "orders",
		Creator:         events.Actor{Kind: events.ActorHuman, ID: "u1", Name: "Ada"},
		ProtocolVersion: 1, ProtocolMinor: 1,
	}}); err != nil {
		return err
	}
	if err := read(protocol.FrameHello); err != nil {
		return err
	}
	return write(protocol.Frame{Type: protocol.FrameToken, Token: &protocol.Token{
		Token: "tok", ExpiresAt: time.Now().Add(30 * time.Minute),
	}})
}

// frozenStepCtx is the room's step context for a one-step proposal whose step
// is action with params: the proposal fields, the hash the runner recomputes,
// the frozen proposal and the frozen step.
func frozenStepCtx(t *testing.T, proposalID, tier, action, params string) context.Context {
	t.Helper()
	p := events.ProposalCreatedPayload{
		ProposalID: proposalID, Title: "set up the source", Risk: "low", Rollback: "undo the step",
		RequiredLayers: []string{"database"},
		Steps:          []events.ProposalStep{{Action: action, Layer: "database", Params: json.RawMessage(params), Description: "step"}},
	}
	hash, err := runner.ComputeParamsHash("r_setup", p.ProposalID, p.Title, p.Risk, p.Rollback, p.RequiredLayers, p.Steps)
	if err != nil {
		t.Fatalf("ComputeParamsHash: %v", err)
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	ctx := room.WithSetupStep(roomCtx(t), room.SetupStep{
		ProposalID: proposalID, StepIndex: 0, ParamsHash: hash,
		Tier: tier, Class: protocol.ClassLocal,
		Action: action, Params: json.RawMessage(params),
	})
	return WithProposal(ctx, raw)
}

// TestWritesThroughTheRealRunner sends every write method's step through the
// real runner: each runs once, with the frozen step's params, and the same
// step sent again (by a session that does not remember it) is refused by the
// runner as duplicate_step.
func TestWritesThroughTheRealRunner(t *testing.T) {
	cat := &stepCatalogue{}
	ex := startRunner(t, cat, true)
	for i, w := range writeCases() {
		t.Run(w.method, func(t *testing.T) {
			ctx := frozenStepCtx(t, fmt.Sprintf("p_%d", i), w.tier, w.action, w.params)
			before := len(cat.executed(w.action))
			if err := w.run(ctx, New(ex, "r_setup", allowT3Steps())); err != nil {
				t.Fatalf("the runner did not run the step's write: %v", err)
			}
			got := cat.executed(w.action)
			if len(got) != before+1 {
				t.Fatalf("the catalogue ran %s %d times, want once", w.action, len(got)-before)
			}
			if string(got[len(got)-1].Params) != w.params {
				t.Fatalf("the runner ran params %s, want the frozen step's %s", got[len(got)-1].Params, w.params)
			}
			// The runner's ledger is the boundary: the same step again is
			// refused, whatever the client remembers.
			err := w.run(ctx, New(ex, "r_setup", allowT3Steps()))
			if !errors.Is(err, protocol.ErrDuplicateStep) {
				t.Fatalf("the same step again = %v, want the runner's duplicate_step", err)
			}
		})
	}
}

// TestRunnerRefusesTheOldWireShapes is the control that keeps the test above
// honest: the request shapes this package sent before the step binding (a
// params projection without the step's other fields; the alter op for a reset
// step) are refused by the same runner as params_hash. If the harness stopped
// checking, these would run.
func TestRunnerRefusesTheOldWireShapes(t *testing.T) {
	cat := &stepCatalogue{extra: map[string]string{"connect_alter_source_offsets": protocol.TierT2}}
	ex := startRunner(t, cat, true)
	send := func(ctx context.Context, op, params string) error {
		step, _ := room.SetupStepFrom(ctx)
		idx := step.StepIndex
		_, err := ex.Execute(ctx, "r_setup", protocol.OpRequest{
			ID: "old_" + op, Op: op, Params: json.RawMessage(params), Tier: protocol.TierT2, Class: protocol.ClassLocal,
			Deadline: time.Now().Add(10 * time.Second), ProposalID: step.ProposalID, StepIndex: &idx,
			ParamsHash: step.ParamsHash, Proposal: ProposalFrom(ctx),
		})
		return err
	}
	end := writeCaseFor(t, "Source.EndSession")
	ctx := frozenStepCtx(t, "p_old_end", end.tier, end.action, end.params)
	if err := send(ctx, end.action, `{"pid":42,"started_at":"2026-01-01T00:00:00Z"}`); !errors.Is(err, protocol.ErrParamsHash) {
		t.Fatalf("a params projection = %v, want params_hash", err)
	}
	alter := writeCaseFor(t, "Connect.AlterSourceOffsets")
	ctx = frozenStepCtx(t, "p_old_alter", alter.tier, alter.action, alter.params)
	if err := send(ctx, "connect_alter_source_offsets", alter.params); !errors.Is(err, protocol.ErrParamsHash) {
		t.Fatalf("the alter op on a reset step = %v, want params_hash", err)
	}
	if n := len(cat.executed(end.action)) + len(cat.executed("connect_alter_source_offsets")); n != 0 {
		t.Fatalf("the runner ran %d refused requests", n)
	}
}

// TestCreateRetryOverTheRealRunner runs connect_create_connector's whole
// Execute through the real runner while Connect answers the create with a
// rebalance: the create op runs once, the action's in-step retry never
// reaches the runner (no duplicate_step), and the error says why.
func TestCreateRetryOverTheRealRunner(t *testing.T) {
	cat := &stepCatalogue{
		// The connector is not registered yet, and Connect answers the
		// create with a rebalance.
		answers: map[string]json.RawMessage{OpConnectListConnectors: json.RawMessage(`{"connectors":[]}`)},
		fail:    map[string]string{OpConnectCreateConnector: ErrorCodeRebalancing},
	}
	ex := startRunner(t, cat, true)
	reg := actions.NewRegistry()
	if err := actions.RegisterLifecycle(reg, actions.LifecycleDeps{Connect: New(ex, "r_setup").Connect()}); err != nil {
		t.Fatal(err)
	}
	act, _ := reg.Get(actions.CreateConnectorName)
	input := `{"connector":"pg-orders-connector","config":{"connector.class":"` + postgresConnectorClass + `",` +
		`"database.hostname":"postgres","database.password":"${secret:source_password}","topic.prefix":"pg"}}`
	ctx := frozenStepCtx(t, "p_create", protocol.TierT2, actions.CreateConnectorName, input)
	_, err := act.Execute(ctx, json.RawMessage(input))
	// The first attempt's error says the step is spent, so the action's
	// retry loop stops there: it never even asks for a second send.
	if !errors.Is(err, clients.ErrWriteSpent) || !errors.Is(err, connect.ErrRebalancing) || errors.Is(err, ErrStepSpent) {
		t.Fatalf("Execute = %v, want the rebalance marked spent, and no second attempt", err)
	}
	if errors.Is(err, protocol.ErrDuplicateStep) || errors.Is(err, protocol.ErrParamsHash) {
		t.Fatalf("Execute = %v: a retry reached the runner", err)
	}
	if n := len(cat.executed(OpConnectCreateConnector)); n != 1 {
		t.Fatalf("the runner ran the create %d times, want 1", n)
	}
}
