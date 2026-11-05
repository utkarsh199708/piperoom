package remoteclients

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/tools"
)

// TestOpsTable pins the whole operation surface: every op has a name, a valid
// tier, a target kind and a default fake answer, and no method appears twice.
// Two methods may share one op (Blockers and BlockersOnTables do: the scoped
// read adds the tables param), so the op names need not be unique; the methods
// do, or a spec was copied.
func TestOpsTable(t *testing.T) {
	methods := map[string]bool{}
	ops := map[string]bool{}
	for _, spec := range Ops() {
		if spec.Op == "" || spec.Method == "" || spec.Target == "" {
			t.Fatalf("incomplete spec %+v", spec)
		}
		if methods[spec.Method] {
			t.Fatalf("method %q has two specs", spec.Method)
		}
		methods[spec.Method] = true
		ops[spec.Op] = true
		switch spec.Tier {
		case protocol.TierT0, protocol.TierT2, protocol.TierT3:
		default:
			t.Fatalf("%s: tier %q is not one this package sends", spec.Method, spec.Tier)
		}
		if _, ok := defaultAnswers[spec.Op]; !ok {
			t.Fatalf("%s: the fake runner has no answer for %q", spec.Method, spec.Op)
		}
		// The room bounds a step by the tier this package sends it at, so
		// an op sent above T2 must be in plan's client floor table.
		if spec.Tier == protocol.TierT3 && plan.ClientFloor(spec.Op) != protocol.TierT3 {
			t.Fatalf("%s is sent T3 but plan.ClientFloor(%s) = %q: the room would cut its confirmation short", spec.Method, spec.Op, plan.ClientFloor(spec.Op))
		}
		if spec.Tier != protocol.TierT3 && plan.ClientFloor(spec.Op) == protocol.TierT3 {
			t.Fatalf("plan.ClientFloor(%s) is T3 but %s sends it at %s", spec.Op, spec.Method, spec.Tier)
		}
	}
	if len(ops) != len(defaultAnswers) {
		t.Fatalf("Ops() names %d distinct ops, the fake answers %d", len(ops), len(defaultAnswers))
	}
}

// TestNoRunner: a client without a paired runner answers ErrNoRunner before it
// builds or sends anything, whatever the method.
func TestNoRunner(t *testing.T) {
	call := func(ctx context.Context, s *Session) error {
		_, err := s.Connect().Status(ctx, "pg-orders-connector")
		return err
	}
	create := writeCaseFor(t, "Connect.CreateConnector")
	grant := writeCaseFor(t, "Source.GrantSelect")
	source := func(ctx context.Context, s *Session) error {
		_, err := s.Source().Settings(ctx)
		return err
	}
	kafka := func(ctx context.Context, s *Session) error {
		_, err := s.Kafka().Topics(ctx)
		return err
	}
	registry := func(ctx context.Context, s *Session) error {
		_, err := s.Registry().Subjects(ctx)
		return err
	}
	tests := []struct {
		name string
		ctx  func(*testing.T) context.Context
		run  func(context.Context, *Session) error
	}{
		{"Connect.Status", roomCtx, call},
		{"Connect.CreateConnector", create.ctx, create.run},
		{"Source.Settings", roomCtx, source},
		{"Source.GrantSelect", grant.ctx, grant.run},
		{"Kafka.Topics", roomCtx, kafka},
		{"Registry.Subjects", roomCtx, registry},
	}
	for _, tc := range tests {
		t.Run(tc.name+" without an executor", func(t *testing.T) {
			err := tc.run(tc.ctx(t), New(nil, ""))
			if !errors.Is(err, ErrNoRunner) || !errors.Is(err, protocol.ErrNoRunner) {
				t.Fatalf("err = %v, want ErrNoRunner (and protocol.ErrNoRunner)", err)
			}
		})
		t.Run(tc.name+" without a room id", func(t *testing.T) {
			f := newFakeRunner()
			// The executor is keyed by room; a session bound to no room and a
			// context with no room id has no runner this call could reach.
			ctx := tools.WithPipeline(context.Background(), testPipeline(t))
			if err := tc.run(ctx, New(f, "")); !errors.Is(err, ErrNoRunner) {
				t.Fatalf("err = %v, want ErrNoRunner", err)
			}
			if got := len(f.allCalls()); got != 0 {
				t.Fatalf("sent %d ops without a runner, want 0", got)
			}
		})
		t.Run(tc.name+" with a lost runner", func(t *testing.T) {
			f := newFakeRunner()
			f.noRunner = true
			if err := tc.run(tc.ctx(t), New(f, "r_setup")); !errors.Is(err, ErrNoRunner) {
				t.Fatalf("err = %v, want ErrNoRunner from the executor", err)
			}
		})
	}
}

// TestWriteNeedsProposalStep: a write without the room's proposal step, or
// with an incomplete one, refuses with ErrNoProposal before building anything.
func TestWriteNeedsProposalStep(t *testing.T) {
	base := roomCtx(t)
	cases := map[string]context.Context{
		"no step":        base,
		"no proposal id": withStep(t, base, protocol.TierT2, protocol.ClassLocal, 0, "hash", ""),
		"no params hash": withStep(t, base, protocol.TierT2, protocol.ClassLocal, 0, "", "p_1"),
		"no class":       withStep(t, base, protocol.TierT2, "", 0, "hash", "p_1"),
	}
	for _, w := range writeCases() {
		for why, ctx := range cases {
			f := newFakeRunner()
			if err := w.run(ctx, New(f, "r_setup")); !errors.Is(err, ErrNoProposal) {
				t.Fatalf("%s %s: err = %v, want ErrNoProposal", w.method, why, err)
			}
			if got := len(f.allCalls()); got != 0 {
				t.Fatalf("%s %s: sent %d ops, want 0", w.method, why, got)
			}
		}
		// The proposal fields without the frozen step: the write cannot
		// know the params the runner binds it to.
		f := newFakeRunner()
		if err := w.run(stepCtxLocal(t), New(f, "r_setup")); !errors.Is(err, ErrStepMismatch) {
			t.Fatalf("%s without the frozen step: err = %v, want ErrStepMismatch", w.method, err)
		}
		if got := len(f.allCalls()); got != 0 {
			t.Fatalf("%s without the frozen step: sent %d ops, want 0", w.method, got)
		}
		t.Run(w.method+" complete", func(t *testing.T) {
			f := newFakeRunner()
			if err := w.run(w.ctx(t), New(f, "r_setup", allowT3Steps())); err != nil {
				t.Fatalf("a complete step was refused: %v", err)
			}
			if got := len(f.allCalls()); got != 1 {
				t.Fatalf("sent %d ops, want 1", got)
			}
		})
	}
}

// TestWriteCarriesProposalFields: a writer sends the proposal fields, the
// class and the raised tier, and the frozen proposal when the context has it.
func TestWriteCarriesProposalFields(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		stepTier string
		wantTier string
	}{
		{name: "connect create T2", method: "Connect.CreateConnector", stepTier: protocol.TierT2, wantTier: protocol.TierT2},
		{name: "source grant T2", method: "Source.GrantSelect", stepTier: protocol.TierT2, wantTier: protocol.TierT2},
		{name: "a step routed T3 raises a T2 op", method: "Source.SetReplicaIdentity", stepTier: protocol.TierT3, wantTier: protocol.TierT3},
		{name: "alter system is T3 even on a T2 step", method: "Source.AlterSystem", stepTier: protocol.TierT2, wantTier: protocol.TierT3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := writeCaseFor(t, tc.method)
			f := newFakeRunner()
			ctx := writeCtx(t, tc.stepTier, protocol.ClassShared, w.action, w.params)
			if err := w.run(ctx, New(f, "r_setup", allowT3Steps())); err != nil {
				t.Fatalf("call: %v", err)
			}
			calls := f.callsFor(w.action)
			if len(calls) != 1 {
				t.Fatalf("op %s sent %d times, want 1", w.action, len(calls))
			}
			req := calls[0].req
			if req.ProposalID != "p_1" || req.StepIndex == nil || *req.StepIndex != 0 || req.ParamsHash != "b1946ac92492d2347c6235b4d2611184" {
				t.Fatalf("proposal fields = %q %v %q, want p_1 0 b1946...", req.ProposalID, req.StepIndex, req.ParamsHash)
			}
			if req.Tier != tc.wantTier {
				t.Fatalf("tier = %q, want %q", req.Tier, tc.wantTier)
			}
			if req.Class != protocol.ClassShared {
				t.Fatalf("class = %q, want %q", req.Class, protocol.ClassShared)
			}
			if len(req.Proposal) == 0 || !json.Valid(req.Proposal) {
				t.Fatalf("the frozen proposal was not carried: %q", req.Proposal)
			}
			if req.Deadline.IsZero() {
				t.Fatal("the request has no deadline")
			}
			if string(req.Params) != w.params {
				t.Fatalf("params = %s, want the frozen step's %s", req.Params, w.params)
			}
		})
	}
}

// TestReadCarriesNoProposalFields: a T0 read never carries proposal fields
// (protocol.OpRequest: nil on a T0 request), even when the call's context is
// an action's step context.
func TestReadCarriesNoProposalFields(t *testing.T) {
	f := newFakeRunner()
	ctx := stepCtx(t, protocol.TierT2, protocol.ClassProduction)
	if _, err := New(f, "r_setup").Connect().Status(ctx, "pg-orders-connector"); err != nil {
		t.Fatalf("status: %v", err)
	}
	req := f.callsFor(OpConnectStatus)[0].req
	if req.Tier != protocol.TierT0 {
		t.Fatalf("tier = %q, want T0", req.Tier)
	}
	if req.ProposalID != "" || req.StepIndex != nil || req.ParamsHash != "" || len(req.Proposal) != 0 {
		t.Fatalf("a T0 read carried proposal fields: %+v", req)
	}
	if req.Class != protocol.ClassProduction {
		t.Fatalf("class = %q, want the step's class", req.Class)
	}
}

// TestReadClass: a read takes the class from WithClass, and defaults to local
// (the lowest; the runner raises it to its own floor) when it knows none.
func TestReadClass(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{"default", roomCtx(t), protocol.ClassLocal},
		{"explicit", WithClass(roomCtx(t), protocol.ClassShared), protocol.ClassShared},
		{"unknown value is ignored", WithClass(roomCtx(t), "prod"), protocol.ClassLocal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeRunner()
			if _, err := New(f, "r_setup").Kafka().Topics(tc.ctx); err != nil {
				t.Fatalf("topics: %v", err)
			}
			if got := f.callsFor(OpKafkaTopics)[0].req.Class; got != tc.want {
				t.Fatalf("class = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestDeadlines: every op gets a deadline; the tier decides it, and
// WithTimeout overrides it. The wanted values are the documented ones, not the
// constants, so a constant cut short (a T3 at 30 s) fails here.
func TestDeadlines(t *testing.T) {
	read := func(ctx context.Context, s *Session) error { _, err := s.Kafka().Topics(ctx); return err }
	create := writeCaseFor(t, "Connect.CreateConnector")
	alter := writeCaseFor(t, "Source.AlterSystem")
	createAt := func(tier string) func(*testing.T) context.Context {
		return func(t *testing.T) context.Context {
			return writeCtx(t, tier, protocol.ClassLocal, create.action, create.params)
		}
	}
	tests := []struct {
		name  string
		opt   Option
		ctx   func(*testing.T) context.Context
		run   func(context.Context, *Session) error
		want  time.Duration
		slack time.Duration
	}{
		{"read", nil, roomCtx, read, 10 * time.Second, 2 * time.Second},
		{"T2 write", nil, createAt(protocol.TierT2), create.run, 30 * time.Second, 2 * time.Second},
		{"T2 write raised to T3", nil, createAt(protocol.TierT3), create.run, 5 * time.Minute, 5 * time.Second},
		{"T3 write", nil, func(t *testing.T) context.Context {
			return writeCtx(t, protocol.TierT2, protocol.ClassLocal, alter.action, alter.params)
		}, alter.run, 5 * time.Minute, 5 * time.Second},
		{"override", WithTimeout(2 * time.Second), roomCtx, read, 2 * time.Second, time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeRunner()
			before := time.Now()
			var s *Session
			if tc.opt != nil {
				s = New(f, "r_setup", tc.opt, allowT3Steps())
			} else {
				s = New(f, "r_setup", allowT3Steps())
			}
			if err := tc.run(tc.ctx(t), s); err != nil {
				t.Fatalf("call: %v", err)
			}
			req := f.allCalls()[0].req
			if req.Deadline.IsZero() {
				t.Fatal("no deadline on the request")
			}
			// Both bounds: a deadline far past the tier's leaks an op, and
			// one short of it (a T3 cut to the T2 30 s) takes away a
			// person's time to confirm.
			left := req.Deadline.Sub(before)
			if left < tc.want-tc.slack || left > tc.want+tc.slack {
				t.Fatalf("deadline is %s away, want %s (±%s)", left, tc.want, tc.slack)
			}
		})
	}
}

// TestErrorSentinels: a failed op with an error_code wraps the sentinel the Q
// actions branch on, and one without a code is a plain error.
func TestErrorSentinels(t *testing.T) {
	del := writeCaseFor(t, "Connect.DeleteConnector")
	tests := []struct {
		code string
		want error
	}{
		{ErrorCodeNotFound, clients.ErrNotFound},
		{ErrorCodeNotSupported, clients.ErrNotSupported},
		{ErrorCodeConnectorExists, connect.ErrConnectorExists},
		{ErrorCodeConnectorNotStopped, connect.ErrConnectorNotStopped},
		{ErrorCodeRebalancing, connect.ErrRebalancing},
		{ErrorCodeSessionChanged, postgres.ErrSessionChanged},
		{ErrorCodeSessionProtected, postgres.ErrSessionProtected},
		{ErrorCodeOutOfScope, tools.ErrOutOfScope},
	}
	for _, tc := range tests {
		t.Run(tc.code, func(t *testing.T) {
			f := newFakeRunner()
			f.failures[OpConnectDeleteConnector] = fakeFailure{summary: "the runner refused: " + tc.code, code: tc.code}
			err := del.run(del.ctx(t), New(f, "r_setup"))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if !strings.Contains(err.Error(), "the runner refused") {
				t.Fatalf("err %q does not carry the runner's summary", err)
			}
		})
	}
	t.Run("no code", func(t *testing.T) {
		f := newFakeRunner()
		f.failures[OpConnectDeleteConnector] = fakeFailure{summary: "connect says no"}
		err := del.run(del.ctx(t), New(f, "r_setup"))
		if err == nil || !strings.Contains(err.Error(), "connect says no") {
			t.Fatalf("err = %v, want the summary", err)
		}
		for _, sentinel := range []error{clients.ErrNotFound, connect.ErrConnectorExists, tools.ErrOutOfScope} {
			if errors.Is(err, sentinel) {
				t.Fatalf("err = %v matched %v without an error_code", err, sentinel)
			}
		}
	})
}

// TestLongSummaryIsClipped: a hostile or sloppy result cannot put an unbounded
// string into an error.
func TestLongSummaryIsClipped(t *testing.T) {
	f := newFakeRunner()
	f.failures[OpKafkaTopics] = fakeFailure{summary: strings.Repeat("A", 4000)}
	if _, err := New(f, "r_setup").Kafka().Topics(roomCtx(t)); err == nil {
		t.Fatal("a failed op returned no error")
	} else {
		if got := len(err.Error()); got > maxErrorText+100 {
			t.Fatalf("error is %d bytes, want at most about %d", got, maxErrorText)
		}
		if !strings.HasSuffix(err.Error(), "…") {
			t.Fatalf("error %q was not marked cut", err.Error())
		}
	}
}

// TestDataCap: a result past the protocol cap is refused, never decoded.
func TestDataCap(t *testing.T) {
	f := newFakeRunner()
	f.overrides[OpConnectListConnectors] = json.RawMessage(`{"connectors":["` + strings.Repeat("a", protocol.MaxDataBytes) + `"]}`)
	if _, err := New(f, "r_setup").Connect().ListConnectors(roomCtx(t)); !errors.Is(err, ErrBadResult) {
		t.Fatalf("err = %v, want ErrBadResult", err)
	}
}

// TestMissingResultField: a result without the field the method needs is
// ErrBadResult, never a zero value.
func TestMissingResultField(t *testing.T) {
	f := newFakeRunner()
	f.overrides[OpConnectStatus] = json.RawMessage(`{"something_else":{}}`)
	if _, err := New(f, "r_setup").Connect().Status(roomCtx(t), "pg-orders-connector"); !errors.Is(err, ErrBadResult) {
		t.Fatalf("err = %v, want ErrBadResult", err)
	}
}

// TestBadResultJSON: a result whose Data is not a JSON object is ErrBadResult,
// never a zero value.
func TestBadResultJSON(t *testing.T) {
	f := newFakeRunner()
	f.overrides[OpKafkaTopics] = json.RawMessage(`{"topics":`)
	if _, err := New(f, "r_setup").Kafka().Topics(roomCtx(t)); !errors.Is(err, ErrBadResult) {
		t.Fatalf("err = %v, want ErrBadResult", err)
	}
}

// TestExecutorErrorIsWrapped: a runner that fails (a dropped socket, a
// cancelled op) surfaces the executor's error, wrapped with the op name and
// still matchable with errors.Is.
func TestExecutorErrorIsWrapped(t *testing.T) {
	f := newFakeRunner()
	f.err = errors.New("runner socket closed")
	_, err := New(f, "r_setup").Kafka().Topics(roomCtx(t))
	if !errors.Is(err, f.err) {
		t.Fatalf("err = %v, want the executor's error", err)
	}
	if !strings.Contains(err.Error(), OpKafkaTopics) {
		t.Fatalf("err %q does not name the op", err)
	}
}

// TestBadArgs: an empty name, a negative task or a negative age is refused
// before any op.
func TestBadArgs(t *testing.T) {
	f := newFakeRunner()
	s := New(f, "r_setup")
	ctx := stepCtxLocal(t)
	read := roomCtx(t)
	tests := map[string]error{
		"empty connector":   firstErr(s.Connect().Status(read, "")),
		"negative task":     s.Connect().RestartTask(ctx, "pg-orders-connector", -1),
		"empty config":      s.Connect().CreateConnector(ctx, "pg-orders-connector", nil),
		"no offsets":        s.Connect().AlterSourceOffsets(ctx, "pg-orders-connector", nil),
		"empty role":        s.Source().GrantSelect(ctx, "", []string{"public.orders"}),
		"negative age":      firstErr(s.Source().Blockers(read, -time.Second)),
		"bad task id":       s.Source().EndSession(ctx, 0, time.Now()),
		"empty publication": s.Source().CreatePublication(ctx, "", []string{"public.orders"}),
	}
	for name, err := range tests {
		if !errors.Is(err, ErrBadArgs) {
			t.Fatalf("%s: err = %v, want ErrBadArgs", name, err)
		}
	}
	if got := len(f.allCalls()); got != 0 {
		t.Fatalf("sent %d ops for bad args, want 0", got)
	}
}

// TestUniqueOpIDs: the runner keys its duplicate-step ledger by the request
// id, so two calls must never share one.
func TestUniqueOpIDs(t *testing.T) {
	f := newFakeRunner()
	ctx := roomCtx(t)
	s := New(f, "r_setup")
	for i := 0; i < 3; i++ {
		if _, err := s.Kafka().Topics(ctx); err != nil {
			t.Fatalf("topics: %v", err)
		}
	}
	ids := map[string]bool{}
	for _, c := range f.allCalls() {
		if c.req.ID == "" {
			t.Fatal("a request has no id")
		}
		if ids[c.req.ID] {
			t.Fatalf("duplicate op id %q", c.req.ID)
		}
		ids[c.req.ID] = true
		if c.roomID != "r_setup" {
			t.Fatalf("room id = %q, want r_setup", c.roomID)
		}
	}
}

// firstErr drops a value and keeps the error.
func firstErr[T any](_ T, err error) error { return err }
