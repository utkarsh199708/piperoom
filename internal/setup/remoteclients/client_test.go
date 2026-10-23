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
	write := func(ctx context.Context, s *Session) error {
		return s.Connect().CreateConnector(ctx, "pg-orders-connector", map[string]string{"connector.class": "c"})
	}
	source := func(ctx context.Context, s *Session) error {
		_, err := s.Source().Settings(ctx)
		return err
	}
	sourceWrite := func(ctx context.Context, s *Session) error {
		return s.Source().GrantSelect(ctx, "debezium", []string{"public.orders"})
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
		{"Connect.CreateConnector", stepCtxLocal, write},
		{"Source.Settings", roomCtx, source},
		{"Source.GrantSelect", stepCtxLocal, sourceWrite},
		{"Kafka.Topics", roomCtx, kafka},
		{"Registry.Subjects", roomCtx, registry},
	}
	for _, tc := range tests {
		t.Run(tc.name+" without an executor", func(t *testing.T) {
			err := tc.run(tc.ctx(t), New(nil))
			if !errors.Is(err, ErrNoRunner) || !errors.Is(err, protocol.ErrNoRunner) {
				t.Fatalf("err = %v, want ErrNoRunner (and protocol.ErrNoRunner)", err)
			}
		})
		t.Run(tc.name+" without a room id", func(t *testing.T) {
			f := newFakeRunner()
			// The executor is keyed by room; a context with no room id has no
			// runner this call could reach.
			ctx := tools.WithPipeline(context.Background(), testPipeline(t))
			if err := tc.run(ctx, New(f)); !errors.Is(err, ErrNoRunner) {
				t.Fatalf("err = %v, want ErrNoRunner", err)
			}
			if got := len(f.allCalls()); got != 0 {
				t.Fatalf("sent %d ops without a runner, want 0", got)
			}
		})
		t.Run(tc.name+" with a lost runner", func(t *testing.T) {
			f := newFakeRunner()
			f.noRunner = true
			if err := tc.run(tc.ctx(t), New(f)); !errors.Is(err, ErrNoRunner) {
				t.Fatalf("err = %v, want ErrNoRunner from the executor", err)
			}
		})
	}
}

// TestWriteNeedsProposalStep: a write without the room's proposal step, or
// with an incomplete one, refuses with ErrNoProposal before building anything.
func TestWriteNeedsProposalStep(t *testing.T) {
	writes := map[string]func(context.Context, *Session) error{
		"Connect.DeleteConnector": func(ctx context.Context, s *Session) error {
			return s.Connect().DeleteConnector(ctx, "pg-orders-connector")
		},
		"Source.SetReplicaIdentity": func(ctx context.Context, s *Session) error {
			return s.Source().SetReplicaIdentity(ctx, "public.orders", "full", "")
		},
		"Source.AlterSystem": func(ctx context.Context, s *Session) error {
			_, err := s.Source().AlterSystem(ctx, "wal_level", "logical")
			return err
		},
	}
	base := roomCtx(t)
	cases := map[string]context.Context{
		"no step":        base,
		"no proposal id": withStep(t, base, protocol.TierT2, protocol.ClassLocal, 0, "hash", ""),
		"no params hash": withStep(t, base, protocol.TierT2, protocol.ClassLocal, 0, "", "p_1"),
		"no class":       withStep(t, base, protocol.TierT2, "", 0, "hash", "p_1"),
	}
	complete := stepCtxLocal(t)
	for name, run := range writes {
		for why, ctx := range cases {
			f := newFakeRunner()
			if err := run(ctx, New(f)); !errors.Is(err, ErrNoProposal) {
				t.Fatalf("%s %s: err = %v, want ErrNoProposal", name, why, err)
			}
			if got := len(f.allCalls()); got != 0 {
				t.Fatalf("%s %s: sent %d ops, want 0", name, why, got)
			}
		}
		t.Run(name+" complete", func(t *testing.T) {
			f := newFakeRunner()
			if err := run(complete, New(f)); err != nil {
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
		name      string
		stepTier  string
		run       func(context.Context, *Session) error
		wantOp    string
		wantTier  string
		wantParam map[string]any
	}{
		{
			name: "connect create T2", stepTier: protocol.TierT2,
			run: func(ctx context.Context, s *Session) error {
				return s.Connect().CreateConnector(ctx, "pg-orders-connector", map[string]string{"connector.class": "c", "topic.prefix": "pg"})
			},
			wantOp: OpConnectCreateConnector, wantTier: protocol.TierT2,
			wantParam: map[string]any{"connector": "pg-orders-connector"},
		},
		{
			name: "source grant T2", stepTier: protocol.TierT2,
			run: func(ctx context.Context, s *Session) error {
				return s.Source().GrantSelect(ctx, "debezium", []string{"public.orders"})
			},
			wantOp: OpSourceGrantSelect, wantTier: protocol.TierT2,
			wantParam: map[string]any{"role": "debezium"},
		},
		{
			name: "a step routed T3 raises a T2 op", stepTier: protocol.TierT3,
			run: func(ctx context.Context, s *Session) error {
				return s.Source().SetReplicaIdentity(ctx, "public.orders", "full", "")
			},
			wantOp: OpSourceSetReplicaIdentity, wantTier: protocol.TierT3,
			wantParam: map[string]any{"table": "public.orders", "mode": "full"},
		},
		{
			name: "alter system is T3 even on a T2 step", stepTier: protocol.TierT2,
			run: func(ctx context.Context, s *Session) error {
				_, err := s.Source().AlterSystem(ctx, "wal_level", "logical")
				return err
			},
			wantOp: OpSourceAlterSystem, wantTier: protocol.TierT3,
			wantParam: map[string]any{"name": "wal_level", "value": "logical"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeRunner()
			ctx := stepCtx(t, tc.stepTier, protocol.ClassShared)
			if err := tc.run(ctx, New(f)); err != nil {
				t.Fatalf("call: %v", err)
			}
			calls := f.callsFor(tc.wantOp)
			if len(calls) != 1 {
				t.Fatalf("op %s sent %d times, want 1", tc.wantOp, len(calls))
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
			var params map[string]any
			if err := json.Unmarshal(req.Params, &params); err != nil {
				t.Fatalf("params: %v", err)
			}
			for k, want := range tc.wantParam {
				if got := params[k]; got != want {
					t.Fatalf("param %s = %v, want %v (params %s)", k, got, want, req.Params)
				}
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
	if _, err := New(f).Connect().Status(ctx, "pg-orders-connector"); err != nil {
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
			if _, err := New(f).Kafka().Topics(tc.ctx); err != nil {
				t.Fatalf("topics: %v", err)
			}
			if got := f.callsFor(OpKafkaTopics)[0].req.Class; got != tc.want {
				t.Fatalf("class = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestDeadlines: every op gets a deadline; the tier decides it, and
// WithTimeout overrides it.
func TestDeadlines(t *testing.T) {
	read := func(ctx context.Context, s *Session) error { _, err := s.Kafka().Topics(ctx); return err }
	write := func(ctx context.Context, s *Session) error {
		return s.Connect().CreateConnector(ctx, "pg-orders-connector", map[string]string{"connector.class": "c"})
	}
	alter := func(ctx context.Context, s *Session) error {
		_, err := s.Source().AlterSystem(ctx, "wal_level", "logical")
		return err
	}
	tests := []struct {
		name  string
		opt   Option
		ctx   func(*testing.T) context.Context
		run   func(context.Context, *Session) error
		want  time.Duration
		slack time.Duration
	}{
		{"read", nil, roomCtx, read, readTimeout, 2 * time.Second},
		{"T2 write", nil, stepCtxLocal, write, t2Timeout, 2 * time.Second},
		{"T3 write", nil, stepCtxLocal, alter, t3Timeout, 5 * time.Second},
		{"override", WithTimeout(2 * time.Second), roomCtx, read, 2 * time.Second, time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeRunner()
			before := time.Now()
			var s *Session
			if tc.opt != nil {
				s = New(f, tc.opt)
			} else {
				s = New(f)
			}
			if err := tc.run(tc.ctx(t), s); err != nil {
				t.Fatalf("call: %v", err)
			}
			req := f.allCalls()[0].req
			if req.Deadline.IsZero() {
				t.Fatal("no deadline on the request")
			}
			left := req.Deadline.Sub(before)
			if left <= 0 || left > tc.want+tc.slack {
				t.Fatalf("deadline is %s away, want about %s", left, tc.want)
			}
		})
	}
}

// TestErrorSentinels: a failed op with an error_code wraps the sentinel the Q
// actions branch on, and one without a code is a plain error.
func TestErrorSentinels(t *testing.T) {
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
			err := New(f).Connect().DeleteConnector(stepCtxLocal(t), "pg-orders-connector")
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
		err := New(f).Connect().DeleteConnector(stepCtxLocal(t), "pg-orders-connector")
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
	if _, err := New(f).Kafka().Topics(roomCtx(t)); err == nil {
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
	if _, err := New(f).Connect().ListConnectors(roomCtx(t)); !errors.Is(err, ErrBadResult) {
		t.Fatalf("err = %v, want ErrBadResult", err)
	}
}

// TestMissingResultField: a result without the field the method needs is
// ErrBadResult, never a zero value.
func TestMissingResultField(t *testing.T) {
	f := newFakeRunner()
	f.overrides[OpConnectStatus] = json.RawMessage(`{"something_else":{}}`)
	if _, err := New(f).Connect().Status(roomCtx(t), "pg-orders-connector"); !errors.Is(err, ErrBadResult) {
		t.Fatalf("err = %v, want ErrBadResult", err)
	}
}

// TestBadArgs: an empty name, a negative task or a negative age is refused
// before any op.
func TestBadArgs(t *testing.T) {
	f := newFakeRunner()
	s := New(f)
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
	s := New(f)
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
