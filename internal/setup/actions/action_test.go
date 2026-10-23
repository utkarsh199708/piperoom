package setupactions

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/setup/catalog"
	"piperoom/internal/setup/forward"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/remoteclients"
)

type fakeRunner struct {
	mu   sync.Mutex
	reqs []protocol.OpRequest
	res  protocol.OpResult
	err  error
	exec func(ctx context.Context, req protocol.OpRequest) (protocol.OpResult, error)
}

func (f *fakeRunner) Execute(ctx context.Context, _ string, req protocol.OpRequest) (protocol.OpResult, error) {
	f.mu.Lock()
	f.reqs = append(f.reqs, req)
	f.mu.Unlock()
	if f.exec != nil {
		return f.exec(ctx, req)
	}
	res := f.res
	res.ID = req.ID
	return res, f.err
}

func (f *fakeRunner) requests() []protocol.OpRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]protocol.OpRequest(nil), f.reqs...)
}

func adapterFor(t *testing.T, cfg Config, name string) actions.Action {
	t.Helper()
	as, err := Actions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range as {
		if a.Name() == name {
			return a
		}
	}
	t.Fatalf("no adapter %s", name)
	return nil
}

func TestAdapters(t *testing.T) {
	reg := actions.NewRegistry()
	if err := Register(reg, Config{Executor: &fakeRunner{}, RoomID: "r_1"}); err != nil {
		t.Fatal(err)
	}
	cat, _ := catalog.Builtin()
	var names []string
	for _, a := range reg.All() {
		names = append(names, a.Name())
		if _, ok := a.(actions.Scoped); !ok {
			t.Errorf("%s is not Scoped", a.Name())
		}
		if _, ok := a.(actions.Tiered); !ok {
			t.Errorf("%s is not Tiered", a.Name())
		}
		if _, ok := a.(actions.Preview); !ok {
			t.Errorf("%s has no Preview", a.Name())
		}
		if _, ok := a.(actions.Receipted); !ok {
			t.Errorf("%s is not Receipted", a.Name())
		}
		if a.ParamsSchema()["type"] != "object" {
			t.Errorf("%s schema = %v", a.Name(), a.ParamsSchema())
		}
	}
	for _, s := range cat.Specs() {
		has := slices.Contains(names, s.Name)
		switch {
		case s.Tier == plan.TierT0 && has:
			t.Errorf("the T0 op %s is an action: a read is a tool, never a proposal", s.Name)
		case s.Tier == plan.TierT3 && has:
			t.Errorf("the T3 op %s is an action while T3 is off on the remote runner", s.Name)
		case (s.Tier == plan.TierT1 || s.Tier == plan.TierT2) && !has:
			t.Errorf("the setup op %s has no adapter", s.Name)
		}
	}
	if PreviewOp != ops.OpPreview {
		t.Fatalf("PreviewOp %q is not the runner's %q", PreviewOp, ops.OpPreview)
	}
}

func TestTier(t *testing.T) {
	cfg := Config{Executor: &fakeRunner{}, RoomID: "r_1"}
	for _, tc := range []struct{ op, class, want string }{
		{"file_write", protocol.ClassLocal, plan.TierT1},
		{"file_write", protocol.ClassShared, plan.TierT2},
		{"compose_up", protocol.ClassProduction, plan.TierT3},
		{"source_create_publication", protocol.ClassLocal, plan.TierT2},
		{"source_create_publication", protocol.ClassProduction, plan.TierT3},
	} {
		got, err := adapterFor(t, cfg, tc.op).(actions.Tiered).Tier(nil, tc.class)
		if err != nil || got != tc.want {
			t.Errorf("Tier(%s, %s) = %q, %v; want %s", tc.op, tc.class, got, err, tc.want)
		}
	}
	for _, class := range []string{"", "dev", "LOCAL"} {
		if got, err := adapterFor(t, cfg, "file_write").(actions.Tiered).Tier(nil, class); err == nil {
			t.Errorf("Tier with class %q = %q, want an error", class, got)
		}
	}
}

func TestValidate(t *testing.T) {
	a := adapterFor(t, Config{}, "source_create_publication")
	ctx := context.Background()
	if err := a.Validate(ctx, json.RawMessage(`{"publication":"dbz","tables":["public.orders"]}`)); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{
		`{"publication":"dbz"}`,
		`{"publication":"dbz","tables":["public.orders"],"op":"source_alter_system"}`,
		`{"publication":"DROP TABLE x","tables":["public.orders"]}`,
		`{"publication":"dbz","tables":["public.orders"],"Tables":["public.x"]}`,
	} {
		if err := a.Validate(ctx, json.RawMessage(p)); !errors.Is(err, catalog.ErrParams) {
			t.Errorf("Validate(%s) = %v", p, err)
		}
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := a.Validate(cctx, json.RawMessage(`{"publication":"dbz","tables":["public.orders"]}`)); !errors.Is(err, context.Canceled) {
		t.Errorf("Validate after cancel = %v", err)
	}
}

const writeParams = `{"path":"notes.txt","content":"hello\n"}`

func TestPreviewCallsOnlyThePreviewOp(t *testing.T) {
	preview := events.PreviewResult{Target: "notes.txt", Current: "absent", Expected: "+ hello", Reversible: true, Rollback: "file_delete"}
	data, _ := json.Marshal(map[string]any{"op": "file_write", "preview": preview})
	f := &fakeRunner{res: protocol.OpResult{OK: true, Summary: "preview", Data: data}}
	a := adapterFor(t, Config{Executor: f}, "file_write").(actions.Preview)

	got, err := a.Preview(room.WithRoomID(context.Background(), "r_1"), json.RawMessage(writeParams))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, preview) {
		t.Fatalf("preview = %+v", got)
	}
	reqs := f.requests()
	if len(reqs) != 1 {
		t.Fatalf("sent %d requests, want 1", len(reqs))
	}
	req := reqs[0]
	if req.Op != PreviewOp || req.Tier != protocol.TierT0 || req.ProposalID != "" || req.StepIndex != nil || req.ParamsHash != "" || len(req.Proposal) != 0 {
		t.Fatalf("request = %+v", req)
	}
	var inner struct {
		Op     string          `json:"op"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(req.Params, &inner); err != nil || inner.Op != "file_write" || string(inner.Params) != writeParams {
		t.Fatalf("preview params = %s", req.Params)
	}
}

func TestPreviewErrors(t *testing.T) {
	ctx := room.WithRoomID(context.Background(), "r_1")
	for _, tc := range []struct {
		name   string
		f      *fakeRunner
		params string
		want   error
		noSend bool
	}{
		{"bad params", &fakeRunner{}, `{"path":"a"}`, catalog.ErrParams, true},
		{"no preview in the result", &fakeRunner{res: protocol.OpResult{OK: true, Data: json.RawMessage(`{"op":"file_write"}`)}}, writeParams, ErrBadPreview, false},
		{"cut result", &fakeRunner{res: protocol.OpResult{OK: true, Truncated: true, Data: json.RawMessage(`{"truncated":true}`)}}, writeParams, ErrBadPreview, false},
		{"not supported", &fakeRunner{res: protocol.OpResult{OK: false, Summary: "no preview", Data: json.RawMessage(`{"error_code":"not_supported"}`)}}, writeParams, clients.ErrNotSupported, false},
		{"refused", &fakeRunner{err: &protocol.RefusedError{Refusal: protocol.Refusal{Op: PreviewOp, Reason: protocol.RefuseUnknownOp}}}, writeParams, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := adapterFor(t, Config{Executor: tc.f}, "file_write").(actions.Preview).Preview(ctx, json.RawMessage(tc.params))
			if err == nil {
				t.Fatal("no error")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			var refused *protocol.RefusedError
			if tc.want == nil && !errors.As(err, &refused) {
				t.Fatalf("err = %v, want the runner's refusal", err)
			}
			if tc.noSend && len(tc.f.requests()) != 0 {
				t.Fatal("sent a request for bad params")
			}
		})
	}
	if _, err := adapterFor(t, Config{}, "file_write").(actions.Preview).Preview(ctx, json.RawMessage(writeParams)); !errors.Is(err, protocol.ErrNoRunner) {
		t.Fatalf("no runner: %v", err)
	}
	if _, err := adapterFor(t, Config{Executor: &fakeRunner{}}, "file_write").(actions.Preview).Preview(context.Background(), json.RawMessage(writeParams)); !errors.Is(err, protocol.ErrNoRunner) {
		t.Fatalf("no room: %v", err)
	}
}

// Against the real runner catalogue: the preview op exists there, refuses a
// write's bad params, and never writes. file_write has no Preview yet, so the
// answer is "not supported" and nothing is on disk.
func TestPreviewAgainstRunnerCatalogue(t *testing.T) {
	dir := t.TempDir()
	r, err := ops.New(ops.Config{ProjectDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	f := &fakeRunner{exec: func(ctx context.Context, req protocol.OpRequest) (protocol.OpResult, error) {
		res, _ := r.Execute(ctx, req)
		return res, nil
	}}
	a := adapterFor(t, Config{Executor: f, RoomID: "r_1"}, "file_write").(actions.Preview)
	_, err = a.Preview(context.Background(), json.RawMessage(writeParams))
	if !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("preview of file_write = %v, want not supported", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "notes.txt")); !os.IsNotExist(err) {
		t.Fatalf("the preview wrote the file: %v", err)
	}
	// The runner's own Validate still runs through the preview.
	_, err = a.Preview(context.Background(), json.RawMessage(`{"path":"run.sh","content":"x"}`))
	if !errors.Is(err, forward.ErrOpFailed) || errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("preview of an executed name = %v", err)
	}
}

func stepCtx(roomID string, st room.SetupStep) context.Context {
	return room.WithSetupStep(room.WithRoomID(context.Background(), roomID), st)
}

func goodStep() room.SetupStep {
	return room.SetupStep{
		ProposalID: "p_1", StepIndex: 2, ParamsHash: "h_1", Tier: plan.TierT1, Class: protocol.ClassLocal,
		Action: "file_write", Params: json.RawMessage(writeParams),
	}
}

func receiptResult() protocol.OpResult {
	return protocol.OpResult{OK: true, Summary: "wrote notes.txt (6 bytes)",
		Receipt: events.SetupReceipt{Kind: "file", Target: "notes.txt", At: time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC), TookMS: 3, OutMAC: "mac_1"}}
}

type fakeProposals struct {
	raw json.RawMessage
	err error
	ids []string
}

func (f *fakeProposals) FrozenProposal(_ context.Context, id string) (json.RawMessage, error) {
	f.ids = append(f.ids, id)
	return f.raw, f.err
}

func TestExecuteReceiptReturnsTheRunnersReceipt(t *testing.T) {
	f := &fakeRunner{res: receiptResult()}
	props := &fakeProposals{raw: json.RawMessage(`{"proposal_id":"p_1"}`)}
	a := adapterFor(t, Config{Executor: f, Proposals: props}, "file_write").(actions.Receipted)

	out, receipt, err := a.ExecuteReceipt(stepCtx("r_1", goodStep()), json.RawMessage(writeParams))
	if err != nil {
		t.Fatal(err)
	}
	if out != "wrote notes.txt (6 bytes)" || !reflect.DeepEqual(receipt, receiptResult().Receipt) {
		t.Fatalf("out %q receipt %+v", out, receipt)
	}
	reqs := f.requests()
	if len(reqs) != 1 {
		t.Fatalf("sent %d requests", len(reqs))
	}
	req := reqs[0]
	if req.Op != "file_write" || string(req.Params) != writeParams || req.Tier != plan.TierT1 || req.Class != protocol.ClassLocal ||
		req.ProposalID != "p_1" || req.StepIndex == nil || *req.StepIndex != 2 || req.ParamsHash != "h_1" ||
		string(req.Proposal) != `{"proposal_id":"p_1"}` || req.Deadline.IsZero() {
		t.Fatalf("request = %+v", req)
	}
	if len(props.ids) != 1 || props.ids[0] != "p_1" {
		t.Fatalf("proposal lookups = %v", props.ids)
	}

	// The proposal carried in the context wins over the lookup; the room's
	// routed tier is never lowered; Execute returns the same output.
	f2 := &fakeRunner{res: receiptResult()}
	props2 := &fakeProposals{}
	a2 := adapterFor(t, Config{Executor: f2, Proposals: props2}, "file_write")
	st := goodStep()
	st.Tier = plan.TierT2
	ctx := remoteclients.WithProposal(stepCtx("r_1", st), json.RawMessage(`{"proposal_id":"p_1","from":"ctx"}`))
	out2, err := a2.Execute(ctx, json.RawMessage(writeParams))
	if err != nil || out2 != out {
		t.Fatalf("Execute = %q, %v", out2, err)
	}
	if got := f2.requests()[0]; got.Tier != plan.TierT2 || string(got.Proposal) != `{"proposal_id":"p_1","from":"ctx"}` || len(props2.ids) != 0 {
		t.Fatalf("request = %+v, lookups %v", got, props2.ids)
	}
}

func TestExecuteReceiptErrors(t *testing.T) {
	refusal := &protocol.RefusedError{Refusal: protocol.Refusal{Op: "file_write", Reason: protocol.RefuseParamsHash}}
	failedRes := receiptResult()
	failedRes.OK = false
	failedRes.Summary = "refused file_write: file is executed or loaded"
	noReceipt := receiptResult()
	noReceipt.Receipt = events.SetupReceipt{}
	for _, tc := range []struct {
		name        string
		cfg         func(*fakeRunner) Config
		step        func(*room.SetupStep)
		noStep      bool
		params      string
		want        error
		noSend      bool
		res         protocol.OpResult
		err         error
		wantReceipt bool
	}{
		{name: "no step in the context", noStep: true, want: ErrNoProposal, noSend: true},
		{name: "step without a proposal id", step: func(s *room.SetupStep) { s.ProposalID = "" }, want: ErrNoProposal, noSend: true},
		{name: "step without a params hash", step: func(s *room.SetupStep) { s.ParamsHash = "" }, want: ErrNoProposal, noSend: true},
		{name: "step without a class", step: func(s *room.SetupStep) { s.Class = "" }, want: ErrNoProposal, noSend: true},
		{name: "step runs another action", step: func(s *room.SetupStep) { s.Action = "file_delete" }, want: ErrStepMismatch, noSend: true},
		{name: "params differ from the step", params: `{"path":"other.txt","content":"x"}`, want: ErrStepMismatch, noSend: true},
		{name: "step params fail the schema", step: func(s *room.SetupStep) { s.Params = json.RawMessage(`{"path":"a"}`) }, params: `{"path":"a"}`, want: catalog.ErrParams, noSend: true},
		{name: "production raises it to T3", step: func(s *room.SetupStep) { s.Class = protocol.ClassProduction }, want: ErrT3Disabled, noSend: true},
		{name: "the room routed it T3", step: func(s *room.SetupStep) { s.Tier = plan.TierT3 }, want: ErrT3Disabled, noSend: true},
		{name: "no runner", cfg: func(*fakeRunner) Config { return Config{} }, want: protocol.ErrNoRunner, noSend: true},
		{name: "proposal lookup fails", cfg: func(f *fakeRunner) Config {
			return Config{Executor: f, Proposals: &fakeProposals{err: errors.New("store down")}}
		}, want: nil, noSend: true},
		{name: "runner refused the hash", err: refusal, want: protocol.ErrParamsHash},
		{name: "runner lost", err: protocol.ErrNoRunner, want: protocol.ErrNoRunner},
		{name: "op failed", res: failedRes, want: forward.ErrOpFailed, wantReceipt: true},
		{name: "receipt missing", res: noReceipt, want: ErrNoReceipt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeRunner{res: tc.res, err: tc.err}
			cfg := Config{Executor: f}
			if tc.cfg != nil {
				cfg = tc.cfg(f)
			}
			st := goodStep()
			if tc.step != nil {
				tc.step(&st)
			}
			ctx := room.WithRoomID(context.Background(), "r_1")
			if !tc.noStep {
				ctx = room.WithSetupStep(ctx, st)
			}
			params := tc.params
			if params == "" {
				params = writeParams
			}
			_, receipt, err := adapterFor(t, cfg, "file_write").(actions.Receipted).ExecuteReceipt(ctx, json.RawMessage(params))
			if err == nil {
				t.Fatal("no error")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if tc.noSend && len(f.requests()) != 0 {
				t.Fatalf("sent %d requests for a call that must send none", len(f.requests()))
			}
			if tc.wantReceipt && receipt.OutMAC == "" {
				t.Fatal("a failed step lost the runner's receipt")
			}
		})
	}
}
