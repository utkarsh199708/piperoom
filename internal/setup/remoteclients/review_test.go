package remoteclients

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients/connect"
	"piperoom/internal/room"
	"piperoom/internal/setup/protocol"
)

// TestBlockersScopedEvenWithoutTables: BlockersOnTables tells the runner it is
// scoped even with no tables (a pipeline without source_tables), so its
// params never equal the unscoped Blockers read, which shows every identity.
func TestBlockersScopedEvenWithoutTables(t *testing.T) {
	f := newFakeRunner()
	src := New(f, "r_setup").Source()
	if _, err := src.Blockers(roomCtx(t), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := src.BlockersOnTables(roomCtx(t), time.Minute, nil); err != nil {
		t.Fatal(err)
	}
	calls := f.callsFor(OpSourceBlockers)
	var unscoped, scoped map[string]any
	if err := json.Unmarshal(calls[0].req.Params, &unscoped); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(calls[1].req.Params, &scoped); err != nil {
		t.Fatal(err)
	}
	if _, ok := unscoped["scoped"]; ok {
		t.Fatalf("Blockers params = %s, want no scoped", calls[0].req.Params)
	}
	if scoped["scoped"] != true {
		t.Fatalf("BlockersOnTables(nil) params = %s, want scoped true", calls[1].req.Params)
	}
}

// TestBlockersOnTablesBlanksOutOfScope: a row the runner marks out of scope
// never carries user, application or query, whatever the runner sent.
func TestBlockersOnTablesBlanksOutOfScope(t *testing.T) {
	f := newFakeRunner()
	f.overrides[OpSourceBlockers] = json.RawMessage(`{"sessions":[` +
		`{"pid":1,"user":"alice","application":"billing","query":"SELECT 1","in_scope":true},` +
		`{"pid":2,"user":"bob","application":"payroll","query":"UPDATE pay","in_scope":false}]}`)
	got, err := New(f, "r_setup").Source().BlockersOnTables(roomCtx(t), time.Minute, []string{"public.orders"})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].User != "alice" || got[0].App != "billing" || got[0].Query != "SELECT 1" {
		t.Fatalf("in-scope row = %+v, want its identity", got[0])
	}
	if got[1].User != "" || got[1].App != "" || got[1].Query != "" {
		t.Fatalf("out-of-scope row = %+v, want identity blanked", got[1])
	}
}

// TestBoundRoomWins: a session bound to one room refuses a call whose context
// names another room, before anything is sent; a session wired without a
// room uses the context's.
func TestBoundRoomWins(t *testing.T) {
	f := newFakeRunner()
	ctx := room.WithRoomID(context.Background(), "r_other")
	if _, err := New(f, "r_setup").Connect().Status(ctx, "pg-orders-connector"); !errors.Is(err, ErrNoRunner) {
		t.Fatalf("Status in another room = %v, want ErrNoRunner", err)
	}
	if n := len(f.allCalls()); n != 0 {
		t.Fatalf("%d calls sent for a mismatched room", n)
	}
	if _, err := New(f, "").Connect().Status(ctx, "pg-orders-connector"); err != nil {
		t.Fatalf("Status with the room from ctx: %v", err)
	}
	if got := f.allCalls()[0].roomID; got != "r_other" {
		t.Fatalf("room = %q, want r_other", got)
	}
	if _, err := New(f, "r_setup").Connect().Status(context.Background(), "pg-orders-connector"); err != nil {
		t.Fatalf("Status with the bound room only: %v", err)
	}
	if got := f.allCalls()[1].roomID; got != "r_setup" {
		t.Fatalf("room = %q, want r_setup", got)
	}
}

// TestStepWriteSentOnce: a proposal step's write reaches the runner once. A
// second write under the same step (a Q action retrying after a rebalance)
// is refused with ErrStepSpent before it is sent, and carries the first
// attempt's error, so the caller still sees why the step failed. Another step
// of the same proposal is its own write.
func TestStepWriteSentOnce(t *testing.T) {
	del := writeCaseFor(t, "Connect.DeleteConnector")
	f := newFakeRunner()
	f.failures[OpConnectDeleteConnector] = fakeFailure{summary: "rebalancing", code: ErrorCodeRebalancing}
	s := New(f, "r_setup")
	ctx := del.ctx(t)

	first := del.run(ctx, s)
	if !errors.Is(first, connect.ErrRebalancing) || errors.Is(first, ErrStepSpent) {
		t.Fatalf("first attempt = %v, want the runner's rebalancing failure", first)
	}
	second := del.run(ctx, s)
	if !errors.Is(second, ErrStepSpent) {
		t.Fatalf("second attempt = %v, want ErrStepSpent", second)
	}
	if !errors.Is(second, connect.ErrRebalancing) {
		t.Fatalf("second attempt = %v, want the first attempt's error wrapped", second)
	}
	if n := len(f.callsFor(OpConnectDeleteConnector)); n != 1 {
		t.Fatalf("delete sent %d times for one step, want 1", n)
	}

	// A successful step is spent too.
	delete(f.failures, OpConnectDeleteConnector)
	next := room.WithSetupStep(ctx, room.SetupStep{
		ProposalID: "p_1", StepIndex: 1, ParamsHash: "b1946ac92492d2347c6235b4d2611184",
		Tier: protocol.TierT2, Class: protocol.ClassLocal,
		Action: del.action, Params: json.RawMessage(del.params),
	})
	if err := del.run(next, s); err != nil {
		t.Fatalf("step 1: %v", err)
	}
	if err := del.run(next, s); !errors.Is(err, ErrStepSpent) || errors.Is(err, connect.ErrRebalancing) {
		t.Fatalf("step 1 again = %v, want ErrStepSpent alone", err)
	}
	if n := len(f.callsFor(OpConnectDeleteConnector)); n != 2 {
		t.Fatalf("delete sent %d times for two steps, want 2", n)
	}
}

// TestCreateRetryNeverResendsTheStep runs connect_create_connector over the
// remote client with a runner that answers the create with a Connect
// rebalance: the action's in-step retry loop ends on ErrStepSpent (wrapping
// the rebalance) and the create op is sent exactly once, never as a second
// request the runner would refuse as duplicate_step.
func TestCreateRetryNeverResendsTheStep(t *testing.T) {
	f := newFakeRunner()
	f.overrides[OpConnectListConnectors] = json.RawMessage(`{"connectors":[]}`)
	f.failures[OpConnectCreateConnector] = fakeFailure{summary: "rebalancing", code: ErrorCodeRebalancing}
	reg := actions.NewRegistry()
	if err := actions.RegisterLifecycle(reg, actions.LifecycleDeps{Connect: New(f, "r_setup").Connect()}); err != nil {
		t.Fatal(err)
	}
	act, _ := reg.Get(actions.CreateConnectorName)
	input := `{"connector":"pg-orders-connector","config":{"connector.class":"` + postgresConnectorClass + `",` +
		`"database.hostname":"postgres","database.password":"${secret:source_password}","topic.prefix":"pg"}}`
	ctx := writeCtx(t, protocol.TierT2, protocol.ClassLocal, actions.CreateConnectorName, input)
	_, err := act.Execute(ctx, json.RawMessage(input))
	if !errors.Is(err, ErrStepSpent) || !errors.Is(err, connect.ErrRebalancing) {
		t.Fatalf("Execute = %v, want ErrStepSpent wrapping the rebalance", err)
	}
	if n := len(f.callsFor(OpConnectCreateConnector)); n != 1 {
		t.Fatalf("create sent %d times, want 1", n)
	}
}

// TestCutOrRedactedResultIsNotAValue: a result the runner cut or redacted is
// refused with its own error, never decoded: a config read back with a
// ${secret:...} the runner put in place of a stored value would otherwise
// compare as a different config, or pass a plaintext-credential check.
func TestCutOrRedactedResultIsNotAValue(t *testing.T) {
	cfg := json.RawMessage(`{"config":{"database.password":"${secret:source_password}"}}`)
	tests := []struct {
		name string
		res  protocol.OpResult
		want error
	}{
		{"truncated", protocol.OpResult{OK: true, Data: cfg, Truncated: true}, ErrTruncated},
		{"withheld", protocol.OpResult{OK: true, Data: json.RawMessage(`{"truncated":true,"note":"data withheld"}`), Truncated: true}, ErrTruncated},
		{"redacted", protocol.OpResult{OK: true, Data: cfg, Redacted: true}, ErrRedacted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ex := resultExecutor{res: tc.res}
			got, err := New(ex, "r_setup").Connect().Config(roomCtx(t), "pg-orders-connector")
			if !errors.Is(err, tc.want) || !errors.Is(err, ErrBadResult) {
				t.Fatalf("err = %v, want %v (an ErrBadResult)", err, tc.want)
			}
			if got != nil {
				t.Fatalf("a %s result was decoded: %v", tc.name, got)
			}
		})
	}
	// The same Data, neither cut nor redacted, is the endpoint's value: a
	// genuine provider reference comes back as stored.
	got, err := New(resultExecutor{res: protocol.OpResult{OK: true, Data: cfg}}, "r_setup").Connect().Config(roomCtx(t), "pg-orders-connector")
	if err != nil || got["database.password"] != "${secret:source_password}" {
		t.Fatalf("Config = %v, %v; want the stored reference", got, err)
	}
}

// resultExecutor answers every op with one fixed result.
type resultExecutor struct{ res protocol.OpResult }

func (e resultExecutor) Execute(_ context.Context, _ string, req protocol.OpRequest) (protocol.OpResult, error) {
	res := e.res
	res.ID = req.ID
	return res, nil
}
