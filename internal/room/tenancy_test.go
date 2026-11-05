package room_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
	"piperoom/internal/tools"
)

// probeTool is a read tool that does nothing; registries are told apart by
// pointer.
type probeTool struct{}

func (probeTool) Name() string                { return "probe" }
func (probeTool) Description() string         { return "probe" }
func (probeTool) InputSchema() map[string]any { return map[string]any{"type": "object"} }
func (probeTool) Kind() tools.Kind            { return tools.KindRead }
func (probeTool) Run(context.Context, json.RawMessage) (tools.Result, error) {
	return tools.Result{Summary: "ok"}, nil
}

func toolRegistry(t *testing.T) *tools.Registry {
	t.Helper()
	r := tools.NewRegistry()
	if err := r.Register(probeTool{}); err != nil {
		t.Fatal(err)
	}
	return r
}

func actionRegistry(t *testing.T, as ...actions.Action) *actions.Registry {
	t.Helper()
	r := actions.NewRegistry()
	for _, a := range as {
		if err := r.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

// depsLog records the agent deps each pipeline's run got.
type depsLog struct {
	mu   sync.Mutex
	deps map[string]agent.Deps
}

func (l *depsLog) run(ctx context.Context, a agent.Alert, d agent.Deps, _ agent.Options) error {
	l.mu.Lock()
	l.deps[a.PipelineID] = d
	l.mu.Unlock()
	<-ctx.Done()
	return nil
}

func (l *depsLog) get(p string) (agent.Deps, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	d, ok := l.deps[p]
	return d, ok
}

// TestStackForGivesEachRoomItsOwnSet: the live run, the shadow run,
// CreateProposal and the executor of a room use the tools and actions of the
// room's pipeline, never the process-wide registries; an unknown pipeline gets
// empty ones.
func TestStackForGivesEachRoomItsOwnSet(t *testing.T) {
	on := true
	own := &owners.Owners{Pipelines: []owners.Pipeline{
		{ID: "pa", SourceTables: []string{"public.a"}},
		{ID: "pb", SourceTables: []string{"public.b"}},
		{ID: "ps", SourceTables: []string{"public.s"}, ShadowSpec: &owners.ShadowSpec{Enabled: &on}},
	}}
	actG := &fakeAction{name: "fix", layer: "kafka-connect"}
	actA := &fakeAction{name: "fix", layer: "kafka-connect"}
	actB := &fakeAction{name: "fix", layer: "kafka-connect"}
	onlyB := &fakeAction{name: "only_b", layer: "kafka-connect"}
	toolsG, toolsA, toolsB, toolsS := toolRegistry(t), toolRegistry(t), toolRegistry(t), toolRegistry(t)
	regA, regB := actionRegistry(t, actA), actionRegistry(t, actB, onlyB)
	sets := map[string]room.PipelineStack{
		"pa": {Tools: toolsA, Actions: regA},
		"pb": {Tools: toolsB, Actions: regB},
		"ps": {Tools: toolsS},
	}
	log := &depsLog{deps: map[string]agent.Deps{}}
	svc := room.New(room.Deps{
		Store: memstore.New(), Users: room.DefaultUsers(), Owners: own,
		Tools: toolsG, Actions: actionRegistry(t, actG),
		StackFor:     func(p string) room.PipelineStack { return sets[p] },
		Run:          log.run,
		WriteHandoff: fakeWrite,
		MaxAgents:    10, // four rooms run at once
	})
	t.Cleanup(svc.Close)
	ctx := context.Background()
	arjun := mustUser(t, svc, "arjun")

	ra, err := svc.CreateRoom(ctx, arjun, "a", "sev2", "pa")
	if err != nil {
		t.Fatal(err)
	}
	rb, err := svc.CreateRoom(ctx, arjun, "b", "sev2", "pb")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.CreateRoomFromAlert(ctx, "s", "sev2", "ps", json.RawMessage(`{"title":"s"}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.CreateRoomFromAlert(ctx, "u", "sev2", "nope", json.RawMessage(`{"title":"u"}`)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		for _, p := range []string{"pa", "pb", "ps", "nope"} {
			if _, ok := log.get(p); !ok {
				return false
			}
		}
		return true
	})
	for p, want := range map[string]*tools.Registry{"pa": toolsA, "pb": toolsB, "ps": toolsS} {
		d, _ := log.get(p)
		if d.Tools != want {
			t.Errorf("%s run: wrong tool registry", p)
		}
	}
	if d, _ := log.get("pa"); d.Actions != regA {
		t.Error("pa run: wrong action registry")
	}
	if d, _ := log.get("ps"); d.Actions != nil {
		t.Error("a shadow run must get no actions")
	}
	// Unknown pipeline: empty registries, never the process-wide ones.
	if d, _ := log.get("nope"); d.Tools == toolsG || d.Tools == nil || len(d.Tools.All()) != 0 || d.Actions == nil || len(d.Actions.All()) != 0 {
		t.Errorf("unknown pipeline run: tools %p (global %p), actions %v", d.Tools, toolsG, d.Actions)
	}

	step := func(action string) events.ProposalCreatedPayload {
		return events.ProposalCreatedPayload{Title: "fix", Risk: "low risk", Rollback: "undo it",
			Steps: []events.ProposalStep{{Action: action, Layer: "kafka-connect", Params: json.RawMessage(`{}`), Description: "d"}}}
	}
	// CreateProposal validates against the room's own actions.
	if _, err := svc.CreateProposal(ctx, ra.ID, step("only_b"), events.AgentActor); err == nil || !strings.Contains(err.Error(), "unknown action") {
		t.Fatalf("only_b in pa's room: err = %v, want unknown action", err)
	}
	if _, err := svc.CreateProposal(ctx, rb.ID, step("only_b"), events.AgentActor); err != nil {
		t.Fatalf("only_b in pb's room: %v", err)
	}
	// The executor runs pb's instance of "fix".
	pid, err := svc.CreateProposal(ctx, rb.ID, step("fix"), events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ApproveProposal(ctx, arjun, pid, "kafka-connect", proposalHash(t, svc, pid)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return actB.Calls() == 1 })
	if actA.Calls() != 0 || actG.Calls() != 0 {
		t.Fatalf("calls: a=%d global=%d, want 0", actA.Calls(), actG.Calls())
	}
}

// TestAllActionsUnionAcrossStacks pins that with per-pipeline client sets
// (StackFor) AllActions returns the union of every set's actions, deduplicated
// by name with the first registration kept.
func TestAllActionsUnionAcrossStacks(t *testing.T) {
	sharedA := &fakeAction{name: "shared", layer: "kafka-connect"}
	onlyA := &fakeAction{name: "only_a", layer: "kafka-connect"}
	dupA := &fakeAction{name: "dup", layer: "kafka-connect"}
	sharedB := &fakeAction{name: "shared", layer: "kafka-connect"}
	onlyB := &fakeAction{name: "only_b", layer: "kafka-connect"}
	dupB := &fakeAction{name: "dup", layer: "kafka-connect"}
	regA := actionRegistry(t, sharedA, onlyA, dupA)
	regB := actionRegistry(t, sharedB, onlyB, dupB)
	svc := room.New(room.Deps{
		Store:  memstore.New(),
		Users:  room.DefaultUsers(),
		Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "pa", SourceTables: []string{"public.a"}}}},
		StackFor: func(string) room.PipelineStack {
			return room.PipelineStack{Actions: regA}
		},
		ActionCatalogue: func() []actions.Action {
			return append(regA.All(), regB.All()...)
		},
		Run: fakeRun,
	})
	t.Cleanup(svc.Close)

	got := svc.AllActions()
	var names []string
	for _, a := range got {
		names = append(names, a.Name())
	}
	want := []string{"shared", "only_a", "dup", "only_b"}
	if len(names) != len(want) {
		t.Fatalf("AllActions names = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("AllActions names = %v, want %v", names, want)
		}
	}
	// The first registration of a duplicated name is kept.
	if got[0] != actions.Action(sharedA) || got[2] != actions.Action(dupA) {
		t.Fatalf("dedup kept the wrong instance: got %v / %v", got[0], got[2])
	}
}

// TestSoloRefusesTenantMembers: a tenant member is someone else named in
// owners.yaml, so solo mode refuses to start.
func TestSoloRefusesTenantMembers(t *testing.T) {
	own := &owners.Owners{
		Tenants:   map[string]owners.Tenant{"a": {Members: owners.Principals{{Kind: owners.KindGroup, Name: "team-a"}}}},
		Pipelines: []owners.Pipeline{{ID: "pa", Tenant: "a"}},
	}
	err := room.CheckSoloUser("arjun", room.DefaultUsers(), own)
	if err == nil || !strings.Contains(err.Error(), "group:team-a") {
		t.Fatalf("err = %v, want the tenant member named", err)
	}
}

// TestCanSeePipelineSoloAndFilter: with tenants, the solo user sees every
// tenant; without tenants everyone sees everything.
func TestCanSeePipelineSoloAndFilter(t *testing.T) {
	own := &owners.Owners{
		Tenants: map[string]owners.Tenant{"a": {Members: owners.Principals{{Kind: owners.KindUser, Name: "priya"}}}},
		Pipelines: []owners.Pipeline{
			{ID: "pa", Tenant: "a"},
			{ID: "pd"}, // tenant default, no members, no owners
		},
	}
	svc := room.New(room.Deps{Store: memstore.New(), Users: room.DefaultUsers(), Owners: own, SoloUser: "arjun", Run: fakeRun})
	t.Cleanup(svc.Close)
	priya, arjun, kabir := mustUser(t, svc, "priya"), mustUser(t, svc, "arjun"), mustUser(t, svc, "kabir")
	if !svc.CanSeePipeline(priya, "pa") || svc.CanSeePipeline(priya, "pd") || svc.CanSeePipeline(kabir, "pa") {
		t.Fatal("tenant filter")
	}
	if !svc.CanSeePipeline(arjun, "pa") || !svc.CanSeePipeline(arjun, "pd") || !svc.CanSeePipeline(arjun, "unknown") {
		t.Fatal("the solo user must see every tenant")
	}
	r, err := svc.CreateRoom(context.Background(), arjun, "t", "sev2", "pa")
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CheckRoomVisible(context.Background(), kabir, r.ID); err == nil {
		t.Fatal("kabir must not see pa's room")
	}
	if list, _ := svc.ListRoomsFor(context.Background(), arjun); len(list) != 1 {
		t.Fatalf("solo list = %d rooms", len(list))
	}
	if list, _ := svc.ListRoomsFor(context.Background(), kabir); len(list) != 0 {
		t.Fatalf("kabir list = %d rooms", len(list))
	}

	open := room.New(room.Deps{Store: memstore.New(), Users: room.DefaultUsers(), Owners: &owners.Owners{Pipelines: own.Pipelines[1:]}, Run: fakeRun})
	t.Cleanup(open.Close)
	if !open.CanSeePipeline(kabir, "pd") || !open.CanSeePipeline(kabir, "unknown") || open.TenancyOn() {
		t.Fatal("no tenants: everyone sees everything")
	}
	if err := open.CheckRoomVisible(context.Background(), kabir, "r_missing"); err != nil {
		t.Fatalf("filter off: CheckRoomVisible must do nothing, got %v", err)
	}
}
