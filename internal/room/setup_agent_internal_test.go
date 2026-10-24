package room

import (
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/store/memstore"
	"piperoom/internal/tools"
)

// U1-25: a setup room's agent runs in setup mode over the setup stack built
// on its paired runner; the stack serves the room's proposals; the ladder's
// setup.verified reaches the agent; and with setup or write mode off nothing
// setup is built or run.

// agentResolver answers db.acme with a private address, so a target
// registers without the test's DNS.
type agentResolver struct{}

func (agentResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return []netip.Addr{netip.MustParseAddr("10.1.2.3")}, nil
}

// runnerProbe is a read tool of the fake setup stack: one T0 op to the paired
// runner through the executor the room handed the builder.
type runnerProbe struct {
	ex     protocol.RunnerExecutor
	roomID string
}

func (*runnerProbe) Name() string                { return "runner_probe" }
func (*runnerProbe) Description() string         { return "probe the runner" }
func (*runnerProbe) InputSchema() map[string]any { return map[string]any{"type": "object"} }
func (*runnerProbe) Kind() tools.Kind            { return tools.KindRead }
func (p *runnerProbe) Run(ctx context.Context, _ json.RawMessage) (tools.Result, error) {
	res, err := p.ex.Execute(ctx, p.roomID, protocol.OpRequest{
		ID: NewID("op_"), Op: "env_detect", Params: json.RawMessage(`{}`),
		Tier: protocol.TierT0, Class: protocol.ClassLocal,
	})
	if err != nil {
		return tools.Result{}, err
	}
	return tools.Result{Summary: res.Summary}, nil
}

// answeringRunner is the paired runner's socket: it answers every op frame
// through the service's result path, as the runner endpoint does.
type answeringRunner struct {
	svc                            *Service
	tenant, roomID, creator, token string

	mu  sync.Mutex
	ops []protocol.OpRequest
}

func (w *answeringRunner) WriteRunnerFrame(_ context.Context, f protocol.Frame) error {
	if f.Type != protocol.FrameOp || f.Op == nil {
		return nil
	}
	op := *f.Op
	w.mu.Lock()
	w.ops = append(w.ops, op)
	w.mu.Unlock()
	go w.svc.RunnerResult(context.Background(), w.tenant, w.roomID, w.creator, w.token,
		protocol.OpResult{ID: op.ID, OK: true, Summary: "docker 27.1 on linux"})
	return nil
}

func (w *answeringRunner) opCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.ops)
}

// setupScript is the agent runner of the test: what a setup run does first
// (a read through the runner, setup_plan, one proposal in the plan and one
// outside it), then it waits for the ladder's outcome.
type setupScript struct {
	start    chan struct{} // closed once the runner's socket is bound
	proposed chan struct{} // closed after the two proposals
	verified chan agent.HumanInput

	mu                    sync.Mutex
	calls                 int
	alert                 agent.Alert
	opts                  agent.Options
	tools                 *tools.Registry
	acts                  *actions.Registry
	probe                 string
	probeErr, planErr     error
	inPlanID              string
	inPlanErr, outPlanErr error
}

func newSetupScript() *setupScript {
	return &setupScript{
		start: make(chan struct{}), proposed: make(chan struct{}),
		verified: make(chan agent.HumanInput, 4),
	}
}

func setupStepProposal(action string) events.ProposalCreatedPayload {
	return events.ProposalCreatedPayload{
		Title: "setup step " + action, Risk: "low", Rollback: "compose down",
		Steps: []events.ProposalStep{{
			Action: action, Layer: events.LayerEnvironment, Params: json.RawMessage(`{"target":"db.acme:5432"}`),
		}},
	}
}

func (sc *setupScript) run(ctx context.Context, alert agent.Alert, deps agent.Deps, opts agent.Options) error {
	sc.mu.Lock()
	sc.calls++
	first := sc.calls == 1
	sc.alert, sc.opts, sc.tools, sc.acts = alert, opts, deps.Tools, deps.Actions
	sc.mu.Unlock()
	if !first {
		<-ctx.Done()
		return nil
	}
	select {
	case <-sc.start:
	case <-ctx.Done():
		return nil
	}
	if alert.Mode != agent.ModeSetup {
		// An incident run: the test reads what it was given, nothing more.
		close(sc.proposed)
		<-ctx.Done()
		return nil
	}
	var probe string
	var probeErr error
	if t, ok := deps.Tools.Get("runner_probe"); ok {
		res, err := t.Run(ctx, json.RawMessage(`{}`))
		probe, probeErr = res.Summary, err
	} else {
		probeErr = errors.New("runner_probe is not in the run's tools")
	}
	planErr := deps.Sink.Emit(ctx, events.SetupPlan, events.SetupPlanPayload{
		PlanID: "pl_1",
		Phases: []events.SetupPlanPhase{{Name: "stack", Steps: []events.SetupPlanStep{
			{Index: 0, Op: "compose_up", Target: "db.acme:5432", Tier: plan.TierT2, Layer: events.LayerEnvironment},
		}}},
	})
	inID, inErr := deps.Proposals.Create(ctx, setupStepProposal("compose_up"))
	_, outErr := deps.Proposals.Create(ctx, setupStepProposal("compose_write"))
	sc.mu.Lock()
	sc.probe, sc.probeErr, sc.planErr = probe, probeErr, planErr
	sc.inPlanID, sc.inPlanErr, sc.outPlanErr = inID, inErr, outErr
	sc.mu.Unlock()
	close(sc.proposed)
	for {
		if err := deps.Inbox.Wait(ctx); err != nil {
			return nil
		}
		ins, err := deps.Inbox.Drain(ctx)
		if err != nil {
			return nil
		}
		for _, in := range ins {
			if in.Kind == inputSetupVerified {
				sc.verified <- in
			}
		}
	}
}

func (sc *setupScript) callCount() int {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.calls
}

// stackBuilds records the builder's calls; err makes it refuse.
type stackBuilds struct {
	mu    sync.Mutex
	calls int
	last  SetupStackInput
	err   error
}

func (b *stackBuilds) build(_ context.Context, in SetupStackInput) (PipelineStack, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	b.last = in
	if b.err != nil {
		return PipelineStack{}, b.err
	}
	reg := tools.NewReadOnlyRegistry()
	if err := reg.Register(&runnerProbe{ex: in.Executor, roomID: in.RoomID}); err != nil {
		return PipelineStack{}, err
	}
	acts := actions.NewRegistry()
	for _, name := range []string{"compose_up", "compose_write"} {
		if err := acts.Register(&tierAction{name: name, layer: events.LayerEnvironment, floor: plan.TierT1}); err != nil {
			return PipelineStack{}, err
		}
	}
	return PipelineStack{Tools: reg, Actions: acts}, nil
}

func (b *stackBuilds) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

type setupAgentOpts struct {
	writeOff  bool
	buildErr  error
	noBuilder bool
}

type setupAgentRig struct {
	t      *testing.T
	svc    *Service
	script *setupScript
	builds *stackBuilds
	cdc    *tools.Registry   // Deps.Tools, the incident rooms' tools
	cdcAct *actions.Registry // Deps.Actions
}

func newSetupAgentRig(t *testing.T, o setupAgentOpts) *setupAgentRig {
	t.Helper()
	users, err := ParseUsers([]byte(tierUsersYAML))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(tierOwnersYAML()), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	r := &setupAgentRig{
		t: t, script: newSetupScript(), builds: &stackBuilds{err: o.buildErr},
		cdc: tools.NewReadOnlyRegistry(), cdcAct: actions.NewRegistry(),
	}
	d := Deps{
		Store: memstore.New(), Users: users, Owners: own, Run: r.script.run,
		Tools: r.cdc, Actions: r.cdcAct, WriteModeOff: o.writeOff,
		Setup:         config.Setup{Enabled: true, TokenBudget: 1_500_000},
		SetupResolver: agentResolver{},
	}
	if !o.noBuilder {
		d.SetupStack = r.builds.build
	}
	r.svc = New(d)
	t.Cleanup(r.svc.Close)
	if err := r.svc.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r
}

func (r *setupAgentRig) user(id string) User {
	r.t.Helper()
	u, ok := r.svc.LookupUser(id)
	if !ok {
		r.t.Fatalf("no user %s", id)
	}
	return u
}

// open opens the setup room the way POST /api/setup does.
func (r *setupAgentRig) open() SetupRoom {
	r.t.Helper()
	res, err := r.svc.CreateSetupRoom(context.Background(), r.user("alice"), events.SetupRequest{
		PipelineID: "shipments-setup", Tenant: "a", Title: "Shipments setup",
		SourceKind: "postgres", Target: "compose", TopicPrefix: "shipments",
		Tables: []string{"public.shipments"}, Connectors: []string{"shipments-src"},
		Targets: []events.SetupTarget{{Kind: "postgres", Address: "db.acme:5432"}},
	})
	if err != nil {
		r.t.Fatalf("CreateSetupRoom: %v", err)
	}
	return res
}

// pair attaches the runner with the room's pairing code and binds its
// socket, as the runner endpoint does after Hello.
func (r *setupAgentRig) pair(res SetupRoom) *answeringRunner {
	r.t.Helper()
	ctx := context.Background()
	alice := events.Actor{Kind: events.ActorHuman, ID: "alice", Name: "Alice"}
	if err := r.svc.AttachRunner(ctx, res.Code, Runner{
		Tenant: "a", RoomID: res.Room.ID, Creator: alice, Label: "laptop", OS: "linux", Arch: "amd64",
		Token: "tok-1", TokenExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		r.t.Fatalf("attach runner: %v", err)
	}
	w := &answeringRunner{svc: r.svc, tenant: "a", roomID: res.Room.ID, creator: "alice", token: "tok-1"}
	if !r.svc.BindRunnerWriter("a", res.Room.ID, "alice", "tok-1", w) {
		r.t.Fatal("bind runner writer refused")
	}
	return w
}

// TestSetupRoomRunsInSetupMode goes from the setup-open path through a
// paired runner: the run starts in setup mode over the setup stack, its read
// reaches the runner, setup.plan lands, the room accepts the proposal in the
// plan and refuses the one outside it, and the ladder's setup.verified
// reaches the agent as a setup_verified input.
func TestSetupRoomRunsInSetupMode(t *testing.T) {
	r := newSetupAgentRig(t, setupAgentOpts{})
	ctx := context.Background()
	res := r.open()
	roomID := res.Room.ID
	if n := r.script.callCount(); n != 0 {
		t.Fatalf("the agent started before a runner paired (%d runs)", n)
	}
	w := r.pair(res)
	waitForCond(t, 5*time.Second, func() bool { return r.script.callCount() == 1 }, "pairing did not start the setup run")
	close(r.script.start)
	select {
	case <-r.script.proposed:
	case <-time.After(10 * time.Second):
		t.Fatal("the setup run never reached its proposals")
	}

	sc := r.script
	sc.mu.Lock()
	alert, opts, runTools, runActs := sc.alert, sc.opts, sc.tools, sc.acts
	probe, probeErr, planErr := sc.probe, sc.probeErr, sc.planErr
	inPlanID, inPlanErr, outPlanErr := sc.inPlanID, sc.inPlanErr, sc.outPlanErr
	sc.mu.Unlock()

	// Setup mode, with the room's request and bounds.
	if alert.Mode != agent.ModeSetup || alert.RoomID != roomID || alert.PipelineID != "shipments-setup" {
		t.Fatalf("alert = mode %q room %q pipeline %q, want setup mode for the room", alert.Mode, alert.RoomID, alert.PipelineID)
	}
	var req events.SetupRequest
	if err := json.Unmarshal(alert.Raw, &req); err != nil || req.PipelineID != "shipments-setup" || req.SourceKind != "postgres" {
		t.Fatalf("alert.Raw = %s (%v), want the room's setup request", alert.Raw, err)
	}
	want := agent.SetupBounds{AttemptsPerStep: 3, StepsPerPlan: 12, WallClock: 45 * time.Minute, TokenBudget: 1_500_000}
	if opts.Setup == nil || *opts.Setup != want {
		t.Fatalf("Options.Setup = %+v, want %+v", opts.Setup, want)
	}

	// The registries are the setup stack, built once over this room's runner.
	if r.builds.count() != 1 {
		t.Fatalf("setup stack built %d times, want 1", r.builds.count())
	}
	r.builds.mu.Lock()
	in := r.builds.last
	r.builds.mu.Unlock()
	if in.RoomID != roomID || in.Request.PipelineID != "shipments-setup" || in.Executor != r.svc.RunnerExecutor() {
		t.Fatalf("builder input = room %q pipeline %q executor %v", in.RoomID, in.Request.PipelineID, in.Executor)
	}
	if len(in.Request.Targets) != 1 || in.Request.Targets[0].Address != "db.acme:5432" || in.Request.Targets[0].Class == "" {
		t.Fatalf("builder targets = %+v, want the registered target with its class", in.Request.Targets)
	}
	if runTools == r.cdc || runActs == r.cdcAct {
		t.Fatal("the setup run got the incident rooms' registries")
	}
	if _, ok := runTools.Get("runner_probe"); !ok {
		t.Fatal("the setup run's tools are not the setup stack's")
	}
	if probeErr != nil || probe != "docker 27.1 on linux" || w.opCount() != 1 {
		t.Fatalf("read through the runner = %q, %v (%d ops), want the runner's answer", probe, probeErr, w.opCount())
	}

	// setup.plan landed through the agent's sink and folded.
	if planErr != nil {
		t.Fatalf("setup.plan through the sink: %v", planErr)
	}
	if got := mustRoom(t, r.svc, roomID); got.Setup == nil || got.Setup.PlanID != "pl_1" {
		t.Fatalf("snapshot plan = %+v, want pl_1", got.Setup)
	}
	planned := false
	for _, e := range roomEvents(t, r.svc, roomID) {
		if e.Type == events.SetupPlan && e.Actor.Kind == events.AgentActor.Kind {
			planned = true
		}
	}
	if !planned {
		t.Fatal("no agent setup.plan in the log")
	}

	// The room validated both proposals against the setup stack's actions.
	if inPlanErr != nil || inPlanID == "" {
		t.Fatalf("the proposal in the plan was refused: %v", inPlanErr)
	}
	if _, ok := openProposal(mustRoom(t, r.svc, roomID), inPlanID); !ok {
		t.Fatalf("proposal %s is not open", inPlanID)
	}
	if !errors.Is(outPlanErr, agent.ErrNotInPlan) {
		t.Fatalf("the proposal outside the plan = %v, want agent.ErrNotInPlan", outPlanErr)
	}

	// The ladder's verdict for the current plan reaches the agent.
	rungs := []events.SetupRung{}
	for _, name := range setupRegistrationRungs {
		rungs = append(rungs, events.SetupRung{Name: name, OK: true})
	}
	rungs[6].Note = "no sink was set up"
	l := &setupLadder{s: r.svc, roomID: roomID, planID: "pl_1"}
	if !l.emitVerified(ctx, rungs, nil) {
		t.Fatal("the ladder's setup.verified was not recorded")
	}
	select {
	case got := <-r.script.verified:
		if got.OK == nil || !*got.OK {
			t.Fatalf("setup_verified OK = %v, want true", got.OK)
		}
		if !strings.Contains(got.Text, "pl_1") || !strings.Contains(got.Text, plan.RungCanaryEvent+": ok") ||
			!strings.Contains(got.Text, "no sink was set up") {
			t.Fatalf("setup_verified text = %q", got.Text)
		}
		if got.Actor != systemActor {
			t.Fatalf("setup_verified actor = %+v, want the server", got.Actor)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("setup.verified never reached the agent")
	}
}

// TestSetupVerifiedInput is the input's rule: only the current plan's
// verified event, OK only with every rung 1 to 7 present and green.
func TestSetupVerifiedInput(t *testing.T) {
	green := func() []events.SetupRung {
		var out []events.SetupRung
		for _, name := range setupRegistrationRungs {
			out = append(out, events.SetupRung{Name: name, OK: true})
		}
		return out
	}
	red := green()
	red[4].OK, red[4].Note = false, "timed out after 1m0s: the read did not answer for this run's key"
	missing := green()[:6]
	withPerson := append(green(), events.SetupRung{Name: plan.RungPersonChange, OK: false, Note: "no change"})
	setupSnap := RoomSnapshot{Setup: &SetupSnapshot{Mode: true, PlanID: "pl_2"}}

	cases := []struct {
		name   string
		snap   RoomSnapshot
		plan   string
		rungs  []events.SetupRung
		want   *bool
		inText string
	}{
		{"green", setupSnap, "pl_2", green(), ptrBool(true), "rungs 1 to 7 are green"},
		{"a red rung", setupSnap, "pl_2", red, ptrBool(false), "canary_event: FAILED - timed out"},
		{"a missing rung", setupSnap, "pl_2", missing, ptrBool(false), "not every rung"},
		{"rung 8 red does not matter", setupSnap, "pl_2", withPerson, ptrBool(true), "person_change: FAILED"},
		{"another plan", setupSnap, "pl_1", green(), nil, ""},
		{"no plan id", setupSnap, "", green(), nil, ""},
		{"an incident room", RoomSnapshot{}, "pl_2", green(), nil, ""},
	}
	for _, c := range cases {
		raw, err := json.Marshal(events.SetupVerifiedPayload{PlanID: c.plan, Rungs: c.rungs})
		if err != nil {
			t.Fatal(err)
		}
		e := events.Event{Seq: 7, Type: events.SetupVerified, Actor: systemActor, Payload: raw}
		in := setupVerifiedInput(c.snap, e)
		if c.want == nil {
			if in != nil {
				t.Errorf("%s: input %+v, want none", c.name, in)
			}
			continue
		}
		if in == nil || in.Kind != inputSetupVerified || in.OK == nil || *in.OK != *c.want || in.Seq != 7 {
			t.Errorf("%s: input %+v, want ok=%v", c.name, in, *c.want)
			continue
		}
		if !strings.Contains(in.Text, c.inText) {
			t.Errorf("%s: text %q lacks %q", c.name, in.Text, c.inText)
		}
		if !startsAgent(c.snap, e) {
			t.Errorf("%s: a setup_verified input does not wake the agent", c.name)
		}
	}
}

// A rung note is tool output: a secret in it is masked before the agent
// reads it, and the text stays bounded.
func TestSetupVerifiedTextMasksAndBounds(t *testing.T) {
	rungs := []events.SetupRung{{Name: plan.RungConnector, OK: false, Note: "connect failed: postgres://app:hunter2@db.acme:5432/orders"}}
	for i := 0; i < 50; i++ {
		rungs = append(rungs, events.SetupRung{Name: plan.RungTasks, Note: strings.Repeat("x", 400)})
	}
	text := setupVerifiedText(events.SetupVerifiedPayload{PlanID: "pl_1", Rungs: rungs}, false)
	if strings.Contains(text, "hunter2") {
		t.Fatalf("the rung note's secret reached the agent: %q", text)
	}
	if len(text) > maxSetupVerifiedText {
		t.Fatalf("text is %d bytes, want at most %d", len(text), maxSetupVerifiedText)
	}
}

// With write mode off no setup stack is built and no setup run starts, even
// with a builder: pairing, a steer and the proposal paths build nothing.
func TestSetupRoomWriteModeOffBuildsNothing(t *testing.T) {
	r := newSetupAgentRig(t, setupAgentOpts{writeOff: true})
	res := r.open()
	r.pair(res)
	if _, err := r.svc.Steer(context.Background(), r.user("alice"), res.Room.ID, "go"); err != nil {
		t.Fatalf("steer: %v", err)
	}
	if acts := r.svc.actionsFor("shipments-setup"); acts != r.cdcAct {
		t.Fatal("with write mode off the setup room's actions are not the plain registry")
	}
	if r.svc.setupToolsFor("shipments-setup") != r.cdc {
		t.Fatal("with write mode off the setup room's tools are not the plain registry")
	}
	time.Sleep(50 * time.Millisecond) // a run that wrongly started would have been called by now
	if n := r.builds.count(); n != 0 {
		t.Fatalf("setup stack built %d times with write mode off", n)
	}
	if n := r.script.callCount(); n != 0 {
		t.Fatalf("setup run started %d times with write mode off", n)
	}
}

// With setup off the builder is never called either (the room service is
// the second gate; cmd/server passes no builder at all then).
func TestSetupStackForSetupOff(t *testing.T) {
	r := newSetupAgentRig(t, setupAgentOpts{})
	r.open()
	r.svc.setupCfg.Enabled = false
	if _, ok, err := r.svc.setupStackFor("shipments-setup"); ok || err != nil {
		t.Fatalf("setupStackFor with setup off = %v, %v; want none", ok, err)
	}
	if r.builds.count() != 0 {
		t.Fatalf("setup stack built %d times with setup off", r.builds.count())
	}
}

// A builder that refuses fails the room closed: the run does not start on
// any other registry, the room says why, and a proposal finds no action.
func TestSetupRoomStackRefusalFailsClosed(t *testing.T) {
	r := newSetupAgentRig(t, setupAgentOpts{buildErr: errors.New("wiring: setup stack: PIPEROOM_WRITE_MODE is off")})
	if err := r.cdcAct.Register(&tierAction{name: "compose_up", layer: events.LayerEnvironment, floor: plan.TierT1}); err != nil {
		t.Fatal(err)
	}
	res := r.open()
	roomID := res.Room.ID
	r.pair(res)
	waitForCond(t, 5*time.Second, func() bool {
		for _, e := range roomEvents(t, r.svc, roomID) {
			var p events.AgentMessagePayload
			if e.Type == events.AgentMessage && json.Unmarshal(e.Payload, &p) == nil &&
				strings.Contains(p.Text, "tools could not be built") {
				return true
			}
		}
		return false
	}, "the room never said the setup stack was refused")
	if n := r.script.callCount(); n != 0 {
		t.Fatalf("the run started %d times without its setup stack", n)
	}
	if _, ok := r.svc.actionsFor("shipments-setup").Get("compose_up"); ok {
		t.Fatal("a refused setup stack fell back to the incident rooms' actions")
	}
	if _, err := r.svc.CreateProposal(context.Background(), roomID, setupStepProposal("compose_up"), events.AgentActor); err == nil {
		t.Fatal("a proposal was accepted with no setup stack")
	}
}

// Incident rooms are unchanged by a builder: their run gets Deps.Tools and
// Deps.Actions, no mode and no setup bounds, and nothing setup is built.
func TestIncidentRoomUnchangedBySetupStack(t *testing.T) {
	r := newSetupAgentRig(t, setupAgentOpts{})
	close(r.script.start)
	snap, err := r.svc.CreateRoom(context.Background(), r.user("alice"), "orders lag", "sev2", "existing")
	if err != nil {
		t.Fatal(err)
	}
	waitForCond(t, 5*time.Second, func() bool { return r.script.callCount() == 1 }, "the incident run never started")
	<-r.script.proposed
	r.script.mu.Lock()
	alert, opts, runTools, runActs := r.script.alert, r.script.opts, r.script.tools, r.script.acts
	r.script.mu.Unlock()
	if alert.Mode != "" || opts.Setup != nil || alert.RoomID != snap.ID {
		t.Fatalf("incident run: mode %q, setup bounds %+v", alert.Mode, opts.Setup)
	}
	if runTools != r.cdc || runActs != r.cdcAct {
		t.Fatal("the incident run did not get Deps.Tools and Deps.Actions")
	}
	if r.builds.count() != 0 {
		t.Fatalf("setup stack built %d times for an incident room", r.builds.count())
	}
}

// Without a builder (a harness) a setup room still runs in setup mode, on the
// room's own registries.
func TestSetupRoomWithoutBuilderUsesRoomRegistries(t *testing.T) {
	r := newSetupAgentRig(t, setupAgentOpts{noBuilder: true})
	res := r.open()
	r.pair(res)
	waitForCond(t, 5*time.Second, func() bool { return r.script.callCount() == 1 }, "pairing did not start the setup run")
	r.script.mu.Lock()
	alert, runTools := r.script.alert, r.script.tools
	r.script.mu.Unlock()
	if alert.Mode != agent.ModeSetup || runTools != r.cdc {
		t.Fatalf("mode %q, tools are the room's: %v", alert.Mode, runTools == r.cdc)
	}
}

func ptrBool(b bool) *bool { return &b }
