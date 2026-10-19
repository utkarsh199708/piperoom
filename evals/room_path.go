package evals

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// The room path. Every run goes through a real room.Service, the same type
// cmd/server serves, over an in-memory store: the room opens from an alert
// (OpenAlertRoom, as POST /api/alerts does) or from the built-in watcher
// (watcher_start.go), the service starts the agent with its own Deps (its
// sink, its inbox, its proposal path with Validate, CheckScope, param keys
// and plan-text checks), builds the alert's pipeline map from the topology,
// and the scripted humans act through the service's own API: Steer as the
// driver, AddComment, and ApproveProposal by each layer owner, after which
// the room's executor runs the steps. Only three things are not production:
// the tool results (replayed), the action writes (scripted outcomes, see
// execScript) and the model (cfg.Runner; evalrun passes the Endpoint that
// wiring.NewAgentEndpoint builds, the constructor cmd/server uses).
//
// So a run that passes here passes in a room. The harness is stricter than a
// room in one way: a runner error ends the run at once, while the room would
// retry it up to three times.

// User ids of the scripted people. They are never real users; the owners
// file of every run names them as the only layer owners.
const (
	evalDriverID  = "eval-driver"
	evalOwnerPfx  = "eval-owner-"
	evalCommenter = "eval-watcher"
)

// executionTimeout bounds the wait for the room's executor after the last
// approval. The executor's own step timeout is 30 s per step.
const executionTimeout = 2 * time.Minute

// roomFixture is what one scenario's rooms need from config: the owners file
// (the scenario's pipeline, every layer owned by a scripted owner) and the
// users.
type roomFixture struct {
	owners *owners.Owners
	users  *room.Users
	driver room.User
	// commenters maps an input's From (or "" for the default) to its user.
	commenters map[string]room.User
}

func ownerUserID(layer string) string { return evalOwnerPfx + layer }

// unsafeUserChars are characters a users.yaml id may not hold.
var unsafeUserChars = regexp.MustCompile(`[^A-Za-z0-9_.-]`)

// loadOwnersFor writes the scenario's pipeline into an owners file, every
// layer owned by its eval owner, and loads it with owners.Load.
func loadOwnersFor(sc Scenario) (*owners.Owners, error) {
	pipe, err := sc.pipelineNode()
	if err != nil {
		return nil, err
	}
	layers := map[string]string{}
	for _, l := range events.Layers {
		layers[l] = "user:" + ownerUserID(l)
	}
	doc, err := yaml.Marshal(map[string]any{"pipelines": []*yaml.Node{pipe}, "layers": layers})
	if err != nil {
		return nil, fmt.Errorf("evals: owners file: %w", err)
	}
	f, err := os.CreateTemp("", "evals-owners-*.yaml")
	if err != nil {
		return nil, fmt.Errorf("evals: owners file: %w", err)
	}
	path := f.Name()
	defer os.Remove(path)
	_, werr := f.Write(doc)
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return nil, fmt.Errorf("evals: owners file: %w", werr)
	}
	own, err := owners.Load(path)
	if err != nil {
		return nil, fmt.Errorf("evals: scenario %q: pipeline is not a valid owners.yaml entry: %w", sc.Name, err)
	}
	return own, nil
}

// newRoomFixture writes the scenario's pipeline into an owners file and loads
// it with owners.Load, so the room sees a pipeline validated (and its watch
// block resolved) exactly as the server's would be.
func newRoomFixture(sc Scenario) (*roomFixture, error) {
	own, err := loadOwnersFor(sc)
	if err != nil {
		return nil, err
	}

	type userEntry struct {
		ID     string   `yaml:"id"`
		Name   string   `yaml:"name"`
		Layers []string `yaml:"layers,omitempty"`
	}
	driverName := "driver"
	commenters := map[string]string{"": evalCommenter}
	for _, in := range sc.Inputs {
		switch {
		case in.Kind == "steer" && in.From != "" && driverName == "driver":
			driverName = in.From
		case in.Kind == "comment" && in.From != "":
			commenters[in.From] = "eval-" + unsafeUserChars.ReplaceAllString(in.From, "_")
		}
	}
	entries := []userEntry{{ID: evalDriverID, Name: driverName}}
	seen := map[string]bool{evalDriverID: true}
	for _, l := range events.Layers {
		entries = append(entries, userEntry{ID: ownerUserID(l), Name: l + " owner", Layers: []string{l}})
		seen[ownerUserID(l)] = true
	}
	for from, id := range commenters {
		if seen[id] {
			continue
		}
		seen[id] = true
		name := from
		if name == "" {
			name = "watcher"
		}
		entries = append(entries, userEntry{ID: id, Name: name})
	}
	ub, err := yaml.Marshal(map[string]any{"users": entries})
	if err != nil {
		return nil, fmt.Errorf("evals: users file: %w", err)
	}
	users, err := room.ParseUsers(ub)
	if err != nil {
		return nil, fmt.Errorf("evals: users file: %w", err)
	}
	fx := &roomFixture{owners: own, users: users, commenters: map[string]room.User{}}
	fx.driver, _ = users.Get(evalDriverID)
	for from, id := range commenters {
		fx.commenters[from], _ = users.Get(id)
	}
	return fx, nil
}

// replayTopology is the room's topology.Builder: Render returns the
// scenario's recorded pipeline map, which the room puts in the alert exactly
// as it would put the live render. With no map Build fails, and the room
// starts the agent without one, as it does when the live build fails.
type replayTopology struct{ text string }

func (t replayTopology) Build(_ context.Context, pipelineID string) (topology.Graph, error) {
	if t.text == "" {
		return topology.Graph{}, errors.New("evals: the scenario records no pipeline map")
	}
	return topology.Graph{PipelineID: pipelineID, Nodes: []topology.Node{}, Edges: []topology.Edge{}}, nil
}

func (t replayTopology) Render(topology.Graph) string { return t.text }

func (replayTopology) Trace(context.Context, string, string, string) ([]events.TraceStage, error) {
	return nil, errors.New("evals: trace is not replayed")
}

func (replayTopology) LayerOwner(string, string) (string, bool) { return "", false }

// roomAgent wraps the runner the room starts. It passes everything through
// and only observes: what alert the room built, when an attempt ends, and
// when the agent waits for human input at the run level (the idle in which a
// person would type). At that wait it holds the agent until the harness has
// acted, so a scripted input lands where the scenario puts it, whatever the
// executor's or the model's timing.
type roomAgent struct {
	inner   agent.Runner
	idle    chan struct{}
	release chan struct{}
	ended   chan error
	stop    chan struct{}

	mu       sync.Mutex
	alert    agent.Alert
	idles    int
	attempts int
}

func newRoomAgent(inner agent.Runner) *roomAgent {
	return &roomAgent{
		inner: inner, idle: make(chan struct{}), release: make(chan struct{}),
		ended: make(chan error, 1), stop: make(chan struct{}),
	}
}

func (a *roomAgent) run(ctx context.Context, alert agent.Alert, d agent.Deps, o agent.Options) error {
	a.mu.Lock()
	a.alert = alert
	a.attempts++
	a.mu.Unlock()
	d.Inbox = &harnessInbox{Inbox: d.Inbox, runDone: ctx.Done(), a: a}
	err := a.inner(ctx, alert, d, o)
	select {
	case a.ended <- err:
	case <-a.stop:
	}
	return err
}

func (a *roomAgent) snapshot() (agent.Alert, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.alert, a.idles
}

// harnessInbox is the room's inbox with the idle hook.
type harnessInbox struct {
	agent.Inbox
	runDone <-chan struct{}
	a       *roomAgent
}

// Wait implements agent.Inbox. Only the run-level wait (the run's own
// context, the same test the room's gatedInbox makes) is an idle; the
// bounded step-result waits pass straight through.
func (w *harnessInbox) Wait(ctx context.Context) error {
	if ctx.Done() != w.runDone {
		return w.Inbox.Wait(ctx)
	}
	w.a.mu.Lock()
	w.a.idles++
	w.a.mu.Unlock()
	select {
	case w.a.idle <- struct{}{}:
		select {
		case <-w.a.release:
		case <-w.a.stop:
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-w.a.stop:
	case <-ctx.Done():
		return ctx.Err()
	}
	return w.Inbox.Wait(ctx)
}

// execScript plays the writes of approved steps. The room's executor calls
// Execute after its own Validate and CheckScope; nothing reaches a real
// system. A step with the armed input's action gets that input's outcome
// (the first such step: the executor runs them in order); every other step
// succeeds. Each executed step goes to the replayer, whose transitions move
// the recorded stack to its next phase.
type execScript struct {
	rp *replayer
	// log notes each executed step (action and target) for the report; nil
	// in tests that do not read it.
	log *writeLog

	mu    sync.Mutex
	armed *Input
}

func (x *execScript) arm(in Input) {
	x.mu.Lock()
	x.armed = &in
	x.mu.Unlock()
}

func (x *execScript) execute(action string, params json.RawMessage) (string, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.note(action, params)
	if in := x.armed; in != nil && in.Action == action {
		x.armed = nil
		if in.Params != nil && !paramsMatch(in.Params, params) {
			// Not the step the scenario scripted (another task, another
			// connector): the target says no, and the stack does not move.
			x.rp.executedWrongParams(action)
			return "", fmt.Errorf("actions: %s %s: not found", action, compactJSON(params))
		}
		ok := in.OK != nil && *in.OK
		x.rp.executed(action, &ok)
		if !ok {
			return "", errors.New(in.Text)
		}
		return in.Text, nil
	}
	ok := true
	x.rp.executed(action, &ok)
	return "succeeded", nil
}

// note logs an executed step with its target names only: the params of a
// step can carry record keys or config values, which a report must not.
func (x *execScript) note(action string, params json.RawMessage) {
	if x.log == nil {
		return
	}
	var all map[string]any
	_ = json.Unmarshal(params, &all) // not an object: logged without a target
	args := map[string]any{}
	for _, k := range writeTargetKeys {
		if v, ok := all[k].(string); ok {
			args[k] = v
		}
	}
	x.log.record(action, args)
}

// roomRun is the state of one run on the room path.
type roomRun struct {
	sc    Scenario
	fx    *roomFixture
	svc   *room.Service
	store *memstore.Store
	rp    *replayer
	exec  *execScript
	ag    *roomAgent
	log   *slog.Logger

	roomID string
	next   int // next scenario input
}

// runOne runs the agent once on sc through a room. The returned error is a
// harness failure (the run could not even start); agent failures and
// failures of the room path itself (the watcher never fired, an approval was
// refused) are recorded in the RunResult.
func runOne(ctx context.Context, sc Scenario, run int, defs []tools.Tool, cfg Config) (RunResult, Transcript, error) {
	rp, err := newScenarioReplayer(sc)
	if err != nil {
		return RunResult{}, Transcript{}, err
	}
	reg, err := rp.registry(defs)
	if err != nil {
		return RunResult{}, Transcript{}, err
	}
	live, wl, err := liveActions(sc.Resources, sc.Recordings)
	if err != nil {
		return RunResult{}, Transcript{}, err
	}
	exec := &execScript{rp: rp, log: wl}
	attempts := &attemptLog{exec: exec}
	areg, err := attempts.wrap(live)
	if err != nil {
		return RunResult{}, Transcript{}, err
	}
	fx, err := newRoomFixture(sc)
	if err != nil {
		return RunResult{}, Transcript{}, err
	}

	st := memstore.New()
	ag := newRoomAgent(cfg.Runner)
	log := cfg.Logger.With("scenario", sc.Name, "run", run)
	svc := room.New(room.Deps{
		Store: st,
		StackFor: func(string) room.PipelineStack {
			return room.PipelineStack{Tools: reg, Actions: areg}
		},
		Topology: replayTopology{text: sc.Alert.PipelineMap},
		Users:    fx.users,
		Owners:   fx.owners,
		Run:      ag.run,
		Options:  cfg.Options,
		Logger:   log,
	})
	rr := &roomRun{sc: sc, fx: fx, svc: svc, store: st, rp: rp, exec: exec, ag: ag, log: log}

	runCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	start := time.Now()
	runErr := rr.drive(runCtx)
	took := time.Since(start)
	// The agent is no longer driven; stop the service and every goroutine it
	// started (the agent, the executor) before reading the log.
	close(ag.stop)
	svc.Close()

	timedOut := errors.Is(runCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
	if timedOut && errors.Is(runErr, context.DeadlineExceeded) {
		runErr = nil // reported as TimedOut, not twice
	}
	if ctx.Err() != nil {
		return RunResult{}, Transcript{}, ctx.Err()
	}

	tr := Transcript{Misses: rp.Misses(), Attempts: attempts.snapshot(), Writes: wl.snapshot(), Replay: rp.trace()}
	alert, idles := ag.snapshot()
	tr.Idles = idles
	if rr.roomID != "" {
		evs, err := roomEvents(context.Background(), st, rr.roomID)
		if err != nil {
			return RunResult{}, Transcript{}, err
		}
		tr.Events = evs
		tr.Room = &RoomTrace{RoomID: rr.roomID, Entry: sc.entry(), Alert: alert}
	}
	res, err := gradeFinal(ctx, cfg, cfg.gate(), sc, Judge(sc, tr, runErr, timedOut))
	if err != nil {
		return RunResult{}, Transcript{}, err
	}
	res.Run = run
	res.DurationMS = took.Milliseconds()
	res = withReplayChecks(sc, tr, res)
	res.Provenance = provenanceFor(cfg, sc, rr.roomID)
	return res, tr, nil
}

// drive opens the room, then answers each idle of the agent with the next
// scripted input, until the inputs run out (the run ends there, as the old
// inbox's ErrInboxClosed did), the agent's run ends, or ctx ends.
func (rr *roomRun) drive(ctx context.Context) error {
	roomID, err := rr.open(ctx)
	if err != nil {
		return err
	}
	rr.roomID = roomID
	rr.rp.setEvents(func() int {
		evs, err := rr.store.Since(context.Background(), roomID, 0, maxRoomEvents)
		if err != nil {
			return 0
		}
		return len(evs)
	})
	if len(rr.sc.Inputs) > 0 {
		// Steers come from the driver; nobody holds control of a new alert
		// room, so the scripted driver takes it, as a person would.
		if _, err := rr.svc.GrantControl(ctx, rr.fx.driver, roomID, rr.fx.driver.ID); err != nil {
			return fmt.Errorf("evals: take control of the room: %w", err)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-rr.ag.ended:
			return err
		case <-rr.ag.idle:
		}
		delivered, err := rr.deliverNext(ctx)
		if err != nil {
			return err
		}
		if !delivered {
			return nil
		}
		select {
		case rr.ag.release <- struct{}{}:
		case err := <-rr.ag.ended:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// open opens the room the way the scenario's incident would.
func (rr *roomRun) open(ctx context.Context) (string, error) {
	if rr.sc.Start.By == StartWatcher {
		return openByWatcher(ctx, rr.sc, rr.svc, rr.fx.owners, rr.log)
	}
	snap, _, err := rr.svc.OpenAlertRoom(ctx, alertin.Alert{
		// A signed webhook alert; the agent reads its title, pipeline and
		// the room's pipeline map (agent.Alert.Raw is not in the prompt).
		Source:     alertin.Generic,
		Status:     alertin.StatusFiring,
		Title:      rr.sc.Alert.Title,
		PipelineID: rr.sc.Pipeline.ID,
		DedupKey:   "eval:" + rr.sc.Name,
	})
	if err != nil {
		return "", fmt.Errorf("evals: open the alert room: %w", err)
	}
	return snap.ID, nil
}

// deliverNext acts out the next scripted input that has something to do. It
// reports false when none is left.
func (rr *roomRun) deliverNext(ctx context.Context) (bool, error) {
	for rr.next < len(rr.sc.Inputs) {
		i := rr.next
		in := rr.sc.Inputs[i]
		rr.next++
		switch {
		case in.Kind == "steer":
			if _, err := rr.svc.Steer(ctx, rr.fx.driver, rr.roomID, in.Text); err != nil {
				return false, fmt.Errorf("evals: inputs[%d]: steer: %w", i, err)
			}
			return true, nil
		case in.Kind == "comment":
			// Only a comment that mentions the agent reaches it in a room;
			// a scenario comment is written for the agent.
			text := in.Text
			if !strings.Contains(strings.ToLower(text), "@agent") {
				text = "@agent " + text
			}
			if _, err := rr.svc.AddComment(ctx, rr.fx.commenters[in.From], rr.roomID, in.StepID, text); err != nil {
				return false, fmt.Errorf("evals: inputs[%d]: comment: %w", i, err)
			}
			return true, nil
		case in.Kind == "action_result" && in.Action != "":
			done, err := rr.executeInput(ctx, i, in)
			if err != nil || done {
				return done, err
			}
		default:
			// Validation refuses these on the room path; never deliver a
			// result no room could produce.
			return false, fmt.Errorf("evals: inputs[%d]: a %s without an action cannot happen in a room", i, in.Kind)
		}
	}
	return false, nil
}

// executeInput approves the newest open proposal of the run that has a step
// with in.Action, as every required layer's owner, and waits for the room's
// executor to finish it. It reports false (and records the skip) when no such
// proposal exists.
func (rr *roomRun) executeInput(ctx context.Context, i int, in Input) (bool, error) {
	evs, err := roomEvents(ctx, rr.store, rr.roomID)
	if err != nil {
		return false, err
	}
	var target *events.ProposalCreatedPayload
	for j := len(evs) - 1; j >= 0 && target == nil; j-- {
		if evs[j].Type != events.ProposalCreated {
			continue
		}
		var p events.ProposalCreatedPayload
		if json.Unmarshal(evs[j].Payload, &p) != nil {
			continue
		}
		row, err := rr.store.GetProposal(ctx, p.ProposalID)
		if err != nil || row.Status != "open" {
			continue
		}
		for _, s := range p.Steps {
			if s.Action == in.Action {
				target = &p
				break
			}
		}
	}
	if target == nil {
		rr.rp.skip(fmt.Sprintf("inputs[%d]: no open proposal of this run has a %s step to run", i, in.Action))
		return false, nil
	}
	rr.exec.arm(in)
	for _, layer := range target.RequiredLayers {
		owner, ok := rr.fx.users.Get(ownerUserID(layer))
		if !ok {
			return false, fmt.Errorf("evals: inputs[%d]: no owner for layer %q", i, layer)
		}
		if _, err := rr.svc.ApproveProposal(ctx, owner, target.ProposalID, layer, target.ParamsHash); err != nil {
			return false, fmt.Errorf("evals: inputs[%d]: approve %s for %s: %w", i, target.ProposalID, layer, err)
		}
	}
	wctx, cancel := context.WithTimeout(ctx, executionTimeout)
	defer cancel()
	for {
		row, err := rr.store.GetProposal(wctx, target.ProposalID)
		if err != nil {
			return false, fmt.Errorf("evals: inputs[%d]: wait for the executor: %w", i, err)
		}
		if row.Status == "executed" || row.Status == "failed" {
			return true, nil
		}
		select {
		case <-wctx.Done():
			return false, fmt.Errorf("evals: inputs[%d]: wait for the executor: %w", i, wctx.Err())
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// maxRoomEvents is far above any run's log; roomEvents pages anyway.
const maxRoomEvents = 1 << 20

// roomEvents reads the room's whole log as the transcript.
func roomEvents(ctx context.Context, st *memstore.Store, roomID string) ([]RecordedEvent, error) {
	var out []RecordedEvent
	after := int64(0)
	for {
		evs, err := st.Since(ctx, roomID, after, 1000)
		if err != nil {
			return nil, fmt.Errorf("evals: read the room log: %w", err)
		}
		for _, e := range evs {
			out = append(out, RecordedEvent{Type: e.Type, Payload: e.Payload, At: e.At, Actor: e.Actor})
			after = e.Seq
		}
		if len(evs) < 1000 {
			return out, nil
		}
	}
}
