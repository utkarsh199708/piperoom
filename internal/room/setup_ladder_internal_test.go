package room

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/setup/plan"
	"piperoom/internal/tools"
)

// The verification ladder runner (U1-8b; docs/design/setup-agent.md §3.4):
// the nonce, the canary insert as a system-origin proposal, the reads issued
// by the room with their step and receipt ids, setup.verified as the ladder
// runner's only event, and rung 8's timeout as a note.
//
// The rig is the tier rig (setup_tiers_internal_test.go): a real event log, a
// real proposal/approval/executor path, fake actions and a fake clock. The
// tests add a canary action and the room's read tools.

// ladderCanaryAction is the fake source_insert_canary: it records the nonce it
// was given (the ladder's own key) and, on Execute, counts as the insert.
type ladderCanaryAction struct {
	mu     sync.Mutex
	layer  string
	nonces []string
	tables []string
	runs   int
	tier   func(class string) string // nil: the op's floor raised to class
}

func (a *ladderCanaryAction) Name() string                 { return setupLadderOpInsertCanary }
func (a *ladderCanaryAction) Layer() string                { return a.layer }
func (a *ladderCanaryAction) Describe() string             { return "insert the ladder's canary row" }
func (a *ladderCanaryAction) ParamsSchema() map[string]any { return map[string]any{"type": "object"} }

func (a *ladderCanaryAction) Validate(_ context.Context, params json.RawMessage) error {
	var p struct {
		Table string `json:"table"`
		Nonce string `json:"nonce"`
	}
	if err := json.Unmarshal(params, &p); err != nil || p.Table == "" || p.Nonce == "" {
		return errors.New("table and nonce are required")
	}
	return nil
}

func (a *ladderCanaryAction) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }

func (a *ladderCanaryAction) Preview(_ context.Context, params json.RawMessage) (actions.PreviewResult, error) {
	return actions.PreviewResult{Target: "canary", Current: "absent", Expected: "one row"}, nil
}

func (a *ladderCanaryAction) Tier(_ json.RawMessage, class string) (string, error) {
	if a.tier != nil {
		return a.tier(class), nil
	}
	return plan.TierOf(plan.TierT2, class)
}

func (a *ladderCanaryAction) Execute(_ context.Context, params json.RawMessage) (string, error) {
	var p struct {
		Table string `json:"table"`
		Nonce string `json:"nonce"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return "", err
	}
	a.mu.Lock()
	a.runs++
	a.nonces = append(a.nonces, p.Nonce)
	a.tables = append(a.tables, p.Table)
	a.mu.Unlock()
	return "inserted", nil
}

func (a *ladderCanaryAction) lastNonce() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.nonces) == 0 {
		return ""
	}
	return a.nonces[len(a.nonces)-1]
}

func (a *ladderCanaryAction) runCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.runs
}

// ladderTool is one fake read tool of the room.
type ladderTool struct {
	name string
	kind tools.Kind
	run  func(ctx context.Context, input json.RawMessage) (tools.Result, error)
}

func (t *ladderTool) Name() string                { return t.name }
func (t *ladderTool) Description() string         { return t.name }
func (t *ladderTool) InputSchema() map[string]any { return map[string]any{"type": "object"} }
func (t *ladderTool) Kind() tools.Kind            { return t.kind }
func (t *ladderTool) Run(ctx context.Context, in json.RawMessage) (tools.Result, error) {
	return t.run(ctx, in)
}

// ladderReads are the room's read tools: healthy connector and topic, a
// configurable key scan (the only place rungs 5 and 8 get their answer), no
// metrics endpoint, no registry, and a warehouse tool the ladder must never
// call (rung 7 is not applicable without a sink).
type ladderReads struct {
	mu        sync.Mutex
	keys      []string
	found     func(key string) bool
	warehouse bool
}

func (d *ladderReads) keysAsked() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string{}, d.keys...)
}

func (d *ladderReads) warehouseCalled() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.warehouse
}

func (d *ladderReads) tools() []tools.Tool {
	status := func(context.Context, json.RawMessage) (tools.Result, error) {
		return tools.Result{Summary: "RUNNING", Data: map[string]any{
			"state": "RUNNING",
			"tasks": []map[string]any{{"id": 0, "state": "RUNNING"}},
		}}, nil
	}
	return []tools.Tool{
		&ladderTool{name: "connect_status", kind: tools.KindRead, run: status},
		&ladderTool{name: "kafka_topic_offsets", kind: tools.KindRead, run: func(_ context.Context, in json.RawMessage) (tools.Result, error) {
			var p struct {
				Topic string `json:"topic"`
			}
			_ = json.Unmarshal(in, &p)
			return tools.Result{Summary: "one partition", Data: map[string]any{
				"topic": p.Topic, "total_records": int64(1),
				"partitions": []map[string]any{{"partition": 0, "start": 0, "end": 1}},
			}}, nil
		}},
		&ladderTool{name: "kafka_find_by_key", kind: tools.KindRead, run: func(_ context.Context, in json.RawMessage) (tools.Result, error) {
			var p struct {
				Topic string `json:"topic"`
				Key   string `json:"key"`
			}
			if err := json.Unmarshal(in, &p); err != nil {
				return tools.Result{}, err
			}
			d.mu.Lock()
			d.keys = append(d.keys, p.Key)
			found := d.found
			d.mu.Unlock()
			ok := false
			if found != nil {
				ok = found(p.Key)
			}
			return tools.Result{Summary: "scan", Data: map[string]any{
				"topic": p.Topic, "key": p.Key, "found": ok,
			}}, nil
		}},
		&ladderTool{name: "schema_registry_subjects", kind: tools.KindRead, run: func(context.Context, json.RawMessage) (tools.Result, error) {
			return tools.Result{Summary: "not configured", Data: map[string]any{"configured": false}}, nil
		}},
		&ladderTool{name: "warehouse_row", kind: tools.KindRead, run: func(context.Context, json.RawMessage) (tools.Result, error) {
			d.mu.Lock()
			d.warehouse = true
			d.mu.Unlock()
			return tools.Result{Summary: "row", Data: map[string]any{"found": true}}, nil
		}},
	}
}

// ladderRig is the tier rig with the canary action and the read tools.
type ladderRig struct {
	*tierRig
	canary *ladderCanaryAction
	reads  *ladderReads
}

func newLadderRig(t *testing.T, o tierOpts) *ladderRig {
	t.Helper()
	r := newTierRig(t, o)
	canary := &ladderCanaryAction{layer: "postgres"}
	if err := r.svc.actions.Register(canary); err != nil {
		t.Fatal(err)
	}
	reads := &ladderReads{}
	reg := tools.NewRegistry()
	for _, tool := range reads.tools() {
		if err := reg.Register(tool); err != nil {
			t.Fatal(err)
		}
	}
	r.svc.tools = reg
	// The waits a test cannot sit out: rung 8 times out in a moment, and the
	// canary's approval has room for the test's own click.
	r.svc.ladderTiming = &setupLadderTimings{
		Approval: 10 * time.Second,
		Event:    5 * time.Second,
		Change:   300 * time.Millisecond,
		Poll:     10 * time.Millisecond,
		Step:     2 * time.Second,
	}
	return &ladderRig{tierRig: r, canary: canary, reads: reads}
}

// roomEvents reads the room's whole log.
func roomEvents(t *testing.T, svc *Service, roomID string) []events.Event {
	t.Helper()
	evs, _, err := svc.Events(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	return evs
}

// ladderVerified returns the room's setup.verified payloads in order.
func ladderVerified(t *testing.T, svc *Service, roomID string) []events.SetupVerifiedPayload {
	t.Helper()
	var out []events.SetupVerifiedPayload
	for _, e := range roomEvents(t, svc, roomID) {
		if e.Type != events.SetupVerified {
			continue
		}
		var p events.SetupVerifiedPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			t.Fatalf("decode setup.verified: %v", err)
		}
		out = append(out, p)
	}
	return out
}

// ladderAwaiting returns the room's setup.awaiting_change payloads.
func ladderAwaiting(t *testing.T, svc *Service, roomID string) []events.SetupAwaitingChangePayload {
	t.Helper()
	var out []events.SetupAwaitingChangePayload
	for _, e := range roomEvents(t, svc, roomID) {
		if e.Type != events.SetupAwaitingChange {
			continue
		}
		var p events.SetupAwaitingChangePayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			t.Fatalf("decode setup.awaiting_change: %v", err)
		}
		out = append(out, p)
	}
	return out
}

// waitLadderCanaryProposal waits for the system-origin proposal the ladder
// created and returns its id and params.
func waitLadderCanaryProposal(t *testing.T, svc *Service, roomID string, after int) (string, []events.ProposalStep) {
	t.Helper()
	var id string
	var steps []events.ProposalStep
	waitForCond(t, 10*time.Second, func() bool {
		n := 0
		for _, e := range roomEvents(t, svc, roomID) {
			if e.Type != events.ProposalCreated {
				continue
			}
			var p events.ProposalCreatedPayload
			if json.Unmarshal(e.Payload, &p) != nil || p.Origin != setupProposalOriginSystem {
				continue
			}
			if n == after {
				id, steps = p.ProposalID, p.Steps
				return true
			}
			n++
		}
		return false
	}, "the ladder never proposed the canary insert")
	return id, steps
}

// ladderProposalNonce is the nonce a ladder canary proposal carries: the key
// its run follows to the topic.
func ladderProposalNonce(t *testing.T, steps []events.ProposalStep) string {
	t.Helper()
	if len(steps) != 1 {
		t.Fatalf("canary proposal steps = %+v, want one", steps)
	}
	var p struct {
		Nonce string `json:"nonce"`
	}
	if err := json.Unmarshal(steps[0].Params, &p); err != nil {
		t.Fatal(err)
	}
	if p.Nonce == "" {
		t.Fatalf("the canary proposal carries no nonce: %s", steps[0].Params)
	}
	return p.Nonce
}

// waitSetupVerified waits until the room holds n setup.verified events.
func waitSetupVerified(t *testing.T, svc *Service, roomID string, n int) []events.SetupVerifiedPayload {
	t.Helper()
	waitForCond(t, 15*time.Second, func() bool {
		return len(ladderVerified(t, svc, roomID)) >= n
	}, "the ladder never wrote setup.verified")
	return ladderVerified(t, svc, roomID)
}

// assertRungReceipts checks the row's record rule: every rung names a step and
// a receipt that exist in the room log after the last executed step. The
// not-applicable rung issues no read, so it has a step and no receipt.
func assertRungReceipts(t *testing.T, svc *Service, roomID string, rungs []events.SetupRung) {
	t.Helper()
	evs := roomEvents(t, svc, roomID)
	var lastExec int64
	steps := map[string]int64{}
	receipts := map[string]int64{}
	for _, e := range evs {
		switch e.Type {
		case events.ActionExecuted:
			lastExec = e.Seq
		case events.AgentStepStart:
			var p events.AgentStepStartPayload
			if json.Unmarshal(e.Payload, &p) == nil {
				steps[p.StepID] = e.Seq
			}
		case events.AgentToolCall:
			var p events.AgentToolCallPayload
			if json.Unmarshal(e.Payload, &p) == nil {
				receipts[p.ToolUseID] = e.Seq
			}
		}
	}
	if lastExec == 0 {
		t.Fatal("no action.executed in the log: the ladder never ran a step")
	}
	for _, rung := range rungs {
		if rung.StepID == "" {
			t.Errorf("rung %s names no step id", rung.Name)
			continue
		}
		seq, ok := steps[rung.StepID]
		if !ok || seq <= lastExec {
			t.Errorf("rung %s: step %q is not in the log after the last executed step (%d)", rung.Name, rung.StepID, lastExec)
		}
		if rung.Name == plan.RungSinkRow {
			if rung.ReceiptID != "" {
				t.Errorf("rung %s issued no read, so it must name no receipt", rung.Name)
			}
			continue
		}
		seq, ok = receipts[rung.ReceiptID]
		if rung.ReceiptID == "" || !ok || seq <= lastExec {
			t.Errorf("rung %s: receipt %q is not in the log after the last executed step (%d)", rung.Name, rung.ReceiptID, lastExec)
		}
	}
}

// TestSetupLadderNonceIsSixteenRandomBytes: the nonce is 16 crypto/rand bytes
// in hex, and two runs never draw the same one.
func TestSetupLadderNonceIsSixteenRandomBytes(t *testing.T) {
	a, err := setupLadderNonce()
	if err != nil {
		t.Fatal(err)
	}
	b, err := setupLadderNonce()
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{a, b} {
		raw, err := hex.DecodeString(n)
		if err != nil || len(raw) != 16 {
			t.Fatalf("nonce %q is not 16 bytes of hex (%v, %d bytes)", n, err, len(raw))
		}
	}
	if a == b {
		t.Fatal("two runs drew the same nonce")
	}
}

// TestSetupLadderKeyedPredicate: a rung about a keyed change is decided only
// by a read that answered for this run's key. A stale event from a previous
// run (another key), or a read with no key at all, never matches.
func TestSetupLadderKeyedPredicate(t *testing.T) {
	rungs, err := plan.Ladder("postgres", "compose", plan.SinkNone)
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{4, 7} { // canary_event, person_change
		predicate := setupLadderKeyed("new-key", rungs[i].Predicate)
		cases := []struct {
			name string
			data string
			want bool
		}{
			{"a previous run's key is stale", `{"key":"old-key","found":true}`, false},
			{"a read with no key answers nothing", `{"found":true}`, false},
			{"this run's key, not found", `{"key":"new-key","found":false}`, false},
			{"this run's key, found", `{"key":"new-key","found":true}`, true},
		}
		for _, c := range cases {
			ok, _ := predicate(json.RawMessage(c.data))
			if ok != c.want {
				t.Errorf("%s (%s): ok = %v, want %v", rungs[i].Name, c.name, ok, c.want)
			}
		}
	}
}

// TestSetupLadderOnlyTheRunnerWritesVerified: setup.verified and
// setup.awaiting_change are the ladder runner's. The agent's Sink refuses
// them (it accepts the agent's request for the ladder), and so does the
// generic setup audit sink.
func TestSetupLadderOnlyTheRunnerWritesVerified(t *testing.T) {
	r := newLadderRig(t, tierOpts{})
	roomID := r.openSetup("shipments-setup", "a", "alice", "db.acme:5432", "")
	ctx := context.Background()
	payload := events.SetupVerifiedPayload{PlanID: "pl_1"}
	if err := r.svc.agentEmit(ctx, roomID, outsideRun, events.SetupVerified, payload); err == nil {
		t.Fatal("the agent's Sink accepted setup.verified")
	}
	if err := r.svc.agentEmit(ctx, roomID, outsideRun, events.SetupAwaitingChange, events.SetupAwaitingChangePayload{Table: "public.orders"}); err == nil {
		t.Fatal("the agent's Sink accepted setup.awaiting_change")
	}
	if err := r.svc.EmitSetup(ctx, roomID, events.SetupVerified, payload); err == nil {
		t.Fatal("EmitSetup accepted setup.verified")
	}
	if err := r.svc.EmitSetup(ctx, roomID, events.SetupAwaitingChange, events.SetupAwaitingChangePayload{}); err == nil {
		t.Fatal("EmitSetup accepted setup.awaiting_change")
	}
	// The agent's request is not a claim: the Sink accepts it and the room
	// answers with the ladder (here: a note, because the room has no plan).
	if err := r.svc.agentEmit(ctx, roomID, outsideRun, TypeSetupVerificationRequested, struct {
		At time.Time `json:"at"`
	}{At: time.Now().UTC()}); err != nil {
		t.Fatalf("the agent's Sink refused its verification request: %v", err)
	}
	waitForCond(t, 5*time.Second, func() bool {
		for _, e := range roomEvents(t, r.svc, roomID) {
			if e.Type != events.AgentMessage {
				continue
			}
			var p events.AgentMessagePayload
			if json.Unmarshal(e.Payload, &p) == nil && p.Text != "" {
				return true
			}
		}
		return false
	}, "the request did not reach the ladder runner")
	for _, p := range ladderVerified(t, r.svc, roomID) {
		t.Fatalf("a request wrote setup.verified: %+v", p)
	}
}

// TestSetupLadderRunsFromPlanAndRecordsReceipts is the row's main path: the
// canary insert as a system-origin proposal on the ordinary approval and
// executor path, rungs 1 to 7 decided on the reads' Data, the step and
// receipt ids in the log after the last executed step, and rung 8 timing out
// as a note rather than a failure.
func TestSetupLadderRunsFromPlanAndRecordsReceipts(t *testing.T) {
	r := newLadderRig(t, tierOpts{})
	roomID := r.openSetup("shipments-setup", "a", "alice", "db.acme:5432", "")
	r.plan(roomID, "db.acme:5432", "compose_up", "connect_create_connector")
	// Only the nonce the canary insert actually carried is found: the person
	// never makes the change of rung 8.
	r.reads.found = func(key string) bool { return key == r.canary.lastNonce() }

	if err := r.svc.startSetupLadder(roomID); err != nil {
		t.Fatalf("start ladder: %v", err)
	}
	pid, steps := waitLadderCanaryProposal(t, r.svc, roomID, 0)
	if len(steps) != 1 || steps[0].Action != setupLadderOpInsertCanary {
		t.Fatalf("canary proposal steps = %+v, want one %s step", steps, setupLadderOpInsertCanary)
	}
	var params struct {
		Table string `json:"table"`
	}
	if err := json.Unmarshal(steps[0].Params, &params); err != nil {
		t.Fatal(err)
	}
	nonce := ladderProposalNonce(t, steps)
	// The ordinary route: the canary is a T2 step on a shared target, so the
	// tenant admin approves it like any other write.
	if err := r.approve(roomID, pid, "postgres", "bob"); err != nil {
		t.Fatalf("approve the canary insert: %v", err)
	}
	verified := waitSetupVerified(t, r.svc, roomID, 2)

	if got := r.canary.lastNonce(); got != nonce {
		t.Fatalf("the executed canary carries %q, the proposal %q", got, nonce)
	}
	if len(nonce) != 32 {
		t.Fatalf("nonce %q is not 16 bytes in hex", nonce)
	}

	// The first verified event is rungs 1 to 7: the card does not wait on the
	// person (U1-14 gates registration on exactly these rungs).
	first := verified[0]
	if first.PlanID != "pl_1" {
		t.Fatalf("plan_id = %q, want pl_1", first.PlanID)
	}
	if first.NonceHash != setupLadderHash(nonce) {
		t.Fatalf("nonce_hash = %q, want SHA-256 of the run's nonce", first.NonceHash)
	}
	if len(first.Rungs) != 7 {
		t.Fatalf("rungs = %d, want 7", len(first.Rungs))
	}
	for _, rung := range first.Rungs {
		if rung.Name == plan.RungSinkRow {
			if !rung.OK || rung.Note != "no sink was set up" {
				t.Fatalf("rung 7 = %+v, want green with the no-sink note", rung)
			}
			continue
		}
		if !rung.OK {
			t.Fatalf("rung %s is red: %s", rung.Name, rung.Note)
		}
	}
	if first.FirstEventLatencyMS == nil || *first.FirstEventLatencyMS < 0 {
		t.Fatalf("first_event_latency_ms = %v, want a measured latency", first.FirstEventLatencyMS)
	}
	// The keys the ladder scanned: its own nonce (rung 5) and the change key
	// (rung 8) — never a key from anywhere else.
	keys := r.reads.keysAsked()
	if len(keys) == 0 || keys[0] != nonce {
		t.Fatalf("rung 5 scanned %v, want the run's nonce first", keys)
	}

	// The last verified event adds rung 8: the wait ended as a note, and the
	// pipeline is not failed.
	last := verified[len(verified)-1]
	if len(last.Rungs) != 8 {
		t.Fatalf("rungs = %d, want 8", len(last.Rungs))
	}
	person := last.Rungs[7]
	if person.Name != plan.RungPersonChange || person.OK {
		t.Fatalf("rung 8 = %+v, want red (the person made no change)", person)
	}
	if person.Note == "" {
		t.Fatal("rung 8 has no note explaining the wait's end")
	}
	// Rungs 1 to 7 are unchanged by rung 8's timeout.
	for _, rung := range last.Rungs[:7] {
		if !rung.OK {
			t.Fatalf("rung %s went red after rung 8: %s", rung.Name, rung.Note)
		}
	}
	assertRungReceipts(t, r.svc, roomID, last.Rungs)
	if r.reads.warehouseCalled() {
		t.Fatal("the ladder read the warehouse for a setup with no sink")
	}
	// The room was asked for exactly one change, and its SQL carries the key
	// the ladder waits for.
	awaiting := ladderAwaiting(t, r.svc, roomID)
	if len(awaiting) != 1 {
		t.Fatalf("setup.awaiting_change events = %d, want 1", len(awaiting))
	}
	if awaiting[0].Table != "public.piperoom_canary" {
		t.Fatalf("awaiting table = %q, want the canary table", awaiting[0].Table)
	}
	change := keys[len(keys)-1]
	if !containsString(awaiting[0].ExampleSQL, change) {
		t.Fatalf("the example SQL %q does not carry the change key %q", awaiting[0].ExampleSQL, change)
	}
	// Nothing failed: the room has no failed step or proposal.
	for _, e := range roomEvents(t, r.svc, roomID) {
		if e.Type != events.ActionExecuted {
			continue
		}
		var p events.ActionExecutedPayload
		if json.Unmarshal(e.Payload, &p) == nil && !p.OK {
			t.Fatalf("a step failed during the ladder: %+v", p)
		}
	}
}

// containsString reports whether haystack holds needle. It is strings.Contains
// under a name the assertions read as a sentence.
func containsString(haystack, needle string) bool {
	return needle != "" && strings.Contains(haystack, needle)
}

// TestSetupLadderVerifiedFoldsIntoTheSnapshot: the ladder runner's
// setup.verified is the room's verdict, and the view folds it — the rungs of
// the current plan — while a run of an older plan says nothing about this one.
// The generic emit paths refuse the type (a test of its own), so the runner's
// own event is the one that must reach the snapshot.
func TestSetupLadderVerifiedFoldsIntoTheSnapshot(t *testing.T) {
	r := newLadderRig(t, tierOpts{})
	ctx := context.Background()
	roomID := r.openSetup("shipments-setup", "a", "alice", "db.acme:5432", "")
	r.plan(roomID, "db.acme:5432", "compose_up")

	// What the runner writes: under the room's lock, as systemActor.
	verify := func(planID string, rungs ...events.SetupRung) {
		t.Helper()
		st := r.svc.lockCachedRoom(roomID)
		defer r.svc.unlockRoom(st)
		if _, err := r.svc.emitLocked(ctx, st, roomID, events.SetupVerified, systemActor, events.SetupVerifiedPayload{
			PlanID: planID, NonceHash: setupLadderHash("nonce"), Rungs: rungs,
		}); err != nil {
			t.Fatalf("emit setup.verified: %v", err)
		}
	}
	verify("pl_1",
		events.SetupRung{Name: plan.RungConnector, OK: true, Note: "the connector is RUNNING"},
		events.SetupRung{Name: plan.RungCanaryEvent, OK: false, Note: "not yet"},
	)
	snap := mustRoom(t, r.svc, roomID)
	if snap.Setup == nil || snap.Setup.PlanID != "pl_1" {
		t.Fatalf("setup snapshot = %+v, want plan pl_1", snap.Setup)
	}
	if len(snap.Setup.Verified) != 2 {
		t.Fatalf("verified = %+v, want the run's two rungs", snap.Setup.Verified)
	}
	if got := snap.Setup.Verified[0]; got.Name != plan.RungConnector || !got.OK || got.Note == "" {
		t.Fatalf("rung 1 = %+v, want the green connector rung", got)
	}
	if got := snap.Setup.Verified[1]; got.Name != plan.RungCanaryEvent || got.OK {
		t.Fatalf("rung 5 = %+v, want the red canary event rung", got)
	}

	// A verdict of another plan (an older run) leaves this plan's rungs alone.
	verify("pl_old", events.SetupRung{Name: plan.RungConnector, OK: false})
	snap = mustRoom(t, r.svc, roomID)
	if len(snap.Setup.Verified) != 2 || !snap.Setup.Verified[0].OK {
		t.Fatalf("verified = %+v, want the current plan's rungs unchanged", snap.Setup.Verified)
	}
}

// TestSetupLadderStaleCanaryEventDoesNotMatch: a second run is not decided by
// the first run's result. Its canary is never approved, and a result for the
// first run's proposal arriving after the second run started is ignored: the
// run draws a fresh nonce and rung 5 stays red with the reason.
func TestSetupLadderStaleCanaryEventDoesNotMatch(t *testing.T) {
	r := newLadderRig(t, tierOpts{})
	ctx := context.Background()
	roomID := r.openSetup("shipments-setup", "a", "alice", "db.acme:5432", "")
	r.plan(roomID, "db.acme:5432", "compose_up")
	r.reads.found = func(string) bool { return false }

	// Run 1, approved: its canary executes.
	if err := r.svc.startSetupLadder(roomID); err != nil {
		t.Fatal(err)
	}
	pid1, _ := waitLadderCanaryProposal(t, r.svc, roomID, 0)
	if err := r.approve(roomID, pid1, "postgres", "bob"); err != nil {
		t.Fatalf("approve run 1's canary: %v", err)
	}
	waitSetupVerified(t, r.svc, roomID, 2)
	nonce1 := r.canary.lastNonce()

	// Run 2: its canary is never approved, and the approval wait is short.
	r.svc.ladderTiming = &setupLadderTimings{
		Approval: 300 * time.Millisecond, Event: 300 * time.Millisecond,
		Change: 50 * time.Millisecond, Poll: 10 * time.Millisecond, Step: 2 * time.Second,
	}
	if err := r.svc.startSetupLadder(roomID); err != nil {
		t.Fatal(err)
	}
	pid2, steps2 := waitLadderCanaryProposal(t, r.svc, roomID, 1)
	if pid2 == pid1 {
		t.Fatal("run 2 reused run 1's proposal")
	}
	// Run 2 draws its own nonce: its proposal is about another key, so run 1's
	// result can never decide it.
	nonce2 := ladderProposalNonce(t, steps2)
	if nonce2 == nonce1 {
		t.Fatal("run 2 drew run 1's nonce")
	}
	// A stale result for run 1's canary lands after run 2's proposal: it is
	// not run 2's insert, whatever it says.
	if err := r.svc.EmitSetup(ctx, roomID, events.ActionExecuted, events.ActionExecutedPayload{
		ProposalID: pid1, StepIndex: 0, OK: true, Output: "inserted",
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for len(ladderVerified(t, r.svc, roomID)) < 4 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	for _, e := range roomEvents(t, r.svc, roomID) {
		t.Logf("event %d %s %s", e.Seq, e.Type, string(e.Payload))
	}
	verified := ladderVerified(t, r.svc, roomID)
	if len(verified) < 4 {
		t.Fatalf("verified = %d", len(verified))
	}
	last := verified[len(verified)-1]
	if r.canary.runCount() != 1 {
		t.Fatalf("the canary action ran %d times, want once (run 2's insert never ran)", r.canary.runCount())
	}
	if r.canary.lastNonce() != nonce1 {
		t.Fatalf("the canary action ran %q, want only run 1's nonce %q", r.canary.lastNonce(), nonce1)
	}
	for _, rung := range last.Rungs {
		if rung.Name != plan.RungCanaryEvent {
			continue
		}
		if rung.OK {
			t.Fatal("run 2's rung 5 went green on run 1's stale result")
		}
		if !containsString(rung.Note, "waiting for its approval") {
			t.Fatalf("rung 5 note = %q, want the approval wait named", rung.Note)
		}
	}
}

// TestSetupLadderT1CanaryAutoRuns: on a local source the canary runs without a
// click — the setup_local policy schedules and approves it — because its class
// is the source target's local one, though the op's catalogue floor keeps the
// tier at T2 (design §3.4: the canary insert is "auto on a local source"). The
// ladder then finds its nonce on the topic.
func TestSetupLadderT1CanaryAutoRuns(t *testing.T) {
	r := newLadderRig(t, tierOpts{local: true, delay: 20 * time.Second})
	roomID := r.openSetup("shipments-setup", "a", "alice", "127.0.0.1:5432", "")
	r.classify(roomID, "127.0.0.1:5432", plan.ClassLocal)
	r.plan(roomID, "127.0.0.1:5432", "compose_up")
	r.reads.found = func(key string) bool { return key == r.canary.lastNonce() }

	if err := r.svc.startSetupLadder(roomID); err != nil {
		t.Fatal(err)
	}
	pid, _ := waitLadderCanaryProposal(t, r.svc, roomID, 0)
	// No click: the countdown does it.
	waitPayload[events.ProposalScheduledPayload](t, r.svc, roomID, events.ProposalScheduled, pid)
	r.clock.Advance(20 * time.Second)
	r.tick()
	waitForCond(t, 10*time.Second, func() bool { return r.canary.runCount() == 1 }, "the canary insert never auto-ran")

	verified := waitSetupVerified(t, r.svc, roomID, 2)
	for _, rung := range verified[0].Rungs {
		if rung.Name == plan.RungSinkRow {
			continue
		}
		if !rung.OK {
			t.Fatalf("rung %s is red: %s", rung.Name, rung.Note)
		}
	}
}

// TestSetupLocalPolicyCoversTheLadderCanaryOnly: the setup_local policy's
// exception for the canary insert is narrow — the ladder's own proposal, that
// one op, at a local-class route. An agent's proposal never qualifies, a
// shared-class canary waits for its approver, and a T1 proposal still runs by
// policy as design §3.3 says.
func TestSetupLocalPolicyCoversTheLadderCanaryOnly(t *testing.T) {
	canary := Proposal{Origin: setupProposalOriginSystem, Steps: []events.ProposalStep{{Action: setupLadderOpInsertCanary}}}
	cases := []struct {
		name  string
		p     Proposal
		route setupRoute
		want  bool
	}{
		{"a T1 proposal", Proposal{Origin: events.ProposalOriginAgent, Steps: []events.ProposalStep{{Action: "compose_up"}}}, setupRoute{Tier: plan.TierT1, Class: plan.ClassShared}, true},
		{"the ladder's canary on a local source", canary, setupRoute{Tier: plan.TierT2, Class: plan.ClassLocal}, true},
		{"the ladder's canary on a shared source", canary, setupRoute{Tier: plan.TierT2, Class: plan.ClassShared}, false},
		{"an agent's canary step on a local source", Proposal{Origin: events.ProposalOriginAgent, Steps: canary.Steps}, setupRoute{Tier: plan.TierT2, Class: plan.ClassLocal}, false},
		{"a mixed proposal carrying the canary", Proposal{Origin: setupProposalOriginSystem, Steps: []events.ProposalStep{{Action: setupLadderOpInsertCanary}, {Action: "compose_up"}}}, setupRoute{Tier: plan.TierT2, Class: plan.ClassLocal}, false},
		{"a T3 proposal", Proposal{Origin: events.ProposalOriginAgent, Steps: []events.ProposalStep{{Action: "source_alter_system"}}}, setupRoute{Tier: plan.TierT3, Class: plan.ClassProduction}, false},
	}
	for _, c := range cases {
		if got := setupLocalRuns(c.p, c.route); got != c.want {
			t.Errorf("%s: setupLocalRuns = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestSetupLadderWithoutPlanIsANote: the ladder starts from the room's
// setup.plan. Without one it says so on the room's feed and writes no
// verdict.
func TestSetupLadderWithoutPlanIsANote(t *testing.T) {
	r := newLadderRig(t, tierOpts{})
	roomID := r.openSetup("shipments-setup", "a", "alice", "db.acme:5432", "")
	if err := r.svc.startSetupLadder(roomID); err != nil {
		t.Fatal(err)
	}
	waitForCond(t, 5*time.Second, func() bool {
		for _, e := range roomEvents(t, r.svc, roomID) {
			if e.Type != events.AgentMessage {
				continue
			}
			var p events.AgentMessagePayload
			if json.Unmarshal(e.Payload, &p) == nil && containsString(p.Text, "no setup plan") {
				return true
			}
		}
		return false
	}, "the ladder did not note the missing plan")
	if got := ladderVerified(t, r.svc, roomID); len(got) != 0 {
		t.Fatalf("a run without a plan wrote setup.verified: %+v", got)
	}
}

// TestRequestSetupVerification is POST /api/rooms/{id}/setup/verify's service
// rule: driver only, 409 no_runner without a paired runner, 409 already_decided
// while a run is in flight.
func TestRequestSetupVerification(t *testing.T) {
	r := newLadderRig(t, tierOpts{})
	ctx := context.Background()
	roomID := r.openSetup("shipments-setup", "a", "alice", "db.acme:5432", "")
	r.plan(roomID, "db.acme:5432", "compose_up")

	var re *Error
	if err := r.svc.RequestSetupVerification(ctx, r.user("alice"), roomID); !errors.As(err, &re) || re.Code != "no_runner" {
		t.Fatalf("without a runner = %v, want 409 no_runner", err)
	}
	if err := r.svc.AttachRunner(ctx, "CODE"+roomID, Runner{
		RoomID: roomID, Creator: events.Actor{Kind: events.ActorHuman, ID: "alice", Name: "Alice"},
		Label: "laptop", OS: "linux", Arch: "amd64", Token: "tok", TokenExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("attach runner: %v", err)
	}
	if err := r.svc.RequestSetupVerification(ctx, r.user("alice"), roomID); err != nil {
		t.Fatalf("the driver's verify: %v", err)
	}
	if err := r.svc.RequestSetupVerification(ctx, r.user("alice"), roomID); !errors.As(err, &re) || re.Code != "already_decided" {
		t.Fatalf("a second request = %v, want 409 already_decided", err)
	}
	if err := r.svc.RequestSetupVerification(ctx, r.user("carol"), roomID); !errors.As(err, &re) || re.Code != "not_driver" {
		t.Fatalf("a non-driver = %v, want 403 not_driver", err)
	}
}
