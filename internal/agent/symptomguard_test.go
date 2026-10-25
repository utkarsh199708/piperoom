package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// fakeWatch answers WatchStatuses with rows[i] on call i; the last answer
// repeats. err, when set, is every call's answer.
type fakeWatch struct {
	mu    sync.Mutex
	rows  [][]clients.WatchStatus
	err   error
	calls int
}

func (f *fakeWatch) WatchStatuses(_ context.Context, ids []string) ([]clients.WatchStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if len(f.rows) == 0 {
		return nil, nil
	}
	return f.rows[min(f.calls-1, len(f.rows)-1)], nil
}

func (f *fakeWatch) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// watchRow is a fresh status row of pipeline orders, checked 40 s ago.
func watchRow(sigs ...clients.WatchSignal) []clients.WatchStatus {
	return []clients.WatchStatus{{PipelineID: "orders", CheckedAt: time.Now().Add(-40 * time.Second), Signals: sigs}}
}

func firing(signal string) clients.WatchSignal {
	return clients.WatchSignal{Signal: signal, State: clients.WatchFiring, Severity: "sev2", Node: "pg:debezium_orders", Since: time.Now().Add(-20 * time.Minute)}
}

func clearing(signal string) clients.WatchSignal {
	s := firing(signal)
	at := time.Now().Add(-30 * time.Second)
	s.ClearingSince = &at
	return s
}

// symptomTools are guardTools with rule 1 reading w (nil: no reader).
func symptomTools(mode string, w clients.WatchStatusReader, logs *bytes.Buffer) (*updateHypothesisTool, *postMessageTool, *fakeSink, string) {
	hyp, msg, sink, id := guardTools(mode, logs)
	if w != nil {
		hyp.guard.symptom = newSymptomGuard(w, "orders", hyp.guard.log, time.Now, realClock{}.Sleep)
	}
	return hyp, msg, sink, id
}

const healthyText = "The pipeline is healthy again, nothing to fix."

func msgInput(text string) json.RawMessage {
	return json.RawMessage(`{"text":` + strconvQuote(text) + `}`)
}

// A healthy claim while slot_wal fires is refused in enforce, logged in log
// (names only), ignored in off, in update_hypothesis and post_message alike.
func TestSymptomGuardModes(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{GuardOff, GuardLog, GuardEnforce} {
		var logs bytes.Buffer
		w := &fakeWatch{rows: [][]clients.WatchStatus{watchRow(firing(owners.SignalSlotWAL))}}
		hyp, msg, sink, id := symptomTools(mode, w, &logs)
		_, herr := hyp.Run(ctx, hypInput(healthyText, id, "medium"))
		_, merr := msg.Run(ctx, msgInput(healthyText))
		switch mode {
		case GuardEnforce:
			for _, err := range []error{herr, merr} {
				if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "hypothesis guard (symptom_persists)") {
					t.Fatalf("enforce: %v, want a symptom_persists refusal", err)
				}
			}
			if n := len(sink.ofType(events.AgentHypothesis)) + len(sink.ofType(events.AgentMessage)); n != 0 {
				t.Fatalf("enforce: %d events emitted for refused calls", n)
			}
		default:
			if herr != nil || merr != nil {
				t.Fatalf("%s: errors %v / %v, want none", mode, herr, merr)
			}
		}
		wantCalls := map[string]int{GuardOff: 0, GuardLog: 2, GuardEnforce: 2}[mode]
		if got := w.callCount(); got != wantCalls {
			t.Fatalf("%s: %d store reads, want %d", mode, got, wantCalls)
		}
		would := strings.Count(logs.String(), "hypothesis guard would refuse")
		if want := map[string]int{GuardOff: 0, GuardLog: 2, GuardEnforce: 0}[mode]; would != want {
			t.Fatalf("%s: %d would-refuse lines, want %d:\n%s", mode, would, want, logs.String())
		}
		if mode == GuardLog {
			for _, want := range []string{`"rule":"symptom_persists"`, `"signals":["slot_wal"]`, `"pipeline":"orders"`, `"room":"r_1"`, `"tool":"post_message"`, `"tool":"update_hypothesis"`} {
				if !strings.Contains(logs.String(), want) {
					t.Fatalf("log mode line lacks %s:\n%s", want, logs.String())
				}
			}
		}
		if strings.Contains(logs.String(), "nothing to fix") || strings.Contains(logs.String(), "healthy again") {
			t.Fatalf("%s: the log carries the model's text:\n%s", mode, logs.String())
		}
	}
}

// The long-transaction room: long_tx fires and the agent says healthy. The
// refusal names the signal, the node, the watcher's last check and the reads.
func TestSymptomGuardLongTx(t *testing.T) {
	var logs bytes.Buffer
	row := watchRow(firing(owners.SignalLongTx))
	w := &fakeWatch{rows: [][]clients.WatchStatus{row}}
	hyp, _, _, id := symptomTools(GuardEnforce, w, &logs)
	_, err := hyp.Run(context.Background(), hypInput("Healthy: the slot is active and the connector confirms. Nothing to fix.", id, "medium"))
	if !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("err = %v, want a refusal", err)
	}
	t.Logf("refusal: %s", err)
	for _, want := range []string{
		"hypothesis guard (symptom_persists): a healthy verdict is blocked: the watcher still sees long_tx (node pg:debezium_orders, sev2, firing since ",
		"on pipeline orders (last check " + clockText(row[0].CheckedAt) + ", 40s ago)",
		"Re-read pg_streaming_liveness and pg_long_transactions and compare with the alert's numbers",
		"say it is recovering and the watcher has not cleared yet",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal lacks %q:\n%s", want, err)
		}
	}
	// The suggested wording is not itself a healthy claim.
	if healthyClaim("It is recovering and the watcher has not cleared yet.") {
		t.Fatal("the refusal's suggested wording is a healthy claim")
	}
}

// A refusal names only the reads the room offers; with none offered it still
// names what would answer.
func TestSymptomRefusalOfferedTools(t *testing.T) {
	sigs := []clients.WatchSignal{firing(owners.SignalSlotWAL), firing(owners.SignalConnectorDown)}
	now := time.Now()
	err := symptomRefusal("orders", now, now, sigs, func(n string) bool { return n == "pg_replication_slots" })
	if !strings.Contains(err.Error(), "Re-read pg_replication_slots and compare") || !strings.Contains(err.Error(), "slot_wal (") || !strings.Contains(err.Error(), "; connector_down (") {
		t.Fatalf("refusal = %s", err)
	}
	err = symptomRefusal("orders", now, now, sigs, func(string) bool { return false })
	if !strings.Contains(err.Error(), "Re-read pg_long_transactions, pg_replication_slots and connect_status and compare") {
		t.Fatalf("refusal with no offered tool = %s", err)
	}
}

// Everything that must pass without a refusal, in enforce mode.
func TestSymptomGuardPasses(t *testing.T) {
	stale := watchRow(firing(owners.SignalSlotWAL))
	stale[0].CheckedAt = time.Now().Add(-20 * time.Minute)
	other := watchRow(firing(owners.SignalSlotWAL))
	other[0].PipelineID = "payments"
	acked := firing(owners.SignalConsumerLag)
	acked.State = clients.WatchAcked
	for _, tt := range []struct {
		name  string
		w     *fakeWatch
		text  string
		reads int // -1: any
	}{
		{"self-recovered: clearing", &fakeWatch{rows: [][]clients.WatchStatus{watchRow(clearing(owners.SignalConnectorDown))}}, healthyText, 1},
		{"external alert: no row", &fakeWatch{}, healthyText, 1},
		{"external alert: row without symptom", &fakeWatch{rows: [][]clients.WatchStatus{watchRow()}}, healthyText, 1},
		{"row of another pipeline", &fakeWatch{rows: [][]clients.WatchStatus{other}}, healthyText, 1},
		{"stale row", &fakeWatch{rows: [][]clients.WatchStatus{stale}}, healthyText, 1},
		{"store error", &fakeWatch{err: errors.New("store down")}, healthyText, 1},
		{"advisory signals only", &fakeWatch{rows: [][]clients.WatchStatus{watchRow(
			firing(owners.SignalConfigRisk), firing(events.SignalForecast), firing(events.SignalConfigDrift))}}, healthyText, 1},
		{"not a healthy claim", &fakeWatch{rows: [][]clients.WatchStatus{watchRow(firing(owners.SignalSlotWAL))}},
			"Leading suspect: an open transaction holds the slot; it is recovering, the watcher has not cleared yet.", 0},
		{"nil reader", nil, healthyText, -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			var w clients.WatchStatusReader
			if tt.w != nil {
				w = tt.w
			}
			hyp, msg, _, id := symptomTools(GuardEnforce, w, &logs)
			if _, err := hyp.Run(context.Background(), hypInput(tt.text, id, "medium")); err != nil {
				t.Fatalf("update_hypothesis refused: %v", err)
			}
			if _, err := msg.Run(context.Background(), msgInput(tt.text)); err != nil {
				t.Fatalf("post_message refused: %v", err)
			}
			if tt.w != nil && tt.reads >= 0 && tt.w.callCount() != 2*tt.reads {
				t.Fatalf("store reads = %d, want %d", tt.w.callCount(), 2*tt.reads)
			}
			if tt.name == "store error" && strings.Count(logs.String(), "watch status unreadable") != 1 {
				t.Fatalf("store error: want one warn line per run:\n%s", logs.String())
			}
		})
	}
	// Acked still breaches: refused.
	var logs bytes.Buffer
	hyp, _, _, id := symptomTools(GuardEnforce, &fakeWatch{rows: [][]clients.WatchStatus{watchRow(acked)}}, &logs)
	if _, err := hyp.Run(context.Background(), hypInput(healthyText, id, "medium")); !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "consumer_lag (node pg:debezium_orders, sev2, acked and still breaching since") {
		t.Fatalf("acked: %v, want a refusal", err)
	}
}

// Rule 1 is off without a reader, with the guard off, and in setup mode.
func TestSymptomGuardConstruction(t *testing.T) {
	w := &fakeWatch{}
	base := Deps{Tools: tools.NewRegistry(), Inbox: &fakeInbox{}, Sink: &fakeSink{}, Logger: discardLogger()}
	for _, tt := range []struct {
		name  string
		alert Alert
		watch clients.WatchStatusReader
		mode  string
		on    bool
	}{
		{"reader, log mode", Alert{PipelineID: "orders"}, w, "", true},
		{"reader, enforce", Alert{PipelineID: "orders"}, w, GuardEnforce, true},
		{"no reader", Alert{PipelineID: "orders"}, nil, GuardEnforce, false},
		{"guard off", Alert{PipelineID: "orders"}, w, GuardOff, false},
		{"setup mode", Alert{PipelineID: "orders", Mode: ModeSetup}, w, GuardEnforce, false},
	} {
		d := base
		d.Watch = tt.watch
		r, err := newRunner(tt.alert, d, Options{HypothesisGuard: tt.mode})
		if err != nil {
			t.Fatalf("%s: newRunner: %v", tt.name, err)
		}
		if got := r.guard.symptomWatch() != nil; got != tt.on {
			t.Fatalf("%s: rule 1 on = %v, want %v", tt.name, got, tt.on)
		}
	}
}

// Two refusals, then the third healthy verdict is recorded at low confidence
// with one [guard] note; the cap is per rule, so rule 2's count is apart.
func TestSymptomGuardCap(t *testing.T) {
	ctx := context.Background()
	var logs bytes.Buffer
	hyp, _, sink, id := symptomTools(GuardEnforce, &fakeWatch{rows: [][]clients.WatchStatus{watchRow(firing(owners.SignalSlotWAL))}}, &logs)
	in := hypInput(healthyText, id, "medium")
	for i := 1; i <= guardMaxRefusals; i++ {
		if _, err := hyp.Run(ctx, in); !errors.Is(err, tools.ErrBadInput) {
			t.Fatalf("attempt %d: %v, want a refusal", i, err)
		}
	}
	res, err := hyp.Run(ctx, in)
	if err != nil {
		t.Fatalf("third attempt refused: %v", err)
	}
	if !strings.Contains(res.Summary, "low confidence after 2 refusals") || strings.Contains(res.Summary, "unbacked items") {
		t.Fatalf("summary = %q", res.Summary)
	}
	hs := sink.ofType(events.AgentHypothesis)
	if len(hs) != 1 {
		t.Fatalf("hypotheses = %d, want 1", len(hs))
	}
	var p events.AgentHypothesisPayload
	if err := json.Unmarshal(hs[0].payload, &p); err != nil {
		t.Fatal(err)
	}
	if p.Confidence != "low" || p.Text != healthyText {
		t.Fatalf("capped hypothesis = %+v", p)
	}
	ms := sink.ofType(events.AgentMessage)
	if len(ms) != 1 {
		t.Fatalf("[guard] notes = %d, want 1", len(ms))
	}
	var m events.AgentMessagePayload
	if err := json.Unmarshal(ms[0].payload, &m); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(m.Text, "[guard] healthy verdict accepted after two refusals; the watcher still shows slot_wal firing (last check ") {
		t.Fatalf("note = %q", m.Text)
	}
	t.Logf("note: %s", m.Text)
}

// Rule 2 runs first: a rule 2 refusal never reads the store.
func TestSymptomGuardAfterRuleTwo(t *testing.T) {
	var logs bytes.Buffer
	w := &fakeWatch{rows: [][]clients.WatchStatus{watchRow(firing(owners.SignalSlotWAL))}}
	hyp, _, _, id := symptomTools(GuardEnforce, w, &logs)
	_, err := hyp.Run(context.Background(), hypInput("Healthy, nothing to fix. Ruled out: the publication.", id, "medium"))
	if err == nil || !strings.Contains(err.Error(), "hypothesis guard (unbacked_rule_out)") {
		t.Fatalf("err = %v, want rule 2's refusal", err)
	}
	if w.callCount() != 0 {
		t.Fatalf("store reads after a rule 2 refusal = %d, want 0", w.callCount())
	}
}

// gateClock blocks every Sleep until release is closed (or ctx ends) and
// records the durations.
type gateClock struct {
	release chan struct{}
	mu      sync.Mutex
	slept   []time.Duration
}

func (c *gateClock) Sleep(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	c.slept = append(c.slept, d)
	c.mu.Unlock()
	select {
	case <-c.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *gateClock) durations() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.slept)
}

// idleInbox never has input. Its first Wait signals idle and blocks until its
// context ends (the guard's wake cancels it); every later Wait closes the
// inbox, which ends the run cleanly.
type idleInbox struct {
	mu    sync.Mutex
	waits int
	idle  chan struct{}
}

func (f *idleInbox) Drain(context.Context) ([]HumanInput, error) { return nil, nil }

func (f *idleInbox) Wait(ctx context.Context) error {
	f.mu.Lock()
	f.waits++
	first := f.waits == 1
	f.mu.Unlock()
	if !first {
		return ErrInboxClosed
	}
	close(f.idle)
	<-ctx.Done()
	return ctx.Err()
}

func healthyHypTurn(id int) string {
	return fmt.Sprintf(`{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"tu%d","name":"update_hypothesis","input":{"text":%s,"based_on_steps":[],"confidence":"low"}}]}`, id, strconvQuote(healthyText))
}

const guardEndTurn = `{"stop_reason":"end_turn","content":[{"type":"text","text":"waiting for the watcher"}]}`

// symptomRunner is a runner over one read tool with rule 1 reading w.
func symptomRunner(t *testing.T, model sdkModel, inbox Inbox, sink Sink, w clients.WatchStatusReader, d func(*Deps)) *runner {
	t.Helper()
	reg := tools.NewRegistry()
	if err := reg.Register(&outcomeTool{name: "pg_replication_slots"}); err != nil {
		t.Fatal(err)
	}
	deps := Deps{Tools: reg, Inbox: inbox, Sink: sink, Logger: discardLogger(), Watch: w}
	if d != nil {
		d(&deps)
	}
	r, err := newRunner(Alert{RoomID: "r_1", Title: "orders: slot_wal", PipelineID: "orders"}, deps, Options{MaxTurns: 10, HypothesisGuard: GuardEnforce})
	if err != nil {
		t.Fatalf("newRunner: %v", err)
	}
	r.model = viaSDK(model)
	return r
}

// The recheck: a refusal arms it; the row is clearing at the first recheck,
// so one watch_cleared input wakes the idle agent, which then gives the
// healthy verdict. Without the clear, the three rechecks end with no turn.
func TestRunSymptomRecheckWake(t *testing.T) {
	run := func(t *testing.T, rows [][]clients.WatchStatus, turns ...string) (*scriptedModel, *fakeSink, *fakeWatch, *gateClock) {
		t.Helper()
		model := &scriptedModel{}
		for _, s := range turns {
			model.turns = append(model.turns, mustMessage(t, s))
		}
		w := &fakeWatch{rows: rows}
		inbox := &idleInbox{idle: make(chan struct{})}
		sink := &fakeSink{}
		r := symptomRunner(t, model, inbox, sink, w, nil)
		clk := &gateClock{release: make(chan struct{})}
		r.clk = clk
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Release the recheck only once the agent is idle, so its input is
		// a wake and not part of a running turn.
		go func() {
			select {
			case <-inbox.idle:
				close(clk.release)
			case <-ctx.Done():
			}
		}()
		if err := r.run(ctx); err != nil {
			t.Fatalf("run: %v", err)
		}
		if ctx.Err() != nil {
			t.Fatal("run hung until the test deadline")
		}
		return model, sink, w, clk
	}

	firingRow := watchRow(firing(owners.SignalSlotWAL))
	clearRow := watchRow(clearing(owners.SignalSlotWAL))

	t.Run("clears: one extra turn", func(t *testing.T) {
		model, sink, w, clk := run(t, [][]clients.WatchStatus{firingRow, clearRow}, healthyHypTurn(1), guardEndTurn, guardEndTurn)
		// Turn 1 refused, turn 2 idle, turn 3 is the wake's: one extra.
		if len(model.calls) != 3 {
			t.Fatalf("model calls = %d, want 3 (the wake costs one turn)", len(model.calls))
		}
		js := messagesJSON(model.calls[2])
		if n := strings.Count(js, "[watch] slot_wal is clearing since "); n != 1 {
			t.Fatalf("the woken turn carries %d watch_cleared inputs, want 1:\n%s", n, js)
		}
		if strings.Contains(messagesJSON(model.calls[1]), "[watch]") {
			t.Fatal("the wake reached the model before the agent was idle")
		}
		if got := clk.durations(); !slices.Equal(got, []time.Duration{90 * time.Second}) {
			t.Fatalf("recheck sleeps = %v, want [1m30s]", got)
		}
		if w.callCount() != 2 {
			t.Fatalf("store reads = %d, want 2 (the refusal and one recheck)", w.callCount())
		}
		for _, e := range sink.events {
			if strings.Contains(string(e.payload), "[watch]") {
				t.Fatalf("the runner's own input reached the room: %s %s", e.typ, e.payload)
			}
		}
	})

	t.Run("clears: healthy verdict after the wake", func(t *testing.T) {
		model, sink, _, _ := run(t, [][]clients.WatchStatus{firingRow, clearRow}, healthyHypTurn(1), guardEndTurn, healthyHypTurn(3), guardEndTurn)
		if len(model.calls) != 4 {
			t.Fatalf("model calls = %d, want 4", len(model.calls))
		}
		if n := len(sink.ofType(events.AgentHypothesis)); n != 1 {
			t.Fatalf("hypotheses = %d, want the one after the wake", n)
		}
	})

	t.Run("still firing: no wake", func(t *testing.T) {
		model, _, w, clk := run(t, [][]clients.WatchStatus{firingRow}, healthyHypTurn(1), guardEndTurn)
		if len(model.calls) != 2 {
			t.Fatalf("model calls = %d, want 2 (no wake)", len(model.calls))
		}
		if w.callCount() != 4 {
			t.Fatalf("store reads = %d, want 4 (the refusal and three rechecks)", w.callCount())
		}
		if got := clk.durations(); !slices.Equal(got, []time.Duration{90 * time.Second, 90 * time.Second, 3 * time.Minute}) {
			t.Fatalf("recheck sleeps = %v", got)
		}
	})
}

// settle: an accepted healthy verdict drops a queued wake and stops the
// recheck, so a wake that has nothing left to ask costs no turn.
func TestSymptomSettleDropsWake(t *testing.T) {
	s := newSymptomGuard(&fakeWatch{rows: [][]clients.WatchStatus{watchRow(clearing(owners.SignalSlotWAL))}}, "orders", discardLogger(), time.Now,
		func(context.Context, time.Duration) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.begin(ctx)
	s.arm([]string{owners.SignalSlotWAL})
	select {
	case <-s.wakeChan():
	case <-time.After(5 * time.Second):
		t.Fatal("no wake")
	}
	if !s.active() {
		t.Fatal("a queued wake is not active")
	}
	s.settle()
	if s.active() || len(s.take()) != 0 {
		t.Fatal("settle kept the wake")
	}
	s.stop()
	// After stop no recheck starts.
	s.arm([]string{owners.SignalSlotWAL})
	if s.active() {
		t.Fatal("a recheck started after stop")
	}
}

// Shadow rooms run the same runner with no actions and no proposals, and the
// room service passes them the same reader: rule 1 refuses and caps exactly
// as in the live room.
func TestRunSymptomGuardShadowParity(t *testing.T) {
	outcome := func(t *testing.T, d func(*Deps)) (string, []string) {
		model := &scriptedModel{turns: []anthropic.BetaMessage{
			mustMessage(t, healthyHypTurn(1)), mustMessage(t, healthyHypTurn(2)), mustMessage(t, healthyHypTurn(3)),
		}}
		sink := &fakeSink{}
		w := &fakeWatch{rows: [][]clients.WatchStatus{watchRow(firing(owners.SignalSlotWAL))}}
		r := symptomRunner(t, model, &fakeInbox{waitErr: context.Canceled}, sink, w, d)
		if err := r.run(context.Background()); err != nil {
			t.Fatalf("run: %v", err)
		}
		var out []string
		for _, e := range sink.ofType(events.AgentHypothesis) {
			var p events.AgentHypothesisPayload
			_ = json.Unmarshal(e.payload, &p)
			out = append(out, "hypothesis:"+p.Confidence)
		}
		for _, e := range sink.ofType(events.AgentMessage) {
			var p events.AgentMessagePayload
			_ = json.Unmarshal(e.payload, &p)
			if strings.HasPrefix(p.Text, "[guard]") {
				out = append(out, "note")
			}
		}
		js := messagesJSON(model.calls[len(model.calls)-1])
		return fmt.Sprint(strings.Count(js, "hypothesis guard (symptom_persists)")), out
	}
	liveRefusals, live := outcome(t, func(d *Deps) { d.Actions = actions.NewRegistry() })
	shadowRefusals, shadow := outcome(t, func(d *Deps) { d.Actions, d.Proposals = nil, nil })
	if liveRefusals != "2" || !slices.Equal(live, []string{"hypothesis:low", "note"}) {
		t.Fatalf("live: refusals %s, events %v", liveRefusals, live)
	}
	if shadowRefusals != liveRefusals || !slices.Equal(shadow, live) {
		t.Fatalf("shadow: refusals %s events %v; live: %s %v", shadowRefusals, shadow, liveRefusals, live)
	}
}

// signalReadTools and the first-look table (tableReads) name the same reads
// for every signal the first look knows; connect_list_connectors is
// connect_status's stand-in when the evidence names no connector.
func TestSignalReadToolsMatchFirstLook(t *testing.T) {
	byName := map[string]toolEntry{}
	for _, list := range signalReadTools {
		for _, n := range list {
			byName[n] = toolEntry{}
		}
	}
	byName["connect_list_connectors"] = toolEntry{}
	p := &triagePass{alert: Alert{PipelineID: "orders"}, now: time.Now, r: &runner{toolList: toolList{byName: byName}}}
	sigs := map[string]map[string]any{
		owners.SignalConnectorDown:       connectorDownSignal(),
		owners.SignalSlotInactive:        {"signal": owners.SignalSlotInactive},
		owners.SignalSlotWAL:             {"signal": owners.SignalSlotWAL},
		owners.SignalLongTx:              {"signal": owners.SignalLongTx},
		owners.SignalDLQGrowth:           {"signal": owners.SignalDLQGrowth, "writers": []any{"orders-sink"}},
		owners.SignalConsumerLag:         {"signal": owners.SignalConsumerLag, "group": "order-sync"},
		owners.SignalGroupUnstable:       {"signal": owners.SignalGroupUnstable, "group": "order-sync"},
		owners.SignalOffsetBelowLogStart: {"signal": owners.SignalOffsetBelowLogStart, "topic": "pg.public.orders"},
		owners.SignalConfigRisk:          {"signal": owners.SignalConfigRisk, "risks": []any{map[string]any{"connector": "orders-source"}}},
	}
	for name, sig := range sigs {
		body, ok := parseWatcherBody(watcherRaw(t, sig))
		if !ok {
			t.Fatalf("%s: body not parsed", name)
		}
		uses := p.tableReads(body)
		if len(uses) == 0 {
			t.Fatalf("%s: the first look picks no read", name)
		}
		for _, u := range uses {
			n := u.name
			if n == "connect_list_connectors" {
				n = "connect_status"
			}
			if !slices.Contains(signalReadTools[name], n) {
				t.Errorf("%s: the first look reads %s, signalReadTools has %v", name, u.name, signalReadTools[name])
			}
		}
	}
	for name := range symptomSignals {
		if len(signalReadTools[name]) == 0 {
			t.Errorf("symptom signal %s has no reads to name", name)
		}
	}
}

// The new kind is not quiet: it wakes the idle agent, resets the turn guard
// and starts a new guard episode; its text reaches the model as written.
func TestWatchClearedKind(t *testing.T) {
	in := HumanInput{Kind: kindWatchCleared, Text: "[watch] slot_wal is clearing since 10:03:10Z"}
	if isQuiet(in) || !resetsTurns([]HumanInput{in}) {
		t.Fatal("watch_cleared is quiet")
	}
	if got := formatInput(in, true); got != in.Text {
		t.Fatalf("formatInput = %q", got)
	}
	g := newHypothesisGuard(GuardEnforce, discardLogger(), Alert{}, nil)
	g.refusals[guardRuleSymptom] = 2
	g.observeInputs([]HumanInput{in})
	if g.refusals[guardRuleSymptom] != 0 {
		t.Fatal("watch_cleared did not start a new episode")
	}
}
