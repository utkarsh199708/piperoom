// Package demo provides a deterministic scripted agent, fake topology builder
// and fake clients (fakes.go) so cmd/server --demo works before the dev stack
// is needed. The scripted agent runs the REAL actions from internal/actions
// over the in-memory clients, so the demo exercises the same write path as
// production. It tells the real schema-change story from dev/owners.yaml
// (public.orders → pg-orders-connector → pg.public.orders → order-sync →
// analytics.orders): an amount int → numeric(12,2) change registers an
// incompatible Avro schema and kills the Debezium task; the agent diagnoses it
// and proposes schema_set_compatibility + connect_restart_task.
//
// Controls (controls.go) adds the --demo presenter controls: skip to the next
// step, pause and resume the steps, and reset the fake clients and the story.
package demo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/tools"
	"piperoom/internal/tools/changes"
	"piperoom/internal/topology"
)

var (
	_ agent.Runner           = Run
	_ agent.HandoffWriter    = WriteHandoff
	_ agent.PostmortemWriter = WritePostmortem
)

// Action names the demo proposal references.
const (
	ActionSchemaSetCompatibility = "schema_set_compatibility"
	ActionConnectRestartTask     = "connect_restart_task"
)

const stepDelay = 550 * time.Millisecond

// whatChangedWindow is the window the scripted what_changed step asks for.
const whatChangedWindow = "6h"

// How long before the room opens each recorded change happened, newest first.
// The DDL and the connector config change are the suspects just before the
// alert; the schema version was registered earlier by the same rollout.
const (
	ddlAgo             = 90 * time.Second
	connectorConfigAgo = 12 * time.Minute
	schemaVersionAgo   = 41 * time.Minute
)

// step is one scripted investigation step. input nil means the demo's generic
// {"probe": true} tool input; data is the tool result's Data.
type step struct {
	id, title, tool, summary string
	input, data              any
}

// story is the scripted story with the clock pinned at the zero time. A run
// replays storySteps built from the room's own clock, so the recorded change
// times track when the room opened; the ids, titles, tools and order here
// never change. The controls and their tests use it for step ids and count.
var story = storySteps(time.Time{}, "")

// Run is a scripted fake agent.Run. It replays the schema-change story with
// realistic delays, then waits for steers and approval results. It has no
// presenter controls; the --demo server runs Controls.Run instead.
func Run(ctx context.Context, alert agent.Alert, deps agent.Deps, opts agent.Options) error {
	return runStory(ctx, alert, deps, opts, time.Now(), stepDelay)
}

// runStory is Run with the story clock and the step delay injected, so tests
// can pin the recorded change times and skip the demo's delays.
func runStory(ctx context.Context, alert agent.Alert, deps agent.Deps, opts agent.Options, now time.Time, delay time.Duration) error {
	return runControlled(ctx, newRunState(), nil, alert, deps, now, delay)
}

// runControlled replays the story until the room closes. A reset request
// restarts it from step 1, after the fake clients go back to their starting
// state.
func runControlled(ctx context.Context, rc *runState, fakes *Fakes, alert agent.Alert, deps agent.Deps, now time.Time, delay time.Duration) error {
	if delay <= 0 {
		delay = stepDelay
	}
	emit := func(typ events.Type, payload any) error {
		return deps.Sink.Emit(ctx, typ, payload)
	}
	steps := storySteps(now, alert.PipelineID)
	watcher := &inboxWatcher{in: deps.Inbox}
	for {
		res, err := playStory(ctx, rc, steps, deps, watcher, emit, delay)
		if err != nil {
			return err
		}
		switch res {
		case waitDone:
			return nil
		case waitReset:
			rc.takeReset()
			if fakes != nil {
				fakes.Reset(rc.roomID)
			}
			rc.restartStory()
		}
	}
}

// playStory replays the steps, the hypothesis and the fix proposal, then waits
// for human input. It returns waitReset when a presenter asked to replay.
func playStory(ctx context.Context, rc *runState, steps []step, deps agent.Deps, watcher *inboxWatcher, emit func(events.Type, any) error, delay time.Duration) (runWait, error) {
	_ = emit(events.AgentStatus, events.AgentStatusPayload{State: "working", Detail: "investigating"})

	for i, st := range steps {
		if res, _ := rc.boundary(ctx); res != waitReady {
			return res, nil
		}
		rc.beginStep(i + 1)
		_ = emit(events.AgentStepStart, events.AgentStepStartPayload{StepID: st.id, Title: st.title})
		if res := rc.hold(ctx, delay); res != waitReady {
			return res, nil
		}
		input := st.input
		if input == nil {
			input = map[string]any{"probe": true}
		}
		_ = emit(events.AgentToolCall, events.AgentToolCallPayload{StepID: st.id, ToolUseID: "tu_" + st.id, Tool: st.tool, Input: mustJSON(input)})
		if res := rc.hold(ctx, delay); res != waitReady {
			return res, nil
		}
		_ = emit(events.AgentToolResult, events.AgentToolResultPayload{
			StepID: st.id, ToolUseID: "tu_" + st.id, Tool: st.tool,
			Summary: st.summary, Data: mustJSON(st.data), IsError: false, TookMS: 120,
		})
		if res := rc.hold(ctx, delay); res != waitReady {
			return res, nil
		}
	}

	_ = emit(events.AgentHypothesis, events.AgentHypothesisPayload{
		Text:         "A schema change to orders.amount broke the pg-orders-connector on a Schema Registry incompatibility; no new rows reach the warehouse.",
		BasedOnSteps: []string{"s_1", "s_3", "s_5", "s_6"},
		Confidence:   "high",
		Confirmed:    false,
	})
	// A shadow room has no proposal creator (docs/design/shadow.md 3.7): the
	// run stops at the hypothesis and ends on the closed inbox.
	if deps.Proposals == nil {
		rc.markIdle()
		_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: "Root cause found: the schema change on orders.amount is incompatible under BACKWARD compatibility for pg.public.orders-value."})
		_ = emit(events.AgentStatus, events.AgentStatusPayload{State: "idle", Detail: "no proposals in this room"})
		return waitForInput(ctx, rc, watcher, deps, emit, nil)
	}

	_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: "Root cause found: the schema change on orders.amount is incompatible under BACKWARD compatibility for pg.public.orders-value. I've written a fix proposal."})

	fixSteps := []events.ProposalStep{
		{
			Action:      ActionSchemaSetCompatibility,
			Layer:       "schema-registry",
			Params:      mustJSON(map[string]any{"subject": "pg.public.orders-value", "level": "NONE"}),
			Description: "Allow the new decimal schema to register under the same subject.",
		},
		{
			Action:      ActionConnectRestartTask,
			Layer:       "kafka-connect",
			Params:      mustJSON(map[string]any{"connector": "pg-orders-connector", "task": 0}),
			Description: "Restart Debezium task 0 so it picks up the new schema.",
		},
	}
	proposalID, err := deps.Proposals.Create(ctx, events.ProposalCreatedPayload{
		Title:          "Set subject compatibility to NONE and restart the connector task",
		Steps:          fixSteps,
		Risk:           "low",
		Rollback:       "set compatibility back to BACKWARD",
		RequiredLayers: []string{"schema-registry", "kafka-connect"},
	})
	if err != nil {
		// An unconfigured pipeline disables proposals; that is an end state,
		// not a failure to retry (a retry would replay the whole story).
		var re *room.Error
		if errors.As(err, &re) && re.Code == "validation" {
			rc.markIdle()
			_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: "I cannot propose a fix in this room: " + re.Msg})
			_ = emit(events.AgentStatus, events.AgentStatusPayload{State: "idle", Detail: "proposals disabled"})
			return waitForInput(ctx, rc, watcher, deps, emit, nil)
		}
		return waitDone, err
	}
	rc.markIdle()
	_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: "The fix proposal is waiting for the schema-registry and kafka-connect owners."})
	_ = emit(events.AgentStatus, events.AgentStatusPayload{State: "waiting_approval", Detail: "1 proposal open"})
	// The real agent verifies after the last step ran; the scripted one must
	// do the same or the room waits for a verdict forever (KAN-389). The
	// verifier's re-check pause is the same ctx-aware step delay.
	pause := func() bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(delay):
			return true
		}
	}
	return waitForInput(ctx, rc, watcher, deps, emit, newFixVerifier(proposalID, len(fixSteps), pause, emit))
}

// recoveredMessage is the room's form of the verdict: it keeps the
// compatibility follow-up the real agent reports when a fix leaves a subject
// at NONE. Like every scripted message it names no proposal id (F5b).
const recoveredMessage = "Verification passed: pg-orders-connector task 0 is RUNNING and new rows reached analytics.orders. " +
	"pg.public.orders-value compatibility is still NONE; restore BACKWARD with a separate fix."

// storySteps returns the six scripted reads of the schema-change story. The
// last one is what_changed, whose result carries the F8 change rows the web
// "What changed" lane renders; without it the lane stays empty in --demo.
func storySteps(now time.Time, pipelineID string) []step {
	recorded := storyChanges(now, pipelineID)
	// s_5 and s_6 describe the same ALTER, so they use the same recorded time:
	// what_changed's newest row. A fixed timestamp here would date the DDL log
	// line days before a room opened today.
	ddlAt := now.UTC().Truncate(time.Second).Add(-ddlAgo)
	ddlStamp := ddlAt.Format(time.RFC3339)
	return []step{
		{"s_1", "Check the Debezium connector", "connect_status",
			"pg-orders-connector (source): RUNNING. Tasks: 0=FAILED on 172.19.0.5:8083.\nTask 0 error: org.apache.kafka.connect.errors.DataException: Failed to serialize Avro data from topic pg.public.orders\n  Caused by: ... Schema being registered is incompatible with an earlier schema for subject pg.public.orders-value, details: [{errorType:'MISSING_UNION_BRANCH', description:'The new schema is missing a type inside a union field at path /fields/0/type/1 in the old schema'}, {oldSchemaVersion: 1}, {validateFields: 'false', compatibility: 'BACKWARD'}]; error code: 409\nRejected schema vs pg.public.orders-value v1: amount: int → bytes(decimal 12,2)",
			nil, map[string]any{"connector": "pg-orders-connector", "state": "RUNNING", "tasks": []any{map[string]any{"id": 0, "state": "FAILED", "worker": "172.19.0.5:8083"}}}},
		{"s_2", "Inspect the Schema Registry subject", "schema_registry_versions",
			"subject pg.public.orders-value: versions [1], latest id 1, compatibility BACKWARD (subject-level)",
			nil, map[string]any{"subject": "pg.public.orders-value", "versions": []int{1}, "latest_id": 1, "compatibility": "BACKWARD", "level": "subject"}},
		{"s_3", "Diff the attempted schema against v1", "schema_registry_diff",
			"amount: int → bytes(decimal 12,2)",
			nil, map[string]any{"subject": "pg.public.orders-value", "from_version": 1, "field_diffs": []any{map[string]any{"field": "amount", "from": "int", "to": "bytes(decimal 12,2)"}}}},
		{"s_4", "Check consumer lag", "kafka_consumer_lag",
			"group order-sync: state Stable, members 1, total lag 0",
			nil, map[string]any{"group": "order-sync", "state": "Stable", "members": 1, "total_lag": 0}},
		{"s_5", "Check the DDL log", "pg_recent_ddl",
			ddlStamp + " ALTER TABLE public.orders: ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2) by etl",
			nil, map[string]any{"command": "ALTER TABLE", "object": "public.orders", "sql": "ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2)", "username": "etl", "at": ddlStamp}},
		{"s_6", "Check what changed before the alert", changes.ToolName,
			whatChangedSummary(recorded),
			map[string]any{"window": whatChangedWindow}, recorded},
	}
}

// storyChanges is the recorded what_changed result of the story, newest
// first: the DDL and the connector config change are the suspects just before
// the alert, and the schema version was registered earlier by the same
// rollout. It is built from the real tool's changes.Data type, so the demo
// lane parses exactly the shape production emits.
func storyChanges(now time.Time, pipelineID string) changes.Data {
	if pipelineID == "" {
		pipelineID = "orders"
	}
	now = now.UTC().Truncate(time.Second)
	at := func(d time.Duration) time.Time { return now.Add(-d) }
	ddlAt, cfgAt, verAt := at(ddlAgo), at(connectorConfigAgo), at(schemaVersionAgo)
	return changes.Data{
		Window: whatChangedWindow,
		Changes: []clients.PipelineChange{
			{
				ID: 3, PipelineID: pipelineID, Kind: clients.ChangeDDL, Object: "public.orders", At: ddlAt,
				Actor: "etl", Source: "ddl_audit",
				Fingerprint: changeFingerprint(clients.ChangeDDL, "public.orders", ddlAt.Format(time.RFC3339Nano)),
				Summary:     "ALTER TABLE public.orders | ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2)",
			},
			{
				ID: 2, PipelineID: pipelineID, Kind: clients.ChangeConnectorConfig, Object: "pg-orders-connector", At: cfgAt,
				Source:      "watcher",
				Fingerprint: changeFingerprint(clients.ChangeConnectorConfig, "pg-orders-connector", "heartbeat.interval.ms"),
				Before:      mustJSON(map[string]string{"heartbeat.interval.ms": "10000"}),
				After:       mustJSON(map[string]string{"heartbeat.interval.ms": "5000"}),
				Summary:     "config of pg-orders-connector: 1 key changed (heartbeat.interval.ms)",
			},
			{
				ID: 1, PipelineID: pipelineID, Kind: clients.ChangeSchemaVersion, Object: "pg.public.orders-value", At: verAt,
				Source:      "watcher",
				Fingerprint: changeFingerprint(clients.ChangeSchemaVersion, "pg.public.orders-value", "0", "1"),
				Before:      mustJSON(map[string]int{"version": 0}),
				After:       mustJSON(map[string]int{"version": 1}),
				Summary:     "schema pg.public.orders-value: version 1 registered (was 0)",
			},
		},
	}
}

// whatChangedSummary renders the recorded changes as the tool's one-line-per-
// change summary, so the investigation feed and the lane tell one story.
func whatChangedSummary(d changes.Data) string {
	var sb strings.Builder
	for i, c := range d.Changes {
		if i > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(c.At.Format(time.RFC3339))
		sb.WriteByte(' ')
		sb.WriteString(c.Kind)
		sb.WriteByte(' ')
		sb.WriteString(c.Object)
		if c.Actor != "" {
			sb.WriteString(" by " + c.Actor)
		}
		sb.WriteString(": " + c.Summary)
	}
	return sb.String()
}

// changeFingerprint is the opaque dedup key the change log stores: sha256 of
// the change's identity, hex encoded.
func changeFingerprint(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(h[:])
}

// waitForInput answers steers, comments, forwarded suggestions and approval
// results until the room closes. An inbox that is closed (a shadow room) ends
// it at once with nil. The wait rides on the run's single inbox watcher, so a
// reset (which returns waitReset) never leaves a second Wait racing for the
// same input. fix may be nil (no proposal in this room).
func waitForInput(ctx context.Context, rc *runState, watcher *inboxWatcher, deps agent.Deps, emit func(events.Type, any) error, fix *fixVerifier) (runWait, error) {
	in := deps.Inbox
	if in == nil {
		return waitDone, nil
	}
	rc.markIdle()
	for ctx.Err() == nil {
		switch watcher.wait(ctx, rc) {
		case waitDone:
			return waitDone, nil
		case waitReset:
			return waitReset, nil
		}
		inputs, err := in.Drain(ctx)
		if err != nil {
			return waitDone, nil
		}
		for _, in := range inputs {
			handleInput(ctx, deps, in, emit)
			fix.observe(in)
		}
	}
	return waitDone, nil
}

// inboxWatcher keeps one Inbox.Wait call in flight across the whole run. The
// demo's idle loop selects on it together with the presenter controls, so a
// reset can be noticed without abandoning a waiter that would race the next
// one for an input.
type inboxWatcher struct {
	in   agent.Inbox
	done chan error // nil while no Wait is in flight
}

// wait blocks until an input is pending (waitReady: the caller drains), the
// room closes (waitDone) or a presenter asks to replay (waitReset).
func (w *inboxWatcher) wait(ctx context.Context, rc *runState) runWait {
	for {
		if rc.resetPending() {
			return waitReset
		}
		if w.done == nil {
			w.done = make(chan error, 1)
			go func(ch chan error) { ch <- w.in.Wait(ctx) }(w.done)
		}
		select {
		case <-ctx.Done():
			return waitDone
		case <-rc.wake:
			// A pause, skip or reset signal: re-check the state.
		case err := <-w.done:
			w.done = nil
			if err != nil {
				return waitDone // room closed or ctx cancelled
			}
			return waitReady
		}
	}
}

// fixVerifier is the scripted verify-after-fix pass. It mirrors the real
// agent's rule (internal/agent/verify.go): recovery is verified only once every
// step of the proposal has succeeded. A failed or unknown step, or a result for
// another proposal, never produces a verdict, so a room that only partly ran a
// fix keeps saying "verifying" instead of claiming a recovery it did not see.
//
// One verdict is written per run. The fields are only touched by the run's own
// goroutine (the inbox is drained there), so no lock is needed.
type fixVerifier struct {
	proposalID string
	steps      int
	pause      func() bool
	emit       func(events.Type, any) error
	okSteps    map[int]bool
	failed     bool
	verified   bool
}

// newFixVerifier returns the verifier of proposalID, whose fix has steps steps.
func newFixVerifier(proposalID string, steps int, pause func() bool, emit func(events.Type, any) error) *fixVerifier {
	return &fixVerifier{
		proposalID: proposalID,
		steps:      steps,
		pause:      pause,
		emit:       emit,
		okSteps:    map[int]bool{},
	}
}

// observe reads one agent input and writes the verdict when it completes the
// fix. A nil verifier (a room with no proposal) ignores every input.
func (v *fixVerifier) observe(in agent.HumanInput) {
	if v == nil || v.verified || v.failed || v.steps <= 0 || in.Kind != "action_result" {
		return
	}
	if in.ProposalID != v.proposalID || in.StepIndex == nil {
		return
	}
	if in.OK == nil || !*in.OK {
		// A failed or unknown step is not a recovery (HumanInput.OK nil means
		// the executor stopped before recording the outcome).
		v.failed = true
		return
	}
	if *in.StepIndex < 0 || *in.StepIndex >= v.steps {
		return
	}
	v.okSteps[*in.StepIndex] = true
	for i := 0; i < v.steps; i++ {
		if !v.okSteps[i] {
			return
		}
	}
	// Re-read the pipeline after the last step, as the real agent does; the
	// pause also keeps "verifying" visible long enough for the demo audience.
	if !v.pause() {
		return
	}
	v.verified = true
	_ = v.emit(events.AgentVerification, recoveredVerification(v.proposalID))
	_ = v.emit(events.AgentMessage, events.AgentMessagePayload{Text: recoveredMessage})
}

// recoveredVerification is the scripted verdict of the schema-change fix: the
// same payload the real agent emits, so the room log, the post-mortem and the
// web render it exactly as a production verification. The checks read the state
// the fix left behind: the subject at NONE and task 0 RUNNING, and new rows in
// the warehouse sink.
func recoveredVerification(proposalID string) events.AgentVerificationPayload {
	return events.AgentVerificationPayload{
		ProposalID: proposalID,
		Checks: []events.VerificationCheck{
			{
				Layer:  "schema-registry",
				Name:   "schema_registry_versions",
				OK:     true,
				Detail: "subject pg.public.orders-value: compatibility NONE (was BACKWARD before this fix)",
			},
			{
				Layer:  "kafka-connect",
				Name:   "connect_status",
				OK:     true,
				Detail: "pg-orders-connector (source): RUNNING. Tasks: 0=RUNNING on 172.19.0.5:8083 (was FAILED)",
			},
		},
		DataFlow: &events.DataFlowCheck{
			Checked: true,
			Fresh:   true,
			Sinks: []events.SinkCheck{
				{Table: "analytics.orders", Passed: true, PassedBy: "rows since proposal"},
			},
		},
		Recovered: true,
		Verdict:   "recovered: every layer check passed and new rows reached analytics.orders after the fix.",
	}
}

// handleInput answers one human input. The switch mirrors the input kinds of
// internal/agent/contract.go; a kind the demo does not act on is logged at
// debug level rather than dropped silently, so the next kind added to the
// contract shows up in a demo run (KAN-391).
func handleInput(ctx context.Context, deps agent.Deps, in agent.HumanInput, emit func(events.Type, any) error) {
	switch in.Kind {
	case "steer":
		_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: "Got it: " + in.Text})
	case "suggestion":
		// A forwarded suggestion is the suggester's words, not the driver's
		// (the real agent wraps it as untrusted data), so the demo only
		// acknowledges it and never acts on its content. The cap keeps one
		// large paste from crowding the room.
		text, _ := tools.Truncate(in.Text)
		_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: "Got it (forwarded suggestion): " + text})
	case "comment":
		_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: "Noted from " + in.Actor.Name + ": " + in.Text, ReplyTo: in.CommentID})
	case "approval_result":
		_ = emit(events.AgentStatus, events.AgentStatusPayload{State: "waiting_approval", Detail: in.Text})
	case "action_result":
		_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: in.Text})
	default:
		if deps.Logger != nil {
			deps.Logger.DebugContext(ctx, "demo: no scripted handler for human input kind", "kind", in.Kind, "seq", in.Seq)
		}
	}
}

// fixDone names the demo proposal's steps, in step order, as they read in the
// hand-off's Done list once the step ran.
var fixDone = []string{"Set compatibility to NONE", "Restarted task 0"}

// WriteHandoff is a fake hand-off writer. The cause text is fixed; what ran is
// read from the room's action.executed events, so the summary never claims a
// fix the room did not run. It makes no model call.
func WriteHandoff(ctx context.Context, roomEvents []events.Event, opts agent.Options) (events.HandoffSummary, error) {
	ran := make([]bool, len(fixDone))
	for _, e := range roomEvents {
		if e.Type != events.ActionExecuted {
			continue
		}
		p, err := events.Decode[events.ActionExecutedPayload](e)
		if err != nil || !p.OK || p.StepIndex < 0 || p.StepIndex >= len(ran) {
			continue
		}
		ran[p.StepIndex] = true
	}
	var done, pending []string
	for i, name := range fixDone {
		if ran[i] {
			done = append(done, name)
		} else {
			pending = append(pending, strings.ToLower(name[:1])+name[1:])
		}
	}
	s := events.HandoffSummary{
		WhatBroke: "Debezium task 0 failed after orders.amount was altered to numeric(12,2).",
		Why:       "New schema is incompatible under BACKWARD compatibility for subject pg.public.orders-value.",
		Done:      done,
		RuledOut:  []string{"Kafka brokers", "Consumer group config"},
	}
	switch {
	case len(pending) == len(fixDone):
		s.Open = []string{"The proposed fix has not run: it sets compatibility to NONE and restarts task 0"}
		s.Risks = []string{"Debezium task 0 stays failed and the warehouse stale until the fix runs"}
	case len(pending) > 0:
		s.Open = []string{"The proposed fix is only partly applied; still to run: " + strings.Join(pending, ", ")}
		s.Risks = []string{"The fix is half applied, so the pipeline may still be failing"}
	default:
		s.Done = append(s.Done, "Warehouse caught up")
		s.Open = []string{"Consider pinning decimal.handling.mode"}
		s.Risks = []string{"BACKWARD compatibility is off until re-enabled"}
	}
	if ran[0] && len(pending) > 0 {
		s.Risks = append(s.Risks, "BACKWARD compatibility is off until re-enabled")
	}
	return s, nil
}

// WritePostmortem is a fake closing-summary writer returning the fixture
// summary, with no grounding violations.
func WritePostmortem(ctx context.Context, roomEvents []events.Event, opts agent.Options) (agent.PostmortemDraft, error) {
	s, err := WriteHandoff(ctx, roomEvents, opts)
	return agent.PostmortemDraft{Summary: s}, err
}

// Builder is a fake topology.Builder: the orders CDC graph for the orders
// pipeline and the batch graph for daily-revenue (the ids dev/owners.fast.yaml
// defines). The orders graph shows the failure until a room's fix has run.
// Fakes is the client pair that tells it whether a fix ran; the zero Builder
// uses the pair of the last NewControls, which is how --demo wires it.
type Builder struct {
	Fakes *Fakes
}

var _ topology.Builder = Builder{}

// batchPipelineID is the demo's batch pipeline (dev/owners.fast.yaml).
const batchPipelineID = "daily-revenue"

// defaultFakes is the pair the zero Builder reads, set by NewControls.
var defaultFakes atomic.Pointer[Fakes]

func (b Builder) fakes() *Fakes {
	if b.Fakes != nil {
		return b.Fakes
	}
	return defaultFakes.Load()
}

// recovered reports whether a demo room's fix has run. The topology endpoint
// carries no room id, so a recovery in any room clears the failure on the map.
func (b Builder) recovered() bool {
	f := b.fakes()
	return f != nil && f.Connect.anyRecovered()
}

// Build returns a static graph matching dev/owners.yaml. The batch pipeline
// has its own nodes; the orders graph turns healthy once the fix has run.
func (b Builder) Build(ctx context.Context, pipelineID string) (topology.Graph, error) {
	if pipelineID == batchPipelineID {
		return batchGraph(pipelineID), nil
	}
	return ordersGraph(pipelineID, b.recovered()), nil
}

// batchGraph is the DAG, its dbt models and the warehouse table of
// daily-revenue, all healthy: no demo story breaks the batch pipeline.
func batchGraph(pipelineID string) topology.Graph {
	return topology.Graph{
		PipelineID: pipelineID,
		Nodes: []topology.Node{
			{ID: "dag:revenue_daily", Kind: topology.KindDAG, Name: "revenue_daily", Layer: "orchestrator", Owner: "priya", Health: topology.HealthOK, Facts: map[string]string{"last_run_status": "success"}},
			{ID: "model:stg_orders", Kind: topology.KindModel, Name: "stg_orders", Layer: "transform", Owner: "priya", Health: topology.HealthOK, Facts: map[string]string{"last_run_status": "success"}},
			{ID: "model:fct_revenue", Kind: topology.KindModel, Name: "fct_revenue", Layer: "transform", Owner: "priya", Health: topology.HealthOK, Facts: map[string]string{"last_run_status": "success"}},
			{ID: "wh:analytics.fct_revenue", Kind: topology.KindSinkTable, Name: "analytics.fct_revenue", Layer: "warehouse", Owner: "meera", Health: topology.HealthOK, Facts: map[string]string{"freshness": "fresh"}},
		},
		Edges: []topology.Edge{
			{From: "dag:revenue_daily", To: "model:stg_orders"},
			{From: "model:stg_orders", To: "model:fct_revenue"},
			{From: "model:fct_revenue", To: "wh:analytics.fct_revenue"},
		},
	}
}

// ordersGraph is the orders CDC graph: the story's failure, or the same
// nodes healthy once the fix ran.
func ordersGraph(pipelineID string, recovered bool) topology.Graph {
	srcH, connH, topicH, groupH, whH := topology.HealthWarn, topology.HealthFail, topology.HealthWarn, topology.HealthWarn, topology.HealthFail
	task0, fresh := "FAILED", "stale"
	if recovered {
		srcH, connH, topicH, groupH, whH = topology.HealthOK, topology.HealthOK, topology.HealthOK, topology.HealthOK, topology.HealthOK
		task0, fresh = "RUNNING", "fresh"
	}
	return topology.Graph{
		PipelineID: pipelineID,
		Nodes: []topology.Node{
			{ID: "pg:public.orders", Kind: topology.KindSourceTable, Name: "public.orders", Layer: "postgres", Owner: "priya", Health: srcH},
			{ID: "connect:pg-orders-connector", Kind: topology.KindConnector, Name: "pg-orders-connector", Layer: "kafka-connect", Owner: "arjun", Health: connH, Facts: map[string]string{"task_0": task0}},
			{ID: "topic:pg.public.orders", Kind: topology.KindTopic, Name: "pg.public.orders", Layer: "kafka", Owner: "arjun", Health: topicH},
			{ID: "group:order-sync", Kind: topology.KindConsumerGroup, Name: "order-sync", Layer: "consumer", Owner: "kabir", Health: groupH, Facts: map[string]string{"lag": "0"}},
			{ID: "wh:analytics.orders", Kind: topology.KindSinkTable, Name: "analytics.orders", Layer: "warehouse", Owner: "meera", Health: whH, Facts: map[string]string{"freshness": fresh}},
		},
		Edges: []topology.Edge{
			{From: "pg:public.orders", To: "connect:pg-orders-connector"},
			{From: "connect:pg-orders-connector", To: "topic:pg.public.orders"},
			{From: "topic:pg.public.orders", To: "group:order-sync"},
			{From: "group:order-sync", To: "wh:analytics.orders"},
		},
	}
}

// Render returns a compact text form for the model prompt.
func (Builder) Render(g topology.Graph) string {
	out := "pipeline " + g.PipelineID + "\n"
	for _, n := range g.Nodes {
		out += "- " + n.ID + " (" + string(n.Kind) + ", " + n.Layer + ", " + string(n.Health) + ")\n"
	}
	return out
}

// Trace returns a deterministic stage list matching the fixture. Like the real
// builder, a pipeline or table outside the orders pipeline is topology.ErrBadInput
// (the room service maps it to 400 validation), so the demo and real modes agree
// on the error taxonomy (review item 7).
func (Builder) Trace(ctx context.Context, pipelineID, table, key string) ([]events.TraceStage, error) {
	if pipelineID != "orders" {
		return nil, fmt.Errorf("%w: pipeline %q not found", topology.ErrBadInput, pipelineID)
	}
	if table != "public.orders" {
		return nil, fmt.Errorf("%w: no connector streams table %q in pipeline %q", topology.ErrBadInput, table, pipelineID)
	}
	return []events.TraceStage{
		{Layer: "postgres", Name: "public.orders", Status: "seen", Facts: map[string]string{"row": "id=10042"}},
		{Layer: "kafka-connect", Name: "pg-orders-connector", Status: "seen", Facts: map[string]string{"task": "0"}},
		{Layer: "schema-registry", Name: "pg.public.orders-value", Status: "seen"},
		{Layer: "kafka", Name: "pg.public.orders", Status: "seen", Facts: map[string]string{"partition": "0", "offset": "48213"}},
		{Layer: "consumer", Name: "order-sync", Status: "not_seen"},
		{Layer: "warehouse", Name: "analytics.orders", Status: "stopped_here", Facts: map[string]string{"reason": "no row for id=10042"}},
	}, nil
}

// LayerOwner maps demo layers to demo owners, following dev/owners.yaml
// (schema-registry belongs to priya's source team).
func (Builder) LayerOwner(pipelineID, layer string) (string, bool) {
	m := map[string]string{
		"postgres":        "priya",
		"kafka-connect":   "arjun",
		"schema-registry": "priya",
		"kafka":           "arjun",
		"consumer":        "kabir",
		"warehouse":       "meera",
	}
	o, ok := m[layer]
	return o, ok
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("{}")
	}
	return b
}
