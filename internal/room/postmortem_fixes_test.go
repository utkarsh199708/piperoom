package room_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store/memstore"
)

// pmGolden compares got with testdata/<name>.md (rewritten with -update).
func pmGolden(t *testing.T, name, roomID, got string) {
	t.Helper()
	got = strings.ReplaceAll(got, roomID, "ROOM")
	path := filepath.Join("testdata", name+".md")
	if *updateGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (run with -update): %v", err)
	}
	if got != string(want) {
		t.Fatalf("draft differs from %s (run with -update to accept)\n--- got ---\n%s", path, got)
	}
}

func costOf(v float64) *float64 { return &v }

var pmSystem = events.Actor{Kind: events.ActorSystem, ID: "system", Name: "Piperoom"}

// handoffLog appends a hand-off request and its hand-off. id is the hand-off id.
func handoffLog(t *testing.T, st *memstore.Store, c *clock, roomID, id string, sum events.HandoffSummary) {
	t.Helper()
	to := pmHuman("priya", "Priya")
	app(t, st, c, roomID, events.HandoffRequested, pmHuman("arjun", "Arjun"), events.HandoffRequestedPayload{RequestID: "hr_" + id, To: to})
	app(t, st, c, roomID, events.HandoffCreated, pmHuman("arjun", "Arjun"), events.HandoffCreatedPayload{
		HandoffID: id, RequestID: "hr_" + id, To: to, Summary: sum,
	})
}

// staleSummary is a hand-off written before a person fixed the problem. Two of
// its lines are about the hand-off process, not the incident.
func staleSummary() events.HandoffSummary {
	return events.HandoffSummary{
		WhatBroke: "order-sync stopped consuming",
		Why:       "the consumer is stuck on a poison record",
		Done:      []string{"checked the connector"},
		RuledOut:  []string{"Kafka broker health", "Agent summary lines were truncated"},
		Open:      []string{"Restart the consumer", "One handoff failed"},
		Risks:     []string{"lag keeps growing", "The hand-off was truncated, so some tool output is missing"},
	}
}

// investigation appends the agent's work up to its hypothesis.
func investigation(t *testing.T, st *memstore.Store, c *clock, id string) {
	t.Helper()
	ag := events.AgentActor
	app(t, st, c, id, events.AgentStepStart, ag, events.AgentStepStartPayload{})
	app(t, st, c, id, events.AgentToolCall, ag, events.AgentToolCallPayload{Tool: "consumer_lag"})
	app(t, st, c, id, events.AgentToolResult, ag, events.AgentToolResultPayload{Tool: "consumer_lag"})
	app(t, st, c, id, events.AgentUsage, ag, events.AgentUsagePayload{Model: "m", Role: "main", InputTokens: 1200, OutputTokens: 300, CostUSD: costOf(0.0731)})
	app(t, st, c, id, events.AgentHypothesis, ag, events.AgentHypothesisPayload{Text: "order-sync is stuck on a poison record", Confidence: "high"})
}

// (a) A room a person fixed outside Piperoom, with a hand-off that is older
// than the fix: the sections must not contradict the resolution, and the
// hand-off bookkeeping stays out of the content.
func TestPostmortemFixedByPersonStaleHandoff(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	investigation(t, st, c, r.ID)
	// One hand-off request failed, then one succeeded.
	app(t, st, c, r.ID, events.HandoffRequested, pmHuman("arjun", "Arjun"), events.HandoffRequestedPayload{RequestID: "hr_0", To: pmHuman("priya", "Priya")})
	app(t, st, c, r.ID, events.HandoffFailed, pmSystem, events.HandoffFailedPayload{RequestID: "hr_0", Reason: events.HandoffFailedSummary})
	handoffLog(t, st, c, r.ID, "h_1", staleSummary())
	app(t, st, c, r.ID, events.CommentAdded, pmHuman("kabir", "Kabir"), events.CommentAddedPayload{
		CommentID: "c_1", Kind: events.CommentKindFixNote, Text: "restarted the order-sync consumer by hand; lag is draining",
	})
	app(t, st, c, r.ID, events.RoomClosed, pmHuman("arjun", "Arjun"), events.RoomClosedPayload{Resolution: "Fixed by Kabir: consumer restarted, lag back to zero"})
	app(t, st, c, r.ID, events.PostmortemDrafted, pmSystem, events.PostmortemDraftedPayload{Source: "handoff", SummaryRef: "h_1"})

	md := renderRoom(t, svc, r.ID)
	pmGolden(t, "postmortem_fixed_by_person", r.ID, md)

	// The contradictions of the QA run are gone.
	for _, bad := range []string{"No fix was proposed", "No verification recorded"} {
		if strings.Contains(md, bad) {
			t.Errorf("draft still says %q", bad)
		}
	}
	for _, want := range []string{
		"### Fix notes (done outside Piperoom)",
		"Kabir: restarted the order-sync consumer by hand",
		"No agent verification ran: the fix was made outside Piperoom, per the fix notes and the resolution.",
		"none recorded after the fix",
		"Earlier, from the hand-off at 10:04 (before the fix: fix note at 10:05), possibly resolved since:",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("draft lacks %q", want)
		}
	}
	// The stale lines sit only under their label, never as current items.
	open := section(t, md, "## Still open")
	if !strings.HasPrefix(open, "none recorded after the fix") || !strings.Contains(open, "- Restart the consumer") {
		t.Errorf("Still open = %q", open)
	}
	// Hand-off bookkeeping is not in any content section; the footnote counts it.
	for _, name := range []string{"## What was done", "## Verification", "## Still open", "## Risks", "## Ruled out"} {
		s := section(t, md, name)
		if strings.Contains(s, "truncated") || strings.Contains(s, "handoff failed") {
			t.Errorf("%s holds hand-off bookkeeping: %q", name, s)
		}
	}
	if !strings.Contains(md, "Note: 3 summary lines about the hand-off process") {
		t.Errorf("no footnote for the dropped lines:\n%s", tail(md))
	}
	// The timeline keeps the facts.
	if !strings.Contains(md, "hand-off failed (summary\\_failed)") || !strings.Contains(md, "Kabir logged a fix note: restarted") {
		t.Error("timeline lacks the hand-off failure or the fix note")
	}
}

// (a, variant) The same room with no fix note: the resolution alone says it was
// fixed, so a hand-off written before the close is stale; a hand-off after the
// last executed step is not.
func TestPostmortemResolutionOnlyMarksHandoffStale(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	investigation(t, st, c, r.ID)
	handoffLog(t, st, c, r.ID, "h_1", staleSummary())
	app(t, st, c, r.ID, events.RoomClosed, pmHuman("arjun", "Arjun"), events.RoomClosedPayload{Resolution: "fixed by hand"})
	app(t, st, c, r.ID, events.PostmortemDrafted, pmSystem, events.PostmortemDraftedPayload{Source: "handoff", SummaryRef: "h_1"})

	md := renderRoom(t, svc, r.ID)
	for _, want := range []string{
		"Nothing was run from Piperoom. Arjun closed the room at ",
		"with the resolution: fixed by hand",
		"No agent verification ran: the fix was made outside Piperoom, per the resolution.",
		"(before the fix: resolution at ",
		"none recorded after the fix",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("draft lacks %q:\n%s", want, md)
		}
	}
	if strings.Contains(md, "No fix was proposed") || strings.Contains(md, "No verification recorded") {
		t.Error("draft contradicts the resolution")
	}
}

// (b) A room fixed by an executed proposal: the steps, the agent's
// verification, a hand-off written after the fix kept as current, a proposal
// withdrawn at close, and the priced cost.
func TestPostmortemFixedByProposal(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	id := r.ID
	investigation(t, st, c, id)
	ag := events.AgentActor
	params := json.RawMessage(`{"connector":"orders-cdc","task":0}`)
	app(t, st, c, id, events.ProposalCreated, ag, events.ProposalCreatedPayload{
		ProposalID: "p_1", Title: "Restart task 0 of orders-cdc", Risk: "low", RequiredLayers: []string{"kafka-connect"},
		Steps: []events.ProposalStep{{Action: "connect_restart_task", Layer: "kafka-connect", Params: params, Description: "restart the failed task"}},
	})
	app(t, st, c, id, events.ProposalCreated, ag, events.ProposalCreatedPayload{
		ProposalID: "p_2", Title: "Reset the consumer offsets", Risk: "high", RequiredLayers: []string{"consumer"},
		Steps: []events.ProposalStep{{Action: "reset_offsets", Layer: "consumer", Description: "rewind one hour"}},
	})
	app(t, st, c, id, events.ApprovalGiven, pmHuman("arjun", "Arjun"), events.ApprovalGivenPayload{
		ProposalID: "p_1", Layer: "kafka-connect", Identity: &events.Identity{Method: "dev", Grant: "user:arjun"},
	})
	app(t, st, c, id, events.ActionExecuted, events.Actor{Kind: events.ActorSystem, ID: "executor", Name: "Executor"}, events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 0, OK: true, Output: "ok"})
	app(t, st, c, id, events.AgentUsage, ag, events.AgentUsagePayload{Model: "m", Role: "main", InputTokens: 800, OutputTokens: 200, CostUSD: costOf(0.0482)})
	app(t, st, c, id, events.AgentVerification, ag, events.AgentVerificationPayload{
		ProposalID: "p_1", Recovered: true, Verdict: "recovered: every layer check passed and data is flowing again",
	})
	// A repeated verdict adds no record.
	app(t, st, c, id, events.AgentVerification, ag, events.AgentVerificationPayload{ProposalID: "p_1", Recovered: true, Verdict: "recovered (repeat)", Repeat: true})
	handoffLog(t, st, c, id, "h_1", events.HandoffSummary{
		WhatBroke: "task 0 of orders-cdc failed", Why: "the task hit a transient error",
		Done: []string{"restarted task 0"}, RuledOut: []string{"broker health"},
		Open: []string{"watch the lag for an hour"}, Risks: []string{"a second failure needs a schema check"},
	})
	app(t, st, c, id, events.RoomClosed, pmHuman("priya", "Priya"), events.RoomClosedPayload{Resolution: "fixed"})
	app(t, st, c, id, events.ProposalWithdrawn, pmSystem, events.ProposalWithdrawnPayload{ProposalID: "p_2", Reason: events.ProposalWithdrawnRoomClosed})
	app(t, st, c, id, events.PostmortemDrafted, pmSystem, events.PostmortemDraftedPayload{Source: "handoff", SummaryRef: "h_1"})

	md := renderRoom(t, svc, id)
	pmGolden(t, "postmortem_fixed_by_proposal", id, md)

	for _, want := range []string{
		"Outcome: executed",
		"Outcome: withdrawn (room closed)",
		"- `p_1` recovered: every layer check passed and data is flowing again",
		"- watch the lag for an hour",
		"- a second failure needs a schema check",
		"cost: $0.12",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("draft lacks %q", want)
		}
	}
	if strings.Contains(md, "Earlier, from the") || strings.Contains(md, "No agent verification ran") {
		t.Error("a fix by proposal with a later hand-off must keep its lists current")
	}
	if strings.Contains(md, "still waiting for approval") {
		t.Error("a withdrawn proposal is not waiting for approval")
	}
	if strings.Contains(md, "recovered (repeat)") {
		t.Error("a repeated verdict was listed")
	}
}

// (c) An open room: the draft so far, no fix yet.
func TestPostmortemOpenRoom(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	investigation(t, st, c, r.ID)
	app(t, st, c, r.ID, events.ProposalCreated, events.AgentActor, events.ProposalCreatedPayload{
		ProposalID: "p_1", Title: "Restart the consumer", Risk: "low", RequiredLayers: []string{"consumer"},
		Steps: []events.ProposalStep{{Action: "restart_consumer", Layer: "consumer", Description: "restart order-sync"}},
	})
	app(t, st, c, r.ID, events.CommentAdded, pmHuman("kabir", "Kabir"), events.CommentAddedPayload{CommentID: "c_1", Text: "looking at it"})

	md := renderRoom(t, svc, r.ID)
	pmGolden(t, "postmortem_open_room", r.ID, md)
	for _, want := range []string{
		"Status: open (draft so far)",
		"Outcome: waiting for approval",
		"No verification recorded.",
		"- proposal `p_1` is still waiting for approval",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("draft lacks %q", want)
		}
	}
	for _, bad := range []string{"No agent verification ran", "Nothing was run from Piperoom", "Earlier, from the", "| Resolution |"} {
		if strings.Contains(md, bad) {
			t.Errorf("open draft says %q", bad)
		}
	}
}

// An open room with nothing in it says so plainly.
func TestPostmortemOpenRoomNothingYet(t *testing.T) {
	svc, _, _ := newTestService(t)
	r := createRoom(t, svc, "arjun")
	md := renderRoom(t, svc, r.ID)
	for _, want := range []string{"No fix was proposed.", "No verification recorded.", "## Still open\n\nnone recorded\n", "## Risks\n\nnone recorded\n"} {
		if !strings.Contains(md, want) {
			t.Errorf("draft lacks %q:\n%s", want, md)
		}
	}
	// Sections are separated by a blank line.
	if strings.Contains(md, "No fix was proposed.\n##") {
		t.Error("no blank line before the next heading")
	}
}

// A fix note added to a closed room after the draft was written: the model's
// closing summary pre-dates it, so its open items are labelled.
func TestPostmortemFixNoteAfterDraftMarksModelSummaryStale(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	investigation(t, st, c, r.ID)
	app(t, st, c, r.ID, events.RoomClosed, pmHuman("arjun", "Arjun"), events.RoomClosedPayload{Resolution: "fixed"})
	sum := events.HandoffSummary{WhatBroke: "w", Why: "stuck consumer", Open: []string{"confirm the lag drains"}, Risks: []string{"poison record may return"}}
	app(t, st, c, r.ID, events.PostmortemDrafted, pmSystem, events.PostmortemDraftedPayload{Source: "model", Summary: &sum})

	if md := renderRoom(t, svc, r.ID); !strings.Contains(md, "- confirm the lag drains") || strings.Contains(md, "Earlier, from the") {
		t.Fatalf("a model summary written at close is current:\n%s", md)
	}
	app(t, st, c, r.ID, events.CommentAdded, pmHuman("kabir", "Kabir"), events.CommentAddedPayload{
		CommentID: "c_1", Kind: events.CommentKindFixNote, Text: "lag drained, confirmed",
	})
	md := renderRoom(t, svc, r.ID)
	if !strings.Contains(md, "Earlier, from the closing summary at ") || !strings.Contains(md, "- confirm the lag drains") {
		t.Fatalf("a fix note after the summary must label its lists:\n%s", md)
	}
	if strings.HasPrefix(section(t, md, "## Risks"), "- poison record") {
		t.Fatal("a stale risk is shown as current")
	}
}

// A hand-off after the last fix keeps its lists current, whoever fixed it.
func TestPostmortemHandoffAfterFixStaysCurrent(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	investigation(t, st, c, r.ID)
	app(t, st, c, r.ID, events.CommentAdded, pmHuman("kabir", "Kabir"), events.CommentAddedPayload{
		CommentID: "c_1", Kind: events.CommentKindFixNote, Text: "restarted by hand",
	})
	handoffLog(t, st, c, r.ID, "h_1", events.HandoffSummary{WhatBroke: "w", Why: "y", Open: []string{"watch the lag"}, Risks: []string{"it may recur"}})
	app(t, st, c, r.ID, events.RoomClosed, pmHuman("arjun", "Arjun"), events.RoomClosedPayload{Resolution: "fixed"})
	app(t, st, c, r.ID, events.PostmortemDrafted, pmSystem, events.PostmortemDraftedPayload{Source: "handoff", SummaryRef: "h_1"})
	md := renderRoom(t, svc, r.ID)
	if !strings.Contains(md, "## Still open\n\n- watch the lag\n") || !strings.Contains(md, "## Risks\n\n- it may recur\n") || strings.Contains(md, "Earlier, from the") {
		t.Fatalf("lists written after the fix must be current:\n%s", md)
	}
}

// The cost line uses the room's priced usage.
func TestPostmortemCostLine(t *testing.T) {
	cases := []struct {
		name  string
		usage []events.AgentUsagePayload
		want  string
	}{
		{"none", nil, "- cost: not priced\n"},
		{"unpriced only", []events.AgentUsagePayload{{Model: "m", Role: "main"}}, "- cost: not priced\n"},
		{"priced", []events.AgentUsagePayload{{Model: "m", Role: "main", CostUSD: costOf(0.5)}, {Model: "m", Role: "main", CostUSD: costOf(0.2543)}}, "- cost: $0.75\n"},
		{"tiny", []events.AgentUsagePayload{{Model: "m", Role: "main", CostUSD: costOf(0.0004)}}, "- cost: less than $0.01\n"},
		{"zero is priced", []events.AgentUsagePayload{{Model: "m", Role: "main", CostUSD: costOf(0)}}, "- cost: $0.00\n"},
		{"mixed", []events.AgentUsagePayload{{Model: "m", Role: "main", CostUSD: costOf(1.2)}, {Model: "x", Role: "main"}}, "- cost: $1.20 (calls without a price not counted)\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			svc, st, clk := newTestService(t)
			r := createRoom(t, svc, "arjun")
			for _, u := range c.usage {
				app(t, st, clk, r.ID, events.AgentUsage, events.AgentActor, u)
			}
			if md := renderRoom(t, svc, r.ID); !strings.Contains(md, c.want) {
				t.Fatalf("want %q in\n%s", c.want, tailLines(md, 12))
			}
		})
	}
}

// Fix notes and the resolution are untrusted text and arrive escaped.
func TestPostmortemFixNoteEscaping(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	app(t, st, c, r.ID, events.CommentAdded, pmHuman("kabir", "Kabir"), events.CommentAddedPayload{
		CommentID: "c_1", Kind: events.CommentKindFixNote, Text: "# done\n[x](http://evil) | <b>hi</b>",
	})
	app(t, st, c, r.ID, events.RoomClosed, pmHuman("arjun", "Arjun"), events.RoomClosedPayload{Resolution: "# fixed\n[y](http://evil)"})
	md := renderRoom(t, svc, r.ID)
	for _, bad := range []string{"\n# done", "[x](", "<b>", "\n# fixed", "[y]("} {
		if strings.Contains(md, bad) {
			t.Errorf("unescaped %q in\n%s", bad, md)
		}
	}
}

// Rendering needs no extra clock: the same log renders the same draft.
func TestPostmortemFixNotesDeterministic(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	investigation(t, st, c, r.ID)
	handoffLog(t, st, c, r.ID, "h_1", staleSummary())
	app(t, st, c, r.ID, events.CommentAdded, pmHuman("kabir", "Kabir"), events.CommentAddedPayload{CommentID: "c_1", Kind: events.CommentKindFixNote, Text: "restarted"})
	app(t, st, c, r.ID, events.RoomClosed, pmHuman("arjun", "Arjun"), events.RoomClosedPayload{Resolution: "fixed"})
	app(t, st, c, r.ID, events.PostmortemDrafted, pmSystem, events.PostmortemDraftedPayload{Source: "handoff", SummaryRef: "h_1"})
	a := renderRoom(t, svc, r.ID)
	c.Advance(48 * time.Hour)
	if b := renderRoom(t, svc, r.ID); a != b {
		t.Fatal("the draft depends on the wall clock")
	}
}

// A fix note logged after the hand-off makes the hand-off stale as the source of
// the draft's prose: the closing summary comes from the model instead. A fix
// note before the hand-off leaves the hand-off fresh.
func TestPostmortemSourceFixNoteAfterHandoffIsStale(t *testing.T) {
	cases := []struct {
		name       string
		noteBefore bool
		wantSource string
		wantCalls  int
	}{
		{"fix note after the hand-off", false, "model", 1},
		{"fix note before the hand-off", true, "handoff", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := &modelCalls{}
			svc, st, _ := newPMService(t, m.write)
			r := createRoom(t, svc, "arjun")
			note := func() {
				if _, err := svc.AddCommentKind(context.Background(), mustUser(t, svc, "kabir"), r.ID, "", "restarted by hand", events.CommentKindFixNote); err != nil {
					t.Fatal(err)
				}
			}
			if c.noteBefore {
				note()
			}
			if _, err := svc.CreateHandoff(context.Background(), mustUser(t, svc, "arjun"), r.ID, "priya", false); err != nil {
				t.Fatal(err)
			}
			if !c.noteBefore {
				note()
			}
			closeAs(t, svc, r.ID)
			p := drafted(t, waitPM(t, st, r.ID))
			if p.Source != c.wantSource || m.n != c.wantCalls {
				t.Fatalf("source = %q with %d model calls; want %q with %d", p.Source, m.n, c.wantSource, c.wantCalls)
			}
		})
	}
}

// A step that failed is not a fix: the resolution still stands for the fix, and
// the draft says no step succeeded. A proposal never decided is reported as
// waiting when the room closed.
func TestPostmortemFailedStepThenResolution(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	id := r.ID
	investigation(t, st, c, id)
	for _, pid := range []string{"p_1", "p_2"} {
		app(t, st, c, id, events.ProposalCreated, events.AgentActor, events.ProposalCreatedPayload{
			ProposalID: pid, Title: "Fix " + pid, Risk: "low", RequiredLayers: []string{"consumer"},
			Steps: []events.ProposalStep{{Action: "restart_consumer", Layer: "consumer", Description: "restart"}},
		})
	}
	app(t, st, c, id, events.ActionExecuted, pmSystem, events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 0, OK: false, Output: "boom"})
	app(t, st, c, id, events.RoomClosed, pmHuman("arjun", "Arjun"), events.RoomClosedPayload{Resolution: "restarted by hand"})

	md := renderRoom(t, svc, id)
	for _, want := range []string{
		"No step succeeded from Piperoom. Arjun closed the room at ",
		"Outcome: a step failed",
		"- proposal `p_2` was still waiting for approval when the room closed",
		"No agent verification ran: the fix was made outside Piperoom, per the resolution.",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("draft lacks %q:\n%s", want, md)
		}
	}
}

// A room closed without a resolution has nothing to say it was fixed: no claim
// about a fix outside Piperoom, and the resolution reads "none recorded".
func TestPostmortemClosedWithoutResolution(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	investigation(t, st, c, r.ID)
	app(t, st, c, r.ID, events.RoomClosed, pmHuman("arjun", "Arjun"), events.RoomClosedPayload{})
	md := renderRoom(t, svc, r.ID)
	for _, want := range []string{"| Resolution | none recorded |", "Resolution: none recorded", "No fix was proposed.", "No verification recorded."} {
		if !strings.Contains(md, want) {
			t.Errorf("draft lacks %q:\n%s", want, md)
		}
	}
	if strings.Contains(md, "outside Piperoom") {
		t.Error("claims a fix outside Piperoom with nothing to say so")
	}
}

// section returns the body of the section that starts with heading, up to the
// next "## " heading.
func section(t *testing.T, md, heading string) string {
	t.Helper()
	i := strings.Index(md, "\n"+heading+"\n")
	if i < 0 {
		t.Fatalf("no section %q in\n%s", heading, md)
	}
	body := md[i+len(heading)+2:]
	body = strings.TrimPrefix(body, "\n")
	if j := strings.Index(body, "\n## "); j >= 0 {
		body = body[:j]
	}
	return body
}

func tail(md string) string { return tailLines(md, 6) }

func tailLines(md string, n int) string {
	lines := strings.Split(strings.TrimRight(md, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
