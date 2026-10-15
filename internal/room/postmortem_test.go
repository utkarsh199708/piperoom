package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata golden files")

func app(t *testing.T, st *memstore.Store, c *clock, roomID string, typ events.Type, actor events.Actor, payload any) events.Event {
	t.Helper()
	c.Advance(30 * time.Second)
	e, err := st.Append(context.Background(), roomID, typ, actor, payload)
	if err != nil {
		t.Fatalf("append %s: %v", typ, err)
	}
	return e
}

func pmHuman(id, name string) events.Actor {
	return events.Actor{Kind: events.ActorHuman, ID: id, Name: name, Verified: true}
}

// schemaChangeLog appends a room's worth of events after room.created.
func schemaChangeLog(t *testing.T, st *memstore.Store, c *clock, id string) {
	t.Helper()
	ag := events.AgentActor
	app(t, st, c, id, events.AgentStepStart, ag, events.AgentStepStartPayload{})
	app(t, st, c, id, events.AgentToolCall, ag, events.AgentToolCallPayload{Tool: "connect_status"})
	app(t, st, c, id, events.AgentToolResult, ag, events.AgentToolResultPayload{Tool: "connect_status", IsError: true})
	app(t, st, c, id, events.AgentUsage, ag, events.AgentUsagePayload{Model: "m", Role: "main", InputTokens: 100, OutputTokens: 20})
	app(t, st, c, id, events.AgentHypothesis, ag, events.AgentHypothesisPayload{Text: "task 0 failed: amount is numeric now", Confidence: "medium"})
	app(t, st, c, id, events.CommentAdded, pmHuman("kabir", "Kabir"), map[string]any{"text": "# injected"})
	app(t, st, c, id, events.HypothesisVerdict, pmHuman("kabir", "Kabir"), events.HypothesisVerdictPayload{Verdict: "confirmed", HypothesisSeq: 6})
	params := json.RawMessage(`{ "compat": "NONE" }`)
	app(t, st, c, id, events.ProposalCreated, ag, events.ProposalCreatedPayload{
		ProposalID: "p_1", Title: "Set compatibility to NONE", Risk: "low", RequiredLayers: []string{"kafka"},
		Steps: []events.ProposalStep{{Action: "set_compat", Layer: "kafka", Params: params, Description: "relax |compat|"}},
	})
	app(t, st, c, id, events.ApprovalGiven, pmHuman("priya", "Priya"), events.ApprovalGivenPayload{
		ProposalID: "p_1", Layer: "kafka", Identity: &events.Identity{Method: "dev", Grant: "user:priya"},
	})
	app(t, st, c, id, events.ActionExecuted, events.Actor{Kind: events.ActorSystem, ID: "executor", Name: "Executor"}, events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 0, OK: true, Output: "ok"})
	app(t, st, c, id, events.AgentVerification, ag, events.AgentVerificationPayload{ProposalID: "p_1", Recovered: true, Verdict: "task 0 RUNNING"})
	app(t, st, c, id, events.RoomClosed, pmHuman("priya", "Priya"), events.RoomClosedPayload{Resolution: "fixed"})
}

func renderRoom(t *testing.T, svc *room.Service, id string, opts ...room.PostmortemOption) string {
	t.Helper()
	b, err := svc.RenderPostmortem(context.Background(), id, opts...)
	if err != nil {
		t.Fatalf("RenderPostmortem: %v", err)
	}
	return string(b)
}

func TestPostmortemGolden(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	schemaChangeLog(t, st, c, r.ID)
	got := strings.ReplaceAll(renderRoom(t, svc, r.ID), r.ID, "ROOM")

	const path = "testdata/postmortem_schema_change.md"
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

func TestPostmortemContentAndEscaping(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	schemaChangeLog(t, st, c, r.ID)
	md := renderRoom(t, svc, r.ID)
	for _, want := range []string{
		"Status: closed", "## Timeline", "hypothesis (medium)", "hypothesis confirmed by Kabir",
		"approval given for `p_1`: Priya (user:priya, dev)", "`p_1` step 1 ok", "verification: `p_1` recovered; task 0 RUNNING",
		"## Root cause", "## What was done", "kafka — Priya (user:priya, dev)", "Outcome: executed",
		`{"compat":"NONE"}`, "- tool calls: 1 (1 failed)", "cost: not priced", "summary source: none",
		`relax \|compat\|`,
	} {
		if !strings.Contains(md, want) {
			t.Errorf("draft lacks %q", want)
		}
	}
	if !strings.Contains(md, `Kabir commented: \# injected`) || strings.Contains(md, "| # injected") {
		t.Errorf("comment must arrive escaped:\n%s", md)
	}
	// A hostile title must be escaped too.
	room2 := createRoomTitled(t, svc, "# Title\n[x](http://evil)")
	md2 := renderRoom(t, svc, room2)
	if strings.Contains(md2, "\n# Title") || strings.Contains(md2, "[x](") {
		t.Fatalf("title not escaped:\n%s", md2)
	}
	if !strings.Contains(md2, "Status: open (draft so far)") {
		t.Fatal("an open room says so")
	}
	if strings.Contains(md, "Export sha256") {
		t.Fatal("hash is off by default")
	}
	if h := renderRoom(t, svc, r.ID, room.WithPostmortemHash()); !strings.Contains(h, "Export sha256") {
		t.Fatal("with hash")
	}
}

func createRoomTitled(t *testing.T, svc *room.Service, title string) string {
	t.Helper()
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), title, "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	return snap.ID
}

func TestPostmortemNotFoundAndTooLarge(t *testing.T) {
	svc, _, _ := newTestService(t)
	if _, err := svc.RenderPostmortem(context.Background(), "r_missing"); err == nil {
		t.Fatal("unknown room must fail")
	} else {
		wantCode(t, err, "not_found")
	}
	small := room.New(room.Deps{Store: memstore.New(), Users: room.DefaultUsers(), Run: fakeRun, WriteHandoff: fakeWrite, ExportMaxEvents: 2})
	defer small.Close()
	r, err := small.CreateRoom(context.Background(), mustUser(t, small, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := small.AddComment(context.Background(), mustUser(t, small, "arjun"), r.ID, "", "hi"); err != nil {
			t.Fatal(err)
		}
	}
	_, err = small.RenderPostmortem(context.Background(), r.ID)
	wantCode(t, err, "export_too_large")
}

// ---- source selection at close ----

func newPMService(t *testing.T, w agent.PostmortemWriter) (*room.Service, *memstore.Store, *clock) {
	t.Helper()
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, WriteHandoff: fakeWrite,
		WritePostmortem: w, Now: c.Now,
	})
	t.Cleanup(svc.Close)
	return svc, st, c
}

func waitPM(t *testing.T, st *memstore.Store, id string) events.Event {
	t.Helper()
	var found events.Event
	waitFor(t, 3*time.Second, func() bool {
		for _, e := range allEvents(t, st, id) {
			if e.Type == events.PostmortemDrafted || e.Type == events.PostmortemFailed {
				found = e
				return true
			}
		}
		return false
	})
	return found
}

func closeAs(t *testing.T, svc *room.Service, id string) {
	t.Helper()
	if _, err := svc.CloseRoom(context.Background(), mustUser(t, svc, "arjun"), id, "fixed"); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func drafted(t *testing.T, e events.Event) events.PostmortemDraftedPayload {
	t.Helper()
	if e.Type != events.PostmortemDrafted {
		t.Fatalf("got %s, want postmortem.drafted", e.Type)
	}
	p, err := events.Decode[events.PostmortemDraftedPayload](e)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

type modelCalls struct{ n int }

func (m *modelCalls) write(ctx context.Context, evs []events.Event, _ agent.Options) (agent.PostmortemDraft, error) {
	m.n++
	return agent.PostmortemDraft{
		Summary:    events.HandoffSummary{WhatBroke: "orders stale", Why: "numeric amount", Done: []string{"fix"}},
		Violations: []string{"unknown_number open[0]"},
	}, nil
}

func TestPostmortemSourceHandoffWhenFresh(t *testing.T) {
	m := &modelCalls{}
	svc, st, _ := newPMService(t, m.write)
	r := createRoom(t, svc, "arjun")
	if _, err := svc.CreateHandoff(context.Background(), mustUser(t, svc, "arjun"), r.ID, "priya", false); err != nil {
		t.Fatal(err)
	}
	closeAs(t, svc, r.ID)
	p := drafted(t, waitPM(t, st, r.ID))
	if p.Source != "handoff" || !strings.HasPrefix(p.SummaryRef, "h_") || m.n != 0 {
		t.Fatalf("payload = %+v, model calls = %d; want a hand-off source and no model call", p, m.n)
	}
	md := renderRoom(t, svc, r.ID)
	if !strings.Contains(md, "Why: int\\-\\>bigint") && !strings.Contains(md, "bigint") {
		t.Fatalf("hand-off prose missing:\n%s", md)
	}
	if !strings.Contains(md, "summary source: handoff `h_") {
		t.Fatalf("footer:\n%s", md)
	}
	snap, err := svc.GetRoom(context.Background(), r.ID)
	if err != nil || snap.Postmortem.Status != "drafted" || snap.Postmortem.Source != "handoff" {
		t.Fatalf("snapshot postmortem = %+v, %v", snap.Postmortem, err)
	}
}

func TestPostmortemSourceModelWhenHandoffIsStale(t *testing.T) {
	m := &modelCalls{}
	svc, st, c := newPMService(t, m.write)
	r := createRoom(t, svc, "arjun")
	if _, err := svc.CreateHandoff(context.Background(), mustUser(t, svc, "arjun"), r.ID, "priya", false); err != nil {
		t.Fatal(err)
	}
	// Work after the hand-off makes its summary stale.
	app(t, st, c, r.ID, events.AgentHypothesis, events.AgentActor, events.AgentHypothesisPayload{Text: "later", Confidence: "low"})
	closeAs(t, svc, r.ID)
	p := drafted(t, waitPM(t, st, r.ID))
	if p.Source != "model" || p.Summary == nil || p.Summary.Why != "numeric amount" || len(p.Violations) != 1 || m.n != 1 {
		t.Fatalf("payload = %+v, model calls = %d", p, m.n)
	}
	if md := renderRoom(t, svc, r.ID); !strings.Contains(md, "Grounding check") || !strings.Contains(md, "Why: numeric amount") {
		t.Fatalf("model prose / violation note missing:\n%s", md)
	}
}

func TestPostmortemSourceEventsWithoutWriter(t *testing.T) {
	svc, st, _ := newPMService(t, nil)
	r := createRoom(t, svc, "arjun")
	closeAs(t, svc, r.ID)
	if p := drafted(t, waitPM(t, st, r.ID)); p.Source != "events" {
		t.Fatalf("source = %q, want events", p.Source)
	}
}

func TestPostmortemWriterErrorFailsWithFixedCode(t *testing.T) {
	svc, st, _ := newPMService(t, func(context.Context, []events.Event, agent.Options) (agent.PostmortemDraft, error) {
		return agent.PostmortemDraft{}, errors.New("secret-token-123 in provider error")
	})
	r := createRoom(t, svc, "arjun")
	closeAs(t, svc, r.ID)
	e := waitPM(t, st, r.ID)
	if e.Type != events.PostmortemFailed || !strings.Contains(string(e.Payload), `"summary_failed"`) || strings.Contains(string(e.Payload), "secret") {
		t.Fatalf("event = %s %s", e.Type, e.Payload)
	}
	if e.Actor.ID != "system" {
		t.Fatalf("actor = %+v, want system", e.Actor)
	}
	// The events-only render still works.
	if md := renderRoom(t, svc, r.ID); !strings.Contains(md, "Status: closed") {
		t.Fatal("draft must render after a failure")
	}
	waitFor(t, time.Second, func() bool {
		s, _ := svc.GetRoom(context.Background(), r.ID)
		return s.Postmortem.Status == "failed" && s.Postmortem.Reason == "summary_failed"
	})
}

func TestPostmortemSnapshotStatusAndOpenRoom(t *testing.T) {
	svc, _, _ := newPMService(t, nil)
	r := createRoom(t, svc, "arjun")
	if r.Postmortem.Status != "none" {
		t.Fatalf("open room status = %q", r.Postmortem.Status)
	}
}

func TestPostmortemRestartFallbackIsEventsOnly(t *testing.T) {
	m := &modelCalls{}
	svc, st, c := newPMService(t, m.write)
	r := createRoom(t, svc, "arjun")
	// A closed room whose drafter died: room.closed and nothing after it.
	app(t, st, c, r.ID, events.RoomClosed, pmHuman("arjun", "Arjun"), events.RoomClosedPayload{Resolution: "fixed"})

	restarted := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, WriteHandoff: fakeWrite,
		WritePostmortem: m.write, Now: c.Now,
	})
	defer restarted.Close()
	if err := restarted.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p := drafted(t, waitPM(t, st, r.ID)); p.Source != "events" || m.n != 0 {
		t.Fatalf("payload = %+v, model calls = %d; a restart never calls the model", p, m.n)
	}

	// Long closed: no draft is written, the status is none.
	r2 := createRoom(t, svc, "arjun")
	app(t, st, c, r2.ID, events.RoomClosed, pmHuman("arjun", "Arjun"), events.RoomClosedPayload{Resolution: "fixed"})
	c.Advance(time.Hour)
	again := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, WriteHandoff: fakeWrite, Now: c.Now})
	defer again.Close()
	if err := again.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	s2, err := again.GetRoom(context.Background(), r2.ID)
	if err != nil || s2.Postmortem.Status != "none" {
		t.Fatalf("old room postmortem = %+v, %v", s2.Postmortem, err)
	}
}

func TestPostmortemVerificationSection(t *testing.T) {
	ag := events.AgentActor
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		evs     []events.AgentVerificationPayload
		want    []string
		notWant []string
	}{
		{name: "none", want: []string{"No verification recorded."}},
		{
			name: "recovered",
			evs: []events.AgentVerificationPayload{{ProposalID: "p_1", Recovered: true, Verdict: "recovered: all good",
				Checks: []events.VerificationCheck{{Layer: "kafka", Name: "connect_status", OK: true, Detail: "RUNNING"}},
				DataFlow: &events.DataFlowCheck{Checked: true, Fresh: true,
					Sinks: []events.SinkCheck{{Table: "orders", Newest: now, Passed: true}}}}},
			want:    []string{"- `p_1` recovered: all good; data flow fresh (orders passed)"},
			notWant: []string{"No verification recorded", "failed kafka"},
		},
		{
			name: "failed layer check",
			evs: []events.AgentVerificationPayload{{ProposalID: "p_2", Verdict: "still failing",
				Checks:   []events.VerificationCheck{{Layer: "kafka", Name: "connect_status", OK: false, Detail: "task 0 FAILED"}},
				DataFlow: &events.DataFlowCheck{Checked: false, Detail: "warehouse down"}}},
			want: []string{"`p_2` not recovered; still failing; failed kafka/connect\\_status: task 0 FAILED; data flow not checked: warehouse down"},
		},
		{
			name: "repeat ignored",
			evs: []events.AgentVerificationPayload{
				{ProposalID: "p_1", Recovered: true, Verdict: "first verdict"},
				{ProposalID: "p_1", Recovered: true, Verdict: "second verdict", Repeat: true},
			},
			want:    []string{"first verdict"},
			notWant: []string{"second verdict"},
		},
		{
			name: "hostile detail",
			evs: []events.AgentVerificationPayload{{ProposalID: "p_3", Verdict: "v",
				Checks: []events.VerificationCheck{{Layer: "db", Name: "x", Detail: "ok\n# Heading\n[click](http://evil) <script>alert(1)</script> | a |"}}}},
			want:    []string{"failed db/x: ok \\# Heading \\[click\\]"},
			notWant: []string{"\n# Heading", "<script>", "[click]("},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, st, c := newTestService(t)
			r := createRoom(t, svc, "arjun")
			for _, p := range tc.evs {
				app(t, st, c, r.ID, events.AgentVerification, ag, p)
			}
			md := renderRoom(t, svc, r.ID)
			i := strings.Index(md, "## Verification")
			j := strings.Index(md, "## Still open")
			if i < 0 || j < i {
				t.Fatalf("no verification section:\n%s", md)
			}
			sec := md[i:j]
			for _, w := range tc.want {
				if !strings.Contains(sec, w) {
					t.Errorf("section lacks %q:\n%s", w, sec)
				}
			}
			for _, w := range tc.notWant {
				if strings.Contains(sec, w) {
					t.Errorf("section has %q:\n%s", w, sec)
				}
			}
		})
	}
}
