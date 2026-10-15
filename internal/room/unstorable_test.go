package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

const unstorableMsg = "text contains characters that cannot be stored"

// badTexts are user strings Postgres cannot hold: a NUL, invalid UTF-8 and the
// UTF-8 form of a surrogate (also invalid).
var badTexts = map[string]string{
	"nul":               "a\x00b",
	"invalid utf-8":     "a\xffb",
	"encoded surrogate": "a\xed\xa0\x80b",
}

func wantUnstorable400(t *testing.T, err error) {
	t.Helper()
	var re *room.Error
	if !errors.As(err, &re) {
		t.Fatalf("err = %v, want a *room.Error", err)
	}
	if re.Status != http.StatusBadRequest || re.Code != "validation" || re.Msg != unstorableMsg {
		t.Fatalf("err = %d %s %q, want 400 validation %q", re.Status, re.Code, re.Msg, unstorableMsg)
	}
}

func lastSeq(t *testing.T, st *memstore.Store, roomID string) int64 {
	t.Helper()
	r, err := st.GetRoom(context.Background(), roomID)
	if err != nil {
		t.Fatalf("get room: %v", err)
	}
	return r.LastSeq
}

// Every person-typed text is refused with a 400 before any write: last_seq does
// not move and no side-table row is written.
func TestUserTextUnstorableIs400AndWritesNothing(t *testing.T) {
	ctx := context.Background()
	act := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, st, _ := newTestService(t, act)
	roomID := createRoom(t, svc, "arjun").ID
	arjun, priya := mustUser(t, svc, "arjun"), mustUser(t, svc, "priya")

	pid, err := svc.CreateProposal(ctx, roomID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "low", Rollback: "revert",
		Steps: []events.ProposalStep{{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")}},
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	// The system previews the proposal asynchronously; let that event land so
	// it does not move last_seq under the checks below.
	waitFor(t, 5*time.Second, func() bool {
		return len(eventsOf[events.ProposalPreviewedPayload](t, svc, roomID, events.ProposalPreviewed)) > 0
	})

	for name, bad := range badTexts {
		t.Run(name, func(t *testing.T) {
			before := lastSeq(t, st, roomID)
			rooms, _ := st.ListRooms(ctx)

			calls := map[string]func() error{
				"comment text": func() error {
					_, err := svc.AddComment(ctx, arjun, roomID, "", bad)
					return err
				},
				"comment step id": func() error {
					_, err := svc.AddComment(ctx, arjun, roomID, bad, "hello")
					return err
				},
				"steer by driver": func() error {
					_, err := svc.Steer(ctx, arjun, roomID, bad)
					return err
				},
				"suggestion by non-driver": func() error {
					_, err := svc.Steer(ctx, priya, roomID, bad)
					return err
				},
				"trace key": func() error {
					_, err := svc.Trace(ctx, arjun, roomID, "public.orders", bad)
					return err
				},
				"reject reason": func() error {
					_, err := svc.RejectProposal(ctx, priya, pid, "postgres", bad, proposalHash(t, svc, pid))
					return err
				},
				"reject layer": func() error {
					_, err := svc.RejectProposal(ctx, priya, pid, bad, "no", proposalHash(t, svc, pid))
					return err
				},
				"approve layer": func() error {
					_, err := svc.ApproveProposal(ctx, priya, pid, bad, proposalHash(t, svc, pid))
					return err
				},
				"close resolution": func() error {
					_, err := svc.CloseRoom(ctx, arjun, roomID, bad)
					return err
				},
				"room title": func() error {
					_, err := svc.CreateRoom(ctx, arjun, bad, "sev2", "orders")
					return err
				},
			}
			for call, fn := range calls {
				wantUnstorable400(t, fn())
				if got := lastSeq(t, st, roomID); got != before {
					t.Fatalf("%s: last_seq %d -> %d, want unchanged", call, before, got)
				}
				if after, _ := st.ListRooms(ctx); len(after) != len(rooms) {
					t.Fatalf("%s: rooms %d -> %d, want unchanged", call, len(rooms), len(after))
				}
			}
		})
	}

	// The refused calls left the room open and the proposal undecided: a valid
	// reject with a plain reason still works, and the room still closes.
	if p, err := st.GetProposal(ctx, pid); err != nil || p.Status != "open" {
		t.Fatalf("proposal = %+v, %v; want still open", p, err)
	}
	if _, err := svc.RejectProposal(ctx, priya, pid, "postgres", "not now", proposalHash(t, svc, pid)); err != nil {
		t.Fatalf("valid reject after refused ones: %v", err)
	}
	if _, err := svc.CloseRoom(ctx, arjun, roomID, "done"); err != nil {
		t.Fatalf("valid close after refused ones: %v", err)
	}
}

// Valid text that only looks odd is stored untouched.
func TestUserTextOddButStorable(t *testing.T) {
	ctx := context.Background()
	svc, st, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	text := "café 中 \U0001F600 \\u0000 �"
	ev, err := svc.AddComment(ctx, mustUser(t, svc, "arjun"), roomID, "", text)
	if err != nil {
		t.Fatalf("comment: %v", err)
	}
	p, err := events.Decode[events.CommentAddedPayload](ev)
	if err != nil || p.Text != text {
		t.Fatalf("comment text = %q, %v; want %q", p.Text, err, text)
	}
	if err := store.CheckJSON(allEvents(t, st, roomID)[len(allEvents(t, st, roomID))-1].Payload); err != nil {
		t.Fatalf("stored payload fails the jsonb check: %v", err)
	}
}

// Agent text is made storable, never refused: a run never fails on bad tool
// output, and what is stored passes the same check Postgres enforces.
func TestAgentTextIsSanitizedNotRejected(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	emitErrs := make(chan error, 8)
	done := make(chan struct{})
	reg := actions.NewRegistry()
	act := &nulOutputAction{fakeAction: fakeAction{name: "pg_act", layer: "postgres"}}
	if err := reg.Register(act); err != nil {
		t.Fatalf("register: %v", err)
	}
	svc := room.New(room.Deps{
		Store:   st,
		Actions: reg,
		Users:   room.DefaultUsers(),
		Owners:  testOwners(),
		Now:     c.Now,
		Run: func(ctx context.Context, _ agent.Alert, deps agent.Deps, _ agent.Options) error {
			send := func(typ events.Type, payload any) { emitErrs <- deps.Sink.Emit(ctx, typ, payload) }
			send(events.AgentStepStart, events.AgentStepStartPayload{StepID: "s1", Title: "look\x00at \xff lag"})
			send(events.AgentToolCall, events.AgentToolCallPayload{
				StepID: "s1", ToolUseID: "t1", Tool: "sql",
				Input: json.RawMessage(`{"q":"select 'x` + `\u0000` + `y'","raw":"a` + "\xff" + `b"}`),
			})
			send(events.AgentToolResult, events.AgentToolResultPayload{
				StepID: "s1", ToolUseID: "t1", Tool: "sql", Summary: "rows\x00",
				Data: json.RawMessage(`{"name":"` + `\ud83d` + `","ok":"` + `\\u0000` + `"}`),
			})
			send(events.AgentMessage, events.AgentMessagePayload{Text: "msg \x00 \xed\xa0\x80 end"})
			send(events.AgentHypothesis, events.AgentHypothesisPayload{Text: "bad\x00guess", BasedOnSteps: []string{"s\x001"}, Confidence: "low"})
			close(done)
			<-ctx.Done()
			return nil
		},
		WriteHandoff: func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{
				WhatBroke: "broke\x00", Why: "why \xff", Done: []string{"d\x00"}, RuledOut: []string{"r\xff"},
				Open: []string{"o\x00"}, Risks: []string{"k\x00"},
			}, nil
		},
	})
	t.Cleanup(svc.Close)
	roomID := createRoom(t, svc, "arjun").ID
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fake agent did not finish")
	}
	for i := 0; i < 5; i++ {
		if err := <-emitErrs; err != nil {
			t.Fatalf("agent emit %d failed: %v", i, err)
		}
	}

	// A proposal whose fields carry bad text is stored cleaned.
	pid, err := svc.CreateProposal(ctx, roomID, events.ProposalCreatedPayload{
		Title: "fix\x00 it", Risk: "risk \xff", Rollback: "undo\x00",
		Steps: []events.ProposalStep{{
			Action: "pg_act", Layer: "postgres", Description: "step\x00",
			Params: json.RawMessage(`{"v":"a` + `\u0000` + `b"}`),
		}},
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("agent proposal with bad text was refused: %v", err)
	}
	prow, err := st.GetProposal(ctx, pid)
	if err != nil {
		t.Fatalf("get proposal: %v", err)
	}
	if err := prow.Check(); err != nil {
		t.Fatalf("stored proposal row is not storable: %v", err)
	}
	if prow.Title != "fix� it" || prow.Risk != "risk �" {
		t.Fatalf("proposal row = %q / %q, want U+FFFD in place of the bad bytes", prow.Title, prow.Risk)
	}

	// The executor output of an approved step is cleaned too.
	if _, err := svc.ApproveProposal(ctx, mustUser(t, svc, "priya"), pid, "postgres", proposalHash(t, svc, pid)); err != nil {
		t.Fatalf("approve: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		p, err := st.GetProposal(ctx, pid)
		return err == nil && p.Status == "executed"
	})

	// A hand-off summary from the model is cleaned.
	ev, err := svc.CreateHandoff(ctx, mustUser(t, svc, "arjun"), roomID, "priya", false)
	if err != nil {
		t.Fatalf("handoff with bad summary text was refused: %v", err)
	}
	hp, err := events.Decode[events.HandoffCreatedPayload](ev)
	if err != nil || hp.Summary.WhatBroke != "broke�" || hp.Summary.Why != "why �" || hp.Summary.Done[0] != "d�" {
		t.Fatalf("handoff summary = %+v, %v; want U+FFFD in place of the bad bytes", hp.Summary, err)
	}

	evs := allEvents(t, st, roomID)
	seen := map[events.Type]bool{}
	for _, e := range evs {
		if err := store.CheckJSON(e.Payload); err != nil {
			t.Fatalf("event %d (%s) is stored with unstorable content: %v\n%s", e.Seq, e.Type, err, e.Payload)
		}
		if !utf8.Valid(e.Payload) {
			t.Fatalf("event %d (%s) payload is not valid UTF-8", e.Seq, e.Type)
		}
		seen[e.Type] = true
	}
	for _, typ := range []events.Type{events.AgentStepStart, events.AgentToolCall, events.AgentToolResult, events.AgentMessage, events.AgentHypothesis, events.ProposalCreated, events.ActionExecuted, events.HandoffCreated} {
		if !seen[typ] {
			t.Fatalf("no %s event stored", typ)
		}
	}
	for _, e := range evs {
		switch e.Type {
		case events.AgentMessage:
			p, _ := events.Decode[events.AgentMessagePayload](e)
			if !strings.Contains(p.Text, "msg �") || strings.ContainsRune(p.Text, 0) {
				t.Fatalf("agent message = %q, want the NUL replaced by U+FFFD", p.Text)
			}
		case events.AgentToolResult:
			// An escaped backslash followed by "u0000" is text, not an escape:
			// it must survive untouched, while the lone surrogate escape is cleaned.
			if !strings.Contains(string(e.Payload), `\\u0000`) || strings.Contains(string(e.Payload), `\ud83d`) {
				t.Fatalf("tool result payload = %s", e.Payload)
			}
		case events.ActionExecuted:
			p, _ := events.Decode[events.ActionExecutedPayload](e)
			if p.Output != "out��" {
				t.Fatalf("action output = %q, want %q", p.Output, "out��")
			}
		}
	}
}

// nulOutputAction is an action whose output holds a NUL and an invalid byte.
type nulOutputAction struct{ fakeAction }

func (a *nulOutputAction) Execute(context.Context, json.RawMessage) (string, error) {
	return "out\x00\xff", nil
}

// Bidi embedding, override and isolate controls reorder the text for every
// reader that does not strip them (#358): typed text holding one is refused
// with a 400 and nothing is written; an alert title keeps its text without
// them.
func TestBidiControlsRefusedInTypedTextStrippedFromAlertTitles(t *testing.T) {
	ctx := context.Background()
	svc, st, _ := newTestService(t)
	arjun := mustUser(t, svc, "arjun")
	roomID := createRoom(t, svc, "arjun").ID
	for _, r := range []rune{0x202A, 0x202E, 0x2066, 0x2069} {
		bad := "orders " + string(r) + "gal gnissim"
		before := lastSeq(t, st, roomID)
		calls := map[string]func() error{
			"room title": func() error {
				_, err := svc.CreateRoom(ctx, arjun, bad, "sev2", "orders")
				return err
			},
			"comment": func() error {
				_, err := svc.AddComment(ctx, arjun, roomID, "", bad)
				return err
			},
			"steer": func() error {
				_, err := svc.Steer(ctx, arjun, roomID, bad)
				return err
			},
		}
		for name, call := range calls {
			var re *room.Error
			if err := call(); !errors.As(err, &re) || re.Status != http.StatusBadRequest || re.Code != "validation" || !strings.Contains(re.Msg, "bidirectional") {
				t.Fatalf("%s with U+%04X: err = %v, want 400 validation (bidirectional)", name, r, err)
			}
		}
		if after := lastSeq(t, st, roomID); after != before {
			t.Fatalf("U+%04X: last_seq %d -> %d, want no write", r, before, after)
		}
	}
	if rooms, _ := st.ListRooms(ctx); len(rooms) != 1 {
		t.Fatalf("rooms = %d, want only the first one", len(rooms))
	}

	snap, _, err := svc.OpenAlertRoom(ctx, alertin.Alert{
		Source: alertin.Generic, Status: alertin.StatusFiring, Title: "orders ‮gal gnissim‬",
		Severity: "sev2", PipelineID: "orders", Raw: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("alert room: %v", err)
	}
	if snap.Title != "orders gal gnissim" {
		t.Fatalf("alert title = %q, want the bidi controls dropped", snap.Title)
	}
}

// A watcher note is evidence, not typed text: bidi controls are dropped and
// the note is kept (#358).
func TestRoomNoteDropsBidiControls(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	if err := svc.PostRoomNote(ctx, roomID, "lag ‮gal‬ on ⁦orders⁩"); err != nil {
		t.Fatalf("post note: %v", err)
	}
	msgs := eventsOf[events.AgentMessagePayload](t, svc, roomID, events.AgentMessage)
	if len(msgs) == 0 || msgs[len(msgs)-1].Text != "lag gal on orders" {
		t.Fatalf("notes = %+v, want the last one %q", msgs, "lag gal on orders")
	}
}

// The label form is pre-filled from the agent's hypothesis, which may carry
// bidi controls: they are dropped from cause and fix, not refused (#358).
func TestLabelDropsBidiControlsFromAgentText(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestService(t)
	arjun := mustUser(t, svc, "arjun")
	roomID := createRoom(t, svc, "arjun").ID
	if _, err := svc.CloseRoom(ctx, arjun, roomID, "done"); err != nil {
		t.Fatal(err)
	}
	ev, err := svc.Label(ctx, arjun, roomID, room.LabelInput{Cause: "column ⁦amount⁩ dropped", FixThatWorked: "‪restart‬"})
	if err != nil {
		t.Fatalf("label: %v", err)
	}
	p, err := events.Decode[events.RoomLabelledPayload](ev)
	if err != nil || p.Cause != "column amount dropped" || p.FixThatWorked != "restart" {
		t.Fatalf("label = %+v, %v; want the controls dropped", p, err)
	}
}
