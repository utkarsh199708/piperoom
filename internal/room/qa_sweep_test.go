package room_test

// Regression tests for the 2026-10-14 QA sweep findings in the room service
// that a single process can show (QA5, QA6).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// A 1 MiB alert body of 1e308 tokens is ~54 MB once jsonb writes the numbers
// out: refused with a 400 before anything is written.
func TestAlertBodyNumberAmplificationRefused(t *testing.T) {
	svc, st, _ := newTestService(t)
	var b strings.Builder
	b.WriteString(`{"title":"amp","v":[`)
	for b.Len() < 1<<20-20 {
		b.WriteString("1e308,")
	}
	b.WriteString(`1]}`)
	_, _, err := svc.CreateRoomFromAlert(context.Background(), "amp", "sev2", "orders", json.RawMessage(b.String()))
	wantCode(t, err, "validation")
	if rooms, _ := st.ListRooms(context.Background()); len(rooms) != 0 {
		t.Fatalf("rooms written: %d", len(rooms))
	}
	// A few such numbers are fine.
	if _, _, err := svc.CreateRoomFromAlert(context.Background(), "big", "sev2", "orders", json.RawMessage(`{"v":[1e308,2e300]}`)); err != nil {
		t.Fatalf("small body with large numbers: %v", err)
	}
}

// The model is down when the driver hands over: the hand-off still goes
// through with a summary built from the log, marked as such, and accepting
// it moves the driver role.
func TestHandoffFallsBackWhenModelUnavailable(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Owners: testOwners(), Now: c.Now,
		Run: func(ctx context.Context, _ agent.Alert, deps agent.Deps, _ agent.Options) error {
			_ = deps.Sink.Emit(ctx, events.AgentStepStart, events.AgentStepStartPayload{StepID: "s1", Title: "Check the Debezium connector"})
			_ = deps.Sink.Emit(ctx, events.AgentHypothesis, events.AgentHypothesisPayload{Text: "schema change broke the connector", Confidence: "high"})
			<-ctx.Done()
			return nil
		},
		WriteHandoff: func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{}, errors.New("POST https://api.anthropic.com/v1/messages: 401 invalid x-api-key sk-ant-secret")
		},
	})
	t.Cleanup(svc.Close)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	waitFor(t, 5*time.Second, func() bool {
		snap, err := svc.GetRoom(ctx, roomID)
		return err == nil && snap.Hypothesis != nil
	})

	e, err := svc.CreateHandoff(ctx, mustUser(t, svc, "arjun"), roomID, "priya", false)
	if err != nil {
		t.Fatalf("handoff with the model down: %v", err)
	}
	p, err := events.Decode[events.HandoffCreatedPayload](e)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(p.Summary.WhatBroke, "auto summary (model unavailable)") {
		t.Fatalf("what_broke = %q, want the auto-summary mark", p.Summary.WhatBroke)
	}
	if !strings.Contains(p.Summary.Why, "schema change broke the connector") {
		t.Fatalf("why = %q, want the latest hypothesis", p.Summary.Why)
	}
	if len(p.Summary.Done) != 1 || !strings.Contains(p.Summary.Done[0], "Check the Debezium connector") {
		t.Fatalf("done = %v, want the recent step", p.Summary.Done)
	}
	if strings.Contains(string(e.Payload), "sk-ant") || strings.Contains(string(e.Payload), "401") {
		t.Fatalf("model error leaked into the summary: %s", e.Payload)
	}
	if _, err := svc.AcceptHandoff(ctx, mustUser(t, svc, "priya"), p.HandoffID); err != nil {
		t.Fatalf("accept: %v", err)
	}
	snap, _ := svc.GetRoom(ctx, roomID)
	if snap.Driver == nil || snap.Driver.ID != "priya" {
		t.Fatalf("driver = %+v, want priya", snap.Driver)
	}
}

func agentErrorStatuses(t *testing.T, st *memstore.Store, roomID string) []events.AgentStatusPayload {
	t.Helper()
	var out []events.AgentStatusPayload
	for _, e := range allEvents(t, st, roomID) {
		if e.Type != events.AgentStatus {
			continue
		}
		if p, err := events.Decode[events.AgentStatusPayload](e); err == nil && p.State == "error" {
			out = append(out, p)
		}
	}
	return out
}

// A run that fails on missing credentials: watchers see a fixed, safe detail,
// never the SDK's error text.
func TestAgentErrorStatusIsSanitised(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	runs := make(chan struct{}, 10)
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Owners: testOwners(), Now: c.Now, WriteHandoff: fakeWrite,
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			runs <- struct{}{}
			// What the SDK returns for a 401: the text names the header.
			return fmt.Errorf("agent: model turn: %w", &anthropic.Error{StatusCode: http.StatusUnauthorized})
		},
	})
	t.Cleanup(svc.Close)
	roomID := createRoom(t, svc, "arjun").ID
	waitFor(t, 5*time.Second, func() bool { return len(agentErrorStatuses(t, st, roomID)) >= 1 })
	for _, p := range agentErrorStatuses(t, st, roomID) {
		if p.Detail != "model unavailable: credentials missing or rejected" {
			t.Fatalf("detail = %q", p.Detail)
		}
	}
}

// The runner already wrote its own error status: the supervisor adds no
// second one, so each failed attempt shows once.
func TestAgentErrorStatusOncePerAttempt(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	attempts := make(chan struct{}, 10)
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Owners: testOwners(), Now: c.Now, WriteHandoff: fakeWrite,
		Run: func(ctx context.Context, _ agent.Alert, deps agent.Deps, _ agent.Options) error {
			_ = deps.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working"})
			_ = deps.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "error", Detail: "model request failed"})
			attempts <- struct{}{}
			if len(attempts) >= 2 {
				<-ctx.Done()
				return nil
			}
			return errors.New("agent: model turn: credentials missing")
		},
	})
	t.Cleanup(svc.Close)
	roomID := createRoom(t, svc, "arjun").ID
	waitFor(t, 15*time.Second, func() bool { return len(attempts) >= 2 })
	time.Sleep(100 * time.Millisecond)
	got := agentErrorStatuses(t, st, roomID)
	if len(got) != 2 {
		t.Fatalf("error statuses = %+v, want one per attempt (2)", got)
	}
	for _, p := range got {
		if p.Detail != "model request failed" {
			t.Fatalf("detail = %q", p.Detail)
		}
	}
}

// Store-level closed guard, single process: a comment on a room whose close
// the cache has not seen yet answers room_closed, not 500.
func TestStoreClosedRoomAnswersRoomClosed(t *testing.T) {
	svc, st, c := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	// Another writer closes the room behind the service's back.
	if _, err := st.Append(ctx, roomID, events.RoomClosed, events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}, events.RoomClosedPayload{Resolution: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := st.CloseRoom(ctx, roomID, c.Now()); err != nil {
		t.Fatal(err)
	}
	_, err := svc.AddComment(ctx, mustUser(t, svc, "priya"), roomID, "", "late")
	wantCode(t, err, "room_closed")
}
