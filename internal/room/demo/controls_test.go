package demo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/room"
)

// Presenter controls (KAN-398): skip, pause, resume and reset.

// blockingInbox never delivers input; Wait returns only when the run's context
// ends, like a room that stays open after the story.
type blockingInbox struct {
	wake chan struct{}
}

func newBlockingInbox() *blockingInbox {
	return &blockingInbox{wake: make(chan struct{}, 1)}
}

func (b *blockingInbox) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.wake:
		return nil
	}
}

func (b *blockingInbox) Drain(context.Context) ([]agent.HumanInput, error) {
	return nil, nil
}

// countProposals records how many proposals the run created.
type countProposals struct {
	mu sync.Mutex
	n  int
}

func (p *countProposals) Create(context.Context, events.ProposalCreatedPayload) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.n++
	return "p_KAN398TESTPROPOSAL0000000", nil
}

func (p *countProposals) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n
}

// sinkEvents returns a copy of the recorded events.
func sinkEvents(s *recordingSink) ([]events.Type, []any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.Type(nil), s.types...), append([]any(nil), s.pays...)
}

// stepIDs returns the step.start ids in the order they were emitted.
func stepIDs(s *recordingSink) []string {
	types, pays := sinkEvents(s)
	ids := []string{}
	for i, typ := range types {
		if typ != events.AgentStepStart {
			continue
		}
		if p, ok := pays[i].(events.AgentStepStartPayload); ok {
			ids = append(ids, p.StepID)
		}
	}
	return ids
}

// messages returns every agent.message text in order.
func messages(s *recordingSink) []string {
	types, pays := sinkEvents(s)
	out := []string{}
	for i, typ := range types {
		if typ != events.AgentMessage {
			continue
		}
		if p, ok := pays[i].(events.AgentMessagePayload); ok {
			out = append(out, p.Text)
		}
	}
	return out
}

// waitFor polls cond until it is true or the timeout passes.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// startRun starts Controls.Run for roomID in the background.
func startRun(t *testing.T, c *Controls, roomID string, sink *recordingSink, props *countProposals) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx, agent.Alert{RoomID: roomID}, agent.Deps{
			Sink: sink, Proposals: props, Inbox: newBlockingInbox(),
		}, agent.Options{})
	}()
	return cancel, done
}

// stopRun cancels the run and fails the test if it did not end.
func stopRun(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil after cancel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not end after cancel")
	}
}

// TC2: a skip at step 2 starts step 3 without waiting for stepDelay, and the
// events stay in order.
func TestSkipAdvancesToTheNextStep(t *testing.T) {
	c := NewControls(nil)
	c.delay = 150 * time.Millisecond
	sink := &recordingSink{}
	cancel, done := startRun(t, c, "r_skip", sink, &countProposals{})
	defer stopRun(t, cancel, done)

	waitFor(t, 5*time.Second, "step 2 to start", func() bool { return len(stepIDs(sink)) >= 2 })
	if got := stepIDs(sink); got[0] != "s_1" || got[1] != "s_2" {
		t.Fatalf("steps out of order before skip: %v", got)
	}
	current := len(stepIDs(sink))
	if current >= len(story) {
		t.Fatalf("story had already played out (%d steps) before the skip", current)
	}
	start := time.Now()
	if err := c.Skip("r_skip"); err != nil {
		t.Fatalf("Skip: %v", err)
	}
	want := story[current].id
	waitFor(t, 2*time.Second, "the next step to start", func() bool { return len(stepIDs(sink)) > current })
	if d := time.Since(start); d >= c.delay {
		t.Fatalf("next step started %v after skip; want under %v (no stepDelay)", d, c.delay)
	}
	if got := stepIDs(sink)[current]; got != want {
		t.Fatalf("step after skip = %q, want %q", got, want)
	}

	// The skip event was appended before the step it announced.
	types, pays := sinkEvents(sink)
	msgIdx, stepIdx := -1, -1
	wantMsg := fmt.Sprintf("demo: skipped to step %d", current+1)
	for i := range types {
		if types[i] == events.AgentMessage {
			if p, ok := pays[i].(events.AgentMessagePayload); ok && p.Text == wantMsg {
				msgIdx = i
			}
		}
		if types[i] == events.AgentStepStart {
			if p, ok := pays[i].(events.AgentStepStartPayload); ok && p.StepID == want {
				stepIdx = i
			}
		}
	}
	if msgIdx < 0 || stepIdx < 0 || msgIdx > stepIdx {
		t.Fatalf("skip message at %d, step start at %d; want the message first", msgIdx, stepIdx)
	}
}

// TC3: a pause between steps holds the story; resume continues it.
func TestPauseHoldsUntilResume(t *testing.T) {
	c := NewControls(nil)
	c.delay = 20 * time.Millisecond
	sink := &recordingSink{}
	props := &countProposals{}
	cancel, done := startRun(t, c, "r_pause", sink, props)
	defer stopRun(t, cancel, done)

	waitFor(t, 5*time.Second, "step 2 to start", func() bool { return len(stepIDs(sink)) >= 2 })
	if err := c.Pause("r_pause"); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	// The step in flight finishes, then the story holds before step 3.
	waitFor(t, 5*time.Second, "step 2 to finish", func() bool {
		types, _ := sinkEvents(sink)
		results := 0
		for _, typ := range types {
			if typ == events.AgentToolResult {
				results++
			}
		}
		return results >= 2
	})
	held := len(stepIDs(sink))
	time.Sleep(10 * c.delay)
	if got := len(stepIDs(sink)); got != held {
		t.Fatalf("paused story advanced from %d to %d steps", held, got)
	}
	if !hasMessage(sink, "demo: paused") {
		t.Fatalf("no pause event in the log: %v", messages(sink))
	}
	if err := c.Resume("r_pause"); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	waitFor(t, 5*time.Second, "step 3 to start after resume", func() bool { return len(stepIDs(sink)) > held })
	if got := stepIDs(sink)[held]; got != story[held].id {
		t.Fatalf("step after resume = %q, want %q", got, story[held].id)
	}
	if !hasMessage(sink, "demo: resumed") {
		t.Fatalf("no resume event in the log: %v", messages(sink))
	}
	// The remaining steps played exactly once, in order.
	waitFor(t, 10*time.Second, "the story to finish", func() bool { return props.count() >= 1 })
	ids := stepIDs(sink)
	if len(ids) != len(story) {
		t.Fatalf("step starts = %v, want one each of %d steps", ids, len(story))
	}
	for i := range story {
		if ids[i] != story[i].id {
			t.Fatalf("step %d = %q, want %q", i, ids[i], story[i].id)
		}
	}
}

func hasMessage(s *recordingSink, prefix string) bool {
	for _, m := range messages(s) {
		if strings.HasPrefix(m, prefix) {
			return true
		}
	}
	return false
}

// TC4: a reset after the fix executed restores the fake state and replays the
// story from step 1.
func TestResetRestoresFakesAndReplays(t *testing.T) {
	fakes := NewFakes()
	c := NewControls(fakes)
	c.delay = 5 * time.Millisecond
	sink := &recordingSink{}
	props := &countProposals{}
	cancel, done := startRun(t, c, "r_reset", sink, props)
	defer stopRun(t, cancel, done)

	waitFor(t, 10*time.Second, "the first proposal", func() bool { return props.count() >= 1 })
	ctx := room.WithRoomID(context.Background(), "r_reset")
	// The fix ran: compatibility NONE, task RUNNING.
	if err := fakes.Registry.SetCompatibility(ctx, subjectName, "NONE"); err != nil {
		t.Fatalf("set compatibility: %v", err)
	}
	if err := fakes.Connect.RestartTask(ctx, connectorName, 0); err != nil {
		t.Fatalf("restart task: %v", err)
	}
	if err := c.Reset("r_reset"); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	waitFor(t, 10*time.Second, "the replayed proposal", func() bool { return props.count() >= 2 })
	if !hasMessage(sink, "demo: reset") {
		t.Fatalf("no reset event in the log: %v", messages(sink))
	}
	// The story starts over at step 1.
	waitFor(t, 5*time.Second, "step 1 to replay", func() bool {
		ids := stepIDs(sink)
		first := 0
		for _, id := range ids {
			if id == "s_1" {
				first++
			}
		}
		return first >= 2
	})

	st, err := fakes.Connect.Status(ctx, connectorName)
	if err != nil {
		t.Fatalf("connector status: %v", err)
	}
	if len(st.Tasks) != 1 || st.Tasks[0].State != "FAILED" {
		t.Fatalf("connector task after reset = %+v, want FAILED", st.Tasks)
	}
	level, subjectLevel, err := fakes.Registry.Compatibility(ctx, subjectName)
	if err != nil {
		t.Fatalf("compatibility: %v", err)
	}
	if !subjectLevel || level != "BACKWARD" {
		t.Fatalf("compatibility after reset = %q (subject %v), want BACKWARD", level, subjectLevel)
	}
}

// A reset in one room only restores that room's fake state. Another room that
// already ran its fix keeps NONE/RUNNING (KAN-392 + #785 review).
func TestResetIsPerRoom(t *testing.T) {
	fakes := NewFakes()
	c := NewControls(fakes)
	c.delay = 5 * time.Millisecond
	sinkA, sinkB := &recordingSink{}, &recordingSink{}
	cancelA, doneA := startRun(t, c, "r_a", sinkA, &countProposals{})
	defer stopRun(t, cancelA, doneA)
	cancelB, doneB := startRun(t, c, "r_b", sinkB, &countProposals{})
	defer stopRun(t, cancelB, doneB)

	waitFor(t, 10*time.Second, "both rooms at the proposal", func() bool {
		return hasMessage(sinkA, "The fix proposal") && hasMessage(sinkB, "The fix proposal")
	})
	ctxA := room.WithRoomID(context.Background(), "r_a")
	ctxB := room.WithRoomID(context.Background(), "r_b")
	for _, ctx := range []context.Context{ctxA, ctxB} {
		if err := fakes.Registry.SetCompatibility(ctx, subjectName, "NONE"); err != nil {
			t.Fatalf("set compatibility: %v", err)
		}
		if err := fakes.Connect.RestartTask(ctx, connectorName, 0); err != nil {
			t.Fatalf("restart task: %v", err)
		}
	}

	if err := c.Reset("r_a"); err != nil {
		t.Fatalf("Reset A: %v", err)
	}
	waitFor(t, 10*time.Second, "room A back to the starting state", func() bool {
		st, err := fakes.Connect.Status(ctxA, connectorName)
		if err != nil || len(st.Tasks) != 1 || st.Tasks[0].State != "FAILED" {
			return false
		}
		level, ok, err := fakes.Registry.Compatibility(ctxA, subjectName)
		return err == nil && ok && level == "BACKWARD"
	})

	stB, err := fakes.Connect.Status(ctxB, connectorName)
	if err != nil {
		t.Fatalf("connector B status: %v", err)
	}
	if len(stB.Tasks) != 1 || stB.Tasks[0].State != "RUNNING" {
		t.Fatalf("room B connector after A's reset = %+v, want RUNNING", stB.Tasks)
	}
	levelB, ok, err := fakes.Registry.Compatibility(ctxB, subjectName)
	if err != nil {
		t.Fatalf("compatibility B: %v", err)
	}
	if !ok || levelB != "NONE" {
		t.Fatalf("room B compatibility after A's reset = %q (subject %v), want NONE", levelB, ok)
	}
}

// TC4 (preview half): the action preview over the demo's clients shows the
// starting state again after a reset, which is what a replayed proposal's
// preview card shows.
func TestFakesResetRestoresActionPreview(t *testing.T) {
	fakes := NewFakes()
	reg := actions.NewRegistry()
	if err := actions.Register(reg, fakes.Actions()); err != nil {
		t.Fatal(err)
	}
	act, ok := reg.Get(ActionSchemaSetCompatibility)
	if !ok {
		t.Fatalf("%s not registered", ActionSchemaSetCompatibility)
	}
	params := []byte(`{"subject":"pg.public.orders-value","level":"NONE"}`)
	preview, ok := act.(actions.Preview)
	if !ok {
		t.Fatalf("%s has no Preview", ActionSchemaSetCompatibility)
	}
	ctx := room.WithRoomID(context.Background(), "r_preview")
	if _, err := act.Execute(ctx, params); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	after, err := preview.Preview(ctx, params)
	if err != nil {
		t.Fatalf("Preview after the fix: %v", err)
	}
	if !strings.Contains(after.Current, "NONE") {
		t.Fatalf("preview after the fix = %q, want the written level", after.Current)
	}
	fakes.Reset("r_preview")
	again, err := preview.Preview(ctx, params)
	if err != nil {
		t.Fatalf("Preview after reset: %v", err)
	}
	if !strings.Contains(again.Current, "BACKWARD") {
		t.Fatalf("preview after reset = %q, want the starting level", again.Current)
	}
}

// TC7: a pause in one room does not affect another room's run.
func TestPauseIsPerRoom(t *testing.T) {
	c := NewControls(nil)
	c.delay = 20 * time.Millisecond
	sinkA, sinkB := &recordingSink{}, &recordingSink{}
	cancelA, doneA := startRun(t, c, "r_a", sinkA, &countProposals{})
	defer stopRun(t, cancelA, doneA)
	cancelB, doneB := startRun(t, c, "r_b", sinkB, &countProposals{})
	defer stopRun(t, cancelB, doneB)

	waitFor(t, 5*time.Second, "both rooms at step 2", func() bool {
		return len(stepIDs(sinkA)) >= 2 && len(stepIDs(sinkB)) >= 2
	})
	if err := c.Pause("r_a"); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	waitFor(t, 5*time.Second, "room A to finish step 2", func() bool {
		types, _ := sinkEvents(sinkA)
		results := 0
		for _, typ := range types {
			if typ == events.AgentToolResult {
				results++
			}
		}
		return results >= 2
	})
	heldA := len(stepIDs(sinkA))
	seenB := len(stepIDs(sinkB))
	waitFor(t, 5*time.Second, "room B to move on", func() bool { return len(stepIDs(sinkB)) > seenB })
	time.Sleep(5 * c.delay)
	if got := len(stepIDs(sinkA)); got != heldA {
		t.Fatalf("room A advanced from %d to %d steps while paused", heldA, got)
	}
	if err := c.Resume("r_a"); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	waitFor(t, 5*time.Second, "room A to resume", func() bool { return len(stepIDs(sinkA)) > heldA })
}

// Controls answer demo_idle for a room with no run, and demo_finished when the
// story is over and only the idle wait is left.
func TestControlsReportIdleAndFinished(t *testing.T) {
	c := NewControls(nil)
	c.delay = 5 * time.Millisecond
	for _, tc := range []struct {
		name string
		call func(string) error
	}{
		{"skip", c.Skip}, {"pause", c.Pause}, {"resume", c.Resume}, {"reset", c.Reset},
	} {
		t.Run(tc.name+" idle", func(t *testing.T) {
			err := tc.call("r_none")
			var re *room.Error
			if !errors.As(err, &re) || re.Code != "demo_idle" || re.Status != 409 {
				t.Fatalf("err = %v, want 409 demo_idle", err)
			}
		})
	}

	sink := &recordingSink{}
	props := &countProposals{}
	cancel, done := startRun(t, c, "r_finished", sink, props)
	defer stopRun(t, cancel, done)
	waitFor(t, 10*time.Second, "the proposal", func() bool { return props.count() >= 1 })
	waitFor(t, 2*time.Second, "the idle wait", func() bool {
		types, pays := sinkEvents(sink)
		for i, typ := range types {
			if typ != events.AgentStatus {
				continue
			}
			if p, ok := pays[i].(events.AgentStatusPayload); ok && p.State == "waiting_approval" {
				return true
			}
		}
		return false
	})

	for _, tc := range []struct {
		name string
		call func(string) error
	}{
		{"skip", c.Skip}, {"pause", c.Pause}, {"resume", c.Resume},
	} {
		t.Run(tc.name+" finished", func(t *testing.T) {
			err := tc.call("r_finished")
			var re *room.Error
			if !errors.As(err, &re) || re.Code != "demo_finished" || re.Status != 409 {
				t.Fatalf("err = %v, want 409 demo_finished", err)
			}
		})
	}
	// Reset is the way out of a finished story.
	if err := c.Reset("r_finished"); err != nil {
		t.Fatalf("Reset after finish: %v", err)
	}
	waitFor(t, 5*time.Second, "the replay to start", func() bool {
		types, _ := sinkEvents(sink)
		statuses := 0
		for _, typ := range types {
			if typ == events.AgentStatus {
				statuses++
			}
		}
		return statuses >= 2
	})
}

// The package-level Run keeps working without controls.
func TestRunWithoutControls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sink := &recordingSink{}
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, agent.Alert{}, agent.Deps{Sink: sink, Proposals: &countProposals{}, Inbox: newBlockingInbox()}, agent.Options{})
	}()
	waitFor(t, 10*time.Second, "the proposal", func() bool { return hasMessage(sink, "The fix proposal is waiting") })
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not end")
	}
}
