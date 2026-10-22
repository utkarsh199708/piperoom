package demo

// KAN-392: one actions registry is built once at startup, so the fakes behind
// it are process-wide. These tests pin the fix: their mutable state is keyed by
// the room id the room service carries in the action context, and every room
// starts the schema-change story from its first call.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/room"
)

const (
	fixCompatParams  = `{"subject":"pg.public.orders-value","level":"NONE"}`
	fixRestartParams = `{"connector":"pg-orders-connector","task":0}`
)

// demoFix wires the real write actions over one pair of keyed fakes, exactly
// as cmd/server does once for every demo room.
type demoFix struct {
	conn *FakeConnect
	reg  *FakeRegistry
	acts *actions.Registry
}

func newDemoFix(t *testing.T) *demoFix {
	t.Helper()
	conn, reg := NewFakeConnect(), NewFakeRegistry()
	acts := actions.NewRegistry()
	if err := actions.Register(acts, actions.Deps{Connect: conn, Registry: reg}); err != nil {
		t.Fatalf("register real actions: %v", err)
	}
	return &demoFix{conn: conn, reg: reg, acts: acts}
}

// exec runs one action the way the executor does: Validate then Execute.
func (d *demoFix) exec(ctx context.Context, action, params string) error {
	a, ok := d.acts.Get(action)
	if !ok {
		return fmt.Errorf("action %q is not registered", action)
	}
	if err := a.Validate(ctx, json.RawMessage(params)); err != nil {
		return fmt.Errorf("validate %s: %w", action, err)
	}
	if _, err := a.Execute(ctx, json.RawMessage(params)); err != nil {
		return fmt.Errorf("execute %s: %w", action, err)
	}
	return nil
}

func (d *demoFix) preview(ctx context.Context, action, params string) (actions.PreviewResult, error) {
	a, ok := d.acts.Get(action)
	if !ok {
		return actions.PreviewResult{}, fmt.Errorf("action %q is not registered", action)
	}
	pv, ok := a.(actions.Preview)
	if !ok {
		return actions.PreviewResult{}, fmt.Errorf("action %q has no Preview", action)
	}
	return pv.Preview(ctx, json.RawMessage(params))
}

// wantStory asserts the room in ctx still reports the scripted story: BACKWARD
// compatibility and connector task 0 FAILED, matching demo.Run's summaries.
func (d *demoFix) wantStory(t *testing.T, ctx context.Context, label string) {
	t.Helper()
	compat, err := d.preview(ctx, ActionSchemaSetCompatibility, fixCompatParams)
	if err != nil {
		t.Fatalf("%s: schema preview: %v", label, err)
	}
	if want := storyCompatibility + " (GET /config/" + subjectName + ")"; compat.Current != want {
		t.Fatalf("%s: schema preview current = %q, want %q", label, compat.Current, want)
	}
	task, err := d.preview(ctx, ActionConnectRestartTask, fixRestartParams)
	if err != nil {
		t.Fatalf("%s: connector preview: %v", label, err)
	}
	if !strings.Contains(task.Current, storyTaskState) {
		t.Fatalf("%s: connector preview current = %q, want it to report %s", label, task.Current, storyTaskState)
	}
}

// wantFixed asserts the room in ctx reports the post-fix state.
func (d *demoFix) wantFixed(t *testing.T, ctx context.Context, label string) {
	t.Helper()
	compat, err := d.preview(ctx, ActionSchemaSetCompatibility, fixCompatParams)
	if err != nil {
		t.Fatalf("%s: schema preview: %v", label, err)
	}
	if !strings.HasPrefix(compat.Current, "NONE ") {
		t.Fatalf("%s: schema preview current = %q, want NONE", label, compat.Current)
	}
	task, err := d.preview(ctx, ActionConnectRestartTask, fixRestartParams)
	if err != nil {
		t.Fatalf("%s: connector preview: %v", label, err)
	}
	if !strings.Contains(task.Current, "RUNNING") {
		t.Fatalf("%s: connector preview current = %q, want RUNNING", label, task.Current)
	}
}

func (d *demoFix) runScriptedFix(t *testing.T, ctx context.Context, label string) {
	t.Helper()
	if err := d.exec(ctx, ActionSchemaSetCompatibility, fixCompatParams); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if err := d.exec(ctx, ActionConnectRestartTask, fixRestartParams); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
}

// TC1: executing the scripted fix in room A must not change room B, opened
// later in the same process; B's preview still tells the story.
func TestDemoRoomsReplayStoryIndependently(t *testing.T) {
	d := newDemoFix(t)
	roomA := room.WithRoomID(context.Background(), "r_demo_a")
	roomB := room.WithRoomID(context.Background(), "r_demo_b")

	d.wantStory(t, roomA, "room A before the fix")
	d.wantStory(t, roomB, "room B before the fix")

	d.runScriptedFix(t, roomA, "room A fix")
	d.wantFixed(t, roomA, "room A after the fix")

	// Room B is opened after A's fix ran: it must still show BACKWARD/FAILED.
	d.wantStory(t, roomB, "room B opened after room A's fix")

	// Running the fix in B leaves A's fixed state alone, and a room opened
	// after both still starts from the story.
	d.runScriptedFix(t, roomB, "room B fix")
	d.wantFixed(t, roomB, "room B after the fix")
	d.wantFixed(t, roomA, "room A after room B's fix")
	d.wantStory(t, room.WithRoomID(context.Background(), "r_demo_c"), "room C after both fixes")
}

// Isolated state must not bleed in either direction: an asymmetric fix in each
// room leaves the other room's half of the story untouched.
func TestDemoFakeStateDoesNotBleedBetweenRooms(t *testing.T) {
	d := newDemoFix(t)
	roomA := room.WithRoomID(context.Background(), "r_demo_a")
	roomB := room.WithRoomID(context.Background(), "r_demo_b")

	if err := d.exec(roomA, ActionSchemaSetCompatibility, fixCompatParams); err != nil {
		t.Fatalf("room A schema fix: %v", err)
	}
	if err := d.exec(roomB, ActionConnectRestartTask, fixRestartParams); err != nil {
		t.Fatalf("room B connector fix: %v", err)
	}

	// A changed the schema only; B must still see BACKWARD and A must see NONE.
	if err := d.exec(roomB, ActionSchemaSetCompatibility, fixCompatParams); err != nil {
		t.Fatalf("room B schema fix: %v", err)
	}
	compatA, err := d.preview(roomA, ActionSchemaSetCompatibility, fixCompatParams)
	if err != nil {
		t.Fatalf("room A schema preview: %v", err)
	}
	if !strings.HasPrefix(compatA.Current, "NONE ") {
		t.Fatalf("room A schema preview = %q, want NONE", compatA.Current)
	}
	taskA, err := d.preview(roomA, ActionConnectRestartTask, fixRestartParams)
	if err != nil {
		t.Fatalf("room A connector preview: %v", err)
	}
	if !strings.Contains(taskA.Current, storyTaskState) {
		t.Fatalf("room A connector preview = %q, want %s (room B's restart must not reach it)", taskA.Current, storyTaskState)
	}

	// A room with no id keeps its own bucket, as single-room tests and callers
	// without the context helper rely on.
	d.wantStory(t, context.Background(), "room-less context")
}

// TC2: two demo rooms running the fix at the same time each end consistent.
func TestDemoRoomsRunScriptedFixConcurrently(t *testing.T) {
	d := newDemoFix(t)
	const rooms = 4

	var wg sync.WaitGroup
	errs := make(chan error, rooms)
	for i := 0; i < rooms; i++ {
		id := fmt.Sprintf("r_demo_%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := room.WithRoomID(context.Background(), id)
			if err := d.exec(ctx, ActionSchemaSetCompatibility, fixCompatParams); err != nil {
				errs <- fmt.Errorf("%s: %w", id, err)
				return
			}
			if err := d.exec(ctx, ActionConnectRestartTask, fixRestartParams); err != nil {
				errs <- fmt.Errorf("%s: %w", id, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent fix: %v", err)
	}

	for i := 0; i < rooms; i++ {
		d.wantFixed(t, room.WithRoomID(context.Background(), fmt.Sprintf("r_demo_%d", i)), fmt.Sprintf("room %d", i))
	}
	d.wantStory(t, room.WithRoomID(context.Background(), "r_demo_fresh"), "room opened after the concurrent fixes")
}

// TC3: resetting one room returns it to the story and leaves other rooms
// alone. KAN-398's demo reset reuses FakeConnect.Reset/FakeRegistry.Reset.
func TestDemoFakeResetReturnsRoomToStory(t *testing.T) {
	d := newDemoFix(t)
	roomA := room.WithRoomID(context.Background(), "r_demo_a")
	roomB := room.WithRoomID(context.Background(), "r_demo_b")

	d.runScriptedFix(t, roomA, "room A fix")
	if err := d.exec(roomB, ActionSchemaSetCompatibility, fixCompatParams); err != nil {
		t.Fatalf("room B schema fix: %v", err)
	}

	d.conn.Reset("r_demo_a")
	d.reg.Reset("r_demo_a")
	d.wantStory(t, roomA, "room A after reset")

	// Room B's half-finished state survives A's reset.
	compatB, err := d.preview(roomB, ActionSchemaSetCompatibility, fixCompatParams)
	if err != nil {
		t.Fatalf("room B schema preview: %v", err)
	}
	if !strings.HasPrefix(compatB.Current, "NONE ") {
		t.Fatalf("room B schema preview = %q, want NONE", compatB.Current)
	}
}

// stopAfter cancels the run once the wanted tool result is emitted, keeping
// the summary test short while still reading it from the scripted agent.
type stopAfter struct {
	sink   *recordingSink
	tool   string
	cancel context.CancelFunc
	once   sync.Once
}

func (s *stopAfter) Emit(ctx context.Context, typ events.Type, payload any) error {
	if err := s.sink.Emit(ctx, typ, payload); err != nil {
		return err
	}
	if p, ok := payload.(events.AgentToolResultPayload); ok && p.Tool == s.tool {
		s.once.Do(s.cancel)
	}
	return nil
}

// TC4: the values in the scripted connect_status and schema_registry_versions
// summaries are the values a fresh room's action previews report.
func TestScriptedSummariesMatchFreshRoomPreview(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := &recordingSink{}
	sink := &stopAfter{sink: rec, tool: "schema_registry_versions", cancel: cancel}

	if err := Run(ctx, agent.Alert{}, agent.Deps{Sink: sink, Proposals: idProposals{}, Inbox: &closedInbox{}}, agent.Options{}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	rec.mu.Lock()
	summaries := map[string]string{}
	for i, typ := range rec.types {
		if typ != events.AgentToolResult {
			continue
		}
		if p, ok := rec.pays[i].(events.AgentToolResultPayload); ok {
			summaries[p.Tool] = p.Summary
		}
	}
	rec.mu.Unlock()

	connectSummary, ok := summaries["connect_status"]
	if !ok {
		t.Fatalf("the scripted run emitted no connect_status result: %v", summaries)
	}
	schemaSummary, ok := summaries["schema_registry_versions"]
	if !ok {
		t.Fatalf("the scripted run emitted no schema_registry_versions result: %v", summaries)
	}

	d := newDemoFix(t)
	fresh := room.WithRoomID(context.Background(), "r_demo_fresh")
	compat, err := d.preview(fresh, ActionSchemaSetCompatibility, fixCompatParams)
	if err != nil {
		t.Fatalf("schema preview: %v", err)
	}
	task, err := d.preview(fresh, ActionConnectRestartTask, fixRestartParams)
	if err != nil {
		t.Fatalf("connector preview: %v", err)
	}

	if want := "compatibility " + storyCompatibility; !strings.Contains(schemaSummary, want) {
		t.Fatalf("schema_registry_versions summary %q does not report %q", schemaSummary, want)
	}
	if want := storyCompatibility; !strings.HasPrefix(compat.Current, want) {
		t.Fatalf("schema preview %q does not start with the story's %q", compat.Current, want)
	}
	if want := "0=" + storyTaskState; !strings.Contains(connectSummary, want) {
		t.Fatalf("connect_status summary %q does not report %q", connectSummary, want)
	}
	if !strings.Contains(task.Current, storyTaskState) {
		t.Fatalf("connector preview %q does not report %q", task.Current, storyTaskState)
	}
}

// The story values the fakes start from are the ones the preview assertions
// above pin; keeping them named stops a silent change to one side only.
func TestStoryStartValues(t *testing.T) {
	if storyTaskState != "FAILED" || storyCompatibility != "BACKWARD" {
		t.Fatalf("story starts at %s/%s, want FAILED/BACKWARD", storyTaskState, storyCompatibility)
	}
}
