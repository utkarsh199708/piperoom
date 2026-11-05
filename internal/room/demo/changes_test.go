package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/tools/changes"
)

// storyNow is the fixed clock of the fixture: the demo's what_changed result
// is recorded relative to the room opening, so tests pin the times.
var storyNow = time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)

// compactJSON normalizes a RawMessage so the comparison ignores the fixture's
// formatting (a JSON editor may re-indent it) and compares the documents.
func compactJSON(b json.RawMessage) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		return b
	}
	return buf.Bytes()
}

func normalizeChanges(d changes.Data) changes.Data {
	for i := range d.Changes {
		d.Changes[i].Before = compactJSON(d.Changes[i].Before)
		d.Changes[i].After = compactJSON(d.Changes[i].After)
	}
	return d
}

// The demo's what_changed result must be the exact shape the real tool
// returns, not a hand-made UI object. testdata/what_changed.json is parsed by
// web/src/lib/f8.test.ts too, so one fixture keeps both sides honest (KAN-399
// TC4).
func TestStoryChangesMatchFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/what_changed.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var want changes.Data
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatalf("fixture is not a changes.Data: %v", err)
	}
	got := storyChanges(storyNow, "orders")
	if !reflect.DeepEqual(normalizeChanges(got), normalizeChanges(want)) {
		t.Fatalf("storyChanges changed:\n got %+v\nwant %+v", got, want)
	}
}

// The lane's story: at least the schema registration and the connector config
// change, with the ALTER as the newest suspect and oldest last.
func TestStoryChangesAreNewestFirstAndCoverTheStory(t *testing.T) {
	got := storyChanges(storyNow, "orders")
	wantKinds := []string{clients.ChangeDDL, clients.ChangeConnectorConfig, clients.ChangeSchemaVersion}
	if len(got.Changes) != len(wantKinds) {
		t.Fatalf("changes = %d, want %d", len(got.Changes), len(wantKinds))
	}
	for i, c := range got.Changes {
		if c.Kind != wantKinds[i] {
			t.Errorf("change %d kind = %q, want %q", i, c.Kind, wantKinds[i])
		}
		if c.PipelineID != "orders" {
			t.Errorf("change %d pipeline = %q, want orders", i, c.PipelineID)
		}
		if c.At.After(storyNow) {
			t.Errorf("change %d at %s is after the room opened", i, c.At)
		}
		if i > 0 && !got.Changes[i-1].At.After(c.At) {
			t.Errorf("change %d (%s) is not older than change %d (%s)", i, c.At, i-1, got.Changes[i-1].At)
		}
	}
	if got.Window != whatChangedWindow || got.Omitted != 0 {
		t.Fatalf("window = %q omitted = %d, want %q and 0", got.Window, got.Omitted, whatChangedWindow)
	}
	// A room without a pipeline still tells the orders story.
	if pipe := storyChanges(storyNow, "").Changes[0].PipelineID; pipe != "orders" {
		t.Fatalf("fallback pipeline = %q, want orders", pipe)
	}
}

// The sixth step is emitted as a normal tool result whose Data the web lane
// parses; pin the tool name, input window and payload so it cannot regress.
// s_5 (pg_recent_ddl) and s_6 (what_changed) describe the same ALTER. Their
// timestamps must agree, and both must be relative to the room opening.
func TestDDLLogStepMatchesWhatChanged(t *testing.T) {
	steps := storySteps(storyNow, "orders")
	var ddl *step
	for i := range steps {
		if steps[i].id == "s_5" {
			ddl = &steps[i]
		}
	}
	if ddl == nil {
		t.Fatal("no s_5 step")
	}
	want := storyChanges(storyNow, "orders").Changes[0]
	wantAt := want.At.Format(time.RFC3339)
	data, ok := ddl.data.(map[string]any)
	if !ok {
		t.Fatalf("s_5 data type %T", ddl.data)
	}
	if data["at"] != wantAt {
		t.Fatalf("s_5 at = %v, want %s (what_changed's newest row)", data["at"], wantAt)
	}
	if !strings.HasPrefix(ddl.summary, wantAt+" ALTER TABLE public.orders: ") {
		t.Fatalf("s_5 summary = %q, want it to start with %q", ddl.summary, wantAt)
	}
	if sql, _ := data["sql"].(string); !strings.Contains(want.Summary, sql) {
		t.Fatalf("s_5 sql %q does not match what_changed summary %q", sql, want.Summary)
	}
	if want.At.After(storyNow) || storyNow.Sub(want.At) != ddlAgo {
		t.Fatalf("DDL at %s is not %s before the room opened (%s)", want.At, ddlAgo, storyNow)
	}
}

func TestRunEmitsWhatChangedStep(t *testing.T) {
	sink := &recordingSink{}
	err := runStory(context.Background(), agent.Alert{RoomID: "r", PipelineID: "orders"},
		agent.Deps{Sink: sink, Inbox: &closedInbox{}}, agent.Options{}, storyNow, roomTestDelay)
	if err != nil {
		t.Fatalf("runStory: %v", err)
	}

	var started bool
	var result *events.AgentToolResultPayload
	var input json.RawMessage
	for _, p := range sink.pays {
		switch v := p.(type) {
		case events.AgentStepStartPayload:
			if v.StepID == "s_6" && v.Title == "Check what changed before the alert" {
				started = true
			}
		case events.AgentToolCallPayload:
			if v.StepID == "s_6" {
				if v.Tool != changes.ToolName {
					t.Fatalf("tool = %q, want %q", v.Tool, changes.ToolName)
				}
				input = v.Input
			}
		case events.AgentToolResultPayload:
			if v.Tool == changes.ToolName {
				res := v
				result = &res
			}
		}
	}
	if !started {
		t.Fatal("no step start for s_6")
	}
	if result == nil {
		t.Fatal("no what_changed tool result emitted")
	}
	if result.StepID != "s_6" || result.IsError {
		t.Fatalf("result = %+v, want step s_6 without an error", result)
	}
	var got changes.Data
	if err := json.Unmarshal(result.Data, &got); err != nil {
		t.Fatalf("result data is not changes.Data: %v", err)
	}
	if want := storyChanges(storyNow, "orders"); !reflect.DeepEqual(got, want) {
		t.Fatalf("emitted changes:\n got %+v\nwant %+v", got, want)
	}
	var in struct {
		Window string `json:"window"`
	}
	if err := json.Unmarshal(input, &in); err != nil || in.Window != whatChangedWindow {
		t.Fatalf("tool input = %s, want window %q", input, whatChangedWindow)
	}
}
