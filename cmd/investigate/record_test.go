package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

type memSink struct{ got []events.Type }

func (m *memSink) Emit(_ context.Context, typ events.Type, _ any) error {
	m.got = append(m.got, typ)
	return nil
}

func fixedClock() func() time.Time {
	t := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	return func() time.Time { t = t.Add(time.Second); return t }
}

func readLines(t *testing.T, path string) []json.RawMessage {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []json.RawMessage
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		out = append(out, json.RawMessage(append([]byte(nil), sc.Bytes()...)))
	}
	return out
}

func TestRecorderFormat(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run1")
	next := &memSink{}
	rec, err := newRecorder(dir, next, recordMeta{
		Pipeline: "orders", Title: "orders stale", Model: "claude-opus-5-5", Effort: "high",
		MaxTurns: 40, PromptHash: agent.PromptHash(), Commit: "abc123", FakeTools: true,
	}, fixedClock())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const dsn = "postgres://svc:hunter2hunter2@db.internal:5432/orders"
	emits := []struct {
		typ events.Type
		p   any
	}{
		{events.AgentStatus, events.AgentStatusPayload{State: "working"}},
		{events.AgentStepStart, events.AgentStepStartPayload{StepID: "s_1", Title: "check connector"}},
		{events.AgentToolCall, events.AgentToolCallPayload{StepID: "s_1", ToolUseID: "tu_1", Tool: "pg_row", Input: json.RawMessage(`{"table":"public.orders","key":"7"}`)}},
		{events.AgentToolResult, events.AgentToolResultPayload{
			StepID: "s_1", ToolUseID: "tu_1", Tool: "pg_row", TookMS: 12,
			Summary: "row 7 found; connect with " + dsn,
			Data:    json.RawMessage(`{"row":{"id":7,"email":"jane@example.com","card":"4111111111111111"}}`),
		}},
	}
	for _, e := range emits {
		if err := rec.Emit(ctx, e.typ, e.p); err != nil {
			t.Fatal(err)
		}
	}
	rec.recordInput(agent.HumanInput{Kind: "steer", Text: "also check order_items password=hunter2hunter2"})
	if err := rec.Close(errors.New("model said no")); err != nil {
		t.Fatal(err)
	}

	if len(next.got) != len(emits) {
		t.Errorf("forwarded %d events, want %d", len(next.got), len(emits))
	}
	lines := readLines(t, filepath.Join(dir, "events.jsonl"))
	if len(lines) != len(emits) {
		t.Fatalf("events.jsonl has %d lines, want %d", len(lines), len(emits))
	}
	for i, l := range lines {
		var ev events.Event
		if err := json.Unmarshal(l, &ev); err != nil {
			t.Fatalf("line %d is not an events.Event: %v", i, err)
		}
		if ev.Seq != int64(i+1) || ev.Type != emits[i].typ || ev.At.IsZero() || !strings.HasPrefix(ev.RoomID, "rec_") {
			t.Errorf("line %d = seq %d type %s at %v room %q", i, ev.Seq, ev.Type, ev.At, ev.RoomID)
		}
	}
	var call events.AgentToolCallPayload
	var ev events.Event
	_ = json.Unmarshal(lines[2], &ev)
	if err := json.Unmarshal(ev.Payload, &call); err != nil || !jsonEqual(call.Input, `{"table":"public.orders","key":"7"}`) {
		t.Errorf("tool_call input = %s (%v)", call.Input, err)
	}
	var res events.AgentToolResultPayload
	_ = json.Unmarshal(lines[3], &ev)
	if err := json.Unmarshal(ev.Payload, &res); err != nil {
		t.Fatal(err)
	}
	if res.TookMS != 12 || res.Tool != "pg_row" || !strings.Contains(res.Summary, "row 7 found") {
		t.Errorf("tool_result = %+v", res)
	}
	var data map[string]any
	if err := json.Unmarshal(res.Data, &data); err != nil || data["not_recorded"] != true || data["sha256"] == "" {
		t.Errorf("tool_result data = %s, want a not_recorded stub", res.Data)
	}

	raw, _ := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	in, _ := os.ReadFile(filepath.Join(dir, "inputs.jsonl"))
	for _, leak := range []string{"hunter2", "jane@example.com", "4111111111111111"} {
		if strings.Contains(string(raw), leak) || strings.Contains(string(in), leak) {
			t.Errorf("recording contains %q", leak)
		}
	}

	var meta recordMeta
	b, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &meta); err != nil {
		t.Fatal(err)
	}
	want := recordMeta{Format: recordingFormat, Pipeline: "orders", Model: "claude-opus-5-5", Effort: "high", Commit: "abc123", FakeTools: true}
	if meta.Format != want.Format || meta.Pipeline != want.Pipeline || meta.Model != want.Model || meta.Effort != want.Effort ||
		meta.Commit != want.Commit || !meta.FakeTools {
		t.Errorf("meta = %+v", meta)
	}
	if len(meta.PromptHash) != 64 || meta.PromptHash != agent.PromptHash() {
		t.Errorf("prompt hash = %q", meta.PromptHash)
	}
	if !meta.Complete || meta.Events != 4 || meta.ToolCalls != 1 || meta.ToolResults != 1 || meta.Inputs != 1 {
		t.Errorf("counts/complete wrong: %+v", meta)
	}
	if meta.DurationMS <= 0 || meta.FinishedAt.Before(meta.StartedAt) {
		t.Errorf("timings: start %v end %v dur %d", meta.StartedAt, meta.FinishedAt, meta.DurationMS)
	}
	if meta.Error != "model said no" {
		t.Errorf("meta.Error = %q", meta.Error)
	}
}

func TestRecorderRefusesExistingRecording(t *testing.T) {
	dir := t.TempDir()
	r, err := newRecorder(dir, nil, recordMeta{}, fixedClock())
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Close(nil)
	if _, err := newRecorder(dir, nil, recordMeta{}, fixedClock()); err == nil {
		t.Fatal("second recorder in the same dir: want an error")
	}
}

func TestRecorderMetaIncompleteUntilClosed(t *testing.T) {
	dir := t.TempDir()
	r, err := newRecorder(dir, nil, recordMeta{Model: "m"}, fixedClock())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(nil)
	var meta recordMeta
	b, _ := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err := json.Unmarshal(b, &meta); err != nil || meta.Complete {
		t.Fatalf("meta before close = %s (%v), want complete=false", b, err)
	}
}

func TestRecorderWriteFailureIsReported(t *testing.T) {
	dir := t.TempDir()
	next := &memSink{}
	r, err := newRecorder(dir, next, recordMeta{}, fixedClock())
	if err != nil {
		t.Fatal(err)
	}
	r.evF.Close() // simulate a full disk / closed file
	if err := r.Emit(context.Background(), events.AgentStatus, events.AgentStatusPayload{State: "working"}); err == nil {
		t.Fatal("Emit after a failed write = nil, want error")
	}
	if len(next.got) != 0 {
		t.Error("event was forwarded although it was not recorded")
	}
	if err := r.Close(nil); err == nil {
		t.Fatal("Close = nil, want the write error")
	}
	var meta recordMeta
	b, _ := os.ReadFile(filepath.Join(dir, "meta.json"))
	_ = json.Unmarshal(b, &meta)
	if meta.Complete || meta.Error == "" {
		t.Errorf("meta = %+v, want complete=false with an error", meta)
	}
}

func TestRecordingInboxRecordsDrained(t *testing.T) {
	dir := t.TempDir()
	r, err := newRecorder(dir, nil, recordMeta{}, fixedClock())
	if err != nil {
		t.Fatal(err)
	}
	in := &stdinInbox{notify: make(chan struct{}, 1)}
	in.push(agent.HumanInput{Kind: "steer", Text: "look at order_items"})
	ri := recordingInbox{Inbox: in, rec: r}
	got, err := ri.Drain(context.Background())
	if err != nil || len(got) != 1 {
		t.Fatalf("Drain = %v, %v", got, err)
	}
	_ = r.Close(nil)
	lines := readLines(t, filepath.Join(dir, "inputs.jsonl"))
	if len(lines) != 1 || !strings.Contains(string(lines[0]), "look at order_items") {
		t.Errorf("inputs.jsonl = %s", lines)
	}
}

// jsonEqual compares JSON values, not bytes: redaction re-encodes objects, so
// key order is not preserved.
func jsonEqual(got json.RawMessage, want string) bool {
	var a, b any
	return json.Unmarshal(got, &a) == nil && json.Unmarshal([]byte(want), &b) == nil && reflect.DeepEqual(a, b)
}
