package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
		MaxTurns: 40, SystemPromptSHA256: agent.PromptHash(), Commit: "abc123", FakeTools: true,
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
	if len(meta.SystemPromptSHA256) != 64 || meta.SystemPromptSHA256 != agent.PromptHash() {
		t.Errorf("prompt hash = %q", meta.SystemPromptSHA256)
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
	// The terminal still gets the event: a dead recording must not blind the
	// operator while the agent keeps running.
	if len(next.got) != 1 {
		t.Errorf("forwarded %d events, want 1 even though recording failed", len(next.got))
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

// An event that cannot be encoded is still forwarded, the error comes back
// after the forward, and the recording is marked incomplete.
func TestRecorderForwardsWhenEncodingFails(t *testing.T) {
	dir := t.TempDir()
	next := &memSink{}
	r, err := newRecorder(dir, next, recordMeta{}, fixedClock())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Emit(context.Background(), events.AgentStatus, make(chan int)); err == nil {
		t.Fatal("Emit of an unencodable payload = nil, want error")
	}
	if len(next.got) != 1 {
		t.Fatalf("forwarded %d events, want 1", len(next.got))
	}
	if err := r.Close(nil); err == nil {
		t.Fatal("Close = nil, want the encode error")
	}
	var meta recordMeta
	b, _ := os.ReadFile(filepath.Join(dir, "meta.json"))
	_ = json.Unmarshal(b, &meta)
	if meta.Complete {
		t.Error("meta.complete = true after a failed event")
	}
}

// -title and the pipeline id are operator text and pass through redact too.
func TestRecorderRedactsMetaTitleAndPipeline(t *testing.T) {
	dir := t.TempDir()
	const pw = "hunter2hunter2"
	r, err := newRecorder(dir, nil, recordMeta{
		Title:    "debug postgres://svc:" + pw + "@db.internal:5432/orders",
		Pipeline: "orders password=" + pw,
	}, fixedClock())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(nil); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(dir, "meta.json"))
	if strings.Contains(string(b), pw) {
		t.Fatalf("meta.json holds the password:\n%s", b)
	}
}

// Big integers and out-of-range numbers survive recording as written, and the
// data stub hashes the original bytes.
func TestRecordPayloadKeepsNumbers(t *testing.T) {
	data := json.RawMessage(`{"offset":9007199254740993,"big":1e400}`)
	out, err := recordPayload(events.AgentToolResult, events.AgentToolResultPayload{
		Tool: "kafka_x", Summary: "offset 9007199254740993 of 1e400", Data: data, TookMS: 9007199254740993,
	})
	if err != nil {
		t.Fatalf("recordPayload: %v", err)
	}
	if !strings.Contains(string(out), `"took_ms":9007199254740993`) {
		t.Errorf("large integer changed: %s", out)
	}
	var got struct {
		Data struct {
			NotRecorded bool   `json:"not_recorded"`
			Bytes       int    `json:"bytes"`
			SHA256      string `json:"sha256"`
		} `json:"data"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if !got.Data.NotRecorded || got.Data.Bytes != len(data) || got.Data.SHA256 != hex.EncodeToString(sum[:]) {
		t.Errorf("data stub = %+v, want bytes %d sha256 %x of the original", got.Data, len(data), sum)
	}
	if strings.Contains(string(out), "9007199254740992") {
		t.Errorf("number was rounded: %s", out)
	}
}

// A symlink at the old fixed temp name must not be written through.
func TestRecorderMetaDoesNotFollowSymlink(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(victim, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, filepath.Join(dir, "meta.json.tmp")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	r, err := newRecorder(dir, nil, recordMeta{}, fixedClock())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(nil); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(victim); string(b) != "keep" {
		t.Fatalf("symlink target overwritten: %q", b)
	}
}
