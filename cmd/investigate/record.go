package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/redact"
)

// Recording format (version recordingFormat), one directory per run:
//
//	events.jsonl  one events.Event per line, in emit order, identical in shape to an
//	              export's events[] entry (RoomID = meta.recording_id). The same JSON the
//	              room log and the WebSocket carry. Seq counts from 1, At is the
//	              emit time, RoomID is the recording id. agent.tool_call keeps
//	              its input; agent.tool_result keeps its summary, is_error and
//	              took_ms but NOT its data (see recordPayload).
//	inputs.jsonl  one recordedInput per line: every human input (stdin steers,
//	              auto-approvals, action results) the agent was given.
//	meta.json     one recordMeta: model, effort, system_prompt_sha256, commit (<sha>, or <sha>-dirty), timings,
//	              counts. Written when the run starts (complete=false) and
//	              again when it ends (complete=true).
//
// Everything passes through internal/redact before it is written.
const recordingFormat = "piperoom-recording/1"

// recordMeta is meta.json.
type recordMeta struct {
	Format             string    `json:"format"`
	RecordingID        string    `json:"recording_id"`
	Pipeline           string    `json:"pipeline"`
	Title              string    `json:"title"`
	Model              string    `json:"model"`
	Effort             string    `json:"effort"`
	MaxTurns           int       `json:"max_turns"`
	SystemPromptSHA256 string    `json:"system_prompt_sha256"`
	Commit             string    `json:"commit"`
	FakeTools          bool      `json:"fake_tools"`
	ApproveAll         bool      `json:"approve_all"`
	StartedAt          time.Time `json:"started_at"`
	FinishedAt         time.Time `json:"finished_at,omitzero"`
	DurationMS         int64     `json:"duration_ms"`
	Events             int       `json:"events"`
	Inputs             int       `json:"inputs"`
	ToolCalls          int       `json:"tool_calls"`
	ToolResults        int       `json:"tool_results"`
	// Complete is true once the run ended and the files were closed. A false
	// value means the process died; the event file may be cut short.
	Complete bool `json:"complete"`
	// Error is the redacted, shortened text of the error that ended the run,
	// or of the first write failure of the recorder itself.
	Error string `json:"error,omitempty"`
}

// recordedInput is one line of inputs.jsonl.
type recordedInput struct {
	At    time.Time        `json:"at"`
	Input agent.HumanInput `json:"input"`
}

// recorder is an agent.Sink that writes a recording and passes every event on
// to the next sink (the terminal).
type recorder struct {
	next  agent.Sink
	dir   string
	now   func() time.Time
	evF   *os.File
	inF   *os.File
	mu    sync.Mutex
	meta  recordMeta
	wrErr error
}

// newRecorder creates dir (0700) and its files (0600). It refuses a directory
// that already holds a recording, so a second run can never splice into or
// overwrite an earlier one.
func newRecorder(dir string, next agent.Sink, meta recordMeta, now func() time.Time) (*recorder, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("record: creating %s: %w", dir, err)
	}
	r := &recorder{next: next, dir: dir, now: now, meta: meta}
	var err error
	if r.evF, err = os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600); err != nil {
		return nil, fmt.Errorf("record: %s already holds a recording or is not writable: %w", dir, err)
	}
	if r.inF, err = os.OpenFile(filepath.Join(dir, "inputs.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600); err != nil {
		r.evF.Close()
		return nil, fmt.Errorf("record: opening inputs.jsonl: %w", err)
	}
	r.meta.Format = recordingFormat
	r.meta.StartedAt = now().UTC()
	r.meta.RecordingID = "rec_" + r.meta.StartedAt.Format("20060102T150405Z")
	if err := r.writeMeta(); err != nil {
		r.evF.Close()
		r.inF.Close()
		return nil, err
	}
	return r, nil
}

// Emit records the event, then forwards it. A failed write is remembered and
// returned: a recording with a hole in it must not look complete.
func (r *recorder) Emit(ctx context.Context, typ events.Type, payload any) error {
	r.mu.Lock()
	rec, err := recordPayload(typ, payload)
	if err == nil {
		r.meta.Events++
		switch typ {
		case events.AgentToolCall:
			r.meta.ToolCalls++
		case events.AgentToolResult:
			r.meta.ToolResults++
		}
		err = writeLine(r.evF, events.Event{
			RoomID:  r.meta.RecordingID,
			Seq:     int64(r.meta.Events),
			Type:    typ,
			Actor:   events.AgentActor,
			Payload: rec,
			At:      r.now().UTC(),
		})
	}
	if err != nil {
		r.fail(err)
	}
	r.mu.Unlock()
	if err != nil {
		return fmt.Errorf("record: %w", err)
	}
	if r.next == nil {
		return nil
	}
	return r.next.Emit(ctx, typ, payload)
}

// recordInput writes one human input to inputs.jsonl.
func (r *recorder) recordInput(in agent.HumanInput) {
	r.mu.Lock()
	defer r.mu.Unlock()
	in.Text = redactText(in.Text)
	r.meta.Inputs++
	if err := writeLine(r.inF, recordedInput{At: r.now().UTC(), Input: in}); err != nil {
		r.fail(err)
	}
}

func (r *recorder) fail(err error) {
	if r.wrErr == nil {
		r.wrErr = err
	}
}

// Close finishes the recording. runErr is the error that ended the run (nil
// for a clean end). It returns the first write error of the recording, if any,
// so the caller can say the file is not trustworthy.
func (r *recorder) Close(runErr error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.meta.FinishedAt = r.now().UTC()
	r.meta.DurationMS = r.meta.FinishedAt.Sub(r.meta.StartedAt).Milliseconds()
	switch {
	case r.wrErr != nil:
		r.meta.Error = redactText(r.wrErr.Error())
	case runErr != nil:
		r.meta.Error = redactText(runErr.Error())
	}
	if len(r.meta.Error) > 1000 {
		r.meta.Error = r.meta.Error[:1000]
	}
	r.meta.Complete = r.wrErr == nil
	err := errors.Join(r.writeMeta(), r.evF.Close(), r.inF.Close())
	if r.wrErr != nil {
		return fmt.Errorf("record: %w", r.wrErr)
	}
	return err
}

// writeMeta replaces meta.json atomically (write temp, rename), so a reader
// never sees half a file. Callers hold r.mu or have the recorder to themselves.
func (r *recorder) writeMeta() error {
	b, err := json.MarshalIndent(r.meta, "", "  ")
	if err != nil {
		return fmt.Errorf("record: encoding meta.json: %w", err)
	}
	tmp := filepath.Join(r.dir, "meta.json.tmp")
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return fmt.Errorf("record: writing meta.json: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(r.dir, "meta.json")); err != nil {
		return fmt.Errorf("record: replacing meta.json: %w", err)
	}
	return nil
}

func writeLine(f *os.File, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encoding line: %w", err)
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("writing %s: %w", filepath.Base(f.Name()), err)
	}
	return nil
}

func redactText(s string) string {
	out, _ := redact.Text(s)
	return out
}

// recordPayload returns the JSON to store for one event: secrets masked with
// internal/redact, and the Data of a tool result replaced by its size and
// SHA-256. Data is where record values live (pg_row, kafka_find_by_key,
// warehouse_row), and a recording is meant to be kept and shared, so the
// values are never written. The summary the model read is kept: it is what a
// score needs, and the agent already masks it before it is emitted.
func recordPayload(typ events.Type, payload any) (json.RawMessage, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encoding %s payload: %w", typ, err)
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, fmt.Errorf("decoding %s payload: %w", typ, err)
	}
	if m, ok := v.(map[string]any); ok && typ == events.AgentToolResult {
		if raw, has := m["data"]; has && raw != nil {
			db, _ := json.Marshal(raw)
			sum := sha256.Sum256(db)
			m["data"] = map[string]any{
				"not_recorded": true,
				"bytes":        len(db),
				"sha256":       hex.EncodeToString(sum[:]),
			}
		}
	}
	out, err := json.Marshal(redact.JSON(v))
	if err != nil {
		return nil, fmt.Errorf("encoding redacted %s payload: %w", typ, err)
	}
	return out, nil
}

// recordingInbox shows every input the agent drains to the recorder.
type recordingInbox struct {
	agent.Inbox
	rec *recorder
}

func (ri recordingInbox) Drain(ctx context.Context) ([]agent.HumanInput, error) {
	ins, err := ri.Inbox.Drain(ctx)
	for _, in := range ins {
		ri.rec.recordInput(in)
	}
	return ins, err
}

// buildCommit returns the git commit the binary was built from, "-dirty" when
// the tree had changes, or "unknown". `go run` and `go test` binaries carry no
// VCS stamp; the caller may then try `git rev-parse` (see gitHead).
func buildCommit() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	var rev, dirty string
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "-dirty"
			}
		}
	}
	if rev == "" {
		return ""
	}
	return rev + dirty
}

// gitHead asks git for HEAD of the working directory, for builds without a
// VCS stamp. Empty on any failure; the recording then says "unknown".
func gitHead(ctx context.Context) string {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "HEAD").Output()
	if err != nil {
		return ""
	}
	rev := strings.TrimSpace(string(out))
	if st, err := exec.CommandContext(ctx, "git", "status", "--porcelain", "--untracked-files=no").Output(); err == nil && len(strings.TrimSpace(string(st))) > 0 {
		rev += "-dirty"
	}
	return rev
}
