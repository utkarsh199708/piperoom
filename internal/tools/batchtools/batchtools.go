// Package batchtools registers the read-only tools of a batch pipeline's
// orchestrator (Airflow, P4c): a DAG's last runs, the task instances of one
// run, the end of one task try's log, and how late the DAG is against its
// schedule. Every tool reads through clients.OrchestratorReader (and the
// optional clients.DAGScheduleReader), checks the DAG against the room
// pipeline's dags before any read, and treats text the orchestrator returns
// (run notes, logs) as untrusted: redacted, scanned and fenced as data.
//
// Receipts (tools.Record) are written by the client, one per request, so the
// tools do not record again.
package batchtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// Tool names.
const (
	NameDAGRuns       = "dag_runs"
	NameTaskInstances = "task_instances"
	NameTaskLogTail   = "task_log_tail"
	NameScheduleGap   = "dag_schedule_gap"
)

// callTimeout is the deadline of one tool call (AGENTS.md rule 4); the client
// adds its own 10 s per request on top.
const callTimeout = 10 * time.Second

// Input limits.
const (
	maxIDBytes   = 250 // Airflow's ID_LEN for dag_id, task_id and run_id
	maxTry       = 1000
	maxMapIndex  = 1 << 20
	noteRunes    = 200 // a run note in the summary
	maxListLines = 60  // task instance lines in the summary
)

// idRE is what Airflow accepts for a dag_id or task_id (KEY_REGEX), the same
// set owners.yaml accepts for dags.
var idRE = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

// Deps are the collaborators of the tools.
type Deps struct {
	// Orchestrator reads runs, task instances and logs. Nil is allowed (no
	// AIRFLOW_URL): every tool then answers clients.ErrNotSupported after the
	// scope check.
	Orchestrator clients.OrchestratorReader
	// Schedule reads a DAG's schedule for dag_schedule_gap. Nil uses
	// Orchestrator when it implements clients.DAGScheduleReader (*airflow.Client
	// does); otherwise the tool answers clients.ErrNotSupported.
	Schedule clients.DAGScheduleReader
	// Owners resolves the room's pipeline id to its dags. It must be non-nil.
	Owners *owners.Owners
	// Now is the clock of dag_schedule_gap and run durations; nil is time.Now.
	Now func() time.Time
}

// Register adds the four batch tools to reg. They are registered even without
// an orchestrator, so the tool list the model sees does not depend on setup.
func Register(reg *tools.Registry, d Deps) error {
	if d.Owners == nil {
		return errors.New("batchtools: Owners is nil (wiring error)")
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Schedule == nil && d.Orchestrator != nil {
		if s, ok := d.Orchestrator.(clients.DAGScheduleReader); ok {
			d.Schedule = s
		}
	}
	for _, t := range []tools.Tool{
		&dagRuns{d: d},
		&taskInstances{d: d},
		&taskLogTail{d: d},
		&scheduleGap{d: d},
	} {
		if err := reg.Register(t); err != nil {
			return fmt.Errorf("batchtools: register %s: %w", t.Name(), err)
		}
	}
	return nil
}

// decode reads a tool's JSON input strictly: unknown fields are bad input,
// matching the schemas' additionalProperties: false.
func decode(in json.RawMessage, v any) error {
	if len(in) == 0 {
		return fmt.Errorf("%w: missing input", tools.ErrBadInput)
	}
	dec := json.NewDecoder(strings.NewReader(string(in)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	return nil
}

// checkID validates a dag_id or task_id.
func checkID(field, v string) error {
	switch {
	case v == "":
		return fmt.Errorf("%w: %s is required", tools.ErrBadInput, field)
	case len(v) > maxIDBytes:
		return fmt.Errorf("%w: %s is longer than %d bytes", tools.ErrBadInput, field, maxIDBytes)
	case !idRE.MatchString(v):
		return fmt.Errorf("%w: %s %q: want letters, digits, '_', '.' and '-' only", tools.ErrBadInput, field, oneLine(v, 60))
	}
	return nil
}

// checkRunID validates a dag_run_id. Airflow allows most characters in one
// (scheduled__2026-10-17T00:00:00+00:00), so only length, control characters
// and path tricks are refused; the client path-escapes every segment.
func checkRunID(v string) error {
	switch {
	case v == "":
		return fmt.Errorf("%w: run_id is required", tools.ErrBadInput)
	case len(v) > maxIDBytes:
		return fmt.Errorf("%w: run_id is longer than %d bytes", tools.ErrBadInput, maxIDBytes)
	case !utf8.ValidString(v) || strings.IndexFunc(v, unicode.IsControl) >= 0:
		return fmt.Errorf("%w: run_id has control characters or invalid UTF-8", tools.ErrBadInput)
	case v == "." || v == ".." || strings.ContainsAny(v, "/\\"):
		return fmt.Errorf("%w: run_id %q: '/', '\\', '.' and '..' are not run ids", tools.ErrBadInput, oneLine(v, 60))
	}
	return nil
}

// scopeDAG resolves the call's scope and refuses a DAG outside the room's
// pipeline. It runs before any read.
func (d Deps) scopeDAG(ctx context.Context, dagID string) error {
	sc, err := tools.ScopeFor(ctx, d.Owners)
	if err != nil {
		return err
	}
	return sc.CheckDAG(dagID)
}

// reader returns the orchestrator or an ErrNotSupported error naming the tool.
func (d Deps) reader(tool string) (clients.OrchestratorReader, error) {
	if d.Orchestrator == nil {
		return nil, fmt.Errorf("batchtools: %s: no orchestrator configured (AIRFLOW_URL): %w", tool, clients.ErrNotSupported)
	}
	return d.Orchestrator, nil
}

// Caps of server strings (sanitizeRun, sanitizeTask).
const (
	maxStateRunes    = 40   // state, run_type
	maxOperatorRunes = 80   // operator class name
	maxNoteRunes     = 2000 // a run note in Data
	maxScheduleRunes = 300  // schedule text in Data
)

// clean makes one string from the orchestrator safe for the summary and
// Data: the log's sanitiser (invalid UTF-8, ANSI sequences, control
// characters), secrets masked, folded to one line, cut to n runes. Every
// server-sourced string goes through it: ids, states, operators, run types,
// notes, schedules.
func clean(s string, n int) string {
	s, _ = redact.Text(sanitizeLog(s))
	return oneLine(s, n)
}

// sanitizeRun cleans every string of a run as the server sent it.
func sanitizeRun(r clients.DAGRun) clients.DAGRun {
	r.ID = clean(r.ID, maxIDBytes)
	r.State = clean(r.State, maxStateRunes)
	r.RunType = clean(r.RunType, maxStateRunes)
	r.Note = clean(r.Note, maxNoteRunes)
	return r
}

// sanitizeTask cleans every string of a task instance as the server sent it.
func sanitizeTask(t clients.TaskInstance) clients.TaskInstance {
	t.TaskID = clean(t.TaskID, maxIDBytes)
	t.State = clean(t.State, maxStateRunes)
	t.Operator = clean(t.Operator, maxOperatorRunes)
	return t
}

// oneLine folds whitespace and control characters to single spaces and cuts
// to n runes, ending in "…" when cut.
func oneLine(s string, n int) string {
	s = strings.ToValidUTF8(s, "�")
	s = strings.Join(strings.FieldsFunc(s, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }), " ")
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n]) + "…"
}

// stamp renders a time for the summary; "-" for the zero time.
func stamp(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

// human renders a duration rounded for reading: seconds under a minute,
// minutes under a day, hours and days after.
func human(d time.Duration) string {
	neg := d < 0
	if neg {
		d = -d
	}
	var s string
	switch {
	case d < time.Minute:
		s = d.Round(time.Second).String()
	case d < 24*time.Hour:
		s = d.Round(time.Minute).String()
		s = strings.TrimSuffix(s, "0s")
	default:
		days := d / (24 * time.Hour)
		h := (d % (24 * time.Hour)).Round(time.Hour) / time.Hour
		if h == 24 {
			days, h = days+1, 0
		}
		s = fmt.Sprintf("%dd", days)
		if h > 0 {
			s += fmt.Sprintf("%dh", h)
		}
	}
	if neg {
		return "-" + s
	}
	return s
}
