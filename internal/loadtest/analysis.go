package loadtest

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"piperoom/internal/events"
)

// timeline is when the things that matter happened in one room, all on the
// server clock (event "at").
type timeline struct {
	Room      string    `json:"room"`
	Pipeline  string    `json:"pipeline"`
	Created   time.Time `json:"created"`
	Queued    bool      `json:"queued"`
	Ahead     int       `json:"ahead"` // rooms ahead when it was queued
	SlotAt    time.Time `json:"slot_at"`
	FirstTurn time.Time `json:"first_turn"`
	IdleAt    time.Time `json:"idle_at"`
	Events    int       `json:"events"`
	LastSeq   int64     `json:"last_seq"`
	Proposals int       `json:"proposals,omitempty"`
	Executed  int       `json:"action_executed_events,omitempty"`
}

// QueueWait is room open to agent slot (the first "working" status).
func (t timeline) QueueWait() time.Duration { return sub(t.SlotAt, t.Created) }

// TTFT is room open to the first event of a completed model turn.
func (t timeline) TTFT() time.Duration { return sub(t.FirstTurn, t.Created) }

// Hold is how long the run held its slot: slot to idle.
func (t timeline) Hold() time.Duration { return sub(t.IdleAt, t.SlotAt) }

func sub(a, b time.Time) time.Duration {
	if a.IsZero() || b.IsZero() {
		return -1
	}
	return a.Sub(b)
}

// buildTimeline folds a room's log.
func buildTimeline(room string, evs []events.Event) timeline {
	t := timeline{Room: room, Events: len(evs)}
	for _, e := range evs {
		t.LastSeq = max(t.LastSeq, e.Seq)
		switch e.Type {
		case events.RoomCreated:
			if t.Created.IsZero() {
				t.Created = e.At
				var p events.RoomCreatedPayload
				if json.Unmarshal(e.Payload, &p) == nil {
					t.Pipeline = p.PipelineID
				}
			}
		case events.AgentStatus:
			var p events.AgentStatusPayload
			if json.Unmarshal(e.Payload, &p) != nil {
				continue
			}
			switch p.State {
			case "queued":
				if !t.Queued {
					t.Queued = true
					_, _ = fmt.Sscanf(p.Detail, "waiting for an agent slot (%d ahead)", &t.Ahead)
				}
			case "working":
				if t.SlotAt.IsZero() {
					t.SlotAt = e.At
				}
			case "idle":
				if !t.FirstTurn.IsZero() && t.IdleAt.IsZero() {
					t.IdleAt = e.At
				}
			}
		case events.ProposalCreated:
			t.Proposals++
		case events.ActionExecuted:
			t.Executed++
		case events.AgentStepStart, events.AgentToolCall, events.AgentMessage, events.AgentHypothesis:
			if t.FirstTurn.IsZero() {
				t.FirstTurn = e.At
			}
		}
	}
	return t
}

// fetchTimelines reads every room's log (8 at a time) and folds it.
func (s *stack) fetchTimelines(ctx context.Context, rooms []string) ([]timeline, error) {
	out := make([]timeline, len(rooms))
	errs := make([]error, len(rooms))
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for i, r := range rooms {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			evs, err := s.api(i).Events(ctx, loadUsers[0], r)
			if err != nil {
				errs[i] = err
				return
			}
			out[i] = buildTimeline(r, evs)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ExecStats is the approval and executor outcome of a run with proposals.
type ExecStats struct {
	Proposals  int `json:"proposals"`
	Approved   int `json:"approved"`
	ApproveErr int `json:"approve_errors"`
	// Executions counts lt_noop runs by key across every server; a key run
	// twice is a duplicate execution, a key never run a missing one.
	Executions          int `json:"executions"`
	DuplicateExecutions int `json:"duplicate_executions"`
	MissingExecutions   int `json:"missing_executions"`
	// ExecutedEvents counts action.executed in the logs; more than one per
	// proposal (one step each) is a duplicate.
	ExecutedEvents      int `json:"action_executed_events"`
	DuplicateExecEvents int `json:"duplicate_action_executed_events"`
}

// approveAll approves every open proposal in rooms as the postgres owner,
// reading each room on one server and approving on the next, so with
// several replicas the executor runs where the approval lands. It then waits
// until every approved proposal has executed (or timeout) plus a grace period
// in which a duplicate execution would show up.
func approveAll(ctx context.Context, st *stack, rooms []string, timeout time.Duration) ExecStats {
	var es ExecStats
	var mu sync.Mutex
	keys := map[string]bool{}
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for i, r := range rooms {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			props, err := st.api(i).OpenProposals(ctx, "priya", r)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				es.ApproveErr++
				return
			}
			for _, p := range props {
				es.Proposals++
				mu.Unlock()
				err := st.api(i+1).Approve(ctx, "priya", p.ProposalID, "postgres", p.ParamsHash)
				mu.Lock()
				if err != nil {
					es.ApproveErr++
					continue
				}
				es.Approved++
				keys[r] = true // the fake model's propose step uses the room id as key
			}
		}()
	}
	wg.Wait()
	runs := func() map[string]int {
		total := map[string]int{}
		srv, err := st.serverStats(ctx)
		if err != nil {
			return total
		}
		for _, s := range srv {
			for k, v := range s.ActionRuns {
				total[k] += v
			}
		}
		return total
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		done := 0
		total := runs()
		for k := range keys {
			if total[k] > 0 {
				done++
			}
		}
		if done == len(keys) {
			break
		}
		if !sleepCtx(ctx, 100*time.Millisecond) {
			break
		}
	}
	sleepCtx(ctx, 2*time.Second) // a duplicate executor would run within this
	total := runs()
	for _, v := range total {
		es.Executions += v
		if v > 1 {
			es.DuplicateExecutions++
		}
	}
	for k := range keys {
		if total[k] == 0 {
			es.MissingExecutions++
		}
	}
	return es
}

// countExecuted adds the action.executed counts of the timelines to es.
func (es *ExecStats) countExecuted(tls []timeline) {
	for _, t := range tls {
		es.ExecutedEvents += t.Executed
		if t.Proposals > 0 && t.Executed > t.Proposals {
			es.DuplicateExecEvents += t.Executed - t.Proposals
		}
	}
}

// durations collects f over timelines, skipping missing values (< 0).
func durations(ts []timeline, f func(timeline) time.Duration) []time.Duration {
	out := make([]time.Duration, 0, len(ts))
	for _, t := range ts {
		if d := f(t); d >= 0 {
			out = append(out, d)
		}
	}
	return out
}

// mdTable renders a markdown table.
func mdTable(header []string, rows [][]string) string {
	var b strings.Builder
	b.WriteString("| " + strings.Join(header, " | ") + " |\n|")
	for range header {
		b.WriteString("---|")
	}
	b.WriteString("\n")
	for _, r := range rows {
		b.WriteString("| " + strings.Join(r, " | ") + " |\n")
	}
	return b.String()
}

func fms(f float64) string { return fmt.Sprintf("%.1f", f) }

// distOrDash renders a latency, or "-" when there were no samples.
func distOrDash(v float64, n int) string {
	if n == 0 {
		return "-"
	}
	return fms(v)
}

func distCells(d Dist) []string {
	return []string{fmt.Sprint(d.N), fms(d.P50MS), fms(d.P95MS), fms(d.P99MS), fms(d.MaxMS)}
}

func fmtMiB(b uint64) string { return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20)) }

// renderMarkdown is the generic part of a profile report; the scenario's
// data adds its own tables through markdowner.
func renderMarkdown(r *Result, m Machine) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# loadtest %s", r.Profile)
	if r.Replicas > 1 {
		fmt.Fprintf(&b, " (%d replicas)", r.Replicas)
	}
	if r.Short {
		b.WriteString(" (short)")
	}
	fmt.Fprintf(&b, "\n\nStarted %s, wall %.1f s, load average %q at start and %q at end. Machine: %d CPUs (%s), %s, kernel %s, %s, Postgres %s. %s.\n\n",
		r.Started.UTC().Format(time.RFC3339), r.WallS, r.LoadAvg[0], r.LoadAvg[1], m.CPUs, m.CPUModel, m.MemTotal, m.Kernel, m.GoVersion, m.Postgres, m.Note)
	keys := make([]string, 0, len(r.Params))
	for k := range r.Params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var ps []string
	for _, k := range keys {
		ps = append(ps, k+"="+r.Params[k])
	}
	fmt.Fprintf(&b, "Parameters: `%s`\n\n", strings.Join(ps, " "))
	if md, ok := r.Data.(interface{ markdown() string }); ok {
		b.WriteString(md.markdown())
		b.WriteString("\n")
	}
	if len(r.Findings) > 0 {
		b.WriteString("## Findings\n\n")
		for _, f := range r.Findings {
			b.WriteString("- " + f + "\n")
		}
		b.WriteString("\n")
	}
	b.WriteString("## Checks\n\n")
	rows := make([][]string, 0, len(r.Checks))
	for _, c := range r.Checks {
		ok := "PASS"
		if !c.OK {
			ok = "FAIL"
		}
		rows = append(rows, []string{c.Name, ok, c.Detail})
	}
	b.WriteString(mdTable([]string{"check", "result", "detail"}, rows))
	return b.String()
}
