package batchtools

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// Settings of dag_schedule_gap.
const (
	// gapRuns is how many recent runs the tool reads to find the last
	// success and, without a parseable schedule, the observed interval.
	gapRuns = 25
	// minGrace is the least slack before a DAG counts as late.
	minGrace = 5 * time.Minute
	day      = 24 * time.Hour
)

// Verdicts of dag_schedule_gap.
const (
	VerdictOnTime          = "on_time"
	VerdictLate            = "late"
	VerdictNoSuccess       = "no_success"
	VerdictPaused          = "paused"
	VerdictUnscheduled     = "unscheduled"
	VerdictUnknownInterval = "unknown_interval"
)

// Interval sources.
const (
	sourceSchedule = "schedule"
	sourceObserved = "observed"
)

// GapData is dag_schedule_gap's Data.
type GapData struct {
	DAGID    string `json:"dag_id"`
	Schedule string `json:"schedule"` // as the orchestrator shows it; DAG-author text, untrusted
	Paused   bool   `json:"paused"`
	// IntervalSeconds is the time between runs: from the schedule, else the
	// median gap between recent scheduled runs; 0 when neither is known.
	IntervalSeconds float64 `json:"interval_seconds"`
	IntervalSource  string  `json:"interval_source,omitempty"` // schedule | observed
	// IntervalApprox is true for calendar schedules (monthly, yearly) whose
	// interval varies; the tool uses 30 or 365 days.
	IntervalApprox bool `json:"interval_approx,omitempty"`
	// NextRunAt is when the orchestrator will create the next run.
	NextRunAt time.Time `json:"next_run_at"`
	// SchedulerBehindSeconds is how far now is past NextRunAt (plus grace):
	// the scheduler has not created a run that is due. 0 when it is not.
	SchedulerBehindSeconds float64         `json:"scheduler_behind_seconds"`
	LastRun                *clients.DAGRun `json:"last_run,omitempty"`
	LastSuccess            *clients.DAGRun `json:"last_success,omitempty"`
	// SinceLastSuccessSeconds is now minus the last success's end.
	SinceLastSuccessSeconds float64 `json:"since_last_success_seconds"`
	// ExpectedBy is when the next success was due: last success's end, plus
	// one interval, plus grace (the last success's duration and
	// max(5 min, interval/10)).
	ExpectedBy  time.Time `json:"expected_by"`
	LateSeconds float64   `json:"late_seconds"`
	// MissedIntervals is how many whole intervals passed since the last
	// success's end; 0 when on time.
	MissedIntervals int    `json:"missed_intervals"`
	RunsRead        int    `json:"runs_read"`
	Verdict         string `json:"verdict"`
}

type scheduleGap struct{ d Deps }

func (t *scheduleGap) Name() string { return NameScheduleGap }
func (t *scheduleGap) Description() string {
	return "Compares one Airflow DAG's schedule with its last successful run: the schedule and interval, whether the DAG is paused, " +
		"the last run and last success, when the next success was due, how late it is and how many intervals were missed, " +
		"and whether the scheduler failed to create a due run. Use when data is stale or a batch did not land, to tell a failed run " +
		"from a run that never started (paused DAG, scheduler behind)."
}
func (t *scheduleGap) InputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"dag_id": map[string]any{"type": "string", "description": "Airflow dag_id; one of the room pipeline's dags."},
		},
		"required":             []string{"dag_id"},
		"additionalProperties": false,
	}
}
func (t *scheduleGap) Kind() tools.Kind { return tools.KindRead }

func (t *scheduleGap) Run(ctx context.Context, in json.RawMessage) (tools.Result, error) {
	var input struct {
		DAGID string `json:"dag_id"`
	}
	if err := decode(in, &input); err != nil {
		return tools.Result{}, err
	}
	if err := checkID("dag_id", input.DAGID); err != nil {
		return tools.Result{}, err
	}
	if err := t.d.scopeDAG(ctx, input.DAGID); err != nil {
		return tools.Result{}, err
	}
	r, err := t.d.reader(NameScheduleGap)
	if err != nil {
		return tools.Result{}, err
	}
	if t.d.Schedule == nil {
		return tools.Result{}, fmt.Errorf("batchtools: dag_schedule_gap: the orchestrator reader cannot read a DAG's schedule: %w", clients.ErrNotSupported)
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	dag, err := t.d.Schedule.DAG(ctx, input.DAGID)
	if err != nil {
		return tools.Result{}, fmt.Errorf("batchtools: dag_schedule_gap %s: schedule: %w", input.DAGID, err)
	}
	runs, err := r.DAGRuns(ctx, input.DAGID, gapRuns)
	if err != nil {
		return tools.Result{}, fmt.Errorf("batchtools: dag_schedule_gap %s: runs: %w", input.DAGID, err)
	}
	g := computeGap(input.DAGID, dag, runs, t.d.Now())
	summary, cut := tools.Truncate(renderGap(g))
	return tools.Result{Summary: summary, Data: g, Truncated: cut}, nil
}

// computeGap is the tool's arithmetic, apart from I/O for tests.
func computeGap(dagID string, dag clients.DAG, runs []clients.DAGRun, now time.Time) GapData {
	g := GapData{DAGID: dagID, Schedule: dag.Schedule, Paused: dag.Paused, NextRunAt: dag.NextRunAt, RunsRead: len(runs)}
	interval, approx, unscheduled := parseSchedule(dag.Schedule)
	if interval > 0 {
		g.IntervalSource, g.IntervalApprox = sourceSchedule, approx
	} else if !unscheduled {
		if interval = observedInterval(runs); interval > 0 {
			g.IntervalSource = sourceObserved
		}
	}
	g.IntervalSeconds = interval.Seconds()
	grace := max(minGrace, interval/10)

	if len(runs) > 0 {
		lr := runs[0]
		g.LastRun = &lr
	}
	for _, run := range runs {
		if run.State == "success" {
			ls := run
			g.LastSuccess = &ls
			break
		}
	}
	if !dag.Paused && !dag.NextRunAt.IsZero() && now.After(dag.NextRunAt.Add(grace)) {
		g.SchedulerBehindSeconds = now.Sub(dag.NextRunAt).Seconds()
	}

	var ref time.Time
	if g.LastSuccess != nil {
		ref = successTime(*g.LastSuccess)
		if !ref.IsZero() {
			g.SinceLastSuccessSeconds = max(now.Sub(ref), 0).Seconds()
		}
	}
	if interval > 0 && !ref.IsZero() {
		took := runDuration(g.LastSuccess.StartedAt, g.LastSuccess.EndedAt, now)
		g.ExpectedBy = ref.Add(interval + grace + took)
		if late := now.Sub(g.ExpectedBy); late > 0 {
			g.LateSeconds = late.Seconds()
			g.MissedIntervals = int(now.Sub(ref) / interval)
		}
	}

	switch {
	case dag.Paused:
		g.Verdict = VerdictPaused
	case unscheduled:
		g.Verdict = VerdictUnscheduled
	case g.LastSuccess == nil || ref.IsZero():
		g.Verdict = VerdictNoSuccess
	case interval == 0:
		g.Verdict = VerdictUnknownInterval
	case g.LateSeconds > 0:
		g.Verdict = VerdictLate
	default:
		g.Verdict = VerdictOnTime
	}
	return g
}

// successTime is when a successful run's data landed: its end, else its
// start, else its logical date.
func successTime(r clients.DAGRun) time.Time {
	for _, t := range []time.Time{r.EndedAt, r.StartedAt, r.LogicalDate} {
		if !t.IsZero() {
			return t
		}
	}
	return time.Time{}
}

func renderGap(g GapData) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "DAG %s: verdict %s.", g.DAGID, g.Verdict)
	sched := "none"
	if g.Schedule != "" {
		// DAG-author text: quoted and folded to one line.
		sched = strconv.Quote(clean(g.Schedule, 120))
	}
	fmt.Fprintf(&sb, " Schedule %s", sched)
	if g.IntervalSeconds > 0 {
		approx := ""
		if g.IntervalApprox {
			approx = "about "
		}
		fmt.Fprintf(&sb, ", interval %s%s (from the %s)", approx, human(time.Duration(g.IntervalSeconds*float64(time.Second))), g.IntervalSource)
	} else {
		sb.WriteString(", interval unknown")
	}
	if g.Paused {
		sb.WriteString("; the DAG is PAUSED, so no runs are scheduled")
	}
	sb.WriteString(".")
	if g.LastRun != nil {
		fmt.Fprintf(&sb, " Last run %s: %s, logical %s, started %s.", oneLine(g.LastRun.ID, 120), g.LastRun.State, stamp(g.LastRun.LogicalDate), stamp(g.LastRun.StartedAt))
	} else {
		sb.WriteString(" No runs.")
	}
	if g.LastSuccess != nil {
		fmt.Fprintf(&sb, " Last success %s: logical %s, ended %s (%s ago).", oneLine(g.LastSuccess.ID, 120), stamp(g.LastSuccess.LogicalDate),
			stamp(g.LastSuccess.EndedAt), human(time.Duration(g.SinceLastSuccessSeconds*float64(time.Second))))
	} else if g.RunsRead > 0 {
		fmt.Fprintf(&sb, " No successful run in the last %d run(s).", g.RunsRead)
	}
	if !g.ExpectedBy.IsZero() {
		fmt.Fprintf(&sb, " Next success was due by %s", stamp(g.ExpectedBy))
		if g.LateSeconds > 0 {
			fmt.Fprintf(&sb, ": LATE by %s, %d interval(s) since the last success", human(time.Duration(g.LateSeconds*float64(time.Second))), g.MissedIntervals)
		}
		sb.WriteString(".")
	}
	if !g.NextRunAt.IsZero() {
		fmt.Fprintf(&sb, " The scheduler's next run: %s", stamp(g.NextRunAt))
		if g.SchedulerBehindSeconds > 0 {
			fmt.Fprintf(&sb, ", %s in the past: the scheduler has not created a due run (scheduler down, pool or concurrency limits, or max_active_runs)",
				human(time.Duration(g.SchedulerBehindSeconds*float64(time.Second))))
		}
		sb.WriteString(".")
	}
	return sb.String()
}

// parseSchedule reads the interval of the schedule text the client renders
// (cron or preset, "every <duration>", "every 1d 2h", each optionally followed
// by " (description)"). approx is true for monthly and yearly calendars.
// unscheduled is true for no schedule, "@once" and "None". A shape it does
// not know gives 0, and the caller falls back to the observed interval.
func parseSchedule(s string) (interval time.Duration, approx, unscheduled bool) {
	s = strings.TrimSpace(s)
	if i := strings.Index(s, " ("); i > 0 && strings.HasSuffix(s, ")") {
		s = s[:i]
	}
	switch strings.ToLower(s) {
	case "", "@once", "none", "null":
		return 0, false, true
	case "@continuous":
		return 0, false, false
	case "@hourly":
		return time.Hour, false, false
	case "@daily", "@midnight":
		return day, false, false
	case "@weekly":
		return 7 * day, false, false
	case "@monthly":
		return 30 * day, true, false
	case "@quarterly":
		return 91 * day, true, false
	case "@yearly", "@annually":
		return 365 * day, true, false
	}
	if rest, ok := strings.CutPrefix(s, "every "); ok {
		if d, err := time.ParseDuration(rest); err == nil && d > 0 {
			return d, false, false
		}
		d, approx := relativeDelta(rest)
		return d, approx, false
	}
	d, approx := cronInterval(s)
	return d, approx, false
}

// relativeDelta reads the client's rendering of a relativedelta: "1mo 2d".
func relativeDelta(s string) (time.Duration, bool) {
	units := []struct {
		suffix string
		d      time.Duration
		approx bool
	}{{"mo", 30 * day, true}, {"y", 365 * day, true}, {"d", day, false}, {"h", time.Hour, false}, {"m", time.Minute, false}, {"s", time.Second, false}}
	var total time.Duration
	approx := false
	for _, part := range strings.Fields(s) {
		matched := false
		for _, u := range units {
			if num, ok := strings.CutSuffix(part, u.suffix); ok {
				n, err := strconv.ParseInt(num, 10, 32)
				if err != nil || n <= 0 {
					return 0, false
				}
				total += time.Duration(n) * u.d
				approx = approx || u.approx
				matched = true
				break
			}
		}
		if !matched {
			return 0, false
		}
	}
	return total, approx
}

// cronInterval reads the interval of the common 5-field cron shapes: every
// N minutes, hourly, every N hours, daily, weekly on one day, monthly on one
// day. Lists, ranges and other shapes give 0 (observed interval instead).
func cronInterval(s string) (time.Duration, bool) {
	f := strings.Fields(s)
	if len(f) != 5 {
		return 0, false
	}
	minute, hour, dom, month, dow := f[0], f[1], f[2], f[3], f[4]
	if month != "*" {
		return 0, false
	}
	fixed := func(v string, hi int) bool {
		n, err := strconv.Atoi(v)
		return err == nil && n >= 0 && n <= hi
	}
	step := func(v string, hi int) int {
		rest, ok := strings.CutPrefix(v, "*/")
		if !ok {
			return 0
		}
		n, err := strconv.Atoi(rest)
		if err != nil || n <= 0 || n > hi {
			return 0
		}
		return n
	}
	switch {
	case dom == "*" && dow == "*" && hour == "*" && minute == "*":
		return time.Minute, false
	case dom == "*" && dow == "*" && hour == "*" && step(minute, 59) > 0:
		return time.Duration(step(minute, 59)) * time.Minute, false
	case dom == "*" && dow == "*" && hour == "*" && fixed(minute, 59):
		return time.Hour, false
	case dom == "*" && dow == "*" && fixed(minute, 59) && step(hour, 23) > 0:
		return time.Duration(step(hour, 23)) * time.Hour, false
	case dom == "*" && dow == "*" && fixed(minute, 59) && fixed(hour, 23):
		return day, false
	case dom == "*" && fixed(dow, 7) && fixed(minute, 59) && fixed(hour, 23):
		return 7 * day, false
	case dow == "*" && fixed(dom, 31) && dom != "0" && fixed(minute, 59) && fixed(hour, 23):
		return 30 * day, true
	}
	return 0, false
}

// observedInterval is the median gap between the logical dates of recent
// scheduled runs; 0 with fewer than two.
func observedInterval(runs []clients.DAGRun) time.Duration {
	var dates []time.Time
	for _, r := range runs {
		if r.RunType == "scheduled" && !r.LogicalDate.IsZero() {
			dates = append(dates, r.LogicalDate)
		}
	}
	if len(dates) < 2 {
		return 0
	}
	slices.SortFunc(dates, func(a, b time.Time) int { return b.Compare(a) })
	var gaps []time.Duration
	for i := 1; i < len(dates); i++ {
		if d := dates[i-1].Sub(dates[i]); d > 0 {
			gaps = append(gaps, d)
		}
	}
	if len(gaps) == 0 {
		return 0
	}
	slices.Sort(gaps)
	return gaps[len(gaps)/2]
}
