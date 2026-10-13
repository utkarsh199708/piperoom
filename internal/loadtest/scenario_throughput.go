package loadtest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

func throughputProfile() Profile {
	return Profile{
		Name: "throughput",
		Help: "rooms concurrent rooms x turns model turns each, once per max_agents value; turns/s, queue wait, pool saturation, insert latency",
		Params: append([]ParamSpec{
			{Name: "rooms", Kind: KindInt, Full: "60", Short: "4", Min: 1, Help: "rooms opened at once"},
			{Name: "turns", Kind: KindInt, Full: "5", Short: "2", Min: 1, Help: "model turns per agent run (turns-1 tool calls, then end_turn)"},
			{Name: "max_agents", Kind: KindIntList, Full: "3,10,30", Short: "2", Min: 1, Help: "PIPEROOM_MAX_AGENTS values, one run each"},
			{Name: "subs_per_room", Kind: KindInt, Full: "2", Short: "1", Min: 0, Help: "WebSocket viewers per room during the run"},
			{Name: "propose", Kind: KindBool, Full: "false", Short: "true", Help: "each run proposes a no-op fix; the driver approves it and counts executions"},
			{Name: "timeout", Kind: KindDuration, Full: "15m", Short: "60s", Min: float64(time.Second)},
		}, modelSpecs("500ms", "20ms")...),
		Run: runThroughput,
	}
}

// ThroughputRun is one max_agents value.
type ThroughputRun struct {
	MaxAgents      int            `json:"max_agents"`
	Rooms          int            `json:"rooms"`
	Turns          int64          `json:"model_turns"`
	MakespanMS     float64        `json:"makespan_ms"`
	TurnsPerSec    float64        `json:"turns_per_sec"`
	IdealPerSec    float64        `json:"ideal_turns_per_sec"` // slots / median turn time
	Events         int64          `json:"events"`
	EventsPerSec   float64        `json:"events_per_sec"`
	QueueWait      Dist           `json:"queue_wait"`
	TTFT           Dist           `json:"time_to_first_turn"`
	RunHold        Dist           `json:"run_hold"`
	Appends        Dist           `json:"event_insert_latency"`
	Pool           []PoolStats    `json:"pool"`
	PeakHeap       uint64         `json:"peak_heap_inuse"`
	PeakGoroutines int            `json:"peak_goroutines"`
	Model          ModelStatsLite `json:"model"`
	Subs           SubTotals      `json:"subs"`
	CPU            CPUUse         `json:"cpu"`
	Exec           *ExecStats     `json:"exec,omitempty"` // with propose=true
	Server         []ServerStats  `json:"server"`
}

// ThroughputData is the throughput scenario's output.
type ThroughputData struct {
	Runs []ThroughputRun `json:"runs"`
}

func (d ThroughputData) markdown() string {
	var b strings.Builder
	b.WriteString("## Agent throughput\n\n")
	rows := [][]string{}
	for _, r := range d.Runs {
		pool := poolSummary(r.Pool)
		rows = append(rows, []string{fmt.Sprint(r.MaxAgents), fmt.Sprint(r.Rooms), fmt.Sprint(r.Turns),
			fmt.Sprintf("%.1f", r.MakespanMS/1000), fmt.Sprintf("%.2f", r.TurnsPerSec), fmt.Sprintf("%.2f", r.IdealPerSec),
			fmt.Sprintf("%.0f", r.EventsPerSec), fms(r.QueueWait.P50MS), fms(r.QueueWait.P95MS), fms(r.QueueWait.MaxMS),
			fms(r.Appends.P50MS), fms(r.Appends.P95MS), fms(r.Appends.P99MS), pool, fmtMiB(r.PeakHeap), r.CPU.String()})
	}
	b.WriteString(mdTable([]string{"max_agents", "rooms", "turns", "makespan s", "turns/s", "ideal turns/s", "events/s",
		"queue wait p50 ms", "p95", "max", "insert p50 ms", "p95", "p99", "pool (max acquired/max; saturated; waits)", "peak heap", "CPU (avg cores)"}, rows))
	return b.String()
}

func poolSummary(ps []PoolStats) string {
	var parts []string
	for _, p := range ps {
		parts = append(parts, fmt.Sprintf("%d/%d; %.0f%%; %d waits %.0f ms", p.MaxAcquired, p.MaxConns, 100*p.SaturatedRatio, p.EmptyAcquires, p.EmptyWaitMS))
	}
	return strings.Join(parts, " + ")
}

func runThroughput(ctx context.Context, env *Env, p Params) (*Result, error) {
	res := &Result{}
	data := ThroughputData{}
	for _, m := range p.Ints("max_agents") {
		run, err := throughputOnce(ctx, env, p, m, res)
		if err != nil {
			return nil, fmt.Errorf("max_agents=%d: %w", m, err)
		}
		data.Runs = append(data.Runs, run)
	}
	res.Data = data
	for i, r := range data.Runs {
		eff := 0.0
		if r.IdealPerSec > 0 {
			eff = r.TurnsPerSec / r.IdealPerSec
		}
		res.finding("max_agents=%d: %.2f turns/s (%.0f%% of the %.2f an ideal %d-slot pool gives at the measured turn time), insert p99 %.1f ms, queue wait p95 %.0f ms, pool %s.",
			r.MaxAgents, r.TurnsPerSec, 100*eff, r.IdealPerSec, min(r.MaxAgents*env.Replicas, r.Rooms), r.Appends.P99MS, r.QueueWait.P95MS, poolSummary(r.Pool))
		if i > 0 {
			prev := data.Runs[i-1]
			if r.TurnsPerSec < 1.2*prev.TurnsPerSec && r.MaxAgents > prev.MaxAgents {
				res.finding("Raising max_agents %d -> %d gained less than 20%% turns/s: the server, not the cap, limits throughput here.", prev.MaxAgents, r.MaxAgents)
			}
		}
	}
	return res, nil
}

func throughputOnce(ctx context.Context, env *Env, p Params, maxAgents int, res *Result) (ThroughputRun, error) {
	pipes := []string{"pa"}
	st, err := env.newStack(ctx, fmt.Sprintf("throughput-m%d", maxAgents), ServerConfig{
		MaxAgents: maxAgents, ToolLatency: p.Dur("tool_latency"), ToolSummaryBytes: p.Int("tool_summary_bytes"), Pipelines: pipes,
	}, nil)
	if err != nil {
		return ThroughputRun{}, err
	}
	defer st.close()
	turns := p.Int("turns")
	env.Model.Reset(modelConfig(p, turns-1))
	if err := st.cluster.Reset(ctx); err != nil {
		return ThroughputRun{}, err
	}
	cpu0 := env.cpuMark(ctx, st)
	n := p.Int("rooms")
	propose := p.Bool("propose")
	titles, pp := make([]string, n), make([]string, n)
	for i := range titles {
		titles[i], pp[i] = fmt.Sprintf("throughput %d", i), "pa"
		if propose {
			titles[i] += " " + ModePropose
		}
	}
	ids, _, err := st.openRooms(ctx, titles, pp, 8)
	if err != nil {
		return ThroughputRun{}, err
	}
	// Viewers join right after the rooms open, like people following an alert.
	subCtx, stopSubs := context.WithCancel(ctx)
	var subs []*Sub
	var wg sync.WaitGroup
	for i, id := range ids {
		for j := 0; j < p.Int("subs_per_room"); j++ {
			s := NewSub(id, loadUsers[(j+1)%len(loadUsers)], 0, nil)
			s.Reconnect = true
			s.Replica = (i + j) % len(st.cluster.Procs)
			subs = append(subs, s)
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				s.Run(subCtx, st.cluster.URL(k), env.HTTP)
			}(i + j)
		}
	}
	waitErr := env.waitRuns(ctx, ids, p.Dur("timeout"))
	cpu1 := env.cpuMark(ctx, st)
	mst := env.Model.Stats()
	srv, sErr := st.serverStats(ctx)
	var exec *ExecStats
	if propose && waitErr == nil && sErr == nil {
		// After the throughput numbers: approve every proposal (viewers still
		// connected) and count what the executors ran.
		es := approveAll(ctx, st, ids, time.Minute)
		exec = &es
	}
	stopSubs()
	wg.Wait()
	if waitErr != nil {
		return ThroughputRun{}, waitErr
	}
	if sErr != nil {
		return ThroughputRun{}, sErr
	}
	tls, err := st.fetchTimelines(ctx, ids)
	if err != nil {
		return ThroughputRun{}, err
	}
	run := ThroughputRun{MaxAgents: maxAgents, Rooms: n, Server: srv, Model: liteModel(mst, ids), Subs: Totals(subs, nil), CPU: cpu0.until(cpu1)}
	if exec != nil {
		exec.countExecuted(tls)
		run.Exec = exec
		// Approval results wake the agents for more turns; the duplicate-run
		// check covers that phase too.
		run.Model = liteModel(env.Model.Stats(), ids)
	}
	var first, last time.Time
	for _, t := range tls {
		if first.IsZero() || t.Created.Before(first) {
			first = t.Created
		}
		if t.IdleAt.After(last) {
			last = t.IdleAt
		}
		run.Events += t.LastSeq
	}
	for _, id := range ids {
		if rs := mst.Rooms[id]; rs != nil {
			run.Turns += int64(rs.Requests)
		}
	}
	makespan := last.Sub(first)
	run.MakespanMS = ms(makespan)
	if makespan > 0 {
		run.TurnsPerSec = round3(float64(run.Turns) / makespan.Seconds())
		run.EventsPerSec = round3(float64(run.Events) / makespan.Seconds())
	}
	run.QueueWait = Summarize(durations(tls, timeline.QueueWait))
	run.TTFT = Summarize(durations(tls, timeline.TTFT))
	run.RunHold = Summarize(durations(tls, timeline.Hold))
	if run.RunHold.P50MS > 0 {
		slots := min(maxAgents*env.Replicas, n)
		run.IdealPerSec = round3(float64(slots) * float64(run.Turns) / float64(n) / (run.RunHold.P50MS / 1000))
	}
	for _, s := range srv {
		run.Pool = append(run.Pool, s.Pool)
		run.PeakHeap = max(run.PeakHeap, s.PeakHeapInuse)
		run.PeakGoroutines = max(run.PeakGoroutines, s.PeakGoroutines)
	}
	run.Appends = srv[0].Appends // per replica in Server; replica 0 for the table
	label := fmt.Sprintf("max_agents=%d: ", maxAgents)
	res.check(label+"agent cap respected", mst.MaxInFlight <= maxAgents*env.Replicas, "max in-flight %d, cap %d x %d replicas", mst.MaxInFlight, maxAgents, env.Replicas)
	res.check(label+"no duplicate agent runs", run.Model.DuplicateRuns == 0 && run.Model.ConcurrentDup == 0,
		"rooms with >1 run start %d, with concurrent model calls %d", run.Model.DuplicateRuns, run.Model.ConcurrentDup)
	want := n * turns
	if propose {
		want += n // the propose_fix turn
	}
	res.check(label+"every run made all its turns", run.Turns == int64(want), "model turns %d, want %d", run.Turns, want)
	if e := run.Exec; e != nil {
		res.check(label+"every approved fix executed exactly once", e.Approved == n && e.ApproveErr == 0 && e.DuplicateExecutions == 0 &&
			e.MissingExecutions == 0 && e.DuplicateExecEvents == 0 && e.ExecutedEvents == e.Approved,
			"proposals %d approved %d approve errors %d; executions %d (duplicate %d, missing %d); action.executed events %d (duplicate %d)",
			e.Proposals, e.Approved, e.ApproveErr, e.Executions, e.DuplicateExecutions, e.MissingExecutions, e.ExecutedEvents, e.DuplicateExecEvents)
	}
	res.check(label+"viewers saw gap-free logs", run.Subs.Gaps == 0 && run.Subs.Dups == 0 && run.Subs.Mismatches == 0,
		"gaps %d dups %d parse mismatches %d", run.Subs.Gaps, run.Subs.Dups, run.Subs.Mismatches)
	res.check(label+"no append errors", sumAppendErrors(srv) == 0, "append errors %d", sumAppendErrors(srv))
	return run, nil
}
