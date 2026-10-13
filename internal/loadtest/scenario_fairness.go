package loadtest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// Shared parameter specs.
func modelSpecs(latency, latencyShort string) []ParamSpec {
	return []ParamSpec{
		{Name: "model_latency", Kind: KindDuration, Full: latency, Short: latencyShort, Help: "fake model reply latency per turn"},
		{Name: "model_tokens", Kind: KindInt, Full: "300", Short: "20", Min: 1, Help: "output tokens per model turn"},
		{Name: "tool_latency", Kind: KindDuration, Full: "50ms", Short: "5ms", Help: "fake read-tool latency"},
		{Name: "tool_summary_bytes", Kind: KindInt, Full: "1024", Short: "128", Help: "fake read-tool summary size"},
	}
}

func modelConfig(p Params, toolCalls int) ModelConfig {
	return ModelConfig{Latency: p.Dur("model_latency"), OutputTokens: p.Int("model_tokens"), ToolCallsPerRun: toolCalls, ToolsPerTurn: 1}
}

func fairnessProfile() Profile {
	return Profile{
		Name: "fairness",
		Help: "3 pipelines: A opens big_rooms rooms at once, B and C open small_rooms each small_delay later; time to first agent turn per pipeline",
		Params: append([]ParamSpec{
			{Name: "big_rooms", Kind: KindInt, Full: "50", Short: "6", Min: 1, Help: "rooms pipeline A opens"},
			{Name: "small_rooms", Kind: KindInt, Full: "3", Short: "1", Min: 1, Help: "rooms pipelines B and C open each"},
			{Name: "small_delay", Kind: KindDuration, Full: "500ms", Short: "50ms", Help: "B and C open this long after A starts"},
			{Name: "max_agents", Kind: KindInt, Full: "3", Short: "2", Min: 1, Help: "PIPEROOM_MAX_AGENTS"},
			{Name: "tool_calls", Kind: KindInt, Full: "2", Short: "1", Min: 0, Help: "read-tool calls per agent run"},
			{Name: "timeout", Kind: KindDuration, Full: "10m", Short: "60s", Min: float64(time.Second)},
		}, modelSpecs("300ms", "20ms")...),
		Run: runFairness,
	}
}

// PipelineFairness is one pipeline's numbers.
type PipelineFairness struct {
	Pipeline  string `json:"pipeline"`
	Rooms     int    `json:"rooms"`
	Queued    int    `json:"queued"`
	TTFT      Dist   `json:"time_to_first_turn"`
	QueueWait Dist   `json:"queue_wait"`
	// Overtaken is the most rooms of other pipelines that got a slot after
	// one of this pipeline's rooms opened and before that room got its own.
	Overtaken int `json:"max_overtaken_by_other_pipelines"`
}

// FairnessData is the fairness scenario's output.
type FairnessData struct {
	Pipelines    []PipelineFairness `json:"pipelines"`
	SmallMaxWait float64            `json:"small_pipelines_max_queue_wait_ms"`
	SmallMaxTTFT float64            `json:"small_pipelines_max_ttft_ms"`
	RunHold      Dist               `json:"run_hold"` // slot to idle per run
	// RoundRobinBoundMS is what a pipeline-fair queue would bound the first
	// B/C room's wait to: about one median run hold (a slot frees and goes
	// to the next pipeline in turn).
	RoundRobinBoundMS float64        `json:"round_robin_bound_ms"`
	MaxModelInFlight  int            `json:"max_model_in_flight"`
	Model             ModelStatsLite `json:"model"`
	CPU               CPUUse         `json:"cpu"`
	Server            []ServerStats  `json:"server"`
	Rooms             []timeline     `json:"rooms"`
}

// ModelStatsLite is the fake model summary kept in reports.
type ModelStatsLite struct {
	Requests      int64 `json:"requests"`
	BadRequests   int64 `json:"bad_requests"`
	MaxInFlight   int   `json:"max_in_flight"`
	DuplicateRuns int   `json:"rooms_with_more_than_one_run_start"`
	ConcurrentDup int   `json:"rooms_with_concurrent_model_calls"`
}

func liteModel(st ModelStats, rooms []string) ModelStatsLite {
	l := ModelStatsLite{Requests: st.Requests, BadRequests: st.BadRequests, MaxInFlight: st.MaxInFlight}
	for _, r := range rooms {
		if rs := st.Rooms[r]; rs != nil {
			if rs.RunStarts > 1 {
				l.DuplicateRuns++
			}
			if rs.MaxInFlight > 1 {
				l.ConcurrentDup++
			}
		}
	}
	return l
}

func (d FairnessData) markdown() string {
	var b strings.Builder
	b.WriteString("## Time to first agent turn per pipeline\n\n")
	rows := [][]string{}
	for _, p := range d.Pipelines {
		rows = append(rows, []string{p.Pipeline, fmt.Sprint(p.Rooms), fmt.Sprint(p.Queued),
			fms(p.TTFT.P50MS), fms(p.TTFT.P95MS), fms(p.TTFT.MaxMS),
			fms(p.QueueWait.P50MS), fms(p.QueueWait.P95MS), fms(p.QueueWait.MaxMS), fmt.Sprint(p.Overtaken)})
	}
	b.WriteString(mdTable([]string{"pipeline", "rooms", "queued", "TTFT p50 ms", "p95", "max", "queue wait p50 ms", "p95", "max", "max overtaken"}, rows))
	fmt.Fprintf(&b, "\nB/C max queue wait **%.0f ms**, max TTFT **%.0f ms**. Run hold (slot to idle) p50 %.0f ms, p95 %.0f ms; a pipeline-fair queue would bound the first B/C room's wait to about one run hold (%.0f ms). Fake model max in-flight %d. CPU: %s.\n",
		d.SmallMaxWait, d.SmallMaxTTFT, d.RunHold.P50MS, d.RunHold.P95MS, d.RoundRobinBoundMS, d.MaxModelInFlight, d.CPU)
	return b.String()
}

func runFairness(ctx context.Context, env *Env, p Params) (*Result, error) {
	res := &Result{}
	pipes := []string{"pa", "pb", "pc"}
	maxAgents := p.Int("max_agents")
	st, err := env.newStack(ctx, "fairness", ServerConfig{
		MaxAgents: maxAgents, ToolLatency: p.Dur("tool_latency"), ToolSummaryBytes: p.Int("tool_summary_bytes"), Pipelines: pipes,
	}, nil)
	if err != nil {
		return nil, err
	}
	defer st.close()
	env.Model.Reset(modelConfig(p, p.Int("tool_calls")))
	if err := st.cluster.Reset(ctx); err != nil {
		return nil, err
	}
	cpu0 := env.cpuMark(ctx, st)

	big, small := p.Int("big_rooms"), p.Int("small_rooms")
	var aTitles, aPipes, bcTitles, bcPipes []string
	for i := 0; i < big; i++ {
		aTitles, aPipes = append(aTitles, fmt.Sprintf("fairness A %d", i)), append(aPipes, "pa")
	}
	for i := 0; i < small; i++ {
		for _, pp := range []string{"pb", "pc"} {
			bcTitles, bcPipes = append(bcTitles, fmt.Sprintf("fairness %s %d", strings.ToUpper(pp[1:]), i)), append(bcPipes, pp)
		}
	}
	var aIDs, bcIDs []string
	var aErr, bcErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		aIDs, _, aErr = st.openRooms(ctx, aTitles, aPipes, 8)
	}()
	go func() {
		defer wg.Done()
		if !sleepCtx(ctx, p.Dur("small_delay")) {
			bcErr = ctx.Err()
			return
		}
		bcIDs, _, bcErr = st.openRooms(ctx, bcTitles, bcPipes, 4)
	}()
	wg.Wait()
	if aErr != nil || bcErr != nil {
		return nil, fmt.Errorf("loadtest: fairness: open rooms: %w", errors.Join(aErr, bcErr))
	}
	all := append(slices.Clone(aIDs), bcIDs...)
	if err := env.waitRuns(ctx, all, p.Dur("timeout")); err != nil {
		return nil, err
	}
	cpu1 := env.cpuMark(ctx, st)
	tls, err := st.fetchTimelines(ctx, all)
	if err != nil {
		return nil, err
	}
	mst := env.Model.Stats()
	srv, err := st.serverStats(ctx)
	if err != nil {
		return nil, err
	}

	data := FairnessData{Rooms: tls, Server: srv, MaxModelInFlight: mst.MaxInFlight, Model: liteModel(mst, all), CPU: cpu0.until(cpu1)}
	byPipe := map[string][]timeline{}
	for _, t := range tls {
		byPipe[t.Pipeline] = append(byPipe[t.Pipeline], t)
	}
	// Slot grant order across all rooms, for the overtaken count.
	order := slices.Clone(tls)
	sort.Slice(order, func(i, j int) bool { return order[i].SlotAt.Before(order[j].SlotAt) })
	for _, pp := range pipes {
		ts := byPipe[pp]
		pf := PipelineFairness{Pipeline: pp, Rooms: len(ts),
			TTFT: Summarize(durations(ts, timeline.TTFT)), QueueWait: Summarize(durations(ts, timeline.QueueWait))}
		for _, t := range ts {
			if t.Queued {
				pf.Queued++
			}
			n := 0
			for _, o := range order {
				if o.Pipeline != pp && o.SlotAt.After(t.Created) && o.SlotAt.Before(t.SlotAt) {
					n++
				}
			}
			pf.Overtaken = max(pf.Overtaken, n)
		}
		data.Pipelines = append(data.Pipelines, pf)
		if pp != "pa" {
			data.SmallMaxWait = max(data.SmallMaxWait, pf.QueueWait.MaxMS)
			data.SmallMaxTTFT = max(data.SmallMaxTTFT, pf.TTFT.MaxMS)
		}
	}
	data.RunHold = Summarize(durations(tls, timeline.Hold))
	data.RoundRobinBoundMS = data.RunHold.P50MS
	res.Data = data

	missing := 0
	for _, t := range tls {
		if t.TTFT() < 0 || t.QueueWait() < 0 {
			missing++
		}
	}
	res.check("every room got a first agent turn", missing == 0, "%d of %d rooms without slot or first turn", missing, len(tls))
	res.check("agent cap respected (fake model in-flight <= max_agents x replicas)", mst.MaxInFlight <= maxAgents*env.Replicas,
		"max in-flight %d, cap %d x %d replicas", mst.MaxInFlight, maxAgents, env.Replicas)
	res.check("no duplicate agent runs", data.Model.DuplicateRuns == 0 && data.Model.ConcurrentDup == 0,
		"rooms with >1 run start %d, rooms with concurrent model calls %d", data.Model.DuplicateRuns, data.Model.ConcurrentDup)
	res.check("no append errors", sumAppendErrors(srv) == 0, "append errors %d", sumAppendErrors(srv))

	a := data.Pipelines[0]
	res.finding("Pipeline A (%d rooms) p50 TTFT %.0f ms; B/C (%d rooms each) max queue wait %.0f ms and max TTFT %.0f ms, behind up to %d rooms of other pipelines.",
		big, a.TTFT.P50MS, small, data.SmallMaxWait, data.SmallMaxTTFT, maxOvertaken(data.Pipelines[1:]))
	if data.SmallMaxWait > 3*data.RoundRobinBoundMS && data.RoundRobinBoundMS > 0 {
		res.finding("FIFO starves B and C: their worst wait is %.1fx a run hold; a pipeline-fair queue would bound it to about one run hold (%.0f ms).",
			data.SmallMaxWait/data.RoundRobinBoundMS, data.RoundRobinBoundMS)
	}
	return res, nil
}

func maxOvertaken(ps []PipelineFairness) int {
	n := 0
	for _, p := range ps {
		n = max(n, p.Overtaken)
	}
	return n
}

func sumAppendErrors(srv []ServerStats) int64 {
	var n int64
	for _, s := range srv {
		n += s.AppendErrors
	}
	return n
}
