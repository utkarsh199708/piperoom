package loadtest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
)

func replayProfile() Profile {
	return Profile{
		Name: "replay",
		Help: "rooms of 1k/10k/20k events: replay-from-0 time and bytes for a new subscriber, server memory per replaying client, and a join storm",
		Params: []ParamSpec{
			{Name: "sizes", Kind: KindIntList, Full: "1000,10000,20000", Short: "100,400", Min: 10, Help: "events per seeded room"},
			{Name: "event_bytes", Kind: KindInt, Full: "600", Short: "200", Min: 0, Help: "text size of the larger seeded events"},
			{Name: "repeats", Kind: KindInt, Full: "3", Short: "1", Min: 1, Help: "single-subscriber replays per size"},
			{Name: "concurrent", Kind: KindInt, Full: "20", Short: "3", Min: 1, Help: "subscribers replaying at once for the memory-per-client figure"},
			{Name: "join", Kind: KindInt, Full: "200", Short: "10", Min: 1, Help: "subscribers joining one room at the same moment"},
			{Name: "join_sizes", Kind: KindIntList, Full: "1000,10000,20000", Short: "400", Min: 10, Help: "room sizes for the join storm (must be in sizes)"},
			{Name: "timeout", Kind: KindDuration, Full: "5m", Short: "60s", Min: float64(time.Second), Help: "per measurement"},
		},
		Run: runReplay,
	}
}

// ReplaySingle is one new subscriber replaying a whole room.
type ReplaySingle struct {
	Size      int     `json:"size"`
	Events    int64   `json:"events"`
	Time      Dist    `json:"time"`
	Bytes     int64   `json:"bytes"`
	BytesPerE float64 `json:"bytes_per_event"`
	Failed    int     `json:"failed"`
}

// ReplayGroup is many subscribers replaying the same room at once.
type ReplayGroup struct {
	Size            int            `json:"size"`
	Clients         int            `json:"clients"`
	Completed       int            `json:"completed"`
	Time            Dist           `json:"per_client_time"`
	WallMS          float64        `json:"wall_ms"`
	BaselineHeap    uint64         `json:"baseline_heap_inuse"`
	PeakHeap        uint64         `json:"peak_heap_inuse"`
	PerClientHeap   uint64         `json:"heap_per_client"`
	PeakGoroutines  int            `json:"peak_goroutines"`
	Pool            PoolStats      `json:"pool"`
	Closes          map[string]int `json:"server_closes"`
	DialErrors      int            `json:"dial_errors"`
	Reconnects      int            `json:"reconnects"` // join storm only: clients reconnect with after=, as the web UI does
	BytesPerClient  int64          `json:"bytes_per_client"`
	GapsDupsMisses  int64          `json:"gaps_dups_mismatches"`
	ServerRSSPeakMB float64        `json:"server_rss_after_mb"`
	CPU             CPUUse         `json:"cpu"`
}

// ReplayData is the replay scenario's output.
type ReplayData struct {
	SeedSeconds float64        `json:"seed_seconds"`
	Single      []ReplaySingle `json:"single"`
	Memory      []ReplayGroup  `json:"memory"`
	Join        []ReplayGroup  `json:"join"`
}

func (d ReplayData) markdown() string {
	var b strings.Builder
	b.WriteString("## Replay from seq 0, one new subscriber\n\n")
	rows := [][]string{}
	for _, s := range d.Single {
		rows = append(rows, []string{fmt.Sprint(s.Size), fmt.Sprint(s.Events), fms(s.Time.P50MS), fms(s.Time.MaxMS),
			fmt.Sprintf("%.2f MB", float64(s.Bytes)/1e6), fmt.Sprintf("%.0f", s.BytesPerE), fmt.Sprint(s.Failed)})
	}
	b.WriteString(mdTable([]string{"room size", "events", "replay p50 ms", "max ms", "bytes", "bytes/event", "failed"}, rows))
	group := func(title string, gs []ReplayGroup) {
		fmt.Fprintf(&b, "\n## %s\n\n", title)
		rows := [][]string{}
		for _, g := range gs {
			rows = append(rows, []string{fmt.Sprint(g.Size), fmt.Sprint(g.Clients), fmt.Sprint(g.Completed), fms(g.Time.P50MS), fms(g.Time.P95MS), fms(g.Time.MaxMS),
				fmt.Sprintf("%.0f", g.WallMS), fmtMiB(g.BaselineHeap), fmtMiB(g.PeakHeap), fmtMiB(g.PerClientHeap), fmt.Sprint(g.PeakGoroutines),
				fmt.Sprintf("%d/%d %.0f%%", g.Pool.MaxAcquired, g.Pool.MaxConns, 100*g.Pool.SaturatedRatio), fmt.Sprint(g.Closes), fmt.Sprint(g.Reconnects), g.CPU.String()})
		}
		b.WriteString(mdTable([]string{"room size", "clients", "completed", "p50 ms", "p95", "max", "wall ms", "heap before", "peak heap", "heap/client", "peak goroutines", "pool", "server closes", "reconnects", "CPU (avg cores)"}, rows))
	}
	group("Concurrent replays (server memory per replaying client)", d.Memory)
	group("Join storm (all subscribers dial at once; they reconnect with after= like the web UI)", d.Join)
	return b.String()
}

// seedReplayRooms writes one room per size straight through the store (the
// production append path, with synchronous_commit off for speed: durability
// of seed data does not matter). Titles carry ModeQuiet.
func seedReplayRooms(ctx context.Context, dbURL string, sizes []int, eventBytes int) (map[int]string, error) {
	sep := "?"
	if strings.Contains(dbURL, "?") {
		sep = "&"
	}
	pg, err := store.New(ctx, dbURL+sep+"synchronous_commit=off", slog.New(slog.DiscardHandler))
	if err != nil {
		return nil, fmt.Errorf("loadtest: seed: %w", err)
	}
	defer pg.Close()
	ids := map[int]string{}
	var mu sync.Mutex
	errs := make([]error, len(sizes))
	var wg sync.WaitGroup
	for i, size := range sizes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := seedRoom(ctx, pg, size, eventBytes)
			errs[i] = err
			mu.Lock()
			ids[size] = id
			mu.Unlock()
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return ids, nil
}

func seedRoom(ctx context.Context, pg *store.PG, size, eventBytes int) (string, error) {
	id := room.NewID("r_")
	creator := events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}
	title := fmt.Sprintf("replay %d %s", size, ModeQuiet)
	if err := pg.CreateRoom(ctx, store.Room{ID: id, Title: title, Severity: "sev2", PipelineID: "pa", Status: "open", OpenedAt: time.Now().UTC()}); err != nil {
		return "", fmt.Errorf("loadtest: seed room: %w", err)
	}
	if _, err := pg.Append(ctx, id, events.RoomCreated, creator, events.RoomCreatedPayload{Title: title, Severity: "sev2", PipelineID: "pa"}); err != nil {
		return "", fmt.Errorf("loadtest: seed room: %w", err)
	}
	text := fillerText(eventBytes)
	for i := 1; i < size; i++ {
		step := fmt.Sprintf("s_%d", i/4)
		var err error
		switch i % 4 {
		case 0:
			_, err = pg.Append(ctx, id, events.AgentStepStart, events.AgentActor, events.AgentStepStartPayload{StepID: step, Title: "lt probe"})
		case 1:
			_, err = pg.Append(ctx, id, events.AgentToolCall, events.AgentActor, events.AgentToolCallPayload{
				StepID: step, ToolUseID: "toolu_" + step, Tool: ProbeToolName, Input: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i))})
		case 2:
			_, err = pg.Append(ctx, id, events.AgentToolResult, events.AgentActor, events.AgentToolResultPayload{
				StepID: step, ToolUseID: "toolu_" + step, Tool: ProbeToolName, Summary: text, TookMS: 12})
		default:
			_, err = pg.Append(ctx, id, events.AgentMessage, events.AgentActor, events.AgentMessagePayload{Text: text})
		}
		if err != nil {
			return "", fmt.Errorf("loadtest: seed room %s event %d: %w", id, i, err)
		}
	}
	return id, nil
}

func runReplay(ctx context.Context, env *Env, p Params) (*Result, error) {
	res := &Result{}
	sizes := p.Ints("sizes")
	var ids map[int]string
	seedStart := time.Now()
	st, err := env.newStack(ctx, "replay", ServerConfig{MaxAgents: 3, Pipelines: []string{"pa"}, SampleEvery: 10 * time.Millisecond},
		func(ctx context.Context, dbURL string) error {
			var err error
			ids, err = seedReplayRooms(ctx, dbURL, sizes, p.Int("event_bytes"))
			return err
		})
	if err != nil {
		return nil, err
	}
	defer st.close()
	data := ReplayData{SeedSeconds: round3(time.Since(seedStart).Seconds())}
	targets := map[int]int64{}
	for _, size := range sizes {
		last, err := st.api(0).LastSeq(ctx, loadUsers[0], ids[size])
		if err != nil {
			return nil, err
		}
		targets[size] = last
	}
	timeout := p.Dur("timeout")

	for _, size := range sizes {
		one := ReplaySingle{Size: size, Events: targets[size]}
		var times []time.Duration
		for r := 0; r < p.Int("repeats"); r++ {
			g := replayGroup(ctx, env, st, size, ids[size], targets[size], 1, false, timeout)
			if g.Completed == 1 {
				times = append(times, time.Duration(g.Time.MaxMS*float64(time.Millisecond)))
				one.Bytes = g.BytesPerClient
			} else {
				one.Failed++
			}
		}
		one.Time = Summarize(times)
		if one.Events > 0 {
			one.BytesPerE = round3(float64(one.Bytes) / float64(one.Events))
		}
		data.Single = append(data.Single, one)
		res.check(fmt.Sprintf("size %d: single replays complete", size), one.Failed == 0, "%d of %d failed", one.Failed, p.Int("repeats"))
	}
	for _, size := range sizes {
		g := replayGroup(ctx, env, st, size, ids[size], targets[size], p.Int("concurrent"), false, timeout)
		data.Memory = append(data.Memory, g)
		res.check(fmt.Sprintf("size %d: %d concurrent replays complete and gap-free", size, g.Clients), g.Completed == g.Clients && g.GapsDupsMisses == 0,
			"completed %d, gaps+dups+mismatches %d, closes %v", g.Completed, g.GapsDupsMisses, g.Closes)
	}
	for _, size := range p.Ints("join_sizes") {
		id, ok := ids[size]
		if !ok {
			return nil, fmt.Errorf("loadtest: replay: join size %d is not in sizes", size)
		}
		g := replayGroup(ctx, env, st, size, id, targets[size], p.Int("join"), true, timeout)
		data.Join = append(data.Join, g)
		// A join storm may not complete: that is a capacity result, reported
		// as a finding. What every client did receive must still be in order.
		res.check(fmt.Sprintf("size %d: join storm of %d is gap-free", size, g.Clients), g.GapsDupsMisses == 0,
			"gaps+dups+mismatches %d (completed %d of %d)", g.GapsDupsMisses, g.Completed, g.Clients)
		if g.Completed < g.Clients {
			res.finding("Join storm on a %d-event room: only %d of %d subscribers caught up within %s; server closes %v.",
				targets[size], g.Completed, g.Clients, timeout, g.Closes)
		}
		if n := g.Closes["1008 replay timeout"]; n > 0 {
			res.finding("Join storm on a %d-event room: %d replays were cut by the gateway's 30 s replay timeout (internal/ws replayTimeout) and resumed with after=; %d reconnects in total.",
				targets[size], n, g.Reconnects)
		}
	}
	res.Data = data
	for _, s := range data.Single {
		res.finding("New subscriber, %d-event room: replay p50 %.0f ms, %.1f MB on the wire (%.0f B/event).", s.Events, s.Time.P50MS, float64(s.Bytes)/1e6, s.BytesPerE)
	}
	for _, g := range data.Memory {
		res.finding("%d clients replaying a %d-event room at once: about %s of server heap per replaying client (peak %s over a %s baseline), p95 %.0f ms each.",
			g.Clients, targets[g.Size], fmtMiB(g.PerClientHeap), fmtMiB(g.PeakHeap), fmtMiB(g.BaselineHeap), g.Time.P95MS)
	}
	for _, g := range data.Join {
		res.finding("Join storm of %d on a %d-event room: p50 %.0f ms, p95 %.0f ms, max %.0f ms per client, wall %.0f ms, peak heap %s, pool %d/%d busy in %.0f%% of samples.",
			g.Clients, targets[g.Size], g.Time.P50MS, g.Time.P95MS, g.Time.MaxMS, g.WallMS, fmtMiB(g.PeakHeap), g.Pool.MaxAcquired, g.Pool.MaxConns, 100*g.Pool.SaturatedRatio)
	}
	return res, nil
}

// replayGroup starts n subscribers on roomID from seq 0 at the same moment
// and waits until each has seen target (or the timeout). It measures the
// server heap around it.
func replayGroup(ctx context.Context, env *Env, st *stack, size int, roomID string, target int64, n int, reconnect bool, timeout time.Duration) ReplayGroup {
	g := ReplayGroup{Size: size, Clients: n, Closes: map[string]int{}}
	_ = st.cluster.GC(ctx)
	_ = st.cluster.Reset(ctx)
	if before, err := st.serverStats(ctx); err == nil && len(before) > 0 {
		g.BaselineHeap = before[0].HeapInuse
	}
	cpu0 := env.cpuMark(ctx, st)
	gctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	subs := make([]*Sub, n)
	var wg sync.WaitGroup
	start := time.Now()
	for i := range subs {
		subs[i] = NewSub(roomID, loadUsers[i%len(loadUsers)], target, nil)
		subs[i].Reconnect = reconnect
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			subs[i].Run(gctx, st.cluster.URL(i), env.HTTP)
		}(i)
	}
	var times []time.Duration
	for _, s := range subs {
		select {
		case <-s.Done:
		case <-gctx.Done():
		}
	}
	g.WallMS = ms(time.Since(start))
	g.CPU = cpu0.until(env.cpuMark(ctx, st))
	if after, err := st.serverStats(ctx); err == nil && len(after) > 0 {
		g.PeakHeap = after[0].PeakHeapInuse
		g.PeakGoroutines = after[0].PeakGoroutines
		g.Pool = after[0].Pool
		g.ServerRSSPeakMB = round3(float64(after[0].RSS) / 1e6)
	}
	cancel()
	wg.Wait()
	for _, s := range subs {
		if s.LastSeq.Load() >= target {
			g.Completed++
			times = append(times, s.DoneAt.Sub(s.Started))
			g.BytesPerClient = s.Bytes.Load()
		}
		for k, v := range s.Closes {
			g.Closes[k] += v
		}
		g.DialErrors += s.DialErrors
		g.Reconnects += s.Reconnects
		g.GapsDupsMisses += s.Gaps + s.Dups + s.Mismatches
	}
	g.Time = Summarize(times)
	if g.PeakHeap > g.BaselineHeap && n > 0 {
		g.PerClientHeap = (g.PeakHeap - g.BaselineHeap) / uint64(n)
	}
	return g
}
