package loadtest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

func isolationProfile() Profile {
	return Profile{
		Name: "isolation",
		Help: "quiet rooms with viewers and comments, first alone and then while one room floods tool events; delivery latency of the quiet rooms in both phases",
		Params: []ParamSpec{
			{Name: "quiet_rooms", Kind: KindInt, Full: "10", Short: "2", Min: 1},
			{Name: "subs_per_room", Kind: KindInt, Full: "20", Short: "3", Min: 1},
			{Name: "rate", Kind: KindFloat, Full: "2", Short: "4", Min: 0.01, Help: "comments per second per quiet room"},
			{Name: "window", Kind: KindDuration, Full: "20s", Short: "1500ms", Min: float64(100 * time.Millisecond), Help: "length of each phase"},
			{Name: "pad", Kind: KindInt, Full: "300", Short: "50", Min: 0},
			{Name: "warmup", Kind: KindDuration, Full: "3s", Short: "500ms", Help: "unrecorded comment traffic before the baseline"},
			{Name: "flood_tools_per_turn", Kind: KindInt, Full: "8", Short: "4", Min: 1, Help: "tool calls per model turn in the flood room"},
			{Name: "flood_model_latency", Kind: KindDuration, Full: "0s", Help: "model latency in the flood room"},
			{Name: "flood_tool_latency", Kind: KindDuration, Full: "0s", Help: "fake tool latency (all rooms; only the flood room calls tools)"},
			{Name: "flood_summary_bytes", Kind: KindInt, Full: "2000", Short: "200", Min: 0, Help: "tool summary size in the flood room"},
			{Name: "flood_subs", Kind: KindInt, Full: "20", Short: "3", Min: 0, Help: "viewers of the flood room"},
			{Name: "timeout", Kind: KindDuration, Full: "5m", Short: "60s", Min: float64(time.Second)},
		},
		Run: runIsolation,
	}
}

// IsolationPhase is the quiet rooms' numbers in one phase.
type IsolationPhase struct {
	Name     string    `json:"name"`
	Delivery Dist      `json:"event_to_delivery"`
	EndToEnd Dist      `json:"post_to_delivery"`
	Post     Dist      `json:"comment_post_latency"`
	Appends  Dist      `json:"event_insert_latency"`
	Posted   int64     `json:"comments_posted"`
	Failed   int64     `json:"comment_errors"`
	FloodEPS float64   `json:"flood_room_events_per_sec"`
	Achieved float64   `json:"comments_accepted_per_sec"`
	Offered  float64   `json:"comments_offered_per_sec"`
	Pool     PoolStats `json:"pool"`
	CPU      CPUUse    `json:"cpu"`
}

// IsolationData is the isolation scenario's output.
type IsolationData struct {
	Baseline      IsolationPhase `json:"baseline"`
	Flood         IsolationPhase `json:"flood"`
	FloodDelivery Dist           `json:"flood_room_event_to_delivery"`
	FloodEvents   int64          `json:"flood_room_events"`
	FloodTurns    int            `json:"flood_room_model_turns"`
	FloodTools    int            `json:"flood_room_tool_calls"`
	P95Ratio      float64        `json:"quiet_delivery_p95_ratio"`
	P95DeltaMS    float64        `json:"quiet_delivery_p95_delta_ms"`
	E2EP95Ratio   float64        `json:"quiet_post_to_delivery_p95_ratio"`
	PostP95Ratio  float64        `json:"quiet_post_p95_ratio"`
	Totals        SubTotals      `json:"subs_totals"`
	Server        []ServerStats  `json:"server"`
}

func (d IsolationData) markdown() string {
	var b strings.Builder
	b.WriteString("## Isolation: quiet rooms alone vs. while one room floods\n\n")
	rows := [][]string{}
	for _, ph := range []IsolationPhase{d.Baseline, d.Flood} {
		rows = append(rows, []string{ph.Name, fmt.Sprintf("%.0f", ph.FloodEPS), fmt.Sprintf("%.1f / %.1f", ph.Achieved, ph.Offered),
			fms(ph.Delivery.P50MS), fms(ph.Delivery.P95MS), fms(ph.Delivery.P99MS),
			fms(ph.EndToEnd.P50MS), fms(ph.EndToEnd.P95MS), fms(ph.Post.P50MS), fms(ph.Post.P95MS),
			fms(ph.Appends.P50MS), fms(ph.Appends.P95MS), fmt.Sprintf("%d/%d %.0f%% (%d waits)", ph.Pool.MaxAcquired, ph.Pool.MaxConns, 100*ph.Pool.SaturatedRatio, ph.Pool.EmptyAcquires),
			ph.CPU.String()})
	}
	b.WriteString(mdTable([]string{"phase", "flood events/s", "quiet comments/s accepted / offered", "quiet event->delivery p50 ms", "p95", "p99", "post->delivery p50", "p95", "POST p50", "POST p95", "insert p50", "insert p95", "pool", "CPU (avg cores)"}, rows))
	fmt.Fprintf(&b, "\nQuiet-room delivery p95 ratio flood/baseline: **%.2fx** (+%.1f ms); post->delivery p95 ratio %.2fx; POST p95 ratio %.2fx. Flood room delivery p95 %.1f ms over %d events.\n",
		d.P95Ratio, d.P95DeltaMS, d.E2EP95Ratio, d.PostP95Ratio, d.FloodDelivery.P95MS, d.FloodEvents)
	return b.String()
}

func ratio(a, b float64) float64 {
	if b <= 0 {
		return 0
	}
	return round3(a / b)
}

func runIsolation(ctx context.Context, env *Env, p Params) (*Result, error) {
	res := &Result{}
	st, err := env.newStack(ctx, "isolation", ServerConfig{
		MaxAgents: 3, ToolLatency: p.Dur("flood_tool_latency"), ToolSummaryBytes: p.Int("flood_summary_bytes"), Pipelines: []string{"pa"},
	}, nil)
	if err != nil {
		return nil, err
	}
	defer st.close()
	env.Model.Reset(ModelConfig{FloodLatency: p.Dur("flood_model_latency"), FloodToolsPerTurn: p.Int("flood_tools_per_turn")})
	timeout := p.Dur("timeout")
	quiet, err := quietRooms(ctx, env, st, "isolation quiet", p.Int("quiet_rooms"), timeout)
	if err != nil {
		return nil, err
	}
	rg := newRig(ctx, env, st)
	defer rg.close()
	quietSink := &LatencySink{Delivery: NewRecorder(0), EndToEnd: NewRecorder(0)}
	quietSink.Off.Store(true)
	if _, err := rg.connect(quiet, p.Int("quiet_rooms")*p.Int("subs_per_room"), quietSink, timeout); err != nil {
		return nil, err
	}
	sleepCtx(ctx, env.settle())
	// Unrecorded warm-up, as in fanout.
	rg.comment(ctx, quiet, p.Float("rate"), p.Dur("warmup"), p.Int("pad"), NewRecorder(1))

	phase := func(name string, floodRoom string) (IsolationPhase, error) {
		ph := IsolationPhase{Name: name}
		quietSink.Delivery.Reset()
		quietSink.EndToEnd.Reset()
		if err := st.cluster.Reset(ctx); err != nil {
			return ph, err
		}
		var floodStart int64
		if floodRoom != "" {
			floodStart, _ = st.api(0).LastSeq(ctx, loadUsers[0], floodRoom)
		}
		quietSink.From.Store(time.Now().UnixNano())
		quietSink.Off.Store(false)
		post := NewRecorder(0)
		cpu0 := env.cpuMark(ctx, st)
		t0 := time.Now()
		ph.Posted, ph.Failed = rg.comment(ctx, quiet, p.Float("rate"), p.Dur("window"), p.Int("pad"), post)
		elapsed := time.Since(t0)
		ph.Offered = round3(p.Float("rate") * float64(len(quiet)))
		ph.Achieved = round3(float64(ph.Posted) / elapsed.Seconds())
		if floodRoom != "" {
			end, _ := st.api(0).LastSeq(ctx, loadUsers[0], floodRoom)
			ph.FloodEPS = round3(float64(end-floodStart) / elapsed.Seconds())
		}
		ph.CPU = cpu0.until(env.cpuMark(ctx, st))
		// Late deliveries of this phase's comments still count.
		if _, err := rg.drain(ctx, quiet, 15*time.Second); err != nil {
			return ph, err
		}
		quietSink.Off.Store(true)
		srv, err := st.serverStats(ctx)
		if err != nil {
			return ph, err
		}
		ph.Delivery, ph.EndToEnd, ph.Post = quietSink.Delivery.Dist(), quietSink.EndToEnd.Dist(), post.Dist()
		ph.Appends, ph.Pool = srv[0].Appends, srv[0].Pool
		return ph, nil
	}

	data := IsolationData{}
	if data.Baseline, err = phase("baseline", ""); err != nil {
		return nil, err
	}

	// The flood room: its agent calls tools on every turn; the driver steers
	// it every second so the turn guard (40 turns) never parks it.
	floodIDs, _, err := st.openRooms(ctx, []string{"isolation flood " + ModeFlood}, []string{"pa"}, 1)
	if err != nil {
		return nil, err
	}
	flood := floodIDs[0]
	floodSink := &LatencySink{Delivery: NewRecorder(0)}
	if n := p.Int("flood_subs"); n > 0 {
		if _, err := rg.connect([]string{flood}, n, floodSink, timeout); err != nil {
			return nil, err
		}
	}
	steerCtx, stopSteer := context.WithCancel(ctx)
	var steerWG sync.WaitGroup
	steerWG.Add(1)
	go func() {
		defer steerWG.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-steerCtx.Done():
				return
			case <-t.C:
				_ = st.api(0).Steer(steerCtx, loadUsers[0], flood, "keep going")
			}
		}
	}()
	// Wait for the flood to be under way before measuring.
	for i := 0; i < 100; i++ {
		if rs := env.Model.Stats().Rooms[flood]; rs != nil && rs.ToolUses > 0 {
			break
		}
		sleepCtx(ctx, 50*time.Millisecond)
	}
	data.Flood, err = phase("flood", flood)
	stopSteer()
	steerWG.Wait()
	if err != nil {
		return nil, err
	}
	data.FloodEvents, _ = st.api(0).LastSeq(ctx, loadUsers[0], flood)
	if rs := env.Model.Stats().Rooms[flood]; rs != nil {
		data.FloodTurns, data.FloodTools = rs.Requests, rs.ToolUses
	}
	if err := st.api(0).CloseRoom(ctx, loadUsers[0], flood); err != nil {
		res.finding("closing the flood room failed: %v", err)
	}
	all := append(append([]string{}, quiet...), flood)
	final, err := rg.drain(ctx, all, 30*time.Second)
	if err != nil {
		return nil, err
	}
	srv, err := st.serverStats(ctx)
	if err != nil {
		return nil, err
	}
	rg.close()
	data.Server = srv
	data.Totals = Totals(rg.subs, final)
	data.FloodDelivery = floodSink.Delivery.Dist()
	data.P95Ratio = ratio(data.Flood.Delivery.P95MS, data.Baseline.Delivery.P95MS)
	data.P95DeltaMS = round3(data.Flood.Delivery.P95MS - data.Baseline.Delivery.P95MS)
	data.E2EP95Ratio = ratio(data.Flood.EndToEnd.P95MS, data.Baseline.EndToEnd.P95MS)
	data.PostP95Ratio = ratio(data.Flood.Post.P95MS, data.Baseline.Post.P95MS)
	res.Data = data

	t := data.Totals
	res.check("every subscriber saw every event once, in order", t.Gaps == 0 && t.Dups == 0 && t.MissingTail == 0 && t.Mismatches == 0,
		"gaps %d dups %d missing-at-end %d parse mismatches %d", t.Gaps, t.Dups, t.MissingTail, t.Mismatches)
	res.check("comments accepted in both phases", data.Baseline.Failed == 0 && data.Flood.Failed == 0 && data.Baseline.Posted > 0 && data.Flood.Posted > 0,
		"baseline %d/%d failed, flood %d/%d failed", data.Baseline.Failed, data.Baseline.Posted, data.Flood.Failed, data.Flood.Posted)
	res.check("the flood room really flooded", data.Flood.FloodEPS > 0, "%.0f events/s", data.Flood.FloodEPS)
	res.finding("While one room appended %.0f events/s, quiet-room event->delivery p95 went %.1f -> %.1f ms (%.2fx, +%.1f ms); post->delivery p95 %.1f -> %.1f ms (%.2fx); comment POST p95 %.1f -> %.1f ms (%.2fx).",
		data.Flood.FloodEPS, data.Baseline.Delivery.P95MS, data.Flood.Delivery.P95MS, data.P95Ratio, data.P95DeltaMS,
		data.Baseline.EndToEnd.P95MS, data.Flood.EndToEnd.P95MS, data.E2EP95Ratio, data.Baseline.Post.P95MS, data.Flood.Post.P95MS, data.PostP95Ratio)
	return res, nil
}
