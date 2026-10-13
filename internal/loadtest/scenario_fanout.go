package loadtest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func fanoutProfile() Profile {
	return Profile{
		Name: "fanout",
		Help: "subs WebSocket subscribers over rooms rooms while every room gets rate comments/s; event-to-delivery latency, slow-consumer drops, goroutines after close",
		Params: []ParamSpec{
			{Name: "subs", Kind: KindIntList, Full: "500,1000,2000", Short: "12,24", Min: 1, Help: "subscriber counts, one run each"},
			{Name: "rooms", Kind: KindInt, Full: "50", Short: "4", Min: 1},
			{Name: "rate", Kind: KindFloat, Full: "1", Short: "4", Min: 0.01, Help: "comments per second per room"},
			{Name: "window", Kind: KindDuration, Full: "30s", Short: "2s", Min: float64(100 * time.Millisecond), Help: "measured window"},
			{Name: "pad", Kind: KindInt, Full: "300", Short: "50", Min: 0, Help: "comment text bytes"},
			{Name: "warmup", Kind: KindDuration, Full: "3s", Short: "500ms", Help: "unrecorded comment traffic before the window"},
			{Name: "timeout", Kind: KindDuration, Full: "5m", Short: "60s", Min: float64(time.Second)},
		},
		Run: runFanout,
	}
}

// FanoutRun is one subscriber count.
type FanoutRun struct {
	Subs             int           `json:"subs"`
	Rooms            int           `json:"rooms"`
	ConnectMS        float64       `json:"connect_ms"`
	Posted           int64         `json:"comments_posted"`
	PostErrors       int64         `json:"comment_errors"`
	Post             Dist          `json:"comment_post_latency"`
	Delivery         Dist          `json:"event_to_delivery"`
	EndToEnd         Dist          `json:"post_to_delivery"`
	DeliveredPerSec  float64       `json:"deliveries_per_sec"`
	Appends          Dist          `json:"event_insert_latency"`
	Totals           SubTotals     `json:"subs_totals"`
	GoroutinesBefore int           `json:"server_goroutines_before_subs"`
	GoroutinesPeak   int           `json:"server_goroutines_peak"`
	GoroutinesAfter  int           `json:"server_goroutines_after_close"`
	PeakHeap         uint64        `json:"peak_heap_inuse"`
	Pool             PoolStats     `json:"pool"`
	Server           []ServerStats `json:"server"`
}

// FanoutData is the fan-out scenario's output.
type FanoutData struct {
	Runs []FanoutRun `json:"runs"`
}

func (d FanoutData) markdown() string {
	var b strings.Builder
	b.WriteString("## WebSocket fan-out\n\n")
	rows := [][]string{}
	for _, r := range d.Runs {
		rows = append(rows, []string{fmt.Sprint(r.Subs), fmt.Sprint(r.Rooms), fmt.Sprintf("%.0f", r.DeliveredPerSec),
			fms(r.Delivery.P50MS), fms(r.Delivery.P95MS), fms(r.Delivery.P99MS), fms(r.Delivery.MaxMS),
			fms(r.EndToEnd.P50MS), fms(r.EndToEnd.P99MS), fms(r.Post.P50MS), fms(r.Post.P99MS),
			fmt.Sprint(r.Totals.SlowClosed), fmt.Sprint(r.Totals.Reconnects), fmt.Sprintf("%d/%d/%d", r.GoroutinesBefore, r.GoroutinesPeak, r.GoroutinesAfter),
			fmt.Sprintf("%d/%d %.0f%% (%d waits)", r.Pool.MaxAcquired, r.Pool.MaxConns, 100*r.Pool.SaturatedRatio, r.Pool.EmptyAcquires), fmtMiB(r.PeakHeap)})
	}
	b.WriteString(mdTable([]string{"subs", "rooms", "deliveries/s", "event->delivery p50 ms", "p95", "p99", "max", "post->delivery p50", "p99",
		"POST p50", "POST p99", "send-buffer-full closes", "reconnects", "goroutines before/peak/after", "pool", "peak heap"}, rows))
	return b.String()
}

// rig is a set of rooms with subscribers and commenters on one stack.
type rig struct {
	env       *Env
	st        *stack
	subs      []*Sub
	subWG     sync.WaitGroup
	subCtx    context.Context
	subCancel context.CancelFunc
}

func newRig(ctx context.Context, env *Env, st *stack) *rig {
	r := &rig{env: env, st: st}
	r.subCtx, r.subCancel = context.WithCancel(ctx)
	return r
}

// connect adds n subscribers spread round-robin over rooms and waits until
// all are connected (their replay done) or timeout.
func (r *rig) connect(rooms []string, n int, sink *LatencySink, timeout time.Duration) ([]*Sub, error) {
	var added []*Sub
	for i := 0; i < n; i++ {
		s := NewSub(rooms[i%len(rooms)], loadUsers[i%len(loadUsers)], 0, sink)
		s.Reconnect = true
		added = append(added, s)
		r.subs = append(r.subs, s)
		r.subWG.Add(1)
		k := len(r.subs)
		go func() {
			defer r.subWG.Done()
			s.Run(r.subCtx, r.st.cluster.URL(k), r.env.HTTP)
		}()
	}
	deadline := time.Now().Add(timeout)
	for {
		up := 0
		for _, s := range added {
			if s.Connected.Load() {
				up++
			}
		}
		if up == len(added) {
			return added, nil
		}
		if time.Now().After(deadline) {
			return added, fmt.Errorf("loadtest: only %d of %d subscribers connected within %s", up, len(added), timeout)
		}
		if !sleepCtx(r.subCtx, 50*time.Millisecond) {
			return added, r.subCtx.Err()
		}
	}
}

// comment posts rate comments/s in each room for window, with the rooms'
// phases spread over one period. It returns how many were posted and failed.
func (r *rig) comment(ctx context.Context, rooms []string, rate float64, window time.Duration, pad int, post *Recorder) (int64, int64) {
	var posted, failed atomic.Int64
	period := time.Duration(float64(time.Second) / rate)
	end := time.Now().Add(window)
	var wg sync.WaitGroup
	for i, room := range rooms {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !sleepCtx(ctx, period*time.Duration(i)/time.Duration(len(rooms))) {
				return
			}
			t := time.NewTicker(period)
			defer t.Stop()
			for n := 0; time.Now().Before(end); n++ {
				t0 := time.Now()
				if err := r.st.api(i+n).Comment(ctx, loadUsers[n%len(loadUsers)], room, pad); err != nil {
					if ctx.Err() != nil {
						return
					}
					failed.Add(1)
				} else {
					posted.Add(1)
					post.Add(time.Since(t0))
				}
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
			}
		}()
	}
	wg.Wait()
	return posted.Load(), failed.Load()
}

// drain waits until every subscriber has its room's last event, or timeout,
// and returns each room's last seq.
func (r *rig) drain(ctx context.Context, rooms []string, timeout time.Duration) (map[string]int64, error) {
	final := map[string]int64{}
	for _, room := range rooms {
		last, err := r.st.api(0).LastSeq(ctx, loadUsers[0], room)
		if err != nil {
			return nil, err
		}
		final[room] = last
	}
	deadline := time.Now().Add(timeout)
	for {
		behind := 0
		for _, s := range r.subs {
			if s.LastSeq.Load() < final[s.Room] {
				behind++
			}
		}
		if behind == 0 || time.Now().After(deadline) {
			return final, nil
		}
		if !sleepCtx(ctx, 100*time.Millisecond) {
			return final, ctx.Err()
		}
	}
}

// close disconnects every subscriber and waits for their goroutines.
func (r *rig) close() {
	r.subCancel()
	r.subWG.Wait()
}

// quietRooms opens n rooms whose agents end their run at once, and waits for
// those runs so the rooms carry only what the driver posts.
func quietRooms(ctx context.Context, env *Env, st *stack, prefix string, n int, timeout time.Duration) ([]string, error) {
	titles, pipes := make([]string, n), make([]string, n)
	for i := range titles {
		titles[i], pipes[i] = fmt.Sprintf("%s %d %s", prefix, i, ModeQuiet), "pa"
	}
	ids, _, err := st.openRooms(ctx, titles, pipes, 8)
	if err != nil {
		return nil, err
	}
	return ids, env.waitRuns(ctx, ids, timeout)
}

func runFanout(ctx context.Context, env *Env, p Params) (*Result, error) {
	res := &Result{}
	data := FanoutData{}
	for _, n := range p.Ints("subs") {
		run, err := fanoutOnce(ctx, env, p, n, res)
		if err != nil {
			return nil, fmt.Errorf("subs=%d: %w", n, err)
		}
		data.Runs = append(data.Runs, run)
		res.finding("%d subscribers over %d rooms: event->delivery p50 %.1f ms, p95 %.1f ms, p99 %.1f ms; %d send-buffer-full closes, %d reconnects; pool %d/%d busy in %.0f%% of samples with %d waits; server goroutines %d before, %d after close.",
			n, run.Rooms, run.Delivery.P50MS, run.Delivery.P95MS, run.Delivery.P99MS, run.Totals.SlowClosed, run.Totals.Reconnects,
			run.Pool.MaxAcquired, run.Pool.MaxConns, 100*run.Pool.SaturatedRatio, run.Pool.EmptyAcquires, run.GoroutinesBefore, run.GoroutinesAfter)
	}
	res.Data = data
	return res, nil
}

func fanoutOnce(ctx context.Context, env *Env, p Params, n int, res *Result) (FanoutRun, error) {
	st, err := env.newStack(ctx, fmt.Sprintf("fanout-%d", n), ServerConfig{MaxAgents: 3, Pipelines: []string{"pa"}}, nil)
	if err != nil {
		return FanoutRun{}, err
	}
	defer st.close()
	env.Model.Reset(ModelConfig{})
	timeout := p.Dur("timeout")
	rooms, err := quietRooms(ctx, env, st, "fanout", p.Int("rooms"), timeout)
	if err != nil {
		return FanoutRun{}, err
	}
	run := FanoutRun{Subs: n, Rooms: len(rooms)}
	_ = st.cluster.Reset(ctx)
	before, err := st.serverStats(ctx)
	if err != nil {
		return FanoutRun{}, err
	}
	for _, s := range before {
		run.GoroutinesBefore += s.Goroutines
	}

	sink := &LatencySink{Delivery: NewRecorder(0), EndToEnd: NewRecorder(0)}
	sink.Off.Store(true)
	rg := newRig(ctx, env, st)
	t0 := time.Now()
	if _, err := rg.connect(rooms, n, sink, timeout); err != nil {
		rg.close()
		return FanoutRun{}, err
	}
	run.ConnectMS = ms(time.Since(t0))
	sleepCtx(ctx, time.Second) // let the join presence broadcasts settle
	// Warm-up traffic, not recorded: new pool connections, HTTP keep-alives
	// and the first queries' plans would otherwise land in the window.
	rg.comment(ctx, rooms, p.Float("rate"), p.Dur("warmup"), p.Int("pad"), NewRecorder(1))
	if err := st.cluster.Reset(ctx); err != nil {
		rg.close()
		return FanoutRun{}, err
	}
	sink.From.Store(time.Now().UnixNano())
	sink.Off.Store(false)
	post := NewRecorder(0)
	wStart := time.Now()
	run.Posted, run.PostErrors = rg.comment(ctx, rooms, p.Float("rate"), p.Dur("window"), p.Int("pad"), post)
	final, err := rg.drain(ctx, rooms, 15*time.Second)
	wDur := time.Since(wStart)
	sink.Off.Store(true)
	srv, sErr := st.serverStats(ctx)
	rg.close()
	if err != nil {
		return FanoutRun{}, err
	}
	if sErr != nil {
		return FanoutRun{}, sErr
	}
	// The gateway's goroutines end asynchronously after a close; give them a
	// moment, then count.
	var after []ServerStats
	for i := 0; i < 20; i++ {
		sleepCtx(ctx, 250*time.Millisecond)
		if after, err = st.serverStats(ctx); err != nil {
			return FanoutRun{}, err
		}
		g := 0
		for _, s := range after {
			g += s.Goroutines
		}
		run.GoroutinesAfter = g
		if g <= run.GoroutinesBefore+5 {
			break
		}
	}
	run.Totals = Totals(rg.subs, final)
	run.Post = post.Dist()
	run.Delivery = sink.Delivery.Dist()
	run.EndToEnd = sink.EndToEnd.Dist()
	if wDur > 0 {
		run.DeliveredPerSec = round3(float64(run.Delivery.N) / wDur.Seconds())
	}
	run.Server = srv
	for _, s := range srv {
		run.GoroutinesPeak += s.PeakGoroutines
		run.PeakHeap = max(run.PeakHeap, s.PeakHeapInuse)
	}
	run.Appends = srv[0].Appends
	run.Pool = srv[0].Pool

	label := fmt.Sprintf("subs=%d: ", n)
	t := run.Totals
	res.check(label+"every subscriber saw every event once, in order", t.Gaps == 0 && t.Dups == 0 && t.MissingTail == 0 && t.Mismatches == 0,
		"gaps %d dups %d missing-at-end %d parse mismatches %d", t.Gaps, t.Dups, t.MissingTail, t.Mismatches)
	res.check(label+"comments accepted", run.PostErrors == 0 && run.Posted > 0, "posted %d, errors %d", run.Posted, run.PostErrors)
	res.check(label+"no server goroutine leak after close", run.GoroutinesAfter <= run.GoroutinesBefore+5,
		"%d before subscribers, %d after they closed", run.GoroutinesBefore, run.GoroutinesAfter)
	res.check(label+"no append errors", sumAppendErrors(srv) == 0, "append errors %d", sumAppendErrors(srv))
	return run, nil
}
