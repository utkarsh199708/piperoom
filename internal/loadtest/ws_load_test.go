//go:build load

package loadtest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// loadConfig is read from the environment so one test covers quick smoke runs
// and the full 60 s run in the report.
type loadConfig struct {
	rooms         int           // LOAD_ROOMS
	clients       int           // LOAD_CLIENTS, WebSocket clients per room
	rate          float64       // LOAD_RATE, agent events per second per room
	duration      time.Duration // LOAD_DURATION, measured window
	commentEvery  time.Duration // LOAD_COMMENT_EVERY, one comment per room per interval
	proposalEvery int           // LOAD_PROPOSAL_EVERY, agent proposes after every N events
	padBytes      int           // LOAD_PAD_BYTES, filler in each agent message
	burst         bool          // LOAD_BURST=1: every agent ticks at the same instant (worst case)
	heapProfile   string        // LOAD_HEAP_PROFILE: write a heap profile here at the end of the window
}

func envInt(t *testing.T, key string, def int) int {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("%s: want a positive integer, got %q", key, v)
	}
	return n
}

func envDur(t *testing.T, key string, def time.Duration) time.Duration {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		t.Fatalf("%s: want a positive duration, got %q", key, v)
	}
	return d
}

func readConfig(t *testing.T) loadConfig {
	t.Helper()
	rate := 5.0
	if v := os.Getenv("LOAD_RATE"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f <= 0 {
			t.Fatalf("LOAD_RATE: want a positive number, got %q", v)
		}
		rate = f
	}
	return loadConfig{
		rooms:         envInt(t, "LOAD_ROOMS", 50),
		clients:       envInt(t, "LOAD_CLIENTS", 20),
		rate:          rate,
		duration:      envDur(t, "LOAD_DURATION", 60*time.Second),
		commentEvery:  envDur(t, "LOAD_COMMENT_EVERY", 2*time.Second),
		proposalEvery: envInt(t, "LOAD_PROPOSAL_EVERY", 50),
		padBytes:      envInt(t, "LOAD_PAD_BYTES", 300),
		burst:         os.Getenv("LOAD_BURST") == "1",
		heapProfile:   os.Getenv("LOAD_HEAP_PROFILE"),
	}
}

// loadAction is a scoped no-op write action so proposals can be approved and
// executed (action.executed events) without touching anything.
type loadAction struct{}

func (loadAction) Name() string                                      { return "load_noop" }
func (loadAction) Layer() string                                     { return "postgres" }
func (loadAction) Describe() string                                  { return "no-op action for the load test" }
func (loadAction) ParamsSchema() map[string]any                      { return map[string]any{"type": "object"} }
func (loadAction) Validate(json.RawMessage) error                    { return nil }
func (loadAction) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }
func (loadAction) Execute(context.Context, json.RawMessage) (string, error) {
	return "ok", nil
}

// harness holds the shared state of one run.
type harness struct {
	cfg     loadConfig
	baseURL string
	http    *http.Client

	start   chan struct{} // closed when every client is connected
	stop    chan struct{} // closed at the end of the measured window
	measure atomic.Bool   // true while latencies are recorded

	proposals chan [2]string // {roomID, proposalID} for the approver
	agents    atomic.Int64   // agent runs started, used to stagger their ticks

	emitted     atomic.Int64 // agent events appended during the window
	emitErrs    atomic.Int64
	propCreated atomic.Int64
	propErrs    atomic.Int64
	comments    atomic.Int64
	commentErrs atomic.Int64
	approvals   atomic.Int64
	approveErrs atomic.Int64
}

// latencyMarker prefixes the emit time (unix ns) in agent messages.
const latencyMarker = "lt:"

// run is the fake agent: rate events per second through the real Sink, a
// proposal every proposalEvery events, and it drains its inbox like the real
// loop does.
func (h *harness) run(ctx context.Context, alert agent.Alert, deps agent.Deps, _ agent.Options) error {
	select {
	case <-h.start:
	case <-ctx.Done():
		return nil
	}
	pad := strings.Repeat("x", h.cfg.padBytes)
	period := time.Duration(float64(time.Second) / h.cfg.rate)
	if !h.cfg.burst {
		// Real rooms are not phase-locked: spread the agents evenly over one
		// period so the test measures fan-out, not a synthetic thundering herd.
		idx := (h.agents.Add(1) - 1) % int64(h.cfg.rooms)
		select {
		case <-time.After(period * time.Duration(idx) / time.Duration(h.cfg.rooms)):
		case <-ctx.Done():
			return nil
		}
	}
	tick := time.NewTicker(period)
	defer tick.Stop()
	n := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-h.stop:
			<-ctx.Done()
			return nil
		case <-tick.C:
		}
		n++
		if _, err := deps.Inbox.Drain(ctx); err != nil && ctx.Err() == nil {
			h.emitErrs.Add(1)
		}
		if n%h.cfg.proposalEvery == 0 {
			pid, err := deps.Proposals.Create(ctx, events.ProposalCreatedPayload{
				Title:    "load proposal " + strconv.Itoa(n),
				Steps:    []events.ProposalStep{{Action: "load_noop", Layer: "postgres", Params: json.RawMessage(`{"n":1}`)}},
				Risk:     "no-op step in the load test; nothing changes",
				Rollback: "nothing to roll back; the step changes nothing",
			})
			if err != nil {
				h.propErrs.Add(1)
			} else {
				h.propCreated.Add(1)
				select {
				case h.proposals <- [2]string{alert.RoomID, pid}:
				default:
					h.propErrs.Add(1)
				}
			}
			continue
		}
		text := latencyMarker + strconv.FormatInt(time.Now().UnixNano(), 10) + " " + pad
		if err := deps.Sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: text}); err != nil {
			if ctx.Err() == nil {
				h.emitErrs.Add(1)
			}
			continue
		}
		h.emitted.Add(1)
	}
}

func (h *harness) post(ctx context.Context, path, user, body string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.baseURL+path, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-User", user)
	req.Header.Set("Content-Type", "application/json")
	return h.http.Do(req)
}

func (h *harness) getJSON(ctx context.Context, path, user string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-User", user)
	resp, err := h.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: status %d", path, resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(dst)
}

func drainClose(resp *http.Response) int {
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}

// commenter posts one comment per commentEvery in one room, rotating users.
func (h *harness) commenter(ctx context.Context, roomID string, users []string) {
	t := time.NewTicker(h.cfg.commentEvery)
	defer t.Stop()
	i := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.stop:
			return
		case <-t.C:
		}
		i++
		body := fmt.Sprintf(`{"text":"load comment %d"}`, i)
		resp, err := h.post(ctx, "/api/rooms/"+roomID+"/comments", users[i%len(users)], body)
		if err != nil || drainClose(resp) != http.StatusOK {
			h.commentErrs.Add(1)
			continue
		}
		h.comments.Add(1)
	}
}

// approver approves every proposal the agents create, as the postgres owner,
// with the params_hash read from the room snapshot (like the UI does).
func (h *harness) approver(ctx context.Context) {
	for {
		var rp [2]string
		select {
		case <-ctx.Done():
			return
		case rp = <-h.proposals:
		}
		var snap struct {
			OpenProposals []struct {
				ProposalID string `json:"proposal_id"`
				ParamsHash string `json:"params_hash"`
			} `json:"open_proposals"`
		}
		if err := h.getJSON(ctx, "/api/rooms/"+rp[0], "priya", &snap); err != nil {
			h.approveErrs.Add(1)
			continue
		}
		hash := ""
		for _, p := range snap.OpenProposals {
			if p.ProposalID == rp[1] {
				hash = p.ParamsHash
			}
		}
		body := fmt.Sprintf(`{"layer":"postgres","params_hash":%q}`, hash)
		resp, err := h.post(ctx, "/api/proposals/"+rp[1]+"/approve", "priya", body)
		if err != nil || drainClose(resp) != http.StatusOK {
			h.approveErrs.Add(1)
			continue
		}
		h.approvals.Add(1)
	}
}

// clientStats is what one WebSocket client saw.
type clientStats struct {
	roomID      string
	lastSeq     atomic.Int64 // written by the client goroutine, read by the end check
	received    int64
	gaps        int64 // seqs skipped
	dups        int64 // seqs seen again or out of order
	reconnects  int64
	presence    int64
	latenciesNs []int64
}

type wireMsg struct {
	Type  string `json:"type"`
	Event *struct {
		Seq     int64           `json:"seq"`
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	} `json:"event"`
}

// wsClient keeps one connection open until ctx ends, reconnecting with
// after=lastSeq when the server drops it, and checks seq order.
func (h *harness) wsClient(ctx context.Context, roomID, user string, ready *sync.WaitGroup, cs *clientStats) {
	signalled := false
	signal := func() {
		if !signalled {
			signalled = true
			ready.Done()
		}
	}
	defer signal()
	for ctx.Err() == nil {
		u := strings.Replace(h.baseURL, "http://", "ws://", 1) + "/api/rooms/" + roomID + "/ws?after=" + strconv.FormatInt(cs.lastSeq.Load(), 10)
		conn, _, err := websocket.Dial(ctx, u, &websocket.DialOptions{
			HTTPClient: h.http,
			HTTPHeader: http.Header{"X-User": []string{user}},
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			cs.reconnects++
			time.Sleep(100 * time.Millisecond)
			continue
		}
		conn.SetReadLimit(1 << 20)
		signal()
		h.readConn(ctx, conn, cs)
		_ = conn.CloseNow()
		if ctx.Err() == nil {
			cs.reconnects++
		}
	}
}

func (h *harness) readConn(ctx context.Context, conn *websocket.Conn, cs *clientStats) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Presence keeps the server's 60 s read deadline from closing us.
	go func() {
		t := time.NewTicker(20 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-cctx.Done():
				return
			case <-t.C:
				wctx, wcancel := context.WithTimeout(cctx, 5*time.Second)
				_ = conn.Write(wctx, websocket.MessageText, []byte(`{"type":"presence","state":"viewing"}`))
				wcancel()
			}
		}
	}()
	for {
		_, data, err := conn.Read(cctx)
		if err != nil {
			return
		}
		now := time.Now().UnixNano()
		var m wireMsg
		if err := json.Unmarshal(data, &m); err != nil {
			continue
		}
		if m.Type == "presence" {
			cs.presence++
			continue
		}
		if m.Type != "event" || m.Event == nil {
			continue
		}
		cs.received++
		seq, last := m.Event.Seq, cs.lastSeq.Load()
		switch {
		case seq == last+1:
		case seq <= last:
			cs.dups++
			continue
		default:
			cs.gaps += seq - last - 1
		}
		cs.lastSeq.Store(seq)
		if m.Event.Type != string(events.AgentMessage) || !h.measure.Load() {
			continue
		}
		var p struct {
			Text string `json:"text"`
		}
		if json.Unmarshal(m.Event.Payload, &p) != nil || !strings.HasPrefix(p.Text, latencyMarker) {
			continue
		}
		ts, _, _ := strings.Cut(p.Text[len(latencyMarker):], " ")
		sent, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			continue
		}
		cs.latenciesNs = append(cs.latenciesNs, now-sent)
	}
}

// resourceSample is one sampler reading.
type resourceSample struct {
	goroutines int
	heapInuse  uint64
	rssBytes   uint64
}

func sample() resourceSample {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return resourceSample{goroutines: runtime.NumGoroutine(), heapInuse: ms.HeapInuse, rssBytes: readRSS()}
}

// readRSS reads VmRSS from /proc (Linux); 0 elsewhere.
func readRSS() uint64 {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, "VmRSS:"); ok {
			f := strings.Fields(v)
			if len(f) >= 1 {
				kb, err := strconv.ParseUint(f[0], 10, 64)
				if err == nil {
					return kb * 1024
				}
			}
		}
	}
	return 0
}

func pct(sorted []int64, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return time.Duration(sorted[i])
}

func mib(b uint64) string { return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20)) }

// TestWSFanoutLoad runs the full stack in process: memstore, room service,
// httpapi router with the WebSocket gateway over httptest, a fake agent per
// room, commenters, one approver and rooms*clients WebSocket clients.
func TestWSFanoutLoad(t *testing.T) {
	cfg := readConfig(t)
	t.Logf("config: rooms=%d clients/room=%d rate=%.1f ev/s/room duration=%s comment every %s proposal every %d events pad=%dB burst=%v GOMAXPROCS=%d",
		cfg.rooms, cfg.clients, cfg.rate, cfg.duration, cfg.commentEvery, cfg.proposalEvery, cfg.padBytes, cfg.burst, runtime.GOMAXPROCS(0))

	base := sample()
	quiet := slog.New(slog.DiscardHandler)
	st := memstore.New()
	reg := actions.NewRegistry()
	if err := reg.Register(loadAction{}); err != nil {
		t.Fatalf("register action: %v", err)
	}
	h := &harness{
		cfg:       cfg,
		start:     make(chan struct{}),
		stop:      make(chan struct{}),
		proposals: make(chan [2]string, 4*cfg.rooms),
	}
	svc := room.New(room.Deps{
		Store:   st,
		Actions: reg,
		Users:   room.DefaultUsers(),
		Owners:  &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}}},
		Run:     h.run,
		WriteHandoff: func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{WhatBroke: "n/a"}, nil
		},
		Logger:    quiet,
		MaxAgents: cfg.rooms + 1,
	})
	api := httpapi.New(svc, st, quiet)
	srv := httptest.NewServer(api)
	h.baseURL = srv.URL
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 0
	tr.MaxIdleConnsPerHost = 256
	h.http = &http.Client{Transport: tr, Timeout: 30 * time.Second}

	users := []string{"arjun", "priya", "meera", "kabir"}
	ctx, cancel := context.WithCancel(context.Background())
	var bg sync.WaitGroup
	shutdown := func() {
		cancel()
		bg.Wait()
		api.Close()
		srv.Close()
	}

	// Rooms.
	roomIDs := make([]string, cfg.rooms)
	for i := range roomIDs {
		resp, err := h.post(ctx, "/api/rooms", "arjun", fmt.Sprintf(`{"title":"load %d","severity":"sev2","pipeline_id":"orders"}`, i))
		if err != nil {
			shutdown()
			t.Fatalf("create room: %v", err)
		}
		var snap struct {
			ID string `json:"id"`
		}
		err = json.NewDecoder(resp.Body).Decode(&snap)
		code := drainClose(resp)
		if err != nil || code != http.StatusCreated || snap.ID == "" {
			shutdown()
			t.Fatalf("create room: status %d err %v", code, err)
		}
		roomIDs[i] = snap.ID
	}

	// Clients.
	stats := make([]*clientStats, 0, cfg.rooms*cfg.clients)
	var ready sync.WaitGroup
	connectStart := time.Now()
	for _, id := range roomIDs {
		for j := 0; j < cfg.clients; j++ {
			cs := &clientStats{roomID: id}
			stats = append(stats, cs)
			ready.Add(1)
			bg.Add(1)
			go func(user string) {
				defer bg.Done()
				h.wsClient(ctx, id, user, &ready, cs)
			}(users[j%len(users)])
		}
	}
	ready.Wait()
	connectDur := time.Since(connectStart)
	idle := sample()

	// Load phase.
	bg.Add(1)
	go func() {
		defer bg.Done()
		h.approver(ctx)
	}()
	for _, id := range roomIDs {
		bg.Add(1)
		go func() {
			defer bg.Done()
			h.commenter(ctx, id, users)
		}()
	}
	var peak resourceSample
	var peakMu sync.Mutex
	sampDone := make(chan struct{})
	go func() {
		defer close(sampDone)
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		for {
			select {
			case <-h.stop:
				return
			case <-tk.C:
			}
			s := sample()
			peakMu.Lock()
			peak.goroutines = max(peak.goroutines, s.goroutines)
			peak.heapInuse = max(peak.heapInuse, s.heapInuse)
			peak.rssBytes = max(peak.rssBytes, s.rssBytes)
			peakMu.Unlock()
		}
	}()
	h.measure.Store(true)
	loadStart := time.Now()
	close(h.start)
	time.Sleep(cfg.duration)
	if cfg.heapProfile != "" {
		if f, err := os.Create(cfg.heapProfile); err != nil {
			t.Errorf("heap profile: %v", err)
		} else {
			if err := pprof.WriteHeapProfile(f); err != nil {
				t.Errorf("heap profile: %v", err)
			}
			_ = f.Close()
		}
	}
	close(h.stop)
	h.measure.Store(false)
	loadDur := time.Since(loadStart)
	<-sampDone
	end := sample()

	// Let in-flight events and executor steps reach every client, then
	// compare each client's last seq with its room's final seq.
	finalSeq := map[string]int64{}
	time.Sleep(500 * time.Millisecond)
	for deadline := time.Now().Add(15 * time.Second); ; {
		for _, id := range roomIDs {
			r, err := st.GetRoom(context.Background(), id)
			if err != nil {
				t.Errorf("get room %s: %v", id, err)
			}
			finalSeq[id] = r.LastSeq
		}
		behind := 0
		for _, cs := range stats {
			if cs.lastSeq.Load() < finalSeq[cs.roomID] {
				behind++
			}
		}
		if behind == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	shutdown()
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	after := sample()

	// Aggregate.
	var all []int64
	var gaps, dups, reconnects, received, tail, presence int64
	for _, cs := range stats {
		all = append(all, cs.latenciesNs...)
		gaps += cs.gaps
		dups += cs.dups
		reconnects += cs.reconnects
		received += cs.received
		presence += cs.presence
		if d := finalSeq[cs.roomID] - cs.lastSeq.Load(); d > 0 {
			tail += d
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	var totalEvents int64
	for _, s := range finalSeq {
		totalEvents += s
	}

	var b bytes.Buffer
	fmt.Fprintf(&b, "\n=== WebSocket fan-out load test ===\n")
	fmt.Fprintf(&b, "rooms=%d clients/room=%d (=%d sockets) rate=%.1f ev/s/room window=%s connect=%s\n",
		cfg.rooms, cfg.clients, len(stats), cfg.rate, loadDur.Round(time.Millisecond), connectDur.Round(time.Millisecond))
	fmt.Fprintf(&b, "agent events emitted=%d (%.0f/s) emitErrs=%d proposals=%d propErrs=%d approvals=%d approveErrs=%d comments=%d commentErrs=%d\n",
		h.emitted.Load(), float64(h.emitted.Load())/loadDur.Seconds(), h.emitErrs.Load(), h.propCreated.Load(), h.propErrs.Load(),
		h.approvals.Load(), h.approveErrs.Load(), h.comments.Load(), h.commentErrs.Load())
	fmt.Fprintf(&b, "room log events total=%d; client event frames=%d (%.0f/s) presence frames=%d\n",
		totalEvents, received, float64(received)/loadDur.Seconds(), presence)
	fmt.Fprintf(&b, "fan-out latency (emit -> client receive, agent.message, n=%d): p50=%s p90=%s p99=%s p99.9=%s max=%s\n",
		len(all), pct(all, 50), pct(all, 90), pct(all, 99), pct(all, 99.9), pct(all, 100))
	fmt.Fprintf(&b, "seq check: gaps=%d dups=%d reconnects=%d missing-at-end=%d\n", gaps, dups, reconnects, tail)
	fmt.Fprintf(&b, "goroutines: before=%d connected-idle=%d peak=%d end-of-load=%d after-shutdown=%d\n",
		base.goroutines, idle.goroutines, peak.goroutines, end.goroutines, after.goroutines)
	fmt.Fprintf(&b, "heap in use: before=%s connected-idle=%s peak=%s end-of-load=%s after-shutdown+GC=%s\n",
		mib(base.heapInuse), mib(idle.heapInuse), mib(peak.heapInuse), mib(end.heapInuse), mib(after.heapInuse))
	fmt.Fprintf(&b, "RSS: before=%s connected-idle=%s peak=%s end-of-load=%s\n",
		mib(base.rssBytes), mib(idle.rssBytes), mib(peak.rssBytes), mib(end.rssBytes))
	t.Log(b.String())

	if gaps != 0 || dups != 0 {
		t.Errorf("seq must be gap-free and unique per client: gaps=%d dups=%d", gaps, dups)
	}
	if tail != 0 {
		t.Errorf("%d events never reached their clients", tail)
	}
	if h.emitted.Load() == 0 || len(all) == 0 {
		t.Errorf("no events measured")
	}
	if after.goroutines > base.goroutines+10 {
		t.Errorf("goroutine leak: %d before, %d after shutdown", base.goroutines, after.goroutines)
	}
}
