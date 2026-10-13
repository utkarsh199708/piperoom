package loadtest

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/tools"
)

// ServerEnv is the environment variable that carries a server process's
// ServerConfig as JSON.
const ServerEnv = "LOADTEST_SERVER"

// ReadyPrefix starts the line a server process prints on stdout once it
// listens; the rest of the line is its base URL.
const ReadyPrefix = "LOADTEST_READY "

// ServerConfig configures one server process.
type ServerConfig struct {
	DBURL            string        `json:"db_url"`
	Listen           string        `json:"listen"` // default 127.0.0.1:0
	MaxAgents        int           `json:"max_agents"`
	ToolLatency      time.Duration `json:"tool_latency"`
	ToolSummaryBytes int           `json:"tool_summary_bytes"`
	Pipelines        []string      `json:"pipelines"`
	Replica          int           `json:"replica"`
	// SampleEvery is the pool and memory sampling period; default 50ms.
	SampleEvery time.Duration `json:"sample_every"`
	// LogLevel is debug, info, warn (default) or error.
	LogLevel string `json:"log_level"`
	// MutexProfileFraction > 0 turns on mutex profiling at that rate, read
	// at /debug/pprof/mutex.
	MutexProfileFraction int `json:"mutex_profile_fraction"`
}

// Server is the room service, HTTP API and WebSocket gateway exactly as
// cmd/server assembles them on Postgres, with the read tools and the write
// action replaced by load-test fakes and the model reached through
// ANTHROPIC_BASE_URL (the fake model). It adds /loadtest/* routes for stats.
type Server struct {
	URL string

	cfg    ServerConfig
	log    *slog.Logger
	pool   *pgxpool.Pool
	st     *timedStore
	svc    *room.Service
	api    *httpapi.Server
	http   *http.Server
	ln     net.Listener
	served chan error

	probeRuns atomic.Int64
	actMu     sync.Mutex
	actRuns   map[string]int

	sampler *sampler
}

// timedStore is the Postgres store with the latency of every event append
// recorded. It embeds *store.PG so the room service still finds the shadow
// and purge interfaces on it.
type timedStore struct {
	*store.PG
	rec  *Recorder
	errs atomic.Int64
}

// Append implements events.Store and times the call.
func (t *timedStore) Append(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	start := time.Now()
	e, err := t.PG.Append(ctx, roomID, typ, actor, payload)
	t.rec.Add(time.Since(start))
	if err != nil {
		t.errs.Add(1)
	}
	return e, err
}

// checkModelURL refuses a model endpoint that is not on this machine, so a
// load run can never send its traffic (or a real key) to a real model API.
func checkModelURL(raw string) error {
	if raw == "" {
		return errors.New("loadtest: ANTHROPIC_BASE_URL is not set; the server must talk to the fake model")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("loadtest: ANTHROPIC_BASE_URL: %w", err)
	}
	host := u.Hostname()
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return fmt.Errorf("loadtest: ANTHROPIC_BASE_URL %q is not a loopback address; refusing to load a real model API", raw)
	}
	return nil
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "error":
		return slog.LevelError
	}
	return slog.LevelWarn
}

// StartServer assembles and starts one server. The caller must Close it.
func StartServer(ctx context.Context, cfg ServerConfig, log *slog.Logger) (*Server, error) {
	if err := checkModelURL(os.Getenv("ANTHROPIC_BASE_URL")); err != nil {
		return nil, err
	}
	if cfg.DBURL == "" {
		return nil, errors.New("loadtest: server: empty db_url")
	}
	if len(cfg.Pipelines) == 0 {
		return nil, errors.New("loadtest: server: no pipelines")
	}
	if cfg.Listen == "" {
		cfg.Listen = "127.0.0.1:0"
	}
	if cfg.SampleEvery <= 0 {
		cfg.SampleEvery = 50 * time.Millisecond
	}
	if log == nil {
		log = slog.Default()
	}

	// The pool is built the way store.New builds it (pgxpool.New on the same
	// URL, so the same default size), but kept here so its Stat can be read.
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(pctx, cfg.DBURL)
	if err != nil {
		return nil, fmt.Errorf("loadtest: server: pool: %w", err)
	}
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("loadtest: server: ping: %w", err)
	}
	pg := store.NewPool(pool, cfg.DBURL, log)
	ts := &timedStore{PG: pg, rec: NewRecorder(0)}

	s := &Server{cfg: cfg, log: log, pool: pool, st: ts, actRuns: map[string]int{}, served: make(chan error, 1)}

	toolsReg := tools.NewRegistry()
	if err := toolsReg.Register(probeTool{latency: cfg.ToolLatency, summaryBytes: cfg.ToolSummaryBytes, runs: &s.probeRuns}); err != nil {
		pg.Close()
		return nil, fmt.Errorf("loadtest: server: register probe: %w", err)
	}
	acts := actions.NewRegistry()
	if err := acts.Register(noopAction{mu: &s.actMu, runs: s.actRuns}); err != nil {
		pg.Close()
		return nil, fmt.Errorf("loadtest: server: register action: %w", err)
	}
	// The same endpoint cmd/server builds (wiring.NewAgentEndpoint), with no
	// per-room token cap so a long run is never stopped by the budget.
	ep, err := agent.NewEndpoint(ctx, agent.EndpointConfig{Provider: agent.ProviderAnthropic, RoomTokenBudget: "0"}, log)
	if err != nil {
		pg.Close()
		return nil, fmt.Errorf("loadtest: server: model endpoint: %w", err)
	}
	pipes := make([]owners.Pipeline, 0, len(cfg.Pipelines))
	for _, id := range cfg.Pipelines {
		pipes = append(pipes, owners.Pipeline{ID: id, SourceTables: []string{"public." + id}})
	}
	s.svc = room.New(room.Deps{
		Store:        ts,
		Actions:      acts,
		Tools:        toolsReg,
		Users:        room.DefaultUsers(),
		Owners:       &owners.Owners{Pipelines: pipes},
		Run:          ep.Run,
		WriteHandoff: ep.WriteHandoff,
		Options:      agent.Options{MaxTurns: 40},
		Logger:       log,
		MaxAgents:    cfg.MaxAgents,
		Sessions:     pg.Sessions(),
		OnRoomClosed: func(_ context.Context, roomID, _, _ string) { ep.Forget(roomID) },
	})
	lctx, lcancel := context.WithTimeout(ctx, 60*time.Second)
	err = s.svc.Load(lctx)
	lcancel()
	if err != nil {
		s.svc.Close()
		pg.Close()
		return nil, fmt.Errorf("loadtest: server: load rooms: %w", err)
	}
	s.api = httpapi.New(s.svc, ts, log, httpapi.WithReady(ts), httpapi.WithSessions(pg.Sessions()),
		httpapi.WithLoginStates(pg.LoginStates()), httpapi.WithIdentities(pg.Identities()))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /loadtest/stats", s.handleStats)
	mux.HandleFunc("POST /loadtest/reset", s.handleReset)
	mux.HandleFunc("POST /loadtest/gc", func(w http.ResponseWriter, _ *http.Request) {
		runtime.GC()
		w.WriteHeader(http.StatusNoContent)
	})
	// Profiles of the server under load (loopback only; a load-test server).
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	if cfg.MutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(cfg.MutexProfileFraction)
	}
	mux.Handle("/", s.api)

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		s.api.Close()
		pg.Close()
		return nil, fmt.Errorf("loadtest: server: listen: %w", err)
	}
	s.ln = ln
	s.URL = "http://" + ln.Addr().String()
	s.http = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	s.sampler = newSampler(pool, cfg.SampleEvery)
	go func() { s.served <- s.http.Serve(ln) }()
	return s, nil
}

// Close shuts the server down the way cmd/server does: HTTP shutdown, then
// the WebSocket gateway and room service, then the store.
func (s *Server) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = s.http.Shutdown(ctx)
	<-s.served
	s.api.Close()
	s.sampler.stop()
	s.st.PG.Close()
}

// PoolStats is the pgxpool state: gauges now, counters and sampler figures
// since the last reset.
type PoolStats struct {
	MaxConns       int32   `json:"max_conns"`
	TotalConns     int32   `json:"total_conns"`
	AcquiredConns  int32   `json:"acquired_conns"`
	IdleConns      int32   `json:"idle_conns"`
	Acquires       int64   `json:"acquires"`
	EmptyAcquires  int64   `json:"empty_acquires"` // acquires that had to wait for a connection
	CanceledAcq    int64   `json:"canceled_acquires"`
	EmptyWaitMS    float64 `json:"empty_wait_ms"` // total time spent waiting in those
	AcquireMS      float64 `json:"acquire_ms"`
	Samples        int     `json:"samples"`
	SamplesAtMax   int     `json:"samples_at_max"` // samples with every connection in use
	MaxAcquired    int32   `json:"max_acquired"`
	SaturatedRatio float64 `json:"saturated_ratio"`
}

// ServerStats is what GET /loadtest/stats returns.
type ServerStats struct {
	Replica        int            `json:"replica"`
	PID            int            `json:"pid"`
	Goroutines     int            `json:"goroutines"`
	PeakGoroutines int            `json:"peak_goroutines"`
	HeapInuse      uint64         `json:"heap_inuse"`
	PeakHeapInuse  uint64         `json:"peak_heap_inuse"`
	HeapAlloc      uint64         `json:"heap_alloc"`
	Sys            uint64         `json:"sys"`
	TotalAlloc     uint64         `json:"total_alloc"`
	NumGC          uint32         `json:"num_gc"`
	RSS            uint64         `json:"rss"`
	Pool           PoolStats      `json:"pool"`
	Appends        Dist           `json:"appends"`
	AppendErrors   int64          `json:"append_errors"`
	AppendsDropped int64          `json:"appends_dropped"`
	ProbeRuns      int64          `json:"probe_runs"`
	ActionRuns     map[string]int `json:"action_runs"`
	CPUSeconds     float64        `json:"cpu_seconds"`
}

func (s *Server) stats() ServerStats {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	samples, dropped := s.st.rec.Snapshot()
	s.actMu.Lock()
	acts := make(map[string]int, len(s.actRuns))
	for k, v := range s.actRuns {
		acts[k] = v
	}
	s.actMu.Unlock()
	ps, peakHeap, peakG := s.sampler.read()
	return ServerStats{
		Replica:        s.cfg.Replica,
		PID:            os.Getpid(),
		Goroutines:     runtime.NumGoroutine(),
		PeakGoroutines: peakG,
		HeapInuse:      ms.HeapInuse,
		PeakHeapInuse:  peakHeap,
		HeapAlloc:      ms.HeapAlloc,
		Sys:            ms.Sys,
		TotalAlloc:     ms.TotalAlloc,
		NumGC:          ms.NumGC,
		RSS:            readProcRSS(),
		Pool:           ps,
		Appends:        Summarize(samples),
		AppendErrors:   s.st.errs.Load(),
		AppendsDropped: dropped,
		ProbeRuns:      s.probeRuns.Load(),
		ActionRuns:     acts,
		CPUSeconds:     processCPUSeconds(),
	}
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.stats())
}

func (s *Server) handleReset(w http.ResponseWriter, _ *http.Request) {
	s.st.rec.Reset()
	s.st.errs.Store(0)
	s.sampler.reset()
	w.WriteHeader(http.StatusNoContent)
}

// sampler reads the pool and the heap every period, keeping peaks and the
// share of samples where the pool had no free connection.
type sampler struct {
	pool   *pgxpool.Pool
	cancel context.CancelFunc
	done   chan struct{}

	mu       sync.Mutex
	base     *pgxpool.Stat
	samples  int
	atMax    int
	maxAcq   int32
	peakHeap uint64
	peakG    int
}

func newSampler(pool *pgxpool.Pool, every time.Duration) *sampler {
	ctx, cancel := context.WithCancel(context.Background())
	sp := &sampler{pool: pool, cancel: cancel, done: make(chan struct{}), base: pool.Stat()}
	go func() {
		defer close(sp.done)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			st := pool.Stat()
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			g := runtime.NumGoroutine()
			sp.mu.Lock()
			sp.samples++
			if st.AcquiredConns() >= st.MaxConns() {
				sp.atMax++
			}
			sp.maxAcq = max(sp.maxAcq, st.AcquiredConns())
			sp.peakHeap = max(sp.peakHeap, ms.HeapInuse)
			sp.peakG = max(sp.peakG, g)
			sp.mu.Unlock()
		}
	}()
	return sp
}

func (sp *sampler) stop() {
	sp.cancel()
	<-sp.done
}

func (sp *sampler) reset() {
	st := sp.pool.Stat()
	sp.mu.Lock()
	sp.base = st
	sp.samples, sp.atMax, sp.maxAcq, sp.peakHeap, sp.peakG = 0, 0, 0, 0, 0
	sp.mu.Unlock()
}

func (sp *sampler) read() (PoolStats, uint64, int) {
	st := sp.pool.Stat()
	sp.mu.Lock()
	defer sp.mu.Unlock()
	b := sp.base
	ps := PoolStats{
		MaxConns:      st.MaxConns(),
		TotalConns:    st.TotalConns(),
		AcquiredConns: st.AcquiredConns(),
		IdleConns:     st.IdleConns(),
		Acquires:      st.AcquireCount() - b.AcquireCount(),
		EmptyAcquires: st.EmptyAcquireCount() - b.EmptyAcquireCount(),
		CanceledAcq:   st.CanceledAcquireCount() - b.CanceledAcquireCount(),
		EmptyWaitMS:   ms(st.EmptyAcquireWaitTime() - b.EmptyAcquireWaitTime()),
		AcquireMS:     ms(st.AcquireDuration() - b.AcquireDuration()),
		Samples:       sp.samples,
		SamplesAtMax:  sp.atMax,
		MaxAcquired:   sp.maxAcq,
	}
	if sp.samples > 0 {
		ps.SaturatedRatio = round3(float64(sp.atMax) / float64(sp.samples))
	}
	return ps, sp.peakHeap, sp.peakG
}

// readProcRSS reads VmRSS of this process from /proc (Linux); 0 elsewhere.
func readProcRSS() uint64 {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, "VmRSS:"); ok {
			if f := strings.Fields(v); len(f) >= 1 {
				if kb, err := strconv.ParseUint(f[0], 10, 64); err == nil {
					return kb * 1024
				}
			}
		}
	}
	return 0
}

// ServeMain is the body of a server process (`loadtest serve`, or a test
// binary re-executed by the launcher). It reads the config from ServerEnv,
// prints the ready line on stdout and runs until SIGINT/SIGTERM or until its
// stdin closes, so a server never outlives the driver that started it.
func ServeMain(stdin io.Reader, stdout io.Writer) error {
	var cfg ServerConfig
	if err := json.Unmarshal([]byte(os.Getenv(ServerEnv)), &cfg); err != nil {
		return fmt.Errorf("loadtest: serve: %s: %w", ServerEnv, err)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: parseLevel(cfg.LogLevel)}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		// EOF (or any error) on stdin means the driver is gone.
		_, _ = io.Copy(io.Discard, bufio.NewReader(stdin))
		stop()
	}()
	srv, err := StartServer(ctx, cfg, log)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(stdout, "%s%s\n", ReadyPrefix, srv.URL); err != nil {
		srv.Close()
		return fmt.Errorf("loadtest: serve: write ready line: %w", err)
	}
	log.Info("loadtest: server ready", "url", srv.URL, "replica", cfg.Replica, "max_agents", cfg.MaxAgents)
	select {
	case <-ctx.Done():
	case err := <-srv.served:
		srv.served <- err // Close waits for it
		log.Error("loadtest: serve stopped", "err", err)
	}
	srv.Close()
	return nil
}
