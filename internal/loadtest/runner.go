package loadtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
)

// Profile is one named scenario with its parameters.
type Profile struct {
	Name   string
	Help   string
	Params []ParamSpec
	Run    func(ctx context.Context, env *Env, p Params) (*Result, error)
}

// Profiles returns the built-in scenario profiles in report order.
func Profiles() []Profile {
	return []Profile{fairnessProfile(), throughputProfile(), replayProfile(), fanoutProfile(), isolationProfile()}
}

// SelectProfiles parses "all" or a comma-separated list of profile names,
// keeping the order given.
func SelectProfiles(spec string) ([]Profile, error) {
	all := Profiles()
	if strings.TrimSpace(spec) == "all" {
		return all, nil
	}
	var out []Profile
	for _, name := range strings.Split(spec, ",") {
		name = strings.TrimSpace(name)
		i := slices.IndexFunc(all, func(p Profile) bool { return p.Name == name })
		if i < 0 {
			names := make([]string, 0, len(all))
			for _, p := range all {
				names = append(names, p.Name)
			}
			return nil, fmt.Errorf("loadtest: unknown profile %q (known: all, %s)", name, strings.Join(names, ", "))
		}
		out = append(out, all[i])
	}
	if len(out) == 0 {
		return nil, errors.New("loadtest: no profile selected")
	}
	return out, nil
}

// Check is one invariant a run verified (seq order, the agent cap, ...).
type Check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

// Result is one profile run.
type Result struct {
	Profile  string            `json:"profile"`
	Short    bool              `json:"short"`
	Replicas int               `json:"replicas"`
	Params   map[string]string `json:"params"`
	Started  time.Time         `json:"started"`
	WallS    float64           `json:"wall_s"`
	Data     any               `json:"data"`
	// Findings are plain statements about what the numbers show, such as
	// what broke first. They describe; they never assert a target.
	Findings []string `json:"findings"`
	Checks   []Check  `json:"checks"`
	// LoadAvg is /proc/loadavg at the start and at the end: the box is
	// shared, so a reader must see how busy it was.
	LoadAvg  [2]string `json:"loadavg_start_end"`
	Markdown string    `json:"-"`
}

func (r *Result) check(name string, ok bool, format string, args ...any) {
	r.Checks = append(r.Checks, Check{Name: name, OK: ok, Detail: fmt.Sprintf(format, args...)})
}

func (r *Result) finding(format string, args ...any) {
	r.Findings = append(r.Findings, fmt.Sprintf(format, args...))
}

// Failed lists the checks that did not pass.
func (r *Result) Failed() []Check {
	var out []Check
	for _, c := range r.Checks {
		if !c.OK {
			out = append(out, c)
		}
	}
	return out
}

// Env is what every scenario runs on: the database server, the fake model
// and the launcher for server processes.
type Env struct {
	PG       *Postgres
	Model    *FakeModel
	ModelURL string
	Launcher Launcher
	Replicas int
	Short    bool
	Log      *slog.Logger
	// MutexProfile is passed to every server (ServerConfig.MutexProfileFraction).
	MutexProfile int
	// ServerDBParams are appended to each server's database URL.
	ServerDBParams string
	// PoolMaxConns is ServerConfig.PoolMaxConns for every server.
	PoolMaxConns int
	HTTP         *http.Client

	modelSrv *http.Server
	modelLn  net.Listener
}

// EnvConfig configures NewEnv.
type EnvConfig struct {
	// DBURL is an existing Postgres admin URL; empty starts a throwaway
	// postgres:16 container named <Prefix>-pg-<random>.
	DBURL string
	// TmpfsData keeps the throwaway database in memory (CI smoke only).
	TmpfsData bool
	Prefix    string
	Replicas  int
	Short     bool
	LogDir    string
	Command   func() (string, []string, []string)
	Log       *slog.Logger
	// MutexProfile is ServerConfig.MutexProfileFraction for every server.
	MutexProfile int
	// ServerDBParams are appended to the servers' database URL (k=v&k=v).
	ServerDBParams string
	// PoolMaxConns sets every server's pool size (experiment; 0 = default).
	PoolMaxConns int
}

// NewEnv starts the database (or connects to one) and the fake model.
func NewEnv(ctx context.Context, c EnvConfig) (*Env, error) {
	if c.Log == nil {
		c.Log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	if c.Prefix == "" {
		c.Prefix = "s2lt"
	}
	if err := os.MkdirAll(c.LogDir, 0o755); err != nil {
		return nil, fmt.Errorf("loadtest: log dir: %w", err)
	}
	e := &Env{Model: NewFakeModel(ModelConfig{}), Replicas: max(1, c.Replicas), Short: c.Short, MutexProfile: c.MutexProfile, ServerDBParams: c.ServerDBParams, PoolMaxConns: c.PoolMaxConns, Log: c.Log, HTTP: newHTTPClient()}
	var err error
	if c.DBURL != "" {
		e.PG, err = ExistingPostgres(ctx, c.DBURL)
	} else {
		// A driver killed before its cleanup (SIGKILL, a go test timeout)
		// leaves its container behind; remove those first.
		if gone, err := RemoveStale(ctx, c.Prefix); err != nil {
			c.Log.Warn("loadtest: stale container cleanup failed", "err", err)
		} else if len(gone) > 0 {
			c.Log.Info("loadtest: removed stale containers", "names", gone)
		}
		e.PG, err = StartPostgres(ctx, c.Prefix, c.TmpfsData)
	}
	if err != nil {
		return nil, err
	}
	e.modelLn, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		e.PG.Close()
		return nil, fmt.Errorf("loadtest: fake model listen: %w", err)
	}
	e.ModelURL = "http://" + e.modelLn.Addr().String() + "/"
	e.modelSrv = &http.Server{Handler: e.Model, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = e.modelSrv.Serve(e.modelLn) }()
	e.Launcher = Launcher{Command: c.Command, ModelURL: e.ModelURL, LogDir: c.LogDir}
	return e, nil
}

// Close stops the fake model and removes the database container.
func (e *Env) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = e.modelSrv.Shutdown(ctx)
	e.PG.Close()
}

// stack is one scenario run's database and servers.
type stack struct {
	db      *Database
	cluster *Cluster
	apis    []*API
}

// newStack creates a database, runs seed on it (may be nil), then starts the
// servers.
func (e *Env) newStack(ctx context.Context, name string, sc ServerConfig, seed func(context.Context, string) error) (*stack, error) {
	db, err := e.PG.CreateDatabase(ctx)
	if err != nil {
		return nil, err
	}
	if seed != nil {
		if err := seed(ctx, db.URL); err != nil {
			db.Drop()
			return nil, err
		}
	}
	sc.DBURL = db.URL
	if e.ServerDBParams != "" {
		// Extra connection-string parameters for the servers only, such as
		// pool_max_conns=20 (what an operator would put in PIPEROOM_DB_URL).
		u, err := withQuery(db.URL, e.ServerDBParams)
		if err != nil {
			db.Drop()
			return nil, err
		}
		sc.DBURL = u
	}
	if sc.PoolMaxConns == 0 {
		sc.PoolMaxConns = e.PoolMaxConns
	}
	if sc.MutexProfileFraction == 0 {
		sc.MutexProfileFraction = e.MutexProfile
	}
	c, err := e.Launcher.Start(ctx, sc, e.Replicas, name)
	if err != nil {
		db.Drop()
		return nil, err
	}
	s := &stack{db: db, cluster: c}
	e.Log.Info("loadtest: servers up", "run", name, "urls", c.urls(), "db", db.Name)
	for i, p := range c.Procs {
		s.apis = append(s.apis, &API{Base: p.URL, HTTP: e.HTTP, Replica: i})
	}
	return s, nil
}

func (s *stack) close() {
	s.cluster.Stop()
	s.db.Drop()
}

// api returns the API of replica i mod n, so load spreads over replicas.
func (s *stack) api(i int) *API { return s.apis[i%len(s.apis)] }

// serverStats sums what the servers report (one entry per replica too).
func (s *stack) serverStats(ctx context.Context) ([]ServerStats, error) {
	return s.cluster.Stats(ctx)
}

// waitRuns waits until every room's agent has finished its first run (the
// fake model answered end_turn), or ctx ends.
func (e *Env) waitRuns(ctx context.Context, rooms []string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		st := e.Model.Stats()
		left := 0
		for _, r := range rooms {
			if rs := st.Rooms[r]; rs == nil || rs.EndTurns == 0 {
				left++
			}
		}
		if left == 0 {
			return nil
		}
		if !sleepCtx(ctx, 50*time.Millisecond) {
			return fmt.Errorf("loadtest: %d of %d rooms did not finish their agent run: %w", left, len(rooms), ctx.Err())
		}
	}
}

// openRooms creates rooms with a small pool of concurrent creators. names
// gives each room's title and pipeline; it returns ids in the same order.
func (s *stack) openRooms(ctx context.Context, titles, pipelines []string, workers int) ([]string, []time.Duration, error) {
	ids := make([]string, len(titles))
	lat := make([]time.Duration, len(titles))
	errs := make([]error, len(titles))
	next := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < max(1, workers); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				t0 := time.Now()
				ids[i], errs[i] = s.api(i).CreateRoom(ctx, loadUsers[0], titles[i], pipelines[i])
				lat[i] = time.Since(t0)
			}
		}()
	}
	for i := range titles {
		select {
		case next <- i:
		case <-ctx.Done():
		}
	}
	close(next)
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, nil, err
	}
	// A cancelled ctx stops handing out rooms: some ids are then empty.
	if err := ctx.Err(); err != nil {
		return nil, nil, fmt.Errorf("loadtest: open rooms: %w", err)
	}
	return ids, lat, nil
}

// Machine describes the box a run happened on.
type Machine struct {
	CPUs       int    `json:"cpus"`
	GOMAXPROCS int    `json:"gomaxprocs"`
	CPUModel   string `json:"cpu_model"`
	MemTotal   string `json:"mem_total"`
	Kernel     string `json:"kernel"`
	GoVersion  string `json:"go_version"`
	Postgres   string `json:"postgres"`
	Note       string `json:"note"`
}

// DescribeMachine reads the machine facts from /proc (Linux).
func DescribeMachine(pgVersion string) Machine {
	m := Machine{CPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0), GoVersion: runtime.Version(), Postgres: pgVersion,
		Note: "shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target"}
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if v, ok := strings.CutPrefix(l, "model name"); ok {
				m.CPUModel = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(v), ":"))
				break
			}
		}
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		l, _, _ := strings.Cut(string(b), "\n")
		m.MemTotal = strings.TrimSpace(strings.TrimPrefix(l, "MemTotal:"))
	}
	if b, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		m.Kernel = strings.TrimSpace(string(b))
	}
	return m
}

// RunProfiles runs each profile and writes <out>/<profile>.json and .md.
// A profile that fails is reported and the next one still runs.
func RunProfiles(ctx context.Context, env *Env, profiles []Profile, overrides []Override, outDir string) ([]*Result, error) {
	if err := CheckOverrides(profiles, overrides); err != nil {
		return nil, err
	}
	machine := DescribeMachine(env.PG.Version)
	if err := writeJSON(filepath.Join(outDir, "machine.json"), machine); err != nil {
		return nil, err
	}
	var results []*Result
	var errs []error
	for _, pr := range profiles {
		if err := ctx.Err(); err != nil {
			return results, errors.Join(append(errs, fmt.Errorf("loadtest: stopped before %s: %w", pr.Name, err))...)
		}
		p, err := ParseParams(pr.Name, pr.Params, env.Short, overrides)
		if err != nil {
			return results, err
		}
		env.Log.Info("loadtest: profile start", "profile", pr.Name, "replicas", env.Replicas, "short", env.Short)
		start, load0 := time.Now(), loadAvg()
		res, err := pr.Run(ctx, env, p)
		if err != nil {
			env.Log.Error("loadtest: profile failed", "profile", pr.Name, "err", err)
			errs = append(errs, fmt.Errorf("%s: %w", pr.Name, err))
			continue
		}
		res.Profile, res.Short, res.Replicas, res.Params, res.Started = pr.Name, env.Short, env.Replicas, p.Raw(), start
		res.WallS = round3(time.Since(start).Seconds())
		res.LoadAvg = [2]string{load0, loadAvg()}
		base := pr.Name
		if env.Replicas > 1 {
			base = fmt.Sprintf("%s-r%d", pr.Name, env.Replicas)
		}
		if err := writeJSON(filepath.Join(outDir, base+".json"), res); err != nil {
			return results, err
		}
		md := renderMarkdown(res, machine)
		if err := os.WriteFile(filepath.Join(outDir, base+".md"), []byte(md), 0o644); err != nil {
			return results, fmt.Errorf("loadtest: write report: %w", err)
		}
		res.Markdown = md
		for _, c := range res.Failed() {
			env.Log.Warn("loadtest: check failed", "profile", pr.Name, "check", c.Name, "detail", c.Detail)
		}
		env.Log.Info("loadtest: profile done", "profile", pr.Name, "wall_s", res.WallS, "report", filepath.Join(outDir, base+".md"))
		results = append(results, res)
	}
	return results, errors.Join(errs...)
}

// settle is how long to let a join storm's presence broadcasts die down
// before measuring; short (CI) runs wait less.
func (e *Env) settle() time.Duration {
	if e.Short {
		return 200 * time.Millisecond
	}
	return time.Second
}

// loadAvg returns the first three fields of /proc/loadavg ("" elsewhere).
func loadAvg() string {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return ""
	}
	f := strings.Fields(string(b))
	if len(f) < 3 {
		return ""
	}
	return strings.Join(f[:3], " ")
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("loadtest: encode %s: %w", path, err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		return fmt.Errorf("loadtest: write %s: %w", path, err)
	}
	return nil
}
