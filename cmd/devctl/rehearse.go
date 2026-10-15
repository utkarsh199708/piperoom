package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/stacklock"
)

const (
	// demoPipeline is the pipeline whose room the rehearsal waits for
	// (dev/owners.yaml).
	demoPipeline = "orders"
	// defaultDemoEnv is the demo profile, relative to the repo root.
	defaultDemoEnv = "dev/demo.env"
	// maxRehearseReply caps an API reply (AGENTS.md rule 7).
	maxRehearseReply = 4 << 20
	// cleanupTimeout bounds the final reset, which runs even after Ctrl+C.
	cleanupTimeout = 10 * time.Minute
)

// demoOpts holds the parsed flags of `devctl demo`.
type demoOpts struct {
	fix       bool
	rehearse  bool
	owner     string
	scenario  string
	poll      time.Duration
	timeout   time.Duration
	envFile   string
	server    string
	asUser    string
	userGiven bool
}

// parseDemoFlags parses `devctl demo` arguments. Flags may come before or
// after the scenario, the order an operator types them.
func parseDemoFlags(args []string, defaultOwner string) (demoOpts, error) {
	o := demoOpts{}
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.BoolVar(&o.fix, "fix", false, "repair the scenario and release the stack lock")
	fs.BoolVar(&o.rehearse, "rehearse", false, "run the whole demo once: reset, break, wait for the room, reset")
	fs.StringVar(&o.owner, "owner", defaultOwner, "lock owner tag")
	fs.DurationVar(&o.poll, "poll", 2*time.Second, "with --rehearse: how often to ask the server for the room")
	fs.DurationVar(&o.timeout, "timeout", 3*time.Minute, "with --rehearse: how long to wait for the room before giving up")
	fs.StringVar(&o.envFile, "env", defaultDemoEnv, "with --rehearse: demo profile to check")
	fs.StringVar(&o.server, "server", "", "with --rehearse: server base URL (default http://PIPEROOM_HTTP_ADDR)")
	fs.StringVar(&o.asUser, "as", "arjun", "with --rehearse: user id sent as X-User (dev mode)")
	var pos []string
	rest := args
	for {
		if err := fs.Parse(rest); err != nil {
			return demoOpts{}, fmt.Errorf("%s: %w", demoUsage(), err)
		}
		if fs.NArg() == 0 {
			break
		}
		pos = append(pos, fs.Arg(0))
		rest = fs.Args()[1:]
	}
	if o.fix && o.rehearse {
		return demoOpts{}, errors.New("devctl demo: --fix and --rehearse cannot be combined")
	}
	switch {
	case len(pos) == 1:
		o.scenario = pos[0]
	case len(pos) == 0 && o.rehearse:
		o.scenario = "schema-change"
	default:
		return demoOpts{}, errors.New(demoUsage())
	}
	if _, ok := scenarios[o.scenario]; !ok {
		return demoOpts{}, fmt.Errorf("unknown scenario %q (%s)", o.scenario, strings.Join(scenarioNames(), ", "))
	}
	if o.rehearse {
		if o.poll <= 0 {
			return demoOpts{}, fmt.Errorf("devctl demo: --poll %s: must be positive", o.poll)
		}
		if o.timeout <= 0 || o.poll > o.timeout {
			return demoOpts{}, fmt.Errorf("devctl demo: --timeout %s: must be positive and at least --poll (%s)", o.timeout, o.poll)
		}
		// --as is dev auth only: it sends X-User, which the server ignores in oidc mode.
		if strings.TrimSpace(o.asUser) == "" {
			return demoOpts{}, errors.New("devctl demo: --as must not be empty")
		}
	}
	return o, nil
}

func demoUsage() string {
	return "usage: devctl demo [--fix] <scenario> | devctl demo --rehearse [scenario] [--poll 2s] [--timeout 3m] [--env dev/demo.env] [--server URL] [--as user] (scenarios: " +
		strings.Join(scenarioNames(), ", ") + ")"
}

// secretKey matches names that look like credentials. The demo profile is
// committed, so it must never carry one.
var secretKey = regexp.MustCompile(`(?i)(API_KEY|SECRET|TOKEN|PASSWORD|COOKIE)`)

// parseEnvFile reads KEY=VALUE lines. Blank lines and # comments are skipped,
// "export " is allowed, and one pair of surrounding quotes is removed.
func parseEnvFile(r io.Reader) (map[string]string, error) {
	out := map[string]string{}
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		k, v, ok := strings.Cut(line, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return nil, fmt.Errorf("line %d: want KEY=VALUE", n)
		}
		v = strings.TrimSpace(v)
		if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
			v = v[1 : len(v)-1]
		}
		if secretKey.MatchString(k) {
			return nil, fmt.Errorf("line %d: %s looks like a secret; keep secrets out of the demo profile", n, k)
		}
		out[k] = v
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// checkDemoProfile validates the watcher settings with the server's own
// parser and requires rooms mode, because any other mode opens no room and the
// rehearsal would wait for nothing.
func checkDemoProfile(env map[string]string) (config.Watch, error) {
	w, err := config.ParseWatch(config.Config{
		WatchMode:         env["PIPEROOM_WATCH_MODE"],
		WatchInterval:     env["PIPEROOM_WATCH_INTERVAL"],
		WatchCheckTimeout: env["PIPEROOM_WATCH_CHECK_TIMEOUT"],
		WatchCooldown:     env["PIPEROOM_WATCH_COOLDOWN"],
	})
	if err != nil {
		return config.Watch{}, err
	}
	if w.Mode != config.WatchRooms {
		return config.Watch{}, fmt.Errorf("devctl: demo profile has PIPEROOM_WATCH_MODE=%q; the demo needs rooms", w.Mode)
	}
	return w, nil
}

// stackHostPort reduces a Postgres URL to "host:port" for comparison. localhost,
// 127.0.0.1 and ::1 count as one host; a missing port is 5432. The password is
// never read out.
func stackHostPort(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "", errors.New("not a postgres URL")
	}
	host, port := u.Hostname(), u.Port()
	if port == "" {
		port = "5432"
	}
	if host == "localhost" || host == "::1" {
		host = "127.0.0.1"
	}
	return host + ":" + port, nil
}

// checkRoomDB refuses a profile whose room database is the pipeline's Postgres,
// the one `devctl reset` wipes (it removes the pipeline containers with their
// volumes): the rehearsal would delete the rooms it waits for, and the server
// migrates only at start, so it would not recreate them. roomURL is
// PIPEROOM_DB_URL from the profile (empty = the config default, the stack's own
// roomdb service on :5433, which reset keeps; only `devctl reset --rooms --yes`
// deletes it, QA G19); stackURL is the stack's source database URL.
func checkRoomDB(roomURL, stackURL string) error {
	if strings.TrimSpace(roomURL) == "" {
		// The server's own default.
		roomURL = config.Load().RoomDBURL
	}
	room, err := stackHostPort(roomURL)
	if err != nil {
		return fmt.Errorf("devctl: PIPEROOM_DB_URL in the demo profile: %w", err)
	}
	stack, err := stackHostPort(stackURL)
	if err != nil {
		return fmt.Errorf("devctl: SOURCE_DB_URL: %w", err)
	}
	if room == stack {
		return fmt.Errorf("devctl: the demo profile's room database (%s) is the pipeline's Postgres, which `devctl reset` wipes; point PIPEROOM_DB_URL in dev/demo.env at the stack's roomdb service (localhost:5433/piperoom, also the default; reset keeps it) and restart the server", room)
	}
	return nil
}

// rehearsalSteps are the stack actions a rehearsal performs. The real
// implementation drives Docker; tests use a fake.
type rehearsalSteps interface {
	Reset(ctx context.Context) error
	Load(ctx context.Context) error
	Break(ctx context.Context) error
}

// roomBrief is the part of a RoomSnapshot (docs/api.md) the rehearsal reads.
type roomBrief struct {
	ID          string    `json:"id"`
	Title       string    `json:"title"`
	Severity    string    `json:"severity"`
	PipelineID  string    `json:"pipeline_id"`
	Status      string    `json:"status"`
	OpenedAt    time.Time `json:"opened_at"`
	AgentStatus struct {
		State  string `json:"state"`
		Detail string `json:"detail"`
	} `json:"agent_status"`
}

// roomSource asks the running server about rooms.
type roomSource interface {
	Ping(ctx context.Context) error
	OpenRooms(ctx context.Context, pipeline string) ([]roomBrief, error)
}

// rehearser runs the rehearsal. now and sleep are fields so tests do not wait.
type rehearser struct {
	steps    rehearsalSteps
	rooms    roomSource
	out      io.Writer
	now      func() time.Time
	sleep    func(ctx context.Context, d time.Duration) error
	scenario string
	poll     time.Duration
	timeout  time.Duration
	watch    config.Watch
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (r *rehearser) say(format string, a ...any) {
	_, _ = fmt.Fprintf(r.out, format+"\n", a...)
}

// run does: check the server, reset, load, break, wait for the room, reset.
// The final reset also runs after a failure or Ctrl+C, so a rehearsal never
// leaves the pipeline broken.
func (r *rehearser) run(ctx context.Context) (err error) {
	r.say("rehearsal of %q  (watcher: %s, every %s, cooldown %s)", r.scenario, r.watch.Mode, r.watch.Interval, r.watch.Cooldown)
	r.say("[1/5] checking the server ...")
	if err := r.rooms.Ping(ctx); err != nil {
		return fmt.Errorf("devctl: server not reachable (start it with the demo profile, see dev/demo.env): %w", err)
	}
	open, err := r.rooms.OpenRooms(ctx, demoPipeline)
	if err != nil {
		return err
	}
	if len(open) > 0 {
		return fmt.Errorf("devctl: room %s is already open for pipeline %q; close it in the UI first, or the watcher will join it instead of opening a new one", open[0].ID, demoPipeline)
	}

	r.say("[2/5] resetting to a clean seeded state ...")
	if err := r.steps.Reset(ctx); err != nil {
		return err
	}
	r.say("[3/5] loading a short burst of traffic ...")
	if err := r.steps.Load(ctx); err != nil {
		return err
	}

	// From here the stack may be broken: always put it back.
	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		r.say("[5/5] resetting the stack ...")
		if rerr := r.steps.Reset(cctx); rerr != nil {
			err = errors.Join(err, fmt.Errorf("devctl: final reset failed, run `devctl reset`: %w", rerr))
		}
	}()

	r.say("[4/5] breaking %q, then waiting up to %s for the room (polling every %s) ...", r.scenario, r.timeout, r.poll)
	if err := r.steps.Break(ctx); err != nil {
		return err
	}
	start := r.now()
	room, err := r.waitForRoom(ctx, start)
	if err != nil {
		return err
	}
	took := r.now().Sub(start).Round(time.Second)
	r.say("")
	r.say("room opened %s after the break:", took)
	r.say("  id        %s", room.ID)
	r.say("  title     %s", room.Title)
	r.say("  severity  %s", room.Severity)
	r.say("  agent     %s %s", room.AgentStatus.State, room.AgentStatus.Detail)
	r.say("")
	r.say("PASS: break -> room in %s. Close room %s in the UI before the next rehearsal or the live run.", took, room.ID)
	return nil
}

// waitForRoom polls until an open room for the demo pipeline appears. A poll
// error is reported but does not stop the wait: the server may be busy.
func (r *rehearser) waitForRoom(ctx context.Context, start time.Time) (roomBrief, error) {
	deadline := start.Add(r.timeout)
	var lastErr error
	for {
		rooms, err := r.rooms.OpenRooms(ctx, demoPipeline)
		if err != nil {
			lastErr = err
			r.say("  poll failed: %v", err)
		} else if len(rooms) > 0 {
			return rooms[0], nil
		}
		if !r.now().Add(r.poll).Before(deadline) {
			msg := fmt.Sprintf("devctl: no room opened within %s. Is the server running with dev/demo.env (PIPEROOM_WATCH_MODE=rooms)? Check its log for `watch:` lines", r.timeout)
			if lastErr != nil {
				return roomBrief{}, fmt.Errorf("%s; last poll error: %w", msg, lastErr)
			}
			return roomBrief{}, errors.New(msg)
		}
		if err := r.sleep(ctx, r.poll); err != nil {
			return roomBrief{}, fmt.Errorf("devctl: waiting for the room: %w", err)
		}
	}
}

// httpRooms implements roomSource over the server's HTTP API.
type httpRooms struct {
	hc   *http.Client
	base string
	user string
}

func newHTTPRooms(base, user string) (*httpRooms, error) {
	u, err := url.Parse(strings.TrimRight(base, "/"))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("devctl: bad server URL %q", base)
	}
	return &httpRooms{hc: &http.Client{Timeout: 10 * time.Second}, base: u.String(), user: user}, nil
}

func (h *httpRooms) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.base+path, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("X-User", h.user)
	resp, err := h.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRehearseReply))
	if err != nil {
		return nil, fmt.Errorf("GET %s: read reply: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return nil, fmt.Errorf("GET %s: HTTP %d (--as sends X-User, which works in dev auth mode only and needs a user from dev/users.yaml)", path, resp.StatusCode)
		}
		return nil, fmt.Errorf("GET %s: HTTP %d", path, resp.StatusCode)
	}
	return body, nil
}

func (h *httpRooms) Ping(ctx context.Context) error {
	_, err := h.get(ctx, "/healthz")
	return err
}

func (h *httpRooms) OpenRooms(ctx context.Context, pipeline string) ([]roomBrief, error) {
	body, err := h.get(ctx, "/api/rooms")
	if err != nil {
		return nil, err
	}
	var all []roomBrief
	if err := json.Unmarshal(body, &all); err != nil {
		return nil, fmt.Errorf("decode /api/rooms: %w", err)
	}
	var out []roomBrief
	for _, r := range all {
		if r.PipelineID == pipeline && r.Status == "open" {
			out = append(out, r)
		}
	}
	return out, nil
}

// stackSteps runs a scenario on the real stack.
type stackSteps struct {
	s  *stack
	sc scenario
}

func (p stackSteps) Reset(ctx context.Context) error { return p.s.reset(ctx, false) }

func (p stackSteps) Load(ctx context.Context) error {
	lctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	return p.s.load(lctx, 5)
}

func (p stackSteps) Break(ctx context.Context) error { return p.sc.breakIt(ctx, p.s) }

// rehearseCmd is `devctl demo --rehearse`. It holds the stack lock for the
// whole run; it releases it at the end unless the caller already held it.
func rehearseCmd(ctx context.Context, s *stack, o demoOpts, out io.Writer) error {
	envPath := o.envFile
	if !filepath.IsAbs(envPath) {
		envPath = filepath.Join(s.root, envPath)
	}
	f, err := os.Open(envPath)
	if err != nil {
		return fmt.Errorf("devctl: open demo profile: %w", err)
	}
	env, err := parseEnvFile(f)
	_ = f.Close()
	if err != nil {
		return fmt.Errorf("devctl: demo profile %s: %w", envPath, err)
	}
	w, err := checkDemoProfile(env)
	if err != nil {
		return fmt.Errorf("devctl: demo profile %s: %w", envPath, err)
	}
	if err := checkRoomDB(env["PIPEROOM_DB_URL"], s.cfg.SourceDBURL); err != nil {
		return err
	}
	base := o.server
	if base == "" {
		base = serverURL(s.cfg.HTTPAddr)
	}
	rooms, err := newHTTPRooms(base, o.asUser)
	if err != nil {
		return err
	}
	// Acquire is re-entrant. Release at the end only what this run took, so an
	// operator who locked the stack first (the demo script does) keeps the lock
	// for the live run and the final `unlock`.
	heldBefore := lockHeldBy(o.owner)
	if err := acquireLock(o.owner); err != nil {
		return err
	}
	if !heldBefore {
		defer func() {
			if err := releaseLock(o.owner); err != nil {
				s.log.Warn("demo: release lock failed", "owner", o.owner, "err", err)
			}
		}()
	}
	r := &rehearser{
		steps: stackSteps{s: s, sc: scenarios[o.scenario]}, rooms: rooms, out: out,
		now: time.Now, sleep: sleepCtx, scenario: o.scenario,
		poll: o.poll, timeout: o.timeout, watch: w,
	}
	return r.run(ctx)
}

// lockHeldBy reports whether owner holds the stack lock now. A read error
// counts as not held: the run then takes the lock and releases it again.
func lockHeldBy(owner string) bool {
	rec, err := stacklock.New(stacklock.Path()).Status()
	return err == nil && rec != nil && rec.Owner == owner
}
