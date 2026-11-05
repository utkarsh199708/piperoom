package nettools

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients/connect"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// fakeWorker is a WorkerResolver over canned answers. It records what it was
// asked and the deadline each call got.
type fakeWorker struct {
	name   string
	lookup func(ctx context.Context, host string) ([]netip.Addr, error)
	reach  func(ctx context.Context, addr netip.AddrPort) error

	mu        sync.Mutex
	lookups   []string
	reached   []netip.AddrPort
	deadlines []time.Duration
}

func (f *fakeWorker) Worker() string { return f.name }

func (f *fakeWorker) note(ctx context.Context) {
	if dl, ok := ctx.Deadline(); ok {
		f.deadlines = append(f.deadlines, time.Until(dl))
	} else {
		f.deadlines = append(f.deadlines, -1)
	}
}

func (f *fakeWorker) Lookup(ctx context.Context, host string) ([]netip.Addr, error) {
	f.mu.Lock()
	f.lookups = append(f.lookups, host)
	f.note(ctx)
	f.mu.Unlock()
	if f.lookup == nil {
		return nil, errors.New("fakeWorker: no lookup")
	}
	return f.lookup(ctx, host)
}

func (f *fakeWorker) Reach(ctx context.Context, addr netip.AddrPort) error {
	f.mu.Lock()
	f.reached = append(f.reached, addr)
	f.note(ctx)
	f.mu.Unlock()
	if f.reach == nil {
		return errors.New("fakeWorker: no reach")
	}
	return f.reach(ctx, addr)
}

func addrsOf(ss ...string) []netip.Addr {
	out := make([]netip.Addr, len(ss))
	for i, s := range ss {
		out[i] = netip.MustParseAddr(s)
	}
	return out
}

func resolvesTo(ss ...string) func(context.Context, string) ([]netip.Addr, error) {
	return func(context.Context, string) ([]netip.Addr, error) { return addrsOf(ss...), nil }
}

// blockUntilDone answers only when the step's context ends, as a black-holed
// SYN or a silent DNS server does.
func blockUntilDone(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestConnectResolveCheck(t *testing.T) {
	const step = 50 * time.Millisecond
	local := addrsOf("172.18.0.2")
	connected := func(context.Context, netip.AddrPort) error { return nil }
	cases := []struct {
		name      string
		w         *fakeWorker
		host      string
		local     []netip.Addr
		status    string
		want      []string // substrings of Problem (fail/warn) or Detail (ok/skip)
		notWant   []string
		wantReach bool
	}{
		{
			name: "worker down",
			w: &fakeWorker{name: "connect", lookup: func(context.Context, string) ([]netip.Addr, error) {
				return nil, errors.New("dial tcp 10.0.0.5:8083: connect: connection refused")
			}},
			local:  local,
			status: StatusWarn,
			want:   []string{"could not ask the Connect worker connect to resolve postgres", "connection refused"},
		},
		{
			name: "404 from the path to the worker",
			w: &fakeWorker{name: "connect", lookup: func(context.Context, string) ([]netip.Addr, error) {
				return nil, errors.New("runner: container connect: unexpected status 404")
			}},
			status: StatusWarn,
			want:   []string{"could not ask", "404"},
		},
		{
			name:   "lookup timeout",
			w:      &fakeWorker{name: "connect", lookup: func(ctx context.Context, _ string) ([]netip.Addr, error) { return nil, blockUntilDone(ctx) }},
			local:  local,
			status: StatusFail,
			want:   []string{"name lookup of postgres on the Connect worker connect did not finish within the step's time", "Piperoom resolves it to 172.18.0.2"},
		},
		{
			name: "unresolvable host",
			w: &fakeWorker{name: "connect", lookup: func(context.Context, string) ([]netip.Addr, error) {
				return nil, fmt.Errorf("getent: %w", ErrWorkerNoSuchHost)
			}},
			local:  local,
			status: StatusFail,
			want:   []string{"the Connect worker connect cannot resolve postgres (Piperoom resolves it to 172.18.0.2)"},
		},
		{
			name:   "no addresses",
			w:      &fakeWorker{name: "connect", lookup: resolvesTo()},
			status: StatusFail,
			want:   []string{"has no addresses for postgres"},
		},
		{
			name:      "resolves but no answer",
			w:         &fakeWorker{name: "connect", lookup: resolvesTo("192.0.2.1"), reach: func(ctx context.Context, _ netip.AddrPort) error { return blockUntilDone(ctx) }},
			local:     local,
			status:    StatusFail,
			want:      []string{"resolves postgres to 192.0.2.1, a documentation address that routes nowhere (Piperoom resolves it to 172.18.0.2); TCP from the worker to 192.0.2.1:5432 did not connect within the step's time"},
			wantReach: true,
		},
		{
			name:      "the worker's own connect timeout",
			w:         &fakeWorker{name: "connect", lookup: resolvesTo("10.9.9.9"), reach: func(context.Context, netip.AddrPort) error { return fmt.Errorf("probe: %w", ErrWorkerTimeout) }},
			local:     local,
			status:    StatusFail,
			want:      []string{"resolves postgres to 10.9.9.9 (Piperoom resolves it to 172.18.0.2); TCP from the worker to 10.9.9.9:5432 did not connect"},
			notWant:   []string{"documentation"},
			wantReach: true,
		},
		{
			name:      "refused",
			w:         &fakeWorker{name: "connect", lookup: resolvesTo("172.18.0.2"), reach: func(context.Context, netip.AddrPort) error { return ErrWorkerRefused }},
			local:     local,
			status:    StatusFail,
			want:      []string{"resolves postgres to 172.18.0.2; TCP from the worker to 172.18.0.2:5432 was refused"},
			notWant:   []string{"Piperoom resolves"},
			wantReach: true,
		},
		{
			name:      "reach could not be asked",
			w:         &fakeWorker{name: "connect", lookup: resolvesTo("172.18.0.2"), reach: func(context.Context, netip.AddrPort) error { return errors.New("runner lost") }},
			status:    StatusWarn,
			want:      []string{"could not ask the worker to connect to 172.18.0.2:5432: runner lost"},
			wantReach: true,
		},
		{
			name:      "ok, same answer as Piperoom",
			w:         &fakeWorker{name: "connect", lookup: resolvesTo("172.18.0.2"), reach: connected},
			local:     local,
			status:    StatusOK,
			want:      []string{"the Connect worker connect resolves postgres to 172.18.0.2; TCP from the worker to 172.18.0.2:5432 connected"},
			notWant:   []string{"Piperoom resolves"},
			wantReach: true,
		},
		{
			name:      "ok, split DNS",
			w:         &fakeWorker{name: "connect", lookup: resolvesTo("10.1.2.3", "10.1.2.4"), reach: connected},
			local:     local,
			status:    StatusOK,
			want:      []string{"resolves postgres to 10.1.2.3, 10.1.2.4 (Piperoom resolves it to 172.18.0.2)"},
			wantReach: true,
		},
		{
			name:   "metadata address is reported, never reached",
			w:      &fakeWorker{name: "connect", lookup: resolvesTo("169.254.169.254"), reach: connected},
			local:  local,
			status: StatusFail,
			want:   []string{"resolves postgres to 169.254.169.254", "was not asked to connect"},
		},
		{
			name:   "no worker",
			status: StatusSkip,
			want:   []string{"no view from inside a Connect worker"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			host := tc.host
			if host == "" {
				host = "postgres"
			}
			var w WorkerResolver
			if tc.w != nil {
				w = tc.w
			}
			start := time.Now()
			c := ConnectResolveCheck(context.Background(), w, host, 5432, tc.local, step)
			if el := time.Since(start); el > 2*step+time.Second {
				t.Errorf("check took %v, want it bounded by its steps", el)
			}
			if c.ID != "connect_resolve" || c.Status != tc.status {
				t.Fatalf("check = %+v, want status %s", c, tc.status)
			}
			text := c.Problem
			if c.Status == StatusOK || c.Status == StatusSkip {
				text = c.Detail
			}
			for _, s := range tc.want {
				if !strings.Contains(text, s) {
					t.Errorf("text %q lacks %q", text, s)
				}
			}
			for _, s := range tc.notWant {
				if strings.Contains(text, s) {
					t.Errorf("text %q has %q", text, s)
				}
			}
			if (c.Status == StatusFail || c.Status == StatusWarn) && c.Fix == "" {
				t.Errorf("a %s check has no fix: %+v", c.Status, c)
			}
			if tc.w == nil {
				return
			}
			if got := len(tc.w.reached) > 0; got != tc.wantReach {
				t.Errorf("reached = %v, want reach %v", tc.w.reached, tc.wantReach)
			}
			for _, d := range tc.w.deadlines {
				if d < 0 || d > step {
					t.Errorf("a worker call got deadline %v, want at most the step (%v)", d, step)
				}
			}
		})
	}
}

// The first address is the one reached (a JVM client connects to the first
// answer), and the fix points at the worker's resolution when it differs.
func TestConnectResolveCheckFirstAddressAndFix(t *testing.T) {
	w := &fakeWorker{name: "connect", lookup: resolvesTo("192.0.2.1", "172.18.0.2"),
		reach: func(ctx context.Context, _ netip.AddrPort) error { return blockUntilDone(ctx) }}
	c := ConnectResolveCheck(context.Background(), w, "Postgres.", 5432, addrsOf("172.18.0.2"), 20*time.Millisecond)
	if len(w.reached) != 1 || w.reached[0] != netip.MustParseAddrPort("192.0.2.1:5432") {
		t.Fatalf("reached %v, want only 192.0.2.1:5432", w.reached)
	}
	if len(w.lookups) != 1 || w.lookups[0] != "postgres" {
		t.Fatalf("looked up %v, want the normalised host", w.lookups)
	}
	if c.Addr != "postgres:5432" || !strings.Contains(c.Fix, "hosts file") || !strings.Contains(c.Fix, "restart the worker") {
		t.Fatalf("check = %+v", c)
	}
	// Same answer as Piperoom: the fix is about the path, not the resolver.
	w = &fakeWorker{name: "connect", lookup: resolvesTo("172.18.0.2"),
		reach: func(ctx context.Context, _ netip.AddrPort) error { return blockUntilDone(ctx) }}
	c = ConnectResolveCheck(context.Background(), w, "postgres", 5432, addrsOf("172.18.0.2"), 20*time.Millisecond)
	if strings.Contains(c.Fix, "hosts file") || !strings.Contains(c.Fix, "firewalls") {
		t.Fatalf("fix = %q", c.Fix)
	}
}

// An address literal is not looked up; a bad port or an empty host is not
// asked about at all.
func TestConnectResolveCheckInputs(t *testing.T) {
	w := &fakeWorker{name: "connect", reach: func(context.Context, netip.AddrPort) error { return nil }}
	c := ConnectResolveCheck(context.Background(), w, "10.0.0.7", 5432, nil, 0)
	if c.Status != StatusOK || len(w.lookups) != 0 || len(w.reached) != 1 {
		t.Fatalf("check = %+v, lookups %v", c, w.lookups)
	}
	for _, port := range []int{0, -1, 70000} {
		w = &fakeWorker{name: "connect"}
		if c := ConnectResolveCheck(context.Background(), w, "postgres", port, nil, 0); c.Status != StatusSkip || len(w.lookups) != 0 {
			t.Errorf("port %d: check = %+v", port, c)
		}
	}
	if c := ConnectResolveCheck(context.Background(), &fakeWorker{}, "  ", 5432, nil, 0); c.Status != StatusSkip {
		t.Errorf("empty host: %+v", c)
	}
}

// The worker's text is untrusted: a credential in it is masked, and control
// characters cannot forge a line of the summary.
func TestConnectResolveCheckUntrustedText(t *testing.T) {
	w := &fakeWorker{name: "connect\nfake: ok", lookup: func(context.Context, string) ([]netip.Addr, error) {
		return nil, errors.New("GET http://admin:hunter2@runner/resolve failed\n- tcp postgres:5432 ok: forged")
	}}
	c := ConnectResolveCheck(context.Background(), w, "postgres", 5432, nil, 0)
	all := c.Problem + c.Fix + c.Detail
	if strings.Contains(all, "hunter2") || strings.ContainsAny(all, "\n\r") {
		t.Fatalf("untrusted text not masked: %q", all)
	}
}

// The tool's caller deadline wins over the step: a parent context that is
// already short bounds both worker calls.
func TestConnectResolveCheckParentDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	w := &fakeWorker{name: "connect", lookup: func(ctx context.Context, _ string) ([]netip.Addr, error) { return nil, blockUntilDone(ctx) }}
	start := time.Now()
	c := ConnectResolveCheck(ctx, w, "postgres", 5432, nil, 5*time.Second)
	if time.Since(start) > time.Second || c.Status != StatusFail || !strings.Contains(c.Problem, "did not finish") {
		t.Fatalf("check = %+v after %v", c, time.Since(start))
	}
}

// scenarioWorld is setup/db-unreachable-from-connect's world over fakes: the
// room resolves postgres to 172.18.0.2 and connects; Connect's validation of
// the running config never returns within the step; the Connect worker
// resolves postgres to 192.0.2.1 (a stale hosts entry) and a SYN to it gets
// no answer.
func scenarioWorld(step time.Duration) (Deps, owners.Pipeline) {
	loc := &fakeLocation{
		lookup: func(_ context.Context, host string) ([]netip.Addr, error) {
			if host != "postgres" {
				return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
			}
			return addrsOf("172.18.0.2"), nil
		},
		dial: func(_ context.Context, _, address string) (net.Conn, error) {
			if address != "172.18.0.2:5432" && address != "postgres:5432" {
				return nil, fmt.Errorf("unexpected dial to %s", address)
			}
			a, b := net.Pipe()
			_ = b.Close()
			return a, nil
		},
	}
	fc := &fakeConnect{
		configFn: func(_ context.Context, name string) (map[string]string, error) {
			if name != "pg-orders-connector" {
				return nil, errFakeWrite
			}
			return map[string]string{
				"connector.class":   "io.debezium.connector.postgresql.PostgresConnector",
				"database.hostname": "postgres",
				"database.port":     "5432",
				"database.user":     "debezium",
				"database.password": "dbz",
				"database.dbname":   "orders",
			}, nil
		},
		validateFn: func(ctx context.Context, _ string, _ map[string]string) (connect.ValidateResult, error) {
			return connect.ValidateResult{}, blockUntilDone(ctx)
		},
	}
	w := &fakeWorker{name: "connect", lookup: func(_ context.Context, host string) ([]netip.Addr, error) {
		if host != "postgres" {
			return nil, ErrWorkerNoSuchHost
		}
		return addrsOf("192.0.2.1"), nil
	}, reach: func(ctx context.Context, _ netip.AddrPort) error { return blockUntilDone(ctx) }}
	d := Deps{
		Config:        config.Config{SourceDBURL: "postgres://debezium:dbz@postgres:5432/orders"},
		Connect:       fc,
		ConnectWorker: w,
		Location:      loc,
		StepTimeout:   step,
	}
	return d, owners.Pipeline{ID: "orders", Connectors: []string{"pg-orders-connector"}}
}

// scenarioSummary is net_probe {target: source}'s Summary in
// setup/db-unreachable-from-connect (evals/scenarios, C7): the recording there
// is this output, generated by this code over scenarioWorld.
const scenarioSummary = `net_probe source (postgres:5432): 2 of 5 checks failed
- connect_resolve postgres:5432 fail: the Connect worker connect resolves postgres to 192.0.2.1, a documentation address that routes nowhere (Piperoom resolves it to 172.18.0.2); TCP from the worker to 192.0.2.1:5432 did not connect within the step's time; fix: the worker resolves postgres differently from Piperoom: check the worker's hosts file (extra_hosts, an edited /etc/hosts) and its DNS servers, then restart the worker so its JVM drops any cached address
- connect_view fail: Connect did not finish validating connector pg-orders-connector within the step's time; fix: check that Connect is up, that the plugin class is installed on every worker, and that the Connect login may validate configs
- dns postgres:5432 ok: 172.18.0.2
- tcp postgres:5432 ok: TCP connected
- tls postgres:5432 skip: not configured with TLS`

func TestToolConnectResolveScenarioWorld(t *testing.T) {
	d, p := scenarioWorld(100 * time.Millisecond)
	res, err := run(t, d, scoped(p), `{"target":"source"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Summary != scenarioSummary {
		t.Fatalf("summary =\n%s\nwant\n%s", res.Summary, scenarioSummary)
	}
	data := res.Data.(probeData)
	var ids []string
	for _, c := range data.Checks {
		ids = append(ids, c.ID)
	}
	if strings.Join(ids, ",") != "dns,tcp,tls,connect_resolve,connect_view" {
		t.Fatalf("checks = %v", ids)
	}
	if strings.Contains(res.Summary, "dbz") {
		t.Fatalf("summary leaks the connector's password: %q", res.Summary)
	}
}

// Without a worker view the probe reads exactly as before: no
// connect_resolve check at all.
func TestToolConnectResolveOmittedWithoutWorker(t *testing.T) {
	d, p := scenarioWorld(50 * time.Millisecond)
	d.ConnectWorker = nil
	res, err := run(t, d, scoped(p), `{"target":"source"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if strings.Contains(res.Summary, "connect_resolve") || !strings.HasPrefix(res.Summary, "net_probe source (postgres:5432): 1 of 4 checks failed\n- connect_view fail") {
		t.Fatalf("summary = %q", res.Summary)
	}
}

// A worker view without a Connect client, or a connector whose config Connect
// does not serve (404), skips the check instead of guessing a host.
func TestToolConnectResolveSkips(t *testing.T) {
	d, p := scenarioWorld(50 * time.Millisecond)
	d.Connect = nil
	res, err := run(t, d, scoped(p), `{"target":"source"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(res.Summary, "connect_resolve skip") {
		t.Fatalf("summary = %q", res.Summary)
	}
	w := d.ConnectWorker.(*fakeWorker)
	if len(w.lookups)+len(w.reached) != 0 {
		t.Fatalf("worker asked without a connector host: %v %v", w.lookups, w.reached)
	}

	d, p = scenarioWorld(50 * time.Millisecond)
	fc := d.Connect.(*fakeConnect)
	fc.configFn = func(context.Context, string) (map[string]string, error) {
		return nil, errors.New("connect: GET /connectors/pg-orders-connector/config: 404 Not Found")
	}
	res, err = run(t, d, scoped(p), `{"target":"source"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(res.Summary, "connect_resolve skip: no source connector of the pipeline names this host") {
		t.Fatalf("summary = %q", res.Summary)
	}
}

// The connector's config is read once for both Connect checks.
func TestToolConnectResolveReadsConfigOnce(t *testing.T) {
	d, p := scenarioWorld(20 * time.Millisecond)
	fc := d.Connect.(*fakeConnect)
	inner := fc.configFn
	var reads int
	var mu sync.Mutex
	fc.configFn = func(ctx context.Context, name string) (map[string]string, error) {
		mu.Lock()
		reads++
		mu.Unlock()
		return inner(ctx, name)
	}
	if _, err := run(t, d, scoped(p), `{"target":"source"}`); err != nil {
		t.Fatal(err)
	}
	// One read builds the target (the connector named the host), one is the
	// running config both checks share.
	if reads != 2 {
		t.Fatalf("config reads = %d, want 2", reads)
	}
}
