package nettools

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// recordingLocation records every lookup and dial (with the dial's deadline)
// and touches no network: a dial returns one end of a net.Pipe.
type recordingLocation struct {
	mu        sync.Mutex
	lookups   []string
	dials     []string
	deadlines []time.Time
}

func (r *recordingLocation) Lookup(_ context.Context, host string) ([]netip.Addr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lookups = append(r.lookups, host)
	return []netip.Addr{netip.MustParseAddr("10.0.0.5")}, nil
}

func (r *recordingLocation) Dial(ctx context.Context, _, address string) (net.Conn, error) {
	r.mu.Lock()
	r.dials = append(r.dials, address)
	if dl, ok := ctx.Deadline(); ok {
		r.deadlines = append(r.deadlines, dl)
	}
	r.mu.Unlock()
	a, b := net.Pipe()
	_ = b.Close()
	return a, nil
}

func (r *recordingLocation) dialSet() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for _, d := range r.dials {
		if !seen[d] {
			seen[d] = true
			out = append(out, d)
		}
	}
	sort.Strings(out)
	return out
}

// blockingKafka's metadata read blocks until its context ends.
type blockingKafka struct{ fakeKafka }

func (b *blockingKafka) ClusterHealth(ctx context.Context, _ ...string) (clients.ClusterHealth, error) {
	<-ctx.Done()
	return clients.ClusterHealth{}, ctx.Err()
}

func dataOf(t *testing.T, v any) probeData {
	t.Helper()
	d, ok := v.(probeData)
	if !ok {
		t.Fatalf("data is %T", v)
	}
	return d
}

func marshalled(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Item 1: a metadata or validation error quoting a credential fails the check
// and never reaches the Summary or Data.
func TestToolRedactsClusterAndValidateErrors(t *testing.T) {
	ctx := scoped(owners.Pipeline{ID: "orders"})
	bootstrap := listenLoopback(t)
	kk := &fakeKafka{healthE: errors.New("SASL authentication failed for user=app password=hunter2")}
	res, err := run(t, Deps{Config: config.Config{KafkaBrokers: bootstrap}, Kafka: kk, StepTimeout: time.Second}, ctx, `{"target":"kafka"}`)
	if err != nil {
		t.Fatal(err)
	}
	if c := checkByID(t, dataOf(t, res.Data).Checks, "listeners"); c.Status != StatusFail || !strings.Contains(c.Problem, "cannot read the brokers' metadata") {
		t.Fatalf("listeners = %+v", c)
	}
	if strings.Contains(res.Summary, "hunter2") || strings.Contains(marshalled(t, res.Data), "hunter2") {
		t.Fatalf("listeners leaked the credential:\n%s\n%s", res.Summary, marshalled(t, res.Data))
	}

	src := listenLoopback(t)
	host, port := hostPortOfAddr(t, src)
	fc := &fakeConnect{
		configFn: func(context.Context, string) (map[string]string, error) {
			return map[string]string{"connector.class": "C", "database.hostname": host, "database.port": fmt.Sprint(port)}, nil
		},
		validateFn: func(context.Context, string, map[string]string) (connect.ValidateResult, error) {
			return connect.ValidateResult{}, errors.New("HTTP 500: bad config password=hunter2")
		},
	}
	p := owners.Pipeline{ID: "orders", Connectors: []string{"pg"}}
	res, err = run(t, Deps{Connect: fc, StepTimeout: time.Second}, scoped(p), `{"target":"source"}`)
	if err != nil {
		t.Fatal(err)
	}
	if c := checkByID(t, dataOf(t, res.Data).Checks, "connect_view"); c.Status != StatusFail || !strings.Contains(c.Problem, "Connect could not validate connector pg") {
		t.Fatalf("connect_view = %+v", c)
	}
	if strings.Contains(res.Summary, "hunter2") || strings.Contains(marshalled(t, res.Data), "hunter2") {
		t.Fatalf("connect_view leaked the credential:\n%s\n%s", res.Summary, marshalled(t, res.Data))
	}
}

// Item 2: a hostile issuer cannot forge a summary line or carry a bidi
// override through the expired-certificate Fix, and a long subject is clipped.
func TestProberTLSHostileIssuerClipped(t *testing.T) {
	now := time.Now()
	forged := "\n- tcp 10.0.0.1:1 ok"
	addr, roots, _, _ := tlsServer(t, certOpts{
		cn:  "EvilCA" + forged + "‮",
		dns: []string{"localhost"}, notBefore: now.Add(-30 * 24 * time.Hour), notAfter: now.Add(-3 * 24 * time.Hour),
	})
	checks := Prober{}.Probe(context.Background(), Target{Endpoints: []Endpoint{{Addr: addr, TLS: true,
		Config: &tls.Config{RootCAs: roots, ServerName: "localhost"}}}})
	c := checkByID(t, checks, "tls")
	if c.Status != StatusFail || !strings.Contains(c.Problem, "expired") || !strings.Contains(c.Fix, "EvilCA") {
		t.Fatalf("tls = %+v", c)
	}
	for _, s := range []string{c.Problem, c.Fix, c.Detail} {
		if strings.ContainsAny(s, "\n\r‮") {
			t.Fatalf("remote text reached a check raw: %q", s)
		}
	}
	res := (&netProbe{}).result(probeData{Target: LabelKafka, Kind: KindKafka, Endpoints: []string{addr}, Checks: checks})
	if strings.Contains(res.Summary, forged) || strings.Contains(res.Summary, "‮") {
		t.Fatalf("summary carries a forged line:\n%s", res.Summary)
	}

	long := strings.Repeat("x", 500)
	addr, roots, _, _ = tlsServer(t, certOpts{
		cn:  long,
		dns: []string{"localhost"}, notBefore: now.Add(-time.Hour), notAfter: now.Add(90 * 24 * time.Hour),
	})
	c = checkByID(t, Prober{}.Probe(context.Background(), Target{Endpoints: []Endpoint{{Addr: addr, TLS: true,
		Config: &tls.Config{RootCAs: roots, ServerName: "localhost"}}}}), "tls")
	if strings.Contains(c.Detail, strings.Repeat("x", 121)) || !strings.Contains(c.Detail, "x…") {
		t.Fatalf("subject not clipped in detail (%d bytes): %q", len(c.Detail), c.Detail)
	}
}

// Item 3: Deps.Location reaches the bootstrap probe and the advertised
// listener's probe; nothing dials around it.
func TestLocationReachesListenerProbes(t *testing.T) {
	loc := &recordingLocation{}
	kk := &fakeKafka{health: clients.ClusterHealth{Brokers: []clients.BrokerHealth{
		{NodeID: 1, Host: "boot.test", Port: 9092},
		{NodeID: 2, Host: "10.9.9.9", Port: 9092},
	}}}
	res, err := run(t, Deps{Config: config.Config{KafkaBrokers: "boot.test:9092"}, Kafka: kk, Location: loc, StepTimeout: 200 * time.Millisecond},
		scoped(owners.Pipeline{ID: "orders"}), `{"target":"kafka"}`)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(loc.dialSet(), ","); got != "10.9.9.9:9092,boot.test:9092" {
		t.Fatalf("dials = %v\n%s", loc.dials, res.Summary)
	}
	if got := strings.Join(loc.lookups, ","); got != "boot.test" {
		t.Fatalf("lookups = %v", loc.lookups)
	}
	if c := checkByID(t, dataOf(t, res.Data).Checks, "listeners"); c.Status != StatusWarn {
		t.Fatalf("listeners = %+v", c)
	}
}

// Item 4a: the tool's 10 s budget bounds a step even when the step bound is
// longer and the caller set no deadline.
func TestToolWholeBudgetBoundsSteps(t *testing.T) {
	loc := &recordingLocation{}
	start := time.Now()
	_, err := run(t, Deps{Config: config.Config{ConnectURL: "http://10.0.0.1:8083"}, Location: loc, StepTimeout: time.Hour},
		scoped(owners.Pipeline{ID: "orders"}), `{"target":"connect"}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(loc.deadlines) == 0 {
		t.Fatal("dial had no deadline")
	}
	for _, dl := range loc.deadlines {
		if dl.After(start.Add(timeout + time.Second)) {
			t.Fatalf("dial deadline %v after start, want at most %v", dl.Sub(start), timeout)
		}
	}
}

// Item 4b: a metadata read that hangs is bounded by the step timeout.
func TestToolClusterHealthStepTimeout(t *testing.T) {
	bootstrap := listenLoopback(t)
	start := time.Now()
	res, err := run(t, Deps{Config: config.Config{KafkaBrokers: bootstrap}, Kafka: &blockingKafka{}, StepTimeout: 50 * time.Millisecond},
		scoped(owners.Pipeline{ID: "orders"}), `{"target":"kafka"}`)
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("took %v on a 50ms step", took)
	}
	c := checkByID(t, dataOf(t, res.Data).Checks, "listeners")
	if c.Status != StatusFail || c.Problem != "reading the brokers' metadata did not finish within the step's time" {
		t.Fatalf("listeners = %+v", c)
	}
}

// Item 4c: a validation that hangs is bounded by the step timeout.
func TestToolValidateConfigStepTimeout(t *testing.T) {
	src := listenLoopback(t)
	host, port := hostPortOfAddr(t, src)
	fc := &fakeConnect{
		configFn: func(context.Context, string) (map[string]string, error) {
			return map[string]string{"connector.class": "C", "database.hostname": host, "database.port": fmt.Sprint(port)}, nil
		},
		validateFn: func(ctx context.Context, _ string, _ map[string]string) (connect.ValidateResult, error) {
			<-ctx.Done()
			return connect.ValidateResult{}, ctx.Err()
		},
	}
	start := time.Now()
	res, err := run(t, Deps{Connect: fc, StepTimeout: 50 * time.Millisecond},
		scoped(owners.Pipeline{ID: "orders", Connectors: []string{"pg"}}), `{"target":"source"}`)
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("took %v on a 50ms step", took)
	}
	c := checkByID(t, dataOf(t, res.Data).Checks, "connect_view")
	if c.Status != StatusFail || c.Problem != "Connect did not finish validating connector pg within the step's time" {
		t.Fatalf("connect_view = %+v", c)
	}
}

// Item 4d: resolving the source target reads connector configs within the
// configured step timeout, not the 3 s default, and within the caller's.
func TestToolConnectorConfigReadUsesStepTimeout(t *testing.T) {
	fc := &fakeConnect{configFn: func(ctx context.Context, _ string) (map[string]string, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	p := owners.Pipeline{ID: "orders", Connectors: []string{"pg"}}
	start := time.Now()
	_, err := run(t, Deps{Connect: fc, StepTimeout: 50 * time.Millisecond}, scoped(p), `{"target":"source"}`)
	if err == nil || !strings.Contains(err.Error(), "has no address configured") || !strings.Contains(err.Error(), "config unreadable") {
		t.Fatalf("err = %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("connector config read took %v on a 50ms step", took)
	}

	ctx, cancel := context.WithTimeout(scoped(p), 50*time.Millisecond)
	defer cancel()
	start = time.Now()
	if _, err := run(t, Deps{Connect: fc, StepTimeout: time.Hour}, ctx, `{"target":"source"}`); err == nil {
		t.Fatal("want an error")
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("connector config read took %v on a 50ms caller budget", took)
	}
}

// Item 5: the Connect check validates the connector that names the target's
// host, and skips when none does.
func TestConnectViewPicksConnectorNamingHost(t *testing.T) {
	hosts := map[string]string{"a": "other.test", "b": "DB.test"}
	var validated []string
	fc := &fakeConnect{
		configFn: func(_ context.Context, name string) (map[string]string, error) {
			return map[string]string{"connector.class": "C", "database.hostname": hosts[name]}, nil
		},
		validateFn: func(_ context.Context, _ string, cfg map[string]string) (connect.ValidateResult, error) {
			validated = append(validated, cfg["database.hostname"])
			return connect.ValidateResult{}, nil
		},
	}
	tool := &netProbe{d: Deps{Connect: fc, StepTimeout: time.Second}}
	tgt := Target{Label: LabelSource, Kind: KindSource, Endpoints: []Endpoint{{Addr: "db.test:5432"}}, Connectors: []string{"a", "b"}}
	c := tool.connectView(context.Background(), tgt)
	if c.Status != StatusOK || !strings.Contains(c.Detail, "validated b ") || len(validated) != 1 || validated[0] != "DB.test" {
		t.Fatalf("connect_view = %+v, validated %v", c, validated)
	}
	hosts["b"] = "elsewhere.test"
	validated = nil
	c = tool.connectView(context.Background(), tgt)
	if c.Status != StatusSkip || c.Detail != "no source connector of the pipeline names this host" || len(validated) != 0 {
		t.Fatalf("connect_view = %+v, validated %v", c, validated)
	}
}

// Item 6: a target that resolves to no endpoint is an error, from the room and
// from the setup runner's seam.
func TestNoEndpointIsAnError(t *testing.T) {
	fc := &fakeConnect{configFn: func(context.Context, string) (map[string]string, error) {
		return map[string]string{"connector.class": "C"}, nil
	}}
	loc := &recordingLocation{}
	_, err := run(t, Deps{Connect: fc, Location: loc}, scoped(owners.Pipeline{ID: "orders", Connectors: []string{"sink"}}), `{"target":"source"}`)
	if err == nil || !strings.Contains(err.Error(), `target "source" has no address configured`) {
		t.Fatalf("err = %v", err)
	}
	src := TargetsFromSetup([]events.SetupTarget{{Kind: "connect", Address: ""}})
	_, _, err = Prober{Loc: loc}.ProbeLabel(context.Background(), src, owners.Pipeline{}, "connect")
	if err == nil || !strings.Contains(err.Error(), `target "connect" has no address configured`) {
		t.Fatalf("ProbeLabel err = %v", err)
	}
	if len(loc.dials)+len(loc.lookups) != 0 {
		t.Fatalf("probed with no endpoint: %v %v", loc.dials, loc.lookups)
	}
}

// Item 7: an advertised address that is not a host fails the listener check
// and names the node.
func TestToolAdvertisedBadHostFails(t *testing.T) {
	bootstrap := listenLoopback(t)
	bHost, bPort := hostPortOfAddr(t, bootstrap)
	kk := &fakeKafka{health: clients.ClusterHealth{Brokers: []clients.BrokerHealth{
		{NodeID: 1, Host: bHost, Port: int32(bPort)},
		{NodeID: 7, Host: "bad host!", Port: 9092},
	}}}
	res, err := run(t, Deps{Config: config.Config{KafkaBrokers: bootstrap}, Kafka: kk, StepTimeout: time.Second},
		scoped(owners.Pipeline{ID: "orders"}), `{"target":"kafka"}`)
	if err != nil {
		t.Fatal(err)
	}
	c := checkByID(t, dataOf(t, res.Data).Checks, "listeners")
	if c.Status != StatusFail || !strings.Contains(c.Problem, "node 7 advertises an unusable address") {
		t.Fatalf("listeners = %+v", c)
	}
	if strings.Contains(res.Summary, "bad host!") {
		t.Fatalf("summary echoes the advertised text: %s", res.Summary)
	}
}

// Item 8: on a TLS cluster an advertised listener outside the configured list
// gets a certificate check.
func TestToolAdvertisedListenerGetsTLS(t *testing.T) {
	now := time.Now()
	opts := certOpts{dns: []string{"localhost"}, ips: []string{"127.0.0.1"}, notBefore: now.Add(-time.Hour), notAfter: now.Add(90 * 24 * time.Hour)}
	bootstrap, _, caFile, _ := tlsServer(t, opts)
	advertised, _, _, _ := tlsServer(t, opts)
	bHost, bPort := hostPortOfAddr(t, bootstrap)
	aHost, aPort := hostPortOfAddr(t, advertised)
	kk := &fakeKafka{health: clients.ClusterHealth{Brokers: []clients.BrokerHealth{
		{NodeID: 1, Host: bHost, Port: int32(bPort)},
		{NodeID: 2, Host: aHost, Port: int32(aPort)},
	}}}
	cfg := config.Config{KafkaBrokers: bootstrap, KafkaSecurityProtocol: "SSL", KafkaTLSCAFile: caFile}
	res, err := run(t, Deps{Config: cfg, Kafka: kk, StepTimeout: time.Second}, scoped(owners.Pipeline{ID: "orders"}), `{"target":"kafka"}`)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, c := range dataOf(t, res.Data).Checks {
		if c.ID == "tls" && c.Addr == advertised {
			found = true
			if c.Status == StatusSkip {
				t.Fatalf("advertised listener's tls skipped: %+v", c)
			}
		}
	}
	if !found {
		t.Fatalf("no tls check for %s: %s", advertised, res.Summary)
	}
}

// Item 9: a connector's database.hostname that is not a host is refused with a
// note and yields no endpoint.
func TestConnectorHostnameRefused(t *testing.T) {
	for _, h := range []string{"db host/../x", "a@b"} {
		fc := &fakeConnect{configFn: func(context.Context, string) (map[string]string, error) {
			return map[string]string{"database.hostname": h}, nil
		}}
		tgt, err := RoomTargets(config.Config{}, fc).Target(context.Background(), owners.Pipeline{ID: "p", Connectors: []string{"pg"}}, LabelSource)
		if err != nil {
			t.Fatal(err)
		}
		if len(tgt.Endpoints) != 0 || !strings.Contains(tgt.Note, "connector pg: database.hostname is not a host") {
			t.Fatalf("%q: target = %+v", h, tgt)
		}
	}
}

// Item 10: a long configured broker list stops at maxEndpoints, and a long
// advertised list at maxListeners, with the count in the detail.
func TestCapsEndpointsAndListeners(t *testing.T) {
	var brokers []string
	for i := 0; i < 20; i++ {
		brokers = append(brokers, fmt.Sprintf("b%d.test:9092", i))
	}
	tgt, err := RoomTargets(config.Config{KafkaBrokers: strings.Join(brokers, ",")}, nil).Target(context.Background(), owners.Pipeline{ID: "p"}, LabelKafka)
	if err != nil {
		t.Fatal(err)
	}
	if len(tgt.Endpoints) != 16 {
		t.Fatalf("endpoints = %d, want 16", len(tgt.Endpoints))
	}

	var adv []clients.BrokerHealth
	for i := 0; i < 20; i++ {
		adv = append(adv, clients.BrokerHealth{NodeID: int32(i), Host: fmt.Sprintf("10.1.0.%d", i+1), Port: 9092})
	}
	loc := &recordingLocation{}
	res, err := run(t, Deps{Config: config.Config{KafkaBrokers: "10.0.0.1:9092"}, Kafka: &fakeKafka{health: clients.ClusterHealth{Brokers: adv}},
		Location: loc, StepTimeout: time.Second}, scoped(owners.Pipeline{ID: "orders"}), `{"target":"kafka"}`)
	if err != nil {
		t.Fatal(err)
	}
	advertisedDials := 0
	for _, d := range loc.dialSet() {
		if strings.HasPrefix(d, "10.1.0.") {
			advertisedDials++
		}
	}
	if advertisedDials > 12 {
		t.Fatalf("dialed %d advertised listeners, want at most 12", advertisedDials)
	}
	if c := checkByID(t, dataOf(t, res.Data).Checks, "listeners"); !strings.HasPrefix(c.Detail, "12 of 20 brokers probed") {
		t.Fatalf("listeners = %+v", c)
	}
}

// Item 11: a certificate that expired earlier today reads as expired.
func TestProberTLSExpiredToday(t *testing.T) {
	now := time.Now()
	addr, roots, _, _ := tlsServer(t, certOpts{
		dns: []string{"localhost"}, notBefore: now.Add(-48 * time.Hour), notAfter: now.Add(-time.Hour),
	})
	c := checkByID(t, Prober{}.Probe(context.Background(), Target{Endpoints: []Endpoint{{Addr: addr, TLS: true,
		Config: &tls.Config{RootCAs: roots, ServerName: "localhost"}}}}), "tls")
	if c.Status != StatusFail || !strings.Contains(c.Problem, "expired") || !strings.Contains(c.Detail, "(-1 days)") {
		t.Fatalf("tls = %+v", c)
	}
}

// Item 12: clip caps length; the TLS step sends the dialed host or the
// configured server name as SNI; the DNS detail lists at most 5 answers.
func TestClipLengthCap(t *testing.T) {
	got := clip(strings.Repeat("y", 500))
	if n := utf8.RuneCountInString(got); n > 121 || !strings.HasSuffix(got, "…") {
		t.Fatalf("clip = %d runes %q", n, got)
	}
	if got := clip(strings.Repeat("y", 120)); got != strings.Repeat("y", 120) {
		t.Fatalf("clip cut a 120-rune text: %q", got)
	}
}

func TestProberTLSSendsSNI(t *testing.T) {
	now := time.Now()
	var mu sync.Mutex
	var sni []string
	srv, _, _, _ := tlsServer(t, certOpts{
		dns: []string{"sni.test"}, notBefore: now.Add(-time.Hour), notAfter: now.Add(90 * 24 * time.Hour),
		onHello: func(h *tls.ClientHelloInfo) { mu.Lock(); sni = append(sni, h.ServerName); mu.Unlock() },
	})
	loc := &fakeLocation{
		lookup: func(context.Context, string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
		},
		dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, srv)
		},
	}
	for _, tc := range []struct {
		cfg  *tls.Config
		want string
	}{
		{nil, "sni.test"},
		{&tls.Config{ServerName: "override.test"}, "override.test"},
	} {
		mu.Lock()
		sni = nil
		mu.Unlock()
		e := Endpoint{Addr: "sni.test:443", TLS: true}
		if tc.cfg != nil {
			e.Config = tc.cfg
		}
		Prober{Loc: loc, StepTimeout: time.Second}.Probe(context.Background(), Target{Endpoints: []Endpoint{e}})
		mu.Lock()
		got := strings.Join(sni, ",")
		mu.Unlock()
		if got != tc.want {
			t.Fatalf("SNI = %q, want %q", got, tc.want)
		}
	}
}

func TestProberDNSAnswerCap(t *testing.T) {
	loc := &fakeLocation{lookup: func(context.Context, string) ([]netip.Addr, error) {
		var out []netip.Addr
		for i := 1; i <= 8; i++ {
			out = append(out, netip.AddrFrom4([4]byte{10, 0, 0, byte(i)}))
		}
		return out, nil
	}, dial: func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("no") }}
	c := checkByID(t, Prober{Loc: loc}.Probe(context.Background(), Target{Endpoints: []Endpoint{{Addr: "many.test:1"}}}), "dns")
	if parts := strings.Split(c.Detail, ", "); len(parts) != 5 || parts[4] != "10.0.0.5" {
		t.Fatalf("dns detail = %q", c.Detail)
	}
}
