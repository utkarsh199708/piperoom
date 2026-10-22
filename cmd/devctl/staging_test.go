package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"piperoom/internal/config"
)

type composeService struct {
	Profiles    []string       `yaml:"profiles"`
	Image       string         `yaml:"image"`
	Ports       []string       `yaml:"ports"`
	DependsOn   map[string]any `yaml:"depends_on"`
	Environment map[string]any `yaml:"environment"`
}

func readCompose(t *testing.T) map[string]composeService {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoStack(t).root, "dev", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Services map[string]composeService `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return doc.Services
}

func env(t *testing.T, s composeService, key string) string {
	t.Helper()
	v, ok := s.Environment[key]
	if !ok {
		t.Fatalf("environment %s missing", key)
	}
	return fmt.Sprint(v)
}

// X8d TC1 (structure; the live run needs Docker): the profile adds exactly the two
// staging services, on their own ports, groups and internal topics.
func TestComposeStagingProfile(t *testing.T) {
	svcs := readCompose(t)
	connect, registry := svcs["connect-staging"], svcs["schema-registry-staging"]
	prodConnect, prodRegistry := svcs["connect"], svcs["schema-registry"]
	if connect.Image == "" || registry.Image == "" {
		t.Fatal("staging services missing")
	}
	for name, s := range map[string]composeService{"connect-staging": connect, "schema-registry-staging": registry} {
		if !reflect.DeepEqual(s.Profiles, []string{stagingProfile}) {
			t.Errorf("%s profiles = %v, want [%s]: `devctl up` must not start it", name, s.Profiles, stagingProfile)
		}
		for _, d := range []string{"connect", "schema-registry", "devconsumer"} {
			if _, ok := s.DependsOn[d]; ok {
				t.Errorf("%s depends on production service %s: staging must not wait on production", name, d)
			}
		}
	}

	// Host ports: documented, on loopback (no login), and no clash with any service.
	if !reflect.DeepEqual(connect.Ports, []string{"127.0.0.1:18083:8083"}) {
		t.Errorf("connect-staging ports = %v, want 127.0.0.1:18083:8083", connect.Ports)
	}
	if !reflect.DeepEqual(registry.Ports, []string{"127.0.0.1:18081:8081"}) {
		t.Errorf("schema-registry-staging ports = %v, want 127.0.0.1:18081:8081", registry.Ports)
	}
	seen := map[string]string{}
	for name, s := range svcs {
		for _, p := range s.Ports {
			parts := strings.Split(p, ":")
			host := parts[len(parts)-2]
			if other, dup := seen[host]; dup {
				t.Errorf("host port %s published by %s and %s", host, other, name)
			}
			seen[host] = name
		}
	}

	// Separate group.id and separate config/offset/status topics: sharing any would
	// merge the two workers into one Connect cluster.
	for _, k := range []string{"GROUP_ID", "CONFIG_STORAGE_TOPIC", "OFFSET_STORAGE_TOPIC", "STATUS_STORAGE_TOPIC"} {
		got, prod := env(t, connect, k), env(t, prodConnect, k)
		if got == "" || got == prod {
			t.Errorf("connect-staging %s = %q, production %q: must be set and different", k, got, prod)
		}
	}
	topics := []string{
		env(t, connect, "CONFIG_STORAGE_TOPIC"), env(t, connect, "OFFSET_STORAGE_TOPIC"), env(t, connect, "STATUS_STORAGE_TOPIC"),
	}
	if d := duplicates(topics); d != "" {
		t.Errorf("connect-staging internal topics share the name %q", d)
	}
	for _, topic := range topics {
		if !slices.Contains(stagingTopics, topic) {
			t.Errorf("internal topic %q is not in stagingTopics, so `down --staging` would leave it behind", topic)
		}
	}

	// The registry has its own store topic and group, else the two registries elect one leader.
	if got, prod := env(t, registry, "SCHEMA_REGISTRY_KAFKASTORE_TOPIC"), "_schemas"; got == prod || !slices.Contains(stagingTopics, got) {
		t.Errorf("registry store topic = %q: must differ from %q and be in stagingTopics", got, prod)
	}
	if got, prod := env(t, registry, "SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID"), "schema-registry"; got == prod {
		t.Errorf("registry group id = %q, same as the production default %q", got, prod)
	}
	if _, set := prodRegistry.Environment["SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID"]; set {
		t.Error("production registry sets a group id: its default must stay as it was")
	}
	if env(t, registry, "SCHEMA_REGISTRY_HOST_NAME") != "schema-registry-staging" {
		t.Error("registry host name must be its own service name")
	}

	// Staging Connect converts through the staging registry, never the production one.
	for _, k := range []string{"CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL", "CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL"} {
		if got := env(t, connect, k); got != "http://schema-registry-staging:8081" {
			t.Errorf("%s = %q, want the staging registry", k, got)
		}
	}
	if connect.Image != prodConnect.Image {
		t.Errorf("image %q != production %q: the image should be built once", connect.Image, prodConnect.Image)
	}
}

// The default stack is what it was: only these services start without a profile.
func TestComposeDefaultServicesUnchanged(t *testing.T) {
	var got []string
	for name, s := range readCompose(t) {
		if len(s.Profiles) == 0 {
			got = append(got, name)
		}
	}
	sort.Strings(got)
	want := []string{"connect", "devconsumer", "kafka", "postgres", "schema-registry"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("services without a profile = %v, want %v", got, want)
	}
}

func duplicates(list []string) string {
	seen := map[string]bool{}
	for _, x := range list {
		if seen[x] {
			return x
		}
		seen[x] = true
	}
	return ""
}

// The host URLs the code and the owners file use are the compose ports.
func TestStagingURLsMatchCompose(t *testing.T) {
	svcs := readCompose(t)
	for url, svc := range map[string]string{
		defaultStagingConnectURL:  "connect-staging",
		defaultStagingRegistryURL: "schema-registry-staging",
	} {
		port := url[strings.LastIndex(url, ":")+1:]
		if len(svcs[svc].Ports) != 1 || !strings.Contains(svcs[svc].Ports[0], ":"+port+":") {
			t.Errorf("%s publishes %v, but devctl and the owners file use %s", svc, svcs[svc].Ports, url)
		}
	}
}

// dev/owners.staging.yaml is dev/owners.yaml plus a staging block on orders. It
// is parsed generically: the strict owners loader only knows `staging:` once X8a lands.
func TestOwnersStagingMatchesOwners(t *testing.T) {
	st := repoStack(t)
	read := func(name string) map[string]any {
		raw, err := os.ReadFile(filepath.Join(st.root, "dev", name))
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]any
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		return doc
	}
	plain, staging := read("owners.yaml"), read("owners.staging.yaml")
	pipes, _ := staging["pipelines"].([]any)
	var declared int
	for _, p := range pipes {
		m := p.(map[string]any)
		blk, ok := m["staging"]
		if !ok {
			continue
		}
		declared++
		if m["id"] != "orders" {
			t.Errorf("staging declared on %v, want orders (the only CDC pipeline)", m["id"])
		}
		conns := blk.(map[string]any)["connections"].(map[string]any)
		for key, want := range map[string]string{"connect_url": defaultStagingConnectURL, "schema_registry_url": defaultStagingRegistryURL} {
			ref, _ := conns[key].(map[string]any)
			if ref["value"] != want {
				t.Errorf("staging connections.%s = %v, want {value: %s}", key, conns[key], want)
			}
		}
		for key := range conns {
			if key != "connect_url" && key != "schema_registry_url" {
				t.Errorf("staging connections.%s: this file declares only the two staging URLs", key)
			}
		}
		delete(m, "staging")
	}
	if declared != 1 {
		t.Fatalf("staging blocks = %d, want 1", declared)
	}
	if !reflect.DeepEqual(plain, staging) {
		t.Fatalf("dev/owners.staging.yaml drifted from dev/owners.yaml:\nplain   %v\nstaging %v", plain, staging)
	}
	// The staging URLs differ from the production defaults (the owners rule "never production").
	if _, err := resolveStaging(config.Config{ConnectURL: "http://localhost:8083", SchemaRegistryURL: "http://localhost:8081"}, func(string) string { return "" }); err != nil {
		t.Fatalf("default staging URLs collide with the production defaults: %v", err)
	}
}

// X8d TC3: ports and topic names are in dev/README.md.
func TestReadmeDocumentsStaging(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoStack(t).root, "dev", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(raw)
	words := append([]string{
		"18083", "18081", "up --staging", "down --staging", "reset --staging", "owners.staging.yaml",
		stagingTopicPrefix, stagingSlot, stagingPublication, "piperoom-connect-staging", "schema-registry-staging", "connect-staging",
	}, stagingTopics...)
	for _, w := range words {
		if !strings.Contains(doc, w) {
			t.Errorf("dev/README.md does not mention %q", w)
		}
	}
}

func TestResolveStaging(t *testing.T) {
	prod := config.Config{ConnectURL: "http://localhost:8083", SchemaRegistryURL: "http://localhost:8081"}
	tests := []struct {
		name    string
		cfg     config.Config
		env     map[string]string
		want    stagingEndpoints
		wantErr string
	}{
		{"defaults", prod, nil, stagingEndpoints{defaultStagingConnectURL, defaultStagingRegistryURL}, ""},
		{"override and trailing slash", prod, map[string]string{stagingConnectEnv: " http://stg:8083/ "},
			stagingEndpoints{"http://stg:8083", defaultStagingRegistryURL}, ""},
		{"no production registry", config.Config{ConnectURL: "http://localhost:8083"}, nil,
			stagingEndpoints{defaultStagingConnectURL, defaultStagingRegistryURL}, ""},
		{"connect is production", prod, map[string]string{stagingConnectEnv: "http://localhost:8083"}, stagingEndpoints{}, stagingConnectEnv + " is the production endpoint"},
		{"connect is production, other spelling", prod, map[string]string{stagingConnectEnv: "HTTP://LocalHost:8083/"}, stagingEndpoints{}, "production endpoint"},
		{"registry is production, default port", config.Config{ConnectURL: "http://c:8083", SchemaRegistryURL: "http://reg"},
			map[string]string{stagingRegistryEnv: "http://reg:80"}, stagingEndpoints{}, stagingRegistryEnv + " is the production endpoint"},
		{"not a url", prod, map[string]string{stagingConnectEnv: "localhost:18083"}, stagingEndpoints{}, "http or https"},
		{"ftp", prod, map[string]string{stagingRegistryEnv: "ftp://x"}, stagingEndpoints{}, "http or https"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveStaging(tt.cfg, func(k string) string { return tt.env[k] })
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("resolveStaging = %v, %v, want %v", got, err, tt.want)
			}
		})
	}
}

func TestStagingConnectorConfig(t *testing.T) {
	st := repoStack(t)
	decode := func(b []byte, err error) map[string]any {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	base := decode(st.connectorConfigWith(nil))
	stg := decode(st.connectorConfigWith(stagingConnectorOverrides()))
	for k, want := range stagingConnectorOverrides() {
		if stg[k] != want || base[k] == want {
			t.Errorf("%s: staging %v, production %v: staging must be %q and production must differ", k, stg[k], base[k], want)
		}
	}
	// Everything else is the same connector.
	for k, v := range base {
		if _, overridden := stagingConnectorOverrides()[k]; !overridden && stg[k] != v {
			t.Errorf("%s changed to %v; staging registers the same connector", k, stg[k])
		}
	}
	added := 0
	for k := range stagingConnectorOverrides() {
		if _, ok := base[k]; !ok {
			added++
		}
	}
	if len(stg) != len(base)+added {
		t.Errorf("staging config has %d keys, production %d (+%d new)", len(stg), len(base), added)
	}
}

func TestStagingFlag(t *testing.T) {
	for _, tt := range []struct {
		rest    []string
		want    bool
		wantErr bool
	}{
		{nil, false, false},
		{[]string{"--staging"}, true, false},
		{[]string{"--stage"}, false, true},
		{[]string{"--staging", "x"}, false, true},
	} {
		got, err := stagingFlag("up", tt.rest)
		if got != tt.want || (err != nil) != tt.wantErr {
			t.Errorf("stagingFlag(%v) = %v, %v", tt.rest, got, err)
		}
		if err != nil && !strings.Contains(err.Error(), "usage: devctl up [--staging]") {
			t.Errorf("usage error = %q", err)
		}
	}
	// A bad argument stops before the stack lock or any compose call.
	for _, cmd := range []string{"up", "down", "reset"} {
		err := runCommand(context.Background(), &stack{}, cmd, []string{"--bogus"})
		if err == nil || !strings.Contains(err.Error(), "usage: devctl "+cmd) {
			t.Errorf("devctl %s --bogus = %v, want a usage error", cmd, err)
		}
	}
}

// fakeConnect plays a Connect worker (or registry) and records its requests.
type fakeConnect struct {
	*httptest.Server
	mu    sync.Mutex
	calls []string // "METHOD path"
	puts  map[string]map[string]any
}

func newFakeConnect(t *testing.T) *fakeConnect {
	t.Helper()
	f := &fakeConnect{puts: map[string]map[string]any{}}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.calls = append(f.calls, r.Method+" "+r.URL.Path)
		switch {
		case r.Method == http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			var cfg map[string]any
			_ = json.Unmarshal(b, &cfg)
			f.puts[r.URL.Path] = cfg
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/status"):
			_, _ = io.WriteString(w, `{"connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING"}]}`)
		default:
			_, _ = io.WriteString(w, `[]`)
		}
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeConnect) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeConnect) saw(call string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Contains(f.calls, call)
}

type composeRecorder struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (c *composeRecorder) fn(_ context.Context, _ map[string]string, args ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, strings.Join(args, " "))
	return c.err
}

func stagingStack(t *testing.T, prod, stg *fakeConnect, reg *fakeConnect) (*stack, *composeRecorder) {
	t.Helper()
	t.Setenv(stagingConnectEnv, stg.URL)
	t.Setenv(stagingRegistryEnv, reg.URL)
	rec := &composeRecorder{}
	s := repoStack(t)
	s.cfg = config.Config{ConnectURL: prod.URL, SchemaRegistryURL: "http://127.0.0.1:1"}
	s.http = &http.Client{Timeout: 5 * time.Second}
	s.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	s.composeFn = rec.fn
	s.poll = 5 * time.Millisecond
	return s, rec
}

// X8d TC1: `up --staging` starts the profile, registers the same connector on
// staging under its own prefix and slot, and leaves production's registration alone.
func TestUpStagingRegistersConnectorOnStaging(t *testing.T) {
	prod, stg, reg := newFakeConnect(t), newFakeConnect(t), newFakeConnect(t)
	s, rec := stagingStack(t, prod, stg, reg)
	for i := 0; i < 2; i++ { // the second run is the same calls again: idempotent
		if err := s.upWith(context.Background(), true); err != nil {
			t.Fatalf("up --staging run %d: %v", i+1, err)
		}
	}
	if want := "--profile staging up -d --build --wait"; rec.calls[0] != want {
		t.Fatalf("compose = %q, want %q", rec.calls[0], want)
	}
	path := "/connectors/" + connectorName + "/config"
	sc, pc := stg.puts[path], prod.puts[path]
	if sc == nil || pc == nil {
		t.Fatalf("connector not registered: staging %v, production %v", sc, pc)
	}
	if sc["topic.prefix"] != stagingTopicPrefix || sc["slot.name"] != stagingSlot || sc["publication.name"] != stagingPublication {
		t.Errorf("staging connector = %v: needs its own prefix, slot and publication", sc)
	}
	if pc["topic.prefix"] != "pg" || pc["slot.name"] != "debezium_orders" || pc["publication.name"] != nil {
		t.Errorf("production connector changed: %v", pc)
	}
	if !reg.saw("GET /subjects") {
		t.Error("staging registry was never checked")
	}
}

// The default `up` is untouched: the old compose call, and no request to a staging URL.
func TestUpWithoutStagingTouchesNothingOfStaging(t *testing.T) {
	prod, stg, reg := newFakeConnect(t), newFakeConnect(t), newFakeConnect(t)
	s, rec := stagingStack(t, prod, stg, reg)
	if err := s.up(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rec.calls, []string{"up -d --build --wait"}) {
		t.Fatalf("compose calls = %q", rec.calls)
	}
	if stg.count() != 0 || reg.count() != 0 {
		t.Fatalf("staging got %d and %d requests from a plain up", stg.count(), reg.count())
	}
}

// E4: staging pointed at production is refused before compose runs; a stack that
// is down is a plain error, not a panic.
func TestUpStagingErrors(t *testing.T) {
	prod, stg, reg := newFakeConnect(t), newFakeConnect(t), newFakeConnect(t)
	s, rec := stagingStack(t, prod, stg, reg)
	t.Setenv(stagingConnectEnv, prod.URL)
	err := s.upWith(context.Background(), true)
	if err == nil || !strings.Contains(err.Error(), "production endpoint") || len(rec.calls) != 0 {
		t.Fatalf("err = %v, compose calls = %v: want a refusal before docker runs", err, rec.calls)
	}

	t.Setenv(stagingConnectEnv, stg.URL)
	rec.err = errors.New("docker: not running")
	if err := s.upWith(context.Background(), true); err == nil || !strings.Contains(err.Error(), "docker: not running") {
		t.Fatalf("err = %v, want the compose error", err)
	}
}

// X8d TC2: reset names the staging profile when it tears down (compose's `down`
// ignores containers of a profile that is off), starts staging only on request,
// and its only SQL is the warehouse row count: it never goes near a room database.
func TestResetTearsStagingDownAndLeavesRoomDBAlone(t *testing.T) {
	for _, staging := range []bool{false, true} {
		t.Run(fmt.Sprintf("staging=%v", staging), func(t *testing.T) {
			prod, stg, reg := newFakeConnect(t), newFakeConnect(t), newFakeConnect(t)
			s, rec := stagingStack(t, prod, stg, reg)
			var dbs, queries []string
			s.sqlFn = func(_ context.Context, db string, sql ...string) (string, error) {
				dbs = append(dbs, db)
				queries = append(queries, sql...)
				return "5", nil
			}
			if err := s.resetWith(context.Background(), staging); err != nil {
				t.Fatal(err)
			}
			if rec.calls[0] != "--profile staging down -v --remove-orphans" {
				t.Fatalf("teardown = %q", rec.calls[0])
			}
			up := "up -d --build --wait"
			if staging {
				up = "--profile staging " + up
			}
			if rec.calls[1] != up || len(rec.calls) != 2 {
				t.Fatalf("compose calls = %q, want teardown then %q", rec.calls, up)
			}
			if staging != (stg.count() > 0) {
				t.Fatalf("staging requests = %d with staging=%v", stg.count(), staging)
			}
			for _, d := range dbs {
				if d != "warehouse" {
					t.Errorf("reset ran SQL in database %q", d)
				}
			}
			for _, a := range append(append([]string{}, rec.calls...), queries...) {
				if strings.Contains(a, "demo-room-db") || strings.Contains(strings.ToLower(a), "piperoom ") {
					t.Errorf("reset mentions the room database: %q", a)
				}
			}
		})
	}
}

func TestDownStopsStagingToo(t *testing.T) {
	prod, stg, reg := newFakeConnect(t), newFakeConnect(t), newFakeConnect(t)
	s, rec := stagingStack(t, prod, stg, reg)
	if err := s.down(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rec.calls, []string{"--profile staging down"}) {
		t.Fatalf("compose = %q", rec.calls)
	}
}

// `down --staging` removes the staging stack and its leftovers, keeps production
// up, and is a no-op the second time (E2, E3).
func TestStagingDownCleansEverythingAndIsIdempotent(t *testing.T) {
	prod, stg, reg := newFakeConnect(t), newFakeConnect(t), newFakeConnect(t)
	s, rec := stagingStack(t, prod, stg, reg)
	ks, raw := kfakeStack(t, 1, append([]string{"pg.public.orders", "_connect_configs"}, stagingTopics...)...)
	_ = raw
	s.cfg.KafkaBrokers = ks.cfg.KafkaBrokers
	var sqlCalls int
	var stmts []string
	s.sqlFn = func(_ context.Context, db string, sql ...string) (string, error) {
		sqlCalls++
		if sqlCalls == 1 {
			return "", errors.New("replication slot is active") // the walsender outlives its container briefly
		}
		if db != "orders" {
			t.Errorf("slot dropped in database %q", db)
		}
		stmts = append(stmts, sql...)
		return "", nil
	}
	ctx := testCtx(t)
	for i := 0; i < 2; i++ {
		if err := s.stagingDown(ctx); err != nil {
			t.Fatalf("down --staging run %d: %v", i+1, err)
		}
	}
	if !stg.saw("DELETE /connectors/" + connectorName) {
		t.Error("staging connector was not deleted first")
	}
	if prod.count() != 0 {
		t.Errorf("production Connect got %d requests from `down --staging`", prod.count())
	}
	if want := "--profile staging rm --stop --force connect-staging schema-registry-staging"; rec.calls[0] != want {
		t.Errorf("compose = %q, want %q", rec.calls[0], want)
	}
	joined := strings.Join(stmts, "\n")
	if !strings.Contains(joined, "pg_drop_replication_slot") || !strings.Contains(joined, stagingSlot) ||
		!strings.Contains(joined, "DROP PUBLICATION IF EXISTS "+stagingPublication) {
		t.Errorf("slot and publication not dropped (statements: %q)", joined)
	}
	if strings.Contains(joined, "'debezium_orders'") || strings.Contains(joined, " dbz_publication;") {
		t.Errorf("production slot or publication named: %q", joined)
	}
	// Staging topics are gone; production's stay.
	span := func(topic string) bool {
		_, ok, err := s.topicSpan(ctx, topic)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	for _, topic := range stagingTopics {
		if span(topic) {
			t.Errorf("topic %s left behind", topic)
		}
	}
	for _, topic := range []string{"pg.public.orders", "_connect_configs"} {
		if !span(topic) {
			t.Errorf("production topic %s was deleted", topic)
		}
	}
}

// E4: with the stack down every step reports, none panics, and the topics are still tried.
func TestStagingDownReportsEveryFailure(t *testing.T) {
	prod, stg, reg := newFakeConnect(t), newFakeConnect(t), newFakeConnect(t)
	s, rec := stagingStack(t, prod, stg, reg)
	rec.err = errors.New("docker: not running")
	s.sqlFn = func(context.Context, string, ...string) (string, error) { return "", errors.New("postgres down") }
	s.cfg.KafkaBrokers = "127.0.0.1:1"
	s.stagingStepTimeout = 300 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := s.stagingDown(ctx)
	if err == nil {
		t.Fatal("want an error")
	}
	for _, want := range []string{"docker: not running", "postgres down", "delete topics"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
}
