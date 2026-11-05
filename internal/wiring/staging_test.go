package wiring

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/actions"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// stagingYAML: pa and pb have their own connections (twoTenantYAML), pc and
// pd use the process-wide settings. pa and pc declare staging:, pb and pd
// do not. extra is appended to pa (require_staging:).
func stagingYAML(paExtra string) string {
	y := twoTenantYAML(`
  - {id: pc, topic_prefix: pc, source_tables: [public.c], connectors: [pc-connector]}
  - {id: pd, topic_prefix: pd, source_tables: [public.d], connectors: [pd-connector]}`)
	y = strings.Replace(y, `
  - id: pb`, `
    staging:
      connections:
        connect_url: {env: A_STAGING_CONNECT_URL}
        connect_username: {env: A_STAGING_CONNECT_USER}
        connect_password: {env: A_STAGING_CONNECT_PASSWORD}
        schema_registry_url: {value: "http://sr-staging-a:8081"}`+paExtra+`
  - id: pb`, 1)
	return strings.Replace(y, "connectors: [pc-connector]}", `connectors: [pc-connector],
      staging: {connections: {connect_url: {env: C_STAGING_CONNECT_URL}, kafka_brokers: {env: C_STAGING_KAFKA}}}}`, 1)
}

func stagingEnv() map[string]string {
	m := twoTenantEnv()
	m["A_STAGING_CONNECT_URL"] = "http://connect-staging-a:8083"
	m["A_STAGING_CONNECT_PASSWORD"] = "secret-staging-a"
	m["A_STAGING_CONNECT_USER"] = "stager"
	m["C_STAGING_CONNECT_URL"] = "http://connect-staging-c:8083"
	m["C_STAGING_KAFKA"] = "kafka-prod:9092" // staging may share the production Kafka
	return m
}

// processCfg is the process-wide set of pc and pd.
func processCfg(writeMode string) config.Config {
	return config.Config{SourceDBURL: "src-process", ConnectURL: "http://connect-prod:8083",
		SchemaRegistryURL: "http://sr-prod:8081", ConnectPassword: "secret-process-connect", WriteMode: writeMode}
}

// fakeStaging records every staging set built and closed.
type fakeStaging struct {
	mu     sync.Mutex
	cfgs   []config.Config
	built  map[string]*actions.Staging // by staging ConnectURL
	closed []string
	err    error
}

func (f *fakeStaging) open(cfg config.Config, _ *aws.Config) (*actions.Staging, func(), error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, nil, f.err
	}
	f.cfgs = append(f.cfgs, cfg)
	s := &spy{label: "staging " + cfg.ConnectURL}
	st := &actions.Staging{Connect: s}
	if cfg.KafkaBrokers != "" {
		st.Kafka = s
	}
	if f.built == nil {
		f.built = map[string]*actions.Staging{}
	}
	f.built[cfg.ConnectURL] = st
	label := cfg.ConnectURL
	return st, func() {
		f.mu.Lock()
		f.closed = append(f.closed, label)
		f.mu.Unlock()
	}, nil
}

func stagingDeps(f *fakeOpener, fs *fakeStaging, src connSource) stackDeps {
	d := f.deps(src)
	d.staging = fs.open
	return d
}

// depsStaging returns actions.Deps.Staging as the registered
// connect_update_connector_config holds it. The action keeps its Deps in an
// unexported field, read here by reflection only.
func depsStaging(t *testing.T, areg *actions.Registry) *actions.Staging {
	t.Helper()
	a, ok := areg.Get("connect_update_connector_config")
	if !ok {
		t.Fatal("connect_update_connector_config not registered")
	}
	v := reflect.ValueOf(a).Elem().FieldByName("d")
	if !v.IsValid() {
		t.Fatal("connect_update_connector_config has no d Deps field; update this test")
	}
	f := v.FieldByName("Staging")
	if f.IsNil() {
		return nil
	}
	return (*actions.Staging)(f.UnsafePointer())
}

// TestStagingWriteOn (E1, TC2): with write mode on, each pipeline with
// staging: gets its own staging clients, built from its staging block only,
// as Deps.Staging of its own actions; a pipeline without staging: (TC2) and
// a pipeline sharing the process-wide set with a staged one keep nil.
func TestStagingWriteOn(t *testing.T) {
	own := loadOwners(t, stagingYAML(""))
	f, fs := newFakeOpener(), &fakeStaging{}
	st, err := openStacks(context.Background(), processCfg("on"), nil, own, StackOptions{}, stagingDeps(f, fs, env(stagingEnv())))
	if err != nil {
		t.Fatalf("openStacks: %v", err)
	}
	if len(fs.cfgs) != 2 {
		t.Fatalf("built %d staging sets, want 2 (pa, pc)", len(fs.cfgs))
	}
	a := fs.built["http://connect-staging-a:8083"]
	c := fs.built["http://connect-staging-c:8083"]
	if a == nil || c == nil {
		t.Fatalf("staging sets = %v", fs.built)
	}
	// Built from the staging block alone: nothing from production.
	for _, sc := range fs.cfgs {
		if sc.SourceDBURL != "" || sc.WarehouseDBURL != "" || sc.ConnectPassword == "secret-process-connect" || sc.KafkaSASLPassword != "" {
			t.Fatalf("staging settings took production values: %+v", sc)
		}
		if sc.ConnectURL == "http://connect-staging-a:8083" &&
			(sc.ConnectUsername != "stager" || sc.ConnectPassword != "secret-staging-a" || sc.SchemaRegistryURL != "http://sr-staging-a:8081") {
			t.Fatalf("pa staging settings not resolved like production refs (TC4): %+v", sc)
		}
	}
	for id, want := range map[string]*actions.Staging{"pa": a, "pc": c, "pb": nil, "pd": nil} {
		s := st.For(id)
		if got := depsStaging(t, s.Actions); got != want {
			t.Errorf("For(%s) Deps.Staging = %p, want %p", id, got, want)
		}
		if s.HasStaging() != (want != nil) {
			t.Errorf("For(%s).HasStaging() = %v", id, s.HasStaging())
		}
	}
	// pc and pd share the process-wide clients, tools and topology, but not
	// the action registry holding pc's staging.
	pc, pd := st.For("pc"), st.For("pd")
	if pc.Clients != pd.Clients || pc.Tools != pd.Tools || pc.Name != ProcessWideStack {
		t.Fatal("pc's view must share the process-wide set")
	}
	if pc.Actions == pd.Actions || !slices.Equal(pc.Pipelines, []string{"pc"}) {
		t.Fatalf("pc must have its own registry and pipeline list (%v)", pc.Pipelines)
	}
	if len(st.All()) != 3 {
		t.Fatalf("All() = %d sets, want 3: a view is not a client set", len(st.All()))
	}
	// pa's set serves pa alone: its registry is replaced in place.
	if st.For("pa") != st.All()[1] && st.For("pa") != st.All()[2] {
		t.Fatal("pa's staged set must be the set in All()")
	}
	st.Close()
	if len(fs.closed) != 2 {
		t.Fatalf("staging closers run: %v", fs.closed)
	}
}

// TestStagingWriteOff is TC3: with write mode off (unset, or narrowed by
// ReadOnly) no staging client is built and every Deps.Staging is nil, while
// the block is still resolved and checked.
func TestStagingWriteOff(t *testing.T) {
	for name, tc := range map[string]struct {
		mode     string
		readOnly bool
	}{"unset": {"", false}, "off": {"off", false}, "read-only option": {"on", true}} {
		t.Run(name, func(t *testing.T) {
			own := loadOwners(t, stagingYAML(""))
			f, fs := newFakeOpener(), &fakeStaging{err: errors.New("staging opener must not be called")}
			st, err := openStacks(context.Background(), processCfg(tc.mode), nil, own, StackOptions{ReadOnly: tc.readOnly}, stagingDeps(f, fs, env(stagingEnv())))
			if err != nil {
				t.Fatalf("openStacks: %v", err)
			}
			defer st.Close()
			for _, id := range []string{"pa", "pb", "pc", "pd"} {
				if s := st.For(id); depsStaging(t, s.Actions) != nil || s.HasStaging() {
					t.Errorf("For(%s) has staging with write mode off", id)
				}
			}
			// Still resolved: a missing staging variable stops the start in
			// either mode.
			m := stagingEnv()
			delete(m, "A_STAGING_CONNECT_PASSWORD")
			if _, err := openStacks(context.Background(), processCfg(tc.mode), nil, own, StackOptions{ReadOnly: tc.readOnly}, stagingDeps(newFakeOpener(), fs, env(m))); err == nil {
				t.Fatal("a missing staging variable must stop the start with write mode off too")
			}
		})
	}
}

// TestStagingOverlapResolved is TC1 at start: two different references
// that hold one endpoint (also through scheme default ports) are refused
// before any client set opens, in both write modes, naming settings only.
func TestStagingOverlapResolved(t *testing.T) {
	cases := []struct {
		name, key, val, want string
		only                 []string
	}{
		{"pb's production connect", "A_STAGING_CONNECT_URL", "http://CONNECT-B:8083/", `pipeline "pa": staging.connections.connect_url resolves to the same endpoint as pipeline "pb" connections.connect_url`, nil},
		{"pb not opened is still compared", "A_STAGING_CONNECT_URL", "connect-b", `as pipeline "pb" connections.connect_url`, []string{"pa"}},
		{"process connect, default port", "C_STAGING_CONNECT_URL", "http://connect-prod:8083", `pipeline "pc": staging.connections.connect_url resolves to the same endpoint as the process-wide CONNECT_URL`, nil},
		{"process registry", "A_STAGING_CONNECT_URL", "http://sr-prod:8081/", "the process-wide SCHEMA_REGISTRY_URL", nil},
	}
	for _, tc := range cases {
		for _, mode := range []string{"", "on"} {
			t.Run(tc.name+"/write="+mode, func(t *testing.T) {
				m := stagingEnv()
				m[tc.key] = tc.val
				f, fs := newFakeOpener(), &fakeStaging{}
				_, err := openStacks(context.Background(), processCfg(mode), nil, loadOwners(t, stagingYAML("")), StackOptions{Only: tc.only}, stagingDeps(f, fs, env(m)))
				if err == nil || !errors.Is(err, owners.ErrStagingIsProduction) || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("openStacks = %v, want %q", err, tc.want)
				}
				for _, v := range []string{"connect-b", "CONNECT-B", "connect-prod", "sr-prod", "secret-"} {
					if strings.Contains(err.Error(), v) {
						t.Fatalf("error quotes a value (%s): %v", v, err)
					}
				}
				if len(f.cfgs) != 0 || len(fs.cfgs) != 0 {
					t.Fatalf("opened %d sets and %d staging sets before the check", len(f.cfgs), len(fs.cfgs))
				}
			})
		}
	}
	// A distinct port on the same host is another endpoint.
	m := stagingEnv()
	m["C_STAGING_CONNECT_URL"] = "http://connect-prod:18083"
	st, err := openStacks(context.Background(), processCfg("on"), nil, loadOwners(t, stagingYAML("")), StackOptions{}, stagingDeps(newFakeOpener(), &fakeStaging{}, env(m)))
	if err != nil {
		t.Fatalf("another port refused: %v", err)
	}
	st.Close()
}

// TestStagingSecretErrors is TC4: a staging reference fails like a
// production one, naming the pipeline, the key and the variable, never a
// value; nothing is opened.
func TestStagingSecretErrors(t *testing.T) {
	m := stagingEnv()
	m["A_STAGING_CONNECT_PASSWORD"] = "  "
	f, fs := newFakeOpener(), &fakeStaging{}
	_, err := openStacks(context.Background(), processCfg("on"), nil, loadOwners(t, stagingYAML("")), StackOptions{}, stagingDeps(f, fs, env(m)))
	if err == nil || !errors.Is(err, errConnRef) {
		t.Fatalf("openStacks = %v, want errConnRef", err)
	}
	for _, w := range []string{`pipeline "pa"`, "staging.connections.connect_password", "A_STAGING_CONNECT_PASSWORD", "empty"} {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("err %q does not contain %q", err, w)
		}
	}
	if strings.Contains(err.Error(), "secret-") {
		t.Errorf("err leaks a value: %v", err)
	}
	if len(f.cfgs) != 0 || len(fs.cfgs) != 0 {
		t.Errorf("opened %d sets, %d staging sets", len(f.cfgs), len(fs.cfgs))
	}
}

// TestRequireStagingNeedsStager: a require_staging: entry must name a
// registered action that implements actions.Stager; on refusal everything
// opened is closed.
func TestRequireStagingNeedsStager(t *testing.T) {
	own := loadOwners(t, stagingYAML(`
    require_staging: [connect_restart_task]`))
	for _, mode := range []string{"", "on"} {
		f, fs := newFakeOpener(), &fakeStaging{}
		_, err := openStacks(context.Background(), processCfg(mode), nil, own, StackOptions{}, stagingDeps(f, fs, env(stagingEnv())))
		if err == nil || !strings.Contains(err.Error(), `pipeline "pa": require_staging: "connect_restart_task" is not an action that can be staged`) {
			t.Fatalf("write=%q: openStacks = %v", mode, err)
		}
		if len(f.closed) != len(f.cfgs) || len(fs.closed) != len(fs.cfgs) {
			t.Fatalf("write=%q: opened %d/%d, closed %v/%v", mode, len(f.cfgs), len(fs.cfgs), f.closed, fs.closed)
		}
	}
	// X8b's three Stagers are accepted in both write modes (the registry
	// holds every action), as dev/owners.staging.yaml uses the first.
	staged := loadOwners(t, stagingYAML(`
    require_staging: [connect_update_connector_config, schema_register, schema_set_compatibility]`))
	for _, mode := range []string{"", "on"} {
		f, fs := newFakeOpener(), &fakeStaging{}
		st, err := openStacks(context.Background(), processCfg(mode), nil, staged, StackOptions{}, stagingDeps(f, fs, env(stagingEnv())))
		if err != nil {
			t.Fatalf("write=%q: openStacks with X8b's stagers = %v", mode, err)
		}
		st.Close()
	}
}

// stagerAction is an action that implements actions.Stager.
type stagerAction struct{ name string }

func (a stagerAction) Name() string                                           { return a.name }
func (stagerAction) Layer() string                                            { return "kafka-connect" }
func (stagerAction) Describe() string                                         { return "test" }
func (stagerAction) ParamsSchema() map[string]any                             { return map[string]any{"type": "object"} }
func (stagerAction) Validate(context.Context, json.RawMessage) error          { return nil }
func (stagerAction) Execute(context.Context, json.RawMessage) (string, error) { return "", nil }
func (stagerAction) Stage(context.Context, json.RawMessage) (actions.StageResult, error) {
	return actions.StageResult{}, nil
}

type plainAction struct{ stagerAction }

func (plainAction) Stage() {} // not the Stager signature

func TestStagerNames(t *testing.T) {
	areg := actions.NewRegistry()
	for _, a := range []actions.Action{stagerAction{"connect_tune_throughput"}, plainAction{stagerAction{"connect_restart_task"}}} {
		if err := areg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	if got := stagerNames(areg); !slices.Equal(got, []string{"connect_tune_throughput"}) {
		t.Fatalf("stagerNames = %v", got)
	}
}

// TestOpenStagingClients: the real opener builds only what the block names,
// with the production netsec rules, and never a typed nil.
func TestOpenStagingClients(t *testing.T) {
	st, closeFn, err := openStagingClients(config.Config{ConnectURL: "http://connect-staging:8083"}, nil)
	if err != nil {
		t.Fatalf("openStagingClients: %v", err)
	}
	if st.Connect == nil || st.Registry != nil || st.Kafka != nil {
		t.Fatalf("staging = %+v, want Connect only", st)
	}
	closeFn()

	st, closeFn, err = openStagingClients(config.Config{SchemaRegistryURL: "https://sr-staging:8081", KafkaBrokers: "kafka:9092"}, nil)
	if err != nil {
		t.Fatalf("openStagingClients: %v", err)
	}
	if st.Connect != nil || st.Registry == nil || st.Kafka == nil {
		t.Fatalf("staging = %+v, want Registry and Kafka", st)
	}
	closeFn()

	// netsec refuses a password over plain http for staging as for production.
	_, _, err = openStagingClients(config.Config{ConnectURL: "http://connect-staging:8083", ConnectUsername: "u", ConnectPassword: "secret-pw"}, nil)
	if err == nil || strings.Contains(err.Error(), "secret-pw") {
		t.Fatalf("basic auth over http = %v", err)
	}
	// MSK IAM without the process AWS config fails like production.
	_, _, err = openStagingClients(config.Config{ConnectURL: "http://c:8083", KafkaBrokers: "k:9098", KafkaSecurityProtocol: "SASL_SSL", KafkaSASLMechanism: "AWS_MSK_IAM"}, nil)
	if !errors.Is(err, errNoSharedAWS) {
		t.Fatalf("MSK IAM without AWS = %v", err)
	}
}
