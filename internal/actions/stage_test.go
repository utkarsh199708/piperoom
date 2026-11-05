package actions

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/schemareg"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// ---- fakes ----

func stagePipeline() owners.Pipeline {
	return owners.Pipeline{ID: "orders", TopicPrefix: "pg", Connectors: []string{"orders-src"}, SourceTables: []string{"public.orders"}}
}

func stageCtx() context.Context { return tools.WithPipeline(context.Background(), stagePipeline()) }

// fastStage is Deps with stage timings a test can wait for.
func fastStage(d Deps) Deps {
	d.stageWindow, d.stagePoll, d.stageSettle = 150*time.Millisecond, 5*time.Millisecond, time.Nanosecond
	return d
}

// stageURL is the staging address no error or result may carry.
const stageURL = "http://connect-staging.internal:8083"

// stageConnect is a staging Kafka Connect with one connector, orders-src.
type stageConnect struct {
	t        *testing.T
	mu       sync.Mutex
	missing  bool              // the connector is not on staging
	cfg      map[string]string // its config
	status   func(n int) clients.ConnectorStatus
	statusN  int
	listErr  error
	patchErr error
	putErrs  int   // the first putErrs PUTs fail
	putErr   error // the error they fail with (default: a 409)
	validate *connect.ValidateResult
	// removeOnPatch deletes the connector right after the PATCH.
	removeOnPatch bool
	onStatus      func(n int)
	ops           []string
	patches       []map[string]string
	puts          []map[string]string
}

func (f *stageConnect) op(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, s)
}

func (f *stageConnect) ListConnectors(context.Context) ([]string, error) {
	f.op("list")
	if f.listErr != nil {
		return nil, f.listErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.missing {
		return []string{"other-src"}, nil
	}
	return []string{"other-src", "orders-src"}, nil
}

func (f *stageConnect) Status(context.Context, string) (clients.ConnectorStatus, error) {
	f.op("status")
	f.mu.Lock()
	n := f.statusN
	f.statusN++
	f.mu.Unlock()
	if f.onStatus != nil {
		f.onStatus(n)
	}
	if f.status == nil {
		return running(1), nil
	}
	return f.status(n), nil
}

func (f *stageConnect) Config(context.Context, string) (map[string]string, error) {
	f.op("config")
	f.mu.Lock()
	defer f.mu.Unlock()
	return maps.Clone(f.cfg), nil
}

func (f *stageConnect) RestartTask(context.Context, string, int) error {
	f.t.Error("Stage restarted a task")
	return nil
}

func (f *stageConnect) RestartConnector(context.Context, string) error {
	f.t.Error("Stage restarted a connector")
	return nil
}

func (f *stageConnect) PauseConnector(context.Context, string) error {
	f.t.Error("Stage paused a connector")
	return nil
}

func (f *stageConnect) UpdateConnectorConfigKeys(_ context.Context, _ string, changes map[string]string) error {
	f.op("patch")
	if f.patchErr != nil {
		return f.patchErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.patches = append(f.patches, maps.Clone(changes))
	maps.Copy(f.cfg, changes)
	if f.removeOnPatch {
		f.missing = true
	}
	return nil
}

func (f *stageConnect) PutConnectorConfig(_ context.Context, _ string, cfg map[string]string) error {
	f.op("put")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.putErrs > 0 {
		f.putErrs--
		if f.putErr != nil {
			return f.putErr
		}
		return errors.New("connect: PUT " + stageURL + "/connectors/orders-src/config: status 409: rebalance in progress")
	}
	f.puts = append(f.puts, maps.Clone(cfg))
	f.cfg = maps.Clone(cfg)
	return nil
}

func (f *stageConnect) ValidateConfig(_ context.Context, _ string, _ map[string]string) (connect.ValidateResult, error) {
	f.op("validate")
	if f.validate != nil {
		return *f.validate, nil
	}
	return connect.ValidateResult{}, nil
}

func (f *stageConnect) count(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, o := range f.ops {
		if o == op {
			n++
		}
	}
	return n
}

var (
	_ clients.ConnectClient       = (*stageConnect)(nil)
	_ clients.ConnectFixWriter    = (*stageConnect)(nil)
	_ clients.ConnectConfigPutter = (*stageConnect)(nil)
	_ configValidator             = (*stageConnect)(nil)
)

func running(tasks int) clients.ConnectorStatus {
	st := clients.ConnectorStatus{Name: "orders-src", State: "RUNNING"}
	for i := 0; i < tasks; i++ {
		st.Tasks = append(st.Tasks, clients.TaskStatus{ID: i, State: "RUNNING"})
	}
	return st
}

// prodConnect and prodRegistry are production clients: any call fails the
// test (TC5).
type prodConnect struct{ x8Connect }

func newProdConnect(t *testing.T) *prodConnect {
	return &prodConnect{x8Connect{t: t, prod: true, cfg: map[string]string{}}}
}

func (f *prodConnect) ValidateConfig(context.Context, string, map[string]string) (connect.ValidateResult, error) {
	f.touched("ValidateConfig")
	return connect.ValidateResult{}, nil
}

type prodRegistry struct{ t *testing.T }

func (p prodRegistry) hit(what string) { p.t.Errorf("Stage called the production registry: %s", what) }
func (p prodRegistry) Subjects(context.Context) ([]string, error) {
	p.hit("Subjects")
	return []string{"pg.public.orders-value"}, nil
}
func (p prodRegistry) Versions(context.Context, string) ([]int, error) {
	p.hit("Versions")
	return []int{1}, nil
}
func (p prodRegistry) Schema(context.Context, string, int) (clients.SchemaVersion, error) {
	p.hit("Schema")
	return clients.SchemaVersion{}, nil
}
func (p prodRegistry) Compatibility(context.Context, string) (string, bool, error) {
	p.hit("Compatibility")
	return "BACKWARD", true, nil
}
func (p prodRegistry) SetCompatibility(context.Context, string, string) error {
	p.hit("SetCompatibility")
	return nil
}
func (p prodRegistry) Register(context.Context, string, string) (int, error) {
	p.hit("Register")
	return 1, nil
}
func (p prodRegistry) CheckCompatibility(context.Context, string, string) (bool, []string, error) {
	p.hit("CheckCompatibility")
	return true, nil, nil
}
func (p prodRegistry) DeleteSubjectCompatibility(context.Context, string) error {
	p.hit("DeleteSubjectCompatibility")
	return nil
}

// stageRegistry is a staging Schema Registry with one subject.
type stageRegistry struct {
	mu         sync.Mutex
	missing    bool
	versions   []int
	level      string // the subject's own level; "" = none
	global     string
	compatible bool
	msgs       []string
	checkErr   error
	failSet    func(n int) bool // SetCompatibility call n (from 0) fails
	setN       int
	regID      int
	dedupe     bool // Register finds the schema already registered
	ops        []string
	sets       []string
	deletes    int
}

func (r *stageRegistry) op(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, s)
}

func (r *stageRegistry) Subjects(context.Context) ([]string, error) {
	r.op("subjects")
	if r.missing {
		return []string{"pg.public.other-value"}, nil
	}
	return []string{"pg.public.orders-value"}, nil
}

func (r *stageRegistry) Versions(context.Context, string) ([]int, error) {
	r.op("versions")
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.versions), nil
}

func (r *stageRegistry) Schema(context.Context, string, int) (clients.SchemaVersion, error) {
	r.op("schema")
	return clients.SchemaVersion{}, nil
}

func (r *stageRegistry) Compatibility(context.Context, string) (string, bool, error) {
	r.op("compat")
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.level == "" {
		return r.global, false, nil
	}
	return r.level, true, nil
}

func (r *stageRegistry) SetCompatibility(_ context.Context, _ string, level string) error {
	r.op("set")
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.setN
	r.setN++
	if r.failSet != nil && r.failSet(n) {
		return &url.Error{Op: "Put", URL: stageURL + "/config/x", Err: errors.New("connection refused")}
	}
	r.sets = append(r.sets, level)
	r.level = level
	return nil
}

func (r *stageRegistry) Register(context.Context, string, string) (int, error) {
	r.op("register")
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.dedupe {
		r.versions = append(r.versions, len(r.versions)+1)
	}
	return r.regID, nil
}

func (r *stageRegistry) CheckCompatibility(context.Context, string, string) (bool, []string, error) {
	r.op("check")
	return r.compatible, r.msgs, r.checkErr
}

func (r *stageRegistry) DeleteSubjectCompatibility(context.Context, string) error {
	r.op("delete")
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deletes++
	r.level = ""
	return nil
}

func (r *stageRegistry) did(op string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Contains(r.ops, op)
}

var (
	_ clients.SchemaRegistryClient = (*stageRegistry)(nil)
	_ compatibilityChecker         = (*stageRegistry)(nil)
	_ subjectCompatDeleter         = (*stageRegistry)(nil)
	_ clients.SchemaRegistryClient = prodRegistry{}
)

func origConfig() map[string]string {
	return map[string]string{
		"name":                  "orders-src",
		"connector.class":       "io.debezium.connector.postgresql.PostgresConnector",
		"decimal.handling.mode": "string",
		"database.hostname":     "pg-staging",
	}
}

const cfgRaw = `{"connector":"orders-src","changes":{"decimal.handling.mode":"precise"}}`

func cfgAction(stg *stageConnect, prod *prodConnect) *updateConnectorConfig {
	d := Deps{Connect: prod, Registry: prodRegistry{stg.t}}
	if stg != nil {
		d.Staging = &Staging{Connect: stg}
	}
	return &updateConnectorConfig{d: fastStage(d)}
}

// assertRestored checks the last PUT gave the connector back exactly its
// original config (TC2).
func assertRestored(t *testing.T, stg *stageConnect) {
	t.Helper()
	stg.mu.Lock()
	defer stg.mu.Unlock()
	if len(stg.puts) == 0 || !maps.Equal(stg.puts[len(stg.puts)-1], origConfig()) {
		t.Fatalf("final PUT = %v, want the original %v", stg.puts, origConfig())
	}
}

// ---- connect_update_connector_config ----

// TC1 (healthy), TC2, TC5: applied, RUNNING for the window, restored with
// the original whole config; production never called.
func TestStageConfigHealthy(t *testing.T) {
	stg := &stageConnect{t: t, cfg: origConfig()}
	a := cfgAction(stg, newProdConnect(t))
	start := time.Now()
	got, err := a.Stage(stageCtx(), json.RawMessage(cfgRaw))
	if err != nil {
		t.Fatal(err)
	}
	if got.Outcome() != events.StageHealthy || got.Target != "connector orders-src" || got.Leftover != "" {
		t.Fatalf("result %+v", got)
	}
	if !strings.Contains(got.Detail, "RUNNING, 1 of 1 tasks RUNNING at the end of the 0.15 s window") {
		t.Fatalf("detail %q", got.Detail)
	}
	if time.Since(start) < 150*time.Millisecond {
		t.Fatal("verdict before the observe window ended")
	}
	if got.DryRun == nil || !got.DryRun.OK || got.DryRun.Source != sourceValidate {
		t.Fatalf("dry run %+v", got.DryRun)
	}
	if len(stg.patches) != 1 || !maps.Equal(stg.patches[0], map[string]string{"decimal.handling.mode": "precise"}) {
		t.Fatalf("patches %v", stg.patches)
	}
	assertRestored(t, stg)
	// Order: the write comes after the existence check and the dry run; the
	// restore comes last.
	if i, j, k := slices.Index(stg.ops, "list"), slices.Index(stg.ops, "validate"), slices.Index(stg.ops, "patch"); i < 0 || !(i < j && j < k) {
		t.Fatalf("ops %v", stg.ops)
	}
	if stg.ops[len(stg.ops)-1] != "put" {
		t.Fatalf("restore is not the last call: %v", stg.ops)
	}
	if got.DurationMS < 150 {
		t.Fatalf("duration %d ms", got.DurationMS)
	}
}

// TC1 (failed): a task FAILED within the window ends it early, unhealthy,
// with the cleaned trace; the secret in the trace is not kept; staging is
// restored (TC2).
func TestStageConfigTaskFailed(t *testing.T) {
	trace := "org.apache.kafka.connect.errors.ConnectException: jdbc:postgresql://pg:5432/orders?user=app&password=Hunter2Secret failed\n" +
		"\tat io.debezium.Foo.bar(Foo.java:1)\nCaused by: org.postgresql.util.PSQLException: boom"
	stg := &stageConnect{t: t, cfg: origConfig(), status: func(n int) clients.ConnectorStatus {
		if n < 2 {
			return running(2)
		}
		st := running(2)
		st.Tasks[1].State, st.Tasks[1].Trace = "FAILED", trace
		return st
	}}
	a := cfgAction(stg, newProdConnect(t))
	a.d.stageWindow = 5 * time.Second
	start := time.Now()
	got, err := a.Stage(stageCtx(), json.RawMessage(cfgRaw))
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("a FAILED task did not end the window early")
	}
	if got.Outcome() != events.StageFailed || !got.Applied || got.Healthy {
		t.Fatalf("result %+v", got)
	}
	if !strings.HasPrefix(got.Detail, "task 1 FAILED: ") || !strings.Contains(got.Detail, "Caused by: org.postgresql.util.PSQLException: boom") {
		t.Fatalf("detail %q", got.Detail)
	}
	if strings.Contains(got.Detail, "Hunter2Secret") || strings.Contains(got.Detail, "Foo.java") {
		t.Fatalf("detail keeps a secret or a stack frame: %q", got.Detail)
	}
	assertRestored(t, stg)
}

// A FAILED state read inside the settle time (the old failure, before
// Connect restarted the tasks) is not the verdict.
func TestStageConfigStaleFailureIgnored(t *testing.T) {
	stg := &stageConnect{t: t, cfg: origConfig()}
	var first time.Time
	stg.status = func(n int) clients.ConnectorStatus {
		if n == 0 {
			first = time.Now()
		}
		if time.Since(first) < 40*time.Millisecond {
			st := running(1)
			st.Tasks[0].State, st.Tasks[0].Trace = "FAILED", "old failure"
			return st
		}
		return running(1)
	}
	a := cfgAction(stg, newProdConnect(t))
	a.d.stageSettle = 80 * time.Millisecond
	got, err := a.Stage(stageCtx(), json.RawMessage(cfgRaw))
	if err != nil || got.Outcome() != events.StageHealthy {
		t.Fatalf("result %+v, %v", got, err)
	}
	assertRestored(t, stg)
}

// A connector that is still FAILED at the window's end is unhealthy even
// when the settle time covered the whole window.
func TestStageConfigFailedAtEnd(t *testing.T) {
	stg := &stageConnect{t: t, cfg: origConfig(), status: func(int) clients.ConnectorStatus {
		return clients.ConnectorStatus{Name: "orders-src", State: "FAILED"}
	}}
	a := cfgAction(stg, newProdConnect(t))
	a.d.stageSettle = time.Hour
	got, err := a.Stage(stageCtx(), json.RawMessage(cfgRaw))
	if err != nil || got.Outcome() != events.StageFailed || got.Detail != "connector FAILED (no trace)" {
		t.Fatalf("result %+v, %v", got, err)
	}
	assertRestored(t, stg)
}

// TC6: never RUNNING within the window is unhealthy, after the window.
func TestStageConfigNeverRunning(t *testing.T) {
	stg := &stageConnect{t: t, cfg: origConfig(), status: func(int) clients.ConnectorStatus {
		st := running(1)
		st.Tasks[0].State = "UNASSIGNED"
		return st
	}}
	a := cfgAction(stg, newProdConnect(t))
	start := time.Now()
	got, err := a.Stage(stageCtx(), json.RawMessage(cfgRaw))
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 150*time.Millisecond {
		t.Fatal("verdict before the window ended")
	}
	if got.Outcome() != events.StageFailed || got.Detail != "not RUNNING at the end of the 0.15 s window: connector RUNNING, task 0 UNASSIGNED" {
		t.Fatalf("result %+v", got)
	}
	assertRestored(t, stg)
}

// TC6: ctx ending inside the window stops the stage at once with no verdict,
// and staging is still restored (on its own context).
func TestStageConfigCancelStillRestores(t *testing.T) {
	ctx, cancel := context.WithCancel(stageCtx())
	defer cancel()
	stg := &stageConnect{t: t, cfg: origConfig(), onStatus: func(n int) {
		if n == 1 {
			cancel()
		}
	}}
	a := cfgAction(stg, newProdConnect(t))
	a.d.stageWindow = 10 * time.Second
	start := time.Now()
	got, err := a.Stage(ctx, json.RawMessage(cfgRaw))
	if !errors.Is(err, context.Canceled) || got.Applied {
		t.Fatalf("result %+v, %v", got, err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("cancel was not respected")
	}
	assertRestored(t, stg)

	// A deadline (the room's StageStepTimeout) ends it the same way.
	stg = &stageConnect{t: t, cfg: origConfig()}
	a = cfgAction(stg, newProdConnect(t))
	a.d.stageWindow = 10 * time.Second
	dctx, dcancel := context.WithTimeout(stageCtx(), 60*time.Millisecond)
	defer dcancel()
	if _, err := a.Stage(dctx, json.RawMessage(cfgRaw)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline: %v", err)
	}
	assertRestored(t, stg)
}

// TC2: the restore retries while Connect answers 409, and the final PUT is
// the original config; a restore that never succeeds is reported in
// Leftover without the staging URL.
func TestStageConfigRestoreRetriesAndLeftover(t *testing.T) {
	stg := &stageConnect{t: t, cfg: origConfig(), putErrs: 3}
	a := cfgAction(stg, newProdConnect(t))
	got, err := a.Stage(stageCtx(), json.RawMessage(cfgRaw))
	if err != nil || got.Outcome() != events.StageHealthy || got.Leftover != "" || stg.count("put") != 4 {
		t.Fatalf("result %+v, %v (puts %d)", got, err, stg.count("put"))
	}
	assertRestored(t, stg)

	stg = &stageConnect{t: t, cfg: origConfig(), putErrs: 1 << 30}
	a = cfgAction(stg, newProdConnect(t))
	a.d.restoreTimeout = 100 * time.Millisecond
	got, err = a.Stage(stageCtx(), json.RawMessage(cfgRaw))
	if err != nil || !got.Applied || !strings.Contains(got.Leftover, "restore of connector orders-src's previous config on staging failed") {
		t.Fatalf("result %+v, %v", got, err)
	}
	if strings.Contains(got.Leftover, "connect-staging") || strings.Contains(got.Leftover, "rebalance") {
		t.Fatalf("leftover carries the client's text: %q", got.Leftover)
	}
}

// TC3: a connector that is not on staging is not applied, and nothing is
// written (a PUT would create it).
func TestStageConfigMissingOnStaging(t *testing.T) {
	stg := &stageConnect{t: t, cfg: origConfig(), missing: true}
	got, err := cfgAction(stg, newProdConnect(t)).Stage(stageCtx(), json.RawMessage(cfgRaw))
	if err != nil || got.Applied || got.Detail != "connector not present in staging" || got.Outcome() != events.StageNotApplied {
		t.Fatalf("result %+v, %v", got, err)
	}
	if stg.count("patch")+stg.count("put")+stg.count("config") != 0 {
		t.Fatalf("ops %v", stg.ops)
	}
}

// A connector removed from staging during the stage is not re-created by
// the restore.
func TestStageConfigRemovedDuringStage(t *testing.T) {
	stg := &stageConnect{t: t, cfg: origConfig(), removeOnPatch: true}
	got, err := cfgAction(stg, newProdConnect(t)).Stage(stageCtx(), json.RawMessage(cfgRaw))
	if err != nil || !strings.Contains(got.Leftover, "was removed from staging during the stage; it was not re-created") {
		t.Fatalf("result %+v, %v", got, err)
	}
	if stg.count("put") != 0 {
		t.Fatal("the restore re-created a removed connector")
	}
}

// No staging (none declared, or no Connect in the block): not applied, no
// error, production untouched.
func TestStageNoStaging(t *testing.T) {
	prod, preg := newProdConnect(t), prodRegistry{t}
	for _, d := range []Deps{
		{Connect: prod, Registry: preg},
		{Connect: prod, Registry: preg, Staging: &Staging{}},
	} {
		for _, tc := range []struct {
			a   Stager
			raw string
		}{
			{&updateConnectorConfig{d: d}, cfgRaw},
			{&registerSchema{d: d}, `{"subject":"pg.public.orders-value","schema":"\"string\""}`},
			{&setCompatibility{d: d}, `{"subject":"pg.public.orders-value","level":"NONE"}`},
		} {
			got, err := tc.a.Stage(stageCtx(), json.RawMessage(tc.raw))
			if err != nil || got.Applied || got.Detail != events.StageNoStaging || got.Outcome() != events.StageNotApplied {
				t.Fatalf("%T: %+v, %v", tc.a, got, err)
			}
		}
	}
}

// E2: bad params and scope refusals come before any I/O, staging included.
func TestStageRefusedBeforeIO(t *testing.T) {
	trip := &x8Connect{t: t, prod: true, cfg: map[string]string{}}
	treg := prodRegistry{t}
	d := Deps{Connect: newProdConnect(t), Registry: treg, Staging: &Staging{Connect: trip, Registry: treg}}
	other := owners.Pipeline{ID: "billing", TopicPrefix: "bl", Connectors: []string{"billing-src"}, SourceTables: []string{"public.invoices"}}
	for _, tc := range []struct {
		name string
		a    Stager
		ctx  context.Context
		raw  string
		want error
	}{
		{"config bad params", &updateConnectorConfig{d: d}, stageCtx(), `{"connector":"orders-src","changes":{"database.password":"x"}}`, nil},
		{"config no scope", &updateConnectorConfig{d: d}, context.Background(), cfgRaw, tools.ErrNoScope},
		{"config pipeline id only", &updateConnectorConfig{d: d}, tools.WithPipelineID(context.Background(), "orders"), cfgRaw, tools.ErrNoScope},
		{"config other pipeline", &updateConnectorConfig{d: d}, tools.WithPipeline(context.Background(), other), cfgRaw, tools.ErrOutOfScope},
		{"register bad params", &registerSchema{d: d}, stageCtx(), `{"subject":"pg.public.orders-value"}`, nil},
		{"register other subject", &registerSchema{d: d}, stageCtx(), `{"subject":"bl.public.invoices-value","schema":"\"string\""}`, tools.ErrOutOfScope},
		{"compat bad level", &setCompatibility{d: d}, stageCtx(), `{"subject":"pg.public.orders-value","level":"LOOSE"}`, nil},
		{"compat no scope", &setCompatibility{d: d}, context.Background(), `{"subject":"pg.public.orders-value","level":"NONE"}`, tools.ErrNoScope},
	} {
		_, err := tc.a.Stage(tc.ctx, json.RawMessage(tc.raw))
		if err == nil || (tc.want != nil && !errors.Is(err, tc.want)) {
			t.Fatalf("%s: %v, want %v", tc.name, err, tc.want)
		}
	}
}

// Staging's validate refuses the change: failed, nothing written.
func TestStageConfigValidateRefuses(t *testing.T) {
	stg := &stageConnect{t: t, cfg: origConfig(), validate: &connect.ValidateResult{ErrorCount: 1, Errors: map[string][]string{"decimal.handling.mode": {"Invalid value"}}}}
	got, err := cfgAction(stg, newProdConnect(t)).Stage(stageCtx(), json.RawMessage(cfgRaw))
	if err != nil || got.Applied || got.Outcome() != events.StageFailed || got.DryRun == nil || got.DryRun.OK {
		t.Fatalf("result %+v, %v", got, err)
	}
	if stg.count("patch")+stg.count("put") != 0 {
		t.Fatalf("ops %v", stg.ops)
	}
}

// The visibility guard runs on staging's config: errors.log.enable is
// refused where staging logs record values; nothing is written.
func TestStageConfigGuardOnStagingConfig(t *testing.T) {
	cfg := origConfig()
	cfg["errors.log.include.messages"] = "true"
	stg := &stageConnect{t: t, cfg: cfg}
	got, err := cfgAction(stg, newProdConnect(t)).Stage(stageCtx(), json.RawMessage(`{"connector":"orders-src","changes":{"errors.log.enable":"true"}}`))
	if err != nil || got.Outcome() != events.StageFailed || got.Applied || stg.count("patch") != 0 {
		t.Fatalf("result %+v, %v (ops %v)", got, err, stg.ops)
	}
}

// Staging already has the value: no write and no restore; the connector's
// health is still the verdict.
func TestStageConfigAlreadySet(t *testing.T) {
	cfg := origConfig()
	cfg["decimal.handling.mode"] = "precise"
	stg := &stageConnect{t: t, cfg: cfg}
	got, err := cfgAction(stg, newProdConnect(t)).Stage(stageCtx(), json.RawMessage(cfgRaw))
	if err != nil || got.Outcome() != events.StageHealthy || !strings.Contains(got.Detail, "staging already had decimal.handling.mode, no write made") {
		t.Fatalf("result %+v, %v", got, err)
	}
	if stg.count("patch")+stg.count("put") != 0 {
		t.Fatalf("ops %v", stg.ops)
	}
}

// A failed write is no verdict, its error carries neither the staging URL
// nor Connect's text, and staging is restored anyway.
func TestStageConfigApplyFails(t *testing.T) {
	stg := &stageConnect{t: t, cfg: origConfig(), patchErr: &url.Error{Op: "Patch", URL: stageURL + "/connectors/orders-src/config", Err: errors.New("connection reset")}}
	got, err := cfgAction(stg, newProdConnect(t)).Stage(stageCtx(), json.RawMessage(cfgRaw))
	if err == nil || got.Applied || !strings.Contains(err.Error(), "staging unreachable") || strings.Contains(err.Error(), "connect-staging") {
		t.Fatalf("result %+v, %v", got, err)
	}
	assertRestored(t, stg)

	stg = &stageConnect{t: t, cfg: origConfig(), listErr: &url.Error{Op: "Get", URL: stageURL + "/connectors", Err: errors.New("connection refused")}}
	_, err = cfgAction(stg, newProdConnect(t)).Stage(stageCtx(), json.RawMessage(cfgRaw))
	if err == nil || strings.Contains(err.Error(), "connect-staging") || stg.count("patch") != 0 {
		t.Fatalf("list error: %v", err)
	}
}

// A staging client without the PUT (every fake from before X8) cannot
// restore, so it is not staged at all.
func TestStageConfigNeedsPutter(t *testing.T) {
	var stg struct{ clients.ConnectClient }
	stg.ConnectClient = &x8Connect{t: t, cfg: origConfig()}
	a := &updateConnectorConfig{d: Deps{Connect: newProdConnect(t), Staging: &Staging{Connect: stg}}}
	if _, err := a.Stage(stageCtx(), json.RawMessage(cfgRaw)); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("%v, want ErrNotSupported", err)
	}
}

// ---- schema_register ----

const regRaw = `{"subject":"pg.public.orders-value","schema":"{\"type\":\"record\",\"name\":\"o\",\"fields\":[]}"}`

func regAction(t *testing.T, stg clients.SchemaRegistryClient) *registerSchema {
	d := Deps{Connect: newProdConnect(t), Registry: prodRegistry{t}}
	if stg != nil {
		d.Staging = &Staging{Registry: stg}
	}
	return &registerSchema{d: d}
}

// Compatible on staging: registered there; the new version is the leftover;
// the result names the staging level and id; production untouched (TC5).
func TestStageRegisterCompatible(t *testing.T) {
	stg := &stageRegistry{versions: []int{1, 2}, level: "BACKWARD", compatible: true, regID: 41}
	got, err := regAction(t, stg).Stage(stageCtx(), json.RawMessage(regRaw))
	if err != nil || got.Outcome() != events.StageHealthy {
		t.Fatalf("result %+v, %v", got, err)
	}
	if got.Detail != "compatible under staging level BACKWARD; registered as schema id 41 (version 3)" {
		t.Fatalf("detail %q", got.Detail)
	}
	if !strings.Contains(got.Leftover, "keeps version 3 (schema id 41) on staging") {
		t.Fatalf("leftover %q", got.Leftover)
	}
	if got.DryRun == nil || !got.DryRun.OK || got.DryRun.Text != "compatible: true; staging level BACKWARD" {
		t.Fatalf("dry run %+v", got.DryRun)
	}
	// The check comes before the write.
	if i, j := slices.Index(stg.ops, "check"), slices.Index(stg.ops, "register"); i < 0 || i > j {
		t.Fatalf("ops %v", stg.ops)
	}

	stg = &stageRegistry{versions: []int{1}, global: "FULL", compatible: true, regID: 7, dedupe: true}
	got, err = regAction(t, stg).Stage(stageCtx(), json.RawMessage(regRaw))
	if err != nil || got.Leftover != "" || !strings.Contains(got.Detail, "(already registered; no new version)") || !strings.Contains(got.Detail, "FULL (global)") {
		t.Fatalf("already registered: %+v, %v", got, err)
	}
}

// TC4: incompatible on staging: failed, and no register call.
func TestStageRegisterIncompatible(t *testing.T) {
	stg := &stageRegistry{versions: []int{1}, level: "BACKWARD", msgs: []string{"amount: type changed from bytes to string"}}
	got, err := regAction(t, stg).Stage(stageCtx(), json.RawMessage(regRaw))
	if err != nil || got.Healthy || got.Applied || got.Outcome() != events.StageFailed {
		t.Fatalf("result %+v, %v", got, err)
	}
	if !strings.Contains(got.DryRun.Text, "amount: type changed") || got.DryRun.Source != sourceCompat {
		t.Fatalf("dry run %+v", got.DryRun)
	}
	if stg.did("register") {
		t.Fatal("an incompatible schema was registered on staging")
	}

	// The Registry's own refusal (invalid schema) is a verdict too; its
	// message is not kept (it can quote the schema).
	stg = &stageRegistry{versions: []int{1}, checkErr: &schemareg.Error{Code: 42201, Message: `Invalid schema {"secret":"x"}`}}
	got, err = regAction(t, stg).Stage(stageCtx(), json.RawMessage(regRaw))
	if err != nil || got.Outcome() != events.StageFailed || strings.Contains(got.DryRun.Text, "secret") || stg.did("register") {
		t.Fatalf("invalid schema: %+v, %v", got, err)
	}
}

// A subject that is not on staging (or has no version) is not applied.
func TestStageRegisterMissingSubject(t *testing.T) {
	stg := &stageRegistry{missing: true}
	got, err := regAction(t, stg).Stage(stageCtx(), json.RawMessage(regRaw))
	if err != nil || got.Applied || got.Detail != "subject not present in staging" || stg.did("register") || stg.did("check") {
		t.Fatalf("result %+v, %v", got, err)
	}
	stg = &stageRegistry{checkErr: clients.ErrNotFound}
	got, err = regAction(t, stg).Stage(stageCtx(), json.RawMessage(regRaw))
	if err != nil || got.Applied || !strings.HasPrefix(got.Detail, "subject not present in staging") || stg.did("register") {
		t.Fatalf("no versions: %+v, %v", got, err)
	}
}

// A staging client without the compatibility check registers nothing.
func TestStageRegisterNeedsCheck(t *testing.T) {
	var stg struct{ clients.SchemaRegistryClient }
	inner := &stageRegistry{versions: []int{1}, compatible: true}
	stg.SchemaRegistryClient = inner
	got, err := regAction(t, stg).Stage(stageCtx(), json.RawMessage(regRaw))
	if err != nil || got.Applied || got.Outcome() != events.StageNotApplied || inner.did("register") {
		t.Fatalf("result %+v, %v", got, err)
	}
}

// Exactly the three X8b actions are stageable among the Connect and Registry
// writes of Register, so require_staging: may name them and no other.
func TestStageableActions(t *testing.T) {
	reg := NewRegistry()
	if err := Register(reg, Deps{}); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, a := range reg.All() {
		if _, ok := a.(Stager); ok {
			got = append(got, a.Name())
		}
	}
	slices.Sort(got)
	if want := []string{"connect_update_connector_config", "schema_register", "schema_set_compatibility"}; !slices.Equal(got, want) {
		t.Fatalf("stageable = %v, want %v", got, want)
	}
}

// ---- schema_set_compatibility ----

const compatRaw = `{"subject":"pg.public.orders-value","level":"NONE"}`

// A subject with its own level: set on staging, read back, and set back to
// the old level; production untouched (TC5).
func TestStageCompatSubjectLevel(t *testing.T) {
	stg := &stageRegistry{level: "BACKWARD"}
	a := &setCompatibility{d: Deps{Connect: newProdConnect(t), Registry: prodRegistry{t}, Staging: &Staging{Registry: stg}}}
	got, err := a.Stage(stageCtx(), json.RawMessage(compatRaw))
	if err != nil || got.Outcome() != events.StageHealthy || got.Leftover != "" {
		t.Fatalf("result %+v, %v", got, err)
	}
	if got.Detail != "BACKWARD → NONE on staging; accepted and read back" {
		t.Fatalf("detail %q", got.Detail)
	}
	if !slices.Equal(stg.sets, []string{"NONE", "BACKWARD"}) || stg.level != "BACKWARD" {
		t.Fatalf("sets %v, level %q", stg.sets, stg.level)
	}
}

// A subject that followed the global level gets its subject level removed
// again; without the delete method that is a leftover.
func TestStageCompatGlobalLevel(t *testing.T) {
	stg := &stageRegistry{global: "BACKWARD"}
	a := &setCompatibility{d: Deps{Staging: &Staging{Registry: stg}}}
	got, err := a.Stage(stageCtx(), json.RawMessage(compatRaw))
	if err != nil || got.Outcome() != events.StageHealthy || got.Leftover != "" || stg.deletes != 1 || stg.level != "" {
		t.Fatalf("result %+v, %v (deletes %d, level %q)", got, err, stg.deletes, stg.level)
	}

	var noDel struct{ clients.SchemaRegistryClient }
	inner := &stageRegistry{global: "BACKWARD"}
	noDel.SchemaRegistryClient = inner
	a = &setCompatibility{d: Deps{Staging: &Staging{Registry: noDel}}}
	got, err = a.Stage(stageCtx(), json.RawMessage(compatRaw))
	if err != nil || !got.Healthy || !strings.Contains(got.Leftover, "keeps level NONE on staging") {
		t.Fatalf("no delete: %+v, %v", got, err)
	}
}

// Already at the level on staging: no write.
func TestStageCompatAlreadySet(t *testing.T) {
	stg := &stageRegistry{level: "NONE"}
	got, err := (&setCompatibility{d: Deps{Staging: &Staging{Registry: stg}}}).Stage(stageCtx(), json.RawMessage(compatRaw))
	if err != nil || got.Outcome() != events.StageHealthy || stg.did("set") {
		t.Fatalf("result %+v, %v", got, err)
	}
}

// The restore of a level retries on its own context: a cancelled stage still
// puts the old level back, and a failing first attempt is retried.
func TestStageCompatRestoreAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(stageCtx())
	cancel()
	stg := &stageRegistry{level: "BACKWARD"}
	a := &setCompatibility{d: Deps{Staging: &Staging{Registry: stg}}}
	// The fake ignores ctx, so the set lands; the restore must not inherit
	// the stage's cancellation.
	if _, err := a.Stage(ctx, json.RawMessage(compatRaw)); err != nil {
		t.Fatal(err)
	}
	if stg.level != "BACKWARD" {
		t.Fatalf("level %q after a cancelled stage", stg.level)
	}

	// Call 0 is the stage's set; call 1, the first restore attempt, fails.
	stg = &stageRegistry{level: "BACKWARD", failSet: func(n int) bool { return n == 1 }}
	a = &setCompatibility{d: Deps{stagePoll: time.Millisecond, Staging: &Staging{Registry: stg}}}
	got, err := a.Stage(stageCtx(), json.RawMessage(compatRaw))
	if err != nil || got.Leftover != "" || stg.level != "BACKWARD" || stg.setN != 3 {
		t.Fatalf("result %+v, %v (sets %d, level %q)", got, err, stg.setN, stg.level)
	}

	// A restore that never succeeds is a leftover without the client's text.
	stg = &stageRegistry{level: "BACKWARD", failSet: func(n int) bool { return n > 0 }}
	a = &setCompatibility{d: Deps{stagePoll: time.Millisecond, restoreTimeout: 50 * time.Millisecond, Staging: &Staging{Registry: stg}}}
	got, err = a.Stage(stageCtx(), json.RawMessage(compatRaw))
	if err != nil || !strings.Contains(got.Leftover, "staging unreachable); staging keeps level NONE") || strings.Contains(got.Leftover, "connect-staging") {
		t.Fatalf("failed restore: %+v, %v", got, err)
	}

	// A set that fails is no verdict, and its error has no URL.
	stg = &stageRegistry{level: "BACKWARD", failSet: func(n int) bool { return n == 0 }}
	a = &setCompatibility{d: Deps{stagePoll: time.Millisecond, Staging: &Staging{Registry: stg}}}
	_, err = a.Stage(stageCtx(), json.RawMessage(compatRaw))
	if err == nil || strings.Contains(err.Error(), "connect-staging") || !strings.Contains(err.Error(), "staging unreachable") {
		t.Fatalf("set error: %v", err)
	}
	if stg.level != "BACKWARD" {
		t.Fatalf("level %q", stg.level)
	}
}
