package wiring

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// spy implements every client interface the tools use, including the
// optional ones (PublicationTables, DescribeTopicConfigs, DLQReader), and
// counts every call. Its data names both fixture pipelines' objects, so only
// the tools' own scope rules and the client set they were built over decide
// what is read.
type spy struct {
	label string
	calls atomic.Int64
	mu    sync.Mutex
	what  []string
}

func (s *spy) hit(m string) {
	s.calls.Add(1)
	s.mu.Lock()
	s.what = append(s.what, m)
	s.mu.Unlock()
}

func (s *spy) saw(m string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Contains(s.what, m)
}

func (s *spy) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	s.hit("ReplicationSlots")
	return []clients.ReplicationSlot{{Name: "debezium", Active: true}}, nil
}
func (s *spy) RecentDDL(context.Context, time.Time, int) ([]clients.DDLChange, error) {
	s.hit("RecentDDL")
	return nil, nil
}
func (s *spy) LongTransactions(context.Context, time.Duration) ([]clients.Transaction, error) {
	s.hit("LongTransactions")
	return nil, nil
}

// LongTransactionsOnTables lets pg_long_transactions run: since #107 it
// refuses a source client that cannot filter by table.
func (s *spy) LongTransactionsOnTables(context.Context, time.Duration, []string) (postgres.TableTransactions, error) {
	s.hit("LongTransactionsOnTables")
	return postgres.TableTransactions{}, nil
}
func (s *spy) Row(_ context.Context, _, _, pk string) (map[string]any, bool, error) {
	s.hit("Row")
	return map[string]any{"id": pk}, true, nil
}
func (s *spy) PublicationTables(_ context.Context, _, _ string, tables []string) ([]postgres.PublicationTable, error) {
	s.hit("PublicationTables")
	var out []postgres.PublicationTable
	for _, t := range tables {
		schema, table, _ := strings.Cut(t, ".")
		out = append(out, postgres.PublicationTable{Schema: schema, Table: table, InPublication: true, Selectable: true})
	}
	return out, nil
}

// spySourceReads are the source methods of spy. spySource holds the spy
// behind them only, as the real source client (*postgres.DB) has no write
// method: the P3 pg actions hold the source client, and the reachability
// check must not see the spy's Connect and Registry writes through it.
type spySourceReads interface {
	clients.SourceDB
	LongTransactionsOnTables(context.Context, time.Duration, []string) (postgres.TableTransactions, error)
	PublicationTables(context.Context, string, string, []string) ([]postgres.PublicationTable, error)
	ToastColumns(context.Context, []string) ([]clients.ToastTable, bool, error)
	clients.CountReader
}

type spySource struct{ spySourceReads }

// checkedSlotDropperMethods mirrors pgactions' unexported checkedSlotDropper
// (internal/actions/pgactions/drop_replication_slot.go): the method
// pg_drop_replication_slot's Execute looks for on its writer. Keep the two
// method sets equal; a change there must be copied here.
type checkedSlotDropperMethods interface {
	DropReplicationSlotChecked(ctx context.Context, slot string, allowLost bool) (postgres.SlotSeen, error)
}

// TestSourceReaderHasNoWriteMethod pins the premise spySource rests on: the
// real source client (*postgres.DB), which the P3 pg actions hold as Source in
// both write modes, has no write method. If it ever gained one, the actions
// could write through the reader connection with write mode off.
func TestSourceReaderHasNoWriteMethod(t *testing.T) {
	db := reflect.TypeOf((*postgres.DB)(nil))
	for _, iface := range []reflect.Type{ifaceOf[clients.SourceWriter](), ifaceOf[checkedSlotDropperMethods]()} {
		if db.Implements(iface) {
			t.Errorf("*postgres.DB implements %v: the source reader must hold no write method", iface)
		}
	}
	// The checked drop does exist on the writer, so the check above is not
	// vacuous through a renamed method.
	if !reflect.TypeOf((*postgres.Writer)(nil)).Implements(ifaceOf[checkedSlotDropperMethods]()) {
		t.Fatal("*postgres.Writer lacks DropReplicationSlotChecked: update checkedSlotDropperMethods")
	}
}

func (s *spy) Freshness(context.Context, string, string) (time.Time, error) {
	s.hit("Freshness")
	return time.Now(), nil
}
func (s *spy) LoadErrors(context.Context, string, time.Time, int) ([]clients.LoadError, error) {
	s.hit("LoadErrors")
	return nil, nil
}

// ToastColumns answers the pg_toast_risk catalog read (X1d) with no columns.
func (s *spy) ToastColumns(_ context.Context, tables []string) ([]clients.ToastTable, bool, error) {
	s.hit("ToastColumns")
	out := make([]clients.ToastTable, 0, len(tables))
	for _, t := range tables {
		out = append(out, clients.ToastTable{Table: t, Found: true, ReplicaIdentity: "DEFAULT"})
	}
	return out, false, nil
}
func (s *spy) CountByBucket(context.Context, string, string, time.Time, time.Time, time.Duration) ([]clients.MinuteCount, error) {
	s.hit("CountByBucket")
	return nil, nil
}
func (s *spy) ListConnectors(context.Context) ([]string, error) {
	s.hit("ListConnectors")
	return []string{"pa-connector", "pb-connector"}, nil
}
func (s *spy) Status(_ context.Context, name string) (clients.ConnectorStatus, error) {
	s.hit("Status")
	return clients.ConnectorStatus{Name: name, State: "RUNNING", Tasks: []clients.TaskStatus{{ID: 0, State: "RUNNING"}}}, nil
}
func (s *spy) Config(context.Context, string) (map[string]string, error) {
	s.hit("Config")
	return map[string]string{
		"connector.class":  "io.debezium.connector.postgresql.PostgresConnector",
		"database.user":    "cdc_reader",
		"publication.name": "dbz_publication",
	}, nil
}
func (s *spy) RestartTask(context.Context, string, int) error { s.hit("RestartTask"); return nil }
func (s *spy) RestartConnector(context.Context, string) error {
	s.hit("RestartConnector")
	return nil
}
func (s *spy) Topics(context.Context) ([]string, error) {
	s.hit("Topics")
	return []string{"pa.public.a", "pb.public.b"}, nil
}
func (s *spy) TopicOffsets(context.Context, string) ([]clients.PartitionOffsets, error) {
	s.hit("TopicOffsets")
	return []clients.PartitionOffsets{{Partition: 0, End: 1}}, nil
}
func (s *spy) ConsumerGroups(context.Context) ([]string, error) {
	s.hit("ConsumerGroups")
	return []string{"pa-sync", "pb-sync"}, nil
}
func (s *spy) GroupLag(_ context.Context, g string) (clients.GroupLag, error) {
	s.hit("GroupLag")
	return clients.GroupLag{Group: g}, nil
}
func (s *spy) FindByKey(context.Context, string, string, time.Time, int) ([]clients.RecordRef, error) {
	s.hit("FindByKey")
	return nil, nil
}
func (s *spy) DescribeTopicConfigs(_ context.Context, topic string) (kafka.TopicConfig, error) {
	s.hit("DescribeTopicConfigs")
	return kafka.TopicConfig{Topic: topic, Partitions: 1, ReplicationFactor: 1, RetentionMS: "-1", RetentionBytes: "-1", CleanupPolicy: "delete", MinInsyncReplicas: "1"}, nil
}
func (s *spy) DLQOffsets(_ context.Context, topics ...string) (map[string][]clients.PartitionOffsets, error) {
	s.hit("DLQOffsets")
	out := map[string][]clients.PartitionOffsets{}
	for _, t := range topics {
		out[t] = []clients.PartitionOffsets{{Partition: 0, Start: 0, End: 2}}
	}
	return out, nil
}
func (s *spy) PeekDLQ(context.Context, string, int) ([]clients.DLQRecord, error) {
	s.hit("PeekDLQ")
	return nil, nil
}
func (s *spy) Subjects(context.Context) ([]string, error) {
	s.hit("Subjects")
	return []string{"pa.public.a-value", "pb.public.b-value"}, nil
}
func (s *spy) Versions(context.Context, string) ([]int, error) {
	s.hit("Versions")
	return []int{1}, nil
}
func (s *spy) Schema(_ context.Context, subject string, v int) (clients.SchemaVersion, error) {
	s.hit("Schema")
	return clients.SchemaVersion{Subject: subject, Version: 1, ID: 1, Schema: `"string"`}, nil
}
func (s *spy) Compatibility(context.Context, string) (string, bool, error) {
	s.hit("Compatibility")
	return "BACKWARD", true, nil
}
func (s *spy) SetCompatibility(context.Context, string, string) error {
	s.hit("SetCompatibility")
	return nil
}
func (s *spy) Register(context.Context, string, string) (int, error) {
	s.hit("Register")
	return 1, nil
}

var (
	_ clients.SourceDB             = (*spy)(nil)
	_ clients.Warehouse            = (*spy)(nil)
	_ clients.ConnectClient        = (*spy)(nil)
	_ clients.KafkaAdmin           = (*spy)(nil)
	_ clients.DLQReader            = (*spy)(nil)
	_ clients.SchemaRegistryClient = (*spy)(nil)
)

// twoTenantYAML: pipeline pa (tenant a) reads its DSNs from A_* variables,
// pb (tenant b) from B_*; nothing is shared. Each declares one DLQ topic, and
// top-level layers: names a warehouse owner (for the LayerOwner route).
func twoTenantYAML(extraPipelines string) string {
	conns := func(p string) string {
		return `
    connections:
      source_db_url: {env: ` + p + `_SOURCE_DB_URL}
      warehouse_db_url: {env: ` + p + `_WAREHOUSE_DB_URL}
      kafka_brokers: {env: ` + p + `_KAFKA_BROKERS}
      connect_url: {env: ` + p + `_CONNECT_URL}
      schema_registry_url: {env: ` + p + `_SR_URL}
      kafka_sasl_password: {env: ` + p + `_KAFKA_PASSWORD}`
	}
	pipe := func(id, tenant, table, p string) string {
		return `
  - id: ` + id + `
    tenant: ` + tenant + `
    topic_prefix: ` + id + `
    source_tables: [public.` + table + `]
    connectors: [` + id + `-connector]
    consumer_groups: [` + id + `-sync]
    sink_tables: [analytics.` + table + `]
    sink_ts_column: updated_at
    watch:
      dlq_topics:
        - {topic: ` + id + `-dlq, written_by: ` + id + `-sync}` + conns(p)
	}
	return "tenants:\n  a: {}\n  b: {}\nlayers:\n  warehouse: user:wendy\npipelines:" +
		pipe("pa", "a", "a", "A") + pipe("pb", "b", "b", "B") + extraPipelines + "\n"
}

func loadOwners(t *testing.T, y string) *owners.Owners {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	return o
}

// env is a fake environment with no home directory; "secret-*" values let a
// test check that no value reaches an error.
func env(m map[string]string) connSource {
	return connSource{
		lookupEnv: func(k string) (string, bool) { v, ok := m[k]; return v, ok },
		readFile:  func(p string) ([]byte, error) { return nil, &fs.PathError{Op: "open", Path: p, Err: fs.ErrNotExist} },
		canRead:   func(p string) error { return &fs.PathError{Op: "open", Path: p, Err: fs.ErrNotExist} },
		homeDir:   func() (string, error) { return "", errors.New("no home in tests") },
		exists:    func(string) bool { return false },
	}
}

func twoTenantEnv() map[string]string {
	m := map[string]string{}
	for _, p := range []string{"A", "B"} {
		l := strings.ToLower(p)
		m[p+"_SOURCE_DB_URL"] = "src-" + l
		m[p+"_WAREHOUSE_DB_URL"] = "wh-" + l
		m[p+"_KAFKA_BROKERS"] = "kafka-" + l
		m[p+"_CONNECT_URL"] = "connect-" + l
		m[p+"_SR_URL"] = "sr-" + l
		m[p+"_KAFKA_PASSWORD"] = "secret-kafka-" + l
	}
	return m
}

// fakeOpener records each set's settings and gives it a spy labelled by its
// source DSN, wired exactly as openClients wires real clients: with write
// mode on, the writers come from the real openWriters over fake constructors
// (writemode_test.go), so a set gets a source writer only when its own
// settings hold SOURCE_DB_WRITE_URL.
type fakeOpener struct {
	mu      sync.Mutex
	cfgs    []config.Config
	spies   map[string]*spy
	closed  []string
	failOn  string // a source DSN whose open fails
	openErr error
	// writers lists every fake write client built, in order.
	writers []*fakeWriter
}

func newFakeOpener() *fakeOpener { return &fakeOpener{spies: map[string]*spy{}} }

func (f *fakeOpener) open(ctx context.Context, cfg config.Config, awsCfg *aws.Config, o *owners.Owners, write bool) (*Wired, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cfgs = append(f.cfgs, cfg)
	if f.failOn != "" && cfg.SourceDBURL == f.failOn {
		return nil, f.openErr
	}
	s := &spy{label: cfg.SourceDBURL}
	f.spies[cfg.SourceDBURL] = s
	label := cfg.SourceDBURL
	closeWriters := func() {}
	var wc writeClients
	if write {
		var err error
		// The fake constructors append to f.writers; f.mu is already held.
		wc, closeWriters, err = openWriters(ctx, cfg, awsCfg, fakeWriterOpeners(label, &f.writers))
		if err != nil {
			return nil, err
		}
	}
	return &Wired{
		Source: spySource{s}, Warehouse: s, Connect: s, Kafka: s, Registry: s,
		Builder: topology.NewBuilder(o, s, s, s, s),
		Owners:  o,
		write:   write,
		writers: wc,
		close: func() {
			closeWriters()
			f.mu.Lock()
			f.closed = append(f.closed, label)
			f.mu.Unlock()
		},
	}, nil
}

func (f *fakeOpener) deps(src connSource) stackDeps {
	return stackDeps{open: f.open, src: src, loadAWS: func(context.Context, config.Config) (*aws.Config, error) {
		return nil, errors.New("loadAWS must not be called")
	}}
}

// inputFor builds a valid input for a tool from its schema, naming the
// objects of the pipeline with this id and table.
func inputFor(tl tools.Tool, id, table string) json.RawMessage {
	props, _ := tl.InputSchema()["properties"].(map[string]any)
	vals := map[string]any{
		"connector": id + "-connector", "topic": id + ".public." + table, "group": id + "-sync",
		"subject": id + ".public." + table + "-value", "table": "public." + table, "pk_column": "id",
		"pk": "1", "key": "1", "pipeline_id": id, "query": "connector failed",
		"since": time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
	}
	switch {
	case strings.HasPrefix(tl.Name(), "warehouse_"):
		vals["table"] = "analytics." + table
		vals["ts_column"] = "updated_at"
	case tl.Name() == "dlq_peek":
		// Exactly one of connector or topic: the DLQ topic owners.yaml declares.
		delete(vals, "connector")
		vals["topic"] = id + "-dlq"
	}
	in := map[string]any{}
	for name, raw := range props {
		if v, ok := vals[name]; ok {
			in[name] = v
			continue
		}
		if p, _ := raw.(map[string]any); p != nil && (p["type"] == "integer" || p["type"] == "number") {
			in[name] = 1
		}
	}
	b, _ := json.Marshal(in)
	return b
}

// toolRun is one read tool run: its error and how many calls it made on its
// own set's spy and on the other set's.
type toolRun struct {
	name       string
	err        error
	own, other int64
}

// runEach calls every read tool of reg once, with ctx scoped to pipeline id.
func runEach(t *testing.T, reg *tools.Registry, id, table string, own, other *spy) []toolRun {
	t.Helper()
	var out []toolRun
	for _, tl := range reg.All() {
		if tl.Kind() != tools.KindRead {
			continue
		}
		o, x := own.calls.Load(), other.calls.Load()
		ctx, cancel := context.WithTimeout(tools.WithPipelineID(context.Background(), id), 5*time.Second)
		_, err := tl.Run(ctx, inputFor(tl, id, table))
		cancel()
		out = append(out, toolRun{name: tl.Name(), err: err, own: own.calls.Load() - o, other: other.calls.Load() - x})
	}
	return out
}

// clientFree are the read tools that need no client of the set: runbook text is
// embedded, and the debezium tools answer "not configured" without a metrics URL.
var clientFree = []string{"runbook_lookup", "debezium_metrics", "debezium_snapshot_progress"}

// TestTwoTenantsToolsHitOnlyTheirOwnClients is the core isolation test: each
// pipeline's tools, actions and topology reach only the spy built from that
// pipeline's own variables.
func TestTwoTenantsToolsHitOnlyTheirOwnClients(t *testing.T) {
	own := loadOwners(t, twoTenantYAML(""))
	f := newFakeOpener()
	// Write mode on: the actions part below runs a restart through each set
	// (with it off the actions hold read-only clients; writemode_test.go).
	st, err := openStacks(context.Background(), config.Config{SourceDBURL: "src-process", WriteMode: "on"}, nil, own, StackOptions{}, f.deps(env(twoTenantEnv())))
	if err != nil {
		t.Fatalf("openStacks: %v", err)
	}
	defer st.Close()

	// Two sets, each from its own variables; the process-wide set is not
	// opened because every pipeline has its own connections.
	if len(f.cfgs) != 2 || len(st.All()) != 2 {
		t.Fatalf("opened %d sets (%d), want 2", len(f.cfgs), len(st.All()))
	}
	for _, c := range f.cfgs {
		if c.SourceDBURL == "src-process" {
			t.Fatal("the process-wide set was opened although no pipeline uses it")
		}
	}
	a, b := f.spies["src-a"], f.spies["src-b"]
	if a == nil || b == nil {
		t.Fatalf("spies = %v", f.spies)
	}
	sa, sb := st.For("pa"), st.For("pb")
	if sa == sb || sa.Name != "pipeline pa" || sb.Name != "pipeline pb" {
		t.Fatalf("sets: %q %q", sa.Name, sb.Name)
	}

	// Every read tool of each set succeeds, every one that needs a client
	// reaches its own set's spy, and none reaches the other's.
	for _, side := range []struct {
		set        *Stack
		id, table  string
		own, other *spy
	}{{sa, "pa", "a", a, b}, {sb, "pb", "b", b, a}} {
		runs := runEach(t, side.set.Tools, side.id, side.table, side.own, side.other)
		if len(runs) < 22 {
			t.Fatalf("%s: only %d read tools", side.id, len(runs))
		}
		t.Logf("%s: %d read tools", side.id, len(runs))
		for _, r := range runs {
			if r.err != nil {
				t.Errorf("%s %s: %v", side.id, r.name, r.err)
			}
			if r.other != 0 {
				t.Errorf("%s %s reached the other set's clients %d times", side.id, r.name, r.other)
			}
			if r.own == 0 && !slices.Contains(clientFree, r.name) {
				t.Errorf("%s %s never reached its own set's clients", side.id, r.name)
			}
		}
	}

	// A context that names the other pipeline (a bug in a caller, or a stale
	// context) still cannot reach it: pa's tools do not even know pb.
	beforeA, beforeB := a.calls.Load(), b.calls.Load()
	for _, r := range runEach(t, sa.Tools, "pb", "b", a, b) {
		if slices.Contains(clientFree, r.name) {
			continue
		}
		if !errors.Is(r.err, tools.ErrOutOfScope) && !errors.Is(r.err, tools.ErrBadInput) {
			t.Errorf("pa %s with pb scope: err = %v, want ErrOutOfScope", r.name, r.err)
		}
	}
	if a.calls.Load() != beforeA || b.calls.Load() != beforeB {
		t.Fatal("a pa tool run with pb's scope touched a client")
	}

	// Actions: each set's restart runs on its own Connect only.
	for _, side := range []struct {
		set        *Stack
		connector  string
		own, other *spy
	}{{sa, "pa-connector", a, b}, {sb, "pb-connector", b, a}} {
		act, ok := side.set.Actions.Get("connect_restart_connector")
		if !ok {
			t.Fatalf("%s has no connect_restart_connector", side.set.Name)
		}
		otherBefore := side.other.calls.Load()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := act.Execute(ctx, json.RawMessage(`{"connector":"`+side.connector+`"}`))
		cancel()
		if err != nil {
			t.Fatalf("%s restart: %v", side.set.Name, err)
		}
		if !side.own.saw("RestartConnector") || side.other.calls.Load() != otherBefore {
			t.Fatalf("%s restart: own=%v other moved=%v", side.set.Name, side.own.what, side.other.calls.Load() != otherBefore)
		}
	}
	for _, s := range []*spy{a, b} {
		n := 0
		for _, m := range s.what {
			if m == "RestartConnector" {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("%s's Connect was restarted %d times, want once (its own set's restart)", s.label, n)
		}
	}

	// Topology routes by pipeline id: Build, Render, Trace, LayerOwner.
	beforeA, beforeB = a.calls.Load(), b.calls.Load()
	g, err := st.Build(context.Background(), "pb")
	if err != nil || g.PipelineID != "pb" {
		t.Fatalf("Build(pb) = %+v, %v", g, err)
	}
	if a.calls.Load() != beforeA || b.calls.Load() == beforeB {
		t.Fatal("Build(pb) must read pb's clients only")
	}
	if st.Render(g) == "" {
		t.Fatal("Render(pb graph) is empty")
	}
	beforeA, beforeB = a.calls.Load(), b.calls.Load()
	if _, err := st.Trace(context.Background(), "pb", "public.b", "1"); err != nil {
		t.Fatalf("Trace(pb): %v", err)
	}
	if a.calls.Load() != beforeA || b.calls.Load() == beforeB {
		t.Fatal("Trace(pb) must read pb's clients only")
	}
	if who, ok := st.LayerOwner("pb", "warehouse"); !ok || who != "wendy" {
		t.Fatalf("LayerOwner(pb, warehouse) = %q, %v", who, ok)
	}
	if who, ok := st.LayerOwner("nope", "warehouse"); ok || who != "" {
		t.Fatalf("LayerOwner(unknown) = %q, %v; the empty set knows no owner", who, ok)
	}

	// The settings: no inheritance from the process-wide config.
	for _, c := range f.cfgs {
		if c.KafkaSecurityProtocol != "" || c.ConnectPassword != "" {
			t.Fatalf("a pipeline set inherited process settings: %+v", c)
		}
	}
}

// TestUnknownPipelineGetsEmptySet: a room whose pipeline is unknown or empty
// gets no client at all, also when a process-wide set exists.
func TestUnknownPipelineGetsEmptySet(t *testing.T) {
	for _, withProcess := range []bool{false, true} {
		y := twoTenantYAML("")
		if withProcess {
			y = strings.Replace(y, "tenants:\n  a: {}\n  b: {}\n", "tenants:\n  a: {}\n  b: {}\n  c: {}\n", 1) +
				"  - {id: pc, tenant: c, topic_prefix: pc, source_tables: [public.c], connectors: [pc-connector]}\n"
		}
		f := newFakeOpener()
		st, err := openStacks(context.Background(), config.Config{SourceDBURL: "src-process"}, nil, loadOwners(t, y), StackOptions{}, f.deps(env(twoTenantEnv())))
		if err != nil {
			t.Fatalf("openStacks: %v", err)
		}
		if got := len(st.All()); got != map[bool]int{false: 2, true: 3}[withProcess] {
			t.Fatalf("withProcess=%v: %d sets", withProcess, got)
		}
		for _, id := range []string{"", "nope"} {
			s := st.For(id)
			if s.Name != NoStack || s.Clients != nil || len(s.Actions.All()) != 0 {
				t.Fatalf("withProcess=%v: For(%q) = %q", withProcess, id, s.Name)
			}
			names := []string{}
			for _, tl := range s.Tools.All() {
				names = append(names, tl.Name())
			}
			if !slices.Equal(names, []string{"runbook_lookup"}) {
				t.Fatalf("empty set tools = %v", names)
			}
			if _, err := st.Build(context.Background(), id); !errors.Is(err, errNoClientSet) {
				t.Fatalf("Build(%q) err = %v", id, err)
			}
			if _, err := st.Trace(context.Background(), id, "public.a", "1"); !errors.Is(err, errNoClientSet) {
				t.Fatalf("Trace(%q) err = %v", id, err)
			}
		}
		st.Close()
	}
}

// TestMixedProcessWideAndOwn: a pipeline with no connections uses the
// process-wide set (today's behaviour), and pipeline sets inherit nothing.
func TestMixedProcessWideAndOwn(t *testing.T) {
	y := strings.Replace(twoTenantYAML(""), "tenants:\n  a: {}\n  b: {}\n", "tenants:\n  a: {}\n  b: {}\n  c: {}\n", 1) +
		"  - {id: pc, tenant: c, topic_prefix: pc, source_tables: [public.c], connectors: [pc-connector]}\n"
	own := loadOwners(t, y)
	f := newFakeOpener()
	proc := config.Config{SourceDBURL: "src-process", KafkaSASLPassword: "secret-process",
		ConnectUsername: "process-user", ConnectPassword: "secret-process-connect", KafkaSecurityProtocol: "SASL_SSL", WarehouseKind: "snowflake"}
	st, err := openStacks(context.Background(), proc, nil, own, StackOptions{}, f.deps(env(twoTenantEnv())))
	if err != nil {
		t.Fatalf("openStacks: %v", err)
	}
	defer st.Close()
	if got := st.For("pc"); got.Name != ProcessWideStack || !slices.Equal(got.Pipelines, []string{"pc"}) {
		t.Fatalf("For(pc) = %q %v", got.Name, got.Pipelines)
	}
	if st.All()[0].Name != ProcessWideStack {
		t.Fatal("the process-wide set comes first")
	}
	// The process-wide set's owners view holds pc only.
	if _, ok := st.For("pc").Clients.Owners.Pipeline("pa"); ok {
		t.Fatal("the process-wide view must not know pa")
	}
	// No inheritance: a key a pipeline leaves out stays empty, it never takes
	// the process-wide value.
	for _, c := range f.cfgs {
		if c.SourceDBURL == "src-process" {
			if c.ConnectPassword != "secret-process-connect" {
				t.Fatal("the process-wide set must keep its own settings")
			}
			continue
		}
		if c.KafkaSASLPassword == "secret-process" || c.ConnectUsername != "" || c.ConnectPassword != "" || c.KafkaSecurityProtocol != "" || c.WarehouseKind != "" {
			t.Fatalf("a pipeline set inherited process-wide settings: %+v", c)
		}
	}
}

// TestNoPipelinesOpensNothing: an owners file with no pipeline opens no
// client set; every id gets the empty set.
func TestNoPipelinesOpensNothing(t *testing.T) {
	f := newFakeOpener()
	st, err := openStacks(context.Background(), config.Config{SourceDBURL: "src-process"}, nil, loadOwners(t, "{}\n"), StackOptions{}, f.deps(env(nil)))
	if err != nil {
		t.Fatalf("openStacks: %v", err)
	}
	defer st.Close()
	if len(f.cfgs) != 0 || st.For("anything").Name != NoStack {
		t.Fatalf("cfgs=%d set=%q", len(f.cfgs), st.For("anything").Name)
	}
}

func TestOpenStacksErrors(t *testing.T) {
	own := loadOwners(t, twoTenantYAML(""))
	cases := []struct {
		name   string
		mutate func(map[string]string, *connSource)
		want   []string
	}{
		{"missing env var", func(m map[string]string, _ *connSource) { delete(m, "B_CONNECT_URL") },
			[]string{`pipeline "pb"`, "connections.connect_url", "B_CONNECT_URL", "not set"}},
		{"empty env var", func(m map[string]string, _ *connSource) { m["A_SR_URL"] = "  " },
			[]string{`pipeline "pa"`, "connections.schema_registry_url", "A_SR_URL", "empty"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := twoTenantEnv()
			src := env(m)
			tc.mutate(m, &src)
			f := newFakeOpener()
			_, err := openStacks(context.Background(), config.Config{}, nil, own, StackOptions{}, f.deps(src))
			if err == nil {
				t.Fatal("want an error")
			}
			if !errors.Is(err, errConnRef) {
				t.Errorf("err = %v, want errConnRef", err)
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("err %q does not contain %q", err, w)
				}
			}
			if strings.Contains(err.Error(), "secret-") {
				t.Errorf("err leaks a value: %v", err)
			}
			if len(f.cfgs) != 0 {
				t.Errorf("%d sets opened before the references resolved", len(f.cfgs))
			}
		})
	}
}

func TestResolveFileRefs(t *testing.T) {
	dir := t.TempDir()
	dsn := filepath.Join(dir, "dsn")
	if err := os.WriteFile(dsn, []byte("  secret-dsn-from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte(" \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	big := filepath.Join(dir, "big")
	if err := os.WriteFile(big, make([]byte, maxConnFileBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	pem := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(pem, make([]byte, 3*maxConnFileBytes), 0o600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "missing")

	v, err := resolveRef("source_db_url", owners.ConnRef{File: dsn}, osConnSource)
	if err != nil || v != "secret-dsn-from-file" {
		t.Fatalf("file value = %q, %v", v, err)
	}
	// A TLS key gets the path itself, and a large CA bundle is fine.
	if v, err := resolveRef("kafka_tls_ca_file", owners.ConnRef{File: pem}, osConnSource); err != nil || v != pem {
		t.Fatalf("path key = %q, %v", v, err)
	}
	for name, tc := range map[string]struct {
		key, path, want string
	}{
		"unreadable file":     {"source_db_url", missing, "read file"},
		"unreadable pem":      {"connect_tls_cert_file", missing, "open file"},
		"empty file":          {"connect_password", empty, "is empty"},
		"file over the limit": {"connect_password", big, "larger than"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := resolveRef(tc.key, owners.ConnRef{File: tc.path}, osConnSource)
			if err == nil || !errors.Is(err, errConnRef) || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), tc.path) {
				t.Fatalf("err = %v, want %q naming %s", err, tc.want, tc.path)
			}
		})
	}
	if _, err := resolveRef("source_db_url", owners.ConnRef{}, osConnSource); !errors.Is(err, errConnRef) {
		t.Fatalf("no reference: err = %v", err)
	}
	if _, err := pipelineConfig(owners.Pipeline{ID: "x", Connections: owners.Connections{"nope": {Env: "X"}}}, osConnSource); !errors.Is(err, errConnRef) {
		t.Fatalf("unknown key: err = %v", err)
	}
}

// TestUnreadableFileStopsStart runs a file reference through openStacks.
func TestUnreadableFileStopsStart(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope", "warehouse")
	y := strings.Replace(twoTenantYAML(""), "{env: A_WAREHOUSE_DB_URL}", "{file: "+missing+"}", 1)
	own := loadOwners(t, y)
	f := newFakeOpener()
	src := env(twoTenantEnv())
	src.readFile = readCapped
	_, err := openStacks(context.Background(), config.Config{}, nil, own, StackOptions{}, f.deps(src))
	if err == nil || !strings.Contains(err.Error(), `pipeline "pa"`) || !strings.Contains(err.Error(), "connections.warehouse_db_url") {
		t.Fatalf("err = %v", err)
	}
	if len(f.cfgs) != 0 {
		t.Fatal("a set was opened")
	}
}

// TestOpenFailureClosesEarlierSets: when the second set fails, the first is
// closed and the error names the failing pipeline.
func TestOpenFailureClosesEarlierSets(t *testing.T) {
	own := loadOwners(t, twoTenantYAML(""))
	f := newFakeOpener()
	f.failOn, f.openErr = "src-b", errors.New("dial failed")
	_, err := openStacks(context.Background(), config.Config{}, nil, own, StackOptions{}, f.deps(env(twoTenantEnv())))
	if err == nil || !strings.Contains(err.Error(), `pipeline "pb"`) || !strings.Contains(err.Error(), "dial failed") {
		t.Fatalf("err = %v", err)
	}
	if !slices.Equal(f.closed, []string{"src-a"}) {
		t.Fatalf("closed = %v, want [src-a]", f.closed)
	}
}

// TestRealOpenerNamesPipeline runs the real openClients with a pipeline's own
// bad source DSN: it fails before any network call and names the pipeline.
func TestRealOpenerNamesPipeline(t *testing.T) {
	y := twoTenantYAML("")
	for _, p := range []string{"A", "B"} {
		// A SASL password without a SASL protocol is refused by netsec;
		// this test is about the source step.
		y = strings.Replace(y, "\n      kafka_sasl_password: {env: "+p+"_KAFKA_PASSWORD}", "", 1)
	}
	own := loadOwners(t, y)
	m := twoTenantEnv()
	for _, p := range []string{"A", "B"} {
		m[p+"_SOURCE_DB_URL"] = "postgres://%zz"
		m[p+"_KAFKA_BROKERS"] = "localhost:1"
		m[p+"_CONNECT_URL"] = "http://localhost:1"
		m[p+"_SR_URL"] = "http://localhost:1"
	}
	d := stackDeps{open: openClients, src: env(m)}
	_, err := openStacks(context.Background(), config.Config{}, nil, own, StackOptions{}, d)
	if err == nil || !strings.Contains(err.Error(), `pipeline "pa"`) || !strings.Contains(err.Error(), "source postgres") {
		t.Fatalf("err = %v", err)
	}
}

func TestOnlyOpensOnePipeline(t *testing.T) {
	own := loadOwners(t, twoTenantYAML(""))
	f := newFakeOpener()
	m := twoTenantEnv()
	delete(m, "A_SOURCE_DB_URL") // pa is not opened, so its references are not read
	st, err := openStacks(context.Background(), config.Config{}, nil, own, StackOptions{Only: []string{"pb"}}, f.deps(env(m)))
	if err != nil {
		t.Fatalf("openStacks: %v", err)
	}
	defer st.Close()
	if len(f.cfgs) != 1 || f.cfgs[0].SourceDBURL != "src-b" || st.For("pa").Name != NoStack {
		t.Fatalf("cfgs=%v pa=%q", f.cfgs, st.For("pa").Name)
	}
	if _, err := openStacks(context.Background(), config.Config{}, nil, own, StackOptions{Only: []string{"zz"}}, f.deps(env(m))); err == nil || !strings.Contains(err.Error(), `"zz"`) {
		t.Fatalf("unknown Only: err = %v", err)
	}
}

func TestBadWarehouseKindNamesPipeline(t *testing.T) {
	y := strings.Replace(twoTenantYAML(""), "      kafka_sasl_password: {env: B_KAFKA_PASSWORD}", "      kafka_sasl_password: {env: B_KAFKA_PASSWORD}\n      warehouse_kind: {value: oracle}", 1)
	own := loadOwners(t, y)
	_, err := openStacks(context.Background(), config.Config{}, nil, own, StackOptions{}, newFakeOpener().deps(env(twoTenantEnv())))
	if err == nil || !strings.Contains(err.Error(), `pipeline "pb"`) || !strings.Contains(err.Error(), "connections.warehouse_kind") || !strings.Contains(err.Error(), "postgres, snowflake, bigquery, databricks or clickhouse") {
		t.Fatalf("err = %v", err)
	}
}

// TestLiteralKeysNeverEchoValues: a non-secret key whose reference points at
// the wrong variable (one that holds a DSN or a password) is refused before a
// client sees it, and the error names the key and the variable only.
func TestLiteralKeysNeverEchoValues(t *testing.T) {
	cases := []struct {
		key, ref, secret string
	}{
		{"warehouse_kind", "B_WAREHOUSE_DB_URL", "postgres://u:secret-wh-marker@h/db"},
		{"kafka_security_protocol", "B_KAFKA_PASSWORD", "secret-kafka-marker"},
		{"kafka_sasl_mechanism", "B_KAFKA_PASSWORD", "secret-kafka-marker"},
		{"kafka_tls_server_name", "B_KAFKA_PASSWORD", "secret kafka marker"},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			y := strings.Replace(twoTenantYAML(""), "      kafka_sasl_password: {env: B_KAFKA_PASSWORD}",
				"      kafka_sasl_password: {env: B_KAFKA_PASSWORD}\n      "+tc.key+": {env: "+tc.ref+"}", 1)
			m := twoTenantEnv()
			m[tc.ref] = tc.secret
			f := newFakeOpener()
			_, err := openStacks(context.Background(), config.Config{}, nil, loadOwners(t, y), StackOptions{}, f.deps(env(m)))
			if err == nil || !errors.Is(err, errConnRef) {
				t.Fatalf("err = %v, want a refusal", err)
			}
			for _, w := range []string{`pipeline "pb"`, "connections." + tc.key, tc.ref} {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("err %q does not name %q", err, w)
				}
			}
			if strings.Contains(err.Error(), "marker") {
				t.Fatalf("err echoes the value: %v", err)
			}
			if len(f.cfgs) != 0 {
				t.Fatal("a set was opened")
			}
		})
	}
}

func TestLiteralOK(t *testing.T) {
	for _, tc := range []struct {
		key, v string
		ok     bool
	}{
		{"warehouse_kind", "postgres", true}, {"warehouse_kind", " Snowflake ", true}, {"warehouse_kind", "oracle", false},
		{"kafka_security_protocol", "sasl_ssl", true}, {"kafka_security_protocol", "SSL", true}, {"kafka_security_protocol", "TLS", false},
		{"kafka_sasl_mechanism", "scram-sha-512", true}, {"kafka_sasl_mechanism", "AWS_MSK_IAM", true}, {"kafka_sasl_mechanism", "GSSAPI", false},
		{"kafka_tls_server_name", "broker-1.kafka.internal", true}, {"kafka_tls_server_name", "user:pw@host", false},
		{"kafka_tls_server_name", strings.Repeat("a.", 127) + "a", false},
		{"source_db_url", "anything", false},
	} {
		if got := literalOK(tc.key, tc.v); got != tc.ok {
			t.Errorf("literalOK(%s, %q) = %v, want %v", tc.key, tc.v, got, tc.ok)
		}
	}
}

// TestMSKIAMIsASharedIdentity: two tenants on MSK IAM share the process AWS
// identity, which is refused; one tenant loads AWS when the caller had none.
func TestMSKIAMIsASharedIdentity(t *testing.T) {
	msk := "\n      kafka_security_protocol: {value: SASL_SSL}\n      kafka_sasl_mechanism: {value: AWS_MSK_IAM}"
	both := strings.Replace(twoTenantYAML(""), "{env: A_KAFKA_PASSWORD}", "{env: A_KAFKA_PASSWORD}"+msk, 1)
	both = strings.Replace(both, "{env: B_KAFKA_PASSWORD}", "{env: B_KAFKA_PASSWORD}"+msk, 1)
	_, err := openStacks(context.Background(), config.Config{}, nil, loadOwners(t, both), StackOptions{}, newFakeOpener().deps(env(twoTenantEnv())))
	if err == nil || !strings.Contains(err.Error(), awsProcessIdentityRef) {
		t.Fatalf("err = %v, want the shared AWS identity refused", err)
	}

	one := strings.Replace(twoTenantYAML(""), "{env: A_KAFKA_PASSWORD}", "{env: A_KAFKA_PASSWORD}"+msk, 1)
	f := newFakeOpener()
	d := f.deps(env(twoTenantEnv()))
	var loaded atomic.Int64
	d.loadAWS = func(_ context.Context, c config.Config) (*aws.Config, error) {
		loaded.Add(1)
		if !usesMSKIAM(c) || c.AWSRegion != "eu-west-1" {
			t.Errorf("loadAWS cfg: mechanism %q region %q", c.KafkaSASLMechanism, c.AWSRegion)
		}
		return &aws.Config{}, nil
	}
	st, err := openStacks(context.Background(), config.Config{AWSRegion: "eu-west-1"}, nil, loadOwners(t, one), StackOptions{}, d)
	if err != nil {
		t.Fatalf("one tenant on MSK IAM: %v", err)
	}
	st.Close()
	if loaded.Load() != 1 {
		t.Fatalf("loadAWS called %d times, want 1", loaded.Load())
	}
	d.loadAWS = func(context.Context, config.Config) (*aws.Config, error) { return nil, errors.New("no credentials") }
	if _, err := openStacks(context.Background(), config.Config{}, nil, loadOwners(t, one), StackOptions{}, d); err == nil || !strings.Contains(err.Error(), "MSK IAM") {
		t.Fatalf("AWS load failure: err = %v", err)
	}

	// The process-wide settings on MSK IAM count too: tenant c's pipeline
	// without connections shares the process identity with tenant a.
	mixed := strings.Replace(one, "tenants:\n  a: {}\n  b: {}\n", "tenants:\n  a: {}\n  b: {}\n  c: {}\n", 1) +
		"  - {id: pc, tenant: c, topic_prefix: pc, source_tables: [public.c], connectors: [pc-connector]}\n"
	proc := config.Config{KafkaSecurityProtocol: "SASL_SSL", KafkaSASLMechanism: "AWS_MSK_IAM", AWSRegion: "eu-west-1"}
	_, err = openStacks(context.Background(), proc, &aws.Config{}, loadOwners(t, mixed), StackOptions{}, newFakeOpener().deps(env(twoTenantEnv())))
	if err == nil || !strings.Contains(err.Error(), awsProcessIdentityRef) || !strings.Contains(err.Error(), "a, c") {
		t.Fatalf("process-wide MSK IAM: err = %v, want the shared AWS identity refused", err)
	}
}

// TestSharedTLSKeyFiles: a TLS client key or certificate reached by two
// tenants through different names (a process-wide variable, a variable that
// holds a path, a file reference, a symlink) is one shared credential. A CA
// bundle is public and may be shared.
func TestSharedTLSKeyFiles(t *testing.T) {
	dir := t.TempDir()
	key := filepath.Join(dir, "client.key")
	ca := filepath.Join(dir, "corp-ca.pem")
	for _, p := range []string{key, ca} {
		if err := os.WriteFile(p, []byte("pem"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(dir, "link.key")
	if err := os.Symlink(key, link); err != nil {
		t.Fatal(err)
	}
	withC := func(y string) string {
		return strings.Replace(y, "tenants:\n  a: {}\n  b: {}\n", "tenants:\n  a: {}\n  b: {}\n  c: {}\n", 1) +
			"  - {id: pc, tenant: c, topic_prefix: pc, source_tables: [public.c], connectors: [pc-connector]}\n"
	}
	addB := func(y, line string) string {
		return strings.Replace(y, "      kafka_sasl_password: {env: B_KAFKA_PASSWORD}", "      kafka_sasl_password: {env: B_KAFKA_PASSWORD}\n      "+line, 1)
	}
	addA := func(y, line string) string {
		return strings.Replace(y, "      kafka_sasl_password: {env: A_KAFKA_PASSWORD}", "      kafka_sasl_password: {env: A_KAFKA_PASSWORD}\n      "+line, 1)
	}
	src := func(m map[string]string) connSource {
		s := env(m)
		s.canRead = canOpen
		return s
	}
	cases := []struct {
		name    string
		yaml    string
		proc    config.Config
		env     map[string]string
		refused bool
	}{
		{"process-wide key file and tenant b's file reference", addB(withC(twoTenantYAML("")), "kafka_tls_key_file: {file: "+key+"}"),
			config.Config{KafkaTLSKeyFile: key}, nil, true},
		{"tenant a's variable holding the path and tenant b's file reference", addB(addA(twoTenantYAML(""), "kafka_tls_key_file: {env: A_KEYFILE}"), "connect_tls_key_file: {file: "+key+"}"),
			config.Config{}, map[string]string{"A_KEYFILE": key}, true},
		{"a symlink to the same key", addB(addA(twoTenantYAML(""), "kafka_tls_cert_file: {file: "+link+"}"), "kafka_tls_cert_file: {file: "+key+"}"),
			config.Config{}, nil, true},
		{"one corporate CA for everyone", addB(addA(withC(twoTenantYAML("")), "kafka_tls_ca_file: {file: "+ca+"}"), "schema_registry_tls_ca_file: {env: B_CA}"),
			config.Config{SourceDBURL: "src-process", KafkaTLSCAFile: ca, ConnectTLSCAFile: ca}, map[string]string{"B_CA": ca}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := twoTenantEnv()
			for k, v := range tc.env {
				m[k] = v
			}
			f := newFakeOpener()
			st, err := openStacks(context.Background(), tc.proc, nil, loadOwners(t, tc.yaml), StackOptions{}, f.deps(src(m)))
			if !tc.refused {
				if err != nil {
					t.Fatalf("a shared CA must be allowed: %v", err)
				}
				st.Close()
				return
			}
			if err == nil || !strings.Contains(err.Error(), owners.FileRef(key)) || !strings.Contains(err.Error(), "shared_connections") {
				t.Fatalf("err = %v, want the key file %s refused", err, key)
			}
			if len(f.cfgs) != 0 {
				t.Fatal("a set was opened")
			}
		})
	}
}

// TestLibpqDefaultsRefused: with a pipeline's own connections:, a libpq
// variable or home file that pgx would add to its DSN stops the start; the
// error names it, never its value. Process-wide pipelines alone are not
// affected (today's behaviour).
func TestLibpqDefaultsRefused(t *testing.T) {
	t.Setenv("PGPASSWORD", "secret-pg-marker")
	t.Setenv("PGHOST", "")
	err := checkLibpqDefaults(osConnSource)
	if err == nil || !errors.Is(err, errLibpqDefaults) || !strings.Contains(err.Error(), "PGPASSWORD") || strings.Contains(err.Error(), "marker") {
		t.Fatalf("PGPASSWORD set: err = %v", err)
	}
	if strings.Contains(err.Error(), "PGHOST") {
		t.Fatalf("an empty PGHOST is not set: %v", err)
	}

	// Each variable alone, through openStacks.
	own := loadOwners(t, twoTenantYAML(""))
	for _, v := range libpqVars {
		m := twoTenantEnv()
		m[v] = "secret-pg-marker"
		f := newFakeOpener()
		_, err := openStacks(context.Background(), config.Config{}, nil, own, StackOptions{}, f.deps(env(m)))
		if err == nil || !strings.Contains(err.Error(), v) || strings.Contains(err.Error(), "marker") || len(f.cfgs) != 0 {
			t.Fatalf("%s: err = %v, opened %d", v, err, len(f.cfgs))
		}
	}

	// Home files.
	for _, file := range libpqHomeFiles {
		home := t.TempDir()
		p := filepath.Join(home, file)
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("*:*:*:*:secret-pg-marker\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		src := env(twoTenantEnv())
		src.homeDir = func() (string, error) { return home, nil }
		src.exists = osConnSource.exists
		_, err := openStacks(context.Background(), config.Config{}, nil, own, StackOptions{}, newFakeOpener().deps(src))
		if err == nil || !strings.Contains(err.Error(), p) || strings.Contains(err.Error(), "marker") {
			t.Fatalf("%s: err = %v", file, err)
		}
	}

	// No connections: anywhere: the process-wide settings work as before.
	proc := loadOwners(t, "pipelines:\n  - {id: orders, topic_prefix: pg, source_tables: [public.orders], connectors: [c1]}\n")
	m := map[string]string{"PGPASSWORD": "secret-pg-marker"}
	st, err := openStacks(context.Background(), config.Config{SourceDBURL: "src-process"}, nil, proc, StackOptions{}, newFakeOpener().deps(env(m)))
	if err != nil {
		t.Fatalf("process-wide only: %v", err)
	}
	st.Close()
	// And nothing set: no refusal.
	if err := checkLibpqDefaults(env(nil)); err != nil {
		t.Fatalf("clean environment: %v", err)
	}
}

// recBuilder is a topology.Builder that only records that it was used.
type recBuilder struct{ used atomic.Int64 }

func (b *recBuilder) Build(_ context.Context, id string) (topology.Graph, error) {
	b.used.Add(1)
	return topology.Graph{PipelineID: id}, nil
}
func (b *recBuilder) Render(topology.Graph) string { return "prepared" }
func (b *recBuilder) Trace(context.Context, string, string, string) ([]events.TraceStage, error) {
	b.used.Add(1)
	return nil, nil
}
func (b *recBuilder) LayerOwner(string, string) (string, bool) { return "", false }

// TestPrepareRunsBeforeRegistration: StackOptions.Prepare sees each opened
// set's own Wired, once, before its tools are registered (so a field it sets,
// like #100's room store reader, reaches the tools).
func TestPrepareRunsBeforeRegistration(t *testing.T) {
	y := strings.Replace(twoTenantYAML(""), "tenants:\n  a: {}\n  b: {}\n", "tenants:\n  a: {}\n  b: {}\n  c: {}\n", 1) +
		"  - {id: pc, tenant: c, topic_prefix: pc, source_tables: [public.c], connectors: [pc-connector]}\n"
	f := newFakeOpener()
	seen := map[string]int{}
	builders := map[string]*recBuilder{}
	opts := StackOptions{Prepare: func(w *Wired) {
		var ids []string
		for _, p := range w.Owners.Pipelines {
			ids = append(ids, p.ID)
		}
		key := strings.Join(ids, ",")
		seen[key]++
		b := &recBuilder{}
		builders[key] = b
		w.Builder = b
	}}
	st, err := openStacks(context.Background(), config.Config{SourceDBURL: "src-process"}, nil, loadOwners(t, y), opts, f.deps(env(twoTenantEnv())))
	if err != nil {
		t.Fatalf("openStacks: %v", err)
	}
	defer st.Close()
	if len(seen) != 3 || seen["pa"] != 1 || seen["pb"] != 1 || seen["pc"] != 1 {
		t.Fatalf("Prepare saw %v, want each set once with its own view", seen)
	}
	tl, _ := st.For("pb").Tools.Get("pipeline_map")
	if _, err := tl.Run(tools.WithPipelineID(context.Background(), "pb"), json.RawMessage(`{"pipeline_id":"pb"}`)); err != nil {
		t.Fatalf("pipeline_map: %v", err)
	}
	if builders["pb"].used.Load() != 1 || builders["pa"].used.Load() != 0 {
		t.Fatal("the tools must use what Prepare set, and only for their own set")
	}
	if st.For("pb").Topology != builders["pb"] {
		t.Fatal("the set's topology must be the prepared builder")
	}
}

func TestConnFieldsMatchOwnersKeys(t *testing.T) {
	var keys []string
	for k := range connFields {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	// Relaxed for P4b-1 (LEAD-ARCH ruling on #611): wiring maps the Airflow
	// and dbt keys before owners accepts them, so for now every owners key
	// must have a connFields entry (a key owners accepts but wiring cannot
	// fill would be dropped silently). A3's owners follow-up adds the keys to
	// owners.connKeys and restores the equality check:
	//	if !slices.Equal(keys, owners.ConnectionKeys()) { t.Fatalf(...) }
	for _, k := range owners.ConnectionKeys() {
		if _, ok := connFields[k]; !ok {
			t.Fatalf("owners key %s has no connFields entry\nwiring keys %v\nowners keys %v", k, keys, owners.ConnectionKeys())
		}
	}
	// The keys wiring maps ahead of owners are exactly the batch ones.
	var ahead []string
	for _, k := range keys {
		if !slices.Contains(owners.ConnectionKeys(), k) {
			ahead = append(ahead, k)
		}
	}
	for _, k := range ahead {
		if !strings.HasPrefix(k, "airflow_") && !strings.HasPrefix(k, "dbt_") {
			t.Fatalf("connFields key %s is not an owners key and not an Airflow or dbt key", k)
		}
	}
	// Every setter reaches a distinct field.
	var c config.Config
	for i, k := range keys {
		*connFields[k](&c) = strings.Repeat("x", i+1)
	}
	seen := map[string]bool{}
	for _, k := range keys {
		v := *connFields[k](&c)
		if seen[v] {
			t.Fatalf("two keys write the same field (%s)", k)
		}
		seen[v] = true
	}
	// Every literal key has a description of what it accepts.
	for _, k := range keys {
		if owners.IsLiteralKey(k) && literalWant[k] == "" {
			t.Fatalf("literal key %s has no literalWant entry", k)
		}
	}
}

func TestStacksCloseNilSafe(t *testing.T) {
	var s *Stacks
	s.Close()
	own := loadOwners(t, twoTenantYAML(""))
	f := newFakeOpener()
	st, err := openStacks(context.Background(), config.Config{}, nil, own, StackOptions{}, f.deps(env(twoTenantEnv())))
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	st.Close()
	if len(f.closed) != 2 {
		t.Fatalf("closed %v, want each set once", f.closed)
	}
	if _, err := openStacks(context.Background(), config.Config{}, nil, nil, StackOptions{}, f.deps(env(nil))); err == nil {
		t.Fatal("nil owners: want an error")
	}
}
