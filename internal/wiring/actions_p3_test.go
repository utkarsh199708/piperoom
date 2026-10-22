package wiring

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// p3ActionNames are the P3 write actions RegisterP3Actions adds (P3a, P3b),
// in its registration order.
var p3ActionNames = []string{
	"kafka_reset_consumer_offsets", "dlq_replay", "kafka_replay_range", "connect_snapshot_table",
	"pg_drop_replication_slot", "pg_add_publication_table", "pg_snapshot_signal",
}

// p3Params are params each P3 action accepts for pipeline id of
// twoTenantYAML (connector <id>-connector, table public.a, group <id>-sync,
// DLQ <id>-dlq), so Execute gets past parsing and scope to its writer check.
func p3Params(id string) map[string]json.RawMessage {
	return map[string]json.RawMessage{
		"kafka_reset_consumer_offsets": json.RawMessage(`{"group":"` + id + `-sync","topics":["` + id + `.public.a"],"to":"earliest"}`),
		"dlq_replay":                   json.RawMessage(`{"dlq_topic":"` + id + `-dlq","to_topic":"` + id + `.public.a","count":1}`),
		"kafka_replay_range":           json.RawMessage(`{"topic":"` + id + `.public.a","to_topic":"` + id + `-reprocess","ranges":[{"partition":0,"from_offset":0,"to_offset":0}]}`),
		"connect_snapshot_table":       json.RawMessage(`{"connector":"` + id + `-connector","tables":["public.a"],"via":"signal_topic"}`),
		"pg_drop_replication_slot":     json.RawMessage(`{"connector":"` + id + `-connector","slot":"` + id + `_slot"}`),
		"pg_add_publication_table":     json.RawMessage(`{"connector":"` + id + `-connector","publication":"` + id + `_pub","table":"public.a"}`),
		"pg_snapshot_signal":           json.RawMessage(`{"connector":"` + id + `-connector","tables":["public.a"],"signal_table":"public.debezium_signal"}`),
	}
}

// assertP3ExecuteRefuses runs Execute of every P3 action of areg with params
// it accepts in ctx (the room's scope) and wants the refusal for a missing
// writer: "no write credentials" for the pg actions, "no Kafka write client"
// for the Kafka ones. A typed nil writer would get past the actions' nil
// check and fail differently, or panic.
func assertP3ExecuteRefuses(t *testing.T, areg *actions.Registry, ctx context.Context, params map[string]json.RawMessage) {
	t.Helper()
	for _, n := range p3ActionNames {
		a, ok := areg.Get(n)
		if !ok {
			t.Errorf("%s not registered", n)
			continue
		}
		want := "no Kafka write client"
		if strings.HasPrefix(n, "pg_") {
			want = "no write credentials"
		}
		_, err := a.Execute(ctx, params[n])
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s Execute with write mode off = %v, want the %q refusal", n, err, want)
		}
	}
}

// p3Connect is a whole Connect client (it has the restart writes) whose
// connector config carries every key the P3 actions read.
type p3Connect struct {
	configs atomic.Int64
	writes  atomic.Int64
}

func (c *p3Connect) ListConnectors(context.Context) ([]string, error) {
	return []string{"pa-connector"}, nil
}
func (c *p3Connect) Status(_ context.Context, name string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{Name: name, State: "RUNNING"}, nil
}
func (c *p3Connect) Config(context.Context, string) (map[string]string, error) {
	c.configs.Add(1)
	return map[string]string{
		"connector.class":         "io.debezium.connector.postgresql.PostgresConnector",
		"topic.prefix":            "pa",
		"slot.name":               "pa_slot",
		"publication.name":        "pa_pub",
		"table.include.list":      "public.a",
		"signal.data.collection":  "public.debezium_signal",
		"signal.enabled.channels": "source,kafka",
		"signal.kafka.topic":      "pa-signal",
		// Any value: connect_snapshot_table only needs it set.
		"signal.kafka.bootstrap.servers": "kafka:9092",
	}, nil
}
func (c *p3Connect) RestartTask(context.Context, string, int) error { c.writes.Add(1); return nil }
func (c *p3Connect) RestartConnector(context.Context, string) error {
	c.writes.Add(1)
	return nil
}

// p3Source is a read-only source client with the optional reads the pg
// actions assert for, counting every read.
type p3Source struct{ reads atomic.Int64 }

func (s *p3Source) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	s.reads.Add(1)
	return []clients.ReplicationSlot{{Name: "pa_slot", Plugin: "pgoutput", WALStatus: "extended"}}, nil
}
func (s *p3Source) RecentDDL(context.Context, time.Time, int) ([]clients.DDLChange, error) {
	return nil, nil
}
func (s *p3Source) LongTransactions(context.Context, time.Duration) ([]clients.Transaction, error) {
	return nil, nil
}
func (s *p3Source) Row(context.Context, string, string, string) (map[string]any, bool, error) {
	return nil, false, nil
}
func (s *p3Source) PublicationAddState(context.Context, string, string, []string) (postgres.PublicationAddState, error) {
	s.reads.Add(1)
	return postgres.PublicationAddState{TableExists: true, RelKind: "r", ReplicaIdentity: "d", HasPrimaryKey: true}, nil
}
func (s *p3Source) SnapshotSignalState(_ context.Context, _, _ string, tables []string) (postgres.SnapshotSignalState, error) {
	s.reads.Add(1)
	st := postgres.SnapshotSignalState{PublicationExists: true, SignalExists: true, SignalColumns: true, SignalPublished: true}
	for _, tb := range tables {
		st.Tables = append(st.Tables, postgres.SnapshotTableState{Table: tb, Exists: true, EstimatedRows: 10})
	}
	return st, nil
}

// p3Wired is a hand-built Wired for the cdc pipeline pa with a real Kafka
// reader client on a kfake cluster (so the actions get kafka.Planner and
// kafka.ReplayReader), the fakes above and the given write state.
func p3Wired(t *testing.T, write bool, wc writeClients) (*Wired, *p3Connect, *p3Source, owners.Pipeline) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "pa.public.a", "pa-dlq", "pa-signal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	kc, err := kafka.New(c.ListenAddrs(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kc.Close)
	o := loadOwners(t, threeSetYAML())
	pipe, ok := o.Pipeline("pa")
	if !ok {
		t.Fatal("no pipeline pa")
	}
	conn, src := &p3Connect{}, &p3Source{}
	return &Wired{Connect: conn, Source: src, Kafka: kc, Owners: o, write: write, writers: wc}, conn, src, pipe
}

func registerP3(t *testing.T, w *Wired) *actions.Registry {
	t.Helper()
	areg := actions.NewRegistry()
	if err := RegisterTools(tools.NewReadOnlyRegistry(), areg, w); err != nil {
		t.Fatalf("RegisterTools: %v", err)
	}
	return areg
}

// only returns a registry holding the one action name of areg, so the
// reachability walk sees that action's values alone.
func only(t *testing.T, areg *actions.Registry, name string) *actions.Registry {
	t.Helper()
	a, ok := areg.Get(name)
	if !ok {
		t.Fatalf("no action %s", name)
	}
	r := actions.NewRegistry()
	if err := r.Register(a); err != nil {
		t.Fatal(err)
	}
	return r
}

// TestP3ActionsWriteModeOff: with write mode off every P3 action is listed,
// Preview reads through the read-only clients (the Kafka ones through
// kafka.Planner and kafka.ReplayReader over the reader client), Execute
// refuses for want of a writer, and no value the actions reach can write.
func TestP3ActionsWriteModeOff(t *testing.T) {
	w, conn, src, pipe := p3Wired(t, false, writeClients{})
	areg := registerP3(t, w)
	assertNoWriterReachable(t, "write mode off", areg)

	views := reachableTypes(areg)
	for ty := range readOnlyKafkaViews {
		if !views[ty] {
			t.Errorf("no action reaches %s: the Kafka actions cannot preview with write mode off", ty)
		}
	}

	ctx, cancel := context.WithTimeout(tools.WithPipeline(context.Background(), pipe), 20*time.Second)
	defer cancel()
	params := p3Params("pa")
	for _, n := range p3ActionNames {
		a, ok := areg.Get(n)
		if !ok {
			t.Errorf("%s not registered", n)
			continue
		}
		p, ok := a.(actions.Preview)
		if !ok {
			t.Errorf("%s has no Preview", n)
			continue
		}
		before := conn.configs.Load() + src.reads.Load()
		pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
		res, err := p.Preview(pctx, params[n])
		pcancel()
		// A preview may refuse what the fake state shows (an empty group,
		// an empty DLQ); it must never refuse for a client wiring left out.
		if err != nil {
			for _, missing := range []string{"no Kafka client", "no source database client", "no Connect client", "no write", "no Kafka write"} {
				if strings.Contains(err.Error(), missing) {
					t.Errorf("%s Preview refused for a missing client: %v", n, err)
				}
			}
			t.Logf("%s Preview: %v", n, err)
			continue
		}
		if res.Current == "" && res.Expected == "" {
			t.Errorf("%s Preview = %+v, want what it read", n, res)
		}
		if !strings.HasPrefix(n, "kafka_") && n != "dlq_replay" && conn.configs.Load()+src.reads.Load() == before {
			t.Errorf("%s Preview read neither the Connect config nor the source", n)
		}
	}

	assertP3ExecuteRefuses(t, areg, ctx, params)
	if conn.writes.Load() != 0 {
		t.Fatalf("%d Connect writes with write mode off", conn.writes.Load())
	}
}

// TestP3ActionsWriteModeOn: with write mode on the pg actions hold the source
// writer and the Kafka actions the Kafka writer, those exact values; the
// snapshot action holds the Kafka writer and never the source writer (#620);
// and an Execute reaches the source writer it was given.
func TestP3ActionsWriteModeOn(t *testing.T) {
	sw := fakeSourceWriter{&fakeWriter{kind: "source"}}
	kw := fakeKafkaWriter{&fakeWriter{kind: "kafka"}}
	w, _, _, pipe := p3Wired(t, true, writeClients{source: sw, kafka: kw})
	areg := registerP3(t, w)

	swT, kwT := reflect.TypeOf(sw), reflect.TypeOf(kw)
	for _, n := range p3ActionNames {
		reach := reachableTypes(only(t, areg, n))
		wantSource, wantKafka := strings.HasPrefix(n, "pg_"), !strings.HasPrefix(n, "pg_")
		if reach[swT] != wantSource || reach[kwT] != wantKafka {
			t.Errorf("%s reaches source writer=%v kafka writer=%v, want %v %v", n, reach[swT], reach[kwT], wantSource, wantKafka)
		}
	}

	ctx, cancel := context.WithTimeout(tools.WithPipeline(context.Background(), pipe), 10*time.Second)
	defer cancel()
	add, _ := areg.Get("pg_add_publication_table")
	if _, err := add.Execute(ctx, p3Params("pa")["pg_add_publication_table"]); err != nil {
		t.Fatalf("pg_add_publication_table Execute with write mode on: %v", err)
	}
	if sw.writes.Load() != 1 {
		t.Fatalf("source writer saw %d writes, want 1: Execute did not call the wired writer", sw.writes.Load())
	}
}

// TestP3WriteModeOnNoSourceWriter: write mode on without SOURCE_DB_WRITE_URL
// leaves the pg actions' writer a nil interface, so they refuse at Execute
// while the Kafka actions hold the Kafka writer.
func TestP3WriteModeOnNoSourceWriter(t *testing.T) {
	kw := fakeKafkaWriter{&fakeWriter{kind: "kafka"}}
	w, _, _, pipe := p3Wired(t, true, writeClients{kafka: kw})
	areg := registerP3(t, w)
	ctx := tools.WithPipeline(context.Background(), pipe)
	for _, n := range []string{"pg_drop_replication_slot", "pg_add_publication_table", "pg_snapshot_signal"} {
		a, _ := areg.Get(n)
		if _, err := a.Execute(ctx, p3Params("pa")[n]); err == nil || !strings.Contains(err.Error(), "no write credentials") {
			t.Errorf("%s Execute without a source writer = %v, want no write credentials", n, err)
		}
	}
	if !reachableTypes(only(t, areg, "kafka_reset_consumer_offsets"))[reflect.TypeOf(kw)] {
		t.Error("kafka_reset_consumer_offsets does not hold the Kafka writer")
	}
}

// TestSnapshotTableGetsRealKafkaWriter: the Kafka writer openWriters builds
// is the real *kafka.Writer, which has SendSnapshotSignal, and that is what
// connect_snapshot_table holds, so it never refuses with ErrNoSignalWriter.
func TestSnapshotTableGetsRealKafkaWriter(t *testing.T) {
	wc, closeAll, err := openWriters(context.Background(), config.Config{KafkaBrokers: "127.0.0.1:1"}, nil, defaultWriterOpeners())
	if err != nil {
		t.Fatalf("openWriters: %v", err)
	}
	defer closeAll()
	if wc.source != nil {
		t.Fatal("a source writer without SOURCE_DB_WRITE_URL")
	}
	w, _, _, _ := p3Wired(t, true, wc)
	reach := reachableTypes(only(t, registerP3(t, w), "connect_snapshot_table"))
	wt := reflect.TypeOf(&kafka.Writer{})
	if !reach[wt] {
		t.Fatalf("connect_snapshot_table does not hold the *kafka.Writer")
	}
	if _, ok := wt.MethodByName("SendSnapshotSignal"); !ok {
		t.Fatal("*kafka.Writer has no SendSnapshotSignal")
	}
	for ty := range reach {
		if ty.Implements(ifaceOf[clients.SourceWriter]()) {
			t.Errorf("connect_snapshot_table reaches %s, a source writer", ty)
		}
	}
}

// TestP3ActionsOnlyForCDC: the P3 actions are cdc-only (agent.ActionKinds),
// so a cdc room is offered all of them and a batch room none; and a set that
// serves only batch pipelines registers no action at all.
func TestP3ActionsOnlyForCDC(t *testing.T) {
	w, _, _, _ := p3Wired(t, false, writeClients{})
	areg := registerP3(t, w)
	cdc, err := agent.ActionsForKind(areg, events.PipelineKindCDC)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range p3ActionNames {
		if _, ok := cdc.Get(n); !ok {
			t.Errorf("cdc room lacks %s", n)
		}
	}
	batch, err := agent.ActionsForKind(areg, events.PipelineKindBatch)
	if err != nil {
		t.Fatal(err)
	}
	if got := batch.All(); len(got) != 0 {
		names := make([]string, 0, len(got))
		for _, a := range got {
			names = append(names, a.Name())
		}
		t.Errorf("batch room offered %v, want none", names)
	}

	breg := actions.NewRegistry()
	if err := RegisterTools(tools.NewReadOnlyRegistry(), breg, &Wired{Owners: ownersOf(batchPipe("rev"))}); err != nil {
		t.Fatalf("RegisterTools (batch only): %v", err)
	}
	if len(breg.All()) != 0 {
		t.Errorf("a batch-only set registered %d actions", len(breg.All()))
	}
}

// TestRegisterP3Actions: the exported helper, called with the clients a
// harness passes (as evals.LiveActions will with replay clients), registers
// the six P3 actions in the order wiring's own registration lists them, each
// holding the writer of its kind; with no client at all it still registers
// all six; and a second registration into the same registry is an error.
func TestRegisterP3Actions(t *testing.T) {
	sw := fakeSourceWriter{&fakeWriter{kind: "source"}}
	kw := fakeKafkaWriter{&fakeWriter{kind: "kafka"}}
	areg := actions.NewRegistry()
	planner, records := &p3Planner{}, &p3Records{}
	if err := RegisterP3Actions(areg, P3ActionDeps{
		Connect: &p3Connect{}, Source: &p3Source{}, Planner: planner, Records: records,
		SourceWriter: sw, KafkaWriter: kw,
	}); err != nil {
		t.Fatalf("RegisterP3Actions: %v", err)
	}
	var got []string
	for _, a := range areg.All() {
		got = append(got, a.Name())
	}
	if !slices.Equal(got, p3ActionNames) {
		t.Fatalf("registered %v, want %v", got, p3ActionNames)
	}
	swT, kwT := reflect.TypeOf(sw), reflect.TypeOf(kw)
	for _, n := range p3ActionNames {
		reach := reachableTypes(only(t, areg, n))
		wantSource := strings.HasPrefix(n, "pg_")
		if reach[swT] != wantSource || reach[kwT] != !wantSource {
			t.Errorf("%s reaches source writer=%v kafka writer=%v, want %v %v", n, reach[swT], reach[kwT], wantSource, !wantSource)
		}
	}
	// The harness's Planner and Records reach the Kafka action that reads
	// through each.
	if !reachableTypes(only(t, areg, "kafka_reset_consumer_offsets"))[reflect.TypeOf(planner)] {
		t.Error("kafka_reset_consumer_offsets does not hold the Planner it was given")
	}
	if !reachableTypes(only(t, areg, "dlq_replay"))[reflect.TypeOf(records)] {
		t.Error("dlq_replay does not hold the Records reader it was given")
	}

	// Wiring goes through the helper: its P3 actions come in the same order.
	w, _, _, _ := p3Wired(t, false, writeClients{})
	var wired []string
	for _, a := range registerP3(t, w).All() {
		if slices.Contains(p3ActionNames, a.Name()) {
			wired = append(wired, a.Name())
		}
	}
	if !slices.Equal(wired, p3ActionNames) {
		t.Fatalf("wiring registered the P3 actions as %v, want %v", wired, p3ActionNames)
	}

	empty := actions.NewRegistry()
	if err := RegisterP3Actions(empty, P3ActionDeps{}); err != nil {
		t.Fatalf("RegisterP3Actions with no clients: %v", err)
	}
	if n := len(empty.All()); n != len(p3ActionNames) {
		t.Fatalf("with no clients: %d actions, want %d", n, len(p3ActionNames))
	}

	if err := RegisterP3Actions(empty, P3ActionDeps{}); err == nil {
		t.Fatal("registering the P3 actions twice into one registry: want an error")
	}
}

// p3Planner and p3Records are harness stand-ins for the Kafka read views
// (kafka.Planner, kafka.ReplayReader) a caller passes to RegisterP3Actions.
type p3Planner struct{}

func (*p3Planner) CheckGroupInactive(context.Context, string) error { return nil }
func (*p3Planner) PlanGroupReset(context.Context, string, clients.OffsetTarget) ([]clients.OffsetMove, error) {
	return nil, nil
}

type p3Records struct{}

func (*p3Records) ReadRecords(context.Context, string, int32, int64, int) ([]clients.Record, error) {
	return nil, nil
}
func (*p3Records) DLQOffsets(context.Context, ...string) (map[string][]clients.PartitionOffsets, error) {
	return nil, nil
}

// TestRegisterP3ActionsTypedNil: a caller that puts a typed nil into a
// P3ActionDeps field (a (*kafka.Writer)(nil), a (*postgres.Writer)(nil), a
// nil pointer reader) gets actions holding a nil interface there: no action
// reaches the typed nil, and Execute refuses with the usual "no write
// credentials" / "no Kafka write client" error instead of panicking.
func TestRegisterP3ActionsTypedNil(t *testing.T) {
	o := loadOwners(t, threeSetYAML())
	pipe, ok := o.Pipeline("pa")
	if !ok {
		t.Fatal("no pipeline pa")
	}
	nilKW, nilSW := reflect.TypeOf((*kafka.Writer)(nil)), reflect.TypeOf((*postgres.Writer)(nil))
	cases := []struct {
		name string
		deps P3ActionDeps
		// nils are the typed-nil types no action may reach.
		nils []reflect.Type
	}{
		{"kafka writer", P3ActionDeps{Connect: &p3Connect{}, Source: &p3Source{}, KafkaWriter: (*kafka.Writer)(nil)}, []reflect.Type{nilKW}},
		{"source writer", P3ActionDeps{Connect: &p3Connect{}, Source: &p3Source{}, SourceWriter: (*postgres.Writer)(nil)}, []reflect.Type{nilSW}},
		{"both writers", P3ActionDeps{
			Connect: &p3Connect{}, Source: &p3Source{},
			SourceWriter: (*postgres.Writer)(nil), KafkaWriter: (*kafka.Writer)(nil),
		}, []reflect.Type{nilKW, nilSW}},
		{"every field", P3ActionDeps{
			Connect: (*p3Connect)(nil), Source: (*p3Source)(nil),
			Planner: (*kafka.Planner)(nil), Records: (*kafka.ReplayReader)(nil),
			SourceWriter: (*postgres.Writer)(nil), KafkaWriter: (*kafka.Writer)(nil),
		}, []reflect.Type{
			nilKW, nilSW, reflect.TypeOf((*p3Connect)(nil)), reflect.TypeOf((*p3Source)(nil)),
			reflect.TypeOf((*kafka.Planner)(nil)), reflect.TypeOf((*kafka.ReplayReader)(nil)),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			areg := actions.NewRegistry()
			if err := RegisterP3Actions(areg, tc.deps); err != nil {
				t.Fatalf("RegisterP3Actions: %v", err)
			}
			reach := reachableTypes(areg)
			for _, ty := range tc.nils {
				if reach[ty] {
					t.Errorf("an action holds a typed nil %s; want a nil interface", ty)
				}
			}
			ctx, cancel := context.WithTimeout(tools.WithPipeline(context.Background(), pipe), 10*time.Second)
			defer cancel()
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Execute panicked: %v", r)
				}
			}()
			assertP3ExecuteRefuses(t, areg, ctx, p3Params("pa"))
		})
	}
}

func TestIsNilIface(t *testing.T) {
	var (
		nilMap   map[string]int
		nilSlice []string
		nilFunc  func()
		nilChan  chan int
	)
	cases := []struct {
		name string
		v    any
		want bool
	}{
		{"nil", nil, true},
		{"typed nil pointer", (*kafka.Writer)(nil), true},
		{"typed nil in an interface", clients.KafkaWriter((*kafka.Writer)(nil)), true},
		{"nil map", nilMap, true},
		{"nil slice", nilSlice, true},
		{"nil func", nilFunc, true},
		{"nil chan", nilChan, true},
		{"empty slice", []string{}, false},
		{"pointer", &p3Connect{}, false},
		{"struct value", fakeSourceWriter{}, false},
		{"string", "", false},
	}
	for _, tc := range cases {
		if got := isNilIface(tc.v); got != tc.want {
			t.Errorf("%s: isNilIface = %v, want %v", tc.name, got, tc.want)
		}
	}
}
