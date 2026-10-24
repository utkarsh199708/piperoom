package wiring

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/actions/sourceactions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/forge"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// fakeWriter records one write client the fake constructors built.
type fakeWriter struct {
	kind   string // "source" or "kafka"
	label  string // the source DSN of the set it was built for
	dsn    string // what the source constructor was given
	closed atomic.Bool
	writes atomic.Int64 // write calls that reached it
}

func (w *fakeWriter) Close() { w.closed.Store(true) }

type fakeSourceWriter struct{ *fakeWriter }

func (w fakeSourceWriter) AddPublicationTables(context.Context, string, []string) error {
	w.writes.Add(1)
	return nil
}
func (fakeSourceWriter) DropReplicationSlot(context.Context, string) error { return nil }
func (fakeSourceWriter) InsertSnapshotSignal(context.Context, string, string, []string) (string, error) {
	return "", nil
}

// The Q2b admin methods (postgres.SourceAdminWriter): the real source writer
// has them, so the fake does too and wiring hands it to the Q2 actions.
func (w fakeSourceWriter) SetReplicaIdentity(context.Context, string, string, string) error {
	w.writes.Add(1)
	return nil
}
func (w fakeSourceWriter) GrantSelect(context.Context, string, []string) error {
	w.writes.Add(1)
	return nil
}
func (w fakeSourceWriter) CreateHeartbeatTable(context.Context, string) error {
	w.writes.Add(1)
	return nil
}
func (w fakeSourceWriter) EndSession(context.Context, int64, time.Time) error {
	w.writes.Add(1)
	return nil
}

type fakeKafkaWriter struct{ *fakeWriter }

func (fakeKafkaWriter) ResetGroupOffsets(context.Context, string, clients.OffsetTarget) (int, error) {
	return 0, nil
}
func (fakeKafkaWriter) Produce(context.Context, string, []clients.Record) (int, error) { return 0, nil }

// fakeWriterOpeners build fake writers for the set labelled label and append
// each to *built (not locked: fakeOpener calls it under its own mutex).
func fakeWriterOpeners(label string, built *[]*fakeWriter) writerOpeners {
	return writerOpeners{
		source: func(_ context.Context, dsn string) (closingSourceWriter, error) {
			w := &fakeWriter{kind: "source", label: label, dsn: dsn}
			*built = append(*built, w)
			return fakeSourceWriter{w}, nil
		},
		kafka: func([]string, netsec.Kafka) (closingKafkaWriter, error) {
			w := &fakeWriter{kind: "kafka", label: label}
			*built = append(*built, w)
			return fakeKafkaWriter{w}, nil
		},
	}
}

// threeSetYAML is twoTenantYAML plus pipeline pc, which has no connections:
// block and so uses the process-wide set.
func threeSetYAML() string {
	return strings.Replace(twoTenantYAML(""), "tenants:\n  a: {}\n  b: {}\n", "tenants:\n  a: {}\n  b: {}\n  c: {}\n", 1) +
		"  - {id: pc, tenant: c, topic_prefix: pc, source_tables: [public.c], connectors: [pc-connector]}\n"
}

const (
	readerDSN = "postgres://cdc_reader:secret-read@db.internal:5432/orders"
	writerDSN = "postgres://piperoom_writer:secret-write@db.internal:5432/orders"
)

// reachableTypes returns the dynamic type of every value that code in an
// actions package can reach from the registered actions without reflect or
// unsafe: every field of a type of its own (unexported ones too), the
// exported fields of any other type, and what pointers, interfaces, slices
// and maps hold. It never goes through an unexported field of a type from
// another package: that is how this package's read-only adapters keep the
// whole client out of the actions' reach. Func values are opaque.
func reachableTypes(areg *actions.Registry) map[reflect.Type]bool {
	seen := map[reflect.Type]bool{}
	walkReachable(areg.All(), func(v reflect.Value) { seen[v.Type()] = true })
	return seen
}

// walkReachable calls visit on every value reachable from roots by the rules
// of reachableTypes.
func walkReachable(roots []actions.Action, visit func(reflect.Value)) {
	const actionsPkg = "piperoom/internal/actions"
	ptrs := map[uintptr]bool{}
	var walk func(v reflect.Value, depth int)
	walk = func(v reflect.Value, depth int) {
		if !v.IsValid() || depth > 32 {
			return
		}
		if v.Kind() == reflect.Interface {
			if !v.IsNil() {
				walk(v.Elem(), depth+1)
			}
			return
		}
		visit(v)
		switch v.Kind() {
		case reflect.Pointer:
			if v.IsNil() || ptrs[v.Pointer()] {
				return
			}
			ptrs[v.Pointer()] = true
			walk(v.Elem(), depth+1)
		case reflect.Struct:
			own := strings.HasPrefix(v.Type().PkgPath(), actionsPkg)
			for i := range v.NumField() {
				if own || v.Type().Field(i).IsExported() {
					walk(v.Field(i), depth+1)
				}
			}
		case reflect.Slice, reflect.Array:
			for i := range v.Len() {
				walk(v.Index(i), depth+1)
			}
		case reflect.Map:
			it := v.MapRange()
			for it.Next() {
				walk(it.Value(), depth+1)
			}
		}
	}
	for _, a := range roots {
		walk(reflect.ValueOf(a), 0)
	}
}

func ifaceOf[T any]() reflect.Type { return reflect.TypeOf((*T)(nil)).Elem() }

// refusingAdapters are the only types reachable from the actions with write
// mode off that carry a write method of the contract: each one refuses.
var refusingAdapters = map[reflect.Type]bool{
	reflect.TypeOf(readOnlyConnect{}):              true,
	reflect.TypeOf(readOnlyConnectDryRun{}):        true,
	reflect.TypeOf(readOnlyConnectValidate{}):      true,
	reflect.TypeOf(readOnlyConnectValidateTasks{}): true,
	// readConnect's values with the Q-wave reads, which the Q5 lifecycle
	// actions hold with write mode off (Q5d).
	reflect.TypeOf(readOnlyConnectQ{}):              true,
	reflect.TypeOf(readOnlyConnectTasksQ{}):         true,
	reflect.TypeOf(readOnlyConnectValidateQ{}):      true,
	reflect.TypeOf(readOnlyConnectValidateTasksQ{}): true,
	reflect.TypeOf(readOnlyRegistry{}):              true,
	reflect.TypeOf(readOnlyRegistryDryRun{}):        true,
	reflect.TypeOf(noRegistry{}):                    true,
}

// readOnlyKafkaViews are the only types reachable from the actions with write
// mode off that have the record or offset-plan reads of the Kafka writer:
// kafka.Planner and kafka.ReplayReader hold a Writer over the reader client
// in an unexported field and expose only its reads (P3a), so the Kafka
// actions validate and preview without a writer.
var readOnlyKafkaViews = map[reflect.Type]bool{
	reflect.TypeOf(&kafka.Planner{}):      true,
	reflect.TypeOf(&kafka.ReplayReader{}): true,
}

// adminWriterIfaces are the Q2 source admin writes (Q2b, Q2e): the whole
// postgres.SourceAdminWriter and each one-method interface a sourceactions
// action holds, so a type with any one admin method is caught.
var adminWriterIfaces = []reflect.Type{
	ifaceOf[postgres.SourceAdminWriter](),
	ifaceOf[sourceactions.ReplicaIdentityWriter](),
	ifaceOf[sourceactions.HeartbeatWriter](),
	ifaceOf[sourceactions.GrantWriter](),
	ifaceOf[sourceactions.SessionEnder](),
}

// assertNoWriterReachable fails when any value the actions of areg can reach
// could write: a P3 writer (SourceWriter, KafkaWriter, ConnectFixWriter, or
// the record and offset-plan reads that only the Kafka writer has, outside
// the read-only views above), a Q2 source admin writer (adminWriterIfaces),
// a type with the Connect resume or one-key
// config write the actions assert for, or a ConnectWriter /
// SchemaRegistryWriter other than the refusing adapters.
func assertNoWriterReachable(t *testing.T, name string, areg *actions.Registry) {
	t.Helper()
	writers := append([]reflect.Type{ifaceOf[clients.SourceWriter](), ifaceOf[clients.KafkaWriter](), ifaceOf[clients.ConnectFixWriter](), ifaceOf[connect.ConnectLifecycleWriter](), ifaceOf[forge.ForgeWriter]()}, adminWriterIfaces...)
	writerReads := []reflect.Type{ifaceOf[clients.RecordReader](), ifaceOf[clients.GroupResetPlanner]()}
	connW, regW := ifaceOf[clients.ConnectWriter](), ifaceOf[clients.SchemaRegistryWriter]()
	adapters := 0
	for ty := range reachableTypes(areg) {
		// A refusing adapter is exempt from the writer-interface checks
		// below, so its own method set must carry nothing but known reads
		// and refusing writes (a smuggled forwarding write hides there).
		if refusingAdapters[ty] {
			assertAdapterMethodSet(t, name, ty)
		}
		never := writers
		if !readOnlyKafkaViews[ty] {
			never = append(slices.Clone(writers), writerReads...)
		}
		for _, w := range never {
			if ty.Implements(w) {
				t.Errorf("%s: an action can reach %s, which implements %s", name, ty, w)
			}
		}
		// Resume and UpdateConfig are the Connect writes the actions assert
		// for; the rest are the Q5 lifecycle writes (Q5d), any one of which
		// is enough.
		for _, m := range []string{"Resume", "UpdateConfig", "StopConnector", "ResetSourceOffsets", "AlterSourceOffsets", "CreateConnector", "DeleteConnector", "OpenPullRequest"} {
			if _, ok := ty.MethodByName(m); ok {
				t.Errorf("%s: an action can reach %s, which has the write method %s", name, ty, m)
			}
		}
		if ty.Implements(connW) || ty.Implements(regW) {
			if !refusingAdapters[ty] {
				t.Errorf("%s: an action can reach %s, which can write", name, ty)
				continue
			}
			adapters++
		}
	}
	if len(areg.All()) > 0 && adapters == 0 {
		t.Errorf("%s: no read-only adapter reached; the walk is not seeing the actions' clients", name)
	}
}

// TestWriteModeOffNoWriterReachableFromActions: with PIPEROOM_WRITE_MODE unset
// or off (or on, but StackOptions.ReadOnly), no write client is built and no
// value the actions can reach implements a write interface, except the
// adapters that refuse. Every action still validates (the agent proposes),
// and every Execute fails without touching a client.
func TestWriteModeOffNoWriterReachableFromActions(t *testing.T) {
	for name, tc := range map[string]struct {
		mode     string
		readOnly bool
	}{
		"unset":            {mode: ""},
		"off":              {mode: "off"},
		"spaces":           {mode: " off "},
		"on, ReadOnly set": {mode: "on", readOnly: true},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeOpener()
			cfg := config.Config{SourceDBURL: readerDSN, SourceDBWriteURL: writerDSN, WriteMode: tc.mode}
			st, err := openStacks(context.Background(), cfg, nil, loadOwners(t, threeSetYAML()), StackOptions{ReadOnly: tc.readOnly}, f.deps(env(twoTenantEnv())))
			if err != nil {
				t.Fatalf("openStacks: %v", err)
			}
			defer st.Close()
			if len(st.All()) != 3 {
				t.Fatalf("%d sets, want 3 (process-wide, pa, pb)", len(st.All()))
			}
			if len(f.writers) != 0 {
				t.Fatalf("write mode off built %d write clients", len(f.writers))
			}
			for _, s := range append(st.All(), st.For("nope")) {
				if s.Clients != nil && (s.Clients.write || s.Clients.writers != (writeClients{})) {
					t.Errorf("set %q: write=%v writers=%+v with write mode off", s.Name, s.Clients.write, s.Clients.writers)
				}
				assertNoWriterReachable(t, s.Name, s.Actions)
			}

			// The proposal path still works: Validate reads through the adapters.
			pa := st.For("pa")
			restart, ok := pa.Actions.Get("connect_restart_connector")
			if !ok {
				t.Fatal("no connect_restart_connector: the agent must still be able to propose it")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := restart.Validate(ctx, json.RawMessage(`{"connector":"pa-connector"}`)); err != nil {
				t.Fatalf("Validate with write mode off: %v", err)
			}

			// Execute: every action fails, nothing reaches a write method.
			params := json.RawMessage(`{"connector":"pa-connector","task":0,"subject":"pa.public.a-value","level":"NONE","schema":"\"string\"","key":"decimal.handling.mode","value":"precise"}`)
			for _, a := range pa.Actions.All() {
				_, err := a.Execute(ctx, params)
				if err == nil {
					t.Errorf("%s executed with write mode off", a.Name())
				}
				switch a.Name() {
				case "connect_restart_task", "connect_restart_connector", "schema_set_compatibility", "schema_register":
					if !errors.Is(err, ErrReadOnly) {
						t.Errorf("%s: err = %v, want ErrReadOnly", a.Name(), err)
					}
				}
			}
			sp := f.spies["src-a"]
			for _, m := range []string{"RestartTask", "RestartConnector", "SetCompatibility", "Register"} {
				if sp.saw(m) {
					t.Errorf("write method %s reached the client with write mode off", m)
				}
			}

			// The P3 actions are listed in every set, and each refuses at
			// Execute for want of a writer, with params it accepts, in the
			// room's scope (P3-W).
			for _, s := range st.All() {
				for _, n := range p3ActionNames {
					if _, ok := s.Actions.Get(n); !ok {
						t.Errorf("set %q: %s not registered with write mode off", s.Name, n)
					}
				}
			}
			pipe, _ := loadOwners(t, threeSetYAML()).Pipeline("pa")
			assertP3ExecuteRefuses(t, pa.Actions, tools.WithPipeline(ctx, pipe), p3Params("pa"))

			// The Q2 source admin actions too (Q2e): listed in every set, and
			// each refuses at Execute (not Validate, P0 ruling #606) for want
			// of the admin writer, which write mode off never builds.
			for _, s := range st.All() {
				for _, n := range sourceActionNames {
					if _, ok := s.Actions.Get(n); !ok {
						t.Errorf("set %q: %s not registered with write mode off", s.Name, n)
					}
				}
			}
			assertSourceActionsRefuse(t, pa.Actions, tools.WithPipeline(ctx, pipe), sourceActionParams("pa"))

			// The Q5 lifecycle actions too (Q5d, P0 ruling #606): listed in
			// every set so the agent can propose a stop, an offsets reset, a
			// create or a delete, and each refuses at Execute for want of a
			// lifecycle writer (none is reachable, assertNoWriterReachable
			// above) before any Connect call.
			for _, s := range st.All() {
				for _, n := range lifecycleActionNames {
					if _, ok := s.Actions.Get(n); !ok {
						t.Errorf("set %q: %s not registered with write mode off", s.Name, n)
					}
				}
			}
			assertLifecycleExecuteRefuses(t, pa.Actions, tools.WithPipeline(ctx, pipe), "pa-connector")
			for _, m := range []string{"StopConnector", "ResetSourceOffsets", "AlterSourceOffsets", "CreateConnector", "DeleteConnector"} {
				if sp.saw(m) {
					t.Errorf("lifecycle write %s reached the client with write mode off", m)
				}
			}
		})
	}
}

// TestWriteModeOnBuildsWriters: with PIPEROOM_WRITE_MODE=on the actions get
// the whole clients, every set gets a Kafka writer over its own settings, and
// only the process-wide set gets a source writer, from SOURCE_DB_WRITE_URL: the
// fixture's pipelines with their own connections: block set no
// source_db_write_url, and never get the process-wide one (writeurl_test.go
// covers the key). The P3 rows hand the writers to their actions; Close closes
// them.
func TestWriteModeOnBuildsWriters(t *testing.T) {
	f := newFakeOpener()
	cfg := config.Config{SourceDBURL: readerDSN, SourceDBWriteURL: writerDSN, WriteMode: "on"}
	st, err := openStacks(context.Background(), cfg, nil, loadOwners(t, threeSetYAML()), StackOptions{}, f.deps(env(twoTenantEnv())))
	if err != nil {
		t.Fatalf("openStacks: %v", err)
	}
	for _, s := range st.All() {
		w := s.Clients
		if !w.write || w.writers.kafka == nil {
			t.Errorf("set %q: write=%v kafka writer=%v, want both", s.Name, w.write, w.writers.kafka)
		}
		switch s.Name {
		case ProcessWideStack:
			sw, ok := w.writers.source.(fakeSourceWriter)
			if !ok || sw.dsn != writerDSN {
				t.Errorf("process-wide set: source writer = %#v, want one on SOURCE_DB_WRITE_URL", w.writers.source)
			}
		default:
			if w.writers.source != nil {
				t.Errorf("set %q (own connections:) got a source writer", s.Name)
			}
		}
		// The actions hold the whole client: a write reaches it.
		if !reachableTypes(s.Actions)[reflect.TypeOf(&spy{})] {
			t.Errorf("set %q: the actions do not hold the whole client with write mode on", s.Name)
		}
	}
	// Each set's P3 Kafka actions hold that set's own Kafka writer, never
	// another set's: a reset or replay approved for pipeline pa must not run
	// on tenant b's brokers.
	for _, s := range st.All() {
		own, ok := s.Clients.writers.kafka.(fakeKafkaWriter)
		if !ok {
			t.Fatalf("set %q: kafka writer = %#v, want a fake", s.Name, s.Clients.writers.kafka)
		}
		if want := s.Clients.Kafka.(*spy).label; own.label != want {
			t.Errorf("set %q: its kafka writer was built for %q, want %q", s.Name, own.label, want)
		}
		for _, name := range []string{"kafka_reset_consumer_offsets", "dlq_replay", "connect_snapshot_table"} {
			a, ok := s.Actions.Get(name)
			if !ok {
				t.Errorf("set %q: %s not registered", s.Name, name)
				continue
			}
			// Values reached through an action's unexported fields cannot be
			// Interface()d, so match the writer by its pointer.
			var held []*fakeWriter
			walkReachable([]actions.Action{a}, func(v reflect.Value) {
				if v.Type() != reflect.TypeOf(fakeKafkaWriter{}) {
					return
				}
				p := v.Field(0).Pointer()
				for _, w := range f.writers {
					if reflect.ValueOf(w).Pointer() == p {
						held = append(held, w)
						return
					}
				}
				t.Errorf("set %q: %s holds a Kafka writer the fakes did not build", s.Name, name)
			})
			if len(held) == 0 {
				t.Errorf("set %q: %s holds no Kafka writer with write mode on", s.Name, name)
			}
			for _, h := range held {
				if h != own.fakeWriter {
					t.Errorf("set %q: %s holds the Kafka writer of %q, want its own (%q)", s.Name, name, h.label, own.label)
				}
			}
		}
	}
	act, _ := st.For("pa").Actions.Get("connect_restart_connector")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := act.Execute(ctx, json.RawMessage(`{"connector":"pa-connector"}`)); err != nil || !f.spies["src-a"].saw("RestartConnector") {
		t.Fatalf("restart with write mode on: err=%v, reached the client=%v", err, f.spies["src-a"].saw("RestartConnector"))
	}
	if got := len(f.writers); got != 4 { // three Kafka writers, one source writer
		t.Fatalf("%d write clients built, want 4", got)
	}
	st.Close()
	for _, w := range f.writers {
		if !w.closed.Load() {
			t.Errorf("%s writer of %s not closed by Close", w.kind, w.label)
		}
	}
}

// TestWriteModeBadValueStopsStart: anything but off or on stops the start
// before a client is opened, in OpenStacks and in Open.
func TestWriteModeBadValueStopsStart(t *testing.T) {
	for _, v := range []string{"yes", "true", "1", "ON", "of"} {
		f := newFakeOpener()
		_, err := openStacks(context.Background(), config.Config{SourceDBURL: "src-process", WriteMode: v}, nil, loadOwners(t, threeSetYAML()), StackOptions{}, f.deps(env(twoTenantEnv())))
		if err == nil || !strings.Contains(err.Error(), "PIPEROOM_WRITE_MODE") {
			t.Errorf("openStacks(WriteMode=%q) = %v, want an error naming PIPEROOM_WRITE_MODE", v, err)
		}
		if len(f.cfgs) != 0 {
			t.Errorf("WriteMode=%q: %d sets opened before the refusal", v, len(f.cfgs))
		}
		// ReadOnly does not excuse a typo.
		if _, err := openStacks(context.Background(), config.Config{WriteMode: v}, nil, loadOwners(t, threeSetYAML()), StackOptions{ReadOnly: true}, f.deps(env(twoTenantEnv()))); err == nil {
			t.Errorf("openStacks(WriteMode=%q, ReadOnly) = nil, want an error", v)
		}
	}
	// Open checks the mode first: the owners file here does not exist.
	_, err := Open(context.Background(), config.Config{WriteMode: "true", OwnersFile: filepath.Join(t.TempDir(), "missing.yaml")}, nil)
	if err == nil || !strings.Contains(err.Error(), "PIPEROOM_WRITE_MODE") {
		t.Fatalf("Open(WriteMode=true) = %v, want an error naming PIPEROOM_WRITE_MODE", err)
	}
}

func TestCheckSourceWriteURL(t *testing.T) {
	cases := []struct {
		name, read, write string
		refused           bool
	}{
		{"no write url", readerDSN, "", false},
		{"blank write url", readerDSN, "  ", false},
		{"identical", readerDSN, readerDSN, true},
		{"identical but spaces", readerDSN, " " + readerDSN + "\n", true},
		{"same role, other database and password", readerDSN, "postgres://cdc_reader:other@db.internal:5432/postgres?sslmode=disable", true},
		{"same role, keyword form", readerDSN, "host=db.internal port=5432 user=cdc_reader dbname=orders", true},
		{"same role, default port", readerDSN, "postgres://cdc_reader@db.internal/orders", true},
		{"own write role", readerDSN, writerDSN, false},
		{"other host", readerDSN, "postgres://cdc_reader:secret-read@primary.internal:5432/orders", false},
		{"other port", readerDSN, "postgres://cdc_reader:secret-read@db.internal:6432/orders", false},
		{"write url does not parse", readerDSN, "postgres://%zz", false},
		{"read url does not parse, write differs", "postgres://%zz", writerDSN, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkSourceWriteURL(config.Config{SourceDBURL: tc.read, SourceDBWriteURL: tc.write})
			if got := errors.Is(err, errSameSourceRole); got != tc.refused {
				t.Fatalf("checkSourceWriteURL = %v, refused=%v, want %v", err, got, tc.refused)
			}
			if err != nil {
				for _, leak := range []string{"secret", "cdc_reader", "db.internal", "postgres://"} {
					if strings.Contains(err.Error(), leak) {
						t.Errorf("error leaks %q: %v", leak, err)
					}
				}
			}
		})
	}
}

// TestSameWriteURLStopsStart: SOURCE_DB_WRITE_URL equal to SOURCE_DB_URL stops
// OpenStacks (and Open) before anything is opened, whatever the write mode:
// the mistake must not wait until someone turns write mode on.
func TestSameWriteURLStopsStart(t *testing.T) {
	for _, mode := range []string{"", "off", "on"} {
		f := newFakeOpener()
		cfg := config.Config{SourceDBURL: readerDSN, SourceDBWriteURL: readerDSN, WriteMode: mode}
		_, err := openStacks(context.Background(), cfg, nil, loadOwners(t, threeSetYAML()), StackOptions{}, f.deps(env(twoTenantEnv())))
		if !errors.Is(err, errSameSourceRole) {
			t.Errorf("mode %q: openStacks = %v, want errSameSourceRole", mode, err)
		}
		if err != nil && strings.Contains(err.Error(), "secret-read") {
			t.Errorf("mode %q: error leaks the password: %v", mode, err)
		}
		if len(f.cfgs) != 0 || len(f.writers) != 0 {
			t.Errorf("mode %q: opened %d sets, %d writers before the refusal", mode, len(f.cfgs), len(f.writers))
		}
	}
	_, err := Open(context.Background(), config.Config{SourceDBURL: readerDSN, SourceDBWriteURL: readerDSN, WriteMode: "on"}, nil)
	if !errors.Is(err, errSameSourceRole) {
		t.Fatalf("Open = %v, want errSameSourceRole", err)
	}
}

func TestOpenWriters(t *testing.T) {
	ctx := context.Background()
	errBoom := errors.New("boom")
	type built struct{ source, kafka int }
	count := func(ws []*fakeWriter) built {
		var b built
		for _, w := range ws {
			if w.kind == "source" {
				b.source++
			} else {
				b.kafka++
			}
		}
		return b
	}

	t.Run("kafka only without SOURCE_DB_WRITE_URL", func(t *testing.T) {
		var ws []*fakeWriter
		wc, closeAll, err := openWriters(ctx, config.Config{SourceDBURL: readerDSN, KafkaBrokers: "k:9092"}, nil, fakeWriterOpeners("x", &ws))
		if err != nil || wc.kafka == nil || wc.source != nil || count(ws) != (built{kafka: 1}) {
			t.Fatalf("openWriters = %+v, %v; built %+v", wc, err, count(ws))
		}
		closeAll()
		if !ws[0].closed.Load() {
			t.Fatal("closer left the Kafka writer open")
		}
	})

	t.Run("both with SOURCE_DB_WRITE_URL", func(t *testing.T) {
		var ws []*fakeWriter
		wc, closeAll, err := openWriters(ctx, config.Config{SourceDBURL: readerDSN, SourceDBWriteURL: writerDSN}, nil, fakeWriterOpeners("x", &ws))
		if err != nil || wc.kafka == nil || wc.source == nil {
			t.Fatalf("openWriters = %+v, %v", wc, err)
		}
		if sw := wc.source.(fakeSourceWriter); sw.dsn != writerDSN {
			t.Fatal("the source writer was not opened on SOURCE_DB_WRITE_URL")
		}
		closeAll()
		for _, w := range ws {
			if !w.closed.Load() {
				t.Errorf("closer left the %s writer open", w.kind)
			}
		}
	})

	t.Run("source writer fails: Kafka writer closed", func(t *testing.T) {
		var ws []*fakeWriter
		op := fakeWriterOpeners("x", &ws)
		op.source = func(context.Context, string) (closingSourceWriter, error) { return nil, errBoom }
		_, closeAll, err := openWriters(ctx, config.Config{SourceDBURL: readerDSN, SourceDBWriteURL: writerDSN}, nil, op)
		if !errors.Is(err, errBoom) || closeAll != nil || !strings.Contains(err.Error(), "source postgres writer") {
			t.Fatalf("openWriters = %v (closer %v), want the wrapped source error", err, closeAll != nil)
		}
		if len(ws) != 1 || !ws[0].closed.Load() {
			t.Fatal("the Kafka writer stayed open after the source writer failed")
		}
	})

	t.Run("kafka writer fails: no source writer opened", func(t *testing.T) {
		var ws []*fakeWriter
		op := fakeWriterOpeners("x", &ws)
		op.kafka = func([]string, netsec.Kafka) (closingKafkaWriter, error) { return nil, errBoom }
		if _, _, err := openWriters(ctx, config.Config{SourceDBURL: readerDSN, SourceDBWriteURL: writerDSN}, nil, op); !errors.Is(err, errBoom) || !strings.Contains(err.Error(), "kafka writer") {
			t.Fatalf("openWriters = %v, want the wrapped kafka error", err)
		}
		if len(ws) != 0 {
			t.Fatalf("%d writers built after the Kafka writer failed", len(ws))
		}
	})

	t.Run("same role refused before any writer", func(t *testing.T) {
		var ws []*fakeWriter
		if _, _, err := openWriters(ctx, config.Config{SourceDBURL: readerDSN, SourceDBWriteURL: readerDSN}, nil, fakeWriterOpeners("x", &ws)); !errors.Is(err, errSameSourceRole) {
			t.Fatalf("openWriters = %v, want errSameSourceRole", err)
		}
		if len(ws) != 0 {
			t.Fatalf("%d writers built", len(ws))
		}
	})

	t.Run("MSK IAM without the shared AWS config", func(t *testing.T) {
		var ws []*fakeWriter
		cfg := config.Config{KafkaSecurityProtocol: netsec.SASLSSL, KafkaSASLMechanism: netsec.MechAWSMSKIAM}
		if _, _, err := openWriters(ctx, cfg, nil, fakeWriterOpeners("x", &ws)); !errors.Is(err, errNoSharedAWS) {
			t.Fatalf("openWriters = %v, want errNoSharedAWS", err)
		}
		if len(ws) != 0 {
			t.Fatalf("%d writers built", len(ws))
		}
	})
}

// dryRunSpy is a writeSpy whose Connect and Registry also have the two
// dry-run endpoints, as the real clients do.
type dryRunSpy struct {
	writeSpy
	validated, checked int
}

func (s *dryRunSpy) Config(context.Context, string) (map[string]string, error) {
	return map[string]string{"connector.class": "io.example.Sink", "decimal.handling.mode": "string"}, nil
}
func (s *dryRunSpy) ValidateConfig(context.Context, string, map[string]string) (connect.ValidateResult, error) {
	s.validated++
	return connect.ValidateResult{}, nil
}
func (s *dryRunSpy) CheckCompatibility(context.Context, string, string) (bool, []string, error) {
	s.checked++
	return true, nil, nil
}

// TestWriteModeOffKeepsDryRuns: with write mode off the proposal preview keeps
// the dry-run verdicts (Connect config validate, Registry compatibility check)
// through package actions' own type assertions, and a client without them
// still reads "client has no dry-run". A Wired built by hand is read-only.
func TestWriteModeOffKeepsDryRuns(t *testing.T) {
	preview := func(t *testing.T, areg *actions.Registry, name, params string) *actions.DryRunResult {
		t.Helper()
		a, ok := areg.Get(name)
		if !ok {
			t.Fatalf("no action %s", name)
		}
		p, ok := a.(actions.Preview)
		if !ok {
			t.Fatalf("%s has no Preview", name)
		}
		res, err := p.Preview(context.Background(), json.RawMessage(params))
		if err != nil {
			t.Fatalf("%s Preview: %v", name, err)
		}
		return res.DryRun
	}
	const (
		cfgParams    = `{"connector":"c","key":"decimal.handling.mode","value":"precise"}`
		schemaParams = `{"subject":"s","schema":"\"string\""}`
	)
	register := func(t *testing.T, c clients.ConnectClient, r clients.SchemaRegistryClient) *actions.Registry {
		t.Helper()
		areg := actions.NewRegistry()
		if err := RegisterTools(tools.NewReadOnlyRegistry(), areg, &Wired{Connect: c, Registry: r, Owners: &owners.Owners{}}); err != nil {
			t.Fatalf("RegisterTools: %v", err)
		}
		assertNoWriterReachable(t, "hand-built Wired", areg)
		return areg
	}

	sp := &dryRunSpy{}
	areg := register(t, sp, sp)
	if dr := preview(t, areg, "connect_update_connector_config", cfgParams); dr == nil || dr.Skipped != "" || !dr.OK || sp.validated != 1 {
		t.Errorf("config validate dry-run = %+v (calls %d), want it run through the read-only client", dr, sp.validated)
	}
	if dr := preview(t, areg, "schema_register", schemaParams); dr == nil || dr.Skipped != "" || !dr.OK || sp.checked != 1 {
		t.Errorf("compatibility dry-run = %+v (calls %d), want it run through the read-only client", dr, sp.checked)
	}
	if len(sp.writes) != 0 {
		t.Fatalf("a preview wrote: %v", sp.writes)
	}

	plain := &writeSpy{}
	areg = register(t, plain, plain)
	for name, params := range map[string]string{"connect_update_connector_config": cfgParams, "schema_register": schemaParams} {
		if dr := preview(t, areg, name, params); dr == nil || dr.Skipped == "" {
			t.Errorf("%s dry-run = %+v, want skipped: the client has none", name, dr)
		}
	}
}
