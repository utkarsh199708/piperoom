package wiring

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/config"
)

// P1a-3c: a pipeline with its own connections: block names its source write
// role as connections.source_db_write_url.

const pipelineWriterDSN = "postgres://pa_writer:secret-write-a@db-a.internal:5432/orders"

// writeURLYAML is threeSetYAML with pa's connections: block naming its own
// write URL through the variable A_SOURCE_DB_WRITE_URL.
func writeURLYAML(t *testing.T) string {
	t.Helper()
	const from = "      source_db_url: {env: A_SOURCE_DB_URL}\n"
	y := threeSetYAML()
	if !strings.Contains(y, from) {
		t.Fatalf("fixture changed: no %q line", strings.TrimSpace(from))
	}
	return strings.Replace(y, from, from+"      source_db_write_url: {env: A_SOURCE_DB_WRITE_URL}\n", 1)
}

func writeURLEnv(read, write string) map[string]string {
	m := twoTenantEnv()
	m["A_SOURCE_DB_URL"] = read
	m["A_SOURCE_DB_WRITE_URL"] = write
	return m
}

// heldSourceWriters returns the fake source writers the action a holds.
func heldSourceWriters(t *testing.T, f *fakeOpener, a actions.Action) []*fakeWriter {
	t.Helper()
	var held []*fakeWriter
	walkReachable([]actions.Action{a}, func(v reflect.Value) {
		if v.Type() != reflect.TypeOf(fakeSourceWriter{}) {
			return
		}
		// Values reached through unexported fields cannot be Interface()d,
		// so the writer is matched by its pointer. FieldByName keeps this
		// right if fakeSourceWriter gains fields.
		p := v.FieldByName("fakeWriter").Pointer()
		for _, w := range f.writers {
			if reflect.ValueOf(w).Pointer() == p {
				held = append(held, w)
				return
			}
		}
		t.Errorf("%s holds a source writer the fakes did not build", a.Name())
	})
	return held
}

// TestPipelineWriteURLWriteModeOn: with write mode on, pa's set gets a source
// writer on pa's own source_db_write_url (not SOURCE_DB_WRITE_URL), its pg
// actions hold that writer, and the other sets keep theirs: pb (own
// connections, no key) gets none, the process-wide set gets the one on
// SOURCE_DB_WRITE_URL.
func TestPipelineWriteURLWriteModeOn(t *testing.T) {
	f := newFakeOpener()
	cfg := config.Config{SourceDBURL: readerDSN, SourceDBWriteURL: writerDSN, WriteMode: "on"}
	st, err := openStacks(context.Background(), cfg, nil, loadOwners(t, writeURLYAML(t)), StackOptions{},
		f.deps(env(writeURLEnv("src-a", pipelineWriterDSN))))
	if err != nil {
		t.Fatalf("openStacks: %v", err)
	}
	defer st.Close()

	pa := st.For("pa")
	own, ok := pa.Clients.writers.source.(fakeSourceWriter)
	if !ok || own.dsn != pipelineWriterDSN || own.label != "src-a" {
		t.Fatalf("pa: source writer = %#v, want one on pa's source_db_write_url", pa.Clients.writers.source)
	}
	for _, n := range []string{"pg_drop_replication_slot", "pg_add_publication_table", "pg_snapshot_signal"} {
		a, ok := pa.Actions.Get(n)
		if !ok {
			t.Fatalf("pa: %s not registered", n)
		}
		held := heldSourceWriters(t, f, a)
		if len(held) == 0 {
			t.Errorf("pa: %s holds no source writer with write mode on", n)
		}
		for _, h := range held {
			if h != own.fakeWriter {
				t.Errorf("pa: %s holds the source writer on %q, want pa's own", n, h.dsn)
			}
		}
	}
	// The Kafka actions never get a source writer (#620).
	if snap, _ := pa.Actions.Get("connect_snapshot_table"); len(heldSourceWriters(t, f, snap)) != 0 {
		t.Error("pa: connect_snapshot_table holds a source writer")
	}

	if sw := st.For("pb").Clients.writers.source; sw != nil {
		t.Errorf("pb (own connections:, no source_db_write_url) got a source writer: %#v", sw)
	}
	if sw, ok := st.For("pc").Clients.writers.source.(fakeSourceWriter); !ok || sw.dsn != writerDSN {
		t.Errorf("process-wide set: source writer = %#v, want one on SOURCE_DB_WRITE_URL", st.For("pc").Clients.writers.source)
	}
	var sources int
	for _, w := range f.writers {
		if w.kind == "source" {
			sources++
		}
	}
	if sources != 2 {
		t.Errorf("%d source writers built, want 2 (pa's and the process-wide one)", sources)
	}
}

// TestPipelineWriteURLWriteModeOff: with write mode off (or on with
// ReadOnly), the key is still resolved and checked, but no writer is built
// from it, no set's settings hold it, and no action can reach a writer.
func TestPipelineWriteURLWriteModeOff(t *testing.T) {
	for name, tc := range map[string]struct {
		mode     string
		readOnly bool
	}{
		"unset":            {mode: ""},
		"off":              {mode: "off"},
		"on, ReadOnly set": {mode: "on", readOnly: true},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeOpener()
			cfg := config.Config{SourceDBURL: readerDSN, WriteMode: tc.mode}
			st, err := openStacks(context.Background(), cfg, nil, loadOwners(t, writeURLYAML(t)), StackOptions{ReadOnly: tc.readOnly},
				f.deps(env(writeURLEnv("src-a", pipelineWriterDSN))))
			if err != nil {
				t.Fatalf("openStacks: %v", err)
			}
			defer st.Close()
			if len(f.writers) != 0 {
				t.Fatalf("write mode off built %d write clients", len(f.writers))
			}
			for _, c := range f.cfgs {
				if c.SourceDBWriteURL != "" {
					t.Errorf("set on %q was opened with a source write URL (write mode off)", c.SourceDBURL)
				}
			}
			for _, s := range st.All() {
				if s.Clients.write || s.Clients.writers != (writeClients{}) {
					t.Errorf("set %q: write=%v writers=%+v with write mode off", s.Name, s.Clients.write, s.Clients.writers)
				}
				assertNoWriterReachable(t, s.Name, s.Actions)
			}
		})
	}
}

// TestPipelineWriteURLSameRoleRefused: a source_db_write_url whose resolved
// value logs in as the pipeline's source_db_url role stops the start in every
// write mode, before anything is opened, with an error that names the
// pipeline and the keys but no value. owners.Load passes these: the two keys
// are different references.
func TestPipelineWriteURLSameRoleRefused(t *testing.T) {
	cases := map[string]string{
		"identical":              readerDSN,
		"identical but spaces":   "  " + readerDSN + "\n",
		"same role, other db":    "postgres://cdc_reader:secret-other@db.internal:5432/postgres",
		"same role, keyword DSN": "host=db.internal port=5432 user=cdc_reader password=secret-kw dbname=orders",
	}
	for name, write := range cases {
		for _, mode := range []string{"", "off", "on"} {
			t.Run(name+"/mode="+mode, func(t *testing.T) {
				f := newFakeOpener()
				_, err := openStacks(context.Background(), config.Config{SourceDBURL: "src-process", WriteMode: mode}, nil,
					loadOwners(t, writeURLYAML(t)), StackOptions{}, f.deps(env(writeURLEnv(readerDSN, write))))
				if !errors.Is(err, errSamePipelineSourceRole) {
					t.Fatalf("openStacks = %v, want errSamePipelineSourceRole", err)
				}
				for _, w := range []string{`pipeline "pa"`, "connections.source_db_write_url", "connections.source_db_url"} {
					if !strings.Contains(err.Error(), w) {
						t.Errorf("error %q does not name %q", err, w)
					}
				}
				for _, leak := range []string{"secret", "cdc_reader", "db.internal", "postgres://", "host="} {
					if strings.Contains(err.Error(), leak) {
						t.Errorf("error leaks %q: %v", leak, err)
					}
				}
				if len(f.cfgs) != 0 || len(f.writers) != 0 {
					t.Errorf("opened %d sets, %d writers before the refusal", len(f.cfgs), len(f.writers))
				}
			})
		}
	}
	// A write role of its own on the same server is accepted.
	f := newFakeOpener()
	st, err := openStacks(context.Background(), config.Config{SourceDBURL: "src-process", WriteMode: "on"}, nil,
		loadOwners(t, writeURLYAML(t)), StackOptions{}, f.deps(env(writeURLEnv(readerDSN, writerDSN))))
	if err != nil {
		t.Fatalf("own write role refused: %v", err)
	}
	st.Close()
}

// TestPipelineWriteURLMissingVariable: a source_db_write_url reference that
// does not resolve stops the start (in either mode) and names the variable.
func TestPipelineWriteURLMissingVariable(t *testing.T) {
	for _, mode := range []string{"off", "on"} {
		m := writeURLEnv("src-a", pipelineWriterDSN)
		delete(m, "A_SOURCE_DB_WRITE_URL")
		f := newFakeOpener()
		_, err := openStacks(context.Background(), config.Config{WriteMode: mode}, nil, loadOwners(t, writeURLYAML(t)), StackOptions{}, f.deps(env(m)))
		if !errors.Is(err, errConnRef) || !strings.Contains(err.Error(), "connections.source_db_write_url") || !strings.Contains(err.Error(), "A_SOURCE_DB_WRITE_URL") {
			t.Errorf("mode %s: openStacks = %v, want errConnRef naming the key and the variable", mode, err)
		}
		if len(f.cfgs) != 0 {
			t.Errorf("mode %s: %d sets opened", mode, len(f.cfgs))
		}
	}
}
