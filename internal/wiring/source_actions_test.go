package wiring

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/config"
	"piperoom/internal/tools"
)

// sourceActionNames are the Q2c/Q2d source database admin actions
// RegisterSourceActions adds, in its registration order.
var sourceActionNames = []string{
	"pg_set_replica_identity", "source_create_heartbeat_table",
	"source_grant_select", "source_end_session",
}

// sourceActionParams are params the actions accept for pipeline id of
// threeSetYAML (connector <id>-connector, source table public.<table>), so
// Execute gets past parsing and scope to its writer check.
func sourceActionParams(id, table string) map[string]json.RawMessage {
	return map[string]json.RawMessage{
		"pg_set_replica_identity":       json.RawMessage(`{"table":"public.` + table + `","mode":"full"}`),
		"source_create_heartbeat_table": json.RawMessage(`{"connector":"` + id + `-connector","table":"public.heartbeat"}`),
		"source_grant_select":           json.RawMessage(`{"connector":"` + id + `-connector","role":"debezium","tables":["public.` + table + `"]}`),
		"source_end_session":            json.RawMessage(`{"connector":"` + id + `-connector","pid":4242,"started_at":"2026-10-11T00:00:00Z"}`),
	}
}

// TestSourceActionsRegisteredInEverySet: the four source admin actions are
// listed in every CDC set in both write modes, after the P3 actions, because
// the room previews them and GET /api/actions lists them.
func TestSourceActionsRegisteredInEverySet(t *testing.T) {
	for _, mode := range []string{"", "on"} {
		f := newFakeOpener()
		cfg := config.Config{SourceDBURL: readerDSN, SourceDBWriteURL: writerDSN, WriteMode: mode}
		st, err := openStacks(context.Background(), cfg, nil, loadOwners(t, threeSetYAML()), StackOptions{}, f.deps(env(twoTenantEnv())))
		if err != nil {
			t.Fatalf("mode %q: openStacks: %v", mode, err)
		}
		for _, s := range st.All() {
			var got []string
			for _, a := range s.Actions.All() {
				if slices.Contains(sourceActionNames, a.Name()) {
					got = append(got, a.Name())
				}
			}
			if !slices.Equal(got, sourceActionNames) {
				t.Errorf("mode %q, set %q: registered %v, want %v", mode, s.Name, got, sourceActionNames)
			}
			if s.Clients.write != (mode == "on") {
				t.Errorf("mode %q, set %q: write = %v", mode, s.Name, s.Clients.write)
			}
		}
		st.Close()
	}
}

// TestSourceActionsHoldAdminWriterOnlyWithWriteModeOn is the Q2e done-when:
// with write mode off no admin writer exists for any action to reach (the
// P1a-3 walk in writemode_test.go covers the writer interfaces); with it on
// only the set built on SOURCE_DB_WRITE_URL hands one over, and it is that
// set's own writer. Without a source writer the actions refuse Execute before
// any I/O.
func TestSourceActionsHoldAdminWriterOnlyWithWriteModeOn(t *testing.T) {
	adminType := reflect.TypeOf(fakeSourceWriter{})

	// Off: nothing reachable implements the admin writer, and Execute refuses.
	f := newFakeOpener()
	cfg := config.Config{SourceDBURL: readerDSN, SourceDBWriteURL: writerDSN}
	st, err := openStacks(context.Background(), cfg, nil, loadOwners(t, threeSetYAML()), StackOptions{}, f.deps(env(twoTenantEnv())))
	if err != nil {
		t.Fatalf("openStacks (write mode off): %v", err)
	}
	defer st.Close()
	pa := st.For("pa")
	if actionAdminWriter(pa.Clients) != nil {
		t.Fatal("write mode off: actionAdminWriter returned a writer")
	}
	for _, s := range st.All() {
		for _, a := range s.Actions.All() {
			if !slices.Contains(sourceActionNames, a.Name()) {
				continue
			}
			for ty := range reachableTypes(only(t, s.Actions, a.Name())) {
				if ty.Implements(ifaceOf[postgres.SourceAdminWriter]()) {
					t.Errorf("write mode off: %s of set %q reaches %s, an admin writer", a.Name(), s.Name, ty)
				}
			}
		}
	}
	pipe, _ := loadOwners(t, threeSetYAML()).Pipeline("pa")
	ctx, cancel := context.WithTimeout(tools.WithPipeline(context.Background(), pipe), 3*time.Second)
	defer cancel()
	for name, params := range sourceActionParams("pa", "a") {
		a, ok := pa.Actions.Get(name)
		if !ok {
			t.Fatalf("%s not registered", name)
		}
		if _, err := a.Execute(ctx, params); err == nil || !strings.Contains(err.Error(), "no write credentials") {
			t.Errorf("%s with write mode off: err = %v, want the no-write-credentials refusal", name, err)
		}
	}
	if len(f.writers) != 0 {
		t.Fatalf("write mode off built %d write clients", len(f.writers))
	}

	// On: only the process-wide set has one (the pa/pb pipelines set no
	// source_db_write_url), and its source actions hold that writer.
	f = newFakeOpener()
	cfg.WriteMode = "on"
	st2, err := openStacks(context.Background(), cfg, nil, loadOwners(t, threeSetYAML()), StackOptions{}, f.deps(env(twoTenantEnv())))
	if err != nil {
		t.Fatalf("openStacks (write mode on): %v", err)
	}
	defer st2.Close()
	for _, s := range st2.All() {
		admin := actionAdminWriter(s.Clients)
		want := s.Name == ProcessWideStack
		if (admin != nil) != want {
			t.Errorf("set %q: admin writer present = %v, want %v", s.Name, admin != nil, want)
		}
		hasWriter := false
		for ty := range reachableTypes(s.Actions) {
			if ty == adminType {
				hasWriter = true
			}
		}
		if hasWriter != want {
			t.Errorf("set %q: a source action reaches an admin writer = %v, want %v", s.Name, hasWriter, want)
		}
	}
	// The process-wide writer performs the step: its counter moves, and no
	// other set's writer does. The process-wide set serves the pipelines
	// without a connections: block, pc in this fixture.
	var pw *Stack
	for _, s := range st2.All() {
		if s.Name == ProcessWideStack {
			pw = s
		}
	}
	if pw == nil || !slices.Contains(pw.Pipelines, "pc") {
		t.Fatalf("the process-wide set serves %+v, want pc", pw)
	}
	a, ok := pw.Actions.Get("pg_set_replica_identity")
	if !ok {
		t.Fatal("pg_set_replica_identity not registered")
	}
	before := map[*fakeWriter]int64{}
	for _, w := range f.writers {
		before[w] = w.writes.Load()
	}
	pc, _ := loadOwners(t, threeSetYAML()).Pipeline("pc")
	ctx2, cancel2 := context.WithTimeout(tools.WithPipeline(context.Background(), pc), 3*time.Second)
	defer cancel2()
	if _, err := a.Execute(ctx2, sourceActionParams("pc", "c")["pg_set_replica_identity"]); err != nil {
		t.Fatalf("pg_set_replica_identity with write mode on: %v", err)
	}
	var moved []string
	for _, w := range f.writers {
		if w.writes.Load() > before[w] {
			moved = append(moved, w.kind+" on "+w.dsn)
		}
	}
	if len(moved) != 1 || !strings.HasPrefix(moved[0], "source on "+writerDSN) {
		t.Errorf("the admin write reached %v, want the source writer on SOURCE_DB_WRITE_URL", moved)
	}
}

// TestRegisterSourceActions: the exported helper registers the four in order
// and refuses a second registration into the same registry.
func TestRegisterSourceActions(t *testing.T) {
	areg := actions.NewRegistry()
	if err := RegisterSourceActions(areg, SourceActionDeps{}); err != nil {
		t.Fatalf("RegisterSourceActions: %v", err)
	}
	var got []string
	for _, a := range areg.All() {
		got = append(got, a.Name())
	}
	if !slices.Equal(got, sourceActionNames) {
		t.Fatalf("registered %v, want %v", got, sourceActionNames)
	}
	if err := RegisterSourceActions(areg, SourceActionDeps{}); err == nil {
		t.Fatal("a second registration succeeded")
	}

	// A typed nil writer is stored as a nil interface, so Execute refuses
	// rather than panics (the actions check Writer == nil).
	var typedNil *postgres.Writer
	areg = actions.NewRegistry()
	if err := RegisterSourceActions(areg, SourceActionDeps{Writer: typedNil}); err != nil {
		t.Fatalf("RegisterSourceActions (typed nil): %v", err)
	}
	a, _ := areg.Get("source_grant_select")
	if _, err := a.Execute(context.Background(), sourceActionParams("pa", "a")["source_grant_select"]); !errors.Is(err, tools.ErrNoScope) {
		// No room scope here, so scope is refused first; the point is that
		// the call returns an error instead of panicking on a typed nil.
		t.Fatalf("Execute with a typed nil writer and no scope: err = %v, want ErrNoScope", err)
	}
}
