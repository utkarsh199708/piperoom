package wiring

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/config"
	"piperoom/internal/tools"
)

// sourceActionNames are the Q2 source admin actions RegisterSourceActions
// adds (Q2c, Q2d), in its registration order.
var sourceActionNames = []string{
	"pg_set_replica_identity", "source_create_heartbeat_table", "source_grant_select", "source_end_session",
}

// sourceActionParams are params each Q2 action accepts in pipeline id's
// scope (source table public.<x>, connector <id>-connector).
func sourceActionParams(id string) map[string]json.RawMessage {
	table := "public." + strings.TrimPrefix(id, "p")
	return map[string]json.RawMessage{
		"pg_set_replica_identity":       json.RawMessage(`{"table":"` + table + `","mode":"full"}`),
		"source_create_heartbeat_table": json.RawMessage(`{"connector":"` + id + `-connector","table":"public.debezium_heartbeat"}`),
		"source_grant_select":           json.RawMessage(`{"connector":"` + id + `-connector","role":"debezium","tables":["` + table + `"]}`),
		"source_end_session":            json.RawMessage(`{"connector":"` + id + `-connector","pid":4242,"started_at":"2026-10-23T08:00:00Z"}`),
	}
}

// assertSourceActionsRefuse runs Validate and Execute of every Q2 action of
// areg with params it accepts in ctx (the room's scope) and wants the "no
// write credentials" refusal of a missing admin writer from both. A typed nil
// writer would get past the actions' nil check and fail differently, or
// panic.
func assertSourceActionsRefuse(t *testing.T, areg *actions.Registry, ctx context.Context, params map[string]json.RawMessage) {
	t.Helper()
	const want = "no write credentials"
	for _, n := range sourceActionNames {
		a, ok := areg.Get(n)
		if !ok {
			t.Errorf("%s not registered", n)
			continue
		}
		if err := a.Validate(ctx, params[n]); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s Validate without the admin writer = %v, want the %q refusal", n, err, want)
		}
		if _, err := a.Execute(ctx, params[n]); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s Execute without the admin writer = %v, want the %q refusal", n, err, want)
		}
	}
}

// TestSourceActionsWriteModeOn: with write mode on, the Q2 actions of the
// process-wide set hold its source writer (on SOURCE_DB_WRITE_URL) as their
// admin writer; a set with its own connections and no source_db_write_url
// gets none, and its actions refuse as with write mode off.
func TestSourceActionsWriteModeOn(t *testing.T) {
	f := newFakeOpener()
	cfg := config.Config{SourceDBURL: readerDSN, SourceDBWriteURL: writerDSN, WriteMode: "on"}
	st, err := openStacks(context.Background(), cfg, nil, loadOwners(t, threeSetYAML()), StackOptions{}, f.deps(env(twoTenantEnv())))
	if err != nil {
		t.Fatalf("openStacks: %v", err)
	}
	defer st.Close()

	var pw *Stack
	for _, s := range st.All() {
		if s.Name == ProcessWideStack {
			pw = s
		}
	}
	if pw == nil {
		t.Fatal("no process-wide set")
	}
	own, ok := pw.Clients.writers.source.(fakeSourceWriter)
	if !ok || own.dsn != writerDSN {
		t.Fatalf("process-wide set: source writer = %#v, want one on SOURCE_DB_WRITE_URL", pw.Clients.writers.source)
	}
	for _, n := range sourceActionNames {
		a, ok := pw.Actions.Get(n)
		if !ok {
			t.Fatalf("process-wide set: %s not registered", n)
		}
		held := heldSourceWriters(t, f, a)
		if len(held) != 1 || held[0] != own.fakeWriter {
			t.Errorf("process-wide set: %s holds %d source writers, want exactly its own", n, len(held))
		}
	}

	// pa has its own connections and no source_db_write_url: no admin
	// writer, so each action refuses, and it never holds the process-wide
	// set's writer.
	pa := st.For("pa")
	if adminWriter(pa.Clients) != nil {
		t.Fatal("pa: an admin writer without a source write URL")
	}
	for _, n := range sourceActionNames {
		a, _ := pa.Actions.Get(n)
		if held := heldSourceWriters(t, f, a); len(held) != 0 {
			t.Errorf("pa: %s holds a source writer", n)
		}
	}
	pipe, _ := loadOwners(t, threeSetYAML()).Pipeline("pa")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	assertSourceActionsRefuse(t, pa.Actions, tools.WithPipeline(ctx, pipe), sourceActionParams("pa"))
	if got := own.writes.Load(); got != 0 {
		t.Fatalf("%d admin writes reached the process-wide writer", got)
	}
}

// TestAdminWriterNeedsWriteMode: adminWriter is nil with write mode off even
// when a source writer is somehow present, and nil for a source writer
// without the admin methods.
func TestAdminWriterNeedsWriteMode(t *testing.T) {
	sw := fakeSourceWriter{&fakeWriter{kind: "source"}}
	if adminWriter(&Wired{write: false, writers: writeClients{source: sw}}) != nil {
		t.Error("admin writer with write mode off")
	}
	if adminWriter(&Wired{write: true}) != nil {
		t.Error("admin writer without a source writer")
	}
	if adminWriter(&Wired{write: true, writers: writeClients{source: plainSourceWriter{sw}}}) != nil {
		t.Error("admin writer from a source writer without the admin methods")
	}
	if adminWriter(&Wired{write: true, writers: writeClients{source: sw}}) == nil {
		t.Error("no admin writer with write mode on and a source writer")
	}
}

// plainSourceWriter is a source writer with the P3 methods only.
type plainSourceWriter struct{ w fakeSourceWriter }

func (p plainSourceWriter) AddPublicationTables(ctx context.Context, pub string, t []string) error {
	return p.w.AddPublicationTables(ctx, pub, t)
}
func (p plainSourceWriter) DropReplicationSlot(ctx context.Context, slot string) error {
	return p.w.DropReplicationSlot(ctx, slot)
}
func (p plainSourceWriter) InsertSnapshotSignal(ctx context.Context, a, b string, c []string) (string, error) {
	return p.w.InsertSnapshotSignal(ctx, a, b, c)
}
func (p plainSourceWriter) Close() { p.w.Close() }

// TestRegisterSourceActionsTypedNil: a typed-nil admin writer is handed to
// the actions as an untyped nil, so each refuses cleanly at Validate and
// Execute instead of reaching a nil *postgres.Writer.
func TestRegisterSourceActionsTypedNil(t *testing.T) {
	areg := actions.NewRegistry()
	if err := RegisterSourceActions(areg, SourceActionDeps{Admin: (*postgres.Writer)(nil)}); err != nil {
		t.Fatalf("RegisterSourceActions: %v", err)
	}
	var got []string
	for _, a := range areg.All() {
		got = append(got, a.Name())
	}
	if strings.Join(got, ",") != strings.Join(sourceActionNames, ",") {
		t.Fatalf("registered %v, want %v", got, sourceActionNames)
	}
	for ty := range reachableTypes(areg) {
		for _, w := range adminWriterIfaces {
			if ty.Implements(w) {
				t.Errorf("an action can reach %s, which implements %s", ty, w)
			}
		}
	}
	pipe, _ := loadOwners(t, threeSetYAML()).Pipeline("pa")
	assertSourceActionsRefuse(t, areg, tools.WithPipeline(context.Background(), pipe), sourceActionParams("pa"))
}
