package sourceactions

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/tools"
)

var testStart = time.Date(2026, 10, 23, 8, 0, 0, 123456000, time.UTC)

// allActions builds the four actions over one source, Connect and writer, with
// params each would accept.
func allActions(src *fakeSource, conn *fakeConnect, w *fakeWriter) []struct {
	a      actions.Action
	params json.RawMessage
} {
	var wRI ReplicaIdentityWriter
	var wHB HeartbeatWriter
	var wGS GrantWriter
	var wES SessionEnder
	if w != nil {
		wRI, wHB, wGS, wES = w, w, w, w
	}
	return []struct {
		a      actions.Action
		params json.RawMessage
	}{
		{NewSetReplicaIdentity(ReplicaIdentityDeps{Source: src, Writer: wRI}), json.RawMessage(`{"table":"public.refunds","mode":"full"}`)},
		{NewCreateHeartbeatTable(HeartbeatDeps{Connect: conn, Source: src, Writer: wHB}), json.RawMessage(`{"connector":"pg-orders","table":"public.debezium_heartbeat"}`)},
		{NewGrantSelect(GrantSelectDeps{Connect: conn, Source: src, Writer: wGS}), json.RawMessage(`{"connector":"pg-orders","role":"debezium","tables":["public.refunds"]}`)},
		{NewEndSession(EndSessionDeps{Connect: conn, Source: src, Writer: wES}), json.RawMessage(`{"connector":"pg-orders","pid":4242,"started_at":"` + testStart.Format(time.RFC3339Nano) + `"}`)},
	}
}

// readySource is a source on which every action's step is valid.
func readySource() *fakeSource {
	return &fakeSource{
		tables: map[string]TableReplication{"public.refunds": {Table: "public.refunds", ReplicaIdentity: "default"}},
		role:   RoleInfo{Role: "debezium", Exists: true, CanLogin: true, Replication: true, Selectable: map[string]bool{"public.refunds": false}},
		sessions: []DBSession{{PID: 4242, StartedAt: testStart, XactStart: testStart.Add(time.Minute), User: "app", App: "billing",
			State: "idle in transaction", Query: "UPDATE accounts SET password = 'hunter2' WHERE id = 7"}},
		rows: 1000,
	}
}

// TestPreviewNeverWrites is the actions' read-only guarantee (action.go:
// Preview calls only reader methods): with a recording writer wired in,
// Validate and Preview of every action make no write call.
func TestPreviewNeverWrites(t *testing.T) {
	src, conn, w := readySource(), newConnect(), &fakeWriter{}
	for _, tc := range allActions(src, conn, w) {
		t.Run(tc.a.Name(), func(t *testing.T) {
			if err := tc.a.Validate(scopedCtx(), tc.params); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			res, err := tc.a.(actions.Preview).Preview(scopedCtx(), tc.params)
			if err != nil {
				t.Fatalf("Preview: %v", err)
			}
			if res.Target == "" || res.Current == "" || res.Expected == "" {
				t.Fatalf("Preview is incomplete: %+v", res)
			}
			if len(w.calls) != 0 {
				t.Fatalf("Validate or Preview wrote: %+v", w.calls)
			}
		})
	}
	if src.noDeadline {
		t.Fatal("a read ran without a deadline")
	}
}

// TestPreviewWorksWithoutWriter: write mode off still lets a reviewer see
// the step; Validate refuses it.
func TestPreviewWorksWithoutWriter(t *testing.T) {
	for _, tc := range allActions(readySource(), newConnect(), nil) {
		t.Run(tc.a.Name(), func(t *testing.T) {
			if _, err := tc.a.(actions.Preview).Preview(scopedCtx(), tc.params); err != nil {
				t.Fatalf("Preview without writer: %v", err)
			}
			if err := tc.a.Validate(scopedCtx(), tc.params); err == nil || !errors.Is(err, errNoWriter) {
				t.Fatalf("Validate without writer = %v, want errNoWriter", err)
			}
			if _, err := tc.a.Execute(scopedCtx(), tc.params); !errors.Is(err, errNoWriter) {
				t.Fatalf("Execute without writer = %v, want errNoWriter", err)
			}
		})
	}
}

// TestEveryActionIsScopedAndPreviewed checks the contract shape the room
// service and the registry rely on.
func TestEveryActionIsScopedAndPreviewed(t *testing.T) {
	reg := actions.NewRegistry()
	for _, tc := range allActions(readySource(), newConnect(), &fakeWriter{}) {
		if _, ok := tc.a.(actions.Scoped); !ok {
			t.Errorf("%s is not Scoped", tc.a.Name())
		}
		if _, ok := tc.a.(actions.Preview); !ok {
			t.Errorf("%s has no Preview", tc.a.Name())
		}
		if got := actions.ClassOf(tc.a); got != actions.ClassFix {
			t.Errorf("%s class = %q, want fix", tc.a.Name(), got)
		}
		if tc.a.Layer() != "postgres" || tc.a.Describe() == "" || tc.a.ParamsSchema()["type"] != "object" {
			t.Errorf("%s meta: layer %q", tc.a.Name(), tc.a.Layer())
		}
		if err := reg.Register(tc.a); err != nil {
			t.Errorf("Register(%s): %v", tc.a.Name(), err)
		}
	}
}

// TestOutOfPipelineRefusedBeforeIO: CheckScope decides from params alone and
// Preview and Execute fail closed without a scope, all before any read.
func TestOutOfPipelineRefusedBeforeIO(t *testing.T) {
	other := testPipeline()
	other.ID, other.Connectors, other.SourceTables = "billing", []string{"pg-billing"}, []string{"billing.invoices"}
	src, conn, w := readySource(), newConnect(), &fakeWriter{}
	for _, tc := range allActions(src, conn, w) {
		t.Run(tc.a.Name(), func(t *testing.T) {
			if err := tc.a.(actions.Scoped).CheckScope(other, tc.params); !errors.Is(err, tools.ErrOutOfScope) {
				t.Fatalf("CheckScope(other pipeline) = %v, want ErrOutOfScope", err)
			}
			if err := tc.a.(actions.Scoped).CheckScope(testPipeline(), tc.params); err != nil {
				t.Fatalf("CheckScope(own pipeline) = %v", err)
			}
			octx := tools.WithPipeline(context.Background(), other)
			if err := tc.a.Validate(octx, tc.params); !errors.Is(err, tools.ErrOutOfScope) {
				t.Fatalf("Validate(other pipeline) = %v, want ErrOutOfScope", err)
			}
			if _, err := tc.a.(actions.Preview).Preview(octx, tc.params); !errors.Is(err, tools.ErrOutOfScope) {
				t.Fatalf("Preview(other pipeline) = %v, want ErrOutOfScope", err)
			}
			if _, err := tc.a.Execute(octx, tc.params); !errors.Is(err, tools.ErrOutOfScope) {
				t.Fatalf("Execute(other pipeline) = %v, want ErrOutOfScope", err)
			}
			if _, err := tc.a.(actions.Preview).Preview(context.Background(), tc.params); !errors.Is(err, tools.ErrNoScope) {
				t.Fatalf("Preview(no scope) = %v, want ErrNoScope", err)
			}
			if _, err := tc.a.Execute(context.Background(), tc.params); !errors.Is(err, tools.ErrNoScope) {
				t.Fatalf("Execute(no scope) = %v, want ErrNoScope", err)
			}
		})
	}
	if src.calls != 0 || conn.calls != 0 || len(w.calls) != 0 {
		t.Fatalf("I/O before the scope refusal: source %d, connect %d, writes %d", src.calls, conn.calls, len(w.calls))
	}
}

// TestNonCDCPipelineRefused: a batch pipeline has no source database.
func TestNonCDCPipelineRefused(t *testing.T) {
	batch := testPipeline()
	batch.Kind = "batch"
	for _, tc := range allActions(readySource(), newConnect(), &fakeWriter{}) {
		if err := tc.a.(actions.Scoped).CheckScope(batch, tc.params); !errors.Is(err, tools.ErrOutOfScope) {
			t.Errorf("%s CheckScope(batch) = %v, want ErrOutOfScope", tc.a.Name(), err)
		}
	}
}

// TestUnscopedExecuteWrites: the explicit opt-out (CLI, dev) reaches the
// writer, once per action.
func TestUnscopedExecuteWrites(t *testing.T) {
	w := &fakeWriter{}
	for _, tc := range allActions(readySource(), newConnect(), w) {
		if _, err := tc.a.Execute(tools.WithUnscoped(context.Background()), tc.params); err != nil {
			t.Fatalf("%s Execute(unscoped): %v", tc.a.Name(), err)
		}
	}
	if len(w.calls) != 4 {
		t.Fatalf("writes = %+v, want one per action", w.calls)
	}
}

// TestParamsRefusedBeforeIO: unknown fields and bad identifiers never reach a
// client.
func TestParamsRefusedBeforeIO(t *testing.T) {
	src, conn, w := readySource(), newConnect(), &fakeWriter{}
	acts := allActions(src, conn, w)
	bad := map[string][]string{
		SetReplicaIdentityName: {
			`{"table":"public.refunds","mode":"full","extra":1}`,
			`{"table":"refunds","mode":"full"}`,
			`{"table":"public.re\"funds","mode":"full"}`,
			`{"table":"public.refunds","mode":"nothing"}`,
			`{"table":"public.refunds","mode":"full","index":"refunds_pkey"}`,
			`{"table":"public.refunds","mode":"index"}`,
			`{"table":"public.refunds","mode":"index","index":"x; DROP TABLE y"}`,
			`{"table":"public.refunds","mode":"full"} {}`,
		},
		CreateHeartbeatTableName: {
			`{"connector":"","table":"public.hb"}`,
			`{"connector":"pg-orders","table":"public.hb;"}`,
			`{"connector":"pg-orders"}`,
		},
		GrantSelectName: {
			`{"connector":"pg-orders","role":"debezium","tables":[]}`,
			`{"connector":"pg-orders","role":"PUBLIC","tables":["public.refunds"]}`,
			`{"connector":"pg-orders","role":"pg_read_all_data","tables":["public.refunds"]}`,
			`{"connector":"pg-orders","role":"debezium\" WITH GRANT OPTION","tables":["public.refunds"]}`,
			`{"connector":"pg-orders","role":"debezium","tables":["public.refunds","public.refunds"]}`,
			`{"connector":"pg-orders","role":"debezium","tables":["public.refunds"],"privilege":"ALL"}`,
		},
		EndSessionName: {
			`{"connector":"pg-orders","pid":0,"started_at":"2026-10-23T08:00:00Z"}`,
			`{"connector":"pg-orders","pid":42}`,
			`{"connector":"pg-orders","pid":42,"started_at":"yesterday"}`,
			`{"connector":"pg-orders","pid":42,"started_at":"2026-10-23T08:00:00Z","older_than_minutes":0}`,
			`{"connector":"pg-orders","pid":42,"started_at":"2026-10-23T08:00:00Z","older_than_minutes":50000}`,
		},
	}
	for _, tc := range acts {
		for _, raw := range bad[tc.a.Name()] {
			if err := tc.a.Validate(scopedCtx(), json.RawMessage(raw)); err == nil {
				t.Errorf("%s Validate(%s) accepted", tc.a.Name(), raw)
			}
			if err := tc.a.(actions.Scoped).CheckScope(testPipeline(), json.RawMessage(raw)); err == nil {
				t.Errorf("%s CheckScope(%s) accepted", tc.a.Name(), raw)
			}
			if _, err := tc.a.Execute(scopedCtx(), json.RawMessage(raw)); err == nil {
				t.Errorf("%s Execute(%s) accepted", tc.a.Name(), raw)
			}
		}
	}
	if src.calls != 0 || conn.calls != 0 || len(w.calls) != 0 {
		t.Fatalf("I/O on bad params: source %d, connect %d, writes %d", src.calls, conn.calls, len(w.calls))
	}
}
