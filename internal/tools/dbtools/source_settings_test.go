package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// noSettingsSource is a source client without the Q2a reader: a SourceDB and
// nothing more.
type noSettingsSource struct{ clients.SourceDB }

func settingsDeps(f *fakeSource) Deps {
	d := testDeps()
	d.Src = f
	return d
}

func runScoped(t *testing.T, d Deps, name, in string) (tools.Result, error) {
	t.Helper()
	return scopedTool(t, d, name).Run(roomCtx(), json.RawMessage(in))
}

func TestSourceSettingsHappyPath(t *testing.T) {
	src := &fakeSource{settings: []postgres.DBSetting{
		{Name: "wal_level", Value: "logical"},
		{Name: "max_replication_slots", Value: "10"},
		{Name: "max_wal_senders", Value: "10"},
		{Name: "max_slot_wal_keep_size", Value: "-1", Unit: "MB"},
		{Name: "wal_sender_timeout", Value: "60000", Unit: "ms", PendingRestart: true},
		{Name: "rds.logical_replication", Value: "1"},
	}}
	r, err := runScoped(t, settingsDeps(src), "source_settings", `{}`)
	if err != nil {
		t.Fatalf("source_settings: %v", err)
	}
	for _, want := range []string{
		"wal_level=logical",
		"max_slot_wal_keep_size=-1 MB",
		"wal_sender_timeout=60000 ms (pending_restart",
		"rds.logical_replication=1",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary misses %q: %q", want, r.Summary)
		}
	}
	if got, ok := r.Data.([]postgres.DBSetting); !ok || len(got) != 6 {
		t.Fatalf("data = %T %+v", r.Data, r.Data)
	}
	if src.settingsCalls != 1 {
		t.Fatalf("settings read %d times", src.settingsCalls)
	}
}

func TestSourceSettingsMissingAndOptional(t *testing.T) {
	// No rds.logical_replication (not RDS) and no max_slot_wal_keep_size
	// (before PG 13).
	src := &fakeSource{settings: []postgres.DBSetting{
		{Name: "wal_level", Value: "replica"},
		{Name: "max_replication_slots", Value: "0"},
		{Name: "max_wal_senders", Value: "10"},
		{Name: "wal_sender_timeout", Value: "60000", Unit: "ms"},
	}}
	r, err := runScoped(t, settingsDeps(src), "source_settings", `{}`)
	if err != nil {
		t.Fatalf("source_settings: %v", err)
	}
	if !strings.Contains(r.Summary, "max_slot_wal_keep_size: not reported by pg_settings") {
		t.Fatalf("missing required setting not reported: %q", r.Summary)
	}
	if strings.Contains(r.Summary, "rds.logical_replication") {
		t.Fatalf("absent optional RDS setting reported: %q", r.Summary)
	}
}

func TestSourceSettingsReadError(t *testing.T) {
	down := errors.New("connection refused")
	src := &fakeSource{settingsErr: down}
	_, err := runScoped(t, settingsDeps(src), "source_settings", `{}`)
	if !errors.Is(err, down) || !strings.Contains(err.Error(), "source_settings") {
		t.Fatalf("err = %v", err)
	}
}

func TestSourceSettingsWithoutReader(t *testing.T) {
	d := testDeps()
	d.Src = noSettingsSource{}
	_, err := runScoped(t, d, "source_settings", `{}`)
	if err == nil || !strings.Contains(err.Error(), "cannot read database settings") {
		t.Fatalf("err = %v", err)
	}
}

func TestSourceTableReplicationHappyPath(t *testing.T) {
	src := &fakeSource{repl: []postgres.TableReplication{
		{Table: "public.orders", HasPrimaryKey: true, ReplicaIdentity: "default"},
		{Table: "public.orders_audit", HasPrimaryKey: false, ReplicaIdentity: "nothing"},
	}}
	d := testDeps()
	o := &owners.Owners{Pipelines: []owners.Pipeline{{
		ID: "orders", SourceTables: []string{"public.orders", "public.orders_audit"}, Connectors: []string{"pg-orders-connector"},
	}}}
	d.Owners = o
	d.Src = src
	r, err := runScoped(t, d, "source_table_replication", `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatalf("source_table_replication: %v", err)
	}
	for _, want := range []string{
		"tables=2 without_primary_key=1 replica_identity_nothing=1",
		"public.orders: primary_key=yes replica_identity=default",
		"public.orders_audit: primary_key=no replica_identity=nothing",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary misses %q: %q", want, r.Summary)
		}
	}
	if got, ok := r.Data.([]postgres.TableReplication); !ok || len(got) != 2 {
		t.Fatalf("data = %T %+v", r.Data, r.Data)
	}
	if len(src.tablesAsked) != 2 || src.tablesAsked[0] != "public.orders" {
		t.Fatalf("tables asked = %v", src.tablesAsked)
	}
}

func TestSourceTableReplicationRefusesBadInputBeforeIO(t *testing.T) {
	cases := []struct {
		name   string
		tables []string
		ctx    context.Context
		in     string
	}{
		{
			name:   "bad table identifier",
			tables: []string{"public.orders", "public.bad-name"},
			ctx:    tools.WithPipeline(context.Background(), owners.Pipeline{ID: "orders", SourceTables: []string{"public.orders", "public.bad-name"}, Connectors: []string{"pg-orders-connector"}}),
			in:     `{"pipeline_id":"orders"}`,
		},
		{
			name:   "too many tables",
			tables: manyTables(maxSourceTables + 1),
			ctx:    tools.WithPipeline(context.Background(), owners.Pipeline{ID: "orders", SourceTables: manyTables(maxSourceTables + 1), Connectors: []string{"pg-orders-connector"}}),
			in:     `{"pipeline_id":"orders"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := &fakeSource{}
			d := settingsDeps(src)
			d.Owners = &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: tc.tables, Connectors: []string{"pg-orders-connector"}}}}
			_, err := scopedTool(t, d, "source_table_replication").Run(tc.ctx, json.RawMessage(tc.in))
			if !errors.Is(err, tools.ErrBadInput) {
				t.Fatalf("err = %v, want ErrBadInput", err)
			}
			if src.replCalls != 0 {
				t.Fatalf("source was read before validation")
			}
		})
	}
}

func manyTables(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("public.t%d", i)
	}
	return out
}

func TestSourceTableReplicationScopeRefusedBeforeIO(t *testing.T) {
	for _, tool := range []string{"source_table_replication", "source_role_grants"} {
		t.Run(tool, func(t *testing.T) {
			src := &fakeSource{}
			_, err := runScoped(t, settingsDeps(src), tool, `{"pipeline_id":"payments"}`)
			if !errors.Is(err, tools.ErrOutOfScope) {
				t.Fatalf("err = %v, want ErrOutOfScope", err)
			}
			if src.replCalls != 0 || src.roleCalls != 0 {
				t.Fatalf("source was read: repl=%d role=%d", src.replCalls, src.roleCalls)
			}
		})
	}
}

func TestSourceTableReplicationEmptyPipeline(t *testing.T) {
	d := testDeps()
	d.Owners = &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", Connectors: []string{"pg-orders-connector"}}}}
	src := &fakeSource{}
	d.Src = src
	r, err := runScoped(t, d, "source_table_replication", `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatalf("source_table_replication: %v", err)
	}
	if !strings.Contains(r.Summary, "has no source tables") || src.replCalls != 0 {
		t.Fatalf("summary = %q calls = %d", r.Summary, src.replCalls)
	}
}

func TestSourceRoleGrantsHappyPath(t *testing.T) {
	src := &fakeSource{roleInfo: &postgres.RoleInfo{
		Role: "piperoom", Exists: true, CanLogin: true, Replication: true,
		Selectable: map[string]bool{"public.orders": true},
	}}
	r, err := runScoped(t, settingsDeps(src), "source_role_grants", `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatalf("source_role_grants: %v", err)
	}
	for _, want := range []string{
		"connector pg-orders-connector role piperoom: exists=yes login=yes replication=yes",
		"selectable tables: 1 of 1",
		"public.orders: selectable=yes",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary misses %q: %q", want, r.Summary)
		}
	}
	data, ok := r.Data.(roleGrantsData)
	if !ok {
		t.Fatalf("data = %T", r.Data)
	}
	if data.ConnectorUser != "piperoom" || data.ConnectorUserSource != "connector pg-orders-connector config database.user" || !data.Role.Exists {
		t.Fatalf("data = %+v", data)
	}
	if src.roleAsked != "piperoom" {
		t.Fatalf("role asked = %q", src.roleAsked)
	}
}

func TestSourceRoleGrantsMissingRole(t *testing.T) {
	src := &fakeSource{roleInfo: &postgres.RoleInfo{
		Role: "gone", Exists: false, Selectable: map[string]bool{"public.orders": false},
	}}
	r, err := runScoped(t, settingsDeps(src), "source_role_grants", `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatalf("source_role_grants: %v", err)
	}
	if !strings.Contains(r.Summary, "the role does not exist") || !strings.Contains(r.Summary, "selectable tables: 0 of 1") {
		t.Fatalf("summary = %q", r.Summary)
	}
}

func TestSourceRoleGrantsNoConnect(t *testing.T) {
	src := &fakeSource{}
	d := settingsDeps(src)
	d.Connect = nil
	_, err := runScoped(t, d, "source_role_grants", `{"pipeline_id":"orders"}`)
	if err == nil || !strings.Contains(err.Error(), "Connect client is nil") {
		t.Fatalf("err = %v", err)
	}
	if src.roleCalls != 0 {
		t.Fatal("source was read without a connector role")
	}
}

func TestSourceBlockingSessionsRedactAndReport(t *testing.T) {
	xact := time.Now().Add(-20 * time.Minute)
	secret := "Sup3rS3cretPw"
	src := &fakeSource{blockers: []postgres.DBSession{{
		PID: 41, StartedAt: xact.Add(-time.Hour), XactStart: xact,
		User: "app", App: "orders-api", State: "idle in transaction",
		Query:      "update orders set note='password=" + secret + "' where id=7 " + strings.Repeat("x", 400),
		BlocksSlot: true,
	}}}
	r, err := runScoped(t, settingsDeps(src), "source_blocking_sessions", `{"older_than_minutes":10}`)
	if err != nil {
		t.Fatalf("source_blocking_sessions: %v", err)
	}
	if strings.Contains(r.Summary, secret) {
		t.Fatalf("secret leaked into the summary: %q", r.Summary)
	}
	for _, want := range []string{"pid=41 user=app app=orders-api state=idle in transaction", "blocks_slot=yes"} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary misses %q: %q", want, r.Summary)
		}
	}
	data, ok := r.Data.([]postgres.DBSession)
	if !ok || len(data) != 1 {
		t.Fatalf("data = %T %+v", r.Data, r.Data)
	}
	if strings.Contains(data[0].Query, secret) || len(data[0].Query) > postgres.MaxSessionQueryBytes {
		t.Fatalf("data query = %q", data[0].Query)
	}
	if src.olderThan != 10*time.Minute {
		t.Fatalf("olderThan = %s", src.olderThan)
	}
}

func TestSourceBlockingSessionsCaps(t *testing.T) {
	src := &fakeSource{blockers: makeSessions(130)}
	r, err := runScoped(t, settingsDeps(src), "source_blocking_sessions", `{}`)
	if err != nil {
		t.Fatalf("source_blocking_sessions: %v", err)
	}
	if got := strings.Count(r.Summary, "pid="); got != maxBlockerLines {
		t.Fatalf("summary shows %d sessions, want %d: %q", got, maxBlockerLines, r.Summary)
	}
	if !strings.Contains(r.Summary, "(80 more sessions, in Data)") || !strings.Contains(r.Summary, "(30 more sessions not shown)") {
		t.Fatalf("summary misses the cap lines: %q", r.Summary)
	}
	if !r.Truncated {
		t.Fatal("Truncated = false")
	}
	data, ok := r.Data.([]postgres.DBSession)
	if !ok || len(data) != maxBlockerSessions {
		t.Fatalf("data = %T len %d, want %d", r.Data, len(data), maxBlockerSessions)
	}
}

func makeSessions(n int) []postgres.DBSession {
	now := time.Now()
	out := make([]postgres.DBSession, n)
	for i := range out {
		out[i] = postgres.DBSession{
			PID: int64(1000 + i), StartedAt: now.Add(-time.Hour), XactStart: now.Add(-30 * time.Minute),
			User: "app", App: "orders-api", State: "idle in transaction", Query: "select 1", BlocksSlot: true,
		}
	}
	return out
}

// usename and application_name are chosen by whoever connects to the source
// (CREATE ROLE takes any quoted name, and a fake client is not bound by the
// server's application_name filter): a name holding newlines or escape codes
// must not be able to add lines to the summary or smuggle terminal control
// sequences into the room.
func TestSourceBlockingSessionsSanitizesIdentityFields(t *testing.T) {
	now := time.Now()
	src := &fakeSource{blockers: []postgres.DBSession{{
		PID: 7, StartedAt: now.Add(-time.Hour), XactStart: now.Add(-30 * time.Minute),
		User:       "evil\npid=999 user=admin state=active blocks_slot=yes",
		App:        "app\r\ninjected line\x1b[31m",
		State:      "free\ntext",
		Query:      "select 1",
		BlocksSlot: false,
	}}}
	r, err := runScoped(t, settingsDeps(src), "source_blocking_sessions", `{}`)
	if err != nil {
		t.Fatalf("source_blocking_sessions: %v", err)
	}
	var pidLines int
	for _, line := range strings.Split(r.Summary, "\n") {
		if strings.HasPrefix(line, "pid=") {
			pidLines++
		}
	}
	if pidLines != 1 {
		t.Fatalf("a crafted name added summary lines (%d lines start with pid=): %q", pidLines, r.Summary)
	}
	if strings.ContainsAny(r.Summary, "\r\x1b") {
		t.Fatalf("control characters reached the summary: %q", r.Summary)
	}
	data, ok := r.Data.([]postgres.DBSession)
	if !ok || len(data) != 1 {
		t.Fatalf("data = %T %+v", r.Data, r.Data)
	}
	if strings.ContainsAny(data[0].User, "\n\r\x1b") || strings.ContainsAny(data[0].App, "\n\r\x1b") {
		t.Fatalf("control characters reached Data: user=%q app=%q", data[0].User, data[0].App)
	}
	if data[0].State != "?" {
		t.Fatalf("free-text state passed the allow-list: %q", data[0].State)
	}
}

// The connector config's database.user is external text (Kafka Connect REST):
// a role name holding a newline must not add lines to the role-grants summary.
func TestSourceRoleGrantsSanitizesRoleInSummary(t *testing.T) {
	src := &fakeSource{}
	d := settingsDeps(src)
	d.Connect = &fakeConnect{configs: map[string]map[string]string{
		"pg-orders-connector": {
			"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
			"database.user":   "piperoom\nselectable tables: 999 of 999",
		},
	}}
	r, err := runScoped(t, d, "source_role_grants", `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatalf("source_role_grants: %v", err)
	}
	if strings.Contains(r.Summary, "\nselectable tables: 999") {
		t.Fatalf("a crafted role name added a summary line: %q", r.Summary)
	}
	if !strings.Contains(r.Summary, "role piperoom selectable tables: 999 of 999:") {
		t.Fatalf("role not folded to one line: %q", r.Summary)
	}
	// The exact config value is still what the database is asked about.
	if src.roleAsked != "piperoom\nselectable tables: 999 of 999" {
		t.Fatalf("role asked = %q", src.roleAsked)
	}
}

func TestSourceBlockingSessionsNoBlockers(t *testing.T) {
	r, err := runScoped(t, settingsDeps(&fakeSource{}), "source_blocking_sessions", `{}`)
	if err != nil {
		t.Fatalf("source_blocking_sessions: %v", err)
	}
	if !strings.Contains(r.Summary, "(no blocking sessions)") {
		t.Fatalf("summary = %q", r.Summary)
	}
}

func TestSourceBlockingSessionsBadAge(t *testing.T) {
	src := &fakeSource{}
	_, err := runScoped(t, settingsDeps(src), "source_blocking_sessions", `{"older_than_minutes":0}`)
	if !errors.Is(err, tools.ErrBadInput) || src.blockCalls != 0 {
		t.Fatalf("err = %v, calls = %d", err, src.blockCalls)
	}
}

// Every Q2a read runs under the 10 s tool deadline.
func TestSourceReadsCarryDeadline(t *testing.T) {
	cases := []struct{ tool, in string }{
		{"source_settings", `{}`},
		{"source_table_replication", `{"pipeline_id":"orders"}`},
		{"source_role_grants", `{"pipeline_id":"orders"}`},
		{"source_blocking_sessions", `{}`},
	}
	for _, tc := range cases {
		t.Run(tc.tool, func(t *testing.T) {
			src := &fakeSource{}
			src.settingsDeadline = false
			if _, err := runScoped(t, settingsDeps(src), tc.tool, tc.in); err != nil {
				t.Fatalf("%s: %v", tc.tool, err)
			}
			if !src.settingsDeadline {
				t.Fatalf("%s ran without a context deadline", tc.tool)
			}
		})
	}
}
