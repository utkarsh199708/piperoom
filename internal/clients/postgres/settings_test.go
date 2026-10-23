package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
)

// settingsQuerier answers Query with prepared rows and QueryRow with rowScan,
// and remembers every statement. deadline records whether the method set a
// context deadline, the bound every read must carry.
type settingsQuerier struct {
	fakeQuerier
	queryRows [][]any
	rowScan   func(dest ...any) error
	deadline  bool
}

func (q *settingsQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	q.sqls, q.args = append(q.sqls, sql), append(q.args, args)
	if _, ok := ctx.Deadline(); ok {
		q.deadline = true
	}
	if q.err != nil {
		return nil, q.err
	}
	return &scriptedRows{rows: q.queryRows}, nil
}

func (q *settingsQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	q.sqls, q.args = append(q.sqls, sql), append(q.args, args)
	if _, ok := ctx.Deadline(); ok {
		q.deadline = true
	}
	return scanRow(q.rowScan)
}

func TestSettingsFixedOrderAndPendingRestart(t *testing.T) {
	q := &settingsQuerier{queryRows: [][]any{
		{"wal_level", "logical", "", false},
		{"max_replication_slots", "10", "", true},
		{"max_wal_senders", "10", "", false},
		{"max_slot_wal_keep_size", "-1", "MB", false},
		{"wal_sender_timeout", "60000", "ms", false},
		{"rds.logical_replication", "1", "", false},
	}}
	got, err := testDB(q).Settings(context.Background())
	if err != nil {
		t.Fatalf("Settings: %v", err)
	}
	if len(got) != 6 {
		t.Fatalf("got %d settings, want 6: %+v", len(got), got)
	}
	if got[1].Name != "max_replication_slots" || !got[1].PendingRestart || got[1].Value != "10" {
		t.Fatalf("pending_restart row = %+v", got[1])
	}
	if got[3].Unit != "MB" || got[4].Unit != "ms" {
		t.Fatalf("units = %q, %q", got[3].Unit, got[4].Unit)
	}
	if !q.deadline {
		t.Fatal("Settings ran without a context deadline")
	}
	if len(q.args) != 1 || strings.Join(q.args[0][0].([]string), ",") != strings.Join(SettingNames(), ",") {
		t.Fatalf("query arguments = %v, want the fixed list", q.args)
	}
	if !strings.Contains(q.sqls[0], "FROM pg_settings") || !strings.Contains(q.sqls[0], "pending_restart") {
		t.Fatalf("SQL = %q", q.sqls[0])
	}
}

// On a server without RDS settings (and before PG 13, without
// max_slot_wal_keep_size) the read returns what exists and no error.
func TestSettingsAbsentSettingIsOmitted(t *testing.T) {
	q := &settingsQuerier{queryRows: [][]any{
		{"wal_level", "replica", "", false},
		{"max_replication_slots", "0", "", false},
		{"max_wal_senders", "10", "", false},
		{"wal_sender_timeout", "60000", "ms", false},
	}}
	got, err := testDB(q).Settings(context.Background())
	if err != nil {
		t.Fatalf("Settings: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d settings, want 4: %+v", len(got), got)
	}
	for _, s := range got {
		if s.Name == "rds.logical_replication" || s.Name == "max_slot_wal_keep_size" {
			t.Fatalf("absent setting reported: %+v", s)
		}
	}
}

func TestSettingsErrorIsWrapped(t *testing.T) {
	down := errors.New("connection refused")
	q := &settingsQuerier{fakeQuerier: fakeQuerier{err: down}}
	_, err := testDB(q).Settings(context.Background())
	if !errors.Is(err, down) {
		t.Fatalf("err = %v, want the connection error", err)
	}
	if !strings.Contains(err.Error(), "read settings") {
		t.Fatalf("err = %v", err)
	}
}

func TestTableReplicationMapsIdentityAndKey(t *testing.T) {
	q := &settingsQuerier{queryRows: [][]any{
		{int64(1), true, "d", true},
		{int64(2), true, "n", false},
		{int64(3), true, "f", false},
		{int64(4), true, "i", true},
	}}
	tables := []string{"public.orders", "public.events", "public.audit", "public.with_index"}
	got, err := testDB(q).TableReplication(context.Background(), tables)
	if err != nil {
		t.Fatalf("TableReplication: %v", err)
	}
	want := []struct {
		identity string
		hasPK    bool
	}{
		{"default", true}, {"nothing", false}, {"full", false}, {"index", true},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].Table != tables[i] || got[i].ReplicaIdentity != w.identity || got[i].HasPrimaryKey != w.hasPK {
			t.Fatalf("row %d = %+v, want %+v", i, got[i], w)
		}
	}
	if !q.deadline {
		t.Fatal("TableReplication ran without a context deadline")
	}
	schemas := q.args[0][0].([]string)
	names := q.args[0][1].([]string)
	if strings.Join(schemas, ",") != "public,public,public,public" || names[1] != "events" {
		t.Fatalf("arguments = %v %v", schemas, names)
	}
	if !strings.Contains(q.sqls[0], "relreplident") || !strings.Contains(q.sqls[0], "indisprimary") {
		t.Fatalf("SQL = %q", q.sqls[0])
	}
}

func TestTableReplicationRefusesBadIdentifierBeforeIO(t *testing.T) {
	for _, table := range []string{"orders", "public.orders.extra", `public."Orders"`, "public.or-ders"} {
		q := &settingsQuerier{}
		_, err := testDB(q).TableReplication(context.Background(), []string{table})
		if err == nil {
			t.Fatalf("%q: accepted", table)
		}
		if len(q.sqls) != 0 {
			t.Fatalf("%q: read before validating: %v", table, q.sqls)
		}
	}
}

func TestTableReplicationMissingTableIsNotFound(t *testing.T) {
	q := &settingsQuerier{queryRows: [][]any{
		{int64(1), true, "d", true},
		{int64(2), false, "", false},
	}}
	_, err := testDB(q).TableReplication(context.Background(), []string{"public.orders", "public.gone"})
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "public.gone") {
		t.Fatalf("error does not name the table: %v", err)
	}
}

func TestTableReplicationRowForUnknownTableIsRefused(t *testing.T) {
	q := &settingsQuerier{queryRows: [][]any{{int64(9), true, "d", true}}}
	if _, err := testDB(q).TableReplication(context.Background(), []string{"public.orders"}); err == nil {
		t.Fatal("a row for a table that was not asked for was accepted")
	}
}

func TestRoleInfoExistingRole(t *testing.T) {
	q := &settingsQuerier{
		rowScan: func(dest ...any) error {
			*dest[0].(*bool) = true  // rolcanlogin
			*dest[1].(*bool) = false // rolreplication
			return nil
		},
		queryRows: [][]any{
			{int64(1), true},
			{int64(2), false},
		},
	}
	info, err := testDB(q).RoleInfo(context.Background(), "piperoom", []string{"public.orders", "public.payments"})
	if err != nil {
		t.Fatalf("RoleInfo: %v", err)
	}
	if !info.Exists || !info.CanLogin || info.Replication {
		t.Fatalf("info = %+v", info)
	}
	if len(info.Selectable) != 2 || !info.Selectable["public.orders"] || info.Selectable["public.payments"] {
		t.Fatalf("selectable = %v", info.Selectable)
	}
	if !q.deadline || len(q.sqls) != 2 {
		t.Fatalf("statements = %d, deadline = %v", len(q.sqls), q.deadline)
	}
	if !strings.Contains(q.sqls[0], "pg_roles") || !strings.Contains(q.sqls[1], "has_table_privilege") {
		t.Fatalf("SQL = %q / %q", q.sqls[0], q.sqls[1])
	}
	if q.args[1][0] != "piperoom" {
		t.Fatalf("role argument = %v", q.args[1])
	}
}

func TestRoleInfoMissingRoleIsNotAnError(t *testing.T) {
	q := &settingsQuerier{rowScan: func(...any) error { return pgx.ErrNoRows }}
	info, err := testDB(q).RoleInfo(context.Background(), "gone", []string{"public.orders"})
	if err != nil {
		t.Fatalf("RoleInfo: %v", err)
	}
	if info.Exists || info.CanLogin || info.Replication {
		t.Fatalf("info = %+v", info)
	}
	if got, ok := info.Selectable["public.orders"]; !ok || got {
		t.Fatalf("selectable = %v", info.Selectable)
	}
	if len(q.sqls) != 1 {
		t.Fatalf("a missing role still read privileges: %v", q.sqls)
	}
}

func TestRoleInfoRefusesBadInputBeforeIO(t *testing.T) {
	q := &settingsQuerier{}
	if _, err := testDB(q).RoleInfo(context.Background(), "", []string{"public.orders"}); err == nil {
		t.Fatal("an empty role was accepted")
	}
	if _, err := testDB(q).RoleInfo(context.Background(), "r", []string{"public.or-ders"}); err == nil {
		t.Fatal("a bad table identifier was accepted")
	}
	if len(q.sqls) != 0 {
		t.Fatalf("read before validating: %v", q.sqls)
	}
}

func TestBlockersRedactAndCutQuery(t *testing.T) {
	xact := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	started := xact.Add(-time.Hour)
	secret := "Sup3rS3cretPw"
	long := "update orders set note='password=" + secret + "' where id=7 " + strings.Repeat("x", 400)
	q := &settingsQuerier{queryRows: [][]any{
		{int64(41), started, xact, "app", "orders-api", "idle in transaction", long, true},
	}}
	got, err := testDB(q).Blockers(context.Background(), 5*time.Minute)
	if err != nil {
		t.Fatalf("Blockers: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d sessions, want 1", len(got))
	}
	s := got[0]
	if s.PID != 41 || s.User != "app" || s.App != "orders-api" || s.State != "idle in transaction" || !s.BlocksSlot {
		t.Fatalf("session = %+v", s)
	}
	if !s.StartedAt.Equal(started) || !s.XactStart.Equal(xact) {
		t.Fatalf("times = %v / %v", s.StartedAt, s.XactStart)
	}
	if strings.Contains(s.Query, secret) {
		t.Fatalf("secret not redacted: %q", s.Query)
	}
	if len(s.Query) > MaxSessionQueryBytes {
		t.Fatalf("query is %d bytes, cap %d", len(s.Query), MaxSessionQueryBytes)
	}
	if !strings.HasSuffix(s.Query, "…") {
		t.Fatalf("cut query has no marker: %q", s.Query)
	}
	if !q.deadline || len(q.args) != 1 || q.args[0][0] != 300.0 {
		t.Fatalf("deadline = %v, args = %v", q.deadline, q.args)
	}
	for _, want := range []string{"pg_stat_activity", "backend_xmin", "make_interval", "walsender"} {
		if !strings.Contains(q.sqls[0], want) {
			t.Fatalf("SQL misses %q: %q", want, q.sqls[0])
		}
	}
}

// xid has no min() aggregate and no ordering operators in any Postgres
// version: the horizon must be computed via age(backend_xmin), or every
// Blockers read fails on a real server with "function min(xid) does not
// exist" (regression: the first version used min() and <= on xid).
func TestBlockersSQLComparesXidByAge(t *testing.T) {
	if !strings.Contains(blockersSQL, "age(backend_xmin)") {
		t.Fatalf("blockersSQL does not use age(backend_xmin):\n%s", blockersSQL)
	}
	for _, bad := range []string{"min(backend_xmin)", "backend_xmin <=", "backend_xmin >=", "backend_xmin <", "backend_xmin >"} {
		if strings.Contains(blockersSQL, bad) {
			t.Fatalf("blockersSQL compares xid with %q, which Postgres has no operator for:\n%s", bad, blockersSQL)
		}
	}
}

func TestBlockersRefuseNegativeAge(t *testing.T) {
	q := &settingsQuerier{}
	if _, err := testDB(q).Blockers(context.Background(), -time.Second); err == nil {
		t.Fatal("a negative age was accepted")
	}
	if len(q.sqls) != 0 {
		t.Fatalf("read before validating: %v", q.sqls)
	}
}

func TestRedactSessionQuery(t *testing.T) {
	long := strings.Repeat("é", 200)
	cases := []struct {
		name string
		in   string
		want func(string) bool
	}{
		{"short untouched", "select 1", func(s string) bool { return s == "select 1" }},
		{"secret masked", "password=Sup3rS3cretPw", func(s string) bool { return !strings.Contains(s, "Sup3rS3cretPw") }},
		{"long cut", long, func(s string) bool {
			return len(s) <= MaxSessionQueryBytes && strings.HasSuffix(s, "…") && utf8.ValidString(s)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RedactSessionQuery(tc.in)
			if !tc.want(got) {
				t.Fatalf("RedactSessionQuery(%q) = %q", tc.in, got)
			}
		})
	}
}
