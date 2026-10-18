//go:build integration

package pgactions

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/testenv"
	"piperoom/internal/tools"
)

// pg_drop_replication_slot against the dev stack, on an it_<random> slot. The
// writer connects as the dev SOURCE_DB_URL role, which may drop slots there
// (production gives it SOURCE_DB_WRITE_URL). Slots are server-wide and the T2
// tests of internal/clients/postgres count them, so this test holds the same
// advisory lock (scopeTestLock there).
const scopeTestLock = 74020001

type itFixture struct {
	a     *dropSlot
	admin *pgx.Conn
	dsn   string
	slot  string
	pub   string
	conn  string
	room  context.Context
}

func newITFixture(t *testing.T) itFixture {
	t.Helper()
	dsn := testenv.Require(t).SourceDBURL
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Skipf("postgres not reachable: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	if _, err := admin.Exec(ctx, "SELECT pg_advisory_lock($1)", int64(scopeTestLock)); err != nil {
		t.Fatalf("scope test lock: %v", err)
	}

	name := testenv.Name("dropslot")
	f := itFixture{admin: admin, dsn: dsn, slot: name, pub: name + "_pub", conn: name + "_connector"}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active", f.slot)
		_, _ = admin.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{f.pub}.Sanitize())
	})
	if _, err := admin.Exec(ctx, "CREATE PUBLICATION "+pgx.Identifier{f.pub}.Sanitize()); err != nil {
		t.Fatalf("create publication: %v", err)
	}
	if _, err := admin.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", f.slot); err != nil {
		t.Fatalf("create slot: %v", err)
	}

	src, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatalf("source reader: %v", err)
	}
	t.Cleanup(src.Close)
	w, err := postgres.NewWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	t.Cleanup(w.Close)
	cn := &fakeConnect{cfg: map[string]map[string]string{
		f.conn: {"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "slot.name": f.slot},
	}}
	f.a = &dropSlot{d: DropSlotDeps{Source: src, Connect: cn, Writer: w}}
	f.room = tools.WithPipeline(context.Background(), owners.Pipeline{ID: "it", TopicPrefix: "it", Connectors: []string{f.conn}})
	return f
}

func (f itFixture) raw(t *testing.T) []byte {
	return params(t, map[string]any{"connector": f.conn, "slot": f.slot})
}

func (f itFixture) exists(t *testing.T) bool {
	t.Helper()
	var n int
	if err := f.admin.QueryRow(context.Background(), "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", f.slot).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

// attach makes the slot active: a walsender streams from it until the
// returned func closes the replication connection.
func (f itFixture) attach(t *testing.T) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg, err := pgconn.ParseConfig(f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.RuntimeParams["replication"] = "database"
	rc, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("replication connection: %v", err)
	}
	fe := rc.Frontend()
	fe.Send(&pgproto3.Query{String: "START_REPLICATION SLOT " + pgx.Identifier{f.slot}.Sanitize() +
		" LOGICAL 0/0 (proto_version '1', publication_names " + quoteLiteral(f.pub) + ")"})
	if err := fe.Flush(); err != nil {
		t.Fatalf("start replication: %v", err)
	}
	for {
		msg, err := rc.ReceiveMessage(ctx)
		if err != nil {
			t.Fatalf("start replication: %v", err)
		}
		if e, ok := msg.(*pgproto3.ErrorResponse); ok {
			t.Fatalf("start replication: %s", e.Message)
		}
		if _, ok := msg.(*pgproto3.CopyBothResponse); ok {
			break
		}
	}
	return func() { _ = rc.Close(context.Background()) }
}

// quoteLiteral quotes a test-generated name (it_ + hex) as a SQL literal.
func quoteLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func TestIntegrationDropInactiveSlot(t *testing.T) {
	f := newITFixture(t)
	if err := f.a.Validate(context.Background(), f.raw(t)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	pv, err := f.a.Preview(f.room, f.raw(t))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !strings.Contains(pv.Current, "pgoutput") || !strings.Contains(pv.Current, "inactive") || pv.Reversible {
		t.Fatalf("preview: %+v", pv)
	}
	if !f.exists(t) {
		t.Fatal("Preview dropped the slot")
	}
	out, err := f.a.Execute(f.room, f.raw(t))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(out, f.slot) || f.exists(t) {
		t.Fatalf("slot still there after %q", out)
	}
	if _, err := f.a.Execute(f.room, f.raw(t)); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("second drop: %v", err)
	}
}

func TestIntegrationActiveSlotRefused(t *testing.T) {
	f := newITFixture(t)
	detach := f.attach(t)
	defer detach()
	testenv.WaitFor(t, 10*time.Second, func() (bool, error) {
		var active bool
		err := f.admin.QueryRow(context.Background(), "SELECT active FROM pg_replication_slots WHERE slot_name = $1", f.slot).Scan(&active)
		return active, err
	})
	if err := f.a.Validate(context.Background(), f.raw(t)); !errors.Is(err, clients.ErrSlotActive) {
		t.Fatalf("Validate of an active slot: %v", err)
	}
	if _, err := f.a.Execute(f.room, f.raw(t)); !errors.Is(err, clients.ErrSlotActive) {
		t.Fatalf("Execute of an active slot: %v", err)
	}
	if !f.exists(t) {
		t.Fatal("an active slot was dropped")
	}
}

func TestIntegrationOtherSlotRefused(t *testing.T) {
	f := newITFixture(t)
	raw := params(t, map[string]any{"connector": f.conn, "slot": f.slot + "_other"})
	if err := f.a.Validate(context.Background(), raw); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("another slot: %v", err)
	}
	if !f.exists(t) {
		t.Fatal("slot gone")
	}
}
