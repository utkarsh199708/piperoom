package discover

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

func mysqlConfig(kv ...string) map[string]string {
	c := map[string]string{"connector.class": mysqlSourceClass}
	for i := 0; i+1 < len(kv); i += 2 {
		c[kv[i]] = kv[i+1]
	}
	return c
}

// A Debezium MySQL connector becomes a source_kind: mysql pipeline. Its
// source_tables come from the literal table.include.list entries, or, without
// one, from the prefix's topics filtered by database.include.list /
// database.exclude.list (Q8h, docs/design/mysql-source.md §10.3).
func TestMySQLSourceTables(t *testing.T) {
	topics := []string{"shop.shop.orders", "shop.shop.order_items", "shop.audit.log", "shop.tmp.x", "shop", "schema-history.shop"}
	cases := []struct {
		name      string
		cfg       map[string]string
		want      []string
		wantNotes []string
	}{
		{"table include list", mysqlConfig("topic.prefix", "shop", "table.include.list", "shop.orders,shop.order_items"),
			[]string{"shop.order_items", "shop.orders"}, nil},
		{"table include list, no topic yet", mysqlConfig("topic.prefix", "shop", "table.include.list", "shop.orders, shop.refunds"),
			[]string{"shop.orders", "shop.refunds"}, []string{"no topic yet for shop.refunds"}},
		{"database include list", mysqlConfig("topic.prefix", "shop", "database.include.list", "shop"),
			[]string{"shop.order_items", "shop.orders"}, []string{"guessed from existing topics"}},
		{"database include list, two databases", mysqlConfig("topic.prefix", "shop", "database.include.list", "shop,audit"),
			[]string{"audit.log", "shop.order_items", "shop.orders"}, nil},
		{"database exclude list", mysqlConfig("topic.prefix", "shop", "database.exclude.list", "tmp,audit"),
			[]string{"shop.order_items", "shop.orders"}, nil},
		// schema.include.list is a Postgres key; a MySQL connector ignores it.
		{"schema include list ignored", mysqlConfig("topic.prefix", "shop", "schema.include.list", "tmp"),
			[]string{"audit.log", "shop.order_items", "shop.orders", "tmp.x"}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := one(t, run(t, single(c.cfg, topics...), Options{}))
			if p.SourceKind != sourceKindMySQL {
				t.Errorf("SourceKind = %q, want mysql", p.SourceKind)
			}
			if p.TopicPrefix != "shop" {
				t.Errorf("TopicPrefix = %q", p.TopicPrefix)
			}
			if !slices.Equal(p.SourceTables, c.want) {
				t.Errorf("source_tables = %v, want %v", p.SourceTables, c.want)
			}
			for _, n := range c.wantNotes {
				if !hasNote(p, n) {
					t.Errorf("missing note %q in %v", n, p.Notes)
				}
			}
		})
	}
}

// A MySQL pipeline has no replication slot: no slot note, and with only MySQL
// pipelines the slots are not listed at all (a failing source is never read).
// A cluster with both a Postgres and a MySQL connector yields a file the
// server refuses as written (one client set serves one source kind): the run
// warns, naming the connections: block each MySQL pipeline needs, and the
// warning is in the rendered file (PR #934 review, minor 6). One kind alone
// gets no such warning.
func TestMixedSourceKindsWarn(t *testing.T) {
	const want = "one server client set serves one source kind"
	d := single(mysqlConfig("topic.prefix", "shop", "table.include.list", "shop.orders"), "shop.shop.orders")
	d.Connect.(*fakeConnect).configs["pg-orders-connector"] = pgConfig("topic.prefix", "pg", "table.include.list", "public.orders")
	res := run(t, d, Options{})
	var w string
	for _, s := range res.Warnings {
		if strings.Contains(s, want) {
			w = s
		}
	}
	if w == "" {
		t.Fatalf("no mixed-kind warning in %q", res.Warnings)
	}
	for _, part := range []string{"pg-orders-connector", "(c)", "connections: block", "source_db_url", "mysql://", "one Piperoom deployment per source kind"} {
		if !strings.Contains(w, part) {
			t.Errorf("warning lacks %q: %s", part, w)
		}
	}
	for name, render := range map[string]func(*bytes.Buffer) error{
		"draft": func(b *bytes.Buffer) error { return Render(b, res) },
		"owned": func(b *bytes.Buffer) error { return RenderOwned(b, res, "alice") },
	} {
		var b bytes.Buffer
		if err := render(&b); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(b.String(), want) {
			t.Errorf("%s file lacks the warning:\n%s", name, b.String())
		}
	}

	for name, d := range map[string]Deps{
		"mysql only":    single(mysqlConfig("topic.prefix", "shop", "table.include.list", "shop.orders"), "shop.shop.orders"),
		"postgres only": single(pgConfig("topic.prefix", "pg", "table.include.list", "public.orders"), "pg.public.orders"),
	} {
		for _, s := range run(t, d, Options{}).Warnings {
			if strings.Contains(s, want) {
				t.Errorf("%s: mixed-kind warning: %s", name, s)
			}
		}
	}
}

func TestMySQLNoSlotCheck(t *testing.T) {
	d := single(mysqlConfig("topic.prefix", "shop", "table.include.list", "shop.orders"), "shop.shop.orders")
	d.Source = &fakeSource{err: errBoom}
	r := run(t, d, Options{})
	p := one(t, r)
	if hasNote(p, "slot") || len(r.Warnings) != 0 {
		t.Fatalf("slot checked for a MySQL pipeline: notes %v, warnings %v", p.Notes, r.Warnings)
	}
	// Next to a Postgres pipeline the slot check still runs, for it only.
	d.Connect.(*fakeConnect).configs["pg"] = pgConfig("topic.prefix", "pg", "table.include.list", "public.orders")
	d.Source = &fakeSource{slots: []clients.ReplicationSlot{{Name: "other", Active: true}}}
	r = run(t, d, Options{})
	if len(r.Pipelines) != 2 {
		t.Fatalf("pipelines = %+v", r.Pipelines)
	}
	for _, p := range r.Pipelines {
		if got := hasNote(p, "slot debezium not found"); got != (p.SourceKind == "") {
			t.Errorf("%s (%q): slot note %v, notes %v", p.ID, p.SourceKind, got, p.Notes)
		}
	}
}

// The owned file of a MySQL pipeline loads with owners.Load as a mysql
// pipeline whose mysql layer is owned by the owner, and the users file
// written with it still passes room.ValidateUsers.
func TestRenderOwnedMySQLLoads(t *testing.T) {
	d := single(mysqlConfig("topic.prefix", "shop", "table.include.list", "shop.orders,shop.order_items"), "shop.shop.orders")
	d.Connect.(*fakeConnect).configs["pg-orders-connector"] = pgConfig("topic.prefix", "pg", "table.include.list", "public.orders")
	res := run(t, d, Options{})
	out := renderOwned(t, res, "alice")
	if !strings.Contains(string(out), "source_kind: mysql") {
		t.Fatalf("no source_kind in\n%s", out)
	}
	own, err := owners.Load(writeTemp(t, "owners.yaml", out))
	if err != nil {
		t.Fatalf("owners.Load: %v\n%s", err, out)
	}
	p, ok := own.Pipeline("c")
	if !ok {
		t.Fatalf("no pipeline c in %+v", own.Pipelines)
	}
	if !p.IsMySQL() || p.SourceLayer() != owners.LayerMySQL || !slices.Equal(p.SourceTables, []string{"shop.order_items", "shop.orders"}) {
		t.Fatalf("pipeline c = %+v", p)
	}
	if g, ok := own.CanApprove("c", owners.LayerMySQL, "alice", nil); !ok || g == "" {
		t.Fatalf("alice cannot approve the mysql layer of c (%q, %v)\n%s", g, ok, out)
	}
	pg, ok := own.Pipeline("pg-orders")
	if !ok || pg.IsMySQL() || pg.SourceKind != "" {
		t.Fatalf("pg-orders = %+v, %v", pg, ok)
	}

	var ub bytes.Buffer
	if err := RenderUsers(&ub, "alice"); err != nil {
		t.Fatal(err)
	}
	users, err := room.LoadUsers(writeTemp(t, "users.yaml", ub.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if err := room.ValidateUsers(users, own); err != nil {
		t.Fatalf("room.ValidateUsers: %v", err)
	}

	// The draft form names the source kind and asks for the mysql owner.
	var draft bytes.Buffer
	if err := Render(&draft, res); err != nil {
		t.Fatal(err)
	}
	if s := draft.String(); !strings.Contains(s, "source_kind: mysql") || !strings.Contains(s, "mysql: {approvers: TODO}") {
		t.Fatalf("draft lacks source_kind or the mysql owner:\n%s", s)
	}
}
