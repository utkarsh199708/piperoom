package owners_test

import (
	"fmt"
	"strings"
	"testing"

	"piperoom/internal/topology"
)

const retentionHead = "pipelines:\n  - id: orders\n    topic_prefix: pg\n    source_tables: [public.orders]\n" +
	"    connectors: [c1]\n    sink_tables: [analytics.orders]\n    sink_ts_column: updated_at\n"

// Load accepts a declared_retention value exactly when topology.ParseRetention
// does: owners cannot import topology, so this pins the copy of the grammar.
func TestDeclaredRetentionMatchesParseRetention(t *testing.T) {
	for _, v := range []string{"7d", "36h", "90m", "45s", "1s", "infinite", "36500d", "36501d", "876000h", "876001h",
		"0d", "-1d", "7", "d", "7w", "7D", "1.5d", " 7d", "Infinite", "", "99999999999999999999d", "+7d"} {
		doc := retentionHead + fmt.Sprintf("    declared_retention:\n      topics:\n        pg.public.orders: %q\n", v)
		_, err := loadOwners(t, doc)
		_, _, perr := topology.ParseRetention(v)
		if (err == nil) != (perr == nil) {
			t.Errorf("%q: owners err = %v, topology err = %v", v, err, perr)
		}
	}
}

func TestDeclaredRetentionLoads(t *testing.T) {
	o, err := loadOwners(t, retentionHead+"    declared_retention:\n      topics:\n        pg.public.orders: 7d\n"+
		"      tables:\n        analytics.orders: 400d\n        public.orders: infinite\n")
	if err != nil {
		t.Fatal(err)
	}
	p, _ := o.Pipeline("orders")
	if v, ok := p.DeclaredTopicRetention("pg.public.orders"); !ok || v != "7d" {
		t.Fatalf("topic = %q %v", v, ok)
	}
	if v, ok := p.DeclaredTableRetention("analytics.orders"); !ok || v != "400d" {
		t.Fatalf("table = %q %v", v, ok)
	}
	if _, ok := p.DeclaredTopicRetention("other"); ok {
		t.Fatal("undeclared topic found")
	}
	o, err = loadOwners(t, retentionHead)
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := o.Pipeline("orders"); p.DeclaredRetention != nil {
		t.Fatal("no block should leave it nil")
	}
	if _, ok := p.DeclaredTableRetention("analytics.orders"); !ok {
		t.Fatal("lost the declaration")
	}
}

func TestDeclaredRetentionRefusals(t *testing.T) {
	var many strings.Builder
	for i := range 201 {
		fmt.Fprintf(&many, "        pg.public.t%d: 7d\n", i)
	}
	for name, tc := range map[string]struct{ block, want string }{
		"bad topic name":  {"      topics:\n        \"pg public\": 7d\n", "not a valid Kafka topic name"},
		"bad value":       {"      topics:\n        pg.public.orders: 7w\n", "pg.public.orders"},
		"unknown table":   {"      tables:\n        analytics.other: 7d\n", "analytics.other"},
		"bad table value": {"      tables:\n        analytics.orders: 0d\n", "analytics.orders"},
		"too many":        {"      topics:\n" + many.String(), "at most 200"},
		"unknown key":     {"      views:\n        x: 7d\n", "views"},
	} {
		_, err := loadOwners(t, retentionHead+"    declared_retention:\n"+tc.block)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want it to name %q", name, err, tc.want)
		}
	}
}

// A pipeline that declares its sinks in a sinks: block (no sink_tables:)
// can declare its sink tables' retention too: validation runs after the
// block filled SinkTables, and the watcher judges the same SinkRefs tables.
func TestDeclaredRetentionSinksBlock(t *testing.T) {
	doc := "pipelines:\n  - id: orders\n    topic_prefix: pg\n    source_tables: [public.orders]\n" +
		"    connectors: [c1]\n    sinks:\n      - table: analytics.orders\n        warehouse: Postgres\n" +
		"        writer: {kind: consumer_group, name: order-sync}\n    sink_ts_column: updated_at\n" +
		"    declared_retention:\n      tables:\n        analytics.orders: 30d\n"
	o, err := loadOwners(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := o.Pipeline("orders")
	if v, ok := p.DeclaredTableRetention("analytics.orders"); !ok || v != "30d" {
		t.Fatalf("table = %q %v", v, ok)
	}
	if _, err := loadOwners(t, strings.Replace(doc, "        analytics.orders: 30d", "        analytics.other: 30d", 1)); err == nil {
		t.Fatal("a table outside the sinks: block loaded")
	}
}
