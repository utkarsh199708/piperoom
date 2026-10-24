package discover

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

var update = flag.Bool("update", false, "rewrite testdata/draft.golden.yaml")

func run(t *testing.T, d Deps, o Options) Result {
	t.Helper()
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	r, err := Discover(context.Background(), d, o)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	return r
}

func one(t *testing.T, r Result) Pipeline {
	t.Helper()
	if len(r.Pipelines) != 1 {
		t.Fatalf("pipelines = %+v, want 1", r.Pipelines)
	}
	return r.Pipelines[0]
}

func hasNote(p Pipeline, sub string) bool {
	return slices.ContainsFunc(p.Notes, func(n string) bool { return strings.Contains(n, sub) })
}

func single(cfg map[string]string, topics ...string) Deps {
	return Deps{
		Connect:  &fakeConnect{configs: map[string]map[string]string{"c": cfg}},
		Kafka:    &fakeKafka{topics: topics},
		Registry: &fakeRegistry{},
	}
}

func TestPrefix(t *testing.T) {
	p := one(t, run(t, single(pgConfig("topic.prefix", "pg"), "pg.public.a"), Options{}))
	if p.TopicPrefix != "pg" || hasNote(p, "database.server.name") {
		t.Errorf("topic.prefix: %+v", p)
	}
	p = one(t, run(t, single(pgConfig("database.server.name", "old"), "old.public.a"), Options{}))
	if p.TopicPrefix != "old" || !hasNote(p, "database.server.name") {
		t.Errorf("database.server.name: %+v", p)
	}
	r := run(t, single(pgConfig()), Options{})
	if len(r.Pipelines) != 0 || len(r.Skipped) != 1 || !strings.Contains(r.Skipped[0].Reason, "topic.prefix") {
		t.Errorf("no prefix: %+v", r)
	}
}

func TestClassFilter(t *testing.T) {
	d := Deps{
		Connect: &fakeConnect{configs: map[string]map[string]string{
			"pg":    pgConfig("topic.prefix", "pg"),
			"mysql": {"connector.class": "io.debezium.connector.mysql.MySqlConnector", "topic.prefix": "shop"},
			"sink":  {"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector"},
			"bare":  {},
		}},
		Kafka: &fakeKafka{}, Registry: &fakeRegistry{},
	}
	r := run(t, d, Options{})
	if len(r.Pipelines) != 2 || r.Pipelines[0].Connector != "mysql" || r.Pipelines[0].SourceKind != "mysql" || r.Pipelines[1].Connector != "pg" || r.Pipelines[1].SourceKind != "" {
		t.Fatalf("pipelines = %+v", r.Pipelines)
	}
	want := []Skipped{
		{"bare", "not a Debezium Postgres or MySQL source (no connector.class)"},
		{"sink", "not a Debezium Postgres or MySQL source (class io.confluent.connect.jdbc.JdbcSinkConnector)"},
	}
	if !slices.Equal(r.Skipped, want) {
		t.Errorf("skipped = %+v", r.Skipped)
	}
}

func TestSourceTables(t *testing.T) {
	topics := []string{"pg.public.orders", "pg.public.order_items", "pg.public.tmp_x", "pg.audit.log", "pg.transaction", "pg.eu.public.x", "pgx.public.y"}
	cases := []struct {
		name      string
		cfg       map[string]string
		want      []string
		wantNotes []string
	}{
		{"literal", pgConfig("topic.prefix", "pg", "table.include.list", "public.orders"), []string{"public.orders"}, nil},
		{"literal no topic yet", pgConfig("topic.prefix", "pg", "table.include.list", "public.orders, public.refunds"), []string{"public.orders", "public.refunds"}, []string{"no topic yet for public.refunds"}},
		{"regex", pgConfig("topic.prefix", "pg", "table.include.list", `public\.order.*`), []string{"public.order_items", "public.orders"}, nil},
		{"case-insensitive like Debezium", pgConfig("topic.prefix", "pg", "table.include.list", "Public.Orders"), []string{"public.orders"}, nil},
		{"not RE2", pgConfig("topic.prefix", "pg", "table.include.list", `public\.(?!tmp).*`), nil, []string{`include entry "public\\.(?!tmp).*" is not RE2`, "TODO: no source tables"}},
		{"guessed", pgConfig("topic.prefix", "pg"), []string{"audit.log", "public.order_items", "public.orders", "public.tmp_x"}, []string{"guessed from existing topics"}},
		{"guessed with excludes", pgConfig("topic.prefix", "pg", "table.exclude.list", `public\.tmp_.*`, "schema.include.list", "public"), []string{"public.order_items", "public.orders"}, nil},
		{"schema exclude", pgConfig("topic.prefix", "pg", "schema.exclude.list", "audit"), []string{"public.order_items", "public.orders", "public.tmp_x"}, nil},
		{"nothing", pgConfig("topic.prefix", "none"), nil, []string{"TODO: no source tables"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := one(t, run(t, single(c.cfg, topics...), Options{}))
			if !slices.Equal(p.SourceTables, c.want) {
				t.Errorf("source_tables = %v, want %v", p.SourceTables, c.want)
			}
			for _, n := range c.wantNotes {
				if !hasNote(p, n) {
					t.Errorf("missing note %q in %v", n, p.Notes)
				}
			}
			for _, bad := range []string{"transaction", "eu.public.x"} {
				if slices.Contains(p.SourceTables, bad) {
					t.Errorf("%s taken as a table", bad)
				}
			}
		})
	}
}

func TestNamingOverrideNamesKeysOnly(t *testing.T) {
	cfg := pgConfig("topic.prefix", "pg", "transforms", "secretRoute", "topic.delimiter", "_", "topic.naming.strategy", "com.example.Custom")
	p := one(t, run(t, single(cfg, "pg.public.a"), Options{}))
	if !hasNote(p, "custom topic naming (transforms, topic.delimiter, topic.naming.strategy)") {
		t.Errorf("notes = %v", p.Notes)
	}
	for _, n := range p.Notes {
		if strings.Contains(n, "secretRoute") || strings.Contains(n, "com.example") {
			t.Errorf("note leaks a value: %q", n)
		}
	}
	plain := pgConfig("topic.prefix", "pg", "topic.delimiter", ".", "topic.naming.strategy", "io.debezium.schema.SchemaTopicNamingStrategy")
	if p := one(t, run(t, single(plain, "pg.public.a"), Options{})); hasNote(p, "custom topic naming") {
		t.Errorf("defaults flagged: %v", p.Notes)
	}
}

func groupDeps(groups map[string]clients.GroupLag, errs map[string]error) (Deps, *fakeKafka) {
	k := &fakeKafka{topics: []string{"pg.public.orders", "other.public.x"}, groups: groups, groupErrs: errs}
	return Deps{
		Connect:  &fakeConnect{configs: map[string]map[string]string{"c": pgConfig("topic.prefix", "pg")}},
		Kafka:    k,
		Registry: &fakeRegistry{},
	}, k
}

func TestConsumerGroups(t *testing.T) {
	groups := map[string]clients.GroupLag{
		"yes":     {State: "Stable", Partitions: []clients.PartitionLag{committed("pg.public.orders")}},
		"retired": {State: "Empty", Partitions: []clients.PartitionLag{committed("pg.public.orders")}},
		"never":   {State: "Stable", Partitions: []clients.PartitionLag{{Topic: "pg.public.orders", Committed: -1}}},
		"other":   {State: "Stable", Partitions: []clients.PartitionLag{committed("other.public.x")}},
	}
	d, k := groupDeps(groups, map[string]error{"gone": clients.ErrNotFound, "broken": errBoom})
	r := run(t, d, Options{})
	p := one(t, r)
	if !slices.Equal(p.ConsumerGroups, []string{"retired", "yes"}) {
		t.Errorf("groups = %v", p.ConsumerGroups)
	}
	if !hasNote(p, "group retired state Empty at discovery") {
		t.Errorf("notes = %v", p.Notes)
	}
	if !slices.Equal(r.Warnings, []string{"consumer group broken: lag could not be read"}) {
		t.Errorf("warnings = %v (gone must be skipped quietly)", r.Warnings)
	}
	for g, n := range k.lagCalls {
		if n != 1 {
			t.Errorf("GroupLag(%s) called %d times", g, n)
		}
	}
}

func TestMaxGroups(t *testing.T) {
	groups := map[string]clients.GroupLag{
		"a": {Partitions: []clients.PartitionLag{committed("pg.public.orders")}},
		"b": {Partitions: []clients.PartitionLag{committed("pg.public.orders")}},
		"c": {Partitions: []clients.PartitionLag{committed("pg.public.orders")}},
	}
	d, k := groupDeps(groups, nil)
	r := run(t, d, Options{MaxGroups: 2})
	if p := one(t, r); !slices.Equal(p.ConsumerGroups, []string{"a", "b"}) {
		t.Errorf("groups = %v", p.ConsumerGroups)
	}
	if k.lagCalls["c"] != 0 || !slices.Equal(r.Warnings, []string{"1 consumer groups not checked (more than 2)"}) {
		t.Errorf("warnings = %v calls = %v", r.Warnings, k.lagCalls)
	}
}

func TestRegistryAndSlots(t *testing.T) {
	base := func() Deps {
		d := single(pgConfig("topic.prefix", "pg", "slot.name", "orders_slot"), "pg.public.orders", "pg.public.items")
		d.Registry = &fakeRegistry{subjects: []string{"pg.public.orders-value"}}
		return d
	}
	p := one(t, run(t, base(), Options{}))
	if !hasNote(p, "no subject pg.public.items-value") || hasNote(p, "pg.public.orders-value") {
		t.Errorf("subject notes = %v", p.Notes)
	}
	if hasNote(p, "slot") {
		t.Errorf("Source == nil must give no slot notes: %v", p.Notes)
	}

	d := base()
	d.Registry = &fakeRegistry{err: errBoom}
	r := run(t, d, Options{})
	if len(r.Pipelines) != 1 || !slices.Equal(r.Warnings, []string{"schema registry subjects could not be listed: subject checks skipped"}) {
		t.Errorf("subjects error: %+v", r)
	}

	for _, c := range []struct {
		slots []clients.ReplicationSlot
		note  string
	}{
		{[]clients.ReplicationSlot{{Name: "orders_slot", Active: true}}, ""},
		{[]clients.ReplicationSlot{{Name: "debezium", Active: true}}, "slot orders_slot not found in SOURCE_DB_URL"},
		{[]clients.ReplicationSlot{{Name: "orders_slot", Active: false}}, "slot orders_slot inactive"},
	} {
		d := base()
		d.Source = &fakeSource{slots: c.slots}
		p := one(t, run(t, d, Options{}))
		if c.note == "" && hasNote(p, "slot") || c.note != "" && !hasNote(p, c.note) {
			t.Errorf("slots %+v: notes = %v", c.slots, p.Notes)
		}
	}
}

func TestErrors(t *testing.T) {
	d := happyDeps()
	d.Connect = &fakeConnect{listErr: errBoom}
	if _, err := Discover(context.Background(), d, Options{}); !errors.Is(err, errBoom) || !strings.Contains(err.Error(), "list connectors") {
		t.Errorf("list connectors: %v", err)
	}
	d = happyDeps()
	d.Kafka = &fakeKafka{topicsErr: errBoom}
	if _, err := Discover(context.Background(), d, Options{}); !errors.Is(err, errBoom) || !strings.Contains(err.Error(), "list topics") {
		t.Errorf("list topics: %v", err)
	}

	d = happyDeps()
	d.Connect = &fakeConnect{
		configs: map[string]map[string]string{"pg": pgConfig("topic.prefix", "pg")},
		errs:    map[string]error{"gone": clients.ErrNotFound, "bad": errBoom},
	}
	r := run(t, d, Options{})
	want := []Skipped{{"bad", "config read failed"}, {"gone", "deleted during discovery"}}
	if !slices.Equal(r.Skipped, want) || len(r.Pipelines) != 1 {
		t.Errorf("skipped = %+v", r.Skipped)
	}
}

func TestCallTimeoutDoesNotHang(t *testing.T) {
	d := happyDeps()
	d.Connect = &fakeConnect{configs: map[string]map[string]string{"pg": pgConfig("topic.prefix", "pg")}, blocking: true}
	done := make(chan Result, 1)
	go func() {
		r, _ := Discover(context.Background(), d, Options{CallTimeout: 50 * time.Millisecond, Logger: slog.New(slog.DiscardHandler)})
		done <- r
	}()
	select {
	case r := <-done:
		if len(r.Skipped) != 1 || r.Skipped[0].Reason != "config read failed" {
			t.Errorf("blocked Config: %+v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Discover hung on a blocking client")
	}
}

func TestIDs(t *testing.T) {
	for in, want := range map[string]string{
		"pg-orders-connector": "pg-orders",
		"Legacy Billing":      "legacy-billing",
		"--X__Y--":            "x-y",
		"###":                 "pipeline",
		"connector":           "pipeline",
	} {
		if got := slug(in); got != want {
			t.Errorf("slug(%q) = %q, want %q", in, got, want)
		}
	}
	d := Deps{
		Connect: &fakeConnect{configs: map[string]map[string]string{
			"a.b": pgConfig("topic.prefix", "p1"), "a_b": pgConfig("topic.prefix", "p2"), "a-b": pgConfig("topic.prefix", "p3"),
		}},
		Kafka: &fakeKafka{}, Registry: &fakeRegistry{},
	}
	r := run(t, d, Options{})
	var got []string
	for _, p := range r.Pipelines {
		got = append(got, p.ID+"="+p.Connector)
	}
	if !slices.Equal(got, []string{"a-b=a-b", "a-b-2=a.b", "a-b-3=a_b"}) {
		t.Errorf("ids = %v", got)
	}
}

func TestSharedPrefixWarning(t *testing.T) {
	d := Deps{
		Connect: &fakeConnect{configs: map[string]map[string]string{"x": pgConfig("topic.prefix", "pg"), "y": pgConfig("topic.prefix", "pg")}},
		Kafka:   &fakeKafka{}, Registry: &fakeRegistry{},
	}
	if r := run(t, d, Options{}); !slices.Contains(r.Warnings, "connectors x, y share topic prefix pg") {
		t.Errorf("warnings = %v", r.Warnings)
	}
}

func render(t *testing.T, r Result) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := Render(&b, r); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestGoldenAndDeterminism(t *testing.T) {
	out := render(t, run(t, happyDeps(), Options{}))
	if again := render(t, run(t, happyDeps(), Options{})); !bytes.Equal(out, again) {
		t.Fatal("two runs over the same cluster state differ")
	}
	golden := filepath.Join("testdata", "draft.golden.yaml")
	if *update {
		if err := os.WriteFile(golden, out, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("%v (run go test -update once)", err)
	}
	if !bytes.Equal(out, want) {
		t.Errorf("draft differs from %s:\n%s", golden, out)
	}
	for _, secret := range []string{"s3cr3t-pw", "dbz-user-xyz", "db-host-xyz", "sink-pw-xyz", "secretRoute", "route"} {
		if bytes.Contains(out, []byte(secret)) {
			t.Errorf("output contains config value %q", secret)
		}
	}
}

func TestDraftLoadsWithOwners(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owners.draft.yaml")
	if err := os.WriteFile(path, render(t, run(t, happyDeps(), Options{})), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	p, ok := o.Pipeline("pg-orders")
	if !ok || p.TopicPrefix != "pg" || !slices.Equal(p.SourceTables, []string{"public.orders"}) ||
		!slices.Equal(p.Connectors, []string{"pg-orders-connector"}) || !slices.Equal(p.ConsumerGroups, []string{"order-sync"}) {
		t.Errorf("pg-orders = %+v", p)
	}
	if b, ok := o.Pipeline("legacy-billing"); !ok || !slices.Equal(b.ConsumerGroups, []string{"billing-sync"}) {
		t.Errorf("legacy-billing = %+v", b)
	}
}

func TestHostileNamesStayInComments(t *testing.T) {
	name := "evil\n- id: injected # x"
	d := Deps{
		Connect: &fakeConnect{configs: map[string]map[string]string{
			name:   pgConfig("topic.prefix", "pg", "table.include.list", "public.a\nb"),
			"sink": {"connector.class": "x\n- id: injected2"},
		}},
		Kafka: &fakeKafka{topics: []string{"pg.public.a"}}, Registry: &fakeRegistry{},
	}
	out := render(t, run(t, d, Options{}))
	var doc struct {
		Pipelines []struct {
			ID    string `yaml:"id"`
			Title string `yaml:"title"`
		} `yaml:"pipelines"`
	}
	if err := yaml.Unmarshal(out, &doc); err != nil {
		t.Fatalf("output does not parse: %v\n%s", err, out)
	}
	if len(doc.Pipelines) != 1 || doc.Pipelines[0].Title != name {
		t.Errorf("pipelines = %+v\n%s", doc.Pipelines, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "- id: injected") {
			t.Errorf("injected line escaped a comment: %q", line)
		}
	}
}
