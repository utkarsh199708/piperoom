package owners

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/events"
)

// mysqlPipeline is one MySQL cdc pipeline with extra keys appended.
func mysqlPipeline(extra string) string {
	return `
pipelines:
  - id: orders-mysql
    source_kind: mysql
    topic_prefix: shop
    source_tables: [shop.orders, shop.order_items]
    connectors: [orders-mysql]
` + extra
}

func TestSourceKindLoad(t *testing.T) {
	o, err := Load(writeTemp(t, mysqlPipeline(`    owners:
      mysql: {approvers: [dana]}
layers:
  mysql: dana
  postgres: priya
`)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders-mysql")
	if p.SourceKindOrDefault() != SourceKindMySQL || !p.IsMySQL() || p.SourceLayer() != LayerMySQL {
		t.Fatalf("kind %q, mysql %v, layer %q", p.SourceKindOrDefault(), p.IsMySQL(), p.SourceLayer())
	}
	want := []string{"mysql", "kafka-connect", "schema-registry", "kafka", "consumer", "warehouse", events.LayerEnvironment}
	if got := p.Layers(); !slices.Equal(got, want) {
		t.Fatalf("Layers = %v, want %v", got, want)
	}
	if _, ok := o.CanApprove("orders-mysql", "mysql", "dana", nil); !ok {
		t.Fatal("dana approves the mysql layer")
	}
	if got := o.LayersFor("dana", nil); !slices.Equal(got, []string{"mysql"}) {
		t.Fatalf("LayersFor(dana) = %v", got)
	}
	// The slot signals are off for a MySQL pipeline, loaded or built in code.
	if w := p.Watch(); w.SlotInactive.Enabled || w.SlotWAL.Enabled || !w.ConnectorDown.Enabled || !w.ConsumerLag.Enabled {
		t.Fatalf("watch = %+v", w)
	}
	if w := (Pipeline{SourceKind: SourceKindMySQL}).Watch(); w.SlotInactive.Enabled || w.SlotWAL.Enabled {
		t.Fatal("code-built MySQL pipeline: slot signals must be off")
	}

	// Postgres is the default: every existing file reads the same.
	o, err = Load(writeTemp(t, validYAML))
	if err != nil {
		t.Fatal(err)
	}
	pg, _ := o.Pipeline("orders")
	if pg.SourceKindOrDefault() != SourceKindPostgres || pg.IsMySQL() || pg.SourceLayer() != "postgres" {
		t.Fatalf("default kind %q layer %q", pg.SourceKindOrDefault(), pg.SourceLayer())
	}
	if got := pg.Layers(); !slices.Equal(got, events.LayersOf(events.PipelineKindCDC)) {
		t.Fatalf("Postgres layers = %v", got)
	}
	if w := pg.Watch(); !w.SlotInactive.Enabled || !w.SlotWAL.Enabled {
		t.Fatal("Postgres slot signals stay on")
	}
	// An explicit postgres is the same.
	if _, err := Load(writeTemp(t, strings.Replace(validYAML, "    topic_prefix: pg", "    source_kind: postgres\n    topic_prefix: pg", 1))); err != nil {
		t.Fatalf("source_kind: postgres: %v", err)
	}

	var nilP *Pipeline
	if nilP.SourceKindOrDefault() != SourceKindPostgres || nilP.IsMySQL() {
		t.Fatal("nil pipeline is the default kind")
	}
	batch := Pipeline{Kind: events.PipelineKindBatch}
	if batch.SourceLayer() != "" || batch.IsMySQL() {
		t.Fatal("a batch pipeline has no source layer")
	}
	if got := (&Pipeline{SourceKind: "db2"}).Layers(); got != nil {
		t.Fatalf("unknown source kind: Layers = %v, want nil", got)
	}
	if (&Pipeline{SourceKind: "db2"}).SourceLayer() != "" {
		t.Fatal("unknown source kind has no layer")
	}
}

func TestSourceKindLoadErrors(t *testing.T) {
	for _, tc := range []struct{ name, yaml, want string }{
		{"unknown kind", strings.Replace(mysqlPipeline(""), "source_kind: mysql", "source_kind: db2", 1), `source_kind "db2": want postgres, mysql, sqlserver, mongodb, oracle`},
		{"secret-looking kind", strings.Replace(mysqlPipeline(""), "source_kind: mysql", "source_kind: 'mysql://u:p@h'", 1), "not a plain name"},
		{"upper case", strings.Replace(mysqlPipeline(""), "source_kind: mysql", "source_kind: MySQL", 1), "want postgres, mysql, sqlserver, mongodb, oracle"},
		{"batch", `
pipelines:
  - id: nightly
    kind: batch
    source_kind: mysql
    dags: [nightly]
`, "source_kind is a cdc field"},
		{"watch slot", mysqlPipeline("    watch: {slot: debezium}\n"), "watch.slot: a mysql source has no replication slot"},
		{"watch wal", mysqlPipeline("    watch: {wal_headroom_bytes: 50GiB}\n"), "watch.wal_headroom_bytes: a mysql source has no WAL"},
		{"slot_inactive", mysqlPipeline("    watch: {signals: {slot_inactive: {enabled: false}}}\n"), "watch.signals.slot_inactive: a mysql source"},
		{"slot_wal", mysqlPipeline("    watch: {signals: {slot_wal: {fire: 1GiB}}}\n"), "watch.signals.slot_wal: a mysql source"},
		{"postgres owner on mysql", mysqlPipeline("    owners: {postgres: {approvers: [priya]}}\n"), `owners: layer "postgres" is not a cdc layer`},
		{"mysql owner on postgres", strings.Replace(validYAML, "    topic_prefix: pg", "    owners: {mysql: {approvers: [dana]}}\n    topic_prefix: pg", 1), `owners: layer "mysql" is not a cdc layer`},
		{"mysql repo glob on postgres", strings.Replace(validYAML, "    topic_prefix: pg", `    repos: [{id: r, url: https://github.com/a/r, base: main, paths: [{glob: "a/*", layer: mysql}]}]
    topic_prefix: pg`, 1), `layer "mysql" is not a layer of the pipeline`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
	// The source-neutral watch keys still load on MySQL.
	if _, err := Load(writeTemp(t, mysqlPipeline("    watch: {enabled: true, signals: {connector_down: {for: 2m}, consumer_lag: {fire: 5000}}}\n"))); err != nil {
		t.Fatalf("neutral watch keys: %v", err)
	}
	// A mysql repo glob is fine on a MySQL pipeline.
	if _, err := Load(writeTemp(t, mysqlPipeline(`    repos: [{id: r, url: https://github.com/a/r, base: main, paths: [{glob: "db/*.sql", layer: mysql}]}]
`))); err != nil {
		t.Fatalf("mysql repo glob: %v", err)
	}
}

func TestKnownLayers(t *testing.T) {
	got := KnownLayers()
	if !slices.Contains(got, LayerMySQL) || !slices.Equal(got[:len(events.Layers)], events.Layers) {
		t.Fatalf("KnownLayers = %v", got)
	}
	if strings.Count(strings.Join(got, ","), LayerMySQL) != 1 {
		t.Fatalf("mysql listed once: %v", got)
	}
	got[0] = "x"
	if KnownLayers()[0] != events.Layers[0] {
		t.Fatal("KnownLayers must return a new slice")
	}
	// A top-level layer outside every kind is still refused.
	if _, err := Load(writeTemp(t, validYAML+"  mssql: priya\n")); err == nil || !strings.Contains(err.Error(), `unknown layer "mssql"`) {
		t.Fatalf("unknown layer: %v", err)
	}
}

func TestLayersOfSource(t *testing.T) {
	for _, tc := range []struct {
		kind, src string
		first     string
		n         int
	}{
		{"", "", "postgres", 7},
		{"cdc", "postgres", "postgres", 7},
		{"cdc", "mysql", "mysql", 7},
		{"cdc", "sqlserver", "sqlserver", 7},
		{"cdc", "mongodb", "mongodb", 7},
		{"cdc", "oracle", "oracle", 7},
		{"batch", "mysql", "orchestrator", 3},
		{"cdc", "db2", "", 0},
		{"stream", "", "", 0},
	} {
		got := layersOfSource(tc.kind, tc.src)
		if len(got) != tc.n || (tc.n > 0 && got[0] != tc.first) {
			t.Errorf("layersOfSource(%q, %q) = %v", tc.kind, tc.src, got)
		}
	}
}

func TestProvisionalSourceKind(t *testing.T) {
	req := events.SetupRequest{
		PipelineID: "orders-mysql", TopicPrefix: "shop", SourceKind: "mysql",
		Tables: []string{"shop.orders"}, Connectors: []string{"orders-mysql"},
		Targets: []events.SetupTarget{{Kind: "mysql", Address: "db.acme:3306"}},
	}
	p, err := Provisional(req)
	if err != nil {
		t.Fatalf("Provisional: %v", err)
	}
	if !p.IsMySQL() || p.Connections["source_db_url"].Value != "db.acme:3306" {
		t.Fatalf("pipeline = %+v", p)
	}
	if p.Layers()[0] != LayerMySQL {
		t.Fatalf("layers = %v", p.Layers())
	}
	for _, k := range []string{"postgres", "sqlserver", ""} {
		req.SourceKind = k
		p, err := Provisional(req)
		if err != nil || p.SourceKind != "" {
			t.Fatalf("source kind %q: %q, %v", k, p.SourceKind, err)
		}
	}
}

func TestOverlapMySQLDefaultPort(t *testing.T) {
	tenants := map[string]Tenant{"a": {Admins: Principals{{Kind: KindUser, Name: "ann"}}}}
	newP, err := Provisional(events.SetupRequest{
		PipelineID: "new", Tenant: "a", TopicPrefix: "shop2", SourceKind: "mysql",
		Tables: []string{"shop.z"}, Connectors: []string{"z"},
		Targets: []events.SetupTarget{{Kind: "mysql", Address: "db.acme:3306"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, value, sourceKind string
		want                    bool
	}{
		{"mysql url without port", "mysql://u@DB.acme/shop", SourceKindMySQL, true},
		{"bare host on mysql", "db.acme", SourceKindMySQL, true},
		{"bare host on postgres is 5432", "db.acme", "", false},
		{"mysql url on another port", "mysql://db.acme:3307/shop", SourceKindMySQL, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loaded := Pipeline{ID: "qa", Tenant: "a", SourceKind: tc.sourceKind, TopicPrefix: "other", SourceTables: []string{"shop.x"}, Connectors: []string{"x"},
				Connections: Connections{"source_db_url": {Value: tc.value}}}
			out := loadedOwners(tenants, loaded).Overlap(newP, nil)
			if got := hasOverlap(out, OverlapHost, "db.acme:3306", "qa"); got != tc.want {
				t.Fatalf("overlaps = %+v, want host overlap %v", out, tc.want)
			}
		})
	}
	if schemeDefaultPort("MySQL") != "3306" {
		t.Fatal("mysql scheme default port")
	}
}

func TestConnectorIncludeMySQL(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cfg      map[string]string
		in, out  []string
		literal  []string
		invalid  int
		declared []string
		gaps     []string
	}{
		{
			name:    "tables only",
			cfg:     map[string]string{"table.include.list": "shop.orders,shop.order_items"},
			in:      []string{"shop.orders", "shop.order_items"},
			out:     []string{"shop.users", "other.orders"},
			literal: []string{"shop.orders", "shop.order_items"},
		},
		{
			name: "database include, no table list",
			cfg:  map[string]string{"database.include.list": "shop"},
			in:   []string{"shop.orders", "shop.anything", "SHOP.orders"},
			out:  []string{"crm.users", "shopx.orders"},
		},
		{
			name:    "database exclude removes a literal table",
			cfg:     map[string]string{"database.exclude.list": "legacy", "table.include.list": "shop.orders,legacy.orders"},
			in:      []string{"shop.orders"},
			out:     []string{"legacy.orders"},
			literal: []string{"shop.orders"},
		},
		{
			name:     "database include leaves a declared table out",
			cfg:      map[string]string{"database.include.list": "shop", "table.include.list": "shop.orders,crm.users"},
			in:       []string{"shop.orders"},
			out:      []string{"crm.users"},
			literal:  []string{"shop.orders"},
			declared: []string{"shop.orders"},
		},
		{
			name: "database pattern",
			cfg:  map[string]string{"database.include.list": "shop_[0-9]+"},
			in:   []string{"shop_1.orders"},
			out:  []string{"shop_x.orders"},
		},
		{
			name:    "invalid database entry",
			cfg:     map[string]string{"database.include.list": "shop(", "table.include.list": "shop.orders"},
			out:     []string{"shop.orders"},
			invalid: 1,
		},
		{
			name:     "gap",
			cfg:      map[string]string{"table.include.list": "shop.orders,shop.payments"},
			in:       []string{"shop.orders", "shop.payments"},
			literal:  []string{"shop.orders", "shop.payments"},
			declared: []string{"shop.orders"},
			gaps:     []string{"shop.payments"},
		},
		{
			// A Postgres connector has no database lists: unchanged.
			name:    "postgres",
			cfg:     map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "table.include.list": "public.orders", "database.dbname": "orders"},
			in:      []string{"public.orders"},
			out:     []string{"public.users"},
			literal: []string{"public.orders"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := ParseConnectorInclude(tc.cfg)
			for _, tb := range tc.in {
				if !c.Includes(tb) {
					t.Errorf("Includes(%s) = false", tb)
				}
			}
			for _, tb := range tc.out {
				if c.Includes(tb) {
					t.Errorf("Includes(%s) = true", tb)
				}
			}
			if !slices.Equal(c.Literal, tc.literal) {
				t.Errorf("Literal = %v, want %v", c.Literal, tc.literal)
			}
			if len(c.Invalid) != tc.invalid {
				t.Errorf("Invalid = %v", c.Invalid)
			}
			if tc.declared != nil {
				if got := c.OutsideDeclared(tc.declared); !slices.Equal(got, tc.gaps) {
					t.Errorf("OutsideDeclared = %v, want %v", got, tc.gaps)
				}
			}
		})
	}
}

// TestDevOwnersFilesLoad keeps every owners file of the dev stack loading:
// source_kind and repos: are additive.
func TestDevOwnersFilesLoad(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", "dev", "owners*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Skip("no dev owners files")
	}
	for _, f := range files {
		if _, err := os.Stat(f); err != nil {
			t.Fatal(err)
		}
		o, err := Load(f)
		if err != nil {
			t.Errorf("Load(%s): %v", f, err)
			continue
		}
		for _, p := range o.Pipelines {
			if p.IsMySQL() {
				continue
			}
			if p.KindOrDefault() == events.PipelineKindCDC && p.SourceLayer() != "postgres" {
				t.Errorf("%s: pipeline %s source layer %q", f, p.ID, p.SourceLayer())
			}
		}
	}
}

// kindPipeline is mysqlPipeline with another source kind.
func kindPipeline(kind, extra string) string {
	return strings.Replace(mysqlPipeline(extra), "source_kind: mysql", "source_kind: "+kind, 1)
}

func TestNewSourceKindsLoad(t *testing.T) {
	for _, kind := range []string{"sqlserver", "mongodb", "oracle"} {
		t.Run(kind, func(t *testing.T) {
			o, err := Load(writeTemp(t, kindPipeline(kind, "    owners:\n      "+kind+": {approvers: [dana]}\n"+
				"    watch: {enabled: true, signals: {connector_down: {for: 2m}}}\n")))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			p, _ := o.Pipeline("orders-mysql")
			if p.SourceKindOrDefault() != kind || p.SourceLayer() != kind || p.IsMySQL() {
				t.Fatalf("kind %q, layer %q, mysql %v", p.SourceKindOrDefault(), p.SourceLayer(), p.IsMySQL())
			}
			if l := p.Layers(); len(l) != 7 || l[0] != kind {
				t.Fatalf("layers = %v", l)
			}
			if _, ok := o.CanApprove("orders-mysql", kind, "dana", nil); !ok {
				t.Fatalf("dana approves the %s layer", kind)
			}
			if w := p.Watch(); w.SlotInactive.Enabled || w.SlotWAL.Enabled {
				t.Fatalf("slot signals must be off: %+v", w)
			}
			if got := SourceLayerOf(kind); got != kind {
				t.Fatalf("SourceLayerOf = %q", got)
			}
			if !slices.Contains(KnownLayers(), kind) {
				t.Fatalf("KnownLayers lacks %s", kind)
			}
		})
	}
}

func TestNewSourceKindsLoadErrors(t *testing.T) {
	for _, kind := range []string{"sqlserver", "mongodb", "oracle"} {
		for _, tc := range []struct{ name, extra, want string }{
			{"watch slot", "    watch: {slot: debezium}\n", "watch.slot: a " + kind + " source has no replication slot"},
			{"watch wal", "    watch: {wal_headroom_bytes: 50GiB}\n", "watch.wal_headroom_bytes: a " + kind + " source has no WAL"},
			{"slot_inactive", "    watch: {signals: {slot_inactive: {enabled: false}}}\n", "watch.signals.slot_inactive: a " + kind + " source"},
			{"slot_wal", "    watch: {signals: {slot_wal: {fire: 1GiB}}}\n", "watch.signals.slot_wal: a " + kind + " source"},
			{"postgres owner", "    owners: {postgres: {approvers: [priya]}}\n", `owners: layer "postgres" is not a cdc layer`},
			{"mysql owner", "    owners: {mysql: {approvers: [priya]}}\n", `owners: layer "mysql" is not a cdc layer`},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				_, err := Load(writeTemp(t, kindPipeline(kind, tc.extra)))
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("err = %v, want %q", err, tc.want)
				}
			})
		}
		t.Run(kind+"/batch", func(t *testing.T) {
			y := "\npipelines:\n  - id: nightly\n    kind: batch\n    source_kind: " + kind + "\n    dags: [nightly]\n"
			if _, err := Load(writeTemp(t, y)); err == nil || !strings.Contains(err.Error(), "source_kind is a cdc field") {
				t.Fatalf("err = %v", err)
			}
		})
		t.Run(kind+"/owner on postgres pipeline", func(t *testing.T) {
			y := strings.Replace(validYAML, "    topic_prefix: pg", "    owners: {"+kind+": {approvers: [dana]}}\n    topic_prefix: pg", 1)
			if _, err := Load(writeTemp(t, y)); err == nil || !strings.Contains(err.Error(), `layer "`+kind+`" is not a cdc layer`) {
				t.Fatalf("err = %v", err)
			}
		})
	}
	for _, bad := range []string{"SQLServer", "mssql", "mongo", "'oracle://u:p@h'"} {
		if _, err := Load(writeTemp(t, kindPipeline(bad, ""))); err == nil {
			t.Fatalf("source_kind %s must be refused", bad)
		}
	}
}

func TestNewSourceKindsDefaultPorts(t *testing.T) {
	for _, tc := range []struct{ kind, scheme, port string }{
		{SourceKindSQLServer, "sqlserver", "1433"},
		{SourceKindMongoDB, "mongodb", "27017"},
		{SourceKindOracle, "oracle", "1521"},
		{SourceKindMySQL, "mysql", "3306"},
		{"", "postgres", "5432"},
	} {
		p := Pipeline{SourceKind: tc.kind}
		if got := p.defaultPortFor("source_db_url"); got != tc.port {
			t.Errorf("%q: defaultPortFor = %q, want %q", tc.kind, got, tc.port)
		}
		if got := schemeDefaultPort(tc.scheme); got != tc.port {
			t.Errorf("scheme %q: %q, want %q", tc.scheme, got, tc.port)
		}
	}
	// A batch pipeline ignores the kind; a non-source key keeps its default.
	if got := (Pipeline{Kind: events.PipelineKindBatch, SourceKind: "oracle"}).defaultPortFor("source_db_url"); got != "5432" {
		t.Errorf("batch: %q", got)
	}
	if got := (Pipeline{SourceKind: "oracle"}).defaultPortFor("connect_url"); got != "8083" {
		t.Errorf("connect_url: %q", got)
	}
}
