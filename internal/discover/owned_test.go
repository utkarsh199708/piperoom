package discover

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

const jdbcSink = "io.confluent.connect.jdbc.JdbcSinkConnector"

// withSink is the pg-orders source plus one sink connector with the given
// config keys, reading pg.public.orders unless the config says otherwise.
func withSink(sinkCfg map[string]string) Deps {
	cfg := map[string]string{"connector.class": jdbcSink, "topics": "pg.public.orders"}
	for k, v := range sinkCfg {
		cfg[k] = v
	}
	return Deps{
		Connect: &fakeConnect{configs: map[string]map[string]string{
			"pg-orders-connector": pgConfig("topic.prefix", "pg", "table.include.list", "public.orders"),
			"wh-sink":             cfg,
		}},
		Kafka:    &fakeKafka{topics: []string{"pg.public.orders", "pg.public.other"}},
		Registry: &fakeRegistry{},
	}
}

func TestSinkTables(t *testing.T) {
	cases := []struct {
		name string
		cfg  map[string]string
		want []string
	}{
		{"literal schema.table", map[string]string{"table.name.format": "analytics.orders"}, []string{"analytics.orders"}},
		{"surrounding space", map[string]string{"table.name.format": " analytics.orders "}, []string{"analytics.orders"}},
		{"topic template is not a name", map[string]string{"table.name.format": "analytics_${topic}"}, nil},
		{"default format", nil, nil},
		{"bare table, schema unknown", map[string]string{"table.name.format": "orders"}, nil},
		{"three parts", map[string]string{"table.name.format": "db.analytics.orders"}, nil},
		{"quoted identifier", map[string]string{"table.name.format": `"analytics"."orders"`}, nil},
		{"hostile text", map[string]string{"table.name.format": "analytics.orders\n- id: injected"}, nil},
		{"sink reads another topic", map[string]string{"table.name.format": "analytics.orders", "topics": "pg.public.other"}, nil},
		{"two topics, one table", map[string]string{"table.name.format": "analytics.orders", "topics": "pg.public.orders, pg.public.other"}, []string{"analytics.orders"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := one(t, run(t, withSink(c.cfg), Options{}))
			if !slices.Equal(p.SinkTables, c.want) {
				t.Errorf("SinkTables = %v, want %v", p.SinkTables, c.want)
			}
		})
	}
}

func TestSinkConfigSecretsStayOut(t *testing.T) {
	// Only topics and table.name.format are read from a sink connector.
	p := one(t, run(t, withSink(map[string]string{
		"table.name.format": "analytics.orders", "connection.password": "sink-pw-xyz", "connection.url": "jdbc:x://db-host-xyz",
	}), Options{}))
	if got := strings.Join(p.Notes, "\n") + strings.Join(p.SinkTables, ","); strings.Contains(got, "sink-pw-xyz") || strings.Contains(got, "db-host-xyz") {
		t.Errorf("sink config value leaked: %v", p)
	}
}

func TestPipelineTopics(t *testing.T) {
	p := one(t, run(t, single(pgConfig("topic.prefix", "pg", "table.include.list", "public.orders, public.refunds"),
		"pg.public.orders", "pg.transaction", "pg.public.unrelated"), Options{}))
	// public.refunds has no topic yet, so only the existing one is listed.
	if !slices.Equal(p.Topics, []string{"pg.public.orders"}) {
		t.Errorf("Topics = %v", p.Topics)
	}
}

func renderOwned(t *testing.T, r Result, owner string) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := RenderOwned(&buf, r, owner); err != nil {
		t.Fatalf("RenderOwned: %v", err)
	}
	return buf.Bytes()
}

func writeTemp(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRenderOwnedGolden(t *testing.T) {
	out := renderOwned(t, run(t, happyDeps(), Options{}), "alice")
	if again := renderOwned(t, run(t, happyDeps(), Options{}), "alice"); !bytes.Equal(out, again) {
		t.Fatal("two runs over the same cluster state differ")
	}
	golden := filepath.Join("testdata", "owned.golden.yaml")
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
		t.Errorf("owned draft differs from %s:\n%s", golden, out)
	}
}

func TestRenderOwnedLoads(t *testing.T) {
	deps := withSink(map[string]string{"table.name.format": "analytics.orders"})
	out := renderOwned(t, run(t, deps, Options{}), "alice")
	o, err := owners.Load(writeTemp(t, "owners.yaml", out))
	if err != nil {
		t.Fatalf("owners.Load: %v\n%s", err, out)
	}
	want := owners.Principal{Kind: owners.KindUser, Name: "alice"}
	for _, layer := range cdcLayers() {
		if !o.Layers[layer].Contains(want) || len(o.Layers[layer]) != 1 {
			t.Errorf("layer %s = %v, want only user:alice", layer, o.Layers[layer])
		}
	}
	p, ok := o.Pipeline("pg-orders")
	if !ok {
		t.Fatalf("no pg-orders in %+v", o.Pipelines)
	}
	// A found table is named in a comment, not written: Load would refuse
	// sink_tables without sink_ts_column.
	if len(p.SinkTables) != 0 || p.SinkTSColumn != "" {
		t.Errorf("sink = %v / %q, want empty", p.SinkTables, p.SinkTSColumn)
	}
	if !strings.Contains(string(out), "sink table found in a sink connector config: analytics.orders") {
		t.Errorf("found table not named in a comment:\n%s", out)
	}
	if strings.Contains(string(out), "sink table not found") {
		t.Errorf("a found table was also reported missing:\n%s", out)
	}
}

// TestOwnersAndUsersFilesNameTheSameLayers pins the P0 merge bug: when
// events.Layers grew to eight entries, the two renderers built their lists
// separately, the users file listed eight layers and the owners file six, and
// room.ValidateUsers refused the pair. Both files must name exactly the CDC
// layers, in the same order.
func TestOwnersAndUsersFilesNameTheSameLayers(t *testing.T) {
	want := events.LayersOf(events.PipelineKindCDC)
	if !slices.Equal(cdcLayers(), want) {
		t.Fatalf("cdcLayers() = %v, want %v", cdcLayers(), want)
	}
	var ownersDoc struct {
		Layers yaml.Node `yaml:"layers"`
	}
	if err := yaml.Unmarshal(renderOwned(t, run(t, happyDeps(), Options{}), "alice"), &ownersDoc); err != nil {
		t.Fatal(err)
	}
	var ownersLayers []string
	for i := 0; i+1 < len(ownersDoc.Layers.Content); i += 2 {
		ownersLayers = append(ownersLayers, ownersDoc.Layers.Content[i].Value)
	}
	var ub bytes.Buffer
	if err := RenderUsers(&ub, "alice"); err != nil {
		t.Fatal(err)
	}
	var usersDoc struct {
		Users []struct {
			Layers []string `yaml:"layers"`
		} `yaml:"users"`
	}
	if err := yaml.Unmarshal(ub.Bytes(), &usersDoc); err != nil || len(usersDoc.Users) != 1 {
		t.Fatalf("users file: %v, %+v", err, usersDoc)
	}
	if !slices.Equal(ownersLayers, want) || !slices.Equal(usersDoc.Users[0].Layers, want) {
		t.Errorf("owners file layers %v, users file layers %v, want both %v", ownersLayers, usersDoc.Users[0].Layers, want)
	}
	for _, batchOnly := range []string{"orchestrator", "transform"} {
		if slices.Contains(ownersLayers, batchOnly) || slices.Contains(usersDoc.Users[0].Layers, batchOnly) {
			t.Errorf("batch-only layer %s is in a CDC file", batchOnly)
		}
	}
}

func TestRenderOwnedMissingSink(t *testing.T) {
	out := string(renderOwned(t, run(t, withSink(nil), Options{}), "alice"))
	// The comment line sits directly above "sink_tables: []".
	i := strings.Index(out, "sink_tables: []")
	if i < 0 {
		t.Fatalf("no empty sink_tables:\n%s", out)
	}
	above := out[:i]
	above = above[strings.LastIndex(strings.TrimRight(above, " \n"), "\n")+1:]
	if !strings.Contains(above, "# TODO: sink table not found; fill in") {
		t.Errorf("line above sink_tables = %q\n%s", above, out)
	}
	if strings.Contains(out, "analytics") {
		t.Errorf("a sink table was guessed:\n%s", out)
	}
}

func TestRenderOwnedSamePipelinesAsRender(t *testing.T) {
	// -out and -write-owners must describe the same pipelines.
	type doc struct {
		Pipelines []struct {
			ID           string   `yaml:"id"`
			SourceTables []string `yaml:"source_tables"`
		} `yaml:"pipelines"`
	}
	res := run(t, happyDeps(), Options{})
	var a, b doc
	if err := yaml.Unmarshal(render(t, res), &a); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(renderOwned(t, res, "alice"), &b); err != nil {
		t.Fatal(err)
	}
	if len(a.Pipelines) != 2 || !slices.EqualFunc(a.Pipelines, b.Pipelines, func(x, y struct {
		ID           string   `yaml:"id"`
		SourceTables []string `yaml:"source_tables"`
	}) bool {
		return x.ID == y.ID && slices.Equal(x.SourceTables, y.SourceTables)
	}) {
		t.Errorf("pipelines differ:\n%+v\n%+v", a.Pipelines, b.Pipelines)
	}
}

func TestRenderOwnedRefusesBadOwner(t *testing.T) {
	var buf bytes.Buffer
	if err := RenderOwned(&buf, Result{}, "a b"); err == nil || buf.Len() != 0 {
		t.Errorf("RenderOwned(a b) = %v, wrote %q", err, buf.String())
	}
	if err := RenderUsers(&buf, ""); err == nil || buf.Len() != 0 {
		t.Errorf("RenderUsers(empty) = %v, wrote %q", err, buf.String())
	}
}

// TestOwnerRulesMatchTheServer holds ValidateOwner to the loaders the server
// uses: an id it accepts must start the server with both generated files, and
// an id it rejects must be one a real loader rejects too.
func TestOwnerRulesMatchTheServer(t *testing.T) {
	if derivedIDPrefix != room.DerivedIDPrefix {
		t.Fatalf("derivedIDPrefix = %q, room.DerivedIDPrefix = %q", derivedIDPrefix, room.DerivedIDPrefix)
	}
	res := run(t, happyDeps(), Options{})
	accepted := []string{"alice", "a.b", "bob-1", "JDoe", "j_doe", "x", "ünï", "u", "user", "piperoom"}
	for _, id := range accepted {
		t.Run("ok "+id, func(t *testing.T) {
			if err := ValidateOwner(id); err != nil {
				t.Fatalf("ValidateOwner(%q) = %v", id, err)
			}
			own, err := owners.Load(writeTemp(t, "owners.yaml", renderOwned(t, res, id)))
			if err != nil {
				t.Fatalf("owners.Load: %v", err)
			}
			var ub bytes.Buffer
			if err := RenderUsers(&ub, id); err != nil {
				t.Fatal(err)
			}
			users, err := room.ParseUsers(ub.Bytes())
			if err != nil {
				t.Fatalf("room.ParseUsers: %v\n%s", err, ub.Bytes())
			}
			if err := room.ValidateUsers(users, own); err != nil {
				t.Fatalf("room.ValidateUsers: %v", err)
			}
			u, ok := users.Get(id)
			if !ok || u.Name != id {
				t.Fatalf("user %q = %+v, %v", id, u, ok)
			}
			for _, layer := range cdcLayers() {
				if !u.OwnsLayer(layer) {
					t.Errorf("%s does not own %s", id, layer)
				}
			}
		})
	}

	rejected := []string{"", " ", "a b", "a\tb", "a\nb", "a:b", "user:a", "a,b", "u_alice", "u_"}
	for _, id := range rejected {
		t.Run("rejected "+id, func(t *testing.T) {
			if err := ValidateOwner(id); err == nil {
				t.Fatalf("ValidateOwner(%q) = nil", id)
			}
			// A real loader must agree that the id is unusable.
			_, perr := owners.ParsePrincipal("user:" + id)
			data, err := yaml.Marshal(map[string]any{"users": []map[string]any{{"id": id, "name": id, "layers": cdcLayers()}}})
			if err != nil {
				t.Fatal(err)
			}
			_, uerr := room.ParseUsers(data)
			if perr == nil && uerr == nil {
				t.Errorf("both real loaders accept %q", id)
			}
		})
	}
	// The loaders let control characters through; a generated file with one
	// would be hard to read and edit, so ValidateOwner refuses them anyway.
	for _, id := range []string{"a\x00b", "a\x7fb", "a\u0085b"} {
		if err := ValidateOwner(id); err == nil {
			t.Errorf("ValidateOwner(%q) = nil", id)
		}
	}
}
