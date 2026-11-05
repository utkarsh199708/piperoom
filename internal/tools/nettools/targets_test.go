package nettools

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// resolveAll resolves every label a source lists, so a table test reads like
// the old list API.
func resolveAll(t *testing.T, src TargetSource, p owners.Pipeline) ([]Target, error) {
	t.Helper()
	var out []Target
	for _, label := range src.Labels(p) {
		tgt, err := src.Target(context.Background(), p, label)
		if err != nil {
			return nil, err
		}
		out = append(out, tgt)
	}
	return out, nil
}

func targetByLabel(t *testing.T, targets []Target, label string) Target {
	t.Helper()
	for _, tgt := range targets {
		if tgt.Label == label {
			return tgt
		}
	}
	t.Fatalf("no %q target in %+v", label, targets)
	return Target{}
}

func TestHostPortOf(t *testing.T) {
	tests := []struct {
		name, raw, def, want string
	}{
		{"postgres URL", "postgres://u:p@db.internal:5432/orders?sslmode=disable", "5432", "db.internal:5432"},
		{"postgres URL default port", "postgres://u:p@db.internal/orders", "5432", "db.internal:5432"},
		{"escaped password", "postgres://u:p%40x@db.internal:5432/db", "5432", "db.internal:5432"},
		{"bare host", "db.internal", "5432", "db.internal:5432"},
		{"bare host port", "db.internal:6000", "5432", "db.internal:6000"},
		{"userinfo and path", "u:p@db.internal:6000/x", "5432", "db.internal:6000"},
		{"IPv4", "127.0.0.1:9092", "9092", "127.0.0.1:9092"},
		{"bracketed IPv6", "[::1]:9092", "9092", "[::1]:9092"},
		{"bare IPv6", "::1", "5432", "[::1]:5432"},
		{"https URL", "https://registry.internal", "8081", "registry.internal:443"},
		{"http URL", "http://connect.internal/x", "8083", "connect.internal:80"},
		{"config reference", "${file:/run/secrets/db}", "5432", ""},
		{"bad port", "db.internal:notaport", "5432", ""},
		{"port out of range", "db.internal:70000", "5432", ""},
		{"empty", "", "5432", ""},
		{"not a host", "not a host", "5432", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := hostPortOf(tc.raw, tc.def); got != tc.want {
				t.Fatalf("hostPortOf(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestRoomTargetsFromConfig(t *testing.T) {
	cfg := config.Config{
		KafkaBrokers:            "b1:9092, b2:9093",
		KafkaSecurityProtocol:   "SSL",
		KafkaTLSCAFile:          filepath.Join(t.TempDir(), "missing.pem"),
		ConnectURL:              "http://connect.internal:8083",
		SchemaRegistryURL:       "https://registry.internal:8081",
		WarehouseKind:           "postgres",
		WarehouseDBURL:          "postgres://whuser:whpass@wh.internal:5432/analytics",
		SourceDBURL:             "postgres://srcuser:srcpass@src.internal:5432/orders",
		KafkaTLSServerName:      "",
		ConnectTLSCAFile:        "",
		SchemaRegistryTLSCAFile: "",
	}
	targets, err := resolveAll(t, RoomTargets(cfg, nil), owners.Pipeline{ID: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	kafka := targetByLabel(t, targets, LabelKafka)
	if len(kafka.Endpoints) != 2 || kafka.Endpoints[0].Addr != "b1:9092" || kafka.Endpoints[1].Addr != "b2:9093" {
		t.Fatalf("kafka = %+v", kafka)
	}
	if !kafka.Endpoints[0].TLS || kafka.Kind != KindKafka {
		t.Fatalf("kafka = %+v", kafka)
	}
	if c := targetByLabel(t, targets, LabelConnect); len(c.Endpoints) != 1 || c.Endpoints[0].Addr != "connect.internal:8083" || c.Endpoints[0].TLS {
		t.Fatalf("connect = %+v", c)
	}
	if r := targetByLabel(t, targets, LabelRegistry); len(r.Endpoints) != 1 || r.Endpoints[0].Addr != "registry.internal:8081" || !r.Endpoints[0].TLS {
		t.Fatalf("registry = %+v", r)
	}
	if w := targetByLabel(t, targets, LabelWarehouse); len(w.Endpoints) != 1 || w.Endpoints[0].Addr != "wh.internal:5432" {
		t.Fatalf("warehouse = %+v", w)
	}
	if s := targetByLabel(t, targets, LabelSource); len(s.Endpoints) != 1 || s.Endpoints[0].Addr != "src.internal:5432" {
		t.Fatalf("source = %+v", s)
	}
	// A DSN's credentials never reach a target, whatever is marshalled.
	b, err := json.Marshal(targets)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"srcpass", "srcuser", "whpass", "whuser"} {
		if strings.Contains(string(b), secret) {
			t.Fatalf("targets carry %q: %s", secret, b)
		}
	}
}

func TestRoomTargetsProvisionalConnections(t *testing.T) {
	p := owners.Pipeline{ID: "new-pipeline", Connections: owners.Connections{
		"source_db_url":    {Value: "postgres://u:p@src.internal:5544/db"},
		"kafka_brokers":    {Value: "k:9092"},
		"connect_url":      {Value: "http://connect.internal:8083"},
		"warehouse_kind":   {Value: "postgres"},
		"warehouse_db_url": {Value: "postgres://u:p@wh.internal:5432/db"},
	}}
	targets, err := resolveAll(t, RoomTargets(config.Config{}, nil), p)
	if err != nil {
		t.Fatal(err)
	}
	if s := targetByLabel(t, targets, LabelSource); s.Endpoints[0].Addr != "src.internal:5544" {
		t.Fatalf("source = %+v", s)
	}
	if k := targetByLabel(t, targets, LabelKafka); k.Endpoints[0].Addr != "k:9092" {
		t.Fatalf("kafka = %+v", k)
	}
	targetByLabel(t, targets, LabelConnect)
	targetByLabel(t, targets, LabelWarehouse)

	// A resolved setting wins over a literal: the client set's own value is
	// what its clients dial.
	cfg := config.Config{SourceDBURL: "postgres://u:p@resolved.internal:5432/db"}
	targets, err = resolveAll(t, RoomTargets(cfg, nil), p)
	if err != nil {
		t.Fatal(err)
	}
	if s := targetByLabel(t, targets, LabelSource); s.Endpoints[0].Addr != "resolved.internal:5432" {
		t.Fatalf("source = %+v", s)
	}
}

func TestRoomTargetsFromConnectors(t *testing.T) {
	fc := &fakeConnect{configFn: func(_ context.Context, name string) (map[string]string, error) {
		switch name {
		case "pg-orders":
			return map[string]string{
				"connector.class":   "io.debezium.connector.postgresql.PostgresConnector",
				"database.hostname": "src.internal",
				"database.port":     "5433",
				"database.password": "hunter2",
				"database.user":     "debezium",
			}, nil
		case "jdbc-sink":
			return map[string]string{"connection.url": "jdbc:postgresql://wh.internal:5432/db"}, nil
		case "no-host":
			return map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector"}, nil
		default:
			return nil, errors.New("connect answered 500")
		}
	}}
	p := owners.Pipeline{ID: "orders", Connectors: []string{"pg-orders", "jdbc-sink", "no-host", "broken"}}
	targets, err := resolveAll(t, RoomTargets(config.Config{}, fc), p)
	if err != nil {
		t.Fatal(err)
	}
	src := targetByLabel(t, targets, LabelSource)
	if len(src.Endpoints) != 1 || src.Endpoints[0].Addr != "src.internal:5433" || src.Endpoints[0].Connector != "pg-orders" {
		t.Fatalf("source = %+v", src)
	}
	if !strings.Contains(src.Note, "broken: config unreadable") {
		t.Fatalf("note = %q", src.Note)
	}
	if len(src.Connectors) != 4 {
		t.Fatalf("connectors = %+v", src.Connectors)
	}
	b, _ := json.Marshal(targets)
	if strings.Contains(string(b), "hunter2") || strings.Contains(string(b), "wh.internal") {
		t.Fatalf("targets carry a credential or a sink host: %s", b)
	}
	// Without a database.port, the Postgres default is used.
	fc.configFn = func(context.Context, string) (map[string]string, error) {
		return map[string]string{"database.hostname": "src.internal"}, nil
	}
	targets, err = resolveAll(t, RoomTargets(config.Config{}, fc), owners.Pipeline{ID: "p", Connectors: []string{"pg"}})
	if err != nil {
		t.Fatal(err)
	}
	if s := targetByLabel(t, targets, LabelSource); s.Endpoints[0].Addr != "src.internal:5432" {
		t.Fatalf("source = %+v", s)
	}
}

func TestTargetsFromSetup(t *testing.T) {
	src := TargetsFromSetup([]events.SetupTarget{
		{Kind: "connect", Address: "127.0.0.1:8083"},
		{Kind: "kafka", Address: "b1:9092, b2:9092"},
		{Kind: "schema_registry", Address: "registry.internal"},
		{Kind: "postgres", Address: "db.internal"},
		{Kind: "", Address: "ignored:1"},
	})
	targets, err := resolveAll(t, src, owners.Pipeline{})
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 4 {
		t.Fatalf("targets = %+v", targets)
	}
	if k := targetByLabel(t, targets, "kafka"); len(k.Endpoints) != 2 || k.Kind != KindKafka {
		t.Fatalf("kafka = %+v", k)
	}
	if r := targetByLabel(t, targets, "schema_registry"); r.Endpoints[0].Addr != "registry.internal:8081" || r.Kind != KindRegistry {
		t.Fatalf("registry = %+v", r)
	}
	if p := targetByLabel(t, targets, "postgres"); p.Endpoints[0].Addr != "db.internal:5432" || p.Kind != KindSource {
		t.Fatalf("postgres = %+v", p)
	}
	if c := targetByLabel(t, targets, "connect"); c.Kind != KindConnect {
		t.Fatalf("connect = %+v", c)
	}
}

func TestLabelsDoNotReadConnect(t *testing.T) {
	calls := 0
	fc := &fakeConnect{configFn: func(context.Context, string) (map[string]string, error) {
		calls++
		return map[string]string{"database.hostname": "src.internal"}, nil
	}}
	p := owners.Pipeline{ID: "orders", Connectors: []string{"pg-orders"}}
	labels := RoomTargets(config.Config{}, fc).Labels(p)
	if calls != 0 {
		t.Fatalf("Labels read %d connector configs", calls)
	}
	if !hasLabel(labels, LabelSource) {
		t.Fatalf("labels = %+v, want source offered for a declared connector", labels)
	}
}

func TestCanonicalAddr(t *testing.T) {
	tests := []struct{ host, port, want string }{
		{"DB.Internal", "5432", "db.internal:5432"},
		{"127.0.0.1", "1", "127.0.0.1:1"},
		{"::1", "5432", "[::1]:5432"},
		{"[::1]", "5432", "[::1]:5432"},
		{"db", "0", ""},
		{"db", "65536", ""},
		{"db", "", ""},
		{"", "5432", ""},
		{"has space", "5432", ""},
	}
	for _, tc := range tests {
		if got := canonicalAddr(tc.host, tc.port); got != tc.want {
			t.Errorf("canonicalAddr(%q, %q) = %q, want %q", tc.host, tc.port, got, tc.want)
		}
	}
}
