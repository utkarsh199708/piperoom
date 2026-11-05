package setuptools

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/setup/knowledge"
)

func builder(t *testing.T) *ConfigBuilder {
	t.Helper()
	b, err := NewConfigBuilder(loadIndex(t), Versions{"debezium": "3.2.0.Final", "kafka": "3.9.0"})
	if err != nil {
		t.Fatalf("NewConfigBuilder: %v", err)
	}
	return b
}

func TestNewConfigBuilderNeedsIndexAndRelease(t *testing.T) {
	if _, err := NewConfigBuilder(nil, Versions{"debezium": "3.2.0.Final"}); !errors.Is(err, ErrConfigBuild) {
		t.Fatalf("nil index: %v", err)
	}
	if _, err := NewConfigBuilder(loadIndex(t), Versions{"kafka": "3.9.0"}); !errors.Is(err, ErrConfigBuild) {
		t.Fatalf("no Debezium release: %v", err)
	}
}

func TestAddTransforms(t *testing.T) {
	b := builder(t)
	base := map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "topic.prefix": "pg"}
	out, err := b.AddTransforms(base,
		[]Predicate{{Alias: "isHeartbeat", Class: "topic-name-matches", Config: map[string]string{"pattern": "__debezium-heartbeat.*"}}},
		[]Transform{
			{Alias: "unwrap", Class: "io.debezium.transforms.ExtractNewRecordState",
				Config:    map[string]string{"delete.tombstone.handling.mode": "rewrite", "add.fields": "op,source.ts_ms"},
				Predicate: "isHeartbeat", Negate: true},
			{Alias: "mask", Class: "org.apache.kafka.connect.transforms.MaskField$Value", Config: map[string]string{"fields": "email"}},
		})
	if err != nil {
		t.Fatalf("AddTransforms: %v", err)
	}
	want := map[string]string{
		"transforms":             "unwrap,mask",
		"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
		"transforms.unwrap.delete.tombstone.handling.mode": "rewrite",
		"transforms.unwrap.add.fields":                     "op,source.ts_ms",
		"transforms.unwrap.predicate":                      "isHeartbeat",
		"transforms.unwrap.negate":                         "true",
		"transforms.mask.type":                             "org.apache.kafka.connect.transforms.MaskField$Value",
		"transforms.mask.fields":                           "email",
		"predicates":                                       "isHeartbeat",
		"predicates.isHeartbeat.type":                      "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
		"predicates.isHeartbeat.pattern":                   "__debezium-heartbeat.*",
		"topic.prefix":                                     "pg",
	}
	for k, v := range want {
		if out[k] != v {
			t.Errorf("%s = %q, want %q", k, out[k], v)
		}
	}
	if _, ok := base["transforms"]; ok {
		t.Fatal("the base config was changed")
	}
	// A second call appends to the existing list.
	out2, err := b.AddTransforms(out, nil, []Transform{{Alias: "tz", Class: "TimezoneConverter", Config: map[string]string{"converted.timezone": "UTC"}}})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if out2["transforms"] != "unwrap,mask,tz" {
		t.Fatalf("transforms = %q", out2["transforms"])
	}
}

// The board's "done when": a config with an unknown SMT class gets the
// nearest known class.
func TestAddTransformsUnknownClassNamesNearest(t *testing.T) {
	b := builder(t)
	_, err := b.AddTransforms(nil, nil, []Transform{{Alias: "unwrap", Class: "io.debezium.transforms.ExtractNewRecordStat"}})
	if !errors.Is(err, ErrConfigBuild) || !errors.Is(err, knowledge.ErrUnknownEntry) {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(err.Error(), "nearest known: io.debezium.transforms.ExtractNewRecordState") {
		t.Fatalf("err does not name the nearest class: %v", err)
	}
}

func TestAddTransformsRefusals(t *testing.T) {
	b := builder(t)
	base := map[string]string{"transforms": "unwrap", "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"}
	for name, c := range map[string]struct {
		base  map[string]string
		preds []Predicate
		ts    []Transform
		in    string
	}{
		"unknown key":       {ts: []Transform{{Alias: "u", Class: "ExtractNewRecordState", Config: map[string]string{"delete.tombstone.handling": "drop"}}}, in: "nearest known key: delete.tombstone.handling.mode"},
		"bad value":         {ts: []Transform{{Alias: "u", Class: "ExtractNewRecordState", Config: map[string]string{"delete.tombstone.handling.mode": "keep"}}}, in: "must be one of"},
		"alias taken":       {base: base, ts: []Transform{{Alias: "unwrap", Class: "ExtractNewRecordState"}}, in: "already used"},
		"bad alias":         {ts: []Transform{{Alias: "1x", Class: "ExtractNewRecordState"}}, in: "alias"},
		"sink as SMT":       {ts: []Transform{{Alias: "s", Class: "io.debezium.connector.jdbc.JdbcSinkConnector"}}, in: "not a smt"},
		"abstract":          {ts: []Transform{{Alias: "m", Class: "mask-field"}}, in: "name a variant"},
		"missing predicate": {ts: []Transform{{Alias: "u", Class: "ExtractNewRecordState", Predicate: "p"}}, in: "not configured"},
		"negate alone":      {ts: []Transform{{Alias: "u", Class: "ExtractNewRecordState", Negate: true}}, in: "negate without"},
		"reserved key":      {ts: []Transform{{Alias: "u", Class: "ExtractNewRecordState", Config: map[string]string{"type": "x"}}}, in: "already set"},
		"file reference":    {ts: []Transform{{Alias: "u", Class: "ExtractNewRecordState", Config: map[string]string{"add.fields": "${file:/etc/passwd:x}"}}}, in: "reference"},
		"newline":           {ts: []Transform{{Alias: "u", Class: "ExtractNewRecordState", Config: map[string]string{"add.fields": "a\nb"}}}, in: "not one config value"},
		"predicate key":     {preds: []Predicate{{Alias: "p", Class: "topic-name-matches", Config: map[string]string{"regex": "x"}}}, in: "nearest known key: pattern"},
		"class injection":   {ts: []Transform{{Alias: "u", Class: "${env:X}"}}, in: "not a class name"},
	} {
		_, err := b.AddTransforms(c.base, c.preds, c.ts)
		if !errors.Is(err, ErrConfigBuild) || !strings.Contains(err.Error(), c.in) {
			t.Errorf("%s: err = %v, want %q", name, err, c.in)
		}
	}
}

func jdbcSinkOptions() SinkOptions {
	return SinkOptions{
		Sink:   "debezium-jdbc-sink",
		Topics: []string{"pg.public.orders"},
		Config: map[string]string{
			"connection.url":      "jdbc:postgresql://postgres:5432/orders",
			"connection.username": "sink_writer",
			"connection.password": "${secret:sink_password}",
			"insert.mode":         "upsert",
			"primary.key.mode":    "record_key",
			"delete.enabled":      "true",
			"schema.evolution":    "basic",
			"tasks.max":           "1",
			"key.converter":       "org.apache.kafka.connect.json.JsonConverter",
		},
	}
}

func TestSinkConfig(t *testing.T) {
	b := builder(t)
	cfg, err := b.SinkConfig(jdbcSinkOptions())
	if err != nil {
		t.Fatalf("SinkConfig: %v", err)
	}
	if cfg["connector.class"] != "io.debezium.connector.jdbc.JdbcSinkConnector" || cfg["topics"] != "pg.public.orders" {
		t.Fatalf("cfg = %v", cfg)
	}
	if cfg["connection.password"] != "${secret:sink_password}" || cfg["key.converter"] == "" {
		t.Fatalf("cfg = %v", cfg)
	}
	params, err := SinkRegistrationParams("pg-orders-sink", cfg, false)
	if err != nil {
		t.Fatalf("SinkRegistrationParams: %v", err)
	}
	var p struct {
		Connector string            `json:"connector"`
		Config    map[string]string `json:"config"`
	}
	if err := json.Unmarshal(params, &p); err != nil || p.Connector != "pg-orders-sink" || len(p.Config) != len(cfg) {
		t.Fatalf("params = %s (%v)", params, err)
	}
	if SinkRegistrationAction != "connect_create_connector" {
		t.Fatalf("action = %s", SinkRegistrationAction)
	}
}

func TestSinkConfigRefusals(t *testing.T) {
	b := builder(t)
	for name, c := range map[string]struct {
		edit func(*SinkOptions)
		in   string
	}{
		"plaintext password": {func(o *SinkOptions) { o.Config["connection.password"] = "hunter2" }, "${secret:name} reference"},
		"password in url":    {func(o *SinkOptions) { o.Config["connection.url"] = "jdbc:postgresql://postgres:5432/orders?password=x" }, "embeds a password"},
		"userinfo in url":    {func(o *SinkOptions) { o.Config["connection.url"] = "jdbc:postgresql://u:p@postgres:5432/orders" }, "embeds a password"},
		"missing required":   {func(o *SinkOptions) { delete(o.Config, "connection.username") }, "needs connection.username"},
		"unknown key":        {func(o *SinkOptions) { o.Config["pk.mode"] = "record_key" }, "no key"},
		"no topics":          {func(o *SinkOptions) { o.Topics = nil }, "topics"},
		"both topic forms":   {func(o *SinkOptions) { o.TopicsRegex = "pg.*" }, "not both"},
		"bad topic":          {func(o *SinkOptions) { o.Topics = []string{"a b"} }, "not a Kafka topic"},
		"class in config":    {func(o *SinkOptions) { o.Config["connector.class"] = "x" }, "set by the builder"},
		"unknown sink":       {func(o *SinkOptions) { o.Sink = "io.debezium.connector.jdbc.JdbcSink" }, "nearest known: io.debezium.connector.jdbc.JdbcSinkConnector"},
		"server sink":        {func(o *SinkOptions) { o.Sink = "http" }, ""},
		"smt as sink":        {func(o *SinkOptions) { o.Sink = "io.debezium.transforms.ExtractNewRecordState" }, "not a sink"},
	} {
		o := jdbcSinkOptions()
		c.edit(&o)
		_, err := b.SinkConfig(o)
		if !errors.Is(err, ErrConfigBuild) || !strings.Contains(err.Error(), c.in) {
			t.Errorf("%s: err = %v, want %q", name, err, c.in)
		}
	}
}

func TestSinkConfigWithUnwrap(t *testing.T) {
	b := builder(t)
	o := jdbcSinkOptions()
	o.Sink = "io.confluent.connect.jdbc.JdbcSinkConnector"
	o.Config = map[string]string{"connection.url": "jdbc:postgresql://postgres:5432/orders", "pk.mode": "record_key", "insert.mode": "upsert"}
	o.Transforms = []Transform{{Alias: "unwrap", Class: "io.debezium.transforms.ExtractNewRecordState"}}
	cfg, err := b.SinkConfig(o)
	if err != nil {
		t.Fatalf("SinkConfig: %v", err)
	}
	if cfg["transforms"] != "unwrap" || cfg["pk.mode"] != "record_key" {
		t.Fatalf("cfg = %v", cfg)
	}
}

func TestSinkRegistrationParamsRefusals(t *testing.T) {
	cfg := map[string]string{"connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector"}
	if _, err := SinkRegistrationParams("bad name", cfg, false); !errors.Is(err, ErrConfigBuild) {
		t.Fatalf("bad name: %v", err)
	}
	if _, err := SinkRegistrationParams("s", map[string]string{}, false); !errors.Is(err, ErrConfigBuild) {
		t.Fatalf("no class: %v", err)
	}
	if _, err := SinkRegistrationParams("s", map[string]string{"connector.class": "x", "name": "other"}, false); !errors.Is(err, ErrConfigBuild) {
		t.Fatalf("name mismatch: %v", err)
	}
}

func TestCredentialKey(t *testing.T) {
	for k, want := range map[string]bool{
		"connection.password":                                   true,
		"key.converter":                                         false,
		"value.converter.schemas.enable":                        false,
		"value.converter.basic.auth.user.info":                  false,
		"value.converter.schema.registry.ssl.keystore.password": true,
		"api.key":             true,
		"pk.mode":             false,
		"message.key.columns": false,
	} {
		if got := credentialKey(k); got != want {
			t.Errorf("credentialKey(%s) = %v, want %v", k, got, want)
		}
	}
}

func TestAddTransformsRequiredPredicateKey(t *testing.T) {
	_, err := builder(t).AddTransforms(nil, []Predicate{{Alias: "p", Class: "topic-name-matches"}}, nil)
	if !errors.Is(err, ErrConfigBuild) || !strings.Contains(err.Error(), "needs pattern") {
		t.Fatalf("err = %v", err)
	}
}
