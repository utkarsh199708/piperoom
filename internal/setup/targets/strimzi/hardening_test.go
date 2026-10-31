package strimzi

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// The canary-topic template renders only the ladder's canary topic: any
// other table, and so any pipeline topic, is refused before any request, and
// ValidateTopic refuses a hand-made spec naming another topic of the prefix.
func TestCanaryTopicIsOnlyTheCanary(t *testing.T) {
	h := newHarness(t, "", nil)
	h.shell("KafkaTopic", "orders-canary", map[string]any{})
	for _, table := range []string{"orders", "piperoom_canary2", "t"} {
		p := topicApply()
		p["params"].(map[string]any)["table"] = table
		if _, err := run(t, h, "k8s_apply", p); !errors.Is(err, ErrParams) {
			t.Errorf("table %s: err = %v, want ErrParams", table, err)
		}
	}
	if len(h.api.requests) != 0 {
		t.Fatalf("a refused render reached the API: %v", h.api.requests)
	}
	for name, topic := range map[string]string{
		"a pipeline topic":  "pg.public.orders",
		"no schema":         "pg.piperoom_canary",
		"another prefix":    "payments.public.piperoom_canary",
		"a dotted schema":   "pg.a.b.piperoom_canary",
		"the bare prefix":   "pg",
		"canary as a table": "pg.public.piperoom_canary.x",
	} {
		spec := map[string]any{"topicName": topic, "partitions": 1, "replicas": 1}
		if err := h.d.ValidateTopic(spec, "pg"); !errors.Is(err, ErrField) {
			t.Errorf("%s: err = %v, want ErrField", name, err)
		}
	}
	if err := h.d.ValidateTopic(map[string]any{"topicName": "pg.public.piperoom_canary", "partitions": 1, "replicas": 1}, "pg"); err != nil {
		t.Fatalf("the canary was refused: %v", err)
	}
}

// A KafkaTopic that already reconciles another topic is not re-pointed.
func TestCanaryTopicRefusesAnAdoptedTopic(t *testing.T) {
	h := newHarness(t, "", nil)
	path := h.shell("KafkaTopic", "orders-canary", map[string]any{})
	obj := h.api.get(path)
	obj["status"] = map[string]any{"topicName": "pg.public.orders"}
	h.api.put(path, obj)
	if _, err := run(t, h, "k8s_apply", topicApply()); !errors.Is(err, ErrField) {
		t.Fatalf("err = %v, want ErrField", err)
	}
	for _, r := range h.api.requests {
		if strings.HasPrefix(r, "PATCH") {
			t.Fatalf("a write reached the API: %s", r)
		}
	}
}

// k8s_apply never patches a same-named object the pipeline release does not
// own.
func TestApplyRefusesUnownedObject(t *testing.T) {
	h := newHarness(t, "", nil)
	path := objectPath(testNS, "KafkaTopic", "orders-canary")
	h.api.put(path, map[string]any{"kind": "KafkaTopic", "metadata": map[string]any{"name": "orders-canary", "labels": map[string]any{},
		"annotations": map[string]any{PauseAnnotation: "true"}}, "spec": map[string]any{}})
	if _, err := run(t, h, "k8s_apply", topicApply()); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("err = %v, want ErrNotOwned", err)
	}
	sec := newHarness(t, "", map[string]string{"pw": "v"})
	sp := coreObjectPath(testNS, "secrets", "orders-db-credentials")
	sec.api.put(sp, map[string]any{"kind": "Secret", "metadata": map[string]any{"name": "orders-db-credentials", "labels": map[string]any{"app.kubernetes.io/instance": "x"}}})
	if _, err := run(t, sec, "k8s_secret_apply", map[string]any{"keys": map[string]string{"password": "${secret:pw}"}}); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("secret: err = %v, want ErrNotOwned", err)
	}
	for _, r := range append(h.api.requests, sec.api.requests...) {
		if strings.HasPrefix(r, "PATCH") {
			t.Fatalf("a write reached the API: %s", r)
		}
	}
}

func connectorSpec(plugin string, cfg map[string]any) map[string]any {
	return map[string]any{"class": sourceClasses[plugin], "tasksMax": 1, "config": cfg}
}

// The connector config allow-list: keys outside the plugin's table and the
// refused shapes fail with the path named; the setup's own keys pass.
func TestConnectorConfigAllowList(t *testing.T) {
	h := newHarness(t, "", nil)
	ref := "${secrets:" + testNS + "/orders-db-credentials:password}"
	good := map[string]any{
		"database.hostname": "pg", "database.port": "5432", "database.user": "cdc", "database.password": ref,
		"database.dbname": "orders", "plugin.name": "pgoutput", "slot.name": "s", "publication.name": "p",
		"table.include.list": "public.orders", "topic.prefix": "pg", "database.sslmode": "verify-ca",
		"max.batch.size": "2048", // a connector-scope key of the knowledge index's tuning entries
		"transforms":     "unwrap", "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
		"transforms.unwrap.drop.tombstones": "false",
		"key.converter":                     "org.apache.kafka.connect.json.JsonConverter",
	}
	if err := h.d.ValidateConnector(connectorSpec("postgres", good), testNS, testKafka, ""); err != nil {
		t.Fatalf("a good postgres config was refused: %v", err)
	}
	mysql := map[string]any{"database.hostname": "pg", "database.port": "3306", "database.server.id": "1", "topic.prefix": "pg", "database.ssl.mode": "verify_identity",
		"schema.history.internal.producer.security.protocol": "SSL", "schema.history.internal.consumer.security.protocol": "SSL",
		"schema.history.internal.kafka.topic": "pg.history", "schema.history.internal.kafka.bootstrap.servers": "prod-kafka-kafka-bootstrap.data.svc:9093"}
	if err := h.d.ValidateConnector(connectorSpec("mysql", mysql), testNS, testKafka, ""); err != nil {
		t.Fatalf("a good mysql config was refused: %v", err)
	}
	for name, tc := range map[string]struct {
		plugin string
		key    string
		value  string
	}{
		"an unknown key":              {"postgres", "snapshot.select.statement.overrides", "x"},
		"a producer override":         {"postgres", "producer.override.bootstrap.servers", "evil:9092"},
		"a consumer override":         {"postgres", "consumer.override.sasl.jaas.config", "x"},
		"a bootstrap":                 {"postgres", "bootstrap.servers", "evil:9092"},
		"schema history on postgres":  {"postgres", "schema.history.internal.kafka.bootstrap.servers", "evil:9092"},
		"initial statements":          {"postgres", "database.initial.statements", "DROP TABLE x"},
		"heartbeat query":             {"postgres", "heartbeat.action.query", "DELETE FROM x"},
		"a config provider":           {"postgres", "config.providers", "file"},
		"a provider class":            {"postgres", "config.providers.file.class", "org.apache.kafka.common.config.provider.FileConfigProvider"},
		"an unknown converter":        {"postgres", "value.converter", "com.evil.Converter"},
		"a transform class off-index": {"postgres", "transforms.x.type", "com.evil.Transform"},
		"a transform not listed":      {"postgres", "transforms.y.type", "io.debezium.transforms.ExtractNewRecordState"},
		"ssl disabled":                {"postgres", "database.sslmode", "disable"},
		"ssl optional":                {"postgres", "database.sslmode", "prefer"},
		"mysql ssl disabled":          {"mysql", "database.ssl.mode", "disabled"},
		"sqlserver unencrypted":       {"sqlserver", "database.encrypt", "false"},
		"a file path":                 {"postgres", "database.sslrootcert", "/etc/passwd"},
		"a credential in a jdbc url":  {"jdbc", "connection.url", "jdbc:postgresql://u:p@wh:5432/db"},
		"a password param in a url":   {"jdbc", "connection.url", "jdbc:postgresql://wh:5432/db?password=x"},
		"a credential in a mongo url": {"mongodb", "mongodb.connection.string", "mongodb://root:pw@pg:27017/"},
		"a reference inside a url":    {"jdbc", "connection.url", "jdbc:postgresql://wh:5432/db?user=${secrets:data/orders-db-credentials:u}"},
		"a topics regex on the sink":  {"jdbc", "topics.regex", ".*"},
		"tasks.max in config":         {"postgres", "tasks.max", "4"},
		"a mongo key on postgres":     {"postgres", "mongodb.connection.string", "mongodb://pg:27017"},
		"a database key on mongodb":   {"mongodb", "database.hostname", "pg"},
		"a class inside a transform":  {"postgres", "transforms.x.converter", "com.evil.C"},
	} {
		cfg := map[string]any{"topic.prefix": "pg", tc.key: tc.value}
		if strings.HasPrefix(tc.key, "transforms.x") {
			cfg["transforms"] = "x"
		}
		err := h.d.ValidateConnector(connectorSpec(tc.plugin, cfg), testNS, testKafka, "")
		if !errors.Is(err, ErrField) || !strings.Contains(err.Error(), tc.key) && !strings.Contains(err.Error(), "spec.config") {
			t.Errorf("%s: err = %v, want ErrField naming the key", name, err)
		}
	}
	if err := h.d.ValidateConnector(map[string]any{"class": "io.debezium.connector.evil.Connector", "config": map[string]any{}}, testNS, testKafka, ""); !errors.Is(err, ErrField) {
		t.Errorf("an unknown Debezium class: %v", err)
	}
}

// The source host and port of a connector are a source target of the room,
// checked at preview and write; a schema history bootstrap must be one of
// the room Kafka CR's listeners.
func TestConnectorBoundToRoomTargets(t *testing.T) {
	ref := "${secrets:" + testNS + "/orders-db-credentials:password}"
	for name, tc := range map[string]struct {
		plugin string
		cfg    map[string]string
		ok     bool
	}{
		"the room's source":   {"postgres", map[string]string{"database.hostname": "pg", "database.port": "5432", "database.password": ref, "topic.prefix": "pg", "database.sslmode": "verify-full"}, true},
		"case of the host":    {"postgres", map[string]string{"database.hostname": "PG", "database.port": "5432", "topic.prefix": "pg", "database.sslmode": "verify-full"}, true},
		"another host":        {"postgres", map[string]string{"database.hostname": "evil.example", "database.port": "5432", "topic.prefix": "pg"}, false},
		"another port":        {"postgres", map[string]string{"database.hostname": "pg", "database.port": "6432", "topic.prefix": "pg"}, false},
		"no host":             {"postgres", map[string]string{"topic.prefix": "pg"}, false},
		"mongo on the source": {"mongodb", map[string]string{"mongodb.connection.string": "mongodb://pg:5432/?replicaSet=rs0&tls=true", "topic.prefix": "pg"}, true},
		"mongo elsewhere":     {"mongodb", map[string]string{"mongodb.connection.string": "mongodb://pg:5432,evil:27017/", "topic.prefix": "pg"}, false},
		"history on the listener": {"mysql", map[string]string{"database.hostname": "pg", "database.port": "5432", "topic.prefix": "pg", "database.server.id": "1", "database.ssl.mode": "verify_identity",
			"schema.history.internal.producer.security.protocol": "SSL", "schema.history.internal.consumer.security.protocol": "SSL",
			"schema.history.internal.kafka.topic": "pg.history", "schema.history.internal.kafka.bootstrap.servers": "prod-kafka-kafka-bootstrap.data.svc:9093"}, true},
		"history elsewhere": {"mysql", map[string]string{"database.hostname": "pg", "database.port": "5432", "topic.prefix": "pg", "database.server.id": "1",
			"schema.history.internal.kafka.topic": "pg.history", "schema.history.internal.kafka.bootstrap.servers": "evil.example:9092"}, false},
	} {
		h := newHarness(t, "", nil)
		withConnectFacts(h)
		path := h.shell("KafkaConnector", "orders-src", map[string]any{})
		_, err := run(t, h, "k8s_apply", map[string]any{"kind": "KafkaConnector", "template": "connector", "params": map[string]any{
			"connector": "orders-src", "plugin": tc.plugin, "config": tc.cfg,
		}})
		if tc.ok != (err == nil) {
			t.Errorf("%s: err = %v, want ok %v", name, err, tc.ok)
		}
		if !tc.ok {
			if ann := h.api.get(path)["metadata"].(map[string]any)["annotations"].(map[string]any); ann[PauseAnnotation] != "true" {
				t.Errorf("%s: a refused connector was unpaused", name)
			}
			for _, r := range h.api.requests {
				if strings.HasPrefix(r, "PATCH") {
					t.Errorf("%s: a write reached the API: %s", name, r)
				}
			}
		}
	}
}

// k8s_get answers the object redacted, without a Pod's spec or the
// last-applied copy, and keeps what the ladder reads.
func TestK8sGetRedactsAndTrims(t *testing.T) {
	h := newHarness(t, "", nil)
	path := h.shell("KafkaConnector", "orders-src", map[string]any{"config": map[string]any{"database.password": "hunter2-literal-pw"}})
	obj := h.api.get(path)
	obj["metadata"].(map[string]any)["annotations"].(map[string]any)[lastApplied] = `{"spec":{"config":{"database.password":"hunter2-literal-pw"}}}`
	obj["status"] = map[string]any{
		"conditions": []any{map[string]any{"type": "Ready", "status": "True"}},
		"connectorStatus": map[string]any{
			"connector": map[string]any{"state": "FAILED", "trace": "org.postgresql.util.PSQLException: password=hunter2-literal-pw rejected"},
			"tasks":     []any{map[string]any{"id": 0, "state": "RUNNING"}},
		},
	}
	h.api.put(path, obj)
	res, err := run(t, h, "k8s_get", map[string]any{"kind": "KafkaConnector", "name": "orders-src"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res.Data)
	if strings.Contains(string(b), "hunter2") || strings.Contains(string(b), lastApplied) {
		t.Fatalf("k8s_get answered a credential or the last-applied copy: %s", b)
	}
	if !strings.Contains(string(b), `"connectorStatus"`) || !strings.Contains(string(b), `"KafkaConnector"`) || !strings.Contains(string(b), `"RUNNING"`) {
		t.Fatalf("k8s_get dropped what the ladder reads: %s", b)
	}

	pod := objectPath(testNS, "Pod", "orders-connect-connect-0")
	h.api.put(pod, map[string]any{"kind": "Pod", "metadata": map[string]any{"name": "orders-connect-connect-0"},
		"spec":   map[string]any{"containers": []any{map[string]any{"name": "c", "image": "img", "env": []any{map[string]any{"name": "X", "value": "secret-env-value"}}}}, "volumes": []any{}},
		"status": map[string]any{"phase": "Running"}})
	res, err = run(t, h, "k8s_get", map[string]any{"kind": "Pod", "name": "orders-connect-connect-0"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ = json.Marshal(res.Data)
	if strings.Contains(string(b), "secret-env-value") || strings.Contains(string(b), "volumes") || !strings.Contains(string(b), `"Running"`) || !strings.Contains(string(b), `"img"`) {
		t.Fatalf("Pod view = %s", b)
	}
}

// The API base URL is parsed: https anywhere, http only to a loopback IP,
// never a host that merely starts with one.
func TestNewAPIBaseURL(t *testing.T) {
	for url, ok := range map[string]bool{
		"https://10.96.0.1:443":          true,
		"https://kubernetes.default.svc": true,
		"https://[fd00::1]:6443":         true,
		"http://127.0.0.1:8080":          true,
		"http://[::1]:8080":              true,
		"http://127.0.0.1.evil.example":  false,
		"http://127.0.0.1@evil.example":  false,
		"http://localhost:8080":          false,
		"http://10.0.0.1:8080":           false,
		"https://user:pw@10.96.0.1":      false,
		"https://10.96.0.1/api":          false,
		"https://10.96.0.1?x=1":          false,
		"ftp://10.96.0.1":                false,
		"":                               false,
		"https://":                       false,
	} {
		_, err := NewAPI(APIConfig{BaseURL: url, Token: func() (string, error) { return "t", nil }})
		if (err == nil) != ok {
			t.Errorf("NewAPI(%q) err = %v, want ok %v", url, err, ok)
		}
	}
}
