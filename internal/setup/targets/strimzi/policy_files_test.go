package strimzi

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/setup/kafkatopics"
)

// The chart's admission policy (deploy/helm/piperoom
// templates/setup-pipeline.yaml) reads two lists the runner owns, and
// celcheck runs the runner's own renders as allow cases. These tests keep the
// committed files equal to what the runner holds; PIPEROOM_UPDATE_POLICY_FILES=1
// rewrites them.
const chartDir = "../../../../deploy/helm/piperoom"

func compareOrUpdate(t *testing.T, path string, want []byte) {
	t.Helper()
	if os.Getenv("PIPEROOM_UPDATE_POLICY_FILES") == "1" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, want, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("%s is not what the runner holds (err %v): run PIPEROOM_UPDATE_POLICY_FILES=1 go test ./internal/setup/targets/strimzi/ -run PolicyFiles and commit it", path, err)
	}
}

func lines(keys map[string]bool) []byte {
	out := make([]string, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	sort.Strings(out)
	return []byte(strings.Join(out, "\n") + "\n")
}

// policyConnectorKeys is the union, over every plugin, of the config keys the
// runner's allow-list admits (transform and predicate keys aside: the policy
// matches their shape), without the keys it always refuses.
func policyConnectorKeys(t *testing.T) map[string]bool {
	t.Helper()
	if err := loadIndexTables(); err != nil {
		t.Fatal(err)
	}
	cand := map[string]bool{}
	add := func(ks ...string) {
		for _, k := range ks {
			cand[k] = true
		}
	}
	add(commonSourceKeys...)
	add(databaseKeys...)
	add(schemaHistoryKeys...)
	add(jdbcSinkExtraKeys...)
	for _, ks := range pluginKeys {
		add(ks...)
	}
	for _, m := range indexTuning {
		for k := range m {
			add(k)
		}
	}
	for k := range indexJDBC {
		add(k)
	}
	plugins := []string{"jdbc"}
	for p := range sourceClasses {
		plugins = append(plugins, p)
	}
	out := map[string]bool{}
	for k := range cand {
		if _, never := neverKeys[k]; never || transformKeyRE.MatchString(k) {
			continue
		}
		for _, p := range plugins {
			if allowedKey(p, k) {
				out[k] = true
				break
			}
		}
	}
	return out
}

func TestPolicyFilesConnectorKeys(t *testing.T) {
	keys := policyConnectorKeys(t)
	for _, k := range []string{"header.converter", "connector.class", "tasks.max", "heartbeat.action.query", "topics.regex"} {
		if keys[k] {
			t.Fatalf("the policy key list would admit %s", k)
		}
	}
	for k := range keys {
		if !policyKeyRE.MatchString(k) {
			t.Fatalf("key %q cannot be written into a CEL string literal", k)
		}
	}
	compareOrUpdate(t, filepath.Join(chartDir, "files/setup-connector-keys.txt"), lines(keys))
}

// tsv writes rows of tab-separated fields, sorted, one per line. A field
// must be a plain CEL string literal body: no quote, backslash, tab or
// newline.
func tsv(t *testing.T, rows [][]string) []byte {
	t.Helper()
	out := make([]string, 0, len(rows))
	seen := map[string]bool{}
	for _, r := range rows {
		for _, f := range r {
			if f == "" || strings.ContainsAny(f, "'\"\\\t\n") {
				t.Fatalf("row %q: field %q cannot be written into a CEL string literal", r, f)
			}
		}
		l := strings.Join(r, "\t")
		if !seen[l] {
			seen[l] = true
			out = append(out, l)
		}
	}
	sort.Strings(out)
	return []byte(strings.Join(out, "\n") + "\n")
}

// TestPolicyFilesURLParams writes each dialect's URL parameters: free (a
// plain value), enum (name=value, lower-cased: the runner compares values
// case-insensitively), the TLS parameter a URL must set (require; for
// mongodb any one of them), the label parameters (%20 allowed) and each
// dialect's default port.
func TestPolicyFilesURLParams(t *testing.T) {
	var rows [][]string
	add := func(d string, params map[string][]string) {
		for k, vs := range params {
			if !regexp.MustCompile(`^[a-z]{1,64}$`).MatchString(k) {
				t.Fatalf("%s parameter %q is not a lower-case name", d, k)
			}
			if vs == nil {
				rows = append(rows, []string{"free", d, k})
			}
			for _, v := range vs {
				rows = append(rows, []string{"enum", d, k + "=" + strings.ToLower(v)})
			}
		}
	}
	for d, dia := range jdbcDialects {
		add(d, dia.params)
		rows = append(rows, []string{"port", d, dia.port})
	}
	add("mongodb", mongoParams)
	rows = append(rows, []string{"port", "mongodb", "27017"})
	for d, k := range jdbcRequiredParams {
		rows = append(rows, []string{"require", d, k})
	}
	rows = append(rows, []string{"require", "mongodb", "tls"}, []string{"require", "mongodb", "ssl"})
	for k := range jdbcLabelParams {
		rows = append(rows, []string{"label", k})
	}
	for _, r := range rows {
		if r[0] == "free" || r[0] == "enum" {
			for _, bad := range []string{"hostnameincertificate", "allowloadlocalinfile", "socketfactory", "sslfactory", "loggerfile", "autodeserialize", "currentschema"} {
				if strings.HasPrefix(r[2], bad) {
					t.Fatalf("the policy URL parameter list would admit %s", bad)
				}
			}
		}
	}
	compareOrUpdate(t, filepath.Join(chartDir, "files/setup-url-params.txt"), tsv(t, rows))
}

// TestPolicyFilesConnectorValues writes the keys the runner pins to a set of
// values (exact, or fold: compared lower-cased), the Secret key each
// credential key may reference, and the keys each connector class must set.
func TestPolicyFilesConnectorValues(t *testing.T) {
	var rows [][]string
	pin := func(k string, fold bool, vs []string) {
		for _, v := range vs {
			if fold {
				rows = append(rows, []string{"fold", k, strings.ToLower(v)})
			} else {
				rows = append(rows, []string{"exact", k, v})
			}
		}
	}
	for k, pv := range pinnedValues {
		pin(k, pv.fold, pv.values)
	}
	for k, vs := range jdbcClassKeys {
		pin(k, false, vs)
	}
	for _, c := range []string{"producer", "consumer"} {
		for k, vs := range schemaHistoryTLS {
			pin("schema.history.internal."+c+"."+k, false, vs)
		}
		for _, k := range []string{"ssl.truststore.password", "ssl.keystore.password"} {
			pin("schema.history.internal."+c+"."+k, false, []string{strimziStorePassword})
		}
	}
	for k, sk := range credentialKeys {
		rows = append(rows, []string{"cred", k, sk})
	}
	for plugin, k := range requiredSourceKeys {
		rows = append(rows, []string{"require", sourceClasses[plugin], k})
	}
	rows = append(rows, []string{"require", sourceClasses["mongodb"], "mongodb.connection.string"},
		[]string{"require", sourceClasses["jdbc"], "connection.url"})
	compareOrUpdate(t, filepath.Join(chartDir, "files/setup-connector-values.txt"), tsv(t, rows))
}

// TestPolicyFilesSMT writes the transform and predicate classes a config
// may name (sinktype: a JDBC sink only), each class's keys and the values of
// the keys the index lists them for.
func TestPolicyFilesSMT(t *testing.T) {
	if err := loadIndexTables(); err != nil {
		t.Fatal(err)
	}
	var rows [][]string
	for cls := range indexClasses {
		_, denied := deniedSMTClasses[smtBaseClass(cls)]
		switch {
		case !denied:
			rows = append(rows, []string{"type", cls})
		case sinkOnlySMTs[cls]:
			rows = append(rows, []string{"sinktype", cls})
		}
		if denied && !sinkOnlySMTs[cls] {
			continue
		}
		for sub, key := range indexSMTKeys[cls] {
			if _, no := deniedSMTKeys[smtBaseClass(cls)][sub]; no {
				continue
			}
			rows = append(rows, []string{"key", cls, sub})
			if tableNameSMTKeys[smtBaseClass(cls)][sub] {
				rows = append(rows, []string{"nodot", cls, sub})
			}
			for _, v := range key.Values {
				rows = append(rows, []string{"enum", cls, sub, v})
			}
		}
	}
	for _, r := range rows {
		if r[0] == "key" && r[2] == "route.by.field" {
			t.Fatal("route.by.field is in the SMT key list")
		}
	}
	compareOrUpdate(t, filepath.Join(chartDir, "files/setup-smt.txt"), tsv(t, rows))
}

// TestPolicyFilesPatterns writes the runner's patterns the policy matches
// with: the JDBC URL shapes per dialect, the MongoDB string, a parameter
// value and label, a transform key and name list, a topic name.
func TestPolicyFilesPatterns(t *testing.T) {
	rows := [][]string{
		{"mongodb", mongoURLRE.String()},
		{"value", jdbcValueRE.String()},
		{"label", jdbcLabelRE.String()},
		{"transformkey", transformKeyRE.String()},
		{"transformnames", transformNamesRE.String()},
		{"topic", kafkatopics.NamePattern},
		{"dbname", dbNameRE.String()},
	}
	for d, re := range jdbcURLShapes {
		rows = append(rows, []string{"jdbc." + d, re.String()})
	}
	for _, r := range rows {
		if strings.Contains(r[1], "'") {
			t.Fatalf("pattern %s cannot be a CEL raw string", r[0])
		}
	}
	// tsv refuses a backslash; a pattern needs them, and a CEL raw string
	// r'...' keeps them as written.
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r[0]+"\t"+r[1])
	}
	sort.Strings(out)
	compareOrUpdate(t, filepath.Join(chartDir, "files/setup-patterns.txt"), []byte(strings.Join(out, "\n")+"\n"))
}

// TestPolicyFilesRenders writes what the runner sends for the chart's CI
// values (ci/setup/pipeline-values.yaml: pipeline orders, Kafka prod-kafka,
// namespace data, topic prefix pg, registry registry.data.svc:5000, database
// host pg, schema registry http://registry.data.svc:8081) as celcheck allow
// cases: the policy must admit every object the runner writes (correctness 10).
func TestPolicyFilesRenders(t *testing.T) {
	h := newHarness(t, "", nil)
	withConnectFacts(h)
	h.env.Targets = append(h.env.Targets,
		events.SetupTarget{Kind: targetImageRegistry, Address: "registry.data.svc:5000"},
		events.SetupTarget{Kind: targetSchemaRegistry, Address: "registry.data.svc:8081"})
	ref := func(k string) string { return "${secrets:" + testNS + "/" + h.d.dbSecretName() + ":" + k + "}" }
	var tls []string
	for _, c := range []string{"producer", "consumer"} {
		p := "schema.history.internal." + c + "."
		tls = append(tls, p+"security.protocol", "SSL",
			p+"ssl.truststore.location", strimziTrustStore, p+"ssl.truststore.type", "PKCS12", p+"ssl.truststore.password", strimziStorePassword,
			p+"ssl.keystore.location", strimziKeyStore, p+"ssl.keystore.type", "PKCS12", p+"ssl.keystore.password", strimziStorePassword)
	}
	kv := func(base map[string]string, more ...string) map[string]string {
		for i := 0; i+1 < len(more); i += 2 {
			base[more[i]] = more[i+1]
		}
		return base
	}
	connector := func(plugin string, cfg map[string]string) map[string]any {
		return map[string]any{"kind": "KafkaConnector", "template": "connector", "params": map[string]any{
			"connector": "orders-src", "plugin": plugin, "topic_prefix": "pg", "config": cfg}}
	}
	type apply struct {
		name, kind, obj string
		params          map[string]any
	}
	applies := []apply{
		{"render: KafkaConnect", "KafkaConnect", h.d.connectName(), connectApply(func(p map[string]any) { p["registry"] = "registry.data.svc:5000" })},
		{"render: postgres source", "KafkaConnector", "orders-src", connector("postgres", map[string]string{
			"database.hostname": "pg", "database.port": "5432", "database.user": ref("username"), "database.password": ref("password"),
			"database.dbname": "orders", "database.sslmode": "verify-full", "plugin.name": "pgoutput", "slot.name": "orders",
			"publication.name": "orders", "table.include.list": "public.orders", "topic.prefix": "pg",
			"transforms": "unwrap", "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
			"transforms.unwrap.delete.tombstone.handling.mode": "rewrite",
			"value.converter": "io.confluent.connect.avro.AvroConverter", "value.converter.schema.registry.url": "http://registry.data.svc:8081",
		})},
		{"render: mysql source with TLS schema history", "KafkaConnector", "orders-src", connector("mysql", kv(map[string]string{
			"database.hostname": "pg", "database.port": "5432", "database.user": ref("username"), "database.password": ref("password"),
			"database.server.id": "5401", "database.ssl.mode": "verify_identity", "topic.prefix": "pg",
			"schema.history.internal.kafka.topic":             "schemahistory.pg",
			"schema.history.internal.kafka.bootstrap.servers": "prod-kafka-kafka-bootstrap.data.svc:9093",
		}, tls...))},
		{"render: jdbc sink", "KafkaConnector", "orders-src", connector("jdbc", map[string]string{
			"connection.url":      "jdbc:postgresql://pg:5432/warehouse?sslmode=verify-full&ApplicationName=pipe%20room&prepareThreshold=0",
			"connection.username": ref("username"), "connection.password": ref("password"),
			"topics": "pg.public.orders", "errors.tolerance": "all", "errors.deadletterqueue.topic.name": "pg.dlq",
			"transforms": "route", "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
			"transforms.route.regex": "([^.]+)\\.([^.]+)\\.([^.]+)", "transforms.route.replacement": "$3",
		})},
		{"render: canary topic", "KafkaTopic", h.d.canaryTopicName(), map[string]any{"kind": "KafkaTopic", "template": "canary-topic",
			"params": map[string]any{"topic_prefix": "pg", "schema": "public", "table": CanaryTable}}},
		{"render: connect user", "KafkaUser", h.d.connectName(), map[string]any{"kind": "KafkaUser", "template": "connect-user",
			"params": map[string]any{"topic_prefix": "pg", "auth": "tls"}}},
		{"render: read user", "KafkaUser", testPipeline + "-piperoom-ro", map[string]any{"kind": "KafkaUser", "template": "read-user",
			"params": map[string]any{"topic_prefix": "pg", "auth": "scram-sha-512"}}},
	}
	type tc struct {
		Name  string         `json:"name"`
		Allow bool           `json:"allow"`
		Obj   map[string]any `json:"obj"`
	}
	var out []tc
	for _, a := range applies {
		path := h.shell(a.kind, a.obj, map[string]any{})
		if _, err := run(t, h, "k8s_apply", a.params); err != nil {
			t.Fatalf("%s: %v", a.name, err)
		}
		obj := h.api.get(path)
		out = append(out, tc{Name: a.name, Allow: true, Obj: map[string]any{
			"kind": obj["kind"], "metadata": map[string]any{"name": a.obj}, "spec": obj["spec"]}})
	}
	b, err := json.MarshalIndent(out, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	compareOrUpdate(t, filepath.Join(chartDir, "ci/setup/celcheck/renders.json"), append(b, '\n'))
}

var policyKeyRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
