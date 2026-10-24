package knowledge

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// The "Done when" of U10a: at least 25 tuning entries, each citing the
// docs and the source its defaults were read from, each with the symptoms
// that justify it, and setup_config_help {kind: tuning} answering through
// the same Lookup as the SMT and sink kinds.
func TestTuningIndexCoverage(t *testing.T) {
	ix := loadIndex(t)
	tuning := ix.Tuning("")
	if len(tuning) < 25 {
		t.Fatalf("tuning index has %d entries, want at least 25", len(tuning))
	}
	for _, e := range tuning {
		if e.Kind != KindTuning {
			t.Errorf("%s: Tuning returned kind %s", e.Name, e.Kind)
		}
		if len(e.Symptoms) == 0 || len(e.Keys) == 0 || e.Scope == "" {
			t.Errorf("%s: symptoms %d, keys %d, scope %q", e.Name, len(e.Symptoms), len(e.Keys), e.Scope)
		}
		if e.Doc == "" || e.Source == "" {
			t.Errorf("%s: doc %q, source %q", e.Name, e.Doc, e.Source)
		}
	}
	// The knobs the row names, by the key a user or the agent would type.
	for q, want := range map[string]string{
		"KAFKA_HEAP_OPTS":              "connect-worker-heap",
		"max.batch.size":               "dbz-max-batch-size",
		"max.queue.size":               "dbz-max-queue-size",
		"poll.interval.ms":             "dbz-poll-interval",
		"producer.override.batch.size": "producer-batch-size",
		"producer.override.linger.ms":  "producer-linger",
		"compression.type":             "producer-compression",
		"producer.acks":                "producer-acks",
		"connect-worker-heap":          "connect-worker-heap",
	} {
		e, err := ix.Lookup(KindTuning, q)
		if err != nil || e.Name != want {
			t.Errorf("Lookup(tuning, %q) = %q, %v; want %q", q, e.Name, err, want)
		}
	}
	// Snapshot fetch size and tasks.max have an entry per source.
	for _, src := range []string{"postgres", "mysql", "sqlserver", "oracle", "mongodb"} {
		for _, name := range []string{"snapshot-fetch-size-" + src, "tasks-max-" + src} {
			e, err := ix.Lookup(KindTuning, name)
			if err != nil {
				t.Errorf("Lookup(tuning, %q) = %v", name, err)
				continue
			}
			if e.Connector != src {
				t.Errorf("%s: connector %q, want %q", name, e.Connector, src)
			}
			if !slices.ContainsFunc(ix.Tuning(src), func(x Entry) bool { return x.Name == name }) {
				t.Errorf("Tuning(%q) has no %s", src, name)
			}
		}
	}
	for _, q := range []string{"snapshot.fetch.size", "tasks.max"} {
		_, err := ix.Lookup(KindTuning, q)
		if !errors.Is(err, ErrUnknownEntry) || !strings.Contains(err.Error(), "-postgres") {
			t.Errorf("Lookup(tuning, %q) = %v, want an ambiguity naming the per-source entries", q, err)
		}
	}
}

// Defaults pinned to the source they were read from. A change here is a
// change of what the agent tells users, so it must come with a new source
// read, not an edit to make the test pass.
func TestTuningDefaults(t *testing.T) {
	ix := loadIndex(t)
	cases := []struct{ entry, key, want string }{
		{"dbz-max-batch-size", "max.batch.size", "2048"},
		{"dbz-max-queue-size", "max.queue.size", "8192"},
		{"dbz-max-queue-size-in-bytes", "max.queue.size.in.bytes", "0"},
		{"dbz-poll-interval", "poll.interval.ms", "500"},
		{"dbz-heartbeat-interval", "heartbeat.interval.ms", "0"},
		{"dbz-snapshot-max-threads", "snapshot.max.threads", "1"},
		{"dbz-incremental-snapshot-chunk-size", "incremental.snapshot.chunk.size", "1024"},
		{"dbz-retriable-restart", "errors.max.retries", "-1"},
		{"snapshot-fetch-size-postgres", "snapshot.fetch.size", "10240"},
		{"snapshot-fetch-size-mysql", "snapshot.fetch.size", "-2147483648"},
		{"snapshot-fetch-size-mysql", "min.row.count.to.stream.results", "1000"},
		{"snapshot-fetch-size-sqlserver", "snapshot.fetch.size", "2000"},
		{"snapshot-fetch-size-oracle", "snapshot.fetch.size", "10000"},
		{"snapshot-fetch-size-mongodb", "snapshot.fetch.size", "0"},
		{"query-fetch-size-sqlserver", "query.fetch.size", "10000"},
		{"oracle-log-mining-batch-size", "log.mining.batch.size.default", "20000"},
		{"connect-worker-heap", "KAFKA_HEAP_OPTS", "-Xms256M -Xmx2G"},
		{"connect-offset-flush", "offset.flush.interval.ms", "60000"},
		{"connect-task-shutdown-timeout", "task.shutdown.graceful.timeout.ms", "5000"},
		{"connect-client-override-policy", "connector.client.config.override.policy", "All"},
		{"producer-batch-size", "producer.override.batch.size", "16384"},
		{"producer-linger", "producer.override.linger.ms", "5"},
		{"producer-compression", "producer.override.compression.type", "none"},
		{"producer-acks", "producer.override.acks", "all"},
		{"producer-max-request-size", "producer.override.max.request.size", "1048576"},
		{"producer-in-flight", "producer.override.max.in.flight.requests.per.connection", "1"},
		{"producer-in-flight", "producer.override.enable.idempotence", "false"},
		{"consumer-max-poll-records", "consumer.override.max.poll.records", "500"},
		{"consumer-max-poll-interval", "consumer.override.max.poll.interval.ms", "300000"},
	}
	for _, c := range cases {
		e, err := ix.Lookup(KindTuning, c.entry)
		if err != nil {
			t.Errorf("Lookup(%s) = %v", c.entry, err)
			continue
		}
		k, ok := e.Key(c.key)
		if !ok || k.Default == nil || *k.Default != c.want {
			got := "<none>"
			if ok && k.Default != nil {
				got = *k.Default
			}
			t.Errorf("%s %s default = %s, want %s", c.entry, c.key, got, c.want)
		}
	}
	// linger.ms changed default in Kafka 4.0.0; the entry says when.
	e, _ := ix.Lookup(KindTuning, "producer-linger")
	if k, _ := e.Key("producer.override.linger.ms"); k.DefaultSince != "4.0.0" {
		t.Errorf("producer linger default_since = %q, want 4.0.0", k.DefaultSince)
	}
	if s, err := e.Supports("3.9.0"); err != nil || s != Supported {
		t.Errorf("producer-linger Supports(3.9.0) = %v, %v; want Supported", s, err)
	}
	if s, err := e.Supports("2.8.0"); err != nil || s != Unverified {
		t.Errorf("producer-linger Supports(2.8.0) = %v, %v; want Unverified", s, err)
	}
}

// Key lookup folds the producer and consumer prefixes and the Debezium
// Server source prefix, never across clients or kinds.
func TestTuningLookupByKey(t *testing.T) {
	ix := loadIndex(t)
	cases := []struct {
		kind Kind
		q    string
		want string // "" means ErrUnknownEntry
	}{
		{KindTuning, "debezium.source.max.batch.size", "dbz-max-batch-size"},
		{KindTuning, "batch.size", "producer-batch-size"},
		{KindTuning, "producer.batch.size", "producer-batch-size"},
		{KindTuning, "consumer.override.batch.size", ""},
		{KindTuning, "consumer.max.poll.records", "consumer-max-poll-records"},
		{KindTuning, "max.poll.records", "consumer-max-poll-records"},
		{KindTuning, "producer.override.max.poll.records", ""},
		{KindTuning, "log.mining.sleep.time.max.ms", "oracle-log-mining-sleep-time"},
		{KindTuning, "max.batch.size$Value", ""},
		{KindTuning, "producer.override.", ""},
		{KindTuning, "no.such.key", ""},
		{"", "max.batch.size", "dbz-max-batch-size"},
		{KindSMT, "max.batch.size", ""},
		{KindTuning, "extract-new-record-state", ""},
	}
	for _, c := range cases {
		e, err := ix.Lookup(c.kind, c.q)
		if c.want == "" {
			if !errors.Is(err, ErrUnknownEntry) {
				t.Errorf("Lookup(%q, %q) = %s, %v; want ErrUnknownEntry", c.kind, c.q, e.Name, err)
			}
			continue
		}
		if err != nil || e.Name != c.want {
			t.Errorf("Lookup(%q, %q) = %s, %v; want %s", c.kind, c.q, e.Name, err, c.want)
		}
	}
	// A key that only an SMT has is not a tuning key.
	if _, err := ix.Lookup(KindTuning, "delete.tombstone.handling.mode"); !errors.Is(err, ErrUnknownEntry) {
		t.Errorf("Lookup(tuning, an SMT key) = %v, want ErrUnknownEntry", err)
	}
	// Tuning(src) adds only that source's entries to the shared ones.
	for _, e := range ix.Tuning("postgres") {
		if e.Connector != "" && e.Connector != "postgres" {
			t.Errorf("Tuning(postgres) returned %s for %s", e.Name, e.Connector)
		}
	}
}

const baseTuning = `
- name: tune-a
  kind: tuning
  component: debezium
  runtime: connect
  scope: connector
  connector: postgres
  versions: ">=3.0.0.Final"
  summary: s
  keys:
    - {name: max.batch.size, default: "2048"}
  symptoms: [lag grows]
  checks: [c]
  doc: https://debezium.io/documentation/reference/3.2/connectors/postgresql.html
  source: https://repo1.maven.org/maven2/io/debezium/debezium-core/3.2.0.Final/debezium-core-3.2.0.Final-sources.jar
- name: tune-b
  kind: tuning
  component: kafka
  runtime: connect
  scope: env
  versions: ">=3.0.0"
  summary: s
  keys:
    - {name: KAFKA_HEAP_OPTS}
  symptoms: [OutOfMemoryError]
  checks: [c]
  doc: https://kafka.apache.org/documentation/#connect_running
  source: https://raw.githubusercontent.com/apache/kafka/4.1.0/bin/connect-distributed.sh
`

// A tuning entry without symptoms, with a class or an install, a key spelt
// for another scope or an unknown source fails the parse, so it fails
// start-up.
func TestParseTuningRejects(t *testing.T) {
	if _, err := parseIndexYAML(t, baseTuning); err != nil {
		t.Fatalf("base tuning index: %v", err)
	}
	cases := []struct{ name, old, new, wantErr string }{
		{"no symptoms", "symptoms: [lag grows]", "symptoms: []", "at least one symptom"},
		{"blank symptom", "symptoms: [lag grows]", `symptoms: ["  "]`, "empty symptom"},
		{"a class", "scope: connector", "scope: connector\n  class: io.example.Tune", "no class"},
		{"variants", "scope: connector", "scope: connector\n  variants: [Value]", "no class"},
		{"an install", "scope: connector", "scope: connector\n  install: bundled", "installs nothing"},
		{"a plugin_ref", "scope: connector", "scope: connector\n  plugin_ref: postgres", "installs nothing"},
		{"unknown scope", "scope: connector", "scope: cluster", `scope "cluster"`},
		{"no scope", "  scope: connector\n", "", `scope ""`},
		{"no keys", "keys:\n    - {name: max.batch.size, default: \"2048\"}", "keys: []", "at least one key"},
		{"env key in lower case", "{name: KAFKA_HEAP_OPTS}", "{name: kafka_heap_opts}", "not spelt as a key of scope env"},
		{"connector key in upper case", "{name: max.batch.size, default: \"2048\"}", "{name: MAX_BATCH_SIZE}", "not spelt as a key of scope connector"},
		{"unknown connector", "connector: postgres", "connector: db2", "not a connector in plugins.go"},
		{"connector on a Kafka entry", "scope: env", "scope: env\n  connector: postgres", "only a Debezium tuning entry"},
		{"vendor component", "component: kafka", "component: confluent", "tuning entry's component"},
		{"secret key", "{name: max.batch.size, default: \"2048\"}", "{name: max.batch.size, secret: true}", "never a credential"},
		{"since outside the range", "{name: max.batch.size, default: \"2048\"}", "{name: max.batch.size, since: 2.0.0.Final}", "outside the entry's versions"},
		{"doc off the allowed hosts", "doc: https://kafka.apache.org/", "doc: https://evil.example/", "doc:"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if !strings.Contains(baseTuning, c.old) {
				t.Fatalf("base tuning index has no %q", c.old)
			}
			_, err := parseIndexYAML(t, strings.Replace(baseTuning, c.old, c.new, 1))
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("ParseIndex = %v, want an error containing %q", err, c.wantErr)
			}
		})
	}
	// Symptoms, scope and connector belong to tuning entries only.
	smt := `
- name: smt-a
  kind: smt
  component: kafka
  runtime: connect
  class: org.example.SmtA
  versions: ">=3.0.0"
  summary: s
  install: bundled
  symptoms: [lag grows]
  checks: [c]
  doc: https://kafka.apache.org/documentation/#connect_transforms
  source: https://repo1.maven.org/maven2/org/apache/kafka/connect-transforms/4.1.0/connect-transforms-4.1.0-sources.jar
`
	if _, err := parseIndexYAML(t, smt); err == nil || !strings.Contains(err.Error(), "tuning entries only") {
		t.Errorf("an SMT with symptoms: err = %v, want a tuning-only error", err)
	}
}
