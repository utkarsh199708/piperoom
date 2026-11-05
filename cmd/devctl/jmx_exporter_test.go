package main

import (
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// jmxRule is one rule of dev/connect/jmx-exporter.yml.
type jmxRule struct {
	Pattern string            `yaml:"pattern"`
	Name    string            `yaml:"name"`
	Value   any               `yaml:"value"`
	Labels  map[string]string `yaml:"labels"`
}

// jmxSample is what the exporter makes of one MBean attribute.
type jmxSample struct {
	name   string
	labels map[string]string
}

var javaGroupRef = regexp.MustCompile(`\$(\d+)`)

// exportJMX applies the rules the way jmx_exporter 1.0.1 does
// (JmxCollector.processBeanValue): the match string is
// domain<bean properties><attribute keys>attribute: value, each pattern is
// compiled as ^.*(?:pattern).*$, the first rule that matches wins, names go
// through safeName, and a string value is exported only by a rule with a
// fixed value. ok is false when nothing is exported.
func exportJMX(t *testing.T, rules []jmxRule, match string, numeric bool) (jmxSample, bool) {
	t.Helper()
	for _, r := range rules {
		re, err := regexp.Compile("^.*(?:" + r.Pattern + ").*$")
		if err != nil {
			t.Fatalf("pattern %q: %v", r.Pattern, err)
		}
		m := re.FindStringSubmatchIndex(match)
		if m == nil {
			continue
		}
		expand := func(tmpl string) string {
			// Java's $1_ is group 1 then "_"; Go would read a group named "1_".
			return string(re.ExpandString(nil, javaGroupRef.ReplaceAllString(tmpl, "$${$1}"), match, m))
		}
		if r.Value == nil && !numeric {
			return jmxSample{}, false // "Ignoring unsupported bean"
		}
		s := jmxSample{name: jmxStripSuffixes(jmxSafeName(expand(r.Name))), labels: map[string]string{}}
		for k, v := range r.Labels {
			s.labels[jmxSafeName(expand(k))] = expand(v)
		}
		return s, true
	}
	return jmxSample{}, false
}

// jmxStripSuffixes is Prometheus client_java 1.x's sanitizeMetricName, which
// jmx_exporter 1.x applies to every name: reserved suffixes go
// (task-startup-failure-total is served as ..._task_startup_failure;
// connect_worker_metrics reads both forms).
func jmxStripSuffixes(name string) string {
	for {
		trimmed := name
		for _, suf := range []string{"_total", "_created", "_bucket", "_info"} {
			trimmed = strings.TrimSuffix(trimmed, suf)
		}
		if trimmed == name {
			return name
		}
		name = trimmed
	}
}

// jmxSafeName is jmx_exporter's safeName: every character outside
// [a-zA-Z0-9:_] becomes "_", runs of "_" collapse to one.
func jmxSafeName(name string) string {
	var b strings.Builder
	prevUnderscore := false
	if name != "" && name[0] >= '0' && name[0] <= '9' {
		b.WriteByte('_')
	}
	for _, c := range name {
		legal := c == ':' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
		if !legal || c == '_' {
			if !prevUnderscore {
				b.WriteByte('_')
				prevUnderscore = true
			}
			continue
		}
		b.WriteRune(c)
		prevUnderscore = false
	}
	return b.String()
}

// TestJMXExporterRules: the Kafka Connect families connect_worker_metrics
// (Q3b) reads are exported as kafka_connect_*, the Debezium families keep the
// names debeziumtools reads (internal/tools/debeziumtools/testdata), and
// anything else stays out.
func TestJMXExporterRules(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "dev", "connect", "jmx-exporter.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		LowercaseOutputName bool      `yaml:"lowercaseOutputName"`
		Rules               []jmxRule `yaml:"rules"`
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("parse rules: %v", err)
	}
	if cfg.LowercaseOutputName {
		t.Fatal("lowercaseOutputName would rename debezium_metrics_SnapshotRunning, which debeziumtools reads")
	}
	const conn = "pg-orders-connector"
	ct := map[string]string{"connector": conn, "task": "0"}
	tests := []struct {
		match   string
		numeric bool
		want    string
		labels  map[string]string
	}{
		// Q3b families: task errors, skips, DLQ produce failures, startup
		// failures, rebalance state.
		{"kafka.connect<type=task-error-metrics, connector=pg-orders-connector, task=0><>total-record-errors: 4.0", true, "kafka_connect_task_error_metrics_total_record_errors", ct},
		{"kafka.connect<type=task-error-metrics, connector=pg-orders-connector, task=0><>total-record-failures: 4.0", true, "kafka_connect_task_error_metrics_total_record_failures", ct},
		{"kafka.connect<type=task-error-metrics, connector=pg-orders-connector, task=0><>total-records-skipped: 12.0", true, "kafka_connect_task_error_metrics_total_records_skipped", ct},
		{"kafka.connect<type=task-error-metrics, connector=pg-orders-connector, task=0><>deadletterqueue-produce-failures: 0.0", true, "kafka_connect_task_error_metrics_deadletterqueue_produce_failures", ct},
		{"kafka.connect<type=task-error-metrics, connector=pg-orders-connector, task=0><>last-error-timestamp: 1.76E12", true, "kafka_connect_task_error_metrics_last_error_timestamp", ct},
		{"kafka.connect<type=connect-worker-metrics><>task-startup-failure-total: 1.0", true, "kafka_connect_connect_worker_metrics_task_startup_failure", map[string]string{}},
		{"kafka.connect<type=connect-worker-metrics><>connector-startup-failure-total: 0.0", true, "kafka_connect_connect_worker_metrics_connector_startup_failure", map[string]string{}},
		{"kafka.connect<type=connect-worker-metrics, connector=pg-orders-connector><>connector-failed-task-count: 1.0", true, "kafka_connect_connect_worker_metrics_connector_failed_task_count", map[string]string{"connector": conn}},
		{"kafka.connect<type=connect-worker-rebalance-metrics><>rebalancing: 0.0", true, "kafka_connect_connect_worker_rebalance_metrics_rebalancing", map[string]string{}},
		{"kafka.connect<type=connect-worker-rebalance-metrics><>completed-rebalances-total: 3.0", true, "kafka_connect_connect_worker_rebalance_metrics_completed_rebalances", map[string]string{}},
		{"kafka.connect<type=source-task-metrics, connector=pg-orders-connector, task=0><>source-record-poll-total: 100.0", true, "kafka_connect_source_task_metrics_source_record_poll", ct},
		{"kafka.connect<type=connector-task-metrics, connector=pg-orders-connector, task=0><>offset-commit-failure-percentage: 0.0", true, "kafka_connect_connector_task_metrics_offset_commit_failure_percentage", ct},
		// Status words, as a labelled 1.
		{"kafka.connect<type=connector-task-metrics, connector=pg-orders-connector, task=0><>status: failed", false, "kafka_connect_connector_task_status", map[string]string{"connector": conn, "task": "0", "status": "failed"}},
		{"kafka.connect<type=connector-metrics, connector=pg-orders-connector><>status: running", false, "kafka_connect_connector_status", map[string]string{"connector": conn, "status": "running"}},
		// Debezium, as debeziumtools reads it (Debezium 2+ beans carry task).
		{"debezium.postgres<type=connector-metrics, server=pg, task=0, context=streaming><>MilliSecondsBehindSource: 2300", true, "debezium_metrics_MilliSecondsBehindSource", map[string]string{"plugin": "postgres", "name": "pg", "task": "0", "context": "streaming"}},
		{"debezium.postgres<type=connector-metrics, server=pg, task=0, context=snapshot><>SnapshotRunning: true", true, "debezium_metrics_SnapshotRunning", map[string]string{"plugin": "postgres", "name": "pg", "task": "0", "context": "snapshot"}},
		{"debezium.postgres<type=connector-metrics, server=pg, task=0, context=snapshot, key=public.orders><>RowsScanned: 4.820311E7", true, "debezium_metrics_RowsScanned", map[string]string{"plugin": "postgres", "name": "pg", "task": "0", "context": "snapshot", "table": "public.orders"}},
		{"debezium.postgres<type=connector-metrics, context=snapshot, server=pg, key=public.orders><>RowsScanned: 10", true, "debezium_metrics_RowsScanned", map[string]string{"plugin": "postgres", "name": "pg", "context": "snapshot", "table": "public.orders"}},
		{"debezium.postgres<type=connector-metrics, context=snapshot, server=pg><>SnapshotCompleted: false", true, "debezium_metrics_SnapshotCompleted", map[string]string{"plugin": "postgres", "name": "pg", "context": "snapshot"}},
		// Not exported.
		{"kafka.connect<type=connector-metrics, connector=pg-orders-connector><>connector-class: io.debezium.connector.postgresql.PostgresConnector", false, "", nil},
		{"kafka.producer<type=producer-metrics, client-id=connector-producer-pg-orders-connector-0><>record-send-total: 9.0", true, "", nil},
		{"kafka.connect<type=app-info, client-id=connect-1><>version: 4.0.0", false, "", nil},
		{"java.lang<type=Memory><HeapMemoryUsage>used: 1.0E8", true, "", nil},
	}
	for _, tt := range tests {
		got, ok := exportJMX(t, cfg.Rules, tt.match, tt.numeric)
		if tt.want == "" {
			if ok {
				t.Errorf("%s: exported as %s %v, want nothing", tt.match, got.name, got.labels)
			}
			continue
		}
		if !ok || got.name != tt.want || !maps.Equal(got.labels, tt.labels) {
			t.Errorf("%s:\n got  %s %v (exported %v)\n want %s %v", tt.match, got.name, got.labels, ok, tt.want, tt.labels)
		}
		if strings.HasPrefix(tt.want, "kafka_connect_") && !strings.HasPrefix(got.name, "kafka_connect_") {
			t.Errorf("%s: %s lacks the CONNECT_METRICS_PREFIX default kafka_connect_", tt.match, got.name)
		}
	}
}
