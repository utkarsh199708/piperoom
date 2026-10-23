package debeziumtools

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients/prom"
	"piperoom/internal/tools"
)

// ---- connect_worker_metrics (Q3b) ----

// DefaultWorkerMetricsPrefix is the JMX exporter's default name for the
// kafka.connect MBean domain; CONNECT_METRICS_PREFIX overrides it (Q0 item 3).
const DefaultWorkerMetricsPrefix = "kafka_connect_"

// Kafka Connect's own MBeans as the JMX exporter names them without rules:
// prefix + MBean type + attribute, hyphens turned into underscores
// (kafka.connect:type=task-error-metrics,connector=x,task=0 attribute
// total-records-skipped -> kafka_connect_task_error_metrics_total_records_skipped
// {connector="x",task="0"}).
const (
	famTaskErrors = "task_error_metrics_"
	famWorker     = "connect_worker_metrics_"
	famRebalance  = "connect_worker_rebalance_metrics_"
)

// taskErrorAttrs are the task-error-metrics the tool reads, per task.
var taskErrorAttrs = []string{
	"total_record_errors", "total_record_failures", "total_records_skipped",
	"total_retries", "total_errors_logged",
	"deadletterqueue_produce_requests", "deadletterqueue_produce_failures",
	"last_error_timestamp",
}

// connectorAttrs are the per-connector connect-worker-metrics (label connector).
var connectorAttrs = []string{
	"connector_total_task_count", "connector_running_task_count", "connector_failed_task_count",
	"connector_paused_task_count", "connector_unassigned_task_count", "connector_restarting_task_count",
}

// workerAttrs are the worker-wide connect-worker-metrics (no connector label).
var workerAttrs = []string{
	"task_startup_failure_total", "task_startup_attempts_total",
	"connector_startup_failure_total", "connector_startup_attempts_total",
	"task_count", "connector_count",
}

// rebalanceAttrs are the connect-worker-rebalance-metrics. leader-name is a
// string attribute the exporter cannot export as a number.
var rebalanceAttrs = []string{
	"rebalancing", "completed_rebalances_total", "epoch",
	"rebalance_avg_time_ms", "rebalance_max_time_ms", "time_since_last_rebalance_ms",
}

// requiredFamilies are the families the row's diagnosis rests on; each one
// the endpoint lacks is named in the summary, so an operator can fix the
// exporter rules instead of the model reading "0" as "no errors".
var requiredFamilies = []string{
	famTaskErrors + "total_record_errors",
	famTaskErrors + "total_records_skipped",
	famTaskErrors + "deadletterqueue_produce_failures",
	famWorker + "task_startup_failure_total",
	famRebalance + "rebalancing",
}

// maxWorkerTasks bounds the tasks of one connector kept from the endpoint.
const maxWorkerTasks = 64

var metricPrefixRE = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)

// WorkerReport is connect_worker_metrics's Data. Only numbers, the
// connector name the model passed and numeric task ids: no label text from
// the endpoint is kept.
type WorkerReport struct {
	Connector string `json:"connector"`
	Prefix    string `json:"metric_prefix"`
	// Tasks maps a task id to its task-error-metrics (attribute names with
	// underscores, e.g. total_records_skipped).
	Tasks map[int]map[string]float64 `json:"tasks,omitempty"`
	// ConnectorTasks are the connector's task counts on this worker.
	ConnectorTasks map[string]float64 `json:"connector_tasks,omitempty"`
	// Worker are the worker-wide counters, for every connector on the worker.
	Worker    map[string]float64 `json:"worker,omitempty"`
	Rebalance map[string]float64 `json:"rebalance,omitempty"`
	// Missing are the full names of required families the endpoint does not
	// export at all (for any connector).
	Missing []string `json:"missing,omitempty"`
	// Flags are the findings worth acting on, in plain words.
	Flags []string `json:"flags,omitempty"`
}

type workerTool struct {
	d      Deps
	prefix string
	now    func() time.Time
}

// RegisterWorkerMetrics adds connect_worker_metrics to reg. It reads the same
// endpoint as debezium_metrics (Deps.Metrics.URL); prefix is
// CONNECT_METRICS_PREFIX, "" meaning DefaultWorkerMetricsPrefix. It is kept
// apart from Register so that registering it is the wiring row's decision
// (Q3e) and the existing tool list does not change.
func RegisterWorkerMetrics(reg *tools.Registry, d Deps, prefix string) error {
	if d.Owners == nil {
		return fmt.Errorf("debeziumtools: Owners is nil (wiring error)")
	}
	if prefix == "" {
		prefix = DefaultWorkerMetricsPrefix
	}
	if !metricPrefixRE.MatchString(prefix) {
		return fmt.Errorf("debeziumtools: CONNECT_METRICS_PREFIX %q is not a metric name prefix", prefix)
	}
	if d.Scraper == nil {
		d.Scraper = prom.NewClient()
	}
	if err := reg.Register(&workerTool{d: d, prefix: prefix, now: time.Now}); err != nil {
		return fmt.Errorf("debeziumtools: register connect_worker_metrics: %w", err)
	}
	return nil
}

func (t *workerTool) Name() string     { return "connect_worker_metrics" }
func (t *workerTool) Kind() tools.Kind { return tools.KindRead }
func (t *workerTool) Description() string {
	return "Reads Kafka Connect's own worker metrics from the JMX exporter for one connector: per task the record errors, " +
		"records skipped (errors.tolerance=all drops them), retries and dead letter queue produce failures; the connector's " +
		"running, failed, paused and unassigned task counts; the worker's task startup failures; and the rebalance state. " +
		"Use when a task is RUNNING but records go missing, or tasks keep moving or fail to start. The endpoint is one worker: " +
		"tasks on other workers do not appear."
}
func (t *workerTool) InputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector": map[string]any{"type": "string", "description": "Connector name (source or sink) from the room's pipeline."},
		},
		"required":             []string{"connector"},
		"additionalProperties": false,
	}
}

func (t *workerTool) Run(ctx context.Context, in json.RawMessage) (tools.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, toolTimeout)
	defer cancel()
	connector, err := t.checkInput(ctx, in)
	if err != nil {
		return tools.Result{}, err
	}
	if t.d.Metrics.URL == "" {
		return notConfigured(), nil
	}
	samples, err := t.d.Scraper.Scrape(ctx, t.d.Metrics.URL, prom.DefaultMaxBytes)
	if err != nil {
		return tools.Result{}, fmt.Errorf("debeziumtools: connect_worker_metrics: %w", err)
	}
	rep := collectWorker(samples, t.prefix, connector)
	summary, truncated := tools.Truncate(renderWorker(rep, t.now()))
	return tools.Result{Summary: summary, Data: rep, Truncated: truncated}, nil
}

// checkInput checks the input and the room's scope before any I/O. Unlike
// debezium_metrics it needs no Connect read: Connect labels its own metrics
// with the connector name, so a sink connector works too.
func (t *workerTool) checkInput(ctx context.Context, in json.RawMessage) (string, error) {
	var input struct {
		Connector string `json:"connector"`
	}
	if len(in) == 0 {
		return "", fmt.Errorf("%w: missing input", tools.ErrBadInput)
	}
	if err := json.Unmarshal(in, &input); err != nil {
		return "", fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	if input.Connector == "" {
		return "", fmt.Errorf("%w: connector is required", tools.ErrBadInput)
	}
	sc, err := tools.ScopeFor(ctx, t.d.Owners)
	if err != nil {
		return "", err
	}
	if err := sc.CheckConnector(input.Connector); err != nil {
		return "", err
	}
	return input.Connector, nil
}

// collectWorker picks the connector's task and connector samples and the
// worker-wide ones for the fixed attribute lists; every other sample, and
// every label value but a numeric task id, is dropped.
func collectWorker(samples []prom.Sample, prefix, connector string) WorkerReport {
	rep := WorkerReport{Connector: connector, Prefix: prefix}
	type wanted struct{ fam, name string } // family group, full name as the tool knows it
	want := map[string]wanted{}
	add := func(fam string, attrs []string) {
		for _, a := range attrs {
			full := prefix + fam + a
			want[full] = wanted{fam, full}
			// JMX exporter 1.x (Prometheus client_java 1.x) strips a
			// trailing _total from every name (task-startup-failure-total
			// is served as ..._task_startup_failure); 0.x keeps it.
			if short, ok := strings.CutSuffix(full, "_total"); ok {
				want[short] = wanted{fam, full}
			}
		}
	}
	add(famTaskErrors, taskErrorAttrs)
	add(famWorker, connectorAttrs)
	add(famWorker, workerAttrs)
	add(famRebalance, rebalanceAttrs)
	exported := map[string]bool{}
	for _, s := range samples {
		w, ok := want[s.Name]
		if !ok {
			continue
		}
		fam := w.fam
		exported[w.name] = true
		attr := strings.TrimPrefix(w.name, prefix+fam)
		label, hasConnector := s.Labels["connector"]
		switch {
		case fam == famTaskErrors:
			if label != connector {
				continue
			}
			id, err := strconv.Atoi(s.Labels["task"])
			if err != nil || id < 0 {
				continue
			}
			if rep.Tasks == nil {
				rep.Tasks = map[int]map[string]float64{}
			}
			if rep.Tasks[id] == nil {
				if len(rep.Tasks) >= maxWorkerTasks {
					continue
				}
				rep.Tasks[id] = map[string]float64{}
			}
			rep.Tasks[id][attr] = s.Value
		case fam == famWorker && hasConnector:
			if label != connector {
				continue
			}
			rep.ConnectorTasks = put(rep.ConnectorTasks, attr, s.Value)
		case fam == famWorker:
			rep.Worker = put(rep.Worker, attr, s.Value)
		case fam == famRebalance:
			rep.Rebalance = put(rep.Rebalance, attr, s.Value)
		}
	}
	for _, f := range requiredFamilies {
		if !exported[prefix+f] {
			rep.Missing = append(rep.Missing, prefix+f)
		}
	}
	rep.Flags = workerFlags(rep)
	return rep
}

// put sets m[k] once: a second sample of the same name (a duplicate line)
// does not overwrite the first.
func put(m map[string]float64, k string, v float64) map[string]float64 {
	if m == nil {
		m = map[string]float64{}
	}
	if _, dup := m[k]; !dup {
		m[k] = v
	}
	return m
}

func workerFlags(rep WorkerReport) []string {
	var flags []string
	var skipped, dlqFail, failures float64
	for _, m := range rep.Tasks {
		skipped += m["total_records_skipped"]
		dlqFail += m["deadletterqueue_produce_failures"]
		failures += m["total_record_failures"]
	}
	if skipped > 0 {
		flags = append(flags, num(skipped)+" records skipped: errors.tolerance=all drops them unless a DLQ is set")
	}
	if dlqFail > 0 {
		flags = append(flags, num(dlqFail)+" DLQ produce failures: failed records were lost")
	}
	if v := rep.ConnectorTasks["connector_failed_task_count"]; v > 0 {
		flags = append(flags, num(v)+" failed tasks")
	}
	if v := rep.ConnectorTasks["connector_unassigned_task_count"]; v > 0 {
		flags = append(flags, num(v)+" unassigned tasks")
	}
	if v := rep.Worker["task_startup_failure_total"]; v > 0 {
		flags = append(flags, num(v)+" task startup failures on this worker")
	}
	if rep.Rebalance["rebalancing"] == 1 {
		flags = append(flags, "the worker is rebalancing now")
	}
	return flags
}

func renderWorker(rep WorkerReport, now time.Time) string {
	if len(rep.Missing) == len(requiredFamilies) && len(rep.Tasks) == 0 && rep.ConnectorTasks == nil && rep.Worker == nil && rep.Rebalance == nil {
		return fmt.Sprintf("%s: no Connect worker metrics under prefix %q: the exporter does not export %s (set CONNECT_METRICS_PREFIX, or add the kafka.connect MBeans to the JMX exporter rules)",
			rep.Connector, rep.Prefix, strings.Join(rep.Missing, ", "))
	}
	var parts []string
	if len(rep.Flags) > 0 {
		parts = append(parts, "FLAGS: "+strings.Join(rep.Flags, "; "))
	}
	parts = append(parts, renderConnectorTasks(rep))
	parts = append(parts, renderTasks(rep, now))
	if w := renderWorkerWide(rep.Worker); w != "" {
		parts = append(parts, w)
	}
	if r := renderRebalance(rep.Rebalance); r != "" {
		parts = append(parts, r)
	}
	for _, m := range rep.Missing {
		parts = append(parts, "the exporter does not export "+m)
	}
	return rep.Connector + ": " + strings.Join(parts, ". ")
}

func renderConnectorTasks(rep WorkerReport) string {
	m := rep.ConnectorTasks
	if m == nil {
		return "connector task counts: none on this worker's endpoint"
	}
	var items []string
	for _, k := range []string{"running", "failed", "paused", "unassigned", "restarting"} {
		if v, ok := m["connector_"+k+"_task_count"]; ok {
			items = append(items, k+" "+num(v))
		}
	}
	s := "tasks"
	if v, ok := m["connector_total_task_count"]; ok {
		s += " total " + num(v)
	}
	if len(items) > 0 {
		s += " (" + strings.Join(items, ", ") + ")"
	}
	return s
}

func renderTasks(rep WorkerReport, now time.Time) string {
	if len(rep.Tasks) == 0 {
		return "task errors: no samples for this connector (its tasks may run on another worker, or it is not running)"
	}
	ids := make([]int, 0, len(rep.Tasks))
	for id := range rep.Tasks {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	var out []string
	for _, id := range ids {
		m := rep.Tasks[id]
		var items []string
		for _, a := range []struct{ key, label string }{
			{"total_record_errors", "record errors"},
			{"total_record_failures", "record failures"},
			{"total_records_skipped", "skipped"},
			{"total_retries", "retries"},
			{"deadletterqueue_produce_requests", "DLQ produced"},
			{"deadletterqueue_produce_failures", "DLQ produce failures"},
		} {
			if v, ok := m[a.key]; ok {
				items = append(items, num(v)+" "+a.label)
			}
		}
		if ts, ok := m["last_error_timestamp"]; ok && ts > 0 && !math.IsNaN(ts) && !math.IsInf(ts, 0) {
			ago := float64(now.UnixMilli()) - ts
			items = append(items, "last error "+dur(ago)+" ago")
		}
		out = append(out, fmt.Sprintf("task %d: %s", id, strings.Join(items, ", ")))
	}
	return strings.Join(out, "; ")
}

func renderWorkerWide(m map[string]float64) string {
	if m == nil {
		return ""
	}
	var items []string
	if v, ok := m["task_startup_failure_total"]; ok {
		s := num(v) + " task startup failures"
		if a, ok := m["task_startup_attempts_total"]; ok {
			s += " of " + num(a) + " attempts"
		}
		items = append(items, s)
	}
	if v, ok := m["connector_startup_failure_total"]; ok {
		s := num(v) + " connector startup failures"
		if a, ok := m["connector_startup_attempts_total"]; ok {
			s += " of " + num(a) + " attempts"
		}
		items = append(items, s)
	}
	if v, ok := m["connector_count"]; ok {
		items = append(items, num(v)+" connectors")
	}
	if v, ok := m["task_count"]; ok {
		items = append(items, num(v)+" tasks")
	}
	if len(items) == 0 {
		return ""
	}
	return "worker (all connectors): " + strings.Join(items, ", ")
}

func renderRebalance(m map[string]float64) string {
	if m == nil {
		return ""
	}
	var items []string
	if v, ok := m["rebalancing"]; ok {
		items = append(items, fmt.Sprintf("rebalancing=%t", v == 1))
	}
	if v, ok := m["completed_rebalances_total"]; ok {
		items = append(items, num(v)+" completed")
	}
	if v, ok := m["epoch"]; ok {
		items = append(items, "epoch "+num(v))
	}
	if v, ok := m["time_since_last_rebalance_ms"]; ok && v >= 0 {
		items = append(items, "last "+dur(v)+" ago")
	}
	if v, ok := m["rebalance_avg_time_ms"]; ok {
		items = append(items, "avg "+dur(v))
	}
	if v, ok := m["rebalance_max_time_ms"]; ok {
		items = append(items, "max "+dur(v))
	}
	if len(items) == 0 {
		return ""
	}
	return "rebalance: " + strings.Join(items, ", ")
}
