package debeziumtools

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"piperoom/internal/clients/prom"
	"piperoom/internal/tools"
)

// metricNames are the only Debezium metrics the tool reads; everything else
// in the endpoint is ignored. Names are appended to the configured prefix.
var metricNames = []string{
	"Connected", "MilliSecondsBehindSource", "MilliSecondsSinceLastEvent",
	"SnapshotRunning", "SnapshotCompleted", "SnapshotPaused", "RowsScanned",
	"TotalNumberOfEventsSeen", "NumberOfErroneousEvents",
	"QueueRemainingCapacity", "QueueTotalCapacity", "CurrentQueueSizeInBytes",
}

// maxRowsScannedTables bounds the per-table RowsScanned entries rendered.
const maxRowsScannedTables = 5

// Report is the tool's Data.
type Report struct {
	Connector string             `json:"connector"`
	Prefix    string             `json:"topic_prefix"`
	Found     int                `json:"metrics_found"`
	Streaming map[string]float64 `json:"streaming,omitempty"`
	Snapshot  map[string]float64 `json:"snapshot,omitempty"`
	// RowsScannedByTable only lists the pipeline's own source tables.
	RowsScannedByTable map[string]float64 `json:"rows_scanned_by_table,omitempty"`
}

func (t *metricsTool) run(ctx context.Context, in json.RawMessage) (tools.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, toolTimeout)
	defer cancel()
	tg, err := t.d.resolve(ctx, in, t.Name())
	if err != nil {
		return tools.Result{}, err
	}
	if !tg.configured {
		return notConfigured(), nil
	}
	samples, err := t.d.Scraper.Scrape(ctx, t.d.Metrics.URL, prom.DefaultMaxBytes)
	if err != nil {
		return tools.Result{}, fmt.Errorf("debeziumtools: debezium_metrics: %w", err)
	}
	var tables []string
	if !tg.sc.Unscoped() {
		tables = tg.sc.Pipeline().SourceTables
	}
	rep := collect(samples, t.d.Metrics.Prefix, tg.prefix, tg.connector, tables)
	summary := render(rep, t.d.Metrics.Prefix)
	summary, truncated := tools.Truncate(summary)
	return tools.Result{Summary: summary, Data: rep, Truncated: truncated}, nil
}

// collect picks the samples of one connector (its topic.prefix in the "server"
// or "name" label) for the fixed metric names. Label values other than
// context, server/name and a pipeline table are never kept.
func collect(samples []prom.Sample, metricPrefix, topicPrefix, connector string, tables []string) Report {
	rep := Report{Connector: connector, Prefix: topicPrefix, Streaming: map[string]float64{}, Snapshot: map[string]float64{}}
	known := map[string]bool{}
	for _, n := range metricNames {
		known[metricPrefix+n] = true
	}
	own := map[string]bool{}
	for _, t := range tables {
		own[t] = true
	}
	found := map[string]bool{}
	for _, s := range samples {
		if !known[s.Name] {
			continue
		}
		if s.Labels["server"] != topicPrefix && s.Labels["name"] != topicPrefix {
			continue
		}
		short := strings.TrimPrefix(s.Name, metricPrefix)
		var dst map[string]float64
		switch s.Labels["context"] {
		case "streaming":
			dst = rep.Streaming
		case "snapshot":
			dst = rep.Snapshot
		default:
			continue
		}
		found[short] = true
		if tbl, ok := s.Labels["table"]; ok {
			if short == "RowsScanned" && own[tbl] {
				if rep.RowsScannedByTable == nil {
					rep.RowsScannedByTable = map[string]float64{}
				}
				rep.RowsScannedByTable[tbl] = s.Value
			}
			continue
		}
		if _, dup := dst[short]; !dup {
			dst[short] = s.Value
		}
	}
	rep.Found = len(found)
	return rep
}

func render(rep Report, metricPrefix string) string {
	if rep.Found == 0 {
		return fmt.Sprintf("%s: metrics found: 0 of %d under prefix %q (set DEBEZIUM_METRICS_PREFIX, or the exporter has no samples for topic.prefix %q)",
			rep.Connector, len(metricNames), metricPrefix, rep.Prefix)
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s: ", rep.Connector)
	sb.WriteString(renderStreaming(rep.Streaming))
	sb.WriteString("; ")
	sb.WriteString(renderSnapshot(rep))
	return sb.String()
}

func renderStreaming(m map[string]float64) string {
	if len(m) == 0 {
		return "streaming: no metrics (streaming has not started)"
	}
	parts := []string{"streaming"}
	var items []string
	if v, ok := m["Connected"]; ok {
		items = append(items, fmt.Sprintf("connected=%t", v == 1))
	}
	if v, ok := m["MilliSecondsBehindSource"]; ok {
		items = append(items, dur(v)+" behind source")
	}
	if v, ok := m["MilliSecondsSinceLastEvent"]; ok {
		items = append(items, "last event "+dur(v)+" ago")
	}
	if q := queue(m); q != "" {
		items = append(items, q)
	}
	if v, ok := m["TotalNumberOfEventsSeen"]; ok {
		items = append(items, fmt.Sprintf("%s events seen", num(v)))
	}
	if v, ok := m["NumberOfErroneousEvents"]; ok && v > 0 {
		items = append(items, fmt.Sprintf("%s erroneous events", num(v)))
	}
	return parts[0] + " " + strings.Join(items, ", ")
}

func renderSnapshot(rep Report) string {
	m := rep.Snapshot
	if len(m) == 0 {
		return "snapshot: no metrics"
	}
	var items []string
	for _, k := range []string{"SnapshotRunning", "SnapshotCompleted", "SnapshotPaused"} {
		if v, ok := m[k]; ok {
			items = append(items, fmt.Sprintf("%s=%t", strings.ToLower(strings.TrimPrefix(k, "Snapshot")), v == 1))
		}
	}
	if v, ok := m["RowsScanned"]; ok {
		items = append(items, num(v)+" rows scanned")
	}
	if len(rep.RowsScannedByTable) > 0 {
		var ts []string
		for tbl, v := range rep.RowsScannedByTable {
			ts = append(ts, fmt.Sprintf("%s %s", tbl, num(v)))
		}
		sort.Strings(ts)
		if len(ts) > maxRowsScannedTables {
			ts = ts[:maxRowsScannedTables]
		}
		items = append(items, "rows scanned of table "+strings.Join(ts, ", "))
	}
	if v, ok := m["TotalNumberOfEventsSeen"]; ok {
		items = append(items, num(v)+" events seen")
	}
	if len(items) == 0 {
		return "snapshot: no state metrics"
	}
	return "snapshot " + strings.Join(items, ", ")
}

func queue(m map[string]float64) string {
	total, ok1 := m["QueueTotalCapacity"]
	rest, ok2 := m["QueueRemainingCapacity"]
	if !ok1 || !ok2 {
		return ""
	}
	return fmt.Sprintf("queue %s/%s", num(total-rest), num(total))
}

// dur renders milliseconds as "2.3 s", "41 s", "12 min" or "3.2 h".
func dur(ms float64) string {
	if ms < 0 || math.IsNaN(ms) || math.IsInf(ms, 0) {
		return "unknown"
	}
	s := ms / 1000
	switch {
	case s < 10:
		return fmt.Sprintf("%.1f s", s)
	case s < 120:
		return fmt.Sprintf("%.0f s", s)
	case s < 7200:
		return fmt.Sprintf("%.0f min", s/60)
	default:
		return fmt.Sprintf("%.1f h", s/3600)
	}
}

// num prints a metric value as an integer with no float noise.
func num(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return "unknown"
	}
	return fmt.Sprintf("%d", int64(math.Round(v)))
}
