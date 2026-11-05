package debeziumtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/clients/prom"
	"piperoom/internal/tools"
)

// ---- debezium_snapshot_progress (X6b) ----

// snapshotNames are the snapshot metrics debezium_snapshot_progress reads
// (context="snapshot"); everything else in the endpoint is ignored.
var snapshotNames = []string{
	"SnapshotRunning", "SnapshotCompleted", "SnapshotAborted", "SnapshotPaused",
	"SnapshotDurationInSeconds", "SnapshotPausedDurationInSeconds",
	"RowsScanned", "TotalTableCount", "RemainingTableCount",
}

// maxEstimateTables bounds the row estimates one call reads (one catalog
// query each, inside the tool's 10 s).
const maxEstimateTables = 50

type snapshotTool struct{ d Deps }

func (t *snapshotTool) Name() string     { return "debezium_snapshot_progress" }
func (t *snapshotTool) Kind() tools.Kind { return tools.KindRead }
func (t *snapshotTool) Description() string {
	return "Reads the progress of a Debezium source connector's snapshot from its JMX metrics: whether one is running and for how long, " +
		"rows scanned per pipeline table, the scan rate, and an ETA against the source database's row estimates. " +
		"Use when a new or restarted source connector is RUNNING but its topics fill slowly, before any restart " +
		"(a restart during an initial snapshot starts it again from zero)."
}
func (t *snapshotTool) InputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector": map[string]any{"type": "string", "description": "Debezium source connector name from the room's pipeline."},
		},
		"required":             []string{"connector"},
		"additionalProperties": false,
	}
}

// TableProgress is one pipeline table of a snapshot.
type TableProgress struct {
	Table       string `json:"table"`
	RowsScanned int64  `json:"rows_scanned"`
	// EstimatedRows is the source's row estimate; -1 when unknown.
	EstimatedRows int64 `json:"estimated_rows"`
}

// SnapshotProgress is debezium_snapshot_progress's Data. Numbers and the
// pipeline's own table names only.
type SnapshotProgress struct {
	Connector string `json:"connector"`
	Prefix    string `json:"topic_prefix"`
	// MetricsAvailable is false when the endpoint did not answer; Reason
	// then says why and nothing else is known.
	MetricsAvailable bool   `json:"metrics_available"`
	Reason           string `json:"reason,omitempty"`
	Found            int    `json:"metrics_found"`
	Running          bool   `json:"running"`
	Completed        bool   `json:"completed"`
	Aborted          bool   `json:"aborted"`
	Paused           bool   `json:"paused"`
	// AgeSeconds is SnapshotDurationInSeconds: the running snapshot's age,
	// or the last one's duration.
	AgeSeconds      int64           `json:"age_seconds"`
	PausedSeconds   int64           `json:"paused_seconds,omitempty"`
	RowsScanned     int64           `json:"rows_scanned"` // every table of the connector
	RowsPerSecond   float64         `json:"rows_per_second,omitempty"`
	TotalTables     *int64          `json:"total_tables,omitempty"`
	RemainingTables *int64          `json:"remaining_tables,omitempty"`
	Tables          []TableProgress `json:"tables,omitempty"`
	EstimatedRows   int64           `json:"estimated_rows,omitempty"` // sum over Tables when every one is known
	ETASeconds      *int64          `json:"eta_seconds,omitempty"`
	ETANote         string          `json:"eta_note,omitempty"`
}

func (t *snapshotTool) Run(ctx context.Context, in json.RawMessage) (tools.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, toolTimeout)
	defer cancel()
	tg, err := t.d.resolve(ctx, in, t.Name())
	if err != nil {
		return tools.Result{}, err
	}
	if !tg.configured {
		return notConfigured(), nil
	}
	p := SnapshotProgress{Connector: tg.connector, Prefix: tg.prefix}
	samples, err := t.d.Scraper.Scrape(ctx, t.d.Metrics.URL, prom.DefaultMaxBytes)
	if err != nil {
		// A fact, not a tool error: the agent learns the progress is
		// unknown and why, and has nothing to retry. prom's errors name the
		// host only.
		p.Reason = "metrics endpoint did not answer: " + oneLine(err.Error())
		return result(p, fmt.Sprintf("%s: snapshot progress unknown: the %s; this says nothing about the snapshot itself, and calling again now will not help (check the JMX exporter; connect_status shows the task state)",
			p.Connector, p.Reason)), nil
	}
	p.MetricsAvailable = true
	var tables []string
	if !tg.sc.Unscoped() {
		tables = tg.sc.Pipeline().SourceTables
	}
	byTable := collectSnapshot(samples, t.d.Metrics.Prefix, &p)
	for _, tbl := range tables {
		p.Tables = append(p.Tables, TableProgress{Table: tbl, RowsScanned: int64(math.Round(byTable[tbl])), EstimatedRows: -1})
	}
	if p.Running {
		t.eta(ctx, &p)
	}
	return result(p, renderSnapshotProgress(p, t.d.Metrics.Prefix)), nil
}

func result(p SnapshotProgress, summary string) tools.Result {
	s, truncated := tools.Truncate(summary)
	return tools.Result{Summary: s, Data: p, Truncated: truncated}
}

// collectSnapshot fills p from the connector's snapshot samples and returns
// RowsScanned per table label. Only numbers leave this function, and table
// labels only as map keys the caller looks up by the pipeline's own names.
func collectSnapshot(samples []prom.Sample, metricPrefix string, p *SnapshotProgress) map[string]float64 {
	known := map[string]bool{}
	for _, n := range snapshotNames {
		known[metricPrefix+n] = true
	}
	found := map[string]bool{}
	scalars := map[string]float64{}
	byTable := map[string]float64{}
	for _, s := range samples {
		if !known[s.Name] || s.Labels["context"] != "snapshot" || !finite(s.Value) || s.Value < 0 {
			continue
		}
		if s.Labels["server"] != p.Prefix && s.Labels["name"] != p.Prefix {
			continue
		}
		short := strings.TrimPrefix(s.Name, metricPrefix)
		found[short] = true
		if tbl, ok := s.Labels["table"]; ok {
			if short == "RowsScanned" && s.Value > byTable[tbl] {
				byTable[tbl] = s.Value
			}
			continue
		}
		if _, dup := scalars[short]; !dup {
			scalars[short] = s.Value
		}
	}
	p.Found = len(found)
	p.Running = scalars["SnapshotRunning"] == 1
	p.Completed = scalars["SnapshotCompleted"] == 1
	p.Aborted = scalars["SnapshotAborted"] == 1
	p.Paused = scalars["SnapshotPaused"] == 1
	p.AgeSeconds = int64(math.Round(scalars["SnapshotDurationInSeconds"]))
	p.PausedSeconds = int64(math.Round(scalars["SnapshotPausedDurationInSeconds"]))
	if v, ok := scalars["TotalTableCount"]; ok {
		n := int64(v)
		p.TotalTables = &n
	}
	if v, ok := scalars["RemainingTableCount"]; ok {
		n := int64(v)
		p.RemainingTables = &n
	}
	var total float64
	for _, v := range byTable {
		total += v
	}
	if total == 0 {
		total = scalars["RowsScanned"] // an exporter without the table label
	}
	p.RowsScanned = int64(math.Round(total))
	if active := p.AgeSeconds - p.PausedSeconds; active > 0 && p.RowsScanned > 0 {
		p.RowsPerSecond = math.Round(float64(p.RowsScanned)/float64(active)*10) / 10
	}
	return byTable
}

// eta reads the row estimate of each pipeline table and sets p's ETA, or a
// note on why there is none.
func (t *snapshotTool) eta(ctx context.Context, p *SnapshotProgress) {
	switch {
	case t.d.Rows == nil:
		p.ETANote = "no row estimate: the source database is not connected"
		return
	case len(p.Tables) == 0:
		p.ETANote = "no row estimate: the room's pipeline lists no source tables"
		return
	case len(p.Tables) > maxEstimateTables:
		p.ETANote = fmt.Sprintf("no row estimate: more than %d source tables", maxEstimateTables)
		return
	}
	var est, remaining int64
	for i := range p.Tables {
		tp := &p.Tables[i]
		n, err := t.d.Rows.EstimatedRows(ctx, tp.Table)
		switch {
		case errors.Is(err, clients.ErrNotFound):
			p.ETANote = fmt.Sprintf("no row estimate: table %s not found in the source database", tp.Table)
			return
		case err != nil:
			p.ETANote = fmt.Sprintf("no row estimate for %s: %s", tp.Table, oneLine(err.Error()))
			return
		case n < 0:
			p.ETANote = fmt.Sprintf("no row estimate: table %s was never analysed (run ANALYZE on it for an estimate)", tp.Table)
			return
		}
		tp.EstimatedRows = n
		est += n
		if left := n - tp.RowsScanned; left > 0 {
			remaining += left
		}
	}
	p.EstimatedRows = est
	if p.RowsPerSecond <= 0 {
		p.ETANote = "no scan rate yet: no rows scanned"
		return
	}
	secs := int64(math.Round(float64(remaining) / p.RowsPerSecond))
	p.ETASeconds = &secs
	if remaining == 0 {
		p.ETANote = "the rows scanned reached the estimates; the estimates lag recent inserts, so the snapshot may run a little longer"
	}
}

func renderSnapshotProgress(p SnapshotProgress, metricPrefix string) string {
	if p.Found == 0 {
		return fmt.Sprintf("%s: no snapshot metrics found under prefix %q for topic.prefix %q (set DEBEZIUM_METRICS_PREFIX, or the connector's task has not started)",
			p.Connector, metricPrefix, p.Prefix)
	}
	var sb strings.Builder
	if !p.Running {
		fmt.Fprintf(&sb, "%s: no snapshot in progress", p.Connector)
		switch {
		case p.Aborted:
			fmt.Fprintf(&sb, "; the last snapshot was aborted after %s rows in %s", num(float64(p.RowsScanned)), dur(float64(p.AgeSeconds)*1000))
		case p.Completed:
			fmt.Fprintf(&sb, "; the last snapshot completed: %s rows in %s", num(float64(p.RowsScanned)), dur(float64(p.AgeSeconds)*1000))
		default:
			sb.WriteString("; none has completed since the connector's task started (snapshot.mode may skip it)")
		}
		return sb.String()
	}
	fmt.Fprintf(&sb, "%s: snapshot running for %s", p.Connector, dur(float64(p.AgeSeconds)*1000))
	if p.Paused {
		sb.WriteString(", PAUSED now")
	}
	if p.PausedSeconds > 0 {
		fmt.Fprintf(&sb, " (paused %s of it)", dur(float64(p.PausedSeconds)*1000))
	}
	fmt.Fprintf(&sb, "; %s rows scanned", num(float64(p.RowsScanned)))
	if p.RowsPerSecond > 0 {
		fmt.Fprintf(&sb, " at %s rows/s", num(p.RowsPerSecond))
	}
	if p.ETASeconds != nil {
		fmt.Fprintf(&sb, "; ETA ~%s for ~%s rows left of ~%s (pg_class.reltuples estimates)",
			dur(float64(*p.ETASeconds)*1000), num(float64(max(p.EstimatedRows-scannedOf(p.Tables), 0))), num(float64(p.EstimatedRows)))
	}
	if p.ETANote != "" {
		if p.ETASeconds == nil {
			sb.WriteString("; ETA unknown")
		}
		sb.WriteString(" (" + p.ETANote + ")")
	}
	if p.TotalTables != nil && p.RemainingTables != nil {
		fmt.Fprintf(&sb, "; %d of %d tables left", *p.RemainingTables, *p.TotalTables)
	}
	var parts []string
	for _, tp := range p.Tables {
		s := fmt.Sprintf("%s %s", tp.Table, num(float64(tp.RowsScanned)))
		if tp.EstimatedRows > 0 {
			s += fmt.Sprintf(" of ~%s (%d%%)", num(float64(tp.EstimatedRows)), min(100, tp.RowsScanned*100/tp.EstimatedRows))
		}
		parts = append(parts, s)
	}
	sort.Strings(parts)
	if len(parts) > maxRowsScannedTables {
		parts = append(parts[:maxRowsScannedTables], fmt.Sprintf("%d more in Data", len(parts)-maxRowsScannedTables))
	}
	if len(parts) > 0 {
		sb.WriteString("; per table: " + strings.Join(parts, ", "))
	}
	return sb.String()
}

// scannedOf caps each table's scanned rows at its estimate, so the rows left
// match the ETA's sum.
func scannedOf(ts []TableProgress) int64 {
	var n int64
	for _, t := range ts {
		n += min(t.RowsScanned, max(t.EstimatedRows, 0))
	}
	return n
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// oneLine flattens and cuts an error for a summary.
func oneLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}
