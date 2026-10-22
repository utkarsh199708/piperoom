package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

const (
	// toastWideBytes is the ticket's bar for a wide column: an average
	// stored width above 2 KB is about the size Postgres starts to compress
	// and move values out of line (TOAST_TUPLE_THRESHOLD, about 2 KB a row).
	toastWideBytes = 2048
	// toastPointerBytes is the stored width of a value moved out of line (its
	// TOAST pointer). pg_stats.avg_width counts that, not the value, so a
	// column of out-of-line values reads as about 18 bytes wide; a column
	// narrower than this on average holds no out-of-line value.
	toastPointerBytes = 18
	// defaultPlaceholder is Debezium's unavailable.value.placeholder default.
	defaultPlaceholder = "__debezium_unavailable_value"
	// placeholderWindow is how far back the sink count looks.
	placeholderWindow = 24 * time.Hour
	// maxPlaceholderCounts caps the sink counts of one call (each scans a
	// day of a sink table); placeholderCountTimeout caps each one, so one
	// slow table cannot spend the whole 10 s.
	maxPlaceholderCounts    = 10
	placeholderCountTimeout = 2 * time.Second
	// noPlaceholderCount is the note when the warehouse cannot count.
	noPlaceholderCount = "placeholder count not available on this warehouse"
)

// Evidence words of a ToastRisk.
const (
	// toastOutOfLine: the table's TOAST relation is not empty, so values of
	// its toastable columns are stored out of line. The catalog does not say
	// which column's, so every toastable column that can hold one is listed.
	toastOutOfLine = "out_of_line_values"
	// toastWide: the column's average stored width is above 2 KB.
	toastWide = "wide_values"
)

// ToastRisk is one source column at risk of TOAST truncation: Debezium sends
// the placeholder instead of an unchanged out-of-line value on UPDATE when
// the table's replica identity is not FULL. A risk fact, not a fault.
type ToastRisk struct {
	Table           string `json:"table"`
	Column          string `json:"column"`
	Type            string `json:"type"`
	Storage         string `json:"storage"` // x (extended) or e (external)
	ReplicaIdentity string `json:"replica_identity"`
	AvgWidth        int    `json:"avg_width"` // -1: no statistics
	ToastBytes      int64  `json:"toast_bytes"`
	Evidence        string `json:"evidence"` // out_of_line_values or wide_values
	SinkTable       string `json:"sink_table,omitempty"`
	// PlaceholderCount is the sink rows of the last 24 h whose column equals
	// the placeholder; nil when not counted (PlaceholderNote says why).
	PlaceholderCount *int64 `json:"placeholder_count,omitempty"`
	PlaceholderNote  string `json:"placeholder_note,omitempty"`
}

// ToastTableFact is the catalog view of one source table.
type ToastTableFact struct {
	Table            string `json:"table"`
	Found            bool   `json:"found"`
	ReplicaIdentity  string `json:"replica_identity,omitempty"`
	ToastBytes       int64  `json:"toast_bytes"`
	ToastableColumns int    `json:"toastable_columns"`
}

// ToastRiskData is the Data of a pg_toast_risk result.
type ToastRiskData struct {
	Pipeline          string           `json:"pipeline"`
	Placeholder       string           `json:"placeholder"`
	PlaceholderSource string           `json:"placeholder_source"`
	Since             *time.Time       `json:"since,omitempty"` // placeholder count window, when counted
	Until             *time.Time       `json:"until,omitempty"`
	Risks             []ToastRisk      `json:"risks"`
	Tables            []ToastTableFact `json:"tables"`
	ColumnsCut        bool             `json:"columns_cut,omitempty"`
	Notes             []string         `json:"notes,omitempty"`
}

func pgToastRiskTool(d Deps) tools.Tool {
	return &tool{
		name: "pg_toast_risk",
		desc: "Possible TOAST truncation: for the pipeline's source tables (or one), list the toastable columns (storage extended or external) that hold out-of-line values (the table's TOAST relation is not empty) or whose average stored width is above 2 KB, " +
			"on tables whose REPLICA IDENTITY is not FULL. On such a table, an UPDATE that leaves the column unchanged makes Debezium send its unavailable.value.placeholder instead of the value, and a sink that upserts the event overwrites the real value. " +
			"Reads the placeholder from the source connector config (default __debezium_unavailable_value) and, when the warehouse supports it, counts sink rows of the last 24 h holding it (at most 10 columns). " +
			"Catalog reads only (pg_class, pg_attribute, pg_stats.avg_width, the TOAST relation's size): never a row value. A risk fact, not a fault; the catalog cannot tell which column's values are out of line.",
		schema: objSchema(map[string]any{
			"pipeline_id": strProp("Pipeline id from owners.yaml."),
			"table":       strProp("Optional: one source table, \"schema.table\". Default: every source table of the pipeline."),
		}, "pipeline_id"),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				PipelineID string `json:"pipeline_id"`
				Table      string `json:"table"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			if input.PipelineID == "" {
				return tools.Result{}, fmt.Errorf("%w: pipeline_id is required", tools.ErrBadInput)
			}
			if input.Table != "" && !validTable(input.Table) {
				return tools.Result{}, fmt.Errorf("%w: table must be \"schema.table\"", tools.ErrBadInput)
			}
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			p, err := d.resolve(sc, input.PipelineID)
			if err != nil {
				return tools.Result{}, err
			}
			tables := p.SourceTables
			if input.Table != "" {
				if err := sc.CheckSourceTable(input.Table); err != nil {
					return tools.Result{}, err
				}
				if err := scopedTable(p.ID, input.Table, p.SourceTables); err != nil {
					return tools.Result{}, err
				}
				tables = []string{input.Table}
			}
			if len(tables) == 0 {
				return tools.Result{}, fmt.Errorf("%w: pipeline %q has no source_tables", tools.ErrBadInput, p.ID)
			}
			if len(tables) > clients.MaxToastTables {
				return tools.Result{}, fmt.Errorf("%w: pipeline %q has %d source tables, at most %d per call: pass table", tools.ErrBadInput, p.ID, len(tables), clients.MaxToastTables)
			}
			src, ok := d.Src.(clients.ToastColumnReader)
			if !ok {
				return tools.Result{}, fmt.Errorf("dbtools: pg_toast_risk: source client does not support ToastColumns")
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()
			// The clients check scope themselves and know no owners config,
			// so they get the resolved pipeline, not only its id.
			if !sc.Unscoped() {
				ctx = tools.WithPipeline(ctx, p)
			}
			facts, cut, err := src.ToastColumns(ctx, tables)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_toast_risk: %w", err)
			}

			data := ToastRiskData{Pipeline: p.ID, Risks: []ToastRisk{}, ColumnsCut: cut}
			for _, t := range facts {
				data.Tables = append(data.Tables, ToastTableFact{
					Table: t.Table, Found: t.Found, ReplicaIdentity: t.ReplicaIdentity,
					ToastBytes: t.ToastBytes, ToastableColumns: len(t.Columns),
				})
				data.Risks = append(data.Risks, toastRisks(t)...)
			}
			if cut {
				data.Notes = append(data.Notes, fmt.Sprintf("more than %d toastable columns: the last tables' columns are not all listed; pass table", clients.MaxToastColumns))
			}
			if len(data.Risks) > 0 {
				d.countPlaceholders(ctx, p, &data)
			}
			summary, truncated := tools.Truncate(toastSummary(data))
			return tools.Result{Summary: summary, Data: data, Truncated: truncated}, nil
		},
	}
}

// toastRisks returns the columns of t at risk. A FULL table has none: its
// update events carry every column.
func toastRisks(t clients.ToastTable) []ToastRisk {
	if !t.Found || t.ReplicaIdentity == "FULL" {
		return nil
	}
	var out []ToastRisk
	for _, c := range t.Columns {
		var evidence string
		switch {
		case c.AvgWidth > toastWideBytes:
			evidence = toastWide
		case t.ToastBytes > 0 && (c.AvgWidth < 0 || c.AvgWidth >= toastPointerBytes):
			evidence = toastOutOfLine
		default:
			continue
		}
		out = append(out, ToastRisk{
			Table: t.Table, Column: c.Name, Type: c.Type, Storage: c.Storage,
			ReplicaIdentity: t.ReplicaIdentity, AvgWidth: c.AvgWidth, ToastBytes: t.ToastBytes,
			Evidence: evidence,
		})
	}
	return out
}

// countPlaceholders reads the placeholder from the connector config and
// counts it in the sink for at most maxPlaceholderCounts risks. Every
// failure becomes a note on the risk or on the result, never an error: the
// catalog fact stands without the count.
func (d Deps) countPlaceholders(ctx context.Context, p owners.Pipeline, data *ToastRiskData) {
	data.Placeholder, data.PlaceholderSource = defaultPlaceholder, "Debezium default"
	switch {
	case d.Connect == nil:
		data.Notes = append(data.Notes, "connector config not read (no Connect client): assuming the Debezium default placeholder")
	default:
		name, cfg, err := debeziumSourceConnector(ctx, d.Connect, p.Connectors)
		if err != nil {
			data.Notes = append(data.Notes, "connector config not read ("+cutShort(err.Error(), 160)+"): assuming the Debezium default placeholder")
		} else if v := cfg["unavailable.value.placeholder"]; v != "" {
			data.Placeholder, data.PlaceholderSource = v, "connector "+name+" config unavailable.value.placeholder"
		} else {
			data.PlaceholderSource = "Debezium default (connector " + name + " sets none)"
		}
	}

	counter, ok := d.Wh.(clients.PlaceholderCounter)
	switch {
	case !ok:
		data.Notes = append(data.Notes, noPlaceholderCount)
		return
	case p.SinkTSColumn == "":
		data.Notes = append(data.Notes, "placeholder count needs the pipeline's sink_ts_column in owners.yaml")
		return
	case len(data.Placeholder) > clients.MaxPlaceholderBytes:
		data.Notes = append(data.Notes, fmt.Sprintf("placeholder longer than %d bytes: not counted", clients.MaxPlaceholderBytes))
		return
	}
	until := time.Now().UTC()
	since := until.Add(-placeholderWindow)
	data.Since, data.Until = &since, &until
	counted := 0
	for i := range data.Risks {
		r := &data.Risks[i]
		if counted == maxPlaceholderCounts {
			data.Notes = append(data.Notes, fmt.Sprintf("placeholder counted for the first %d columns only", maxPlaceholderCounts))
			return
		}
		sink, err := pickSinkTable(r.Table, "", p.SinkTables)
		switch {
		case len(p.SinkTables) == 0:
			r.PlaceholderNote = "pipeline has no sink_tables"
			continue
		case err != nil:
			r.PlaceholderNote = "sink table not known: several sink tables, none named like the source"
			continue
		case !d.validWarehouseTable(sink) || !d.validWarehouseColumn(r.Column) || !d.validWarehouseColumn(p.SinkTSColumn):
			r.PlaceholderNote = "not counted: a name is not a plain identifier"
			continue
		}
		r.SinkTable = sink
		counted++
		cctx, cancel := context.WithTimeout(ctx, placeholderCountTimeout)
		n, err := counter.CountPlaceholder(cctx, sink, r.Column, data.Placeholder, p.SinkTSColumn, since, until)
		cancel()
		switch {
		case err == nil:
			r.PlaceholderCount = &n
		case errors.Is(err, clients.ErrNotSupported):
			// The whole warehouse cannot count: say it once, stop asking.
			for j := range data.Risks {
				data.Risks[j].PlaceholderNote, data.Risks[j].PlaceholderCount = "", nil
			}
			data.Since, data.Until = nil, nil
			data.Notes = append(data.Notes, noPlaceholderCount)
			return
		case errors.Is(err, clients.ErrNotFound):
			r.PlaceholderNote = "column or table not in the sink"
		case errors.Is(err, context.DeadlineExceeded):
			r.PlaceholderNote = fmt.Sprintf("placeholder count timed out (%s)", placeholderCountTimeout)
		default:
			r.PlaceholderNote = "placeholder count failed: " + cutShort(err.Error(), 160)
		}
	}
}

// toastSummary renders the result: one line per column at risk, then the
// tables without risk by reason, then the notes.
func toastSummary(data ToastRiskData) string {
	var sb strings.Builder
	n := len(data.Tables)
	if len(data.Risks) == 0 {
		fmt.Fprintf(&sb, "pg_toast_risk pipeline %s: no TOAST truncation risk found in %d source %s\n", data.Pipeline, n, plural(n, "table", "tables"))
	} else {
		fmt.Fprintf(&sb, "pg_toast_risk pipeline %s: possible TOAST truncation: %d %s at risk in %d source %s (placeholder %q, %s). A risk fact, not a fault.\n",
			data.Pipeline, len(data.Risks), plural(len(data.Risks), "column", "columns"), n, plural(n, "table", "tables"),
			cutShort(data.Placeholder, 64), data.PlaceholderSource)
	}
	for _, r := range data.Risks {
		what := fmt.Sprintf("may be TOASTed (the table stores out-of-line values, TOAST relation %s)", sizeText(r.ToastBytes))
		if r.Evidence == toastWide {
			what = fmt.Sprintf("TOASTed (average width %d B, above 2 KB)", r.AvgWidth)
		}
		fmt.Fprintf(&sb, "%s.%s (%s): %s, replica identity %s", r.Table, r.Column, r.Type, what, r.ReplicaIdentity)
		switch {
		case r.PlaceholderCount != nil:
			fmt.Fprintf(&sb, ", placeholder seen %d %s in sink %s last 24 h", *r.PlaceholderCount, plural(int(*r.PlaceholderCount), "time", "times"), r.SinkTable)
		case r.PlaceholderNote != "":
			sb.WriteString(", " + r.PlaceholderNote)
		}
		sb.WriteString("\n")
	}
	var full, missing, clean []string
	risky := map[string]bool{}
	for _, r := range data.Risks {
		risky[r.Table] = true
	}
	for _, t := range data.Tables {
		switch {
		case !t.Found:
			missing = append(missing, t.Table)
		case t.ReplicaIdentity == "FULL":
			full = append(full, t.Table)
		case !risky[t.Table]:
			clean = append(clean, t.Table)
		}
	}
	if len(full) > 0 {
		sb.WriteString("replica identity FULL (updates carry every column): " + strings.Join(full, ", ") + "\n")
	}
	if len(clean) > 0 {
		sb.WriteString("no out-of-line or wide toastable column: " + strings.Join(clean, ", ") + "\n")
	}
	if len(missing) > 0 {
		sb.WriteString("not found in the source catalog: " + strings.Join(missing, ", ") + "\n")
	}
	for _, note := range data.Notes {
		sb.WriteString("note: " + note + "\n")
	}
	return sb.String()
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// sizeText renders a byte count in the largest binary unit below it.
func sizeText(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(b)/(1<<10))
	}
	return fmt.Sprintf("%d B", b)
}
