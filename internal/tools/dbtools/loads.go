package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/warehouse"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// loadErrorLimit is how many failed loads the tool asks for.
const loadErrorLimit = 20

// defaultLoadWindow is the look-back when since is not given.
const defaultLoadWindow = 24 * time.Hour

// maxLoadLines is how many failed loads the summary lists.
const maxLoadLines = 5

// tableActivityReader is the fallback of a warehouse without load history (the
// dev Postgres warehouse): write counters and lock waits. It is asserted on
// Deps.Wh at use time, like the optional readers in clients.go; it is a local
// interface because it is not a contract type.
type tableActivityReader interface {
	TableActivity(ctx context.Context, table string) (warehouse.TableActivity, error)
}

func warehouseLoadErrorsTool(d Deps) tools.Tool {
	return &tool{
		name: "warehouse_load_errors",
		desc: "List failed or partial loads into a warehouse sink table (Snowflake COPY_HISTORY) with the first error message, e.g. an invalid identifier after a schema change. " +
			"Use when the warehouse is stale and the sink connector task is FAILED or shows no error. Snowpipe Streaming loads are not listed. " +
			"On the dev Postgres warehouse, which keeps no load history, it reports table write counters and lock waits instead.",
		schema: objSchema(map[string]any{
			"table": strProp("Sink table, \"schema.table\" (or \"db.schema.table\" on Snowflake). Must be one of the room's sink tables."),
			"since": strProp("Only loads at or after this RFC 3339 timestamp. Default: the last 24 hours. Snowflake keeps 14 days."),
		}, "table"),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				Table string `json:"table"`
				Since string `json:"since"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			if !d.validWarehouseTable(input.Table) {
				return tools.Result{}, fmt.Errorf("%w: table must be \"schema.table\" (or \"db.schema.table\" on Snowflake)", tools.ErrBadInput)
			}
			since := time.Now().Add(-defaultLoadWindow)
			if input.Since != "" {
				t, err := parseRFC3339(input.Since)
				if err != nil {
					return tools.Result{}, err
				}
				since = t
			}
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			if err := sc.CheckSinkTable(input.Table); err != nil {
				return tools.Result{}, err
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()
			lr, ok := d.Wh.(clients.LoadErrorReader)
			if !ok {
				return d.loadFallback(ctx, input.Table)
			}
			// A ClickHouse Kafka-engine sink's load history belongs to the
			// engine table, not to the MergeTree table the view fills. With
			// sinks: the engine is declared, so read the right one; a sibling
			// sink's writer is never queried (#KAN-481).
			readTable := loadErrorTable(sc.Pipeline(), input.Table)
			errs, err := lr.LoadErrors(ctx, readTable, since, loadErrorLimit)
			if errors.Is(err, clients.ErrNotSupported) {
				return d.loadFallback(ctx, input.Table)
			}
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: warehouse_load_errors: %w", err)
			}
			summary := loadErrorsSummary(input.Table, since, errs)
			summary, truncated := tools.Truncate(summary)
			return tools.Result{Summary: summary, Data: errs, Truncated: truncated}, nil
		},
	}
}

// loadErrorTable returns the table whose load history to read for sink: the
// declared ClickHouse Kafka engine writer when the pipeline declares one for
// that sink (the engine owns the consumer exceptions and the failed loads),
// else the sink table itself. A pipeline without a sinks: block keeps the old
// behavior; its engine name, if any, comes from the topology's naming
// convention, not from owners.yaml.
func loadErrorTable(p owners.Pipeline, sink string) string {
	if s, ok := p.SinkForTable(sink); ok {
		if engine, ok := s.KafkaEngineWriter(); ok {
			return engine
		}
	}
	return sink
}

// loadErrorsSummary renders the newest failures first, one line each.
func loadErrorsSummary(table string, since time.Time, errs []clients.LoadError) string {
	if len(errs) == 0 {
		return fmt.Sprintf("%s: no failed or partial loads since %s. no COPY_HISTORY rows is not proof of a healthy sink "+
			"(Snowpipe Streaming loads are not listed here)", table, topology.FormatTS(since))
	}
	var sb strings.Builder
	latest := errs[0]
	fmt.Fprintf(&sb, "%s: %d failed or partial load(s) since %s; latest %s %s %d of %d rows",
		table, len(errs), topology.FormatTS(since), topology.FormatTS(latest.At), latest.Status, latest.Rows, latest.Rows+latest.Errors)
	if latest.FirstError != "" {
		sb.WriteString(": " + latest.FirstError)
	}
	for i, e := range errs {
		if i >= maxLoadLines {
			fmt.Fprintf(&sb, "\n… %d more", len(errs)-i)
			break
		}
		fmt.Fprintf(&sb, "\n- %s %s rows=%d errors=%d", topology.FormatTS(e.At), e.Status, e.Rows, e.Errors)
		if e.File != "" {
			sb.WriteString(" file=" + e.File)
		}
		if e.FirstError != "" {
			sb.WriteString(": " + e.FirstError)
		}
	}
	return sb.String()
}

// loadFallback answers for a warehouse that keeps no load history.
func (d Deps) loadFallback(ctx context.Context, table string) (tools.Result, error) {
	const head = "postgres warehouse has no load history"
	ta, ok := d.Wh.(tableActivityReader)
	if !ok {
		return tools.Result{Summary: head + " (no table statistics available either)", Data: map[string]any{"supported": false}}, nil
	}
	a, err := ta.TableActivity(ctx, table)
	if err != nil {
		return tools.Result{}, fmt.Errorf("dbtools: warehouse_load_errors: %w", err)
	}
	vac := "never"
	if a.LastAutovacuum != nil {
		vac = topology.FormatTS(*a.LastAutovacuum)
	}
	summary := fmt.Sprintf("%s. %s: %d rows inserted since stats reset, %d dead tuples, last autovacuum %s, %d session(s) waiting on a lock",
		head, table, a.Inserts, a.DeadTuples, vac, a.LockWaiters)
	return tools.Result{Summary: summary, Data: a}, nil
}
