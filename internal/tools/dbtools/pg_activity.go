package dbtools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/clients/postgres"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// sourceTableStatsReader is implemented by SourceDB implementations that read the
// per-table change counters of pg_stat_user_tables (internal/clients/postgres).
type sourceTableStatsReader interface {
	TableCounters(ctx context.Context, tables []string) ([]postgres.TableCounters, error)
}

// RegisterTableActivity adds pg_table_activity to reg when d.Src can read table
// statistics. A source without that reader gets no tool, so the agent is never
// offered a tool that always fails, and the verifier keeps its slot-based
// idle-source rule.
func RegisterTableActivity(reg *tools.Registry, d Deps) error {
	if d.Owners == nil {
		return fmt.Errorf("dbtools: Owners is nil (wiring error)")
	}
	if _, ok := d.Src.(sourceTableStatsReader); !ok {
		return nil
	}
	t := pgTableActivityTool(d)
	if err := reg.Register(t); err != nil {
		return fmt.Errorf("dbtools: register %s: %w", t.Name(), err)
	}
	return nil
}

// tableActivityRow is one source table as pg_table_activity reports it.
type tableActivityRow struct {
	Table string `json:"table"`
	// Found is false when pg_stat_user_tables does not list the table (it does
	// not exist, or has no statistics yet); the counters are then zero.
	Found           bool       `json:"found"`
	Inserts         int64      `json:"n_tup_ins"`
	Updates         int64      `json:"n_tup_upd"`
	Deletes         int64      `json:"n_tup_del"`
	LiveTuples      int64      `json:"n_live_tup"`
	LastAutovacuum  *time.Time `json:"last_autovacuum,omitempty"`
	LastAutoanalyze *time.Time `json:"last_autoanalyze,omitempty"`
}

// tableActivityData is Data of pg_table_activity.
type tableActivityData struct {
	Tables []tableActivityRow `json:"tables"`
}

func pgTableActivityTool(d Deps) tools.Tool {
	return &tool{
		name: "pg_table_activity",
		desc: "Show the change counters of the pipeline's source tables from pg_stat_user_tables: rows inserted, updated and deleted since the statistics were last reset (n_tup_ins, n_tup_upd, n_tup_del), live rows, and the last autovacuum and autoanalyze. Two readings tell whether the source tables changed in between. No row values are read.",
		schema: objSchema(map[string]any{
			"pipeline_id": strProp("Optional pipeline id; in a room it must be the room's pipeline."),
		}),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				PipelineID string `json:"pipeline_id"`
			}
			if len(in) > 0 {
				if err := decode(in, &input); err != nil {
					return tools.Result{}, err
				}
			}
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			if sc.Unscoped() && input.PipelineID == "" {
				return tools.Result{}, fmt.Errorf("%w: pipeline_id is required outside a room", tools.ErrBadInput)
			}
			p, err := d.resolve(sc, input.PipelineID)
			if err != nil {
				return tools.Result{}, err
			}
			reader, ok := d.Src.(sourceTableStatsReader)
			if !ok {
				return tools.Result{}, fmt.Errorf("dbtools: pg_table_activity: the source client cannot read table statistics")
			}
			tables := p.SourceTables
			if len(tables) == 0 {
				return tools.Result{Summary: fmt.Sprintf("(pipeline %s has no source tables)\n", p.ID), Data: tableActivityData{Tables: []tableActivityRow{}}}, nil
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()
			got, err := reader.TableCounters(ctx, tables)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: pg_table_activity: %w", err)
			}
			byTable := make(map[string]postgres.TableCounters, len(got))
			for _, a := range got {
				byTable[a.Table] = a
			}
			// One row per pipeline table, in owners.yaml order. A row the client
			// returned for any other table is dropped, never shown.
			data := tableActivityData{Tables: make([]tableActivityRow, 0, len(tables))}
			var sb strings.Builder
			for _, t := range tables {
				a, found := byTable[t]
				if !found {
					data.Tables = append(data.Tables, tableActivityRow{Table: t})
					fmt.Fprintf(&sb, "%s: not in pg_stat_user_tables (missing, or no statistics yet)\n", t)
					continue
				}
				data.Tables = append(data.Tables, tableActivityRow{
					Table: t, Found: true, Inserts: a.Inserts, Updates: a.Updates, Deletes: a.Deletes, LiveTuples: a.LiveTuples,
					LastAutovacuum: a.LastAutovacuum, LastAutoanalyze: a.LastAutoanalyze,
				})
				fmt.Fprintf(&sb, "%s: ins=%d upd=%d del=%d live=%d last_autovacuum=%s last_autoanalyze=%s\n",
					t, a.Inserts, a.Updates, a.Deletes, a.LiveTuples, statTime(a.LastAutovacuum), statTime(a.LastAutoanalyze))
			}
			summary, truncated := tools.Truncate(sb.String())
			return tools.Result{Summary: summary, Data: data, Truncated: truncated}, nil
		},
	}
}

// statTime renders a statistics timestamp, or "never" when it did not run.
func statTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return "never"
	}
	return topology.FormatTS(*t)
}
