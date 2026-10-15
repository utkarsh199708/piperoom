package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// The table part of the idle-source rule (QA W1). The replication slots see
// WAL from every table of the server, so a busy table of another pipeline can
// make an idle pipeline look stuck, and heartbeats move a slot without any data
// change. The pipeline's own source tables answer the question directly:
// pg_table_activity reports their n_tup_ins, n_tup_upd and n_tup_del, which
// only grow. Read when the fix was proposed (else at both ends of one
// freshnessWait) and again during the check:
//
//   - every counter unchanged: the pipeline's tables took no write, so no new
//     rows could be expected. Quiet: the layers decide (recovered, data flow
//     inconclusive).
//   - some counter grew: the tables changed and no row reached the sinks, so
//     the change was not carried through. Active: NOT recovered, whatever the
//     slots or topics say.
//   - the tool is missing or fails, a table is not listed, or a counter went
//     down (the statistics were reset): no verdict from the tables. The slot
//     and topic rule (sourceIdle) applies as before.

// tablesIdleDetail is the data-flow detail of a verdict whose source was shown
// idle by the table counters.
const tablesIdleDetail = "inconclusive: the source was idle (the pipeline's source tables show no inserts, updates or deletes since the fix), so no new rows could be expected"

// tableCount is one source table's write counters.
type tableCount struct {
	ins, upd, del int64
}

// readTables reads the pipeline's source tables with pg_table_activity. ok is
// false when the tool is missing or fails, names no table, or does not list
// one of them: then the tables cannot decide.
func (r *runner) readTables(ctx context.Context, verb string) (map[string]tableCount, bool) {
	res, err := r.runStep(ctx, verb, "pg_table_activity", json.RawMessage(`{}`))
	if err != nil {
		return nil, false
	}
	var d struct {
		Tables []struct {
			Table   string `json:"table"`
			Found   bool   `json:"found"`
			Inserts int64  `json:"n_tup_ins"`
			Updates int64  `json:"n_tup_upd"`
			Deletes int64  `json:"n_tup_del"`
		} `json:"tables"`
	}
	if err := decodeData(res.Data, &d); err != nil || len(d.Tables) == 0 {
		return nil, false
	}
	out := make(map[string]tableCount, len(d.Tables))
	for _, t := range d.Tables {
		if !t.Found || t.Table == "" {
			return nil, false
		}
		out[t.Table] = tableCount{ins: t.Inserts, upd: t.Updates, del: t.Deletes}
	}
	return out, true
}

// judgeSource is the idle-source rule: the table counters when they can
// decide, else sourceIdle. detail, when set, replaces idleDetail for a quiet
// source.
func (r *runner) judgeSource(ctx context.Context, rec proposalRecord) (sourceState, string, string) {
	if state, line, ok := r.tablesIdle(ctx, rec); ok {
		detail := ""
		if state == sourceQuiet {
			detail = tablesIdleDetail
		}
		return state, line, detail
	}
	state, line := r.sourceIdle(ctx, rec)
	return state, line, ""
}

// tablesIdle applies the table part of the rule. ok is false when the tables
// cannot decide.
func (r *runner) tablesIdle(ctx context.Context, rec proposalRecord) (sourceState, string, bool) {
	before := rec.tables
	if len(before) == 0 {
		var ok bool
		if before, ok = r.readTables(ctx, "verify"); !ok {
			return sourceActive, "", false
		}
		if err := r.clk.Sleep(ctx, r.timing.freshnessWait); err != nil {
			return sourceActive, "", false
		}
	}
	after, ok := r.readTables(ctx, "verify")
	if !ok {
		return sourceActive, "", false
	}
	changed, comparable := compareTables(before, after)
	switch {
	case !comparable:
		return sourceActive, "", false
	case len(changed) > 0:
		return sourceActive, "source: the pipeline's source tables changed since the fix (" + strings.Join(changed, "; ") + ") but no new rows reached the sinks: the changes were not carried through", true
	}
	return sourceQuiet, "source: the pipeline's source tables had no inserts, updates or deletes since the fix", true
}

// compareTables lists the tables whose counters grew between two readings,
// sorted, as "table: +N inserts, +N updates, +N deletes". comparable is false
// when the readings name different tables or a counter went down.
func compareTables(before, after map[string]tableCount) (changed []string, comparable bool) {
	if len(before) != len(after) {
		return nil, false
	}
	for name, a := range after {
		b, ok := before[name]
		if !ok || a.ins < b.ins || a.upd < b.upd || a.del < b.del {
			return nil, false
		}
		if a != b {
			changed = append(changed, fmt.Sprintf("%s: +%d inserts, +%d updates, +%d deletes", name, a.ins-b.ins, a.upd-b.upd, a.del-b.del))
		}
	}
	slices.Sort(changed)
	return changed, true
}
