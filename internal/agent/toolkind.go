package agent

import (
	"fmt"
	"slices"
	"strings"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// Tools by pipeline kind (tasks/P4-pipeline-kinds-batch.md, P4b; P0 item 6).
//
// A room offers the model only the read tools of its pipeline's kind
// (Alert.PipelineKind, "" = cdc): a batch room never sees kafka_read or a pg_*
// tool, and a CDC room never sees a dag_* or dbt_* tool. A tool the room does
// not offer cannot be called at all, whatever the model or an untrusted text
// asks for, because the runner only runs tools of its own list.
//
// The table is by tool name so wiring (which registers packages by the kinds
// a client set serves) and the runner (which filters per room) read one
// source. A name the table does not know is a CDC tool: CDC rooms keep every
// tool they had before kinds existed, and a batch room gets only what is
// listed here. internal/wiring's TestEveryRegisteredToolHasAKind keeps every
// registered name in the table, so a new tool cannot fall to the default
// unnoticed.

var (
	bothKinds = []string{events.PipelineKindCDC, events.PipelineKindBatch}
	cdcOnly   = []string{events.PipelineKindCDC}
	batchOnly = []string{events.PipelineKindBatch}
)

// sharedTools serve every kind: the warehouse reads (batch verification is
// freshness), the graph, and the tools that need no pipeline client.
var sharedTools = map[string]bool{
	"pipeline_map":          true,
	"warehouse_freshness":   true,
	"warehouse_row":         true,
	"warehouse_load_errors": true,
	"runbook_lookup":        true,
	"similar_incidents":     true,
	"what_changed":          true,
}

// cdcExact are the CDC tools without a CDC prefix: trace_record follows a key
// through Debezium and Kafka, reconcile compares the source table with the
// warehouse, tls_cert_expiry reads the Kafka and Connect endpoints, net_probe
// (Q6a) probes the source, Kafka, Connect and registry endpoints.
var cdcExact = map[string]bool{
	"trace_record":    true,
	"reconcile":       true,
	"tls_cert_expiry": true,
	"net_probe":       true,
}

// cdcPrefixes and batchPrefixes name whole tool families. The batch tools
// (P4c-2 batchtools: dag_runs, task_instances, task_log_tail,
// dag_schedule_gap; P4d-2 dbttools: dbt_run_results, dbt_model_lineage,
// dbt_model_sql) are covered before their packages exist.
var (
	cdcPrefixes   = []string{"kafka_", "pg_", "connect_", "schema_registry_", "debezium_", "dlq_", "source_", "mysql_"}
	batchPrefixes = []string{"dag_", "task_", "dbt_"}
)

// ToolPipelineKinds returns the pipeline kinds whose rooms offer the read
// tool called name, and whether the table names it (an exact name or a
// family prefix). A name it does not know answers cdc only, with known
// false. The returned slice is new on every call.
func ToolPipelineKinds(name string) (kinds []string, known bool) {
	switch {
	case sharedTools[name]:
		return slices.Clone(bothKinds), true
	case cdcExact[name], hasAnyPrefix(name, cdcPrefixes):
		return slices.Clone(cdcOnly), true
	case hasAnyPrefix(name, batchPrefixes):
		return slices.Clone(batchOnly), true
	}
	return slices.Clone(cdcOnly), false
}

// ToolServesKind reports whether a room of pipeline kind (already
// normalised, see RoomKind) offers the tool called name.
func ToolServesKind(name, kind string) bool {
	kinds, _ := ToolPipelineKinds(name)
	return slices.Contains(kinds, kind)
}

func hasAnyPrefix(name string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// RoomKind normalises Alert.PipelineKind: "" (rooms before P4, a pipeline
// owners.yaml does not know) is cdc. Any value other than cdc or batch is an
// error: owners.Load refuses it, so it can only come from a caller's bug, and
// guessing a kind would offer the wrong tools.
func RoomKind(kind string) (string, error) {
	switch kind {
	case "", events.PipelineKindCDC:
		return events.PipelineKindCDC, nil
	case events.PipelineKindBatch:
		return events.PipelineKindBatch, nil
	}
	return "", fmt.Errorf("agent: Alert.PipelineKind %q: want %s or %s (empty reads as %s)",
		kind, events.PipelineKindCDC, events.PipelineKindBatch, events.PipelineKindCDC)
}

// ToolsForKind returns a new read-only registry with the tools of reg that a
// room of pipeline kind offers (ToolServesKind), in reg's order, so the tool
// list stays byte-identical for every room of one kind (prompt caching). kind
// is Alert.PipelineKind as given ("" = cdc). reg itself is not changed: one
// client set's registry serves the rooms of every kind it holds.
func ToolsForKind(reg *tools.Registry, kind string) (*tools.Registry, error) {
	k, err := RoomKind(kind)
	if err != nil {
		return nil, err
	}
	out := tools.NewReadOnlyRegistry()
	if reg == nil {
		return out, nil
	}
	for _, t := range reg.All() {
		if !ToolServesKind(t.Name(), k) {
			continue
		}
		if err := out.Register(t); err != nil {
			return nil, fmt.Errorf("agent: tools for kind %s: %w", k, err)
		}
	}
	return out, nil
}

// KindedAction is implemented by a write action that serves pipeline kinds
// other than cdc. PipelineKinds returns the kinds whose rooms may propose it.
// An action without the method is a CDC action: every action today writes to
// Postgres, Kafka Connect, Schema Registry or Kafka, so a batch room is
// offered none of them until an action declares batch. The method is
// optional so the actions contract (internal/actions/action.go) does not
// change; an action package implements it without importing this one.
type KindedAction interface {
	PipelineKinds() []string
}

// ActionKinds returns the pipeline kinds whose rooms may propose a: its
// declared kinds when it implements KindedAction, cdc otherwise. The returned
// slice is new on every call.
func ActionKinds(a actions.Action) []string {
	if k, ok := a.(KindedAction); ok {
		return slices.Clone(k.PipelineKinds())
	}
	return slices.Clone(cdcOnly)
}

// ActionsForKind returns a new registry with the actions of reg that a room
// of pipeline kind may propose (ActionKinds), in reg's order, so the
// propose_fix description stays byte-identical for every room of one kind.
// CheckScope refuses a step outside the room's pipeline anyway; this keeps the
// model from being offered fixes it can never use. A nil reg stays nil (no
// actions wired), and reg itself is not changed.
func ActionsForKind(reg *actions.Registry, kind string) (*actions.Registry, error) {
	k, err := RoomKind(kind)
	if err != nil {
		return nil, err
	}
	if reg == nil {
		return nil, nil
	}
	out := actions.NewRegistry()
	for _, a := range reg.All() {
		if !slices.Contains(ActionKinds(a), k) {
			continue
		}
		if err := out.Register(a); err != nil {
			return nil, fmt.Errorf("agent: actions for kind %s: %w", k, err)
		}
	}
	return out, nil
}
