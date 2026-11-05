package agent

import (
	"fmt"
	"strings"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// Tools and actions by source kind (Q8g, docs/design/mysql-source.md §3).
//
// A CDC pipeline's source database is Postgres (the default) or MySQL
// (owners.yaml source_kind). A MySQL room offers no pg_* tool or action and a
// Postgres room no mysql_* one: wiring registers only the source's names for a
// client set (one set has one source kind; Open refuses a mix), and the runner
// filters again, so a registry built any other way cannot offer the wrong
// source's names either.
//
// Alert carries no source kind yet (contract change request in the Q8g PR:
// Alert.SourceKind, filled by the room service from owners.yaml). Until it
// does, the runner reads the kind off the registry it was given
// (RoomSourceKind): wiring's per-set registry holds MySQL-only tools exactly
// when the set's source is MySQL.

// mysqlOnlyToolNames are the read tools a MySQL room alone offers in this
// wave beside the mysql_* family: the neutral names of pg_row and
// pg_long_transactions (design §7.4, D5; Postgres rooms keep the pg_ names).
var mysqlOnlyToolNames = map[string]bool{
	"source_row":               true,
	"source_long_transactions": true,
}

// schemaHistoryTool is kafka_schema_history (Q8e). It is source-neutral by
// name (any Debezium connector with a schema history topic, design §7.3), but
// the Postgres connector keeps none, so a Postgres room is not offered it:
// its tool list, prompt and evals stay as they were.
const schemaHistoryTool = "kafka_schema_history"

// SourceKind normalises a source kind as owners.yaml spells it: "" is
// postgres; postgres and mysql are themselves; anything else is an error
// (owners.Load refuses it, so it can only come from a caller's bug).
func SourceKind(kind string) (string, error) {
	switch kind {
	case "", events.SourceKindPostgres:
		return events.SourceKindPostgres, nil
	case events.SourceKindMySQL:
		return events.SourceKindMySQL, nil
	}
	return "", fmt.Errorf("agent: source kind %q: want %s or %s (empty reads as %s)",
		kind, events.SourceKindPostgres, events.SourceKindMySQL, events.SourceKindPostgres)
}

// ToolServesSource reports whether a room whose source is of sourceKind
// (normalised, see SourceKind) offers the read tool called name: pg_* only on
// Postgres; mysql_*, source_row, source_long_transactions and
// kafka_schema_history only on MySQL; every other tool on both.
func ToolServesSource(name, sourceKind string) bool {
	switch {
	case strings.HasPrefix(name, "pg_"):
		return sourceKind == events.SourceKindPostgres
	case strings.HasPrefix(name, "mysql_"), mysqlOnlyToolNames[name], name == schemaHistoryTool:
		return sourceKind == events.SourceKindMySQL
	}
	return true
}

// ActionServesSource reports whether a room whose source is of sourceKind
// may propose the write action called name: pg_* (pg_set_replica_identity,
// every pgactions action) only on Postgres, mysql_* (none today) only on
// MySQL, every other action (the source_* admin actions, Connect, Schema
// Registry and Kafka) on both.
func ActionServesSource(name, sourceKind string) bool {
	switch {
	case strings.HasPrefix(name, "pg_"):
		return sourceKind == events.SourceKindPostgres
	case strings.HasPrefix(name, "mysql_"):
		return sourceKind == events.SourceKindMySQL
	}
	return true
}

// RoomSourceKind returns the source kind of the room whose read tools are
// reg: mysql when reg holds a MySQL-only tool (mysql_*, source_row,
// source_long_transactions), postgres otherwise (no source tool at all, as in
// a batch room, reads as postgres). A registry holding both a pg_* tool and a
// MySQL-only one is an error: no client set serves both, so it is a caller's
// bug, and guessing would offer the wrong source's tools and prompt.
func RoomSourceKind(reg *tools.Registry) (string, error) {
	if reg == nil {
		return events.SourceKindPostgres, nil
	}
	var pg, my string
	for _, t := range reg.All() {
		n := t.Name()
		switch {
		case strings.HasPrefix(n, "pg_"):
			if pg == "" {
				pg = n
			}
		case strings.HasPrefix(n, "mysql_"), mysqlOnlyToolNames[n]:
			if my == "" {
				my = n
			}
		}
	}
	switch {
	case pg != "" && my != "":
		return "", fmt.Errorf("agent: Deps.Tools holds the Postgres-only tool %s and the MySQL-only tool %s: one room has one source kind", pg, my)
	case my != "":
		return events.SourceKindMySQL, nil
	}
	return events.SourceKindPostgres, nil
}

// ToolsForSource returns a new read-only registry with the tools of reg that
// a room whose source is of sourceKind offers (ToolServesSource), in reg's
// order. A Postgres registry built by wiring comes back with the same tools
// in the same order, so its tool list stays byte-identical.
func ToolsForSource(reg *tools.Registry, sourceKind string) (*tools.Registry, error) {
	sk, err := SourceKind(sourceKind)
	if err != nil {
		return nil, err
	}
	out := tools.NewReadOnlyRegistry()
	if reg == nil {
		return out, nil
	}
	for _, t := range reg.All() {
		if !ToolServesSource(t.Name(), sk) {
			continue
		}
		if err := out.Register(t); err != nil {
			return nil, fmt.Errorf("agent: tools for source %s: %w", sk, err)
		}
	}
	return out, nil
}

// ActionsForSource returns a new registry with the actions of reg that a
// room whose source is of sourceKind may propose (ActionServesSource), in
// reg's order. A nil reg stays nil.
func ActionsForSource(reg *actions.Registry, sourceKind string) (*actions.Registry, error) {
	sk, err := SourceKind(sourceKind)
	if err != nil {
		return nil, err
	}
	if reg == nil {
		return nil, nil
	}
	out := actions.NewRegistry()
	for _, a := range reg.All() {
		if !ActionServesSource(a.Name(), sk) {
			continue
		}
		if err := out.Register(a); err != nil {
			return nil, fmt.Errorf("agent: actions for source %s: %w", sk, err)
		}
	}
	return out, nil
}

// sourceDescribed is implemented by an action whose description depends on
// the source kind (sourceactions.SourceDescribed: source_create_heartbeat_table
// and source_end_session). Describe() stays the Postgres text, so a Postgres
// room's action list does not change.
type sourceDescribed interface {
	DescribeFor(sourceKind string) string
}

// describeFor is a's one-line description in a room whose source is of
// sourceKind: DescribeFor(sourceKind) on MySQL when a implements it,
// Describe() otherwise (always on Postgres, so its list keeps its bytes).
func describeFor(a actions.Action, sourceKind string) string {
	if sourceKind == events.SourceKindMySQL {
		if d, ok := a.(sourceDescribed); ok {
			return d.DescribeFor(sourceKind)
		}
	}
	return a.Describe()
}
