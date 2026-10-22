// Package topology builds the pipeline map: which tables, connectors, topics,
// consumer groups and sink tables form a pipeline, who owns each, and how
// healthy each is.
//
// CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
// The implementation is owned by A3.
package topology

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"piperoom/internal/events"
)

// NodeKind is the kind of a pipeline node.
type NodeKind string

const (
	KindSourceTable   NodeKind = "source_table"
	KindConnector     NodeKind = "connector"
	KindTopic         NodeKind = "topic"
	KindConsumerGroup NodeKind = "consumer_group"
	KindSinkTable     NodeKind = "sink_table"
	// KindDLQTopic is a dead-letter topic written by one of the pipeline's
	// sink connectors (errors.deadletterqueue.topic.name) or declared in
	// owners.yaml (watch.dlq_topics). It is a side node: edges lead into it,
	// never out. Its layer and owner are the writer's.
	KindDLQTopic NodeKind = "dlq_topic"
	// KindSchemaSubject is a Schema Registry subject (id "sr:<subject>", layer
	// "schema-registry") that the pipeline's connectors serialize with or its
	// consumers deserialize with. It is not a data stage: only depends_on
	// edges point at it. Facts: compatibility, latest_version and
	// registered_recently (true | false | unknown); new_version_seen (age,
	// health warn) when registered_recently is true; version_deleted_seen
	// (age, health warn) when the latest version went down, which is not a
	// registration; exists=false (health warn) when a converter config names
	// it but nothing is registered; error=unreachable (health unknown).
	KindSchemaSubject NodeKind = "schema_subject"

	// Batch pipelines (P4): DAG -> task -> model -> sink_table, data edges.
	// KindDAG is an orchestrator DAG (id "dag:<dag_id>", layer orchestrator).
	KindDAG NodeKind = "dag"
	// KindTask is one task of the DAG's latest run (id "task:<dag_id>.<task_id>",
	// layer orchestrator); its state is a fact.
	KindTask NodeKind = "task"
	// KindModel is a transform model (id "model:<name>", layer transform),
	// from TransformReader.ModelLineage, parents before children.
	KindModel NodeKind = "model"

	// ClickHouse Kafka ingestion (CH-3): topic -> kafka_engine ->
	// materialized_view -> sink_table, data edges. Both nodes take the
	// warehouse's layer and owner.
	// KindKafkaEngine is a ClickHouse Kafka engine table that consumes one
	// topic (id "kafka_engine:<db>.<table>"). Its health comes from what the
	// warehouse reports about it; with no confirmation that it exists it is
	// unknown, never ok.
	KindKafkaEngine NodeKind = "kafka_engine"
	// KindMaterializedView is a ClickHouse materialized view that reads a Kafka
	// engine table and fills a sink table (id "materialized_view:<db>.<view>").
	// When the builder finds the engine, view or topic by a naming convention
	// rather than by reading the view's definition, the edges it draws are
	// MappingInferred.
	KindMaterializedView NodeKind = "materialized_view"
)

// Health is a node's state.
type Health string

const (
	HealthOK      Health = "ok"
	HealthWarn    Health = "warn"
	HealthFail    Health = "fail"
	HealthUnknown Health = "unknown"
)

// Node is one part of a pipeline. ID is "<prefix>:<name>" with prefixes
// pg, connect, topic, dlq, group, wh, sr, for batch pipelines dag, task,
// model, and for a ClickHouse warehouse kafka_engine, materialized_view (e.g.
// "connect:pg-orders-connector", "dlq:orders-dlq", "sr:pg.public.orders-value",
// "dag:revenue_daily", "kafka_engine:analytics.orders_kafka").
type Node struct {
	ID     string            `json:"id"`
	Kind   NodeKind          `json:"kind"`
	Name   string            `json:"name"`
	Layer  string            `json:"layer"` // one of events.Layers
	Owner  string            `json:"owner"` // user id from owners.yaml
	Health Health            `json:"health"`
	Facts  map[string]string `json:"facts,omitempty"` // short display facts, e.g. "lag": "0"
	// Tags are governance labels (X7): TagPII, TagRetention. Unlike Facts
	// they describe what the data is, not how the component is doing.
	// Omitted when empty, so a graph from before X7 keeps its JSON shape.
	Tags map[string]string `json:"tags,omitempty"`
}

// ---- X7: governance tags on nodes (contract X7-0, docs/design/lineage.md) ----
//
// Tags carry names, classes and durations only: never a row value, a sample
// or a config value. Who may read them is who may read the topology (the
// pipeline's tenants; anyone else gets 404). Keys match ^[a-z][a-z0-9_]{0,31}$,
// values are printable text without newlines, at most MaxTagValueBytes, and a
// node has at most MaxNodeTags (ValidTag checks one pair).

const (
	// TagPII lists the node's columns classed as personal data, from column
	// names and types only (X7c): FormatPIITag's "column:class" pairs, e.g.
	// "customer_email:email,phone_number:phone". On source_table and
	// sink_table nodes. It is a display and export label: a safety decision
	// (X1b's sampling skips PII columns) classifies the column list itself,
	// because a long list is cut (TagPIIOmitted).
	TagPII = "pii"
	// TagPIIOmitted is the number of PII columns FormatPIITag left out of
	// TagPII (cap or a name it cannot hold), as a decimal; absent when 0.
	TagPIIOmitted = "pii_omitted"
	// TagRetention is the owners.yaml declared_retention of the node's topic
	// or table in FormatRetention form ("7d", "36h", "infinite"); absent when
	// none is declared. The configured retention is the watcher's to compare
	// (X7d, config_risk), never a tag.
	TagRetention = "retention"
)

// Caps of Node.Tags.
const (
	MaxNodeTags      = 16
	MaxTagValueBytes = 2048
)

// ValidTag reports whether key and value may be a Node.Tags entry.
func ValidTag(key, value string) error {
	if !tagKeyRE(key) {
		return fmt.Errorf("topology: tag key %q: want ^[a-z][a-z0-9_]{0,31}$", key)
	}
	if len(value) > MaxTagValueBytes {
		return fmt.Errorf("topology: tag %s: value is %d bytes, max %d", key, len(value), MaxTagValueBytes)
	}
	if !utf8.ValidString(value) || strings.IndexFunc(value, unsafeTagRune) >= 0 {
		return fmt.Errorf("topology: tag %s: value must be printable text on one line, without bidirectional controls", key)
	}
	return nil
}

// unsafeTagRune is a control character or a bidirectional control
// (U+202A-U+202E, U+2066-U+2069): a tag is shown as text next to names, and
// the room refuses the same runes in typed text (#358), so a column name
// cannot reorder what the lane shows.
func unsafeTagRune(r rune) bool {
	return unicode.IsControl(r) || r >= 0x202A && r <= 0x202E || r >= 0x2066 && r <= 0x2069
}

func tagKeyRE(k string) bool {
	if k == "" || len(k) > 32 || k[0] < 'a' || k[0] > 'z' {
		return false
	}
	for i := 1; i < len(k); i++ {
		c := k[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			return false
		}
	}
	return true
}

// PIIClass is the kind of personal data a column holds (X7c).
type PIIClass string

const (
	PIIEmail      PIIClass = "email"
	PIIPhone      PIIClass = "phone"
	PIIName       PIIClass = "name"
	PIIAddress    PIIClass = "address"
	PIINationalID PIIClass = "national_id" // SSN, national insurance, tax id
	PIICard       PIIClass = "card"        // payment card number, PAN, last four
	PIIIP         PIIClass = "ip"
	PIIDOB        PIIClass = "dob"
	// PIIOther is a column owners.yaml pii.include names without a class.
	PIIOther PIIClass = "other"
)

// PIIClasses returns every PIIClass, in a fixed order.
func PIIClasses() []PIIClass {
	return []PIIClass{PIIEmail, PIIPhone, PIIName, PIIAddress, PIINationalID, PIICard, PIIIP, PIIDOB, PIIOther}
}

// FormatPIITag renders a column -> class map as the TagPII value: pairs
// "column:class" sorted by column, joined by ",", at most MaxTagValueBytes.
// omitted counts the pairs left out: past the cap, a column name holding ','
// ':', a control character or a bidirectional control, or a class that is
// not one of PIIClasses. The
// caller sets TagPIIOmitted when omitted > 0; an empty map is "", 0.
func FormatPIITag(cols map[string]PIIClass) (value string, omitted int) {
	names := make([]string, 0, len(cols))
	for c := range cols {
		names = append(names, c)
	}
	slices.Sort(names)
	var sb strings.Builder
	full := false
	for _, c := range names {
		cl := cols[c]
		if full || c == "" || strings.ContainsAny(c, ",:") || strings.IndexFunc(c, unsafeTagRune) >= 0 ||
			!utf8.ValidString(c) || !slices.Contains(PIIClasses(), cl) {
			omitted++
			continue
		}
		pair := c + ":" + string(cl)
		n := len(pair)
		if sb.Len() > 0 {
			n++
		}
		// Once a pair does not fit, the rest are cut too, so the tag is
		// always the first columns in name order.
		if sb.Len()+n > MaxTagValueBytes {
			full = true
			omitted++
			continue
		}
		if sb.Len() > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(pair)
	}
	return sb.String(), omitted
}

// ParsePIITag reads a TagPII value back. "" is an empty map. A pair without
// ':' or with an unknown class is an error, so a reader never guesses.
func ParsePIITag(v string) (map[string]PIIClass, error) {
	out := map[string]PIIClass{}
	if v == "" {
		return out, nil
	}
	for _, pair := range strings.Split(v, ",") {
		c, cl, ok := strings.Cut(pair, ":")
		if !ok || c == "" || !slices.Contains(PIIClasses(), PIIClass(cl)) {
			return nil, fmt.Errorf("topology: pii tag: bad pair %q", pair)
		}
		out[c] = PIIClass(cl)
	}
	return out, nil
}

// RetentionInfinite is the declared or configured retention "keep forever"
// (Kafka retention.ms=-1).
const RetentionInfinite = "infinite"

// MaxRetention bounds a retention ParseRetention accepts (100 years); a
// longer one is written "infinite".
const MaxRetention = 100 * 365 * 24 * time.Hour

// ParseRetention reads an owners.yaml declared_retention value: a whole
// number >= 1 followed by d, h, m or s ("7d", "36h"), or "infinite". It is
// also the form FormatRetention writes, so evidence and tags read back.
func ParseRetention(s string) (d time.Duration, infinite bool, err error) {
	if s == RetentionInfinite {
		return 0, true, nil
	}
	if len(s) < 2 {
		return 0, false, fmt.Errorf("topology: retention %q: want <n>d, <n>h, <n>m, <n>s or %q", s, RetentionInfinite)
	}
	var unit time.Duration
	switch s[len(s)-1] {
	case 'd':
		unit = 24 * time.Hour
	case 'h':
		unit = time.Hour
	case 'm':
		unit = time.Minute
	case 's':
		unit = time.Second
	default:
		return 0, false, fmt.Errorf("topology: retention %q: want <n>d, <n>h, <n>m, <n>s or %q", s, RetentionInfinite)
	}
	digits := s[:len(s)-1]
	for _, r := range digits {
		if r < '0' || r > '9' {
			return 0, false, fmt.Errorf("topology: retention %q: want a whole number before the unit", s)
		}
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || n < 1 || n > int64(MaxRetention/unit) {
		return 0, false, fmt.Errorf("topology: retention %q: want 1 to %d%c, or %q", s, int64(MaxRetention/unit), s[len(s)-1], RetentionInfinite)
	}
	return time.Duration(n) * unit, false, nil
}

// FormatRetention writes a retention in ParseRetention form with the largest
// unit that divides it ("7d", "36h", "90m", "45s"; sub-second parts are
// dropped, and anything under 1 s is "0s"). infinite, or d > MaxRetention,
// is "infinite".
func FormatRetention(d time.Duration, infinite bool) string {
	if infinite || d > MaxRetention {
		return RetentionInfinite
	}
	d = d.Truncate(time.Second)
	switch {
	case d <= 0:
		return "0s"
	case d%(24*time.Hour) == 0:
		return strconv.FormatInt(int64(d/(24*time.Hour)), 10) + "d"
	case d%time.Hour == 0:
		return strconv.FormatInt(int64(d/time.Hour), 10) + "h"
	case d%time.Minute == 0:
		return strconv.FormatInt(int64(d/time.Minute), 10) + "m"
	}
	return strconv.FormatInt(int64(d/time.Second), 10) + "s"
}

// EdgeKind says what an edge means. The empty value means EdgeData, so a
// graph or client from before edge kinds reads every edge as data flow.
type EdgeKind string

const (
	// EdgeData is data flow: records go from From to To.
	EdgeData EdgeKind = "data"
	// EdgeDependsOn means From needs To to work but no records flow between
	// them, e.g. a connector or consumer group that serializes or
	// deserializes with a schema subject. To is upstream of From when looking
	// for a root cause.
	EdgeDependsOn EdgeKind = "depends_on"
)

// EdgeMapping says how the builder knows an edge exists. The empty value
// means MappingMapped, so a graph from before mappings reads as proven.
type EdgeMapping string

const (
	// MappingMapped is proven from live config or metadata (connector config,
	// committed group offsets, Schema Registry subjects) or declared
	// explicitly in owners.yaml.
	MappingMapped EdgeMapping = "mapped"
	// MappingInferred is not proven; it is implied by owners.yaml (both ends
	// in the same pipeline) or by a naming convention. Render marks it
	// "(unconfirmed)"; the agent treats it as a hypothesis to check.
	MappingInferred EdgeMapping = "inferred"
)

// Edge joins two nodes of a pipeline. Kind and Mapping are left empty for
// proven data flow, so that common edge keeps its original JSON shape.
type Edge struct {
	From    string      `json:"from"`
	To      string      `json:"to"`
	Kind    EdgeKind    `json:"kind,omitempty"`    // "" means EdgeData
	Mapping EdgeMapping `json:"mapping,omitempty"` // "" means MappingMapped
	// Columns is the column lineage of a data edge into a dataset node (X7;
	// see ColumnMap). Nil when unknown: no catalog, no schema, a depends_on
	// edge or an edge into a processing node. ColumnsOmitted counts the maps
	// LimitColumns cut. Both are omitted when empty, so an edge from before
	// X7 keeps its JSON shape.
	Columns        []ColumnMap `json:"columns,omitempty"`
	ColumnsOmitted int         `json:"columns_omitted,omitempty"`
}

// IsDependency reports whether e is a depends_on edge.
func (e Edge) IsDependency() bool { return e.Kind == EdgeDependsOn }

// IsInferred reports whether e is assumed, not proven.
func (e Edge) IsInferred() bool { return e.Mapping == MappingInferred }

// ---- X7: column lineage on edges (contract X7-0, docs/design/lineage.md) ----
//
// Column lineage says which upstream column each downstream column comes
// from, hop by hop: source columns (the database catalog,
// clients.ColumnReader), topic value fields (the latest Schema Registry
// schema), sink columns (the warehouse catalog). Names and transforms only:
// never a value, a default or a sample. It is filled by the builder (X7a),
// exported to OpenLineage (X7b) and readable by whoever may read the
// topology.

// IsDataset reports whether nodes of kind k hold data (a table, a topic, a
// view or a model), so column lineage may end at them and OpenLineage maps
// them to a Dataset. Connectors, consumer groups, DAGs and tasks move data
// and map to a Job; a schema subject is neither.
func IsDataset(k NodeKind) bool {
	switch k {
	case KindSourceTable, KindTopic, KindDLQTopic, KindSinkTable, KindModel, KindKafkaEngine, KindMaterializedView:
		return true
	}
	return false
}

// ColumnTransform says how a downstream column comes from an upstream one.
type ColumnTransform string

const (
	// ColumnSame is the same value under the same name (matched by name).
	ColumnSame ColumnTransform = ""
	// ColumnRename is the same value under another name: the renames list of
	// Kafka Connect's ReplaceField SMT ("amount:total"), the only built-in
	// rename.
	ColumnRename ColumnTransform = "rename"
	// ColumnUnwrap is a Debezium envelope field taken out by
	// ExtractNewRecordState: From "after.<f>", To "<f>".
	ColumnUnwrap ColumnTransform = "unwrap"
	// ColumnDropped is an upstream column with no downstream column: an SMT
	// removed it (ReplaceField exclude) or the sink lacks it. To is "". A
	// dropped column is a fact the render and the agent show
	// ("orders.discount not in sink").
	ColumnDropped ColumnTransform = "dropped"
	// ColumnAdded is a downstream column fed by no upstream column (a
	// sink-only column, or a field such as __op that unwrap's add.fields
	// adds). From and Input are "".
	ColumnAdded ColumnTransform = "added"
)

// ColumnMap is one column of a data edge's lineage. It sits on the data edge
// into a dataset node (IsDataset(To's kind)); To names a column (or field)
// of that node. Input names the upstream dataset node From belongs to: the
// edge's From when that is a dataset, else a dataset with a data edge into
// the edge's From (the writer's input), or, when that one's fields are
// unknown (no Schema Registry), the nearest dataset further upstream whose
// columns are known, and then Mapping is MappingInferred. Topic fields are
// named by their path in the record value with dots ("after.amount" in a
// Debezium envelope, "amount" once unwrapped). The builder lists the maps in
// the downstream column order, then the dropped columns in upstream order,
// so the same graph always gives the same list.
type ColumnMap struct {
	Input     string          `json:"input,omitempty"`     // upstream dataset node id; "" only for ColumnAdded
	From      string          `json:"from,omitempty"`      // upstream column; "" only for ColumnAdded
	To        string          `json:"to,omitempty"`        // downstream column; "" only for ColumnDropped
	Transform ColumnTransform `json:"transform,omitempty"` // "" means ColumnSame
	Mapping   EdgeMapping     `json:"mapping,omitempty"`   // "" means MappingMapped: proven by catalog, schema or SMT config
}

// MaxEdgeColumns caps Edge.Columns. A Postgres table has at most 1600
// columns; a wider list says nothing a reader can use.
const MaxEdgeColumns = 1000

// LimitColumns keeps the first MaxEdgeColumns maps of cols (the caller has
// put them in the documented order) and returns how many it cut, for
// Edge.ColumnsOmitted.
func LimitColumns(cols []ColumnMap) (kept []ColumnMap, omitted int) {
	if len(cols) <= MaxEdgeColumns {
		return cols, 0
	}
	return cols[:MaxEdgeColumns:MaxEdgeColumns], len(cols) - MaxEdgeColumns
}

// Graph is one pipeline (JSON shape matches docs/api.md "Topology").
type Graph struct {
	PipelineID string `json:"pipeline_id"`
	Nodes      []Node `json:"nodes"`
	Edges      []Edge `json:"edges"`
	// Partial is true when an optional reader the pipeline's kind needs is
	// not configured or answers clients.ErrNotSupported, so its nodes are
	// left out (P4: no OrchestratorReader leaves out dag and task nodes).
	// It is not "unhealthy": an unreachable system still gives
	// HealthUnknown nodes. Omitted when false, so CDC graphs are unchanged.
	Partial bool `json:"partial,omitempty"`
}

// Builder builds graphs and traces records.
type Builder interface {
	// Build returns the current graph with live health. Unreachable systems
	// give HealthUnknown on their nodes, not an error.
	Build(ctx context.Context, pipelineID string) (Graph, error)
	// Render returns a compact, deterministic text form of g for the model's
	// first prompt: one line per node in data-flow order (upstream first),
	// each followed by its "from:" (data inputs) and "depends on:" lines,
	// inferred edges marked "(unconfirmed)", under a legend line saying the
	// first failing component and the root cause can differ.
	// X7: a graph with no Columns and no Tags renders byte for byte as
	// before. An edge with Columns adds, under its To node, one "columns:"
	// line naming only the changes (renamed, unwrapped, dropped, added; same
	// name columns counted, not listed); a node's Tags follow its facts in
	// key order. Both stay under the render cap.
	Render(g Graph) string
	// Trace follows one source row (by primary key) through every stage.
	Trace(ctx context.Context, pipelineID, table, key string) ([]events.TraceStage, error)
	// LayerOwner returns the user id that owns a layer of a pipeline.
	LayerOwner(pipelineID, layer string) (string, bool)
}
