package tools

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// Pipeline scoping for read tools (security finding F11).
//
// The agent runner puts the room's pipeline into the context it gives every
// tool (WithPipelineID). A tool calls ScopeFor at the start of Run and checks
// every name it was given (connector, topic, group, subject, table, pipeline_id)
// against the scope BEFORE it talks to any client. A tool must never trust a
// pipeline_id from its own input.
//
// The default is refuse: a context with no pipeline and no explicit opt-out
// (WithUnscoped) gives ErrNoScope.

// ErrOutOfScope is wrapped by every refusal of a name that is not part of the
// room's pipeline.
var ErrOutOfScope = errors.New("out of scope")

// ErrNoScope means the tool context carries neither a room pipeline nor an
// explicit opt-out.
var ErrNoScope = errors.New("no pipeline scope")

type scopeKey struct{}

type scopeVal struct {
	pipeline    owners.Pipeline
	hasPipeline bool
	pipelineID  string
	unscoped    bool
}

func scopeOf(ctx context.Context) scopeVal {
	v, _ := ctx.Value(scopeKey{}).(scopeVal)
	return v
}

// WithPipeline returns a context scoped to the full pipeline p.
func WithPipeline(ctx context.Context, p owners.Pipeline) context.Context {
	v := scopeOf(ctx)
	v.pipeline, v.hasPipeline, v.pipelineID = p, true, p.ID
	return context.WithValue(ctx, scopeKey{}, v)
}

// WithPipelineID returns a context scoped to the pipeline with this id. The
// tool resolves the id against its own owners config (ScopeFor), so a caller
// that only knows the id (the agent runner: Alert.PipelineID) can scope tools.
// An empty id clears any pipeline the context already carries (the explicit
// WithUnscoped opt-out is kept), so a caller with no pipeline cannot inherit
// another room's scope.
func WithPipelineID(ctx context.Context, id string) context.Context {
	if id == "" {
		v := scopeOf(ctx)
		if !v.hasPipeline && v.pipelineID == "" {
			return ctx
		}
		v.pipeline, v.hasPipeline, v.pipelineID = owners.Pipeline{}, false, ""
		return context.WithValue(ctx, scopeKey{}, v)
	}
	v := scopeOf(ctx)
	v.pipeline, v.hasPipeline, v.pipelineID = owners.Pipeline{}, false, id
	return context.WithValue(ctx, scopeKey{}, v)
}

// WithUnscoped is the explicit opt-out for CLI and dev use with no room: tools
// then keep their old, unscoped behaviour. It only matters when the context
// has no pipeline; a room pipeline always wins over the opt-out.
func WithUnscoped(ctx context.Context) context.Context {
	v := scopeOf(ctx)
	v.unscoped = true
	return context.WithValue(ctx, scopeKey{}, v)
}

// PipelineFrom returns the full pipeline stored by WithPipeline. It reports
// false when the context holds only a pipeline id (use ScopeFor to resolve it)
// or nothing.
func PipelineFrom(ctx context.Context) (owners.Pipeline, bool) {
	v := scopeOf(ctx)
	return v.pipeline, v.hasPipeline
}

// Scope is the resolved scope of one tool call.
type Scope struct {
	p        owners.Pipeline
	unscoped bool
	// derived reads the consumer groups the topology derived from the
	// pipeline's own sink engines (owners.Owners.DerivedConsumerGroups): a
	// ClickHouse Kafka engine table's kafka_group_name is not in owners.yaml,
	// but the group is part of the pipeline, so CheckGroup accepts it. It is
	// nil when the call was resolved without an Owners instance (an unscoped
	// call, or a context carrying the full pipeline and no config).
	derived func(pipelineID string) []string
}

// ScopeFor resolves the scope of a tool call from ctx. o is the tool's owners
// config, used only when ctx holds a pipeline id. The result is, in order:
//   - the room's pipeline, when ctx has one (a pipeline id that o does not
//     know is an ErrOutOfScope error);
//   - an unscoped Scope, when the caller opted out with WithUnscoped;
//   - ErrNoScope otherwise.
func ScopeFor(ctx context.Context, o *owners.Owners) (Scope, error) {
	v := scopeOf(ctx)
	var derived func(string) []string
	if o != nil {
		derived = o.DerivedConsumerGroups
	}
	switch {
	case v.hasPipeline:
		return Scope{p: v.pipeline, derived: derived}, nil
	case v.pipelineID != "":
		if o == nil {
			return Scope{}, fmt.Errorf("tools: owners not configured, cannot resolve the room's pipeline %q (wiring must pass Owners)", v.pipelineID)
		}
		p, ok := o.Pipeline(v.pipelineID)
		if !ok {
			return Scope{}, fmt.Errorf("%w: the room's pipeline %q is not in the owners config", ErrOutOfScope, v.pipelineID)
		}
		return Scope{p: p, derived: derived}, nil
	case v.unscoped:
		return Scope{unscoped: true, derived: derived}, nil
	}
	return Scope{}, fmt.Errorf("%w: this tool call has no room pipeline, so read tools are refused (CLI/dev callers must opt out explicitly)", ErrNoScope)
}

// Unscoped reports whether the caller opted out of scoping.
func (s Scope) Unscoped() bool { return s.unscoped }

// Pipeline returns the room's pipeline. It is the zero value when unscoped.
func (s Scope) Pipeline() owners.Pipeline { return s.p }

func refuse(kind, name string, s Scope, allowed string) error {
	return fmt.Errorf("%w: %s %q is not part of this room's pipeline %q (%s)", ErrOutOfScope, kind, shorten(name), s.p.ID, allowed)
}

// shorten caps an agent-supplied name before it is echoed in an error.
func shorten(s string) string {
	const max = 80
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	return string([]rune(s)[:max]) + "…"
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// CheckPipelineID refuses a pipeline_id from a tool input that is not the
// room's pipeline. An empty id passes (the tool decides whether it is required).
// When unscoped, any id passes.
func (s Scope) CheckPipelineID(id string) error {
	if s.unscoped || id == "" || id == s.p.ID {
		return nil
	}
	return fmt.Errorf("%w: pipeline_id %q is not this room's pipeline %q; read tools only work on the room's own pipeline", ErrOutOfScope, shorten(id), s.p.ID)
}

// PipelineID returns the pipeline id a tool must use: the room's pipeline when
// scoped (after CheckPipelineID passed), the input id when unscoped.
func (s Scope) PipelineID(inputID string) string {
	if s.unscoped {
		return inputID
	}
	return s.p.ID
}

// CheckConnector refuses a connector that is not one of the pipeline's connectors.
func (s Scope) CheckConnector(name string) error {
	if s.unscoped || contains(s.p.Connectors, name) {
		return nil
	}
	return refuse("connector", name, s, "allowed connectors: "+strings.Join(s.p.Connectors, ", "))
}

// derivedGroups returns the consumer groups the topology derived from the
// pipeline's own sink engines, or nil. They are part of the pipeline like the
// declared ones, so CheckGroup and FilterGroups accept them; the scope stays
// strict because the set is read from the pipeline's own objects and replaced
// on every topology build (owners.SetDerivedConsumerGroups).
func (s Scope) derivedGroups() []string {
	if s.derived == nil || s.p.ID == "" {
		return nil
	}
	return s.derived(s.p.ID)
}

// groupAllowed reports whether name is one of the pipeline's consumer groups:
// declared in owners.yaml or derived from a sink engine it owns.
func (s Scope) groupAllowed(name string) bool {
	return contains(s.p.ConsumerGroups, name) || contains(s.derivedGroups(), name)
}

// allowedGroups lists the consumer groups the scope accepts for an error:
// the declared ones and the ones derived from the pipeline's own engines.
func (s Scope) allowedGroups() []string {
	return slices.Concat(s.p.ConsumerGroups, s.derivedGroups())
}

// CheckGroup refuses a consumer group that is not one of the pipeline's groups
// or a group its own sink engines consume with (a ClickHouse Kafka engine
// table's kafka_group_name, read by the topology).
func (s Scope) CheckGroup(name string) error {
	if s.unscoped || s.groupAllowed(name) {
		return nil
	}
	return refuse("consumer group", name, s, "allowed consumer groups: "+strings.Join(s.allowedGroups(), ", "))
}

// Topics returns the pipeline's topics: "<topic_prefix>.<source_table>" for
// every source table. Same rule as the write actions' subject check, which
// derives subjects from these topics.
func (s Scope) Topics() []string {
	if s.p.TopicPrefix == "" {
		return nil
	}
	out := make([]string, 0, len(s.p.SourceTables))
	for _, t := range s.p.SourceTables {
		out = append(out, s.p.TopicPrefix+"."+t)
	}
	return out
}

// CheckTopic refuses a topic that is not one of the pipeline's topics.
func (s Scope) CheckTopic(topic string) error {
	if s.unscoped || contains(s.Topics(), topic) {
		return nil
	}
	return refuse("topic", topic, s, "allowed topics: "+strings.Join(s.Topics(), ", "))
}

// DLQTopics returns the dead-letter topics the pipeline declares in owners.yaml
// (watch.dlq_topics). Topics a sink connector names in its own config are not
// listed: dlq_peek reaches those through the connector, never by topic name.
func (s Scope) DLQTopics() []string {
	declared := s.p.Watch().DLQTopics
	out := make([]string, 0, len(declared))
	for _, d := range declared {
		out = append(out, d.Topic)
	}
	return out
}

// CheckDLQTopic refuses a topic that is not declared in the pipeline's
// watch.dlq_topics. A CDC topic is refused too (owners.yaml cannot declare
// one), so dlq_peek never becomes a way around CheckTopic.
func (s Scope) CheckDLQTopic(topic string) error {
	if s.unscoped {
		return nil
	}
	declared := s.DLQTopics()
	if contains(declared, topic) {
		return nil
	}
	if len(declared) == 0 {
		return refuse("DLQ topic", topic, s, "none declared (use connector for a sink's DLQ)")
	}
	return refuse("DLQ topic", topic, s, "declared DLQ topics: "+strings.Join(declared, ", "))
}

// subjectTopic strips the "-key"/"-value" suffix from a Schema Registry subject.
func subjectTopic(subject string) (string, bool) {
	for _, suf := range []string{"-key", "-value"} {
		if strings.HasSuffix(subject, suf) {
			return strings.TrimSuffix(subject, suf), true
		}
	}
	return "", false
}

// SubjectInScope reports whether subject is "<pipeline topic>-key" or
// "<pipeline topic>-value". The topic must match exactly: a suffix match would
// accept another pipeline's topic that merely ends with the same table name.
func (s Scope) SubjectInScope(subject string) bool {
	if s.unscoped {
		return true
	}
	topic, ok := subjectTopic(subject)
	return ok && contains(s.Topics(), topic)
}

// CheckSubject refuses a subject that is not derived from a pipeline topic.
func (s Scope) CheckSubject(subject string) error {
	if s.SubjectInScope(subject) {
		return nil
	}
	var allowed []string
	for _, t := range s.Topics() {
		allowed = append(allowed, t+"-key", t+"-value")
	}
	return refuse("subject", subject, s, "allowed subjects: "+strings.Join(allowed, ", "))
}

// CheckSourceTable refuses a table that is not one of the pipeline's source tables.
func (s Scope) CheckSourceTable(table string) error {
	if s.unscoped || contains(s.p.SourceTables, table) {
		return nil
	}
	return refuse("table", table, s, "allowed source tables: "+strings.Join(s.p.SourceTables, ", "))
}

// CheckSinkTable refuses a table that is not one of the pipeline's sink tables.
func (s Scope) CheckSinkTable(table string) error {
	if s.unscoped || contains(s.p.SinkTables, table) {
		return nil
	}
	return refuse("table", table, s, "allowed sink tables: "+strings.Join(s.p.SinkTables, ", "))
}

// ---- batch pipelines (P4) ----
//
// A batch pipeline's scope is its dags, models and sink_tables (CheckSinkTable
// above serves both kinds). It has no topic prefix, source tables, connectors
// or consumer groups, so every cdc check refuses on it, and a cdc pipeline has
// no dags or models, so CheckDAG and CheckModel refuse on it.

// Kind returns the room pipeline's kind, events.PipelineKindCDC or
// events.PipelineKindBatch; "" when unscoped, where no pipeline applies.
func (s Scope) Kind() string {
	if s.unscoped {
		return ""
	}
	return s.p.KindOrDefault()
}

// CheckDAG refuses a DAG id that is not one of the pipeline's dags.
func (s Scope) CheckDAG(dagID string) error {
	if s.unscoped || contains(s.p.DAGs, dagID) {
		return nil
	}
	return refuse("DAG", dagID, s, allowed("DAGs", s.p.DAGs))
}

// CheckModel refuses a model that is not one of the pipeline's models. name
// is a model name as owners.yaml writes it, or a model unique_id
// (model.<package>.<name>[.<version>]); owners.ModelsContain applies the same
// rule as the topology builder: a bare entry admits any package, a unique_id
// entry only its own package (and version, when it names one).
func (s Scope) CheckModel(name string) error {
	if s.unscoped || owners.ModelsContain(s.p.Models, name) {
		return nil
	}
	return refuse("model", name, s, allowed("models", s.p.Models))
}

// LineageReader reads one model's direct parents and children.
// clients.TransformReader satisfies it.
type LineageReader interface {
	ModelLineage(ctx context.Context, model string) (clients.Lineage, error)
}

// CheckModelOrAdjacent allows one of the pipeline's models or a direct parent
// or child of one, by the manifest's lineage: the rule of the dbt tools that
// read a model's lineage or SQL. Lineage is read only for a name that is not a
// pipeline model. It fails closed: a lineage read that fails is an error,
// never "in scope". A pipeline model that the manifest no longer has
// (clients.ErrNotFound) only has no neighbours. A nil reader is an error that
// wraps clients.ErrNotSupported.
func (s Scope) CheckModelOrAdjacent(ctx context.Context, name string, lr LineageReader) error {
	if s.unscoped || owners.ModelsContain(s.p.Models, name) {
		return nil
	}
	why := allowed("models", s.p.Models) + " and their direct parents and children"
	// A plain name only: a unique_id would let "model.other_pkg.x" pass as x.
	if n, ok := owners.ModelName(name); !ok || n != name {
		return refuse("model", name, s, why)
	}
	if len(s.p.Models) > 0 && lr == nil {
		return fmt.Errorf("tools: pipeline %q: no transform reader to read the lineage of its models: %w", s.p.ID, clients.ErrNotSupported)
	}
	for _, m := range s.p.Models {
		l, err := lr.ModelLineage(ctx, m)
		if errors.Is(err, clients.ErrNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("tools: pipeline %q: read the lineage of model %q to check %q: %w", s.p.ID, m, shorten(name), err)
		}
		for _, ref := range slices.Concat(l.Parents, l.Children) {
			if n, ok := owners.ModelName(ref); ok && n == name {
				return nil
			}
		}
	}
	return refuse("model", name, s, why)
}

// allowed lists names for a refusal, or says there are none: a cdc pipeline
// asked for a DAG gets "this pipeline has no DAGs", not an empty list.
func allowed(what string, names []string) string {
	if len(names) == 0 {
		return "this pipeline has no " + what
	}
	return "allowed " + what + ": " + strings.Join(names, ", ")
}

// TopicInScope reports whether topic is one of the pipeline's topics (always
// true when unscoped).
func (s Scope) TopicInScope(topic string) bool {
	return s.unscoped || contains(s.Topics(), topic)
}

// SourceTableInScope reports whether table is one of the pipeline's source tables.
func (s Scope) SourceTableInScope(table string) bool {
	return s.unscoped || contains(s.p.SourceTables, table)
}

// AnySourceTable reports whether match accepts one of the pipeline's source
// tables, for callers that must compare names by parsed identifier rather
// than by exact text.
func (s Scope) AnySourceTable(match func(table string) bool) bool {
	return s.unscoped || slices.ContainsFunc(s.p.SourceTables, match)
}

// FilterConnectors keeps only the pipeline's connectors.
func (s Scope) FilterConnectors(names []string) []string {
	return s.filter(names, func(n string) bool { return contains(s.p.Connectors, n) })
}

// FilterGroups keeps only the pipeline's consumer groups: declared in
// owners.yaml or derived from a sink engine the pipeline owns.
func (s Scope) FilterGroups(names []string) []string {
	if s.unscoped {
		return names
	}
	allowed := s.allowedGroups()
	return s.filter(names, func(n string) bool { return contains(allowed, n) })
}

// FilterSubjects keeps only subjects derived from the pipeline's topics.
func (s Scope) FilterSubjects(names []string) []string {
	return s.filter(names, s.SubjectInScope)
}

func (s Scope) filter(names []string, keep func(string) bool) []string {
	if s.unscoped {
		return names
	}
	var out []string
	for _, n := range names {
		if keep(n) {
			out = append(out, n)
		}
	}
	return out
}

// NotShown is the line a scoped list tool adds when it dropped objects that
// are not the pipeline's: a count and a kind (one for n == 1, many else),
// never a name. It is "" for n <= 0.
func NotShown(n int, one, many string) string {
	if n <= 0 {
		return ""
	}
	return fmt.Sprintf("%d other %s not shown (not part of this room's pipeline)", n, Plural(n, one, many))
}

// Plural returns one when n is 1 and many otherwise.
func Plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// ---- source database objects ----

// ConfigReader reads one Kafka Connect connector config. clients.ConnectClient
// satisfies it.
type ConfigReader interface {
	Config(ctx context.Context, name string) (map[string]string, error)
}

// Slots returns the replication slots that belong to the room's pipeline, by
// the watcher's rule:
//   - owners.yaml watch.slot, when set. It is the whole answer and needs no
//     Connect call, so the slot stays visible while Connect is down.
//   - otherwise slot.name from each of the pipeline's connector configs, or
//     Debezium's default "debezium" for a PostgresConnector that sets none.
//
// It fails closed. A connector config that cannot be read is an error, never
// "every slot": the caller then shows no slot. A slot.name that is set but is
// not a valid slot name (blank included) is an error too, so it never turns
// into the default slot, which can belong to another pipeline. Only a config
// with no slot.name key means the default. An empty list with a nil error
// means no connector of the pipeline is a Debezium Postgres source.
//
// Unscoped callers do not filter, so calling it unscoped is an error.
func (s Scope) Slots(ctx context.Context, cr ConfigReader) ([]string, error) {
	if s.unscoped {
		return nil, errors.New("tools: Slots needs a room pipeline (unscoped callers do not filter)")
	}
	if declared := s.p.Watch().Slot; declared != "" {
		return []string{declared}, nil
	}
	if len(s.p.Connectors) == 0 {
		// A batch pipeline: no connector, so no slot, whatever Connect says.
		return nil, nil
	}
	if cr == nil {
		return nil, fmt.Errorf("tools: pipeline %q: no Connect client to read the connector configs, and owners.yaml sets no watch.slot", s.p.ID)
	}
	var out []string
	for _, name := range s.p.Connectors {
		cfg, err := cr.Config(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("tools: pipeline %q: read connector %q config to find its slot (set watch.slot in owners.yaml to name it without Connect): %w", s.p.ID, name, err)
		}
		slot, err := owners.ConnectorSlot(cfg)
		if err != nil {
			return nil, fmt.Errorf("tools: pipeline %q: connector %q has an invalid slot.name %q: %w", s.p.ID, name, shorten(cfg["slot.name"]), err)
		}
		if slot != "" && !contains(out, slot) {
			out = append(out, slot)
		}
	}
	return out, nil
}
