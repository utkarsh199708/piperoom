package topology

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// ErrBadInput marks a request whose pipeline id or table is not known to the
// owners config. Tools wrap it in tools.ErrBadInput so the agent reports it as
// an input problem, not a target-system failure.
var ErrBadInput = errors.New("topology: bad input")

// Thresholds holds the boundaries used to decide node health.
type Thresholds struct {
	// SlotRetainedWarnBytes is the retained WAL size at or above which a slot
	// warns. Build overrides it per pipeline with owners watch.slot_wal.fire
	// (ThresholdsFor); this value is only the base default.
	SlotRetainedWarnBytes int64
	// GroupLagWarn is the consumer lag above which a group warns.
	GroupLagWarn int64
	// SinkStaleWarn is the sink freshness age that raises a warning.
	SinkStaleWarn time.Duration
	// SinkStaleFail is the sink freshness age that raises a failure.
	SinkStaleFail time.Duration
	// HealthBudget caps the parallel health-gathering phase.
	HealthBudget time.Duration
}

// DefaultThresholds returns the thresholds the task specifies.
func DefaultThresholds() Thresholds {
	return Thresholds{
		SlotRetainedWarnBytes: 1 << 30, // 1 GiB
		GroupLagWarn:          1000,
		SinkStaleWarn:         10 * time.Minute,
		SinkStaleFail:         30 * time.Minute,
		HealthBudget:          5 * time.Second,
	}
}

// ThresholdsFor returns base with the pipeline's watch settings applied. It is
// the one owners -> Thresholds mapping: the slot_wal "fire" size in owners.yaml
// is also the retained-WAL level where pipeline_map warns, so the map and the
// watcher cannot disagree about what "too much WAL" means.
func ThresholdsFor(base Thresholds, p owners.Pipeline) Thresholds {
	base.SlotRetainedWarnBytes = p.Watch().SlotWAL.FireBytes
	return base
}

// builder builds pipeline graphs and traces records.
type builder struct {
	o       *owners.Owners
	connect clients.ConnectClient
	kafka   clients.KafkaAdmin
	src     clients.SourceDB
	wh      clients.Warehouse
	t       Thresholds
	log     *slog.Logger

	// warnMu guards warned, the set of invalid include/exclude list entries
	// already reported by this builder instance.
	warnMu sync.Mutex
	warned map[string]bool
}

// NewBuilder constructs a Builder with default thresholds.
func NewBuilder(o *owners.Owners, connect clients.ConnectClient, kafka clients.KafkaAdmin, src clients.SourceDB, wh clients.Warehouse) Builder {
	return newBuilder(o, connect, kafka, src, wh, slog.Default())
}

// newBuilder is NewBuilder with an explicit logger; same-package tests use it
// to capture warnings on their own logger.
func newBuilder(o *owners.Owners, connect clients.ConnectClient, kafka clients.KafkaAdmin, src clients.SourceDB, wh clients.Warehouse, log *slog.Logger) *builder {
	if log == nil {
		log = slog.Default()
	}
	return &builder{
		o:       o,
		connect: connect,
		kafka:   kafka,
		src:     src,
		wh:      wh,
		t:       DefaultThresholds(),
		log:     log,
		warned:  map[string]bool{},
	}
}

// Build returns the current graph for a pipeline with live health. Unreachable
// systems give HealthUnknown on their nodes, never an error for the build.
func (b *builder) Build(ctx context.Context, pipelineID string) (Graph, error) {
	p, ok := b.o.Pipeline(pipelineID)
	if !ok {
		return Graph{}, fmt.Errorf("%w: pipeline %q not found", ErrBadInput, pipelineID)
	}

	cctx, cancel := context.WithTimeout(ctx, b.t.HealthBudget)
	defer cancel()

	info := b.collect(cctx, p)

	g := Graph{PipelineID: pipelineID}

	for _, tbl := range p.SourceTables {
		g.Nodes = append(g.Nodes, b.sourceNode(p, tbl, info))
	}

	var topics []string
	connTopics := map[string][]string{}
	placeholder := map[string]bool{}
	for _, cn := range p.Connectors {
		g.Nodes = append(g.Nodes, b.connectorNode(p, cn, info))
		var ts []string
		isFallback := false
		if snap, ok := info.connect[cn]; ok && snap.err == nil {
			ts = deriveTopics(snap.config, p.SourceTables, b.warnBadListEntry)
		} else {
			// Connect unreachable: keep the graph shape with placeholder
			// topics so the topic node and its edges still exist.
			ts = fallbackTopics(p.SourceTables)
			isFallback = true
		}
		connTopics[cn] = ts
		for _, t := range ts {
			if !slices.Contains(topics, t) {
				topics = append(topics, t)
			}
			if isFallback {
				placeholder[t] = true
			}
		}
	}

	// Only query Kafka for topics we actually derived; placeholder topics have
	// unknown health because their real names are unavailable.
	var realTopics []string
	for _, t := range topics {
		if !placeholder[t] {
			realTopics = append(realTopics, t)
		}
	}
	b.collectTopics(cctx, realTopics, info)

	for _, t := range topics {
		if placeholder[t] {
			g.Nodes = append(g.Nodes, b.placeholderTopicNode(p, t))
		} else {
			g.Nodes = append(g.Nodes, b.topicNode(p, t, info))
		}
	}
	// The pipeline's topics, for the group lag: the ones derived from the
	// connector configs plus "<topic_prefix>.<table>" from owners.yaml, so
	// the filter still knows them while Connect is down.
	ownTopics := map[string]bool{}
	for _, t := range topics {
		ownTopics[t] = true
	}
	if p.TopicPrefix != "" {
		for _, t := range p.SourceTables {
			ownTopics[p.TopicPrefix+"."+t] = true
		}
	}
	for _, gr := range p.ConsumerGroups {
		g.Nodes = append(g.Nodes, b.groupNode(p, gr, info, ownTopics))
	}
	for _, s := range p.SinkTables {
		g.Nodes = append(g.Nodes, b.sinkNode(p, s, info))
	}

	// Edges: table -> connector -> topic -> group -> sink.
	for _, tbl := range p.SourceTables {
		for _, cn := range p.Connectors {
			g.Edges = append(g.Edges, Edge{From: "pg:" + tbl, To: "connect:" + cn})
		}
	}
	for _, cn := range p.Connectors {
		for _, t := range connTopics[cn] {
			g.Edges = append(g.Edges, Edge{From: "connect:" + cn, To: "topic:" + t})
		}
	}
	for _, t := range topics {
		for _, gr := range p.ConsumerGroups {
			g.Edges = append(g.Edges, Edge{From: "topic:" + t, To: "group:" + gr})
		}
	}
	for _, gr := range p.ConsumerGroups {
		for _, s := range p.SinkTables {
			g.Edges = append(g.Edges, Edge{From: "group:" + gr, To: "wh:" + s})
		}
	}

	return g, nil
}

// LayerOwner returns the user id that owns a layer. Layer ownership is global
// in owners.yaml today, so the pipeline id is accepted for interface symmetry.
func (b *builder) LayerOwner(pipelineID, layer string) (string, bool) {
	return b.o.LayerOwner(layer)
}

func (b *builder) layerOwner(layer string) string {
	u, _ := b.o.LayerOwner(layer)
	return u
}

func (b *builder) sourceNode(p owners.Pipeline, tbl string, info *collected) Node {
	n := Node{
		ID:     "pg:" + tbl,
		Kind:   KindSourceTable,
		Name:   tbl,
		Layer:  "postgres",
		Owner:  b.layerOwner("postgres"),
		Health: HealthUnknown,
	}
	if info.slotsErr != nil {
		n.Facts = map[string]string{"error": "unreachable"}
		return n
	}
	slotName := b.slotName(p, tbl, info)
	if slotName == "" {
		n.Facts = map[string]string{"slot": "unknown"}
		return n
	}
	var slot *clients.ReplicationSlot
	for i := range info.slots {
		if info.slots[i].Name == slotName {
			slot = &info.slots[i]
			break
		}
	}
	if slot == nil {
		n.Facts = map[string]string{"slot": "missing"}
		return n
	}
	n.Health, n.Facts = sourceHealth(*slot, ThresholdsFor(b.t, p))
	return n
}

// slotName finds the replication slot name for the connector that streams tbl.
func (b *builder) slotName(p owners.Pipeline, tbl string, info *collected) string {
	for _, cn := range p.Connectors {
		snap, ok := info.connect[cn]
		if !ok || snap.err != nil {
			continue
		}
		if streamsTable(snap.config, tbl, b.warnBadListEntry) {
			return slotNameFor(cn, snap.config)
		}
	}
	return ""
}

func (b *builder) connectorNode(p owners.Pipeline, cn string, info *collected) Node {
	n := Node{
		ID:     "connect:" + cn,
		Kind:   KindConnector,
		Name:   cn,
		Layer:  "kafka-connect",
		Owner:  b.layerOwner("kafka-connect"),
		Health: HealthUnknown,
	}
	snap := info.connect[cn]
	if snap.err != nil {
		if errors.Is(snap.err, clients.ErrNotFound) {
			n.Health = HealthFail
			n.Facts = map[string]string{"missing": "true"}
			return n
		}
		n.Facts = map[string]string{"error": "unreachable"}
		return n
	}
	n.Health, n.Facts = connectorHealth(snap.status)
	return n
}

func (b *builder) topicNode(p owners.Pipeline, topic string, info *collected) Node {
	n := Node{
		ID:     "topic:" + topic,
		Kind:   KindTopic,
		Name:   topic,
		Layer:  "kafka",
		Owner:  b.layerOwner("kafka"),
		Health: HealthUnknown,
	}
	n.Health, n.Facts = topicHealth(info.topicErr[topic], info.topicNewest[topic], info.now)
	return n
}

// placeholderTopicNode is used when connector config is unavailable: the topic
// node exists to keep the graph shape but its real name and health are unknown.
func (b *builder) placeholderTopicNode(p owners.Pipeline, topic string) Node {
	n := Node{
		ID:     "topic:" + topic,
		Kind:   KindTopic,
		Name:   topic,
		Layer:  "kafka",
		Owner:  b.layerOwner("kafka"),
		Health: HealthUnknown,
		Facts:  map[string]string{"error": "unreachable"},
	}
	return n
}

func (b *builder) groupNode(p owners.Pipeline, gr string, info *collected, ownTopics map[string]bool) Node {
	n := Node{
		ID:     "group:" + gr,
		Kind:   KindConsumerGroup,
		Name:   gr,
		Layer:  "consumer",
		Owner:  b.layerOwner("consumer"),
		Health: HealthUnknown,
	}
	snap := info.groups[gr]
	if snap.err != nil {
		n.Facts = map[string]string{"error": "unreachable"}
		return n
	}
	n.Health, n.Facts = groupHealth(pipelineLag(snap.lag, ownTopics), b.t)
	return n
}

// pipelineLag keeps only the partitions of the pipeline's own topics and sums
// the lag again from them. A group can also read other pipelines' topics:
// their lag is not this pipeline's health and must not show on its map (the
// same rule as kafka_consumer_lag). A lag without partitions is kept as it is:
// there is nothing to tell apart, and the real client only returns one when
// the group has no readable partition at all.
func pipelineLag(l clients.GroupLag, ownTopics map[string]bool) clients.GroupLag {
	if len(l.Partitions) == 0 {
		return l
	}
	var kept []clients.PartitionLag
	var total int64
	for _, pl := range l.Partitions {
		if ownTopics[pl.Topic] {
			kept = append(kept, pl)
			total += pl.Lag
		}
	}
	l.Partitions, l.TotalLag = kept, total
	return l
}

func (b *builder) sinkNode(p owners.Pipeline, s string, info *collected) Node {
	n := Node{
		ID:     "wh:" + s,
		Kind:   KindSinkTable,
		Name:   s,
		Layer:  "warehouse",
		Owner:  b.layerOwner("warehouse"),
		Health: HealthUnknown,
	}
	snap := info.freshness[s]
	if snap.err != nil {
		n.Facts = map[string]string{"error": "unreachable"}
	} else {
		n.Health, n.Facts = sinkHealth(snap.ts, info.now, b.t)
	}
	// The timestamp column is set on every sink node, including unreachable
	// ones, so the agent knows which column its recovery check must read.
	if n.Facts == nil {
		n.Facts = map[string]string{}
	}
	n.Facts["ts_column"] = p.SinkTSColumn
	return n
}

// ---- Debezium include/exclude lists ----
//
// Debezium interprets table.include.list, table.exclude.list, schema.include.list
// and schema.exclude.list as comma-separated regular expressions, each
// anchored to the whole identifier and matched case-insensitively: table lists
// match the fully-qualified schema.table id, schema lists match the schema
// name. Exclude lists win over include lists. See "Advanced PostgreSQL
// connector configuration properties"
// in https://debezium.io/documentation/reference/stable/connectors/postgresql.html

// regexMetaChars are the RE2 metacharacters. An entry without any of them can
// only match itself, so it takes the literal fast path and skips compilation.
const regexMetaChars = `\.+*?()|[]{}^$`

// listPattern is one entry of a Debezium include or exclude list.
type listPattern struct {
	literal string         // compared case-insensitively when re is nil
	re      *regexp.Regexp // nil for entries with no regex metacharacters
}

func (p listPattern) matches(id string) bool {
	if p.re == nil {
		return strings.EqualFold(p.literal, id)
	}
	return p.re.MatchString(id)
}

// listMatcher matches identifiers against one include/exclude list pair
// (table-level or schema-level) the way the Debezium connector would: any
// include entry must match (an empty include list accepts everything) and no
// exclude entry may match.
type listMatcher struct {
	includes []listPattern
	excludes []listPattern
}

func newListMatcher(include, exclude string, warn func(string)) listMatcher {
	return listMatcher{
		includes: compileList(include, warn),
		excludes: compileList(exclude, warn),
	}
}

func (m listMatcher) matches(id string) bool {
	if len(m.includes) > 0 && !anyPatternMatches(m.includes, id) {
		return false
	}
	return !anyPatternMatches(m.excludes, id)
}

func anyPatternMatches(ps []listPattern, id string) bool {
	for _, p := range ps {
		if p.matches(id) {
			return true
		}
	}
	return false
}

// compileList turns a comma-separated Debezium list into patterns. Debezium
// compiles these lists with Pattern.CASE_INSENSITIVE and matches the whole
// identifier (Strings.setOfRegex + Matcher.matches in its Predicates), so
// every entry is compiled as "(?i)^(?:entry)$". Entries without regex
// metacharacters take the literal fast path and match case-insensitively via
// EqualFold. warn, when non-nil, is called for each entry that fails to
// compile; callers dedupe.
func compileList(list string, warn func(string)) []listPattern {
	entries := splitCSV(list)
	if len(entries) == 0 {
		return nil
	}
	out := make([]listPattern, 0, len(entries))
	for _, e := range entries {
		if !strings.ContainsAny(e, regexMetaChars) {
			out = append(out, listPattern{literal: e})
			continue
		}
		re, err := regexp.Compile("(?i)^(?:" + e + ")$")
		if err != nil {
			if warn != nil {
				warn(e)
			}
			out = append(out, listPattern{literal: e})
			continue
		}
		out = append(out, listPattern{re: re})
	}
	return out
}

// warnBadListEntry reports once per builder instance that a list entry did not
// compile as a regex and was compared literally. The dedupe set lives on the
// builder (no package-level state), so it covers both the Build and the Trace
// matching paths and disappears with the builder.
func (b *builder) warnBadListEntry(entry string) {
	b.warnMu.Lock()
	defer b.warnMu.Unlock()
	if b.warned == nil {
		b.warned = map[string]bool{}
	}
	if b.warned[entry] {
		return
	}
	b.warned[entry] = true
	log := b.log
	if log == nil {
		log = slog.Default()
	}
	log.Warn("topology: include/exclude list entry is not a valid regex, comparing literally", "entry", entry)
}

// streamMatcher decides which pipeline tables a connector streams, following
// Debezium: the table lists match the whole schema.table id, the schema lists
// match its schema part.
type streamMatcher struct {
	tables  listMatcher
	schemas listMatcher
}

func newStreamMatcher(cfg map[string]string, warn func(string)) streamMatcher {
	return streamMatcher{
		tables:  newListMatcher(cfg["table.include.list"], cfg["table.exclude.list"], warn),
		schemas: newListMatcher(cfg["schema.include.list"], cfg["schema.exclude.list"], warn),
	}
}

func (m streamMatcher) streams(tbl string) bool {
	schema, _, _ := strings.Cut(tbl, ".")
	return m.tables.matches(tbl) && m.schemas.matches(schema)
}

// streamsTable reports whether a connector with the given config streams tbl.
// An empty include list streams every table in the pipeline. warn, when
// non-nil, receives invalid list entries for reporting.
func streamsTable(cfg map[string]string, tbl string, warn func(string)) bool {
	return newStreamMatcher(cfg, warn).streams(tbl)
}

func slotNameFor(_ string, cfg map[string]string) string {
	if n := cfg["slot.name"]; n != "" {
		return n
	}
	return "debezium"
}

// fallbackTopics returns placeholder topic names when connector config is
// unavailable, so the graph keeps its shape. The Debezium topic prefix is
// unknown, so the fully-qualified table names stand in for the topics.
func fallbackTopics(tables []string) []string {
	return append([]string(nil), tables...)
}

// deriveTopics returns the Debezium topic names for the pipeline tables this
// connector streams: <topic.prefix>.<schema>.<table>. warn is passed to the
// matching for invalid list entries.
func deriveTopics(cfg map[string]string, tables []string, warn func(string)) []string {
	prefix := cfg["topic.prefix"]
	if prefix == "" {
		return nil
	}
	m := newStreamMatcher(cfg, warn)
	var topics []string
	for _, t := range tables {
		if m.streams(t) {
			topics = append(topics, prefix+"."+t)
		}
	}
	return topics
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
