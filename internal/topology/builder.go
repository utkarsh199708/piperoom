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
	// DLQRecentWarn is how young a dead-letter topic's newest record must be
	// for the topic to warn. Build overrides it per pipeline with owners
	// watch.dlq_growth.window (ThresholdsFor); this is the base default.
	DLQRecentWarn time.Duration
	// SchemaRecentWarn is how long after this builder saw a subject's latest
	// version change the subject still counts as registered recently (and
	// warns).
	SchemaRecentWarn time.Duration
}

// DefaultThresholds returns the thresholds the task specifies.
func DefaultThresholds() Thresholds {
	return Thresholds{
		SlotRetainedWarnBytes: 1 << 30, // 1 GiB
		GroupLagWarn:          1000,
		SinkStaleWarn:         10 * time.Minute,
		SinkStaleFail:         30 * time.Minute,
		HealthBudget:          5 * time.Second,
		DLQRecentWarn:         time.Hour,
		SchemaRecentWarn:      time.Hour,
	}
}

// ThresholdsFor returns base with the pipeline's watch settings applied. It is
// the one owners -> Thresholds mapping: the slot_wal "fire" size in owners.yaml
// is also the retained-WAL level where pipeline_map warns, so the map and the
// watcher cannot disagree about what "too much WAL" means. The dlq_growth
// window is likewise the age under which a dead-letter record still counts as
// new, so the map warns exactly when the watcher would count the record.
func ThresholdsFor(base Thresholds, p owners.Pipeline) Thresholds {
	w := p.Watch()
	base.SlotRetainedWarnBytes = w.SlotWAL.FireBytes
	base.DLQRecentWarn = w.DLQGrowth.Window
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
	// now is the clock for ages in node facts; tests replace it.
	now func() time.Time

	// warnMu guards warned, the set of invalid include/exclude list entries
	// already reported by this builder instance.
	warnMu sync.Mutex
	warned map[string]bool

	// sr is optional: without it the graph has no schema-subject nodes.
	sr clients.SchemaRegistryClient
	// srMu guards subjects, this builder's last reading of each subject's
	// latest version (see subjectSeen).
	srMu     sync.Mutex
	subjects map[string]subjectSeen
}

// Option configures a Builder made by NewBuilder.
type Option func(*builder)

// WithSchemaRegistry lets the builder add the Schema Registry subjects the
// pipeline's connectors and consumers depend on, with depends_on edges.
func WithSchemaRegistry(sr clients.SchemaRegistryClient) Option {
	return func(b *builder) { b.sr = sr }
}

// NewBuilder constructs a Builder with default thresholds.
func NewBuilder(o *owners.Owners, connect clients.ConnectClient, kafka clients.KafkaAdmin, src clients.SourceDB, wh clients.Warehouse, opts ...Option) Builder {
	b := newBuilder(o, connect, kafka, src, wh, slog.Default())
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// newBuilder is NewBuilder with an explicit logger; same-package tests use it
// to capture warnings on their own logger.
func newBuilder(o *owners.Owners, connect clients.ConnectClient, kafka clients.KafkaAdmin, src clients.SourceDB, wh clients.Warehouse, log *slog.Logger) *builder {
	if log == nil {
		log = slog.Default()
	}
	return &builder{
		o:        o,
		connect:  connect,
		kafka:    kafka,
		src:      src,
		wh:       wh,
		t:        DefaultThresholds(),
		log:      log,
		now:      time.Now,
		warned:   map[string]bool{},
		subjects: map[string]subjectSeen{},
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
	info.matchers = b.sourceMatchers(p, info)

	// Topics and data edges come from config and metadata collect already
	// read (edges.go); the schema subjects follow from the topics' users.
	fp := b.planFlow(p, info)
	dlqs := discoverDLQs(p, info)
	subjects, deps := b.planSubjects(info, fp)

	// Only query Kafka for topics we actually derived; placeholder topics have
	// unknown health because their real names are unavailable.
	var queried []string
	for _, t := range fp.topics {
		if !fp.placeholder[t] {
			queried = append(queried, t)
		}
	}
	for _, d := range dlqs {
		// A DLQ that is also a CDC topic is refused when owners.yaml loads;
		// the guard only keeps one Kafka call per name.
		if !slices.Contains(queried, d.topic) {
			queried = append(queried, d.topic)
		}
	}
	// Kafka and the registry are independent, so they share the budget.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		b.collectTopics(cctx, queried, info)
	}()
	go func() {
		defer wg.Done()
		b.collectSchemas(cctx, subjects, info)
	}()
	wg.Wait()

	g := Graph{PipelineID: pipelineID}
	for _, tbl := range p.SourceTables {
		g.Nodes = append(g.Nodes, b.sourceNode(p, tbl, info))
	}
	for _, cn := range p.Connectors {
		n := b.connectorNode(p, cn, info)
		// Connector DLQ facts need the DLQ topic reading (dlq_topic_missing).
		b.addConnectorDLQFacts(&n, info.connect[cn], info)
		g.Nodes = append(g.Nodes, n)
	}
	for _, t := range fp.topics {
		if fp.placeholder[t] {
			g.Nodes = append(g.Nodes, b.placeholderTopicNode(p, t, fp.invalidName[t]))
		} else {
			g.Nodes = append(g.Nodes, b.topicNode(p, t, info))
		}
	}
	th := ThresholdsFor(b.t, p)
	for _, d := range dlqs {
		g.Nodes = append(g.Nodes, b.dlqNode(d, th, info))
	}
	// The pipeline's topics, for the group lag: the ones derived from the
	// connector configs plus "<topic_prefix>.<table>" from owners.yaml, so
	// the filter still knows them while Connect is down.
	ownTopics := map[string]bool{}
	for _, t := range fp.topics {
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
	// Subjects are side nodes, after the data flow.
	for _, s := range subjects {
		g.Nodes = append(g.Nodes, b.subjectNode(s, info, th))
	}

	g.Edges = fp.edges
	// DLQ edges are side edges into the dead-letter node; nothing leaves it.
	// Their writers come from connector config or an explicit owners.yaml
	// declaration, so they are mapped.
	for _, d := range dlqs {
		for _, w := range d.writerIDs() {
			g.Edges = append(g.Edges, Edge{From: w, To: "dlq:" + d.topic})
		}
	}
	g.Edges = append(g.Edges, deps...)

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
	slotName, err := b.slotName(p, tbl, info)
	if err != nil {
		// A broken slot.name never falls back to the default slot, which can
		// be another pipeline's: the slot is unknown, not someone else's.
		n.Facts = map[string]string{"slot": "invalid"}
		return n
	}
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

// slotName finds the replication slot that streams tbl, by the rule of
// tools.Scope.Slots and the watcher (owners.ConnectorSlot), so the map and
// pg_replication_slots name the same slot:
//   - owners.yaml watch.slot, when set, is the answer and needs no Connect
//     call, so the slot stays visible while Connect is down;
//   - otherwise the slot of the connector that streams tbl. info.matchers
//     holds only readable source connectors: a sink has no include list,
//     which would read as "streams every table" and lend the table a slot.
//
// "" with a nil error means no slot is known (Connect down, or the streaming
// connector is not a Debezium Postgres source). A present but invalid
// slot.name is an error.
func (b *builder) slotName(p owners.Pipeline, tbl string, info *collected) (string, error) {
	if declared := p.Watch().Slot; declared != "" {
		return declared, nil
	}
	for _, cn := range p.Connectors {
		m, ok := info.matchers[cn]
		if ok && m.streams(tbl) {
			return owners.ConnectorSlot(info.connect[cn].config)
		}
	}
	return "", nil
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

// placeholderTopicNode is used when connector config is unavailable, or when
// it names an illegal topic (invalid): the config itself is then a likely root
// cause, so the fact says so instead of "unreachable". The topic
// node exists to keep the graph shape but its real name and health are unknown.
func (b *builder) placeholderTopicNode(p owners.Pipeline, topic string, invalid bool) Node {
	n := Node{
		ID:     "topic:" + topic,
		Kind:   KindTopic,
		Name:   topic,
		Layer:  "kafka",
		Owner:  b.layerOwner("kafka"),
		Health: HealthUnknown,
		Facts:  map[string]string{"error": "unreachable"},
	}
	if invalid {
		n.Facts = map[string]string{"topic_name": "invalid"}
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
	lag, ok := pipelineLag(snap.lag, ownTopics)
	if !ok {
		// The group reads partitions, but none of this pipeline's topics (a
		// RegexRouter or topic-routing SMT renames them, or the group reads
		// other topics only). Its lag is unknown here, not zero.
		n.Facts = map[string]string{"lag": lagUnknown, "state": lag.State}
		return n
	}
	n.Health, n.Facts = groupHealth(lag, b.t)
	return n
}

// lagUnknown is the lag fact of a group none of whose partitions is one of the
// pipeline's topics.
const lagUnknown = "unknown (no partition of this pipeline's topics)"

// pipelineLag keeps only the partitions of the pipeline's own topics and sums
// the lag again from them. A group can also read other pipelines' topics:
// their lag is not this pipeline's health and must not show on its map (the
// same rule as kafka_consumer_lag). Unknown (unreadable) partitions are kept
// the same way, so a broker outage on the pipeline's topics reaches
// groupHealth (#508). ok is false when the group has partitions but none of
// them is the pipeline's: the lag is then unknown, never 0. A lag without
// partitions is kept as it is: there is nothing to tell apart, and the real
// client only returns one when the group has no readable partition.
func pipelineLag(l clients.GroupLag, ownTopics map[string]bool) (clients.GroupLag, bool) {
	if len(l.Partitions) == 0 && len(l.Unknown) == 0 {
		return l, true
	}
	var kept, unknown []clients.PartitionLag
	var total int64
	for _, pl := range l.Partitions {
		if ownTopics[pl.Topic] {
			kept = append(kept, pl)
			total += pl.Lag
		}
	}
	for _, pl := range l.Unknown {
		if ownTopics[pl.Topic] {
			unknown = append(unknown, pl)
		}
	}
	l.Partitions, l.TotalLag, l.Unknown = kept, total, unknown
	return l, len(kept) > 0 || len(unknown) > 0
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
	switch {
	case errors.Is(snap.err, clients.ErrNotFound):
		// The table or its timestamp column is gone: nothing can load.
		n.Health = HealthFail
		n.Facts = map[string]string{"missing": "true"}
	case snap.err != nil:
		n.Facts = map[string]string{"error": "unreachable"}
	default:
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
	// exact is false when some entry may not match the way Java's regex
	// engine would (see compileList); what the lists prove is then unconfirmed.
	exact bool
}

func newListMatcher(include, exclude string, warn func(string)) listMatcher {
	inc, incExact := compileList(include, warn)
	exc, excExact := compileList(exclude, warn)
	return listMatcher{includes: inc, excludes: exc, exact: incExact && excExact}
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
// EqualFold. An entry compileConfigRegex refuses (RE2 rejects it, or Go may
// not give Java's answer) is compared literally and reported to warn, when
// non-nil; callers dedupe. A list longer than maxConfigRegex compiles none of
// its regex entries. Either way exact is false.
func compileList(list string, warn func(string)) (patterns []listPattern, exact bool) {
	entries := splitCSV(list)
	if len(entries) == 0 {
		return nil, true
	}
	tooLong := len(list) > maxConfigRegex
	exact = !tooLong
	out := make([]listPattern, 0, len(entries))
	for _, e := range entries {
		if !strings.ContainsAny(e, regexMetaChars) {
			out = append(out, listPattern{literal: e})
			continue
		}
		if tooLong {
			out = append(out, listPattern{literal: e})
			continue
		}
		re, _, ok := compileConfigRegex("(?i)", e)
		if !ok {
			if warn != nil {
				warn(e)
			}
			exact = false
			out = append(out, listPattern{literal: e})
			continue
		}
		out = append(out, listPattern{re: re})
	}
	return out, exact
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
	log.Warn("topology: include/exclude list entry is not a regex Go can evaluate like Java, comparing literally", "entry", entry)
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

// exact reports whether streams gives the connector's own answer.
func (m streamMatcher) exact() bool { return m.tables.exact && m.schemas.exact }

// streamsTable reports whether a connector with the given config streams tbl.
// An empty include list streams every table in the pipeline. warn, when
// non-nil, receives invalid list entries for reporting.
func streamsTable(cfg map[string]string, tbl string, warn func(string)) bool {
	return newStreamMatcher(cfg, warn).streams(tbl)
}

// fallbackTopics returns placeholder topic names when connector config is
// unavailable, so the graph keeps its shape. The Debezium topic prefix is
// unknown, so the fully-qualified table names stand in for the topics.
func fallbackTopics(tables []string) []string {
	return append([]string(nil), tables...)
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
