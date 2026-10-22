package topology

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// Column lineage (X7a, docs/design/lineage.md section 2): Edge.Columns on the
// CDC hops connect:<source> -> topic:<t> and <writer> -> wh:<sink>, from the
// source and sink catalogs (clients.ColumnReader, optional on the clients)
// and the topic's latest value schema in Schema Registry, matched by name
// after the connectors' transforms. Every read shares the build's
// HealthBudget; a read that fails or is not supported leaves its hops
// without Columns and never fails the build. Names only, never a value.

// columnReads is what one Build read for column lineage. A missing key is
// unknown (no reader, an error, or no schema).
type columnReads struct {
	mu          sync.Mutex
	srcCols     map[string][]string // source table -> column names, ordinal order
	sinkCols    map[string][]string // sink ref key -> column names, ordinal order
	topicFields map[string][]string // topic -> value field paths
}

// collectLineage reads the catalogs and value schemas the pipeline's hops
// need, in parallel under ctx. The catalog readers check scope themselves and
// fail closed, so ctx carries the pipeline (tools.WithPipeline).
func (b *builder) collectLineage(ctx context.Context, p owners.Pipeline, fp flowPlan, info *collected) *columnReads {
	lr := &columnReads{srcCols: map[string][]string{}, sinkCols: map[string][]string{}, topicFields: map[string][]string{}}
	sctx := tools.WithPipeline(ctx, p)
	var wg sync.WaitGroup
	run := func(f func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f()
		}()
	}
	src, srcOK := b.src.(clients.ColumnReader)
	if srcOK {
		for _, tbl := range p.SourceTables {
			run(func() {
				if cols, ok := b.readColumns(sctx, src, p.ID, "source", tbl); ok {
					lr.mu.Lock()
					lr.srcCols[tbl] = cols
					lr.mu.Unlock()
				}
			})
		}
	}
	sinkOK := false
	for _, ref := range p.SinkRefs() {
		wh, connected := b.sinkClient(p, ref)
		cr, ok := wh.(clients.ColumnReader)
		if !connected || !ok {
			continue
		}
		sinkOK = true
		run(func() {
			if cols, ok := b.readColumns(sctx, cr, p.ID, "sink", ref.Table); ok {
				lr.mu.Lock()
				lr.sinkCols[ref.Key] = cols
				lr.mu.Unlock()
			}
		})
	}
	// A topic's fields matter only next to a catalog on one side.
	if b.sr != nil && info.srErr == nil && (srcOK || sinkOK) {
		for _, t := range fp.topics {
			subject := t + "-value"
			if fp.placeholder[t] || !info.srListed[subject] {
				continue
			}
			run(func() {
				sv, err := b.sr.Schema(ctx, subject, -1)
				if err != nil {
					b.log.Debug("topology: value schema unreadable; the topic has no column lineage", "pipeline", p.ID, "subject", subject, "err", err)
					return
				}
				fields, ok := avroValueFields(sv.Schema)
				if !ok {
					b.log.Debug("topology: value schema is not an Avro record; the topic has no column lineage", "pipeline", p.ID, "subject", subject)
					return
				}
				lr.mu.Lock()
				lr.topicFields[t] = fields
				lr.mu.Unlock()
			})
		}
	}
	wg.Wait()
	return lr
}

// readColumns reads one table's column names. ErrNotSupported turns that side
// off for the pipeline with one INFO log per builder; another error leaves the
// table unknown (logged at debug: catalog errors carry names, never values).
func (b *builder) readColumns(ctx context.Context, cr clients.ColumnReader, pipelineID, side, table string) ([]string, bool) {
	cols, err := cr.Columns(ctx, table)
	if err != nil {
		if errors.Is(err, clients.ErrNotSupported) {
			b.infoOnce("\x00columns:"+pipelineID+":"+side,
				"topology: the "+side+" client cannot read column catalogs; no column lineage on that side", "pipeline", pipelineID)
		} else {
			b.log.Debug("topology: column catalog unreadable; its hops have no column lineage", "pipeline", pipelineID, "table", table, "err", err)
		}
		return nil, false
	}
	cols = slices.Clone(cols[:min(len(cols), clients.MaxTableColumns)])
	slices.SortStableFunc(cols, func(a, b clients.ColumnInfo) int { return a.Position - b.Position })
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names, true
}

// infoOnce logs msg at INFO the first time key is seen by this builder.
func (b *builder) infoOnce(key, msg string, args ...any) {
	b.warnMu.Lock()
	seen := b.warned[key]
	if b.warned == nil {
		b.warned = map[string]bool{}
	}
	b.warned[key] = true
	b.warnMu.Unlock()
	if !seen {
		b.log.Info(msg, args...)
	}
}

// topicFeed is the one source table, and its connector, a topic carries.
type topicFeed struct{ table, connector string }

// topicFeeds maps each real topic to the source table it carries, by the
// rule planFlow names topics with (<topic.prefix>.<table>, routed). A topic
// that carries several tables (a logical-table router merges them) has no
// single input and is left out.
func topicFeeds(p owners.Pipeline, info *collected, fp flowPlan) map[string]topicFeed {
	out := map[string]topicFeed{}
	shared := map[string]bool{}
	for _, cn := range p.Connectors {
		m, ok := info.matchers[cn]
		cfg := info.connect[cn].config
		if !ok || cfg["topic.prefix"] == "" {
			continue
		}
		exact := m.exact() && isDebezium(cfg)
		for _, tbl := range p.SourceTables {
			if exact && !m.streams(tbl) {
				continue
			}
			routed, _ := routeTopics(cfg, []string{cfg["topic.prefix"] + "." + tbl})
			t := routed[0]
			if fp.placeholder[t] {
				continue
			}
			if prev, ok := out[t]; ok && prev != (topicFeed{tbl, cn}) {
				shared[t] = true
			}
			out[t] = topicFeed{tbl, cn}
		}
	}
	for t := range shared {
		delete(out, t)
	}
	return out
}

// addColumns fills Edge.Columns on the pipeline's CDC hops from lr.
func (b *builder) addColumns(g *Graph, p owners.Pipeline, info *collected, fp flowPlan, lr *columnReads) {
	if len(lr.srcCols) == 0 && len(lr.sinkCols) == 0 {
		return
	}
	feeds := topicFeeds(p, info, fp)
	refs := map[string]owners.SinkRef{}
	for _, ref := range p.SinkRefs() {
		refs["wh:"+ref.Key] = ref
	}
	for i := range g.Edges {
		e := &g.Edges[i]
		if e.IsDependency() {
			continue
		}
		var maps []ColumnMap
		if t, ok := strings.CutPrefix(e.To, "topic:"); ok && strings.HasPrefix(e.From, "connect:") {
			maps = topicHop(t, strings.TrimPrefix(e.From, "connect:"), info, lr, feeds)
		} else if ref, ok := refs[e.To]; ok {
			maps = sinkHop(g.Edges, e.From, ref, info, fp, lr, feeds)
		}
		if len(maps) > 0 {
			e.Columns, e.ColumnsOmitted = LimitColumns(maps)
		}
	}
}

// topicHop maps a source table's columns to its topic's value fields through
// the source connector. Without the topic's fields (no registry, an error,
// not Avro) the hop has no Columns.
func topicHop(topic, cn string, info *collected, lr *columnReads, feeds map[string]topicFeed) []ColumnMap {
	f, ok := feeds[topic]
	if !ok || f.connector != cn {
		return nil
	}
	cols, okCols := lr.srcCols[f.table]
	fields, okFields := lr.topicFields[topic]
	if !okCols || !okFields {
		return nil
	}
	up, modeled := sourceFlow(cols, info.connect[cn].config)
	return matchColumns("pg:"+f.table, up, fields, !modeled, false)
}

// sinkHop maps the writer's input to the sink table's columns. The input is
// the topic the writer reads (by name when it reads several); when the
// topic's fields are unknown it is the topic's source table, matched by name
// through both connectors' transforms and inferred (TC3). A sink connector's
// transforms are applied; a consumer group is an application Piperoom cannot
// read, so its maps are inferred.
func sinkHop(edges []Edge, writer string, ref owners.SinkRef, info *collected, fp flowPlan, lr *columnReads, feeds map[string]topicFeed) []ColumnMap {
	down, ok := lr.sinkCols[ref.Key]
	if !ok {
		return nil
	}
	var steps []smtStep
	modeled, native := false, false
	switch {
	case strings.HasPrefix(writer, "connect:"):
		if snap, ok := info.connect[strings.TrimPrefix(writer, "connect:")]; ok && snap.err == nil {
			steps, modeled = parseChain(snap.config)
			native = strings.TrimSpace(snap.config["connector.class"]) == debeziumJDBCSinkClass
		}
	case strings.HasPrefix(writer, "group:"):
	default:
		return nil // a ClickHouse view or a model: not a CDC writer hop
	}
	topic, byName := writerTopic(edges, writer, ref.Table, fp)
	if topic == "" {
		return nil
	}
	inferred := !modeled || byName
	var input string
	var up []flowField
	if fields, ok := lr.topicFields[topic]; ok {
		input = "topic:" + topic
		up = make([]flowField, len(fields))
		for i, f := range fields {
			up[i] = flowField{from: f, name: f}
		}
	} else {
		f, ok := feeds[topic]
		cols, okCols := lr.srcCols[f.table]
		if !ok || !okCols {
			return nil
		}
		input, inferred = "pg:"+f.table, true
		up, _ = sourceFlow(cols, info.connect[f.connector].config)
	}
	return matchColumns(input, applyChain(up, steps), down, inferred, native)
}

// writerTopic is the real topic a writer reads into sink table: its only
// input topic, or among several the one whose last name part is the table's
// (byName, so the maps are inferred). "" when neither picks one.
func writerTopic(edges []Edge, writer, table string, fp flowPlan) (topic string, byName bool) {
	var topics []string
	for _, e := range edges {
		t, ok := strings.CutPrefix(e.From, "topic:")
		if ok && e.To == writer && !e.IsDependency() && !fp.placeholder[t] && !slices.Contains(topics, t) {
			topics = append(topics, t)
		}
	}
	if len(topics) == 1 {
		return topics[0], false
	}
	tail := table[strings.LastIndexByte(table, '.')+1:]
	for _, t := range topics {
		if strings.EqualFold(t[strings.LastIndexByte(t, '.')+1:], tail) {
			if topic != "" {
				return "", false
			}
			topic = t
		}
	}
	return topic, topic != ""
}
