package topology

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// ---- fakes: catalogs that check scope like the real clients, a registry with schemas ----

// colCatalog answers Columns from cols, refusing like pgconv.Reconcile: no
// pipeline in ctx or a table off its side's list is refused (and recorded).
type colCatalog struct {
	mu      sync.Mutex
	sink    bool
	cols    map[string][]string
	err     error
	block   bool // wait for ctx to end
	asked   []string
	refused []string
}

func (c *colCatalog) Columns(ctx context.Context, table string) ([]clients.ColumnInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	sc, err := tools.ScopeFor(ctx, nil)
	if err == nil {
		if c.sink {
			err = sc.CheckSinkTable(table)
		} else {
			err = sc.CheckSourceTable(table)
		}
	}
	if err != nil {
		c.refused = append(c.refused, table)
		return nil, err
	}
	c.asked = append(c.asked, table)
	if c.block {
		c.mu.Unlock()
		<-ctx.Done()
		c.mu.Lock()
		return nil, ctx.Err()
	}
	if c.err != nil {
		return nil, c.err
	}
	names, ok := c.cols[table]
	if !ok {
		return nil, fmt.Errorf("fake: %s: %w", table, clients.ErrNotFound)
	}
	out := make([]clients.ColumnInfo, len(names))
	for i, n := range names {
		out[i] = clients.ColumnInfo{Name: n, Type: "text", Position: i + 1, Nullable: true}
	}
	return out, nil
}

type colSource struct {
	*fakeSource
	*colCatalog
}

type colWarehouse struct {
	*fakeWarehouse
	*colCatalog
}

// schemaSR is fakeSR with value schemas.
type schemaSR struct {
	*fakeSR
	schemas   map[string]string
	schemaErr error
}

func (s *schemaSR) Schema(_ context.Context, subject string, _ int) (clients.SchemaVersion, error) {
	if s.schemaErr != nil {
		return clients.SchemaVersion{}, s.schemaErr
	}
	v, ok := s.schemas[subject]
	if !ok {
		return clients.SchemaVersion{}, clients.ErrNotFound
	}
	return clients.SchemaVersion{Subject: subject, Version: 1, ID: 1, Schema: v}, nil
}

// envelopeSchema is a Debezium Avro envelope for a table with cols: "after"
// names the record "before" defines, as Debezium writes it.
func envelopeSchema(cols ...string) string {
	return `{"type":"record","name":"Envelope","namespace":"pg.public.orders","fields":[` +
		`{"name":"before","type":["null",` + valueRecord("Value", cols...) + `],"default":null},` +
		`{"name":"after","type":["null","Value"],"default":null},` +
		`{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.postgresql","fields":[{"name":"version","type":"string"},{"name":"db","type":"string"}]}},` +
		`{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null}]}`
}

func valueRecord(name string, cols ...string) string {
	fs := make([]string, len(cols))
	for i, c := range cols {
		fs[i] = fmt.Sprintf(`{"name":%q,"type":["null","string"],"default":null}`, c)
	}
	return `{"type":"record","name":"` + name + `","fields":[` + strings.Join(fs, ",") + `]}`
}

var (
	fiveCols = []string{"id", "customer_id", "amount", "discount", "updated_at"}
	fourCols = []string{"id", "customer_id", "amount", "updated_at"}
)

// lineageFixture is the orders pipeline with a Debezium source connector and
// a JDBC sink connector that unwraps; the test edits it before build.
type lineageFixture struct {
	srcCfg, sinkCfg map[string]string
	src, sink       *colCatalog
	sr              *schemaSR // nil: no registry
	log             *bytes.Buffer
	budget          time.Duration
}

func newLineageFixture(srcCols, sinkCols []string) *lineageFixture {
	return &lineageFixture{
		srcCfg: map[string]string{
			"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
			"topic.prefix":    "pg", "table.include.list": "public.orders", "slot.name": "debezium_orders",
			"value.converter": "io.confluent.connect.avro.AvroConverter",
		},
		sinkCfg: map[string]string{
			"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
			"topics":          "pg.public.orders",
			"transforms":      "unwrap", "transforms.unwrap.type": unwrapClass,
		},
		src:  &colCatalog{cols: map[string][]string{"public.orders": srcCols}},
		sink: &colCatalog{sink: true, cols: map[string][]string{"analytics.orders": sinkCols}},
		sr: &schemaSR{
			fakeSR:  &fakeSR{subjects: []string{"pg.public.orders-value"}, versions: map[string][]int{"pg.public.orders-value": {1}}},
			schemas: map[string]string{"pg.public.orders-value": envelopeSchema(srcCols...)},
		},
		log: &bytes.Buffer{},
	}
}

func lineageOwners() *owners.Owners {
	o := testOwners()
	o.Pipelines[0].TopicPrefix = "pg"
	o.Pipelines[0].Connectors = []string{"pg-orders-connector", "orders-sink"}
	o.Pipelines[0].ConsumerGroups = nil
	return o
}

func (f *lineageFixture) builder() *builder {
	c := &fakeConnect{
		config: map[string]map[string]string{"pg-orders-connector": f.srcCfg, "orders-sink": f.sinkCfg},
		status: map[string]clients.ConnectorStatus{
			"pg-orders-connector": {State: "RUNNING", Tasks: []clients.TaskStatus{{State: "RUNNING"}}},
			"orders-sink":         {State: "RUNNING", Tasks: []clients.TaskStatus{{State: "RUNNING"}}},
		},
	}
	k := &fakeKafka{offsets: map[string][]clients.PartitionOffsets{
		"pg.public.orders": {{Partition: 0, Start: 0, End: 10, LastTimestamp: srNow.Add(-time.Minute)}},
	}}
	b := newBuilder(lineageOwners(), c, k,
		colSource{&fakeSource{slots: []clients.ReplicationSlot{{Name: "debezium_orders", Active: true}}}, f.src},
		colWarehouse{&fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": srNow.Add(-time.Minute)}}, f.sink},
		slog.New(slog.NewTextHandler(f.log, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if f.sr != nil {
		b.sr = f.sr
	}
	if f.budget > 0 {
		b.t.HealthBudget = f.budget
	}
	b.now = func() time.Time { return srNow }
	return b
}

func (f *lineageFixture) build(t testing.TB) Graph {
	t.Helper()
	g, err := f.builder().Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return g
}

func edgeOf(t testing.TB, g Graph, from, to string) Edge {
	t.Helper()
	for _, e := range g.Edges {
		if e.From == from && e.To == to && !e.IsDependency() {
			return e
		}
	}
	t.Fatalf("no edge %s -> %s in %+v", from, to, g.Edges)
	return Edge{}
}

const (
	srcHop  = "connect:pg-orders-connector"
	topicID = "topic:pg.public.orders"
	sinkW   = "connect:orders-sink"
	sinkID  = "wh:analytics.orders"
	srcID   = "pg:public.orders"
)

func cm(input, from, to string, tr ColumnTransform, m EdgeMapping) ColumnMap {
	return ColumnMap{Input: input, From: from, To: to, Transform: tr, Mapping: m}
}

func wantColumns(t *testing.T, e Edge, want []ColumnMap) {
	t.Helper()
	if !reflect.DeepEqual(e.Columns, want) {
		got, _ := json.MarshalIndent(e.Columns, "", " ")
		exp, _ := json.MarshalIndent(want, "", " ")
		t.Fatalf("columns of %s -> %s:\n got %s\nwant %s", e.From, e.To, got, exp)
	}
}

// TC1, E1: 5 source columns, an envelope with 5 fields, a sink with 4: the
// topic hop maps all 5 by name into the envelope; the sink hop unwraps 4 and
// lists discount as dropped ("orders.discount not in sink").
func TestColumnsTC1DroppedColumn(t *testing.T) {
	f := newLineageFixture(fiveCols, fourCols)
	g := f.build(t)

	var topicWant []ColumnMap
	for _, c := range fiveCols {
		topicWant = append(topicWant, cm(srcID, c, "after."+c, ColumnSame, ""))
	}
	wantColumns(t, edgeOf(t, g, srcHop, topicID), topicWant)

	var sinkWant []ColumnMap
	for _, c := range fourCols {
		sinkWant = append(sinkWant, cm(topicID, "after."+c, c, ColumnUnwrap, ""))
	}
	sinkWant = append(sinkWant, cm(topicID, "after.discount", "", ColumnDropped, ""))
	wantColumns(t, edgeOf(t, g, sinkW, sinkID), sinkWant)

	// Other edges carry no columns: into a connector, the DLQ, a subject.
	for _, e := range g.Edges {
		if len(e.Columns) > 0 && !(e.From == srcHop && e.To == topicID) && !(e.From == sinkW && e.To == sinkID) {
			t.Fatalf("unexpected columns on %s -> %s", e.From, e.To)
		}
	}
	// E3: only the pipeline's own tables were asked, each once, in scope.
	if !slices.Equal(f.src.asked, []string{"public.orders"}) || !slices.Equal(f.sink.asked, []string{"analytics.orders"}) ||
		len(f.src.refused)+len(f.sink.refused) > 0 {
		t.Fatalf("catalog calls: src %v/%v sink %v/%v", f.src.asked, f.src.refused, f.sink.asked, f.sink.refused)
	}
}

// TC2: ReplaceField renames amount:total, after the unwrap on the sink and on
// the source connector, is a rename map.
func TestColumnsTC2Rename(t *testing.T) {
	sinkCols := []string{"id", "customer_id", "total", "discount", "updated_at"}
	f := newLineageFixture(fiveCols, sinkCols)
	f.sinkCfg["transforms"] = "unwrap,rename"
	f.sinkCfg["transforms.rename.type"] = replaceFieldValueClass
	f.sinkCfg["transforms.rename.renames"] = "amount:total"
	e := edgeOf(t, f.build(t), sinkW, sinkID)
	if got := e.Columns[2]; got != cm(topicID, "after.amount", "total", ColumnRename, "") {
		t.Fatalf("amount map = %+v", got)
	}
	if len(e.Columns) != 5 {
		t.Fatalf("columns = %+v", e.Columns)
	}

	// On the source connector: the topic is flat, amount arrives as total.
	f = newLineageFixture(fiveCols, sinkCols)
	f.srcCfg["transforms"] = "unwrap,rename"
	f.srcCfg["transforms.unwrap.type"] = unwrapClass
	f.srcCfg["transforms.rename.type"] = replaceFieldValueClass
	f.srcCfg["transforms.rename.renames"] = "amount:total"
	f.sr.schemas["pg.public.orders-value"] = valueRecord("Value", "id", "customer_id", "total", "discount", "updated_at", "__op")
	g := f.build(t)
	wantColumns(t, edgeOf(t, g, srcHop, topicID), []ColumnMap{
		cm(srcID, "id", "id", "", ""), cm(srcID, "customer_id", "customer_id", "", ""),
		cm(srcID, "amount", "total", ColumnRename, ""), cm(srcID, "discount", "discount", "", ""),
		cm(srcID, "updated_at", "updated_at", "", ""), {To: "__op", Transform: ColumnAdded},
	})
}

// TC3: with no registry the topic hop has no columns, and the sink hop maps
// the source table to the sink by name through both connectors, inferred.
func TestColumnsTC3NoRegistry(t *testing.T) {
	f := newLineageFixture(fiveCols, fourCols)
	f.sr = nil
	g := f.build(t)
	if e := edgeOf(t, g, srcHop, topicID); e.Columns != nil {
		t.Fatalf("topic hop without a registry has columns: %+v", e.Columns)
	}
	var want []ColumnMap
	for _, c := range fourCols {
		want = append(want, cm(srcID, c, c, ColumnSame, MappingInferred))
	}
	want = append(want, cm(srcID, "discount", "", ColumnDropped, MappingInferred))
	wantColumns(t, edgeOf(t, g, sinkW, sinkID), want)
}

// TC4: a registry error (listing, schema read, or a schema that is not Avro)
// gives the topic hop no columns and never fails the build.
func TestColumnsTC4RegistryError(t *testing.T) {
	for name, edit := range map[string]func(*lineageFixture){
		"listing": func(f *lineageFixture) { f.sr.subjectsErr = errors.New("registry down") },
		"schema":  func(f *lineageFixture) { f.sr.schemaErr = errors.New("HTTP 500") },
		"protobuf": func(f *lineageFixture) {
			f.sr.schemas["pg.public.orders-value"] = `syntax = "proto3"; message Value { int64 id = 1; }`
		},
		"not listed": func(f *lineageFixture) { f.sr.subjects = nil },
	} {
		t.Run(name, func(t *testing.T) {
			f := newLineageFixture(fiveCols, fourCols)
			edit(f)
			g := f.build(t)
			if e := edgeOf(t, g, srcHop, topicID); e.Columns != nil {
				t.Fatalf("topic hop has columns: %+v", e.Columns)
			}
			e := edgeOf(t, g, sinkW, sinkID)
			if len(e.Columns) != 5 || e.Columns[0].Input != srcID || e.Columns[0].Mapping != MappingInferred {
				t.Fatalf("sink hop = %+v", e.Columns)
			}
		})
	}
}

// The transforms table of docs/design/lineage.md section 2.
func TestColumnsTransforms(t *testing.T) {
	tests := map[string]struct {
		sinkCfg map[string]string
		sink    []string
		want    []ColumnMap
	}{
		"exclude": {
			sinkCfg: map[string]string{"transforms": "unwrap,drop", "transforms.drop.type": replaceFieldValueClass, "transforms.drop.exclude": "discount"},
			sink:    fiveCols,
			want: []ColumnMap{
				cm(topicID, "after.id", "id", ColumnUnwrap, ""), cm(topicID, "after.customer_id", "customer_id", ColumnUnwrap, ""),
				cm(topicID, "after.amount", "amount", ColumnUnwrap, ""), {To: "discount", Transform: ColumnAdded},
				cm(topicID, "after.updated_at", "updated_at", ColumnUnwrap, ""), cm(topicID, "after.discount", "", ColumnDropped, ""),
			},
		},
		"whitelist": {
			sinkCfg: map[string]string{"transforms": "unwrap,keep", "transforms.keep.type": replaceFieldValueClass, "transforms.keep.whitelist": "id,amount"},
			sink:    []string{"id", "amount"},
			want: []ColumnMap{
				cm(topicID, "after.id", "id", ColumnUnwrap, ""), cm(topicID, "after.amount", "amount", ColumnUnwrap, ""),
				cm(topicID, "after.customer_id", "", ColumnDropped, ""), cm(topicID, "after.discount", "", ColumnDropped, ""),
				cm(topicID, "after.updated_at", "", ColumnDropped, ""),
			},
		},
		"add.fields and unknown smt": {
			sinkCfg: map[string]string{"transforms": "unwrap,mask", "transforms.unwrap.add.fields": "op",
				"transforms.mask.type": "org.apache.kafka.connect.transforms.MaskField$Value"},
			sink: []string{"id", "__op"},
			want: []ColumnMap{
				cm(topicID, "after.id", "id", ColumnUnwrap, MappingInferred), {To: "__op", Transform: ColumnAdded, Mapping: MappingInferred},
				cm(topicID, "after.customer_id", "", ColumnDropped, MappingInferred), cm(topicID, "after.amount", "", ColumnDropped, MappingInferred),
				cm(topicID, "after.discount", "", ColumnDropped, MappingInferred), cm(topicID, "after.updated_at", "", ColumnDropped, MappingInferred),
			},
		},
		"predicate": {
			sinkCfg: map[string]string{"transforms": "unwrap", "transforms.unwrap.predicate": "isOrders"},
			sink:    []string{"id"},
			want: []ColumnMap{
				cm(topicID, "after.id", "id", ColumnUnwrap, MappingInferred), cm(topicID, "after.customer_id", "", ColumnDropped, MappingInferred),
				cm(topicID, "after.amount", "", ColumnDropped, MappingInferred), cm(topicID, "after.discount", "", ColumnDropped, MappingInferred),
				cm(topicID, "after.updated_at", "", ColumnDropped, MappingInferred),
			},
		},
		"debezium jdbc sink unwraps itself; folded names": {
			sinkCfg: map[string]string{"connector.class": debeziumJDBCSinkClass, "transforms": ""},
			sink:    []string{"ID", "CUSTOMER_ID", "AMOUNT", "DISCOUNT", "UPDATED_AT"},
			want: []ColumnMap{
				cm(topicID, "after.id", "ID", ColumnUnwrap, ""), cm(topicID, "after.customer_id", "CUSTOMER_ID", ColumnUnwrap, ""),
				cm(topicID, "after.amount", "AMOUNT", ColumnUnwrap, ""), cm(topicID, "after.discount", "DISCOUNT", ColumnUnwrap, ""),
				cm(topicID, "after.updated_at", "UPDATED_AT", ColumnUnwrap, ""),
			},
		},
		"no unwrap: name match without the envelope is a guess": {
			sinkCfg: map[string]string{"transforms": ""},
			sink:    []string{"id"},
			want: []ColumnMap{
				cm(topicID, "after.id", "id", ColumnUnwrap, MappingInferred), cm(topicID, "after.customer_id", "", ColumnDropped, ""),
				cm(topicID, "after.amount", "", ColumnDropped, ""), cm(topicID, "after.discount", "", ColumnDropped, ""),
				cm(topicID, "after.updated_at", "", ColumnDropped, ""),
			},
		},
		"bad renames list": {
			sinkCfg: map[string]string{"transforms": "unwrap,r", "transforms.r.type": replaceFieldValueClass, "transforms.r.renames": "amount"},
			sink:    []string{"id"},
			want: []ColumnMap{
				cm(topicID, "after.id", "id", ColumnUnwrap, MappingInferred), cm(topicID, "after.customer_id", "", ColumnDropped, MappingInferred),
				cm(topicID, "after.amount", "", ColumnDropped, MappingInferred), cm(topicID, "after.discount", "", ColumnDropped, MappingInferred),
				cm(topicID, "after.updated_at", "", ColumnDropped, MappingInferred),
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			f := newLineageFixture(fiveCols, tc.sink)
			for k, v := range tc.sinkCfg {
				f.sinkCfg[k] = v
			}
			wantColumns(t, edgeOf(t, f.build(t), sinkW, sinkID), tc.want)
		})
	}
}

// A consumer group is an application: its sink maps are inferred.
func TestColumnsGroupWriterInferred(t *testing.T) {
	f := newLineageFixture(fiveCols, fourCols)
	o := lineageOwners()
	o.Pipelines[0].Connectors = []string{"pg-orders-connector"}
	o.Pipelines[0].ConsumerGroups = []string{"order-sync"}
	b := f.builder()
	b.o = o
	b.kafka = &fakeKafka{lags: map[string]clients.GroupLag{"order-sync": lagOn("pg.public.orders")}}
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	e := edgeOf(t, g, "group:order-sync", sinkID)
	if len(e.Columns) != 5 || e.Columns[0] != cm(topicID, "after.id", "id", ColumnUnwrap, MappingInferred) ||
		e.Columns[4] != cm(topicID, "after.discount", "", ColumnDropped, MappingInferred) {
		t.Fatalf("group hop = %+v", e.Columns)
	}
}

// E4: a 1600-column table keeps the first 1000 maps and counts the rest, and
// its render line stays short.
func TestColumnsCap(t *testing.T) {
	var cols []string
	for i := range clients.MaxTableColumns {
		cols = append(cols, fmt.Sprintf("c%04d", i))
	}
	f := newLineageFixture(cols, cols[:10])
	b := f.builder()
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	e := edgeOf(t, g, srcHop, topicID)
	if len(e.Columns) != MaxEdgeColumns || e.ColumnsOmitted != clients.MaxTableColumns-MaxEdgeColumns {
		t.Fatalf("topic hop: %d maps, %d omitted", len(e.Columns), e.ColumnsOmitted)
	}
	s := edgeOf(t, g, sinkW, sinkID)
	if len(s.Columns) != MaxEdgeColumns || s.ColumnsOmitted != clients.MaxTableColumns-MaxEdgeColumns ||
		s.Columns[10] != cm(topicID, "after.c0010", "", ColumnDropped, "") {
		t.Fatalf("sink hop: %d maps, %d omitted, [10] %+v", len(s.Columns), s.ColumnsOmitted, s.Columns[10])
	}
	out := b.Render(g)
	if len(out) > maxRenderBytes || !strings.Contains(out, "+970 more; 600 more columns not listed") {
		t.Fatalf("render (%d bytes):\n%s", len(out), out)
	}
}

// E5: a catalog that cannot answer turns its side off with one INFO log; an
// error leaves the hop without columns; a reader slower than the budget
// cannot hold the build past it. None fails the build.
func TestColumnsReaderFailures(t *testing.T) {
	f := newLineageFixture(fiveCols, fourCols)
	f.sink.err = fmt.Errorf("snowflake: %w", clients.ErrNotSupported)
	b := f.builder()
	for range 2 {
		g, err := b.Build(context.Background(), "orders")
		if err != nil {
			t.Fatal(err)
		}
		if e := edgeOf(t, g, sinkW, sinkID); e.Columns != nil {
			t.Fatalf("unsupported sink has columns: %+v", e.Columns)
		}
		if e := edgeOf(t, g, srcHop, topicID); len(e.Columns) != 5 {
			t.Fatalf("the source side must still map: %+v", e.Columns)
		}
	}
	if n := strings.Count(f.log.String(), "cannot read column catalogs"); n != 1 || !strings.Contains(f.log.String(), "level=INFO") {
		t.Fatalf("want one INFO log, got %d:\n%s", n, f.log)
	}

	f = newLineageFixture(fiveCols, fourCols)
	f.src.err = errors.New("connection refused")
	g := f.build(t)
	if e := edgeOf(t, g, srcHop, topicID); e.Columns != nil {
		t.Fatalf("source error: topic hop has columns %+v", e.Columns)
	}
	if e := edgeOf(t, g, sinkW, sinkID); len(e.Columns) != 5 || e.Columns[0].Input != topicID {
		t.Fatalf("source error: the sink hop still maps from the topic: %+v", e.Columns)
	}

	f = newLineageFixture(fiveCols, fourCols)
	f.src.block, f.sink.block = true, true
	f.budget = 50 * time.Millisecond
	start := time.Now()
	g = f.build(t)
	if d := time.Since(start); d > time.Second {
		t.Fatalf("build took %v past a 50 ms budget", d)
	}
	for _, e := range g.Edges {
		if e.Columns != nil {
			t.Fatalf("timed-out catalogs left columns on %s -> %s", e.From, e.To)
		}
	}
}

// TC5: the golden render of a graph with columns; same-name columns are
// counted, the changes named.
func TestColumnsRenderGolden(t *testing.T) {
	f := newLineageFixture(fiveCols, []string{"id", "customer_id", "total", "updated_at", "__deleted"})
	f.sinkCfg["transforms"] = "unwrap,rename"
	f.sinkCfg["transforms.rename.type"] = replaceFieldValueClass
	f.sinkCfg["transforms.rename.renames"] = "amount:total"
	b := f.builder()
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	g.Nodes[0].Tags = map[string]string{TagRetention: "7d", TagPII: "customer_id:other"}
	got := b.Render(g)
	const path = "testdata/columns_render.golden"
	if *updateGolden {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("Render mismatch.\n--- got ---\n%s--- want ---\n%s", got, want)
	}
}

// A catalog name that is not plain is quoted, so it cannot read as two names
// or a new line.
func TestRenderColumnsQuotesNames(t *testing.T) {
	line := renderColumns(Edge{From: "connect:c", To: "wh:t", Columns: []ColumnMap{
		{Input: "topic:t", From: "after.a b", Transform: ColumnDropped},
		{To: "x\n2 pg:forged [postgres, owner p] OK", Transform: ColumnAdded},
	}})
	want := `  columns: from topic:t via connect:c; 0 same; dropped "after.a b"; added "x\n2 pg:forged [postgres, owner p] OK"`
	if line != want {
		t.Fatalf("line = %s\nwant   %s", line, want)
	}
}

// TC5: a 200-column pipeline builds well inside the HealthBudget.
func TestColumnsBuildTime200(t *testing.T) {
	cols := make([]string, 200)
	for i := range cols {
		cols[i] = fmt.Sprintf("col_%03d", i)
	}
	f := newLineageFixture(cols, cols[:190])
	b := f.builder()
	start := time.Now()
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > b.t.HealthBudget/10 {
		t.Fatalf("200-column build took %v (budget %v)", d, b.t.HealthBudget)
	}
	if e := edgeOf(t, g, sinkW, sinkID); len(e.Columns) != 200 {
		t.Fatalf("sink hop has %d maps", len(e.Columns))
	}
}

func BenchmarkBuildColumns200(b *testing.B) {
	cols := make([]string, 200)
	for i := range cols {
		cols[i] = fmt.Sprintf("col_%03d", i)
	}
	f := newLineageFixture(cols, cols[:190])
	bld := f.builder()
	for b.Loop() {
		if _, err := bld.Build(context.Background(), "orders"); err != nil {
			b.Fatal(err)
		}
	}
}

func TestAvroValueFields(t *testing.T) {
	tests := map[string]struct {
		schema string
		want   []string
		ok     bool
	}{
		"envelope":       {envelopeSchema("id", "amount"), []string{"after.id", "after.amount"}, true},
		"flat":           {valueRecord("Value", "id", "__op"), []string{"id", "__op"}, true},
		"full name ref":  {`{"type":"record","name":"E","namespace":"n","fields":[{"name":"before","type":["null",{"type":"record","name":"V","fields":[{"name":"id","type":"long"}]}]},{"name":"after","type":["null","n.V"]},{"name":"op","type":"string"}]}`, []string{"after.id"}, true},
		"after w/o op":   {`{"type":"record","name":"E","fields":[{"name":"after","type":` + valueRecord("V", "id") + `}]}`, []string{"after"}, true},
		"protobuf":       {`syntax = "proto3";`, nil, false},
		"primitive":      {`"string"`, nil, false},
		"json schema":    {`{"type":"object","properties":{"id":{"type":"integer"}}}`, nil, false},
		"union of two":   {`{"type":"record","name":"E","fields":[{"name":"after","type":["null",` + valueRecord("A", "x") + `,` + valueRecord("B", "y") + `]},{"name":"op","type":"string"}]}`, []string{"after", "op"}, true},
		"nameless field": {`{"type":"record","name":"E","fields":[{"type":"long"},{"name":"id","type":"long"}]}`, []string{"id"}, true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, ok := avroValueFields(tc.schema)
			if ok != tc.ok || !slices.Equal(got, tc.want) {
				t.Fatalf("avroValueFields = %v, %v; want %v, %v", got, ok, tc.want, tc.ok)
			}
		})
	}
	// A deeply nested schema is walked to a bound, not to the end.
	deep := strings.Repeat(`{"type":"array","items":`, 10000) + `"long"` + strings.Repeat(`}`, 10000)
	if _, ok := avroValueFields(deep); ok {
		t.Fatal("a nested array is not a record")
	}
}

// E6: column lineage carries names and transforms only. A fake row value in
// the source cannot reach the graph or its render, since nothing reads rows.
func TestColumnsNamesOnly(t *testing.T) {
	f := newLineageFixture(fiveCols, fourCols)
	b := f.builder()
	b.src.(colSource).rows = map[string]map[string]any{"public.orders|7": {"discount": "SECRET-VALUE"}}
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(g)
	if bytes.Contains(raw, []byte("SECRET-VALUE")) || strings.Contains(b.Render(g), "SECRET-VALUE") {
		t.Fatal("a row value reached the graph")
	}
}
