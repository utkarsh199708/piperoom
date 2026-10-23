//go:build xwavegen

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/dbtools"
	"piperoom/internal/tools/kafkatools"
	"piperoom/internal/topology"
	"piperoom/internal/watch"
)

// ---- scenarios ----

type scen struct {
	file  string
	build func() *xstate
	st    *xstate
	// narrow: a second reconcile answer (a sequence) over this window.
	narrow []time.Time
	calls  []call
	// watcher window on the fixed clock
	watchFrom, watchUntil time.Time
}

func fx(h, m, s, ms int) time.Time {
	return time.Date(2026, 10, 21, h, m, s, ms*int(time.Millisecond), time.UTC)
}

func baseState(rng *rand.Rand, until time.Time) *xstate {
	s := newState()
	s.history(1300, time.Date(2026, 10, 20, 7, 12, 5, 0, time.UTC), until, rng)
	return s
}

func noop(*orow) {}

func filteredState() *xstate {
	rng := rand.New(rand.NewPCG(14, 1))
	s := baseState(rng, fx(7, 29, 58, 0))
	deploy := fx(8, 29, 40, 0)
	backfillAt := fx(8, 31, 12, 412)
	filtering := func(r orow, _ string) (orow, bool) { return r, r.status != "backfill" }
	doneBackfill := false
	for t := fx(7, 30, 0, 0); t.Before(fx(9, 40, 0, 0)); t = t.Add(time.Second) {
		at := t.Add(time.Duration(rng.IntN(200)) * time.Millisecond)
		sink := goodSink
		if !at.Before(deploy) {
			sink = filtering
		}
		if !doneBackfill && !at.Before(backfillAt) {
			for g := int64(1); g <= 40; g++ {
				s.insert(backfillAt, g, 900+g, "backfill", sink)
			}
			s.insert(fx(8, 31, 13, 87), 77, 4200, "placed", sink) // marker
			doneBackfill = true
		}
		if rng.IntN(3) == 0 {
			var placed []int64
			for id, r := range s.src {
				if r.status == "placed" {
					placed = append(placed, id)
				}
			}
			if len(placed) > 0 {
				slices.Sort(placed)
				id := placed[rng.IntN(len(placed))]
				s.update(at, id, func(r *orow) { r.status = "shipped" }, sink)
				continue
			}
		}
		s.insert(at, int64(rng.IntN(1000)), int64(1+rng.IntN(5000)), "placed", sink)
	}
	return s
}

func dupState() *xstate {
	rng := rand.New(rand.NewPCG(16, 1))
	s := baseState(rng, fx(8, 47, 13, 402))
	at := fx(9, 31, 40, 233)
	for g := int64(1); g <= 10; g++ {
		s.insert(at, g, 600+g, "placed", goodSink)
	}
	s.insert(fx(9, 31, 41, 15), 77, 4200, "placed", goodSink) // marker
	// newest event of each key in the tail, up to 10, in original order
	var picked []tevent
	seen := map[int64]bool{}
	for i := len(s.events) - 1; i >= 0 && len(picked) < 10; i-- {
		e := s.events[i]
		if seen[e.id] {
			continue
		}
		seen[e.id] = true
		picked = append(picked, e)
	}
	slices.Reverse(picked)
	for i, e := range picked {
		s.republish(e, fx(9, 31, 43, 510+i))
	}
	return s
}

func droppedState() *xstate {
	rng := rand.New(rand.NewPCG(15, 1))
	s := baseState(rng, fx(8, 47, 13, 402))
	pinned := s.lastHistory.Truncate(time.Microsecond)
	nullTS := func(r orow, _ string) (orow, bool) { r.updNull = true; return r, true }
	at := fx(9, 22, 5, 614)
	for g := int64(1); g <= 20; g++ {
		id := s.insert(at, g, 800+g, "placed", nullTS)
		s.src[id].upd = pinned
	}
	return s
}

func toastState() *xstate {
	rng := rand.New(rand.NewPCG(17, 1))
	s := baseState(rng, fx(8, 47, 13, 402))
	s.ddl = []clients.DDLChange{{At: fx(9, 24, 10, 318), CommandTag: "ALTER TABLE", ObjectIdentity: "public.orders",
		Query: "ALTER TABLE public.orders REPLICA IDENTITY DEFAULT", Username: "piperoom"}}
	s.replica = "DEFAULT"
	s.toastB = 24576
	var ids []int64
	at := fx(9, 24, 10, 902)
	for g := int64(1); g <= 3; g++ {
		ids = append(ids, s.insert(at, g, 300+g, "toast-big-"+strings.Repeat("9f", 6), goodSink))
	}
	touch := fx(9, 24, 13, 207)
	ph := func(r orow, op string) (orow, bool) {
		if op == "u" {
			r.status = placeholder
		}
		return r, true
	}
	for _, id := range ids {
		s.update(touch, id, func(r *orow) { r.amount++ }, ph)
	}
	return s
}

func commonCalls(sampleKey string) []call {
	return []call{
		{tool: "pipeline_map", input: map[string]any{"pipeline_id": "orders"}},
		{tool: "connect_list_connectors"},
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		{tool: "connect_config", input: map[string]any{"connector": "pg-orders-connector"}},
		{tool: "schema_registry_subjects"},
		{tool: "schema_registry_versions", input: map[string]any{"subject": "pg.public.orders-key"}},
		{tool: "schema_registry_versions", input: map[string]any{"subject": "pg.public.orders-value"}},
		{tool: "kafka_topic_offsets", input: map[string]any{"topic": ordersTopic}},
		{tool: "kafka_topic_config", input: map[string]any{"pipeline_id": "orders", "table": "public.orders"}},
		{tool: "kafka_list_groups"},
		{tool: "kafka_consumer_lag", input: map[string]any{"group": "order-sync"}},
		{tool: "pg_replication_slots"},
		{tool: "pg_recent_ddl", input: map[string]any{"since": ago(24 * time.Hour)}, free: []string{"since"}},
		{tool: "pg_long_transactions"},
		{tool: "pg_publication_tables", input: map[string]any{"pipeline_id": "orders"}},
		{tool: "warehouse_freshness", input: map[string]any{"table": "analytics.orders", "ts_column": "updated_at"}},
		{tool: "reconcile", input: map[string]any{"table": "public.orders", "since": fx(7, 40, 0, 0).Format(time.RFC3339), "until": fx(9, 38, 0, 0).Format(time.RFC3339)}, free: []string{"since", "until"}},
		{tool: "kafka_dup_keys", input: map[string]any{"topic": ordersTopic, "since": ago(2 * time.Hour)}, free: []string{"since"}},
		{tool: "pg_toast_risk", input: map[string]any{"pipeline_id": "orders"}},
		{tool: "pg_row", input: map[string]any{"pipeline_id": "orders", "table": "public.orders", "pk_column": "id", "key": sampleKey}},
		{tool: "warehouse_row", input: map[string]any{"pipeline_id": "orders", "table": "analytics.orders", "pk_column": "id", "key": sampleKey}},
		{tool: "kafka_find_by_key", input: map[string]any{"topic": ordersTopic, "key": sampleKey, "since": ago(3 * time.Hour)}, free: []string{"since"}},
		{tool: "trace_record", input: map[string]any{"pipeline_id": "orders", "table": "public.orders", "key": sampleKey}},
		// wildcards: out-of-scope names and unknown keys
		{tool: "pipeline_map", input: map[string]any{"pipeline_id": "default"}, wildcard: true},
		{tool: "connect_status", input: map[string]any{"connector": "orders-connector"}, wildcard: true},
		{tool: "connect_config", input: map[string]any{"connector": "orders-connector"}, wildcard: true},
		{tool: "schema_registry_versions", input: map[string]any{"subject": "orders-value"}, wildcard: true},
		{tool: "kafka_topic_offsets", input: map[string]any{"topic": "orders.events"}, wildcard: true},
		{tool: "kafka_consumer_lag", input: map[string]any{"group": "orders-consumer"}, wildcard: true},
		{tool: "pg_row", input: map[string]any{"pipeline_id": "orders", "table": "public.orders", "pk_column": "id", "key": "999999"}, wildcard: true},
		{tool: "warehouse_row", input: map[string]any{"pipeline_id": "orders", "table": "analytics.orders", "pk_column": "id", "key": "999999"}, wildcard: true},
		{tool: "kafka_find_by_key", input: map[string]any{"topic": ordersTopic, "key": "999999", "since": ago(3 * time.Hour)}, wildcard: true},
		{tool: "trace_record", input: map[string]any{"pipeline_id": "orders", "table": "public.orders", "key": "999999"}, wildcard: true},
	}
}

func scens() []scen {
	return []scen{
		{file: "evals/scenarios/sink-filtered-rows.yaml", build: filteredState, calls: commonCalls(sampleFiltered), narrow: []time.Time{fx(8, 30, 0, 0), fx(8, 40, 0, 0)},
			watchFrom: fx(9, 25, 0, 0), watchUntil: fx(9, 41, 0, 0)},
		{file: "evals/scenarios/consumer-duplicates.yaml", build: dupState, calls: commonCalls(sampleDup),
			watchFrom: fx(9, 40, 0, 0), watchUntil: fx(9, 41, 0, 0)},
		{file: "evals/scenarios/sink-dropped-column.yaml", build: droppedState, calls: commonCalls(sampleDropped),
			watchFrom: fx(9, 22, 30, 0), watchUntil: fx(11, 40, 0, 0)},
		{file: "evals/scenarios/toast-placeholder.yaml", build: toastState, calls: commonCalls(sampleToast),
			watchFrom: fx(9, 24, 30, 0), watchUntil: fx(11, 40, 0, 0)},
	}
}

// sample keys: set after building (ids are deterministic); filled in main.
var sampleFiltered, sampleDup, sampleDropped, sampleToast = "0", "0", "0", "0"

// ---- base world on a clock ----

func buildWorld(st *xstate, c clock) *xw {
	w := newWorld()
	cfg := map[string]string{
		"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "database.hostname": "postgres",
		"database.port": "5432", "database.user": "piperoom", "database.password": "piperoom-dev", "database.dbname": "orders",
		"topic.prefix": "pg", "plugin.name": "pgoutput", "slot.name": "debezium_orders",
		"publication.autocreate.mode": "filtered", "table.include.list": "public.orders", "tasks.max": "1",
	}
	w.addConnector("pg-orders-connector", "source", "RUNNING", "connect:8083", cfg, task(0, "RUNNING", "connect:8083", ""))
	w.subjects["pg.public.orders-key"] = []schema{{1, keySchema(ordersTopic, avroField("id", avroLong))}}
	w.subjects["pg.public.orders-value"] = []schema{{2, envelope(ordersTopic, avroField("id", avroLong), avroField("customer_id", avroLong),
		avroField("amount", avroInt), avroField("status", avroString), avroField("updated_at", avroZoned))}}
	w.slots = []clients.ReplicationSlot{slot("debezium_orders", true, 1146880, "reserved")}
	w.pubs = []postgres.PublicationTable{{Schema: "public", Table: "orders", InPublication: true, Selectable: true}}
	x := &xw{world: w, st: st, c: c}
	w.topics[ordersTopic] = xkafka{fakeKafka{w}, x}.offsets()
	return x
}

func clientsOf(x *xw) (xsrc, xwh, xkafka) {
	return xsrc{fakeSource{x.world}, x}, xwh{fakeWarehouse{x.world}, x}, xkafka{fakeKafka{x.world}, x}
}

func runTool(o *owners.Owners, x *xw, name string, input map[string]any, c clock) outObs {
	conn := fakeConnect{x.world}
	src, wh, k := clientsOf(x)
	reg := tools.NewRegistry()
	if err := kafkatools.Register(reg, kafkatools.Deps{Connect: conn, Registry: fakeRegistry{x.world}, Kafka: k, Owners: o}); err != nil {
		return outObs{Error: "gen: " + err.Error()}
	}
	b := topology.NewBuilder(o, conn, k, src, wh)
	if err := dbtools.Register(reg, dbtools.Deps{Src: src, Wh: wh, Builder: b, Connect: conn, Owners: o, Kafka: k}); err != nil {
		return outObs{Error: "gen: " + err.Error()}
	}
	t, ok := reg.Get(name)
	if !ok {
		return outObs{Error: "gen: unknown tool " + name}
	}
	raw, _ := json.Marshal(input)
	res, err := t.Run(tools.WithPipelineID(context.Background(), "orders"), raw)
	fix := func(s string) string { return shiftTimes(s, c) }
	if err != nil {
		return outObs{Error: fix(err.Error())}
	}
	ob := outObs{Summary: fix(res.Summary), Truncated: res.Truncated}
	if res.Data != nil {
		b, _ := json.Marshal(res.Data)
		ob.Data = decodeData([]byte(fix(string(b))))
	}
	return ob
}

var timestampRe = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})`)

func shiftTimes(s string, c clock) string {
	shift := fixedNow.Sub(c.now)
	return timestampRe.ReplaceAllStringFunc(s, func(m string) string {
		t, err := time.Parse(time.RFC3339Nano, m)
		if err != nil {
			return m
		}
		if d := t.Sub(c.now); d > 60*24*time.Hour || d < -60*24*time.Hour {
			return m
		}
		layout := time.RFC3339
		if strings.Contains(m, ".") {
			layout = time.RFC3339Nano
		}
		return t.Add(shift).UTC().Format(layout)
	})
}

func decodeData(b []byte) any {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return string(b)
	}
	return plain(v)
}

func plain(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			x[k] = plain(e)
		}
		return x
	case []any:
		for i, e := range x {
			x[i] = plain(e)
		}
		return x
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		f, _ := x.Float64()
		return f
	}
	return v
}

type outRec struct {
	Tool      string         `yaml:"tool"`
	Input     map[string]any `yaml:"input,omitempty,flow"`
	Summary   string         `yaml:"summary,omitempty"`
	Data      any            `yaml:"data,omitempty"`
	Truncated bool           `yaml:"truncated,omitempty"`
	Error     string         `yaml:"error,omitempty"`
	Sequence  []outObs       `yaml:"sequence,omitempty"`
}

type outObs struct {
	Summary   string `yaml:"summary,omitempty"`
	Data      any    `yaml:"data,omitempty"`
	Truncated bool   `yaml:"truncated,omitempty"`
	Error     string `yaml:"error,omitempty"`
}

func (s scen) recordings(c clock, o *owners.Owners) ([]outRec, *xw, error) {
	x := buildWorld(s.st, c)
	var out []outRec
	for _, cl := range s.calls {
		in := map[string]any{}
		for k, v := range cl.input {
			if d, ok := v.(ago); ok {
				v = c.ago(time.Duration(d)).Format(time.RFC3339)
			}
			in[k] = v
		}
		rec := map[string]any{}
		if !cl.wildcard {
			for k, v := range cl.input {
				if !slices.Contains(cl.free, k) {
					rec[k] = v
				}
			}
		}
		var ob outObs
		if cl.tool == "reconcile" {
			// Explicit until before the wall clock: the tool's time.Now only
			// bounds the window, so the world can sit on the fixed clock and
			// the HH:MM bucket labels need no shift.
			fc := clock{now: fixedNow}
			ob = runTool(o, buildWorld(s.st, fc), cl.tool, in, fc)
		} else {
			ob = runTool(o, x, cl.tool, in, c)
		}
		if ob.Summary == "" && ob.Error == "" {
			return nil, nil, fmt.Errorf("%s %v: empty summary", cl.tool, in)
		}
		r := outRec{Tool: cl.tool, Summary: ob.Summary, Data: ob.Data, Truncated: ob.Truncated, Error: ob.Error}
		if len(rec) > 0 {
			r.Input = rec
		}
		if cl.tool == "reconcile" && s.narrow != nil {
			fc := clock{now: fixedNow}
			in2 := map[string]any{"table": "public.orders", "since": s.narrow[0].Format(time.RFC3339), "until": s.narrow[1].Format(time.RFC3339)}
			ob2 := runTool(o, buildWorld(s.st, fc), cl.tool, in2, fc)
			r = outRec{Tool: r.Tool, Input: r.Input, Sequence: []outObs{ob, ob2}}
		}
		out = append(out, r)
	}
	out = append(out, outRec{Tool: "similar_incidents", Summary: "No similar closed incident found on pipeline orders (3 past rooms searched)."})
	return out, x, nil
}

func (s scen) render(o *owners.Owners) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second + 5*time.Millisecond)))
		c := clock{now: time.Now().UTC().Truncate(time.Second)}
		out, err := s.renderAt(c, o)
		if err != nil {
			return nil, err
		}
		if time.Now().UTC().Truncate(time.Second).Equal(c.now) {
			return out, nil
		}
	}
	return nil, errors.New("could not generate within one second")
}

func (s scen) renderAt(c clock, o *owners.Owners) ([]byte, error) {
	b, err := os.ReadFile(s.file)
	if err != nil {
		return nil, err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, err
	}
	root := doc.Content[0]
	recs, _, err := s.recordings(c, o)
	if err != nil {
		return nil, err
	}
	pm := recs[0]
	alert := child(root, "alert")
	if err := setKey(alert, "pipeline_map", pm.Summary); err != nil {
		return nil, err
	}
	child(alert, "pipeline_map").Style = yaml.LiteralStyle
	if err := setKey(root, "recordings", recs); err != nil {
		return nil, err
	}
	styleSummaries(child(root, "recordings"))
	var out bytes.Buffer
	enc := yaml.NewEncoder(&out)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return nil, err
	}
	_ = enc.Close()
	return spaceTopLevel(out.Bytes()), nil
}

func child(m *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

func setKey(m *yaml.Node, key string, v any) error {
	var n yaml.Node
	if err := n.Encode(v); err != nil {
		return err
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			n.HeadComment, n.LineComment = m.Content[i+1].HeadComment, m.Content[i+1].LineComment
			m.Content[i+1] = &n
			return nil
		}
	}
	return fmt.Errorf("key %s missing", key)
}

func styleSummaries(n *yaml.Node) {
	if n == nil {
		return
	}
	if n.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(n.Content); i += 2 {
			if v := n.Content[i+1]; n.Content[i].Value == "summary" && v.Kind == yaml.ScalarNode {
				v.Style = yaml.LiteralStyle
			}
		}
	}
	for _, c := range n.Content {
		if c.Kind == yaml.MappingNode || c.Kind == yaml.SequenceNode {
			styleSummaries(c)
		}
	}
}

func spaceTopLevel(b []byte) []byte {
	lines := strings.Split(string(b), "\n")
	var out []string
	for i, l := range lines {
		if i > 0 && l != "" && l[0] != ' ' && l[0] != '#' && l[0] != '-' && !strings.HasPrefix(l, "description:") {
			j := len(out)
			for j > 0 && strings.HasPrefix(out[j-1], "#") {
				j--
			}
			if j > 0 && out[j-1] != "" {
				out = append(out[:j], append([]string{""}, out[j:]...)...)
			}
		}
		out = append(out, l)
	}
	return []byte(strings.Join(out, "\n"))
}

// ---- the real watcher over the fixed-clock world ----

type sink struct {
	mu     sync.Mutex
	opened chan struct{}
	alerts []alertin.Alert
	notes  []string
}

func (s *sink) OpenAlertRoom(_ context.Context, a alertin.Alert) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.alerts = append(s.alerts, a)
	if len(s.alerts) == 1 {
		close(s.opened)
	}
	return "room-1", true, nil
}
func (s *sink) OpenRoomForPipeline(string) (string, bool) { return "", false }
func (s *sink) LastRoomForKey(string, string) (string, *time.Time, bool) {
	return "", nil, false
}
func (s *sink) PostEvidence(_ context.Context, _, text string) error {
	s.mu.Lock()
	s.notes = append(s.notes, text)
	s.mu.Unlock()
	return nil
}

func runWatcher(s scen, o *owners.Owners) {
	x := buildWorld(s.build(), clock{now: fixedNow})
	src, wh, k := clientsOf(x)
	clk := newEvalClock(s.watchFrom)
	sk := &sink{opened: make(chan struct{})}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	w, err := watch.New(watch.Deps{
		Source: src, Connect: fakeConnect{x.world}, Owners: o, Rooms: sk, Lag: k, Warehouse: wh,
		Config: config.Watch{Mode: config.WatchRooms, Interval: 15 * time.Second, CheckTimeout: 10 * time.Second},
		Clock:  clk, Log: log, Jitter: func(time.Duration) time.Duration { return 0 },
	})
	if err != nil {
		fmt.Println("watcher:", err)
		return
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	for clk.Now().Before(s.watchUntil) {
		at, err := clk.waitArmed(ctx, sk.opened, done)
		if err != nil || at.IsZero() {
			break
		}
		clk.advanceTo(at)
	}
	stop()
	<-done
	fmt.Printf("== watcher %s from %s to %s (clock now %s)\n", s.file, s.watchFrom.Format(time.TimeOnly), s.watchUntil.Format(time.TimeOnly), clk.Now().Format(time.TimeOnly))
	if len(sk.alerts) == 0 {
		fmt.Println("   no room opened")
	}
	for _, a := range sk.alerts {
		fmt.Printf("   TITLE: %s\n   SEV: %s DEDUP: %s\n   RAW: %s\n   AGENT EVIDENCE: %s\n", a.Title, a.Severity, a.DedupKey, a.Raw, agent.AlertEvidenceText(a.Raw))
	}
	for _, n := range sk.notes {
		fmt.Println("   NOTE:", n)
	}
}

func main() {
	write := flag.Bool("write", false, "fill the scenario files")
	flag.Parse()
	o, err := owners.Load("dev/owners.yaml")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(os.Args) > 1 && os.Args[len(os.Args)-1] == "probe" {
		probes(o)
		return
	}
	// sample keys
	{
		st := filteredState()
		for id, r := range st.src {
			if r.status == "backfill" && (sampleFiltered == "0" || id < mustInt(sampleFiltered)) {
				sampleFiltered = strconv.FormatInt(id, 10)
			}
		}
		st = dupState()
		for _, e := range st.events {
			if e.dup {
				sampleDup = strconv.FormatInt(e.id, 10)
				break
			}
		}
		st = droppedState()
		for id, r := range st.snk {
			if r.updNull && (sampleDropped == "0" || id < mustInt(sampleDropped)) {
				sampleDropped = strconv.FormatInt(id, 10)
			}
		}
		st = toastState()
		for id, r := range st.snk {
			if r.status == placeholder && (sampleToast == "0" || id < mustInt(sampleToast)) {
				sampleToast = strconv.FormatInt(id, 10)
			}
		}
		fmt.Println("samples:", sampleFiltered, sampleDup, sampleDropped, sampleToast)
	}
	for _, s := range scens() {
		s.calls = commonCalls(map[string]string{
			"evals/scenarios/sink-filtered-rows.yaml":  sampleFiltered,
			"evals/scenarios/consumer-duplicates.yaml": sampleDup,
			"evals/scenarios/sink-dropped-column.yaml": sampleDropped,
			"evals/scenarios/toast-placeholder.yaml":   sampleToast,
		}[s.file])
		s.st = s.build()
		runWatcher(s, o)
		if !*write {
			continue
		}
		out, err := s.render(o)
		if err != nil {
			fmt.Fprintln(os.Stderr, s.file, err)
			os.Exit(1)
		}
		if err := os.WriteFile(s.file, out, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("wrote", s.file)
	}
}

func mustInt(s string) int64 { n, _ := strconv.ParseInt(s, 10, 64); return n }
