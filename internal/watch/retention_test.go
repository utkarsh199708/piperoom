package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// ---- helpers ----

// cfgKafka is the DLQ fake plus clients.TopicConfigReader.
type cfgKafka struct {
	*fakeKafka
	mu    sync.Mutex
	cfgs  map[string]clients.TopicConfig
	err   error
	calls []string
	// noScope counts calls whose ctx carried no pipeline.
	noScope int
}

func newCfgKafka(k *fakeKafka) *cfgKafka {
	return &cfgKafka{fakeKafka: k, cfgs: map[string]clients.TopicConfig{}}
}

func (k *cfgKafka) DescribeTopicConfigs(ctx context.Context, topic string) (clients.TopicConfig, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, ok := ctx.Deadline(); !ok {
		return clients.TopicConfig{}, errors.New("no deadline")
	}
	if _, ok := tools.PipelineFrom(ctx); !ok {
		k.noScope++
	}
	k.calls = append(k.calls, topic)
	if k.err != nil {
		return clients.TopicConfig{}, k.err
	}
	c, ok := k.cfgs[topic]
	if !ok {
		return clients.TopicConfig{}, fmt.Errorf("fake: topic %s: %w", topic, clients.ErrNotFound)
	}
	return c, nil
}

func (k *cfgKafka) set(topic, ms, bytes, policy string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	// MinInsyncReplicas carries a marker that must never leave the watcher.
	k.cfgs[topic] = clients.TopicConfig{Topic: topic, RetentionMS: ms, RetentionBytes: bytes, CleanupPolicy: policy,
		MinInsyncReplicas: "SECRET-MARKER", Partitions: 3}
}

func (k *cfgKafka) setErr(err error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.err = err
}

func (k *cfgKafka) callCount() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.calls)
}

const day = 24 * time.Hour

func ms(d time.Duration) string { return fmt.Sprint(d.Milliseconds()) }

// retPipe is pipeline "orders" (CDC topic orders.public.t) with the given
// signals text and declared_retention topics.
func retPipe(signals string, topics map[string]string) pipeSpec {
	var b strings.Builder
	if signals != "" {
		b.WriteString("    watch:\n      signals:\n" + signals)
	}
	if len(topics) > 0 {
		b.WriteString("    declared_retention:\n      topics:\n")
		for t, v := range topics {
			fmt.Fprintf(&b, "        %s: %s\n", t, v)
		}
	}
	return pipeSpec{id: "orders", watch: b.String()}
}

// retHarness is a harness whose Kafka client reads topic configs.
func retHarness(t *testing.T, pipes ...pipeSpec) (*harness, *cfgKafka) {
	t.Helper()
	h := newHarness(t, opts{pipes: pipes})
	k := newCfgKafka(h.kafka)
	h.deps.Kafka = k
	w, err := New(h.deps)
	if err != nil {
		t.Fatal(err)
	}
	h.w = w
	return h, k
}

func (h *harness) retentionEv() map[string]any {
	h.w.mu.Lock()
	defer h.w.mu.Unlock()
	ss := h.w.pipes["orders"].sigs[retentionID]
	if ss == nil {
		return nil
	}
	return ss.last.ev
}

// ---- judging (TC1, TC2, TC3) ----

func TestJudgeRetention(t *testing.T) {
	topic := func(cdc bool, declared, ms, bytes, policy string) *retObj {
		return &retObj{topic: "pg.public.orders", cdc: cdc, declared: declared, ok: true,
			cfg: clients.TopicConfig{RetentionMS: ms, RetentionBytes: bytes, CleanupPolicy: policy}}
	}
	table := func(declared string, tr clients.TableRetention) *retObj {
		return &retObj{table: "analytics.orders", declared: declared, ok: true, tr: tr}
	}
	tests := []struct {
		name string
		o    *retObj
		want []events.RetentionRisk
	}{
		{"TC1 declared 7d, actual 1d", topic(true, "7d", ms(day), "-1", "delete"), []events.RetentionRisk{{
			Code: events.RiskRetentionShorter, Sev: "sev4", Topic: "pg.public.orders", Configured: "1d", Declared: "7d",
			CleanupPolicy: "delete", Text: "pg.public.orders retains 1d; declared 7d"}}},
		{"TC1 equal", topic(true, "7d", ms(7*day), "-1", "delete"), nil},
		{"TC1 equal in hours", topic(true, "168h", ms(7*day), "", "delete"), nil},
		{"TC1 no declaration", topic(true, "", ms(day), "-1", "delete"), nil},
		{"TC2 infinite vs 30d", topic(true, "30d", "-1", "-1", "delete"), []events.RetentionRisk{{
			Code: events.RiskRetentionLonger, Sev: "sev4", Topic: "pg.public.orders", Configured: "infinite", Declared: "30d",
			CleanupPolicy: "delete", Text: "pg.public.orders retains infinite; declared 30d"}}},
		{"longer by a second", topic(false, "7d", ms(7*day+time.Second), "", "delete"), []events.RetentionRisk{{
			Code: events.RiskRetentionLonger, Sev: "sev4", Topic: "pg.public.orders", Configured: "604801s", Declared: "7d",
			CleanupPolicy: "delete", Text: "pg.public.orders retains 604801s; declared 7d"}}},
		{"declared infinite, actual 7d", topic(true, "infinite", ms(7*day), "", "delete"), []events.RetentionRisk{{
			Code: events.RiskRetentionShorter, Sev: "sev4", Topic: "pg.public.orders", Configured: "7d", Declared: "infinite",
			CleanupPolicy: "delete", Text: "pg.public.orders retains 7d; declared infinite"}}},
		{"infinite both", topic(true, "infinite", "-1", "", "delete"), nil},
		{"size cap named", topic(true, "7d", ms(day), "1073741824", "delete"), []events.RetentionRisk{{
			Code: events.RiskRetentionShorter, Sev: "sev4", Topic: "pg.public.orders", Configured: "1d", Declared: "7d",
			RetentionBytes: 1 << 30, CleanupPolicy: "delete",
			Text: "pg.public.orders retains 1d; declared 7d (a retention.bytes cap is set)"}}},
		{"unreadable retention.ms is not judged", topic(true, "7d", "", "", "delete"), nil},
		{"TC3 compact CDC topic, no declaration", topic(true, "", ms(7*day), "", "compact"), []events.RetentionRisk{{
			Code: events.RiskCompactCDCTopic, Sev: "sev4", Topic: "pg.public.orders", CleanupPolicy: "compact",
			Text: "pg.public.orders has cleanup.policy=compact: deletes are compacted away and a replay cannot see them"}}},
		{"TC3 compact CDC topic with a declaration also keeps forever", topic(true, "7d", ms(7*day), "", "compact"), []events.RetentionRisk{
			{Code: events.RiskCompactCDCTopic, Sev: "sev4", Topic: "pg.public.orders", Declared: "7d", CleanupPolicy: "compact",
				Text: "pg.public.orders has cleanup.policy=compact: deletes are compacted away and a replay cannot see them"},
			{Code: events.RiskRetentionLonger, Sev: "sev4", Topic: "pg.public.orders", Configured: "infinite", Declared: "7d",
				CleanupPolicy: "compact", Text: "pg.public.orders retains infinite; declared 7d"}}},
		{"compact,delete keeps deletes for retention.ms", topic(true, "", ms(7*day), "", "compact,delete"), nil},
		{"compact DLQ topic is not a CDC topic", topic(false, "", ms(7*day), "", "compact"), nil},
		{"unknown policy text never reaches evidence", topic(true, "", "1", "", "compact, x=secret"), []events.RetentionRisk{{
			Code: events.RiskCompactCDCTopic, Sev: "sev4", Topic: "pg.public.orders", CleanupPolicy: "compact",
			Text: "pg.public.orders has cleanup.policy=compact: deletes are compacted away and a replay cannot see them"}}},
		{"table without a retention setting keeps rows forever", table("400d", clients.TableRetention{}), []events.RetentionRisk{{
			Code: events.RiskRetentionLonger, Sev: "sev4", Table: "analytics.orders", Configured: "infinite", Declared: "400d",
			Text: "analytics.orders keeps rows infinite; declared 400d"}}},
		{"table equal", table("400d", clients.TableRetention{Set: true, After: 400 * day}), nil},
		{"table shorter", table("400d", clients.TableRetention{Set: true, After: 90 * day}), []events.RetentionRisk{{
			Code: events.RiskRetentionShorter, Sev: "sev4", Table: "analytics.orders", Configured: "90d", Declared: "400d",
			Text: "analytics.orders keeps rows 90d; declared 400d"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := judgeRetention(tc.o)
			g, _ := json.Marshal(got)
			w, _ := json.Marshal(tc.want)
			if string(g) != string(w) {
				t.Fatalf("got  %s\nwant %s", g, w)
			}
		})
	}
}

func TestRetentionTextIsCapped(t *testing.T) {
	long := strings.Repeat("a", 240)
	rs := judgeRetention(&retObj{topic: long, cdc: true, ok: true, cfg: clients.TopicConfig{CleanupPolicy: "compact"}})
	if len(rs) != 1 {
		t.Fatalf("risks = %+v", rs)
	}
	if e := retentionEntry(rs[0]); len(e["text"].(string)) > maxRetentionText {
		t.Fatalf("text is %d bytes", len(e["text"].(string)))
	}
}

// ---- the watcher ----

// TC1/E1/E2/TC5: a finding fires config_risk after its `for`, at sev4, opens
// no room even with action room, is in the status row (the fleet's
// signals_firing) on its topic node, and resolves when the topic is fixed.
func TestRetentionFindingFiresWithoutARoomAndResolves(t *testing.T) {
	h, k := retHarness(t, retPipe("        config_risk: {action: room}\n", map[string]string{"orders.public.t": "7d"}))
	fs := &fakeStatus{}
	h.deps.Status = fs
	w, err := New(h.deps)
	if err != nil {
		t.Fatal(err)
	}
	h.w = w
	k.set("orders.public.t", ms(day), "-1", "delete")

	h.step(time.Minute)
	if h.phase("orders", retentionID) != phaseOK {
		t.Fatalf("fired before its for\n%s", h.logs)
	}
	h.step(time.Minute)
	if h.phase("orders", retentionID) != phaseFiring {
		t.Fatalf("did not fire after its for\n%s", h.logs)
	}
	if n := len(h.rooms.created()); n != 0 || h.rooms.callCount() != 0 {
		t.Fatalf("a retention finding opened or joined a room: %d\n%s", n, h.logs)
	}
	ev := h.retentionEv()
	risks, _ := ev["risks"].([]map[string]any)
	if ev["signal"] != owners.SignalConfigRisk || ev["severity"] != "sev4" || len(risks) != 1 ||
		risks[0]["code"] != events.RiskRetentionShorter || risks[0]["configured"] != "1d" || risks[0]["declared"] != "7d" ||
		risks[0]["text"] != "orders.public.t retains 1d; declared 7d" {
		t.Fatalf("evidence = %v", ev)
	}
	row, _ := fs.row("orders")
	if len(row.Signals) != 1 || row.Signals[0].Signal != owners.SignalConfigRisk || row.Signals[0].Severity != "sev4" ||
		row.Signals[0].State != clients.WatchFiring || row.Signals[0].Node != "topic:orders.public.t" {
		t.Fatalf("status row = %+v", row.Signals)
	}
	if k.noScope != 0 {
		t.Fatalf("%d reads without the pipeline in ctx", k.noScope)
	}

	// Fixed: read again at the next refresh, clear for `for`, resolved.
	k.set("orders.public.t", ms(7*day), "-1", "delete")
	h.step(retentionRefresh)
	if h.phase("orders", retentionID) != phaseFiring {
		t.Fatal("resolved before its for")
	}
	h.step(time.Minute)
	if h.phase("orders", retentionID) != phaseOK {
		t.Fatalf("did not resolve\n%s", h.logs)
	}
	h.step(time.Minute)
	if row, _ := fs.row("orders"); len(row.Signals) != 0 {
		t.Fatalf("status after resolve = %+v", row.Signals)
	}
	if strings.Contains(h.logs.String(), "SECRET-MARKER") {
		t.Fatal("a topic config value reached the log")
	}
}

// TC1: no declaration and no compact policy: no finding, nothing fires.
func TestRetentionNoDeclarationNoFinding(t *testing.T) {
	h, k := retHarness(t, retPipe("", nil))
	k.set("orders.public.t", ms(day), "-1", "delete")
	h.steps(5, time.Minute)
	if h.phase("orders", retentionID) != phaseOK || h.retentionEv() != nil {
		t.Fatalf("fired without a declaration\n%s", h.logs)
	}
	if k.callCount() != 1 {
		t.Fatalf("calls = %d, want 1 (CDC topics are read for the compact check, once per refresh)", k.callCount())
	}
}

// TC3: compact on a CDC topic is a finding without any declaration.
func TestRetentionCompactCDCTopic(t *testing.T) {
	h, k := retHarness(t, retPipe("", nil))
	k.set("orders.public.t", ms(7*day), "", "compact")
	h.steps(3, time.Minute)
	if h.phase("orders", retentionID) != phaseFiring {
		t.Fatalf("compact CDC topic did not fire\n%s", h.logs)
	}
	risks, _ := h.retentionEv()["risks"].([]map[string]any)
	if len(risks) != 1 || risks[0]["code"] != events.RiskCompactCDCTopic || risks[0]["cleanup_policy"] != "compact" {
		t.Fatalf("risks = %v", risks)
	}
	if _, ok := risks[0]["configured"]; ok {
		t.Fatalf("configured set on a compact finding: %v", risks[0])
	}
}

// TC4/E3: an unreadable topic config (no DescribeConfigs ACL) logs one INFO
// line, judges nothing, and leaves a firing finding firing.
func TestRetentionUnreadableConfigOneInfoNoFinding(t *testing.T) {
	h, k := retHarness(t, retPipe("", map[string]string{"orders.public.t": "7d"}))
	k.setErr(errors.New("TOPIC_AUTHORIZATION_FAILED: not authorized to describe configs"))
	for range 4 {
		h.step(retentionRefresh)
	}
	if got := h.logs.count("retention check cannot read a config"); got != 1 {
		t.Fatalf("info lines = %d, want 1\n%s", got, h.logs)
	}
	if strings.Contains(h.logs.String(), "level=WARN msg=\"watch: check failed") {
		t.Fatalf("a warning was logged\n%s", h.logs)
	}
	if h.phase("orders", retentionID) != phaseOK || h.retentionEv() != nil {
		t.Fatal("an unreadable config made a finding")
	}

	// It reads again: the finding fires; then the ACL goes away again and the
	// finding stays (an unreadable config never resolves it).
	k.setErr(nil)
	k.set("orders.public.t", ms(day), "", "delete")
	h.step(retentionRefresh)
	h.step(time.Minute)
	if h.phase("orders", retentionID) != phaseFiring {
		t.Fatalf("did not fire once readable\n%s", h.logs)
	}
	k.setErr(errors.New("not authorized"))
	for range 3 {
		h.step(retentionRefresh)
	}
	if h.phase("orders", retentionID) != phaseFiring {
		t.Fatal("an unreadable config resolved the finding")
	}
	if got := h.logs.count("retention check cannot read a config"); got != 2 {
		t.Fatalf("info lines = %d, want 2 (one per failing spell)\n%s", got, h.logs)
	}
}

// The finding rides along when another signal opens a room, and config_risk's
// own room lists it after the connector risks.
func TestRetentionRidesAlongInRooms(t *testing.T) {
	h, k := retHarness(t, retPipe("", map[string]string{"orders.public.t": "7d"}))
	k.set("orders.public.t", "-1", "", "delete")
	h.steps(2, time.Minute)
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	cr := h.rooms.created()
	if len(cr) != 1 {
		t.Fatalf("rooms = %d\n%s", len(cr), h.logs)
	}
	body := evidenceOf(t, cr[0].alert.Raw)
	rs, _ := body["config_risks"].([]any)
	if len(rs) != 1 || rs[0].(map[string]any)["code"] != events.RiskRetentionLonger {
		t.Fatalf("config_risks = %v", body["config_risks"])
	}
	if strings.Contains(cr[0].alert.Title, "retention") {
		t.Fatalf("title names the retention finding: %q", cr[0].alert.Title)
	}

	h2, k2 := retHarness(t, retPipe("        config_risk: {action: room}\n", map[string]string{"orders.public.t": "7d"}))
	k2.set("orders.public.t", ms(day), "", "delete")
	h2.setSinkConfig("orders", map[string]string{"errors.tolerance": "all"})
	h2.steps(4, time.Minute)
	ev := h2.openedEv(t, owners.SignalConfigRisk)
	list, _ := ev["risks"].([]any)
	if len(list) != 2 || list[0].(map[string]any)["code"] != "silent_drop_sink" || list[1].(map[string]any)["code"] != events.RiskRetentionShorter {
		t.Fatalf("risks = %v", ev["risks"])
	}
	if ev["severity"] != "sev2" {
		t.Fatalf("severity = %v: a retention entry must not change it", ev["severity"])
	}
	// Closing the room acks config_risk but not the retention state.
	h2.w.OnRoomClosed(h2.rooms.created()[0].roomID, "orders", DedupKey("orders"))
	if h2.phase("orders", retentionID) != phaseFiring {
		t.Fatal("a room close acked the retention finding")
	}
}

// Bounded reads: at most maxRetentionReads per tick, the rest next tick, none
// again before retentionRefresh.
func TestRetentionReadsAreBounded(t *testing.T) {
	var tables []string
	for i := range 20 {
		tables = append(tables, fmt.Sprintf("public.t%02d", i))
	}
	p := retPipe("", nil)
	p.tables = "[" + strings.Join(tables, ", ") + "]"
	h, k := retHarness(t, p)
	for _, tb := range tables {
		k.set("orders."+tb, ms(7*day), "", "delete")
	}
	h.step(time.Minute)
	if k.callCount() != maxRetentionReads {
		t.Fatalf("first tick read %d, want %d", k.callCount(), maxRetentionReads)
	}
	h.step(time.Minute)
	if k.callCount() != 20 {
		t.Fatalf("second tick read up to %d, want 20", k.callCount())
	}
	h.steps(5, time.Minute)
	if k.callCount() != 20 {
		t.Fatalf("read again before the refresh: %d", k.callCount())
	}
	h.step(retentionRefresh)
	if k.callCount() != 20+maxRetentionReads {
		t.Fatalf("after the refresh: %d", k.callCount())
	}
}

// Scope before I/O: a declared topic outside the pipeline's graph is never
// read (one INFO line); a client without the reader turns the check off.
func TestRetentionScopeAndNoReader(t *testing.T) {
	h, k := retHarness(t, retPipe("", map[string]string{"orders.public.t": "7d", "billing.public.x": "1d"}))
	k.set("orders.public.t", ms(7*day), "", "delete")
	k.set("billing.public.x", ms(day), "", "delete")
	h.steps(3, time.Minute)
	for _, c := range k.calls {
		if c != "orders.public.t" {
			t.Fatalf("read %q, outside the pipeline", c)
		}
	}
	if got := h.logs.count("not in the pipeline's graph"); got != 1 {
		t.Fatalf("missing-topic lines = %d\n%s", got, h.logs)
	}

	h2 := newHarness(t, opts{pipes: []pipeSpec{retPipe("", map[string]string{"orders.public.t": "7d"})}})
	h2.steps(3, time.Minute)
	if got := h2.logs.count("cannot read topic configs"); got != 1 {
		t.Fatalf("no-reader lines = %d\n%s", got, h2.logs)
	}
	if h2.phase("orders", retentionID) != phaseOK {
		t.Fatal("fired without a reader")
	}
}

// A declared DLQ topic is judged (no compact check: it is not a CDC topic).
func TestRetentionDeclaredDLQTopic(t *testing.T) {
	p := pipeSpec{id: "orders", watch: dlqWatch("orders", "orders-dlq", "", false) +
		"    declared_retention:\n      topics:\n        orders-dlq: 14d\n"}
	h, k := retHarness(t, p)
	k.set("orders.public.t", ms(7*day), "", "delete")
	k.set("orders-dlq", ms(3*day), "", "compact")
	h.steps(3, time.Minute)
	risks, _ := h.retentionEv()["risks"].([]map[string]any)
	if len(risks) != 1 || risks[0]["topic"] != "orders-dlq" || risks[0]["code"] != events.RiskRetentionLonger {
		t.Fatalf("risks = %v\n%s", risks, h.logs)
	}
	h.w.mu.Lock()
	node := h.w.pipes["orders"].sigs[retentionID].last.node
	h.w.mu.Unlock()
	if node != "dlq:orders-dlq" {
		t.Fatalf("node = %q", node)
	}
}

// E6: each pipeline gets its own findings; none opens a room, so no storm.
func TestRetentionPerPipeline(t *testing.T) {
	a := retPipe("", map[string]string{"orders.public.t": "7d"})
	b := pipeSpec{id: "billing", watch: "    declared_retention:\n      topics:\n        billing.public.t: 7d\n"}
	c := pipeSpec{id: "stock", watch: "    declared_retention:\n      topics:\n        stock.public.t: 7d\n"}
	h, k := retHarness(t, a, b, c)
	k.set("orders.public.t", ms(day), "", "delete")
	k.set("billing.public.t", ms(7*day), "", "delete")
	k.set("stock.public.t", "-1", "", "delete")
	h.steps(3, time.Minute)
	if h.phase("orders", retentionID) != phaseFiring || h.phase("billing", retentionID) != phaseOK || h.phase("stock", retentionID) != phaseFiring {
		t.Fatalf("phases orders=%v billing=%v stock=%v", h.phase("orders", retentionID), h.phase("billing", retentionID), h.phase("stock", retentionID))
	}
	if h.rooms.callCount() != 0 {
		t.Fatal("retention findings reached the room service")
	}
}

// Turning config_risk off drops the retention state and stops the reads.
func TestRetentionOffWithConfigRisk(t *testing.T) {
	h, k := retHarness(t, retPipe("        config_risk: {enabled: false}\n", map[string]string{"orders.public.t": "7d"}))
	k.set("orders.public.t", ms(day), "", "delete")
	h.steps(3, time.Minute)
	if k.callCount() != 0 || h.retentionEv() != nil {
		t.Fatalf("calls = %d with config_risk off", k.callCount())
	}
}
