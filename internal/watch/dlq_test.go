package watch

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// ---- helpers ----

// dlqWatch is the watch: block of a pipeline that declares one DLQ topic.
// signals is the extra text under `signals:` (already indented 8 spaces).
func dlqWatch(pipe, topic, signals string, shared bool) string {
	var b strings.Builder
	b.WriteString("    watch:\n")
	if signals != "" {
		b.WriteString("      signals:\n" + signals)
	}
	if topic != "" {
		fmt.Fprintf(&b, "      dlq_topics:\n        - topic: %s\n          written_by: g-%s\n          shared: %t\n", topic, pipe, shared)
	}
	return b.String()
}

// dlqPipe is a pipeline "orders" with a declared DLQ "orders-dlq".
func dlqPipe(signals string) pipeSpec {
	return pipeSpec{id: "orders", watch: dlqWatch("orders", "orders-dlq", signals, false)}
}

const sinkClass = "io.confluent.connect.jdbc.JdbcSinkConnector"

// setSinkConfig makes pg-<pipe> a sink with the given error settings. The slot
// name stays so the slot signals keep working in the same harness.
func (h *harness) setSinkConfig(pipe string, kv map[string]string) {
	h.conn.mu.Lock()
	defer h.conn.mu.Unlock()
	cfg := map[string]string{"connector.class": sinkClass, "slot.name": "debezium_" + pipe, "heartbeat.interval.ms": "10000"}
	for k, v := range kv {
		cfg[k] = v
	}
	h.conn.cfg["pg-"+pipe] = cfg
}

func (h *harness) sigPhase(sig string) phase { return h.phase("orders", sig) }

// lastAlertEv is the evidence signal of the newest opened room.
func (h *harness) openedEv(t *testing.T, sig string) map[string]any {
	t.Helper()
	cr := h.rooms.created()
	if len(cr) == 0 {
		t.Fatalf("no room was opened\n%s", h.logs)
	}
	return signalEv(t, evidenceOf(t, cr[len(cr)-1].alert.Raw), sig)
}

func num(t *testing.T, m map[string]any, key string) int64 {
	t.Helper()
	f, ok := m[key].(float64)
	if !ok {
		t.Fatalf("evidence %q = %v (%T), want a number\n%v", key, m[key], m[key], m)
	}
	return int64(f)
}

// ---- no call, baseline, backlog ----

func TestDLQNoTopicsNoCall(t *testing.T) {
	h := newHarness(t, opts{}) // no declared DLQ, sources only
	h.steps(5, time.Minute)
	if got := h.kafka.callLists(); len(got) != 0 {
		t.Fatalf("DLQOffsets called with no DLQ topics: %v", got)
	}
	if h.sigPhase(owners.SignalDLQGrowth) != phaseOK {
		t.Fatal("dlq_growth fired without a topic")
	}
}

func TestDLQOneCallPerTickForAllTopics(t *testing.T) {
	pipes := []pipeSpec{
		{id: "orders", watch: dlqWatch("orders", "shared-dlq", "", true)},
		{id: "payments", watch: dlqWatch("payments", "shared-dlq", "", true)},
		{id: "users", watch: dlqWatch("users", "users-dlq", "", false)},
	}
	h := newHarness(t, opts{pipes: pipes})
	h.steps(3, time.Minute)
	calls := h.kafka.callLists()
	if len(calls) != 3 {
		t.Fatalf("want one DLQOffsets call per tick, got %d: %v", len(calls), calls)
	}
	for _, c := range calls {
		if !slices.Equal(c, []string{"shared-dlq", "users-dlq"}) {
			t.Fatalf("topics = %v, want the two DLQ topics once each, sorted", c)
		}
	}
}

func TestDLQBacklogAtStartNeverFires(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 1204) // already there when the watcher starts
	h.steps(30, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseOK || len(h.rooms.created()) != 0 {
		t.Fatalf("a pre-existing backlog fired\n%s", h.logs)
	}
	// Records then arrive: the evidence still says the backlog is older.
	h.kafka.add("orders-dlq", 3)
	h.steps(7, time.Minute)
	if len(h.rooms.created()) != 1 {
		t.Fatalf("new records on top of a backlog did not fire\n%s", h.logs)
	}
	ev := h.openedEv(t, owners.SignalDLQGrowth)
	if ev["older_than_watcher"] != true || num(t, ev, "new_in_window") != 3 || num(t, ev, "records") != 1207 {
		t.Fatalf("evidence = %v", ev)
	}
}

func TestDLQRestartDoesNotTreatTheBacklogAsNew(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	h.kafka.add("orders-dlq", 5)
	h.steps(7, time.Minute)
	if len(h.rooms.created()) != 1 {
		t.Fatalf("the first watcher did not fire\n%s", h.logs)
	}
	// A new watcher (restart) has no memory; the 5 records are old to it.
	w2, err := New(h.deps)
	if err != nil {
		t.Fatal(err)
	}
	h.w = w2
	h.steps(12, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseOK {
		t.Fatalf("a restarted watcher counted the existing backlog as new\n%s", h.logs)
	}
}

// ---- fire, hold, burst, trickle ----

func TestDLQFiresAfterFor(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute) // baseline
	h.kafka.add("orders-dlq", 3)
	h.steps(5, time.Minute) // rise seen on the first of these; for = 5m
	if len(h.rooms.created()) != 0 {
		t.Fatalf("fired before `for` ran out\n%s", h.logs)
	}
	h.step(time.Minute)
	cr := h.rooms.created()
	if len(cr) != 1 {
		t.Fatalf("want one room, got %d\n%s", len(cr), h.logs)
	}
	a := cr[0].alert
	if a.DedupKey != "watcher:orders" {
		t.Fatalf("dedup key = %q", a.DedupKey)
	}
	if !strings.HasPrefix(a.Title, "orders: DLQ orders-dlq +3 in 1h, first ") || !strings.Contains(a.Title, "Z, 3 present") {
		t.Fatalf("title = %q", a.Title)
	}
	ev := h.openedEv(t, owners.SignalDLQGrowth)
	for k, want := range map[string]any{
		"topic": "orders-dlq", "exists": true, "declared": true, "shared": false,
		"older_than_watcher": false, "window": "1h", "severity": "sev3",
	} {
		if ev[k] != want {
			t.Fatalf("evidence %s = %v, want %v\n%v", k, ev[k], want, ev)
		}
	}
	if num(t, ev, "records") != 3 || num(t, ev, "end_offset_sum") != 3 || num(t, ev, "new_in_window") != 3 || num(t, ev, "partitions") != 1 {
		t.Fatalf("numbers wrong: %v", ev)
	}
	if v, ok := ev["rate_per_hour"]; !ok || v != nil {
		t.Fatalf("rate_per_hour = %v, want null while the rate is unknown", v)
	}
	if ev["first_seen"] == nil || ev["writers"] == nil {
		t.Fatalf("evidence = %v", ev)
	}
}

func TestDLQSlowTrickleBelowFireNeverFires(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("        dlq_growth: {fire: 5}\n")}})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	h.kafka.add("orders-dlq", 3)
	h.steps(20, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseOK || len(h.rooms.created()) != 0 {
		t.Fatalf("3 new records fired with fire: 5\n%s", h.logs)
	}
	// The hold does not run the timer: 2 more, and it still needs `for`.
	h.kafka.add("orders-dlq", 2)
	h.steps(5, time.Minute)
	if len(h.rooms.created()) != 0 {
		t.Fatal("fired before `for`")
	}
	h.step(time.Minute)
	if len(h.rooms.created()) != 1 {
		t.Fatalf("5 new records did not fire\n%s", h.logs)
	}
}

func TestDLQHoldDoesNotFlap(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("        dlq_growth: {fire: 5, for: 2m}\n")}})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	h.kafka.add("orders-dlq", 5)
	h.steps(3, time.Minute) // fires
	if h.sigPhase(owners.SignalDLQGrowth) != phaseFiring {
		t.Fatalf("did not fire\n%s", h.logs)
	}
	// Samples older than the window drop out; new_in_window falls to 0 and
	// the signal resolves only after `for` of quiet.
	h.steps(61, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseOK {
		t.Fatalf("did not resolve after a quiet window\n%s", h.logs)
	}
	if len(h.rooms.resolved()) != 1 {
		t.Fatalf("want one resolved alert, got %d", len(h.rooms.resolved()))
	}
}

func TestDLQBurstIsSev2WithRate(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	// 40 minutes of steady growth: 3 per minute.
	for range 40 {
		h.kafka.add("orders-dlq", 3)
		h.step(time.Minute)
	}
	cr := h.rooms.created()
	if len(cr) != 1 {
		t.Fatalf("want one room\n%s", h.logs)
	}
	// The room opened at minute 6, when the rate was still unknown.
	if ev := h.openedEv(t, owners.SignalDLQGrowth); ev["severity"] != "sev2" || num(t, ev, "new_in_window") < 10 {
		t.Fatalf("a burst should be sev2: %v", ev)
	}
	// Later the join carries the rate.
	posts := h.rooms.postsContaining("rate_per_hour")
	if len(posts) == 0 {
		t.Fatal("no join with a rate")
	}
	last := posts[len(posts)-1].text
	if strings.Contains(last, `"rate_per_hour":null`) || !strings.Contains(last, "/h,") {
		t.Fatalf("the rate stayed unknown after 40 minutes:\n%s", last)
	}
}

func TestDLQTrickleIsSev3UntilARateExists(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	h.kafka.add("orders-dlq", 1)
	h.steps(6, time.Minute)
	if ev := h.openedEv(t, owners.SignalDLQGrowth); ev["severity"] != "sev3" {
		t.Fatalf("one record, no rate yet: want sev3, got %v", ev["severity"])
	}
}

func TestDLQRetentionLowersRecordsNotGrowth(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 100)
	h.step(time.Minute)
	h.kafka.add("orders-dlq", 5)
	h.kafka.setStart("orders-dlq", 50) // retention removed 50 records
	h.steps(7, time.Minute)
	ev := h.openedEv(t, owners.SignalDLQGrowth)
	if num(t, ev, "records") != 55 || num(t, ev, "new_in_window") != 5 || num(t, ev, "end_offset_sum") != 105 {
		t.Fatalf("evidence = %v", ev)
	}
}

func TestDLQNewEpisodeGetsNewFirstSeen(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}, cooldown: time.Minute})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	h.kafka.add("orders-dlq", 2)
	h.steps(7, time.Minute)
	first1, _ := h.openedEv(t, owners.SignalDLQGrowth)["first_seen"].(string)
	if first1 == "" || h.sigPhase(owners.SignalDLQGrowth) != phaseFiring {
		t.Fatalf("first episode did not fire\n%s", h.logs)
	}
	// Quiet for window + for: resolved.
	h.steps(70, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseOK {
		t.Fatalf("did not resolve\n%s", h.logs)
	}
	h.kafka.add("orders-dlq", 2)
	h.steps(7, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseFiring {
		t.Fatalf("second episode did not fire\n%s", h.logs)
	}
	posts := h.rooms.postsContaining("new_in_window")
	if len(posts) == 0 {
		t.Fatal("no join for the second episode")
	}
	if strings.Contains(posts[len(posts)-1].text, first1) {
		t.Fatalf("the second episode kept the first first_seen %s:\n%s", first1, posts[len(posts)-1].text)
	}
}

// ---- topic missing, appearing, deleted ----

func TestDLQTopicMissingIsHealthyForADeclaredTopic(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.steps(20, time.Minute) // the topic does not exist
	if h.sigPhase(owners.SignalDLQGrowth) != phaseOK || len(h.rooms.created()) != 0 {
		t.Fatalf("a missing declared topic fired\n%s", h.logs)
	}
	if h.sigPhase(owners.SignalConfigRisk) != phaseOK {
		t.Fatal("a missing declared topic raised a config risk")
	}
}

func TestDLQTopicAppearsLaterWithRecords(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.steps(10, time.Minute) // missing
	h.kafka.add("orders-dlq", 4)
	h.steps(7, time.Minute)
	if len(h.rooms.created()) != 1 {
		t.Fatalf("records in a topic that appeared later did not fire\n%s", h.logs)
	}
	ev := h.openedEv(t, owners.SignalDLQGrowth)
	if ev["older_than_watcher"] != false || num(t, ev, "new_in_window") != 4 {
		t.Fatalf("evidence = %v", ev)
	}
}

func TestDLQTopicDeletedClearsAndRecreatedCounts(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 10)
	h.steps(5, time.Minute)
	h.kafka.remove("orders-dlq")
	h.steps(10, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseOK || len(h.rooms.created()) != 0 {
		t.Fatalf("a deleted topic fired\n%s", h.logs)
	}
	h.kafka.set("orders-dlq", 0, 3) // created again, with 3 records
	h.steps(7, time.Minute)
	if len(h.rooms.created()) != 1 {
		t.Fatalf("the recreated topic did not fire\n%s", h.logs)
	}
	if ev := h.openedEv(t, owners.SignalDLQGrowth); num(t, ev, "new_in_window") != 3 {
		t.Fatalf("evidence = %v", ev)
	}
}

func TestDLQTopicRecreatedBetweenTicksCountsAsNew(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 100)
	h.steps(5, time.Minute)
	h.kafka.set("orders-dlq", 0, 2) // deleted and created again between two ticks
	h.steps(7, time.Minute)
	if len(h.rooms.created()) != 1 {
		t.Fatalf("offsets that went back did not count as new\n%s", h.logs)
	}
	if ev := h.openedEv(t, owners.SignalDLQGrowth); num(t, ev, "new_in_window") != 2 {
		t.Fatalf("evidence = %v", ev)
	}
}

func TestDLQSinkTopicMissingIsAConfigRisk(t *testing.T) {
	h := newHarness(t, opts{})
	h.setSinkConfig("orders", map[string]string{
		"errors.tolerance": "all", "errors.deadletterqueue.topic.name": "orders-sink-dlq",
		"errors.log.enable": "true", "errors.deadletterqueue.context.headers.enable": "true",
	})
	h.steps(4, time.Minute)
	if h.sigPhase(owners.SignalConfigRisk) != phaseFiring {
		t.Fatalf("a missing sink DLQ topic is not a config risk\n%s", h.logs)
	}
	if !strings.Contains(h.logs.String(), "orders: dlq_topic_missing") {
		t.Fatalf("log line lacks the connector and code\n%s", h.logs)
	}
	if h.sigPhase(owners.SignalDLQGrowth) != phaseOK {
		t.Fatal("dlq_growth fired for a missing topic")
	}
	// The worker creates the topic: the risk resolves.
	h.kafka.set("orders-sink-dlq", 0, 0)
	h.steps(4, time.Minute)
	if h.sigPhase(owners.SignalConfigRisk) != phaseOK {
		t.Fatalf("the risk stayed after the topic was created\n%s", h.logs)
	}
}

func TestDLQDiscoversTheTopicFromASinkConfig(t *testing.T) {
	h := newHarness(t, opts{})
	h.setSinkConfig("orders", map[string]string{"errors.tolerance": "all", "errors.deadletterqueue.topic.name": "orders-sink-dlq"})
	h.kafka.set("orders-sink-dlq", 0, 0)
	h.step(time.Minute)
	h.kafka.add("orders-sink-dlq", 7)
	h.steps(7, time.Minute)
	if len(h.rooms.created()) != 1 {
		t.Fatalf("no room for a discovered DLQ\n%s", h.logs)
	}
	ev := h.openedEv(t, owners.SignalDLQGrowth)
	if ev["topic"] != "orders-sink-dlq" || ev["declared"] != false || ev["context_headers"] != false {
		t.Fatalf("evidence = %v", ev)
	}
	if ws, _ := ev["writers"].([]any); len(ws) != 1 || ws[0] != "pg-orders" {
		t.Fatalf("writers = %v", ev["writers"])
	}
	risks, _ := ev["risks"].([]any)
	if !slices.Contains(risks, any("dlq_no_log")) {
		t.Fatalf("risks = %v, want dlq_no_log", ev["risks"])
	}
}

func TestDLQCDCTopicIsNeverADLQ(t *testing.T) {
	h := newHarness(t, opts{})
	h.setSinkConfig("orders", map[string]string{"errors.deadletterqueue.topic.name": "orders.public.t"}) // the CDC topic
	h.kafka.add("orders.public.t", 5)
	h.steps(5, time.Minute)
	if got := h.kafka.callLists(); len(got) != 0 {
		t.Fatalf("a CDC topic was read as a DLQ: %v", got)
	}
}

// ---- errors, absent reader, deadline ----

func TestDLQOffsetsErrorIsUnknownAndLoggedOnce(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	h.kafka.add("orders-dlq", 2)
	h.step(time.Minute) // breaching since here
	h.kafka.setErr(errors.New("broker unreachable"))
	h.steps(15, time.Minute)
	if len(h.rooms.created()) != 0 || h.sigPhase(owners.SignalDLQGrowth) != phaseOK {
		t.Fatalf("an unreadable topic fired\n%s", h.logs)
	}
	if n := h.logs.count("check failed, signal state unchanged"); n != 1 {
		t.Fatalf("want one warning for 15 failing ticks, got %d\n%s", n, h.logs)
	}
	// The timers were left alone: the breach started before the outage, so it
	// fires on the first good tick.
	h.kafka.setErr(nil)
	h.step(time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseFiring {
		t.Fatalf("the error ticks reset the timer\n%s", h.logs)
	}
	if h.logs.count("check works again") != 1 {
		t.Fatalf("recovery not logged\n%s", h.logs)
	}
}

func TestDLQReaderAbsentDisablesTheSignalWithOneLine(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}, noKafka: true})
	h.setSinkConfig("orders", map[string]string{"errors.tolerance": "all"}) // silent drop
	h.steps(6, time.Minute)
	if n := h.logs.count("dlq_growth is off"); n != 1 {
		t.Fatalf("want exactly one line about the absent reader, got %d\n%s", n, h.logs)
	}
	if h.sigPhase(owners.SignalDLQGrowth) != phaseOK {
		t.Fatal("dlq_growth ran without a reader")
	}
	// config_risk does not need Kafka.
	if h.sigPhase(owners.SignalConfigRisk) != phaseFiring {
		t.Fatalf("config_risk stopped with the reader\n%s", h.logs)
	}
}

func TestDLQHangingKafkaDoesNotHoldTheTick(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}, checkTimeout: 100 * time.Millisecond})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	h.kafka.hook = func(context.Context) { <-release } // ignores its context
	h.breakConnector("orders", "x")

	start := time.Now()
	h.w.tick(context.Background())
	if took := time.Since(start); took > time.Second {
		t.Fatalf("tick took %v with a hanging Kafka client", took)
	}
	h.step(3 * time.Minute)
	if h.sigPhase(owners.SignalConnectorDown) != phaseFiring {
		t.Fatalf("a hanging Kafka client held up connector_down\n%s", h.logs)
	}
	if !strings.Contains(h.logs.String(), "read DLQ offsets") {
		t.Fatalf("the timeout was not reported\n%s", h.logs)
	}
}

func TestDLQRunStopsPromptlyWithoutLeaks(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}, checkTimeout: 10 * time.Second})
	h.kafka.hook = func(ctx context.Context) { <-ctx.Done() } // honours its context
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.w.Run(ctx) }()
	waitFor(t, "the tick to reach Kafka", func() bool { return len(h.kafka.callLists()) == 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1 s of cancel")
	}
	waitFor(t, "goroutines to return to the start count", func() bool { return runtime.NumGoroutine() <= before })
}

// ---- rooms: join, acked, storm ----

func TestDLQJoinsAnOpenConnectorDownRoom(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	if len(h.rooms.created()) != 1 {
		t.Fatalf("connector_down did not open a room\n%s", h.logs)
	}
	h.kafka.add("orders-dlq", 9)
	h.steps(7, time.Minute)
	if len(h.rooms.created()) != 1 {
		t.Fatal("the DLQ opened a second room")
	}
	posts := h.rooms.postsContaining("DLQ orders-dlq +9")
	if len(posts) == 0 || !strings.Contains(posts[0].text, "new signal") {
		t.Fatalf("no join with the DLQ evidence: %v", posts)
	}
}

func TestDLQAckedRoomStaysQuietWhileItTrickles(t *testing.T) {
	// The cooldown outlasts the trickle below; a signal that still fires one
	// cooldown after the close re-opens (rearm_test.go).
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}, cooldown: 2 * time.Hour})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	h.kafka.add("orders-dlq", 1)
	h.steps(7, time.Minute)
	cr := h.rooms.created()
	if len(cr) != 1 {
		t.Fatalf("no room\n%s", h.logs)
	}
	h.rooms.closeRoom(cr[0].roomID, h.clk.Now())
	h.w.OnRoomClosed(cr[0].roomID, "orders", "watcher:orders")
	if h.sigPhase(owners.SignalDLQGrowth) != phaseAcked {
		t.Fatalf("not acked after the driver closed the room")
	}
	// It keeps trickling for longer than the cooldown: no new room.
	for range 20 {
		h.kafka.add("orders-dlq", 1)
		h.step(5 * time.Minute)
	}
	if len(h.rooms.created()) != 1 {
		t.Fatalf("a trickle re-opened an acked incident: %d rooms", len(h.rooms.created()))
	}
	// Quiet for window + for: the episode ends; the next record is a new one.
	h.steps(70, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseOK {
		t.Fatalf("did not clear\n%s", h.logs)
	}
	h.kafka.add("orders-dlq", 1)
	h.steps(7, time.Minute)
	if len(h.rooms.created()) != 2 {
		t.Fatalf("the next episode did not open a fresh room: %d rooms\n%s", len(h.rooms.created()), h.logs)
	}
}

func sharedPipes(n int) []pipeSpec {
	ids := []string{"a", "b", "c", "d"}[:n]
	var ps []pipeSpec
	for _, id := range ids {
		ps = append(ps, pipeSpec{id: id, watch: dlqWatch(id, "shared-dlq", "", true)})
	}
	return ps
}

func TestDLQSharedTopicTwoPipelinesTwoRooms(t *testing.T) {
	h := newHarness(t, opts{pipes: sharedPipes(2)})
	h.kafka.set("shared-dlq", 0, 0)
	h.step(time.Minute)
	h.kafka.add("shared-dlq", 4)
	h.steps(7, time.Minute)
	cr := h.rooms.created()
	if len(cr) != 2 {
		t.Fatalf("want a room per pipeline, got %d\n%s", len(cr), h.logs)
	}
	for _, c := range cr {
		ev := signalEv(t, evidenceOf(t, c.alert.Raw), owners.SignalDLQGrowth)
		if ev["shared"] != true {
			t.Fatalf("shared flag missing: %v", ev)
		}
	}
}

func TestDLQSharedTopicThreePipelinesIsAStorm(t *testing.T) {
	h := newHarness(t, opts{pipes: sharedPipes(3)})
	h.kafka.set("shared-dlq", 0, 0)
	h.step(time.Minute)
	h.kafka.add("shared-dlq", 4)
	h.steps(7, time.Minute)
	cr := h.rooms.created()
	if len(cr) != 1 {
		t.Fatalf("storm cap: want one room, got %d\n%s", len(cr), h.logs)
	}
	if !strings.Contains(cr[0].alert.Title, "and 2 other pipelines") {
		t.Fatalf("title = %q", cr[0].alert.Title)
	}
}

// ---- config_risk ----

func TestConfigRiskDefaultsToLogAndFiresOnTheSecondTick(t *testing.T) {
	h := newHarness(t, opts{})
	h.setSinkConfig("orders", map[string]string{"errors.tolerance": "all"})
	h.step(time.Minute)
	if h.sigPhase(owners.SignalConfigRisk) != phaseOK {
		t.Fatal("fired on the first tick")
	}
	h.step(time.Minute)
	if h.sigPhase(owners.SignalConfigRisk) != phaseFiring {
		t.Fatalf("did not fire on the second tick\n%s", h.logs)
	}
	h.steps(5, time.Minute)
	if len(h.rooms.created()) != 0 || h.rooms.callCount() != 0 {
		t.Fatalf("a log-only signal touched the rooms (%d calls)", h.rooms.callCount())
	}
	if !strings.Contains(h.logs.String(), "silent_drop_sink") || !strings.Contains(h.logs.String(), "action=log") {
		t.Fatalf("log line missing\n%s", h.logs)
	}
}

func TestConfigRiskActionRoomOpensARoom(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders", watch: "    watch:\n      signals:\n        config_risk: {action: room}\n"}}})
	h.setSinkConfig("orders", map[string]string{"errors.tolerance": "all"})
	h.steps(4, time.Minute)
	cr := h.rooms.created()
	if len(cr) != 1 {
		t.Fatalf("want one room\n%s", h.logs)
	}
	if cr[0].alert.Title != "orders: config risk pg-orders silent_drop_sink" || cr[0].alert.Severity != "sev2" {
		t.Fatalf("title %q severity %q", cr[0].alert.Title, cr[0].alert.Severity)
	}
	ev := h.openedEv(t, owners.SignalConfigRisk)
	rs, _ := ev["risks"].([]any)
	if len(rs) != 1 {
		t.Fatalf("risks = %v", ev["risks"])
	}
	r0, _ := rs[0].(map[string]any)
	if r0["code"] != "silent_drop_sink" || r0["connector"] != "pg-orders" || !strings.Contains(fmt.Sprint(r0["text"]), "dropped with no trace") {
		t.Fatalf("risk = %v", r0)
	}
}

func TestConfigRiskNeverPutsAConfigValueInEvidenceOrLogs(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders", watch: "    watch:\n      signals:\n        config_risk: {action: room}\n"}}})
	const sentinel = "S3NTINEL-hunter2"
	h.setSinkConfig("orders", map[string]string{
		"errors.tolerance": "all", "connection.password": sentinel, "connection.url": "jdbc:postgresql://" + sentinel,
		"errors.deadletterqueue.topic.name": "bad topic " + sentinel, // not a valid topic name
		"errors.log.include.messages":       "true", "errors.log.enable": "true",
	})
	h.steps(6, time.Minute)
	if len(h.rooms.created()) == 0 {
		t.Fatalf("no room\n%s", h.logs)
	}
	var all strings.Builder
	for _, o := range h.rooms.created() {
		all.WriteString(o.alert.Title + string(o.alert.Raw))
	}
	for _, p := range h.rooms.postsContaining("") {
		all.WriteString(p.text)
	}
	all.WriteString(h.logs.String())
	if strings.Contains(all.String(), sentinel) {
		t.Fatalf("a config value leaked:\n%s", all.String())
	}
}

func TestConfigRiskInfoDoesNotBreach(t *testing.T) {
	h := newHarness(t, opts{})
	// DLQ on, logging on, headers off: only the info finding dlq_no_headers.
	h.setSinkConfig("orders", map[string]string{"errors.tolerance": "all", "errors.deadletterqueue.topic.name": "orders-sink-dlq", "errors.log.enable": "true"})
	h.kafka.set("orders-sink-dlq", 0, 0)
	h.steps(6, time.Minute)
	if h.sigPhase(owners.SignalConfigRisk) != phaseOK {
		t.Fatalf("an info finding breached\n%s", h.logs)
	}
}

func TestConfigRiskUnreadableConfigIsUnknown(t *testing.T) {
	h := newHarness(t, opts{})
	h.conn.mu.Lock()
	h.conn.cfgErr["pg-orders"] = errors.New("connect: 500")
	h.conn.mu.Unlock()
	h.steps(6, time.Minute)
	if h.sigPhase(owners.SignalConfigRisk) != phaseOK {
		t.Fatal("an unreadable config changed state")
	}
	if !strings.Contains(h.logs.String(), "signal=config_risk") || !strings.Contains(h.logs.String(), "connect: 500") {
		t.Fatalf("the failure was not reported\n%s", h.logs)
	}
}

func TestConfigRisksRideAlongInAnotherSignalsRoom(t *testing.T) {
	h := newHarness(t, opts{})
	h.setSinkConfig("orders", map[string]string{"errors.tolerance": "all", "errors.deadletterqueue.topic.name": "orders-sink-dlq"})
	h.kafka.set("orders-sink-dlq", 0, 0)
	h.breakConnector("orders", "boom")
	h.steps(5, time.Minute)
	cr := h.rooms.created()
	if len(cr) != 1 {
		t.Fatalf("want one room\n%s", h.logs)
	}
	body := evidenceOf(t, cr[0].alert.Raw)
	risks, _ := body["config_risks"].([]any)
	if len(risks) == 0 {
		t.Fatalf("no config_risks on the body: %v", body)
	}
	for _, r := range risks {
		m := r.(map[string]any)
		if m["text"] != nil {
			t.Fatalf("the body list must carry codes only: %v", m)
		}
	}
	if !strings.Contains(string(cr[0].alert.Raw), "dlq_no_log") {
		t.Fatalf("dlq_no_log missing: %s", cr[0].alert.Raw)
	}
}

func TestConfigRiskOffMeansNoRisksInEvidence(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders", watch: "    watch:\n      signals:\n        config_risk: {enabled: false}\n"}}})
	h.setSinkConfig("orders", map[string]string{"errors.tolerance": "all"})
	h.breakConnector("orders", "boom")
	h.steps(5, time.Minute)
	cr := h.rooms.created()
	if len(cr) != 1 || strings.Contains(string(cr[0].alert.Raw), "config_risks") {
		t.Fatalf("risks shown with config_risk disabled")
	}
}

func TestCommas(t *testing.T) {
	for in, want := range map[int64]string{0: "0", 7: "7", 999: "999", 1000: "1,000", 1204: "1,204", 1234567: "1,234,567", -1500: "-1,500"} {
		if got := commas(in); got != want {
			t.Errorf("commas(%d) = %q, want %q", in, got, want)
		}
	}
}

// ---- review round 1 ----

// lastSev is the severity of the latest reading of a signal.
func (h *harness) lastSev(sig string) string {
	h.w.mu.Lock()
	defer h.w.mu.Unlock()
	return h.w.pipes["orders"].sigs[sig].last.sev
}

func TestConfigRiskSurvivesKafkaErrors(t *testing.T) {
	h := newHarness(t, opts{})
	h.setSinkConfig("orders", map[string]string{
		"errors.tolerance": "all", "errors.deadletterqueue.topic.name": "orders-sink-dlq",
		"errors.log.enable": "true", "errors.deadletterqueue.context.headers.enable": "true",
	})
	h.steps(4, time.Minute) // the topic is missing: dlq_topic_missing fires
	if h.sigPhase(owners.SignalConfigRisk) != phaseFiring {
		t.Fatalf("setup: not firing\n%s", h.logs)
	}
	h.kafka.setErr(errors.New("broker unreachable"))
	h.steps(3, time.Minute)
	if h.sigPhase(owners.SignalConfigRisk) != phaseFiring {
		t.Fatalf("a failed Kafka read resolved config_risk\n%s", h.logs)
	}
	// Even with no last answer in memory, the read error is unknown, not clear.
	h.w.mu.Lock()
	h.w.dlqState = map[string]*dlqTopicState{}
	h.w.mu.Unlock()
	h.steps(5, time.Minute)
	if h.sigPhase(owners.SignalConfigRisk) != phaseFiring {
		t.Fatalf("config_risk resolved on an unknown topic state\n%s", h.logs)
	}
	if h.logs.count("signal resolved") != 0 {
		t.Fatalf("a resolve was logged\n%s", h.logs)
	}
}

func TestDLQOneAbsentAnswerDoesNotMakeTheBacklogNew(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 1204)
	h.steps(5, time.Minute)
	h.kafka.remove("orders-dlq") // a broker restart answers "unknown topic" once
	h.step(time.Minute)
	h.kafka.set("orders-dlq", 0, 1204)
	h.steps(20, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseOK || len(h.rooms.created()) != 0 {
		t.Fatalf("an old backlog fired after one absent answer\n%s", h.logs)
	}
	// Records that arrived during the gap are still counted.
	h.kafka.remove("orders-dlq")
	h.step(time.Minute)
	h.kafka.set("orders-dlq", 0, 1210)
	h.steps(7, time.Minute)
	if len(h.rooms.created()) != 1 {
		t.Fatalf("records written during the gap were lost\n%s", h.logs)
	}
	if ev := h.openedEv(t, owners.SignalDLQGrowth); num(t, ev, "new_in_window") != 6 {
		t.Fatalf("evidence = %v", ev)
	}
}

func TestDLQSteadyTrickleStaysSev3(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	sawFiring := false
	for m := 1; m <= 180; m++ {
		if m%20 == 0 {
			h.kafka.add("orders-dlq", 1)
		}
		h.step(time.Minute)
		if h.sigPhase(owners.SignalDLQGrowth) == phaseFiring {
			sawFiring = true
			if sev := h.lastSev(owners.SignalDLQGrowth); sev != "sev3" {
				t.Fatalf("minute %d: one record per 20 minutes is %s, want sev3", m, sev)
			}
		}
	}
	if !sawFiring {
		t.Fatalf("never fired\n%s", h.logs)
	}
}

func TestDLQAcceleratingTrickleBecomesSev2(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	for m := 1; m <= 60; m++ {
		if m%20 == 0 {
			h.kafka.add("orders-dlq", 1)
		}
		h.step(time.Minute)
	}
	if sev := h.lastSev(owners.SignalDLQGrowth); sev != "sev3" {
		t.Fatalf("before the burst: %s, want sev3", sev)
	}
	for range 5 { // five records in five minutes, still under 10 in the window
		h.kafka.add("orders-dlq", 1)
		h.step(time.Minute)
	}
	if sev := h.lastSev(owners.SignalDLQGrowth); sev != "sev2" {
		t.Fatalf("an accelerating trickle is %s, want sev2", sev)
	}
}

func TestDLQWindowPrunesOldSamples(t *testing.T) {
	// window 30m is below the ring's one-hour keep, so only the window cutoff
	// can make the old records stop counting.
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("        dlq_growth: {window: 30m, for: 2m}\n")}})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	h.kafka.add("orders-dlq", 3)
	h.steps(4, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseFiring {
		t.Fatalf("did not fire\n%s", h.logs)
	}
	h.steps(34, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseOK {
		t.Fatalf("records older than the window still count\n%s", h.logs)
	}
}

func TestDLQUnreadableConfigDoesNotResolve(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	h.kafka.add("orders-dlq", 3)
	h.steps(7, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseFiring {
		t.Fatalf("did not fire\n%s", h.logs)
	}
	h.conn.mu.Lock()
	h.conn.cfgErr["pg-orders"] = errors.New("connect: 500")
	h.conn.mu.Unlock()
	h.steps(80, time.Minute) // the topic is quiet for longer than window + for
	if h.sigPhase(owners.SignalDLQGrowth) != phaseFiring || len(h.rooms.resolved()) != 0 {
		t.Fatalf("an unreadable config let dlq_growth resolve\n%s", h.logs)
	}
}

func TestDLQRisksInEvidenceOnlyWithConfigRiskOn(t *testing.T) {
	for _, on := range []bool{true, false} {
		sig := fmt.Sprintf("        config_risk: {enabled: %t}\n", on)
		h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders", watch: "    watch:\n      signals:\n" + sig}}})
		h.setSinkConfig("orders", map[string]string{"errors.deadletterqueue.topic.name": "orders-sink-dlq"})
		h.kafka.set("orders-sink-dlq", 0, 0)
		h.step(time.Minute)
		h.kafka.add("orders-sink-dlq", 7)
		h.steps(7, time.Minute)
		ev := h.openedEv(t, owners.SignalDLQGrowth)
		if _, has := ev["risks"]; has != on {
			t.Fatalf("config_risk enabled=%t but risks present=%t: %v", on, has, ev)
		}
	}
}

func TestDLQIdleTopicStateIsDropped(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.w.mu.Lock()
	h.w.dlqState["gone-dlq"] = &dlqTopicState{lastWanted: t0}
	h.w.mu.Unlock()
	h.step(30 * time.Minute)
	h.w.mu.Lock()
	_, kept := h.w.dlqState["gone-dlq"]
	h.w.mu.Unlock()
	if !kept {
		t.Fatal("dropped too early")
	}
	h.step(90 * time.Minute)
	h.w.mu.Lock()
	_, kept = h.w.dlqState["gone-dlq"]
	h.w.mu.Unlock()
	if kept {
		t.Fatal("a topic nobody asks for stayed in memory")
	}
}

// ---- review round 2 ----

func TestDLQDeclaredTopicMissingForLongThenAppears(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.steps(90, time.Minute) // longer than the window and the ring: still missing
	if len(h.rooms.created()) != 0 {
		t.Fatalf("fired for a missing topic\n%s", h.logs)
	}
	h.kafka.add("orders-dlq", 4)
	h.steps(7, time.Minute)
	if len(h.rooms.created()) != 1 {
		t.Fatalf("a consumer DLQ that appeared after 90 minutes did not fire\n%s", h.logs)
	}
	if ev := h.openedEv(t, owners.SignalDLQGrowth); num(t, ev, "new_in_window") != 4 || ev["older_than_watcher"] != false {
		t.Fatalf("evidence = %v", ev)
	}
}

func TestDLQBacklogAbsentForLongBackWithMore(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 100)
	h.steps(5, time.Minute)
	h.kafka.remove("orders-dlq")
	h.steps(90, time.Minute)
	h.kafka.set("orders-dlq", 0, 105)
	h.steps(7, time.Minute)
	if len(h.rooms.created()) != 1 {
		t.Fatalf("5 records written during a long absence did not fire\n%s", h.logs)
	}
	if ev := h.openedEv(t, owners.SignalDLQGrowth); num(t, ev, "new_in_window") != 5 || ev["older_than_watcher"] != true {
		t.Fatalf("evidence = %v", ev)
	}
}

func TestDLQRecreatedAfterAnAbsenceLongerThanTheWindow(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 100)
	h.steps(5, time.Minute)
	h.kafka.remove("orders-dlq")
	h.steps(90, time.Minute)
	h.kafka.set("orders-dlq", 0, 3) // created again, smaller than the old sum
	h.steps(7, time.Minute)
	if len(h.rooms.created()) != 1 {
		t.Fatalf("a recreated topic did not fire\n%s", h.logs)
	}
	if ev := h.openedEv(t, owners.SignalDLQGrowth); num(t, ev, "new_in_window") != 3 {
		t.Fatalf("evidence = %v", ev)
	}
}

func TestDLQBatchedTrickleHoldsSev3(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	sawFiring := false
	for m := 1; m <= 300; m++ {
		if m%20 == 0 {
			h.kafka.add("orders-dlq", 2)
		}
		h.step(time.Minute)
		if h.sigPhase(owners.SignalDLQGrowth) == phaseFiring {
			sawFiring = true
			if sev := h.lastSev(owners.SignalDLQGrowth); sev != "sev3" {
				t.Fatalf("minute %d: 2 records per 20 minutes is %s, want sev3", m, sev)
			}
		}
	}
	if !sawFiring {
		t.Fatalf("never fired\n%s", h.logs)
	}
	if n := len(h.rooms.postsContaining("severity raised")); n != 0 {
		t.Fatalf("%d \"severity raised\" posts for a steady trickle", n)
	}
}

func TestIsRisingBoundaries(t *testing.T) {
	for _, c := range []struct {
		older, newer int64
		want         bool
	}{
		{0, 4, false}, // 2*newer = 8 < 10: too small to be a surge
		{0, 5, true},  // exactly at the threshold
		{0, 1, false},
		{2, 5, false}, // 5 < 2*2+2
		{2, 6, true},  // at 2*older+2
		{3, 7, false}, // 7 < 2*3+2 = 8
		{3, 8, true},  // at 2*older+2
		{4, 9, false},
		{4, 10, true},
		{5, 5, false}, // steady
	} {
		if got := isRising(c.older, c.newer); got != c.want {
			t.Errorf("isRising(older %d, newer %d) = %v, want %v", c.older, c.newer, got, c.want)
		}
	}
}

// ringWith builds a one-hour ring with one sample a minute: older records land
// at minute -50 (older half), newer ones at minute -10 (newer half).
func ringWith(now time.Time, older, newer int64) *dlqTopicState {
	st := &dlqTopicState{}
	var total int64
	for m := -60; m <= 0; m++ {
		switch m {
		case -50:
			total += older
		case -10:
			total += newer
		}
		st.samples = append(st.samples, sample{at: now.Add(time.Duration(m) * time.Minute), retained: total})
	}
	return st
}

func TestWindowedRisingNeedsAFullRing(t *testing.T) {
	now := t0.Add(3 * time.Hour)
	_, _, _, _, rising := windowed(ringWith(now, 0, 5), now, time.Hour, 2*time.Minute)
	if !rising {
		t.Fatal("5 against 0 over a full ring is a surge")
	}
	_, _, _, _, rising = windowed(ringWith(now, 0, 4), now, time.Hour, 2*time.Minute)
	if rising {
		t.Fatal("4 against 0 is below the threshold")
	}
	// The same counts on a ring that starts 20 minutes ago cannot be called a surge.
	young := ringWith(now, 0, 5)
	young.samples = young.samples[40:]
	_, _, _, _, rising = windowed(young, now, time.Hour, 2*time.Minute)
	if rising {
		t.Fatal("a young ring read as a surge")
	}
}

func TestConfigRiskInEvidenceDuringAKafkaOutage(t *testing.T) {
	h := newHarness(t, opts{})
	h.setSinkConfig("orders", map[string]string{
		"errors.deadletterqueue.topic.name": "orders-sink-dlq",
		"errors.log.enable":                 "true", "errors.deadletterqueue.context.headers.enable": "true",
	})
	h.steps(4, time.Minute) // the topic is missing: dlq_topic_missing is known
	h.kafka.setErr(errors.New("broker unreachable"))
	h.breakConnector("orders", "boom")
	h.steps(5, time.Minute)
	cr := h.rooms.created()
	if len(cr) != 1 {
		t.Fatalf("want one room\n%s", h.logs)
	}
	if !strings.Contains(string(cr[0].alert.Raw), "dlq_topic_missing") {
		t.Fatalf("the last known topic state was not used during the outage: %s", cr[0].alert.Raw)
	}
}

func TestDLQMissingConnectorDoesNotLeaveTheSignalUnknown(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	h.kafka.add("orders-dlq", 3)
	h.steps(7, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseFiring {
		t.Fatalf("did not fire\n%s", h.logs)
	}
	// Kafka Connect no longer knows the connector (404): connector_down says so.
	h.conn.setStatusErr("pg-orders", fmt.Errorf("connect: %w", clients.ErrNotFound))
	h.steps(80, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseOK {
		t.Fatalf("dlq_growth stayed unknown behind a 404 connector\n%s", h.logs)
	}
	if strings.Contains(h.logs.String(), "DLQ topics unknown") {
		t.Fatalf("a 404 connector was reported as an unreadable config\n%s", h.logs)
	}
}

// ---- review round 3 ----

// firingSinkDLQ sets up a pipeline whose sink connector names the DLQ topic
// and gets that signal firing in a room.
func firingSinkDLQ(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t, opts{})
	h.setSinkConfig("orders", map[string]string{
		"errors.tolerance": "all", "errors.deadletterqueue.topic.name": "orders-sink-dlq",
		"errors.log.enable": "true", "errors.deadletterqueue.context.headers.enable": "true",
	})
	h.kafka.set("orders-sink-dlq", 0, 0)
	h.step(time.Minute)
	h.kafka.add("orders-sink-dlq", 3)
	h.steps(7, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseFiring || len(h.rooms.created()) != 1 {
		t.Fatalf("setup: dlq_growth is not firing in one room\n%s", h.logs)
	}
	return h
}

func TestDLQSinkReturning404DoesNotResolveAndRefire(t *testing.T) {
	h := firingSinkDLQ(t)
	h.conn.setStatusErr("pg-orders", fmt.Errorf("connect: %w", clients.ErrNotFound))
	for range 10 { // longer than `for`
		h.step(time.Minute)
		if h.sigPhase(owners.SignalDLQGrowth) != phaseFiring {
			t.Fatalf("dlq_growth left firing while the sink answered 404\n%s", h.logs)
		}
	}
	h.conn.mu.Lock()
	delete(h.conn.statusErr, "pg-orders") // the connector is back, records unchanged
	h.conn.mu.Unlock()
	before := len(h.rooms.postsContaining("new signal"))
	h.steps(10, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseFiring {
		t.Fatalf("dlq_growth not firing after the connector came back\n%s", h.logs)
	}
	if n := len(h.rooms.resolved()); n != 0 {
		t.Fatalf("%d resolved alerts: the room was resolved by a 404\n%s", n, h.logs)
	}
	if after := len(h.rooms.postsContaining("new signal")); after != before {
		t.Fatalf("dlq_growth re-fired into the room as a new signal (%d -> %d)", before, after)
	}
	for _, line := range strings.Split(h.logs.String(), "\n") {
		if strings.Contains(line, "signal resolved") && strings.Contains(line, "signal=dlq_growth") {
			t.Fatalf("dlq_growth resolved: %s", line)
		}
	}
}

func TestDLQRememberedConnectorExpires(t *testing.T) {
	h := firingSinkDLQ(t)
	h.conn.setStatusErr("pg-orders", fmt.Errorf("connect: %w", clients.ErrNotFound))
	// Gone for longer than the ring window: nothing is watched any more.
	h.steps(70, time.Minute)
	h.w.mu.Lock()
	n := len(h.w.dlqSeen)
	h.w.mu.Unlock()
	if n != 0 {
		t.Fatalf("a connector gone for longer than the window is still remembered (%d)", n)
	}
}

func TestDLQFiringStaysFiringOnStatus500(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	h.kafka.add("orders-dlq", 3)
	h.steps(7, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseFiring {
		t.Fatalf("did not fire\n%s", h.logs)
	}
	h.conn.setStatusErr("pg-orders", errors.New("connect: 500"))
	h.steps(80, time.Minute) // quiet for longer than window + for
	if h.sigPhase(owners.SignalDLQGrowth) != phaseFiring || len(h.rooms.resolved()) != 0 {
		t.Fatalf("a 500 on Status let dlq_growth resolve\n%s", h.logs)
	}
}

func TestConfigRiskWith404ConnectorIsNotUnknown(t *testing.T) {
	h := newHarness(t, opts{})
	h.conn.setStatusErr("pg-orders", fmt.Errorf("connect: %w", clients.ErrNotFound))
	h.steps(5, time.Minute)
	h.w.mu.Lock()
	errText := h.w.pipes["orders"].sigs[owners.SignalConfigRisk].errText
	h.w.mu.Unlock()
	if errText != "" || strings.Contains(h.logs.String(), "config not read") {
		t.Fatalf("config_risk is unknown behind a 404 connector: %q\n%s", errText, h.logs)
	}
}

func TestDLQConfigNotFoundMeansNoDLQTopic(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.kafka.set("orders-dlq", 0, 0)
	h.step(time.Minute)
	h.kafka.add("orders-dlq", 3)
	h.steps(7, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseFiring {
		t.Fatalf("did not fire\n%s", h.logs)
	}
	// Status works, Config answers 404: no config to find a DLQ topic in. The
	// declared topic is still watched and, quiet, resolves; no "unknown".
	h.conn.mu.Lock()
	h.conn.cfgErr["pg-orders"] = fmt.Errorf("connect: %w", clients.ErrNotFound)
	h.conn.mu.Unlock()
	h.steps(80, time.Minute)
	if h.sigPhase(owners.SignalDLQGrowth) != phaseOK {
		t.Fatalf("a 404 on Config left dlq_growth unknown\n%s", h.logs)
	}
	if strings.Contains(h.logs.String(), "DLQ topics unknown") {
		t.Fatalf("a 404 on Config was reported as unreadable\n%s", h.logs)
	}
}

func TestDLQSamplesStayBoundedAfterDaysAbsent(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{dlqPipe("")}})
	h.steps(3*24*60, time.Minute) // three days with the topic missing
	h.w.mu.Lock()
	st := h.w.dlqState["orders-dlq"]
	n := len(st.samples)
	h.w.mu.Unlock()
	if n > maxDLQSamples || n > 62 {
		t.Fatalf("%d samples after 3 days absent, want about window/interval (60)", n)
	}
	// And the same once it exists.
	h.kafka.set("orders-dlq", 0, 5)
	h.steps(3*24*60, time.Minute)
	h.w.mu.Lock()
	n = len(h.w.dlqState["orders-dlq"].samples)
	h.w.mu.Unlock()
	if n > 62 {
		t.Fatalf("%d samples after 3 days present, want about 60", n)
	}
}
