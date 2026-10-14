package watch

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// The harness pipeline "orders" has group g-orders and CDC topic orders.public.t.
const (
	grp = "g-orders"
	cdc = "orders.public.t"
)

func TestConsumerLagFiresAfterForAndClearsWithHysteresis(t *testing.T) {
	// defaults: fire 1000, clear 500, for 10m
	h := newHarness(t, opts{lag: true})
	h.lag.set(grp, "Stable", 2, cdc, 100, 1600) // lag 1500, flat
	h.step(0)
	h.steps(9, time.Minute) // 9 min breaching
	if h.phase("orders", owners.SignalConsumerLag) != phaseOK || len(h.rooms.created()) != 0 {
		t.Fatal("consumer_lag fired before for")
	}
	h.step(time.Minute)
	if h.phase("orders", owners.SignalConsumerLag) != phaseFiring {
		t.Fatal("consumer_lag did not fire at for")
	}
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("rooms = %d, want 1", len(rooms))
	}
	a := rooms[0].alert
	if !strings.Contains(a.Title, "group g-orders lag 1500 (+0/min, 2 members)") || a.Severity != "sev3" {
		t.Fatalf("title %q severity %q", a.Title, a.Severity)
	}
	ev := signalEv(t, evidenceOf(t, a.Raw), owners.SignalConsumerLag)
	if ev["group"] != grp || ev["total_lag"] != float64(1500) || ev["members"] != float64(2) || ev["state"] != "Stable" {
		t.Fatalf("evidence: %v", ev)
	}
	if wp, _ := ev["worst_partitions"].([]any); len(wp) != 1 {
		t.Fatalf("worst_partitions: %v", ev["worst_partitions"])
	}

	// Between clear and fire: no clear timer, stays firing.
	h.lag.set(grp, "Stable", 2, cdc, 900, 1600) // 700
	h.steps(20, time.Minute)
	if h.phase("orders", owners.SignalConsumerLag) != phaseFiring {
		t.Fatal("a lag between clear and fire resolved the signal")
	}
	h.lag.set(grp, "Stable", 2, cdc, 1200, 1600) // 400 < clear
	h.step(time.Minute)                          // clearing since now
	h.steps(9, time.Minute)
	if h.phase("orders", owners.SignalConsumerLag) != phaseFiring {
		t.Fatal("resolved before for")
	}
	h.step(time.Minute)
	if h.phase("orders", owners.SignalConsumerLag) != phaseOK {
		t.Fatal("did not resolve after for below clear")
	}
	if len(h.rooms.resolved()) != 1 {
		t.Fatalf("resolved alerts = %d, want 1", len(h.rooms.resolved()))
	}
}

func TestConsumerLagGrowingFor2xForIsSev2(t *testing.T) {
	h := newHarness(t, opts{lag: true})
	lag := int64(1000)
	for i := range 40 {
		h.lag.set(grp, "Stable", 2, cdc, 0, lag)
		lag += 100
		h.step(time.Minute)
		if i == 10 && h.phase("orders", owners.SignalConsumerLag) == phaseFiring {
			r := h.w.pipes["orders"].sigs[owners.SignalConsumerLag].last
			if r.sev != "sev3" {
				t.Fatalf("sev at first fire = %s, want sev3 (growth not held 2×for yet)", r.sev)
			}
		}
	}
	r := h.w.pipes["orders"].sigs[owners.SignalConsumerLag].last
	if r.sev != "sev2" || r.ev["lag_rate_per_min"] != int64(100) {
		t.Fatalf("after 40 min of growth: sev %s rate %v, want sev2 +100/min", r.sev, r.ev["lag_rate_per_min"])
	}
	if !strings.Contains(r.label, "+100/min") {
		t.Fatalf("label %q", r.label)
	}
}

func TestConsumerLagEmptyGroupIsSev2(t *testing.T) {
	h := newHarness(t, opts{lag: true})
	h.lag.set(grp, "Empty", 0, cdc, 100, 1571) // 1471, nobody consuming
	h.steps(12, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("rooms = %d, want 1", len(rooms))
	}
	if rooms[0].alert.Severity != "sev2" || !strings.Contains(rooms[0].alert.Title, "lag 1471") || !strings.Contains(rooms[0].alert.Title, "0 members") {
		t.Fatalf("alert %q %q", rooms[0].alert.Title, rooms[0].alert.Severity)
	}

	// members_expected: false keeps it at sev3.
	h2 := newHarness(t, opts{lag: true, pipes: []pipeSpec{{id: "orders", watch: "    watch:\n      signals:\n        consumer_lag: {members_expected: false}\n"}}})
	h2.lag.set(grp, "Empty", 0, cdc, 100, 1571)
	h2.steps(12, time.Minute)
	if r := h2.rooms.created(); len(r) != 1 || r[0].alert.Severity != "sev3" {
		t.Fatalf("members_expected false: %+v", r)
	}
}

func TestGroupUnstableFlapsFireAtOnce(t *testing.T) {
	h := newHarness(t, opts{lag: true})
	states := []string{"Stable", "PreparingRebalance", "Stable", "PreparingRebalance"}
	for i, st := range states {
		h.lag.set(grp, st, 2, cdc, 0, 10)
		h.step(time.Minute)
		if i < 3 && h.phase("orders", owners.SignalGroupUnstable) != phaseOK {
			t.Fatalf("fired after %d changes", i)
		}
	}
	// 3 changes in 3 minutes: fires at once, well before `for` (5m).
	if h.phase("orders", owners.SignalGroupUnstable) != phaseFiring {
		t.Fatal("3 flaps did not fire at once")
	}
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("rooms = %d, want 1", len(rooms))
	}
	a := rooms[0].alert
	if !strings.Contains(a.Title, "group g-orders flapping (3 state changes in 15m)") || a.Severity != "sev3" {
		t.Fatalf("title %q sev %q", a.Title, a.Severity)
	}
	ev := signalEv(t, evidenceOf(t, a.Raw), owners.SignalGroupUnstable)
	if ev["state_changes_15m"] != float64(3) || ev["state"] != "PreparingRebalance" {
		t.Fatalf("evidence: %v", ev)
	}
	if st, _ := ev["states"].([]any); len(st) != 4 {
		t.Fatalf("states: %v", ev["states"])
	}
}

func TestGroupUnstableStuckRebalancingFiresAfterFor(t *testing.T) {
	h := newHarness(t, opts{lag: true})
	h.lag.set(grp, "CompletingRebalance", 2, cdc, 0, 10)
	h.step(0)
	h.steps(4, time.Minute)
	if h.phase("orders", owners.SignalGroupUnstable) != phaseOK {
		t.Fatal("fired before for")
	}
	h.step(time.Minute)
	if h.phase("orders", owners.SignalGroupUnstable) != phaseFiring {
		t.Fatal("did not fire after for")
	}
	// A memberless Dead group is calm for group_unstable.
	h.lag.set(grp, "Dead", 0, cdc, 0, 10)
	h.steps(6, time.Minute)
	if h.phase("orders", owners.SignalGroupUnstable) != phaseOK {
		t.Fatal("a memberless Dead group kept group_unstable firing")
	}
}

func TestGroupUnstableIsSev2WithLag(t *testing.T) {
	h := newHarness(t, opts{lag: true})
	h.lag.set(grp, "PreparingRebalance", 2, cdc, 0, 5000)
	h.steps(12, time.Minute)
	if h.phase("orders", owners.SignalConsumerLag) != phaseFiring {
		t.Fatal("consumer_lag not firing")
	}
	if r := h.w.pipes["orders"].sigs[owners.SignalGroupUnstable].last; r.sev != "sev2" {
		t.Fatalf("group_unstable sev = %s with consumer_lag firing, want sev2", r.sev)
	}
}

func TestOffsetBelowLogStartIsSev1AtOnce(t *testing.T) {
	h := newHarness(t, opts{lag: true})
	h.lag.set(grp, "Stable", 1, cdc, 100, 17_200_500)
	h.kafka.set(cdc, 17_200_100, 17_200_500) // retention moved past the commit
	h.step(0)
	if h.phase("orders", owners.SignalOffsetBelowLogStart) != phaseFiring {
		t.Fatal("did not fire on the first tick")
	}
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("rooms = %d, want 1", len(rooms))
	}
	a := rooms[0].alert
	if a.Severity != "sev1" || !strings.Contains(a.Title, "group g-orders committed below log start on orders.public.t/0 (17.2M records gone)") {
		t.Fatalf("title %q sev %q", a.Title, a.Severity)
	}
	ev := signalEv(t, evidenceOf(t, a.Raw), owners.SignalOffsetBelowLogStart)
	if ev["committed"] != float64(100) || ev["log_start"] != float64(17_200_100) || ev["records_lost"] != float64(17_200_000) || ev["topic"] != cdc {
		t.Fatalf("evidence: %v", ev)
	}
	// The offsets call asked for the CDC topic only.
	calls := h.kafka.callLists()
	if len(calls) != 1 || len(calls[0]) != 1 || calls[0][0] != cdc {
		t.Fatalf("offset calls: %v", calls)
	}

	// The group is reset past the log start: clears after connector_down.for (3m).
	h.lag.set(grp, "Stable", 1, cdc, 17_200_400, 17_200_500)
	h.step(time.Minute) // clearing since now
	h.steps(2, time.Minute)
	if h.phase("orders", owners.SignalOffsetBelowLogStart) != phaseFiring {
		t.Fatal("resolved before connector_down.for")
	}
	h.step(time.Minute)
	if h.phase("orders", owners.SignalOffsetBelowLogStart) != phaseOK {
		t.Fatal("did not resolve after connector_down.for")
	}
}

func TestOffsetBelowLogStartOffsetsErrorIsUnknown(t *testing.T) {
	h := newHarness(t, opts{lag: true})
	h.lag.set(grp, "Stable", 1, cdc, 100, 500)
	h.kafka.set(cdc, 0, 500)
	h.kafka.setErr(errors.New("kafka: broker down"))
	h.steps(5, time.Minute)
	if h.phase("orders", owners.SignalOffsetBelowLogStart) != phaseOK {
		t.Fatal("an offsets error fired the signal")
	}
	if h.logs.count("signal=offset_below_log_start err=\"kafka: broker down\"") == 0 {
		t.Fatalf("the error was not logged:\n%s", h.logs)
	}
	// Nothing committed (-1): no offsets call at all.
	h2 := newHarness(t, opts{lag: true})
	h2.lag.set(grp, "Stable", 1, cdc, -1, 500)
	h2.step(0)
	if len(h2.kafka.callLists()) != 0 {
		t.Fatalf("offsets read for a group with no commits: %v", h2.kafka.callLists())
	}
}

func TestLagReaderMissingLeavesSignalsUnknown(t *testing.T) {
	h := newHarness(t, opts{}) // no Deps.Lag
	h.steps(15, time.Minute)
	for _, sig := range []string{owners.SignalConsumerLag, owners.SignalGroupUnstable, owners.SignalOffsetBelowLogStart} {
		if h.phase("orders", sig) != phaseOK {
			t.Fatalf("%s left ok without a reader", sig)
		}
	}
	if n := h.logs.count("no consumer-group lag reader"); n != 1 {
		t.Fatalf("missing-reader line logged %d times, want 1", n)
	}
	if len(h.kafka.callLists()) != 0 {
		t.Fatalf("offsets read without a lag reader: %v", h.kafka.callLists())
	}
}

func TestGroupLagErrorKeepsState(t *testing.T) {
	h := newHarness(t, opts{lag: true})
	h.lag.set(grp, "Stable", 2, cdc, 0, 5000)
	h.steps(11, time.Minute)
	if h.phase("orders", owners.SignalConsumerLag) != phaseFiring {
		t.Fatal("not firing")
	}
	h.lag.setErr(grp, errors.New("kafka: coordinator not available"))
	h.steps(30, time.Minute)
	if h.phase("orders", owners.SignalConsumerLag) != phaseFiring {
		t.Fatal("an erroring group read cleared the signal")
	}
	if h.logs.count("coordinator not available") == 0 {
		t.Fatal("the error was not logged")
	}
}

func TestCrashLoopCounterFiresWhileRunning(t *testing.T) {
	h := newHarness(t, opts{})
	h.step(0) // RUNNING
	trace := "org.apache.kafka.connect.errors.ConnectException: MISSING_UNION_BRANCH\n\tat frame"
	for i := 0; i < 6; i++ { // F R F R F R: 3 restarts in 6 minutes
		if i%2 == 0 {
			h.breakConnector("orders", trace)
		} else {
			h.fixConnector("orders")
		}
		h.step(time.Minute)
	}
	// The 3rd restart starts the breach although the task reads RUNNING; it
	// keeps looping until `for` (3m) is over.
	for i := 0; i < 3; i++ {
		if i%2 == 0 {
			h.breakConnector("orders", trace)
		} else {
			h.fixConnector("orders")
		}
		h.step(time.Minute)
	}
	h.fixConnector("orders")
	h.step(time.Minute) // RUNNING now, 5 restarts in the window
	if h.phase("orders", owners.SignalConnectorDown) != phaseFiring {
		t.Fatal("a crash-looping task reading RUNNING did not fire connector_down")
	}
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("rooms = %d, want 1", len(rooms))
	}
	a := rooms[0].alert
	if !strings.Contains(a.Title, "task 0 crash-looping (") || !strings.Contains(a.Title, "restarts in 15m)") || a.Severity != "sev2" {
		t.Fatalf("title %q sev %q", a.Title, a.Severity)
	}
	r := h.w.pipes["orders"].sigs[owners.SignalConnectorDown].last
	conns := r.ev["connectors"].([]map[string]any)
	tasks := conns[0]["tasks"].([]map[string]any)
	if tasks[0]["state"] != "RUNNING" || tasks[0]["crash_loop"] != true || tasks[0]["trace"] != "org.apache.kafka.connect.errors.ConnectException: MISSING_UNION_BRANCH" {
		t.Fatalf("task evidence: %v", tasks[0])
	}
	if !strings.Contains(r.label, "crash-looping (5 restarts in 15m)") {
		t.Fatalf("label %q", r.label)
	}

	// The loop stops: once the restarts age out of the window it resolves.
	h.steps(25, time.Minute)
	if h.phase("orders", owners.SignalConnectorDown) != phaseOK {
		t.Fatal("did not resolve after the loop stopped")
	}
}

func TestCrashLoopNeedsEnoughRestarts(t *testing.T) {
	h := newHarness(t, opts{})
	for range 2 { // two restarts, 10 minutes apart: not a loop
		h.breakConnector("orders", "boom")
		h.step(time.Minute)
		h.fixConnector("orders")
		h.steps(10, time.Minute)
	}
	if h.phase("orders", owners.SignalConnectorDown) != phaseOK || len(h.rooms.created()) != 0 {
		t.Fatal("two restarts fired connector_down")
	}
	// An error tick between FAILED and RUNNING still counts the restart.
	h.breakConnector("orders", "boom")
	h.step(time.Minute)
	h.conn.setStatusErr("pg-orders", errors.New("connect: timeout"))
	h.step(time.Minute)
	h.fixConnector("orders")
	h.step(time.Minute)
	if n := restarts(h.w.pipes["orders"].taskHist["pg-orders/0"].events, h.clk.Now().Add(-15*time.Minute)); n != 2 {
		t.Fatalf("restarts in window = %d, want 2", n)
	}
}

func TestMaxOpenRoomsIsNotMaxAgents(t *testing.T) {
	h := newHarness(t, opts{})
	want, err := config.ParseWatchMaxOpenRooms(config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if h.w.maxRooms != want || want != 6 {
		t.Fatalf("default cap = %d, want PIPEROOM_WATCH_MAX_OPEN_ROOMS default %d (6)", h.w.maxRooms, want)
	}
	// With 4 pipelines firing on different ticks (no storm) and the default
	// cap, all 4 open: more than the default agent pool (3).
	pipes := []pipeSpec{{id: "a"}, {id: "b"}, {id: "c"}, {id: "d"}}
	h = newHarness(t, opts{pipes: pipes})
	for _, p := range pipes {
		h.breakConnector(p.id, "boom")
		h.steps(4, time.Minute)
	}
	if got := len(h.rooms.created()); got != 4 {
		t.Fatalf("rooms = %d, want 4", got)
	}
	// An explicit cap still holds.
	h = newHarness(t, opts{pipes: pipes, maxRooms: 2})
	for _, p := range pipes {
		h.breakConnector(p.id, "boom")
		h.steps(4, time.Minute)
	}
	if got := len(h.rooms.created()); got != 2 {
		t.Fatalf("rooms with cap 2 = %d, want 2", got)
	}
}

func TestPostEvidenceCarriesJSON(t *testing.T) {
	h := newHarness(t, opts{evidenceSink: true})
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	if len(h.rooms.created()) != 1 {
		t.Fatal("no room")
	}
	// A second signal joins the room with its evidence JSON.
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(6, time.Minute)
	posts := h.rooms.postsContaining("new signal")
	if len(posts) != 1 {
		t.Fatalf("joins = %d, want 1", len(posts))
	}
	if len(posts[0].ev) == 0 {
		t.Fatal("the join carried no evidence JSON")
	}
	var ev map[string]any
	if err := json.Unmarshal(posts[0].ev, &ev); err != nil {
		t.Fatalf("evidence is not JSON: %v", err)
	}
	if ev["source"] != "watcher" || ev["reason"] != "new signal" {
		t.Fatalf("evidence: %v", ev)
	}
	signalEv(t, ev, owners.SignalSlotInactive)
	// The text still holds the same JSON, for a sink that only reads text.
	if !strings.Contains(posts[0].text, string(posts[0].ev)) {
		t.Fatal("the text and the evidence differ")
	}

	// A join into a room someone else opened carries it too.
	h2 := newHarness(t, opts{evidenceSink: true})
	h2.rooms.foreign["orders"] = "r_am"
	h2.breakConnector("orders", "boom")
	h2.steps(4, time.Minute)
	posts = h2.rooms.postsContaining("joined an open room")
	if len(posts) != 1 || len(posts[0].ev) == 0 {
		t.Fatalf("foreign join: %+v", posts)
	}
	// The all-clear note in a joined room has no numbers.
	h2.fixConnector("orders")
	h2.steps(4, time.Minute)
	posts = h2.rooms.postsContaining("all watcher signals are clear")
	if len(posts) != 1 || posts[0].ev != nil {
		t.Fatalf("clear note: %+v", posts)
	}
}

// Each group is read once per tick, not once per signal.
func TestGroupReadOncePerTick(t *testing.T) {
	h := newHarness(t, opts{lag: true, pipes: []pipeSpec{{id: "a"}, {id: "b"}}})
	h.step(0)
	if n := h.lag.callCount(); n != 2 { // g-a and g-b
		t.Fatalf("GroupLag calls = %d, want 2", n)
	}
	h.step(time.Minute)
	if n := h.lag.callCount(); n != 4 {
		t.Fatalf("GroupLag calls after two ticks = %d, want 4", n)
	}
}
