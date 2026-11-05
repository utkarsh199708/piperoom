package watch

import (
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// ---- fake sample store ----

type fakeSamples struct {
	mu        sync.Mutex
	pts       map[string][]clients.SamplePoint // pipeline|metric
	appends   map[string]int
	appendErr error
	readErr   error
}

func newFakeSamples() *fakeSamples {
	return &fakeSamples{pts: map[string][]clients.SamplePoint{}, appends: map[string]int{}}
}

func (f *fakeSamples) AppendSample(ctx context.Context, pid, metric string, at time.Time, v float64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appends[pid+"|"+metric]++
	if f.appendErr != nil {
		return f.appendErr
	}
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return errors.New("fake: not finite")
	}
	f.pts[pid+"|"+metric] = append(f.pts[pid+"|"+metric], clients.SamplePoint{At: at, Value: v})
	return nil
}

func (f *fakeSamples) Samples(ctx context.Context, pid, metric string, since time.Time) ([]clients.SamplePoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readErr != nil {
		return nil, f.readErr
	}
	var out []clients.SamplePoint
	for _, p := range f.pts[pid+"|"+metric] {
		if !p.At.Before(since) {
			out = append(out, p)
		}
	}
	return out, nil
}

func (f *fakeSamples) count(pid, metric string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.appends[pid+"|"+metric]
}

func (f *fakeSamples) set(appendErr, readErr error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appendErr, f.readErr = appendErr, readErr
}

// withSamples rebuilds the harness's watcher with a sample store.
func withSamples(t *testing.T, h *harness, fs *fakeSamples) {
	t.Helper()
	h.deps.Samples = fs
	w, err := New(h.deps)
	if err != nil {
		t.Fatal(err)
	}
	h.w = w
}

// series is n samples 15 min apart ending at end, value f(i).
func sampleSeries(end time.Time, n int, f func(i int) float64) []clients.SamplePoint {
	out := make([]clients.SamplePoint, n)
	for i := range n {
		out[i] = clients.SamplePoint{At: end.Add(-time.Duration(n-1-i) * config.WatchSampleInterval), Value: f(i)}
	}
	return out
}

// ---- TC2: the forecast math ----

func TestForecastReading(t *testing.T) {
	const gb = 1e9
	now := t0
	h := 48 * time.Hour
	// 1 GB/h: a sample every 15 min adds 0.25 GB; the last one is 60 GB.
	linear := sampleSeries(now, 96, func(i int) float64 { return 60*gb - float64(95-i)*0.25*gb })
	// The same trend with large alternating noise: R² about 0.3.
	noisy := sampleSeries(now, 96, func(i int) float64 {
		v := 60*gb - float64(95-i)*0.25*gb
		if i%2 == 0 {
			return v + 10.5*gb
		}
		return v - 10.5*gb
	})
	if f, _ := fitLine(noisy); f.r2 < 0.2 || f.r2 > 0.4 {
		t.Fatalf("noisy fixture R² = %.2f, want about 0.3", f.r2)
	}
	cases := []struct {
		name    string
		pts     []clients.SamplePoint
		horizon time.Duration
		want    verdict
	}{
		{"linear, ETA 40 h inside 48 h", linear, h, vBreach},
		{"linear, ETA 40 h outside 24 h holds", linear, 24 * time.Hour, vHold},
		{"linear, ETA 40 h past twice 12 h clears", linear, 12 * time.Hour, vClear},
		{"noisy R² 0.3", noisy, h, vClear},
		{"flat", sampleSeries(now, 96, func(int) float64 { return 60 * gb }), h, vClear},
		{"shrinking", sampleSeries(now, 96, func(i int) float64 { return 90*gb - float64(i)*gb }), h, vClear},
		{"7 samples is too few", linear[len(linear)-7:], h, vHold},
		{"8 samples is enough", linear[len(linear)-8:], h, vBreach},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := forecastReading(now, clients.WatchMetricSlotWALBytes, "orders_slot", c.pts, 100*gb, c.horizon)
			if r.v != c.want {
				t.Fatalf("verdict = %v, want %v (value %s)", r.v, c.want, r.value)
			}
			if r.v != vBreach {
				if r.ev != nil {
					t.Fatalf("a non-firing reading carries evidence: %v", r.ev)
				}
				return
			}
			eta, _ := time.Parse(time.RFC3339, r.ev["eta"].(string))
			if d := eta.Sub(now); d < 39*time.Hour || d > 41*time.Hour {
				t.Fatalf("eta in %s, want 40h", d)
			}
			if r.ev["signal"] != events.SignalForecast || r.ev["severity"] != "sev4" || r.ev["metric"] != clients.WatchMetricSlotWALBytes ||
				r.ev["object"] != "orders_slot" || r.ev["limit"] != 100*gb || r.ev["current"] != 60*gb {
				t.Fatalf("evidence = %v", r.ev)
			}
			if s := r.ev["slope_per_hour"].(float64); math.Abs(s-gb) > 1 {
				t.Fatalf("slope = %v, want 1 GB/h", s)
			}
			if r.ev["r2"] != 1.0 || int(r.ev["samples"].(float64)) != len(c.pts) {
				t.Fatalf("fit = %v", r.ev)
			}
			// E7: the contract keys and nothing else.
			want := []string{"current", "eta", "limit", "metric", "object", "r2", "samples", "severity", "signal", "slope_per_hour"}
			if got := sortedKeys(r.ev); !slices.Equal(got, want) {
				t.Fatalf("keys = %v", got)
			}
			if !strings.Contains(r.label, "orders_slot") || !strings.Contains(r.label, "in ~40h") {
				t.Fatalf("label = %q", r.label)
			}
		})
	}
	// A line already past the limit says "now", never a time in the past.
	over := sampleSeries(now, 8, func(i int) float64 { return 101*gb + float64(i)*gb })
	if r := forecastReading(now, clients.WatchMetricSlotWALBytes, "s", over, 100*gb, h); r.v != vBreach || r.ev["eta"] != now.Format(time.RFC3339) {
		t.Fatalf("past limit: %v %v", r.v, r.ev["eta"])
	}
}

// TC3: oldest unread record 6 days old with 7 days of retention, rising one
// hour per hour (a stuck consumer): data loss in under a day.
func TestForecastTopicRetention(t *testing.T) {
	day := 24 * 3600.0
	pts := sampleSeries(t0, 16, func(i int) float64 { return 6*day - float64(15-i)*900 })
	r := forecastReading(t0, clients.WatchMetricTopicOldestUnreadAge, "orders.public.t", pts, 7*day, 48*time.Hour)
	if r.v != vBreach {
		t.Fatalf("verdict = %v", r.v)
	}
	eta, _ := time.Parse(time.RFC3339, r.ev["eta"].(string))
	if d := eta.Sub(t0); d > 24*time.Hour || d < 23*time.Hour {
		t.Fatalf("eta in %s, want under 1 day", d)
	}
	if !strings.Contains(r.label, "data loss") || !strings.Contains(r.label, "orders.public.t") {
		t.Fatalf("label = %q", r.label)
	}
}

func TestSlotLimit(t *testing.T) {
	cases := []struct {
		retained int64
		safe     *int64
		headroom int64
		want     float64
	}{
		{10, nil, 0, 0},
		{10, nil, 100, 100},
		{10, ptr(int64(50)), 0, 60},
		{10, ptr(int64(50)), 40, 40},
		{10, ptr(int64(50)), 100, 60},
		{10, ptr(int64(-5)), 0, 0},
	}
	for _, c := range cases {
		if got := slotLimit(clients.ReplicationSlot{RetainedWALBytes: c.retained, SafeWALBytes: c.safe}, c.headroom); got != c.want {
			t.Errorf("slotLimit(%+v) = %v, want %v", c, got, c.want)
		}
	}
}

// ---- watcher: sampling, rooms, cooldown ----

const forecastPipe = "    watch:\n      wal_headroom_bytes: 100GiB\n      signals:\n        slot_wal: {fire: 95GiB, clear: 90GiB}\n"

// TC6: a 1 s tick writes at most one sample per metric per 15 minutes.
func TestSamplesNoWriteStorm(t *testing.T) {
	h := newHarness(t, opts{lag: true})
	fs := newFakeSamples()
	withSamples(t, h, fs)
	h.steps(1801, time.Second) // 30 min 1 s: samples at 1 s, 15 min 1 s, 30 min 1 s
	for _, m := range []string{clients.WatchMetricSlotWALBytes, clients.WatchMetricGroupLag} {
		if n := fs.count("orders", m); n != 3 {
			t.Errorf("%s: %d samples in 30 min of 1 s ticks, want 3", m, n)
		}
	}
	// No DLQ topic, no db size reader, no unread-age reader: nothing written.
	for _, m := range []string{clients.WatchMetricDLQRecords, clients.WatchMetricDBSizeBytes, clients.WatchMetricTopicOldestUnreadAge} {
		if n := fs.count("orders", m); n != 0 {
			t.Errorf("%s: %d samples, want 0", m, n)
		}
	}
	if len(h.rooms.created()) != 0 {
		t.Fatal("a flat pipeline opened a room")
	}
}

// growSlot runs n ticks 15 min apart, adding 0.25 GiB (1 GiB/h) to the slot each tick.
func growSlot(h *harness, from *int64, n int) {
	for range n {
		*from += gib / 4
		h.src.setRetained("debezium_orders", *from)
		h.step(config.WatchSampleInterval)
	}
}

// TC4, E1, E7: a forecast opens one quiet sev4 room under its own key; later
// ticks join it instead of opening another; the real slot_wal signal joins the
// same room and raises its severity.
func TestForecastRoomThenRealSignalJoins(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders", watch: forecastPipe}}, evidenceSink: true})
	fs := newFakeSamples()
	withSamples(t, h, fs)
	retained := 55 * gib
	growSlot(h, &retained, 7)
	if len(h.rooms.created()) != 0 {
		t.Fatalf("fired with fewer than %d samples", config.WatchForecastMinSamples)
	}
	growSlot(h, &retained, 1)
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("rooms = %d, want 1\n%s", len(rooms), h.logs)
	}
	a := rooms[0].alert
	if a.DedupKey != ForecastDedupKey("orders", clients.WatchMetricSlotWALBytes) || a.Severity != "sev4" ||
		!strings.Contains(a.Title, "forecast: slot debezium_orders WAL reaches 100.0 GB") {
		t.Fatalf("alert = %+v", a)
	}
	ev := signalEv(t, evidenceOf(t, a.Raw), events.SignalForecast)
	if ev["metric"] != clients.WatchMetricSlotWALBytes || ev["object"] != "debezium_orders" || ev["limit"] != float64(100*gib) {
		t.Fatalf("evidence = %v", ev)
	}
	for k := range ev {
		switch k {
		case "signal", "severity", "metric", "object", "current", "limit", "eta", "slope_per_hour", "r2", "samples", "since", "firing_for_seconds":
		default:
			t.Errorf("unexpected evidence key %q", k)
		}
	}

	// Many more ticks: still one room.
	growSlot(h, &retained, 40)
	if n := len(h.rooms.created()); n != 1 {
		t.Fatalf("rooms = %d after more ticks, want 1", n)
	}
	// The real signal: past 95 GiB for its 10 minutes.
	for retained < 96*gib {
		growSlot(h, &retained, 1)
	}
	growSlot(h, &retained, 1)
	if n := len(h.rooms.created()); n != 1 {
		t.Fatalf("the real signal opened a second room (%d)", n)
	}
	joined := h.rooms.postsContaining("new signal")
	if len(joined) == 0 || joined[len(joined)-1].roomID != rooms[0].roomID {
		t.Fatalf("slot_wal did not join the forecast room\n%s", h.logs)
	}
	sig := signalEv(t, evidenceOf(t, joined[len(joined)-1].ev), owners.SignalSlotWAL)
	if sig["severity"] == "sev4" {
		t.Fatalf("slot_wal joined at sev4: %v", sig)
	}
}

// TC4, X3c TC4: closing a forecast room starts that metric's cooldown only; a
// real signal on the pipeline still opens a room.
func TestForecastRoomCloseKeepsRealSignals(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders", watch: forecastPipe}}})
	fs := newFakeSamples()
	withSamples(t, h, fs)
	retained := 55 * gib
	growSlot(h, &retained, 8)
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("rooms = %d", len(rooms))
	}
	now := h.clk.Now()
	h.rooms.closeRoom(rooms[0].roomID, now)
	h.w.OnRoomClosed(rooms[0].roomID, "orders", rooms[0].alert.DedupKey)
	if p := h.phase("orders", forecastID(clients.WatchMetricSlotWALBytes)); p != phaseAcked {
		t.Fatalf("forecast phase = %v, want acked", p)
	}
	if !h.w.forecastCooling(now, "orders", clients.WatchMetricSlotWALBytes) || h.w.forecastCooling(now, "orders", clients.WatchMetricTopicOldestUnreadAge) {
		t.Fatal("cooldown is not per metric")
	}
	growSlot(h, &retained, 3) // inside the 60 min cooldown (15 min samples)
	if n := len(h.rooms.created()); n != 1 {
		t.Fatalf("an acked forecast opened another room (%d)", n)
	}
	// A real signal right after: not muted by the forecast room's close.
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	rooms = h.rooms.created()
	if len(rooms) != 2 || rooms[1].alert.DedupKey != DedupKey("orders") {
		t.Fatalf("connector_down after a closed forecast room: %+v\n%s", rooms, h.logs)
	}
}

// E3: a store that cannot write or read logs one warning, keeps the
// forecast state, and is not retried every tick.
func TestSampleStoreErrors(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders", watch: forecastPipe}}})
	fs := newFakeSamples()
	withSamples(t, h, fs)
	fs.set(errors.New("store: append sample: connection refused"), nil)
	h.steps(60, time.Minute)
	if n := fs.count("orders", clients.WatchMetricSlotWALBytes); n != 4 {
		t.Fatalf("append attempts in 1 h of 1 min ticks = %d, want 4", n)
	}
	if n := warnings(h, "connection refused"); n != 1 {
		t.Fatalf("warnings = %d, want 1\n%s", n, h.logs)
	}

	// Growth recorded, then the read side fails: no fire, one warning.
	fs.set(nil, nil)
	retained := 55 * gib
	growSlot(h, &retained, 7)
	fs.set(nil, errors.New("store: list samples: timeout"))
	growSlot(h, &retained, 3)
	if len(h.rooms.created()) != 0 || h.phase("orders", forecastID(clients.WatchMetricSlotWALBytes)) != phaseOK {
		t.Fatal("a read error fired the forecast")
	}
	if n := warnings(h, "list samples: timeout"); n != 1 {
		t.Fatalf("read warnings = %d, want 1", n)
	}
	fs.set(nil, nil)
	growSlot(h, &retained, 1)
	if len(h.rooms.created()) != 1 {
		t.Fatal("the forecast did not fire once the store answered again")
	}
}

// unreadLag is a lag reader that also knows the oldest unread record and the
// topic retention (the optional readers of forecast.go).
type unreadLag struct {
	*fakeLag
	oldest    time.Time
	topic     string
	retention string
}

func (u *unreadLag) OldestUnread(ctx context.Context, group string) (map[string]time.Time, error) {
	return map[string]time.Time{u.topic: u.oldest, "other.topic": u.oldest.Add(-30 * 24 * time.Hour)}, nil
}

func (u *unreadLag) DescribeTopicConfigs(ctx context.Context, topic string) (clients.TopicConfig, error) {
	if topic != u.topic {
		return clients.TopicConfig{}, errors.New("fake: wrong topic")
	}
	return clients.TopicConfig{Topic: topic, RetentionMS: u.retention}, nil
}

// TC3 end to end: the stuck consumer's unread records near retention open a
// forecast room; a topic outside the pipeline is never looked at.
func TestForecastTopicRetentionRoom(t *testing.T) {
	h := newHarness(t, opts{lag: true})
	ts := h.w.targets(context.Background())
	if len(ts) != 1 || len(ts[0].cdcTopics) == 0 {
		t.Fatalf("targets = %+v", ts)
	}
	ul := &unreadLag{fakeLag: h.lag, topic: ts[0].cdcTopics[0], oldest: t0.Add(-6 * 24 * time.Hour), retention: "604800000"}
	h.deps.Lag = ul
	fs := newFakeSamples()
	withSamples(t, h, fs)
	h.steps(8, config.WatchSampleInterval)
	rooms := h.rooms.created()
	if len(rooms) != 1 || rooms[0].alert.DedupKey != ForecastDedupKey("orders", clients.WatchMetricTopicOldestUnreadAge) {
		t.Fatalf("rooms = %+v\n%s", rooms, h.logs)
	}
	ev := signalEv(t, evidenceOf(t, rooms[0].alert.Raw), events.SignalForecast)
	if ev["object"] != ul.topic || ev["limit"] != 604800.0 || !strings.Contains(rooms[0].alert.Title, "data loss") {
		t.Fatalf("evidence = %v title %q", ev, rooms[0].alert.Title)
	}
}

// The watcher stops with its context while sampling (run with -race).
func TestRunStopsWithSamples(t *testing.T) {
	h := newHarness(t, opts{lag: true})
	withSamples(t, h, newFakeSamples())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.w.Run(ctx) }()
	for range 5 {
		h.clk.Advance(config.WatchSampleInterval)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// warnings counts the WARN lines that contain s.
func warnings(h *harness, s string) int {
	n := 0
	for _, l := range strings.Split(h.logs.String(), "\n") {
		if strings.Contains(l, "level=WARN") && strings.Contains(l, s) {
			n++
		}
	}
	return n
}
