package watch

// The X1e/X1g silent-corruption breaks (cmd/devctl/scenarios_silent.go,
// dev/scenarios.md sections 14 to 17) as table states (X1b-fix, KAN-416).
// A small model of the pipeline (a source table, a consumer that loads it
// into a sink table, a CDC topic) produces the state each break leaves, on
// an idle stack and under load, and the real watcher reads it through the
// optional readers. The negative tests keep a healthy pipeline quiet and
// never judge rows that are still in flight.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/events"
)

const (
	mSource = "public.orders"
	mSink   = "analytics.orders"
	mTopic  = "orders.public.orders"
	mTS     = "updated_at"
)

// mRow is one row: its sink_ts_column (zero = NULL) and its other columns.
type mRow struct {
	ts   time.Time
	cols map[string]string
}

// modelDB is one side of the pipeline: one table, with every optional reader
// the watcher asserts (Reconciler, the primary key, Freshness, ColumnReader,
// ToastColumnReader on the source, PlaceholderCounter on the sink).
type modelDB struct {
	mu      sync.Mutex
	table   string
	rows    map[string]mRow
	columns []clients.ColumnInfo
	toast   []clients.ToastColumn
	phCols  []string // CountPlaceholder's columns, per call
	phErr   error
}

func newModelDB(table string, cols ...string) *modelDB {
	m := &modelDB{table: table, rows: map[string]mRow{}}
	for i, c := range cols {
		typ := "text"
		if strings.HasSuffix(c, "_at") {
			typ = "timestamp with time zone"
		}
		m.columns = append(m.columns, clients.ColumnInfo{Name: c, Type: typ, Position: i + 1, Nullable: true})
	}
	return m
}

func (m *modelDB) check(table string) error {
	if table != m.table {
		return fmt.Errorf("model: table %s: %w", table, clients.ErrNotFound)
	}
	return nil
}

func in(at, from, to time.Time) bool { return !at.IsZero() && !at.Before(from) && at.Before(to) }

func (m *modelDB) CountBetween(ctx context.Context, table, ts string, from, to time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(table); err != nil {
		return 0, err
	}
	var n int64
	for _, r := range m.rows {
		if in(r.ts, from, to) {
			n++
		}
	}
	return n, nil
}

func (m *modelDB) SampleKeys(ctx context.Context, table, pk, ts string, since, until time.Time, n int) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(table); err != nil {
		return nil, err
	}
	var keys []string
	for k, r := range m.rows {
		if in(r.ts, since, until) {
			keys = append(keys, k)
		}
	}
	slices.SortFunc(keys, func(a, b string) int { // newest first
		if c := m.rows[b].ts.Compare(m.rows[a].ts); c != 0 {
			return c
		}
		return strings.Compare(a, b)
	})
	return keys[:min(len(keys), clients.ReconcileSampleSize(n))], nil
}

func (m *modelDB) RowTimes(ctx context.Context, table, pk string, keys, cols []string) (map[string]map[string]time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(table); err != nil {
		return nil, err
	}
	out := map[string]map[string]time.Time{}
	for _, k := range keys {
		r, ok := m.rows[k]
		if !ok {
			continue
		}
		times := map[string]time.Time{}
		if !r.ts.IsZero() && slices.Contains(cols, mTS) {
			times[mTS] = r.ts
		}
		out[k] = times
	}
	return out, nil
}

func (m *modelDB) PrimaryKeyColumn(ctx context.Context, table string) (string, error) {
	return "id", nil
}

func (m *modelDB) Freshness(ctx context.Context, table, ts string) (time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(table); err != nil {
		return time.Time{}, err
	}
	var newest time.Time
	for _, r := range m.rows {
		if r.ts.After(newest) {
			newest = r.ts
		}
	}
	return newest, nil
}

func (m *modelDB) Row(ctx context.Context, table, pk, key string) (map[string]any, bool, error) {
	return nil, false, errors.New("model: the watcher must never read a whole row")
}

func (m *modelDB) Columns(ctx context.Context, table string) ([]clients.ColumnInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(table); err != nil {
		return nil, err
	}
	return slices.Clone(m.columns), nil
}

func (m *modelDB) dropColumn(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.columns = slices.DeleteFunc(m.columns, func(c clients.ColumnInfo) bool { return c.Name == name })
}

func (m *modelDB) ToastColumns(ctx context.Context, tables []string) ([]clients.ToastTable, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []clients.ToastTable
	for _, t := range tables {
		out = append(out, clients.ToastTable{Table: t, Found: t == m.table, ReplicaIdentity: "DEFAULT", ToastBytes: 8192,
			Columns: slices.Clone(m.toast)})
	}
	return out, false, nil
}

func (m *modelDB) CountPlaceholder(ctx context.Context, table, column, placeholder, ts string, since, until time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.phCols = append(m.phCols, column)
	if m.phErr != nil {
		return 0, m.phErr
	}
	if err := m.check(table); err != nil {
		return 0, err
	}
	if placeholder != debeziumPlaceholder || !until.After(since) || until.Sub(since) > clients.MaxPlaceholderWindow {
		return 0, fmt.Errorf("model: bad placeholder call: %w", clients.ErrNotSupported)
	}
	var n int64
	for _, r := range m.rows {
		if in(r.ts, since, until) && r.cols[column] == placeholder {
			n++
		}
	}
	return n, nil
}

func (m *modelDB) placeholderCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.phCols)
}

// modelSource is the watcher's source: replication slots from the harness,
// the table from the model.
type modelSource struct {
	*fakeSource
	*modelDB
}

// mRecord is one change event on the CDC topic.
type mRecord struct {
	key string
	lsn int64
	at  time.Time
}

// modelTopic is the Kafka client's clients.DupScanner over the model's CDC
// topic: a duplicate is a (key, lsn) seen earlier in the scanned window, a
// regression an lsn below the key's highest so far.
type modelTopic struct {
	mu   sync.Mutex
	recs []mRecord
}

func (m *modelTopic) DupScan(ctx context.Context, topic string, since time.Time, maxScan int) (clients.DupReport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rep := clients.DupReport{Topic: topic, MaxScan: maxScan}
	seen, top := map[[2]any]bool{}, map[string]int64{}
	dupKeys, regKeys := map[string]bool{}, map[string]bool{}
	for _, r := range m.recs {
		if r.at.Before(since) || rep.Scanned == maxScan {
			continue
		}
		rep.Scanned++
		id := [2]any{r.key, r.lsn}
		if seen[id] {
			rep.Duplicates++
			dupKeys[r.key] = true
		} else if r.lsn < top[r.key] {
			rep.OrderRegressions++
			regKeys[r.key] = true
		}
		seen[id] = true
		top[r.key] = max(top[r.key], r.lsn)
	}
	rep.Keys, rep.RegressionKeys = len(dupKeys), len(regKeys)
	return rep, nil
}

type modelKafka struct {
	*fakeKafka
	*modelTopic
}

// pipeModel is the pipeline: rows written to the source go through the
// consumer (load) to the sink, now or later (in flight), and each write is a
// change event on the topic.
type pipeModel struct {
	src, snk *modelDB
	topic    *modelTopic
	// load is the consumer: what the sink stores for a source row, false
	// when the row is skipped. A break replaces it.
	load    func(r mRow) (mRow, bool)
	pending []string
	next    int
	lsn     int64
}

func healthyLoad(r mRow) (mRow, bool) { return r, true }

func (m *pipeModel) deliver(key string) {
	m.src.mu.Lock()
	r := m.src.rows[key]
	m.src.mu.Unlock()
	out, ok := m.load(mRow{ts: r.ts, cols: mapsClone(r.cols)})
	if !ok {
		return
	}
	m.snk.mu.Lock()
	m.snk.rows[key] = out
	m.snk.mu.Unlock()
}

func mapsClone(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// deliverPending loads every row still in flight.
func (m *pipeModel) deliverPending() {
	for _, k := range m.pending {
		m.deliver(k)
	}
	m.pending = nil
}

// put writes a source row (a new one when key is "") at ts and emits its
// change event; deliver says whether the consumer loads it now.
func (m *pipeModel) put(key string, ts time.Time, cols map[string]string, deliver bool) string {
	if key == "" {
		key = fmt.Sprintf("secret-key-%05d", m.next)
		m.next++
	}
	m.src.mu.Lock()
	r, ok := m.src.rows[key]
	if !ok {
		r = mRow{cols: map[string]string{"status": "placed", "amount": "100"}}
	}
	r.ts = ts
	for k, v := range cols {
		r.cols[k] = v
	}
	m.src.rows[key] = r
	m.src.mu.Unlock()
	m.lsn++
	m.topic.mu.Lock()
	m.topic.recs = append(m.topic.recs, mRecord{key: key, lsn: m.lsn, at: ts})
	m.topic.mu.Unlock()
	if deliver {
		m.deliver(key)
	} else {
		m.pending = append(m.pending, key)
	}
	return key
}

// write puts n new rows at ts.
func (m *pipeModel) write(n int, ts time.Time, cols map[string]string) []string {
	var keys []string
	for range n {
		keys = append(keys, m.put("", ts, cols, true))
	}
	return keys
}

// traffic delivers what was in flight, then writes n rows spread over
// [from, to); the rows of the last inFlight before to stay in flight.
func (m *pipeModel) traffic(from, to time.Time, n int, inFlight time.Duration) {
	m.deliverPending()
	step := to.Sub(from) / time.Duration(n)
	for i := range n {
		at := from.Add(time.Duration(i)*step + 7*time.Millisecond)
		m.put("", at, nil, !at.After(to.Add(-inFlight)))
	}
}

// modelHarness runs the real watcher on a pipeModel.
type modelHarness struct {
	*harness
	m *pipeModel
}

func newModelHarness(t *testing.T) *modelHarness {
	t.Helper()
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders", tables: "[" + mSource + "]",
		watch: "    sink_tables: [" + mSink + "]\n    sink_ts_column: " + mTS + "\n"}}})
	m := &pipeModel{
		src:   newModelDB(mSource, "id", "customer_id", "amount", "status", mTS),
		snk:   newModelDB(mSink, "id", "customer_id", "amount", "status", mTS, "loaded_at"),
		topic: &modelTopic{},
		load:  healthyLoad,
	}
	m.src.toast = []clients.ToastColumn{{Name: "status", Type: "text", Storage: "x", AvgWidth: 12}}
	// Two hours of history, all loaded.
	for i := range 120 {
		m.put("", t0.Add(-3*time.Hour+time.Duration(i)*time.Minute+3*time.Millisecond), nil, true)
	}
	d := h.deps
	d.Source = modelSource{h.src, m.src}
	d.Warehouse = m.snk
	d.Kafka = modelKafka{h.kafka, m.topic}
	w, err := New(d)
	if err != nil {
		t.Fatal(err)
	}
	h.w, h.deps = w, d
	return &modelHarness{harness: h, m: m}
}

// run steps n reconcile intervals; with load, traffic runs during each.
func (h *modelHarness) run(n int, load bool) {
	h.t.Helper()
	for range n {
		now := h.clk.Now()
		if load {
			h.m.traffic(now, now.Add(15*time.Minute), 30, time.Minute)
		}
		h.step(15 * time.Minute)
	}
}

// fired is every signal id that fired, from the watcher's log lines.
func (h *modelHarness) fired() []string {
	var out []string
	sc := bufio.NewScanner(strings.NewReader(h.logs.String()))
	for sc.Scan() {
		line := sc.Text()
		if !strings.Contains(line, `msg="watch: signal fired"`) {
			continue
		}
		_, rest, _ := strings.Cut(line, " signal=")
		id, _, _ := strings.Cut(rest, " ")
		if !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	return out
}

// evidence is a firing signal's current evidence entry.
func (h *modelHarness) evidence(id string) map[string]any {
	h.t.Helper()
	h.w.mu.Lock()
	defer h.w.mu.Unlock()
	ss := h.w.pipes["orders"].sigs[id]
	if ss == nil || ss.last.ev == nil {
		h.t.Fatalf("%s has no evidence\n%s", id, h.logs)
	}
	return ss.last.ev
}

func (h *modelHarness) label(id string) string {
	h.w.mu.Lock()
	defer h.w.mu.Unlock()
	return h.w.pipes["orders"].sigs[id].last.label
}

var (
	countID = driftID(events.SignalCountDrift, mSource)
	rowID   = driftID(events.SignalRowDrift, mSource)
	dupID   = driftID(events.SignalDupKeys, mTopic)
	skewID  = driftID(events.SignalOrderSkew, mTopic)
)

func (h *modelHarness) wantFired(ids ...string) {
	h.t.Helper()
	got := h.fired()
	for _, id := range ids {
		if !slices.Contains(got, id) {
			h.t.Fatalf("%s did not fire; fired %v\n%s", id, got, h.logs)
		}
	}
}

func (h *modelHarness) wantQuiet() {
	h.t.Helper()
	if got := h.fired(); len(got) != 0 || len(h.rooms.created()) != 0 {
		h.t.Fatalf("a healthy pipeline fired %v (%d rooms)\n%s", got, len(h.rooms.created()), h.logs)
	}
	assertNoKeys(h.t, &driftHarness{harness: h.harness})
}

// ---- section 14: sink-filtered-rows ----

// filterBackfill is the release whose filter drops status=backfill rows.
func filterBackfill(r mRow) (mRow, bool) { return r, r.cols["status"] != "backfill" }

func breakFiltered(h *modelHarness) {
	now := h.clk.Now()
	h.m.deliverPending()
	h.m.load = filterBackfill
	h.m.write(40, now.Add(time.Minute), map[string]string{"status": "backfill"})
	h.m.write(1, now.Add(time.Minute+time.Millisecond), nil) // the marker: loaded, so the sink's newest is after the skipped rows
}

func TestScenarioSinkFilteredRowsIdle(t *testing.T) {
	h := newModelHarness(t)
	h.step(0)
	breakFiltered(h)
	h.run(8, false) // two idle hours: the old windows slid away from the break
	h.wantFired(countID, rowID)
	ev := h.evidence(countID)
	if ev["missing"] != float64(40) || ev["source"] != float64(41) || ev["sink"] != float64(1) {
		t.Fatalf("count_drift evidence %v", ev)
	}
	if !strings.Contains(ev["detail"].(string), "sink idle") {
		t.Fatalf("detail %q does not say the window ends at the sink's newest row", ev["detail"])
	}
	if ev := h.evidence(rowID); ev["missing"] != float64(40) {
		t.Fatalf("row_drift evidence %v", ev)
	}
	assertNoKeys(t, &driftHarness{harness: h.harness})
}

func TestScenarioSinkFilteredRowsUnderLoad(t *testing.T) {
	h := newModelHarness(t)
	h.run(1, true)
	breakFiltered(h)
	h.run(8, true)
	h.wantFired(countID, rowID)
}

// ---- section 15: sink-dropped-column ----

// dropTS is the release whose upsert no longer writes updated_at.
func dropTS(r mRow) (mRow, bool) { r.ts = time.Time{}; return r, true }

func TestScenarioSinkDroppedColumnIdle(t *testing.T) {
	h := newModelHarness(t)
	h.step(0)
	h.m.load = dropTS
	// Late-arriving rows at exactly the sink's newest updated_at (so health
	// cannot see them), loaded with it NULL.
	newest, _ := h.m.snk.Freshness(context.Background(), mSink, mTS)
	for range 20 {
		h.m.put("", newest, nil, true)
	}
	h.run(4, false)
	h.wantFired(rowID, countID)
	ev := h.evidence(rowID)
	if ev["mismatched"] != float64(20) || ev["missing"] != float64(0) {
		t.Fatalf("row_drift evidence %v", ev)
	}
	if ev := h.evidence(countID); ev["missing"] != float64(20) {
		t.Fatalf("count_drift evidence %v", ev)
	}
}

// Under load every new row lands with updated_at NULL: the sink's newest row
// stays at the break and looks like a growing lag, so the shifted windows
// only ever see rows from before it. The landed-row probe sees the NULLs.
func TestScenarioSinkDroppedColumnUnderLoad(t *testing.T) {
	h := newModelHarness(t)
	h.run(1, true)
	h.m.deliverPending()
	h.m.load = dropTS
	h.run(2, true)
	h.wantFired(rowID)
	if l := h.label(rowID); !strings.Contains(l, "updated_at NULL on the sink") {
		t.Fatalf("label %q", l)
	}
	if ev := h.evidence(rowID); ev["mismatched"].(float64) < 1 {
		t.Fatalf("row_drift evidence %v", ev)
	}
}

// The column itself gone from the sink's catalog: a structural breach, with
// no other read (each keys on the column).
func TestScenarioSinkColumnDroppedFromCatalog(t *testing.T) {
	h := newModelHarness(t)
	h.step(0)
	h.m.snk.dropColumn(mTS)
	h.step(15 * time.Minute)
	h.wantFired(rowID)
	if l := h.label(rowID); !strings.Contains(l, "1 column missing on the sink (updated_at)") {
		t.Fatalf("label %q", l)
	}

	// A column both sides had at the first read, gone from the sink later.
	h2 := newModelHarness(t)
	h2.step(0)
	h2.m.snk.dropColumn("status")
	h2.step(15 * time.Minute)
	h2.wantFired(rowID)
	if ev := h2.evidence(rowID); !strings.Contains(ev["detail"].(string), "columns missing on the sink: status") {
		t.Fatalf("evidence %v", ev)
	}
	// A column only the sink has (loaded_at) is a projection, never drift.
	h3 := newModelHarness(t)
	h3.m.snk.dropColumn("loaded_at")
	h3.step(0)
	h3.step(15 * time.Minute)
	h3.wantQuiet()
}

// ---- section 16: consumer-duplicates ----

func breakDuplicates(h *modelHarness) {
	now := h.clk.Now()
	keys := h.m.write(10, now.Add(time.Minute), nil)
	h.m.topic.mu.Lock()
	defer h.m.topic.mu.Unlock()
	var copies []mRecord
	for _, r := range h.m.topic.recs {
		if slices.Contains(keys, r.key) {
			r.at = now.Add(2 * time.Minute) // republished: same key, same lsn
			copies = append(copies, r)
		}
	}
	h.m.topic.recs = append(h.m.topic.recs, copies...)
}

func TestScenarioConsumerDuplicates(t *testing.T) {
	for _, load := range []bool{false, true} {
		t.Run(fmt.Sprint("load=", load), func(t *testing.T) {
			h := newModelHarness(t)
			h.run(1, load)
			breakDuplicates(h)
			h.run(2, load)
			h.wantFired(dupID)
			if slices.Contains(h.fired(), skewID) {
				t.Fatal("a duplicate was read as an order regression")
			}
			if slices.Contains(h.fired(), countID) || slices.Contains(h.fired(), rowID) {
				t.Fatalf("the upsert absorbs the copies, yet %v fired", h.fired())
			}
		})
	}
}

// ---- section 17: toast-placeholder ----

// placeholderOnUpdate is Debezium under REPLICA IDENTITY DEFAULT: an update
// that leaves the TOASTed status unchanged carries the placeholder.
func placeholderOnUpdate(r mRow) (mRow, bool) {
	if strings.HasPrefix(r.cols["status"], "toast-big-") && r.cols["amount"] == "101" {
		r.cols["status"] = debeziumPlaceholder
	}
	return r, true
}

func breakToast(h *modelHarness) {
	now := h.clk.Now()
	h.m.deliverPending()
	keys := h.m.write(3, now.Add(time.Minute), map[string]string{"status": "toast-big-0123456789abcdef"})
	h.m.load = placeholderOnUpdate
	for _, k := range keys {
		h.m.put(k, now.Add(2*time.Minute), map[string]string{"amount": "101"}, true)
	}
}

func TestScenarioToastPlaceholder(t *testing.T) {
	for _, load := range []bool{false, true} {
		t.Run(fmt.Sprint("load=", load), func(t *testing.T) {
			h := newModelHarness(t)
			h.run(1, load)
			breakToast(h)
			h.run(1, load)
			h.wantFired(rowID)
			ev := h.evidence(rowID)
			if ev["mismatched"] != float64(3) || ev["missing"] != float64(0) {
				t.Fatalf("row_drift evidence %v", ev)
			}
			if d := ev["detail"].(string); !strings.Contains(d, "placeholder in status: 3 rows") {
				t.Fatalf("detail %q", d)
			}
			if slices.Contains(h.fired(), countID) {
				t.Fatal("counts agree, yet count_drift fired")
			}
			assertNoKeys(t, &driftHarness{harness: h.harness})
		})
	}
}

// ---- negatives: a healthy pipeline stays quiet, rows in flight are not judged ----

func TestScenarioHealthyIdleStaysQuiet(t *testing.T) {
	h := newModelHarness(t)
	h.run(12, false)
	h.wantQuiet()
	if len(h.m.snk.placeholderCalls()) == 0 {
		t.Fatal("the placeholder check never ran")
	}
}

func TestScenarioHealthyUnderLoadStaysQuiet(t *testing.T) {
	h := newModelHarness(t)
	h.run(12, true)
	h.wantQuiet()
}

// The consumer stops: the source keeps writing, nothing new reaches the
// sink. The sink is not idle (the source is ahead of it), so the windows stay
// behind its newest row and the probe skips rows the sink does not hold.
func TestScenarioRowsInFlightAreNotJudged(t *testing.T) {
	h := newModelHarness(t)
	h.run(1, true)
	h.m.deliverPending()
	for range 6 {
		now := h.clk.Now()
		h.m.traffic(now, now.Add(15*time.Minute), 30, 15*time.Minute) // none delivered
		h.m.pending = nil                                             // and never: the consumer is down
		h.step(15 * time.Minute)
	}
	h.wantQuiet()
}

// The source wrote just now and the sink has it: within rowSettle the sink is
// not called idle, so rows a slower partition may still carry are not judged
// against the sink's newest row.
func TestSinkIdleNeedsSettledQuietSource(t *testing.T) {
	now := t0
	for _, tc := range []struct {
		name             string
		fresh, srcNewest time.Time
		want             bool
	}{
		{"quiet, caught up", now.Add(-10 * time.Minute), now.Add(-10 * time.Minute), true},
		{"source within the tolerance", now.Add(-10 * time.Minute), now.Add(-10*time.Minute + 500*time.Millisecond), true},
		{"source ahead: rows in flight", now.Add(-10 * time.Minute), now.Add(-9 * time.Minute), false},
		{"sink's newest inside the settle", now.Add(-time.Minute), now.Add(-time.Minute), false},
		{"source not read", now.Add(-10 * time.Minute), time.Time{}, false},
		{"empty sink", time.Time{}, now.Add(-10 * time.Minute), false},
	} {
		if got := sinkIdle(now, tc.fresh, tc.srcNewest); got != tc.want {
			t.Errorf("%s: sinkIdle = %v, want %v", tc.name, got, tc.want)
		}
	}
	w := driftWindows(now, now.Add(-90*time.Minute+300*time.Millisecond), now.Add(-90*time.Minute))
	end := now.Add(-90*time.Minute + time.Second)
	if !w.idle || !w.countTo.Equal(end) || !w.rowCutoff.Equal(end) || !w.landedUntil.Equal(end) || !w.countFrom.Equal(end.Add(-driftWindow)) || w.probe {
		t.Fatalf("idle windows %+v, want every window to end at %v", w, end)
	}
}

// ---- bounds and readers off ----

func TestPlaceholderColumnsBoundedRotatedAndNoPII(t *testing.T) {
	h := newModelHarness(t)
	var toast []clients.ToastColumn
	for _, c := range []string{"a1", "a2", "a3", "a4", "a5", "email_body", mTS} {
		toast = append(toast, clients.ToastColumn{Name: c, Type: "text", Storage: "x"})
		if c != mTS {
			h.m.src.columns = append(h.m.src.columns, clients.ColumnInfo{Name: c, Type: "text"})
			h.m.snk.columns = append(h.m.snk.columns, clients.ColumnInfo{Name: c, Type: "text"})
		}
	}
	h.m.src.toast = toast
	h.step(0)
	first := h.m.snk.placeholderCalls()
	if len(first) != maxPlaceholderColumns {
		t.Fatalf("one run counted %v, want %d columns", first, maxPlaceholderColumns)
	}
	h.step(15 * time.Minute)
	all := h.m.snk.placeholderCalls()
	for _, c := range []string{"a1", "a2", "a3", "a4", "a5"} {
		if !slices.Contains(all, c) {
			t.Fatalf("rotation never counted %s: %v", c, all)
		}
	}
	if slices.Contains(all, "email_body") || slices.Contains(all, mTS) {
		t.Fatalf("counted a personal-data column or sink_ts_column: %v", all)
	}
}

func TestPlaceholderNotSupportedTurnsOffOnce(t *testing.T) {
	h := newModelHarness(t)
	h.m.snk.phErr = fmt.Errorf("snowflake: %w", clients.ErrNotSupported)
	h.run(3, false)
	if n := len(h.m.snk.placeholderCalls()); n != 1 {
		t.Fatalf("CountPlaceholder called %d times after ErrNotSupported", n)
	}
	if n := h.logs.count("the warehouse cannot count the placeholder"); n != 1 {
		t.Fatalf("%d INFO lines, want one\n%s", n, h.logs)
	}
	h.wantQuiet()
}

// A failing placeholder count leaves row_drift's state alone (unknown), and a
// breach found by another part still fires.
func TestPlaceholderErrorIsUnknown(t *testing.T) {
	h := newModelHarness(t)
	h.step(0)
	h.m.snk.mu.Lock()
	h.m.snk.phErr = errors.New("warehouse: connection reset")
	h.m.snk.mu.Unlock()
	h.step(15 * time.Minute)
	if h.phase("orders", rowID) != phaseOK || h.logs.count("check failed, signal state unchanged") != 1 {
		t.Fatalf("want one warning and no state change\n%s", h.logs)
	}
	breakFiltered(h)
	h.step(15 * time.Minute)
	h.wantFired(rowID)
}

func TestMergeRowParts(t *testing.T) {
	tg := target{}
	pr := tablePair{mSource, mSink}
	at := t0
	clear := rowPart{v: vClear, sample: 50}
	hold := rowPart{v: vHold}
	unknown := rowPart{v: vUnknown, err: errors.New("down")}
	breach := rowPart{v: vBreach, mismatched: 3, from: at, to: at.Add(time.Hour), label: "x", detail: "y"}
	for _, tc := range []struct {
		parts []rowPart
		want  verdict
	}{
		{[]rowPart{clear, hold}, vClear},
		{[]rowPart{hold, hold}, vHold},
		{[]rowPart{clear, unknown}, vUnknown},
		{[]rowPart{unknown, breach}, vBreach},
		{[]rowPart{clear, breach}, vBreach},
	} {
		if r := mergeRow(tg, pr, tc.parts); r.v != tc.want {
			t.Errorf("merge %v = %v, want %v", tc.parts, r.v, tc.want)
		}
	}
	r := mergeRow(tg, pr, []rowPart{clear, breach})
	if r.ev["sample"] != float64(50) || r.ev["mismatched"] != float64(3) || r.ev["detail"] != "sink "+mSink+"; y" || r.label != "row drift "+mSource+": x" {
		t.Fatalf("merged %+v", r)
	}
}
