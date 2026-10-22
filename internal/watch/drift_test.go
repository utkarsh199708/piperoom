package watch

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// ---- drift fakes ----

type countCall struct {
	table, ts string
	from, to  time.Time
}

type rowCall struct {
	table, pk string
	keys      int
	cols      []string
}

// fakeRec is one side's clients.Reconciler (and primary-key reader). Counts
// and rows are per table; a table with no rows entry has every sampled key.
type fakeRec struct {
	mu        sync.Mutex
	counts    map[string]int64
	countErr  error
	keys      []string
	at        time.Time // every row's timestamp columns
	missing   map[string]bool
	shifted   map[string]bool // keys whose timestamps are an hour off
	rowsErr   error
	pkErr     error
	cols      []clients.ColumnInfo
	countCs   []countCall
	sampleCs  []string // tables
	rowCs     []rowCall
	pkCs      int
	colTables []string
	// scopeSeen records whether every call carried a pipeline in ctx.
	unscoped int
}

func newFakeRec() *fakeRec {
	return &fakeRec{counts: map[string]int64{}, missing: map[string]bool{}, shifted: map[string]bool{}}
}

func (f *fakeRec) noteScope(ctx context.Context) {
	if _, ok := tools.PipelineFrom(ctx); !ok {
		f.unscoped++
	}
}

func (f *fakeRec) CountBetween(ctx context.Context, table, ts string, from, to time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.noteScope(ctx)
	f.countCs = append(f.countCs, countCall{table, ts, from, to})
	if f.countErr != nil {
		return 0, f.countErr
	}
	return f.counts[table], nil
}

func (f *fakeRec) SampleKeys(ctx context.Context, table, pk, ts string, since time.Time, n int) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.noteScope(ctx)
	f.sampleCs = append(f.sampleCs, table)
	if n != driftSampleKeys {
		return nil, fmt.Errorf("fake: n = %d", n)
	}
	return slices.Clone(f.keys), f.countErr
}

func (f *fakeRec) RowTimes(ctx context.Context, table, pk string, keys, cols []string) (map[string]map[string]time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.noteScope(ctx)
	f.rowCs = append(f.rowCs, rowCall{table, pk, len(keys), slices.Clone(cols)})
	if f.rowsErr != nil {
		return nil, f.rowsErr
	}
	out := map[string]map[string]time.Time{}
	for _, k := range keys {
		if f.missing[k] {
			continue
		}
		at := f.at
		if f.shifted[k] {
			at = at.Add(time.Hour)
		}
		m := map[string]time.Time{}
		for _, c := range cols {
			m[c] = at
		}
		out[k] = m
	}
	return out, nil
}

func (f *fakeRec) PrimaryKeyColumn(ctx context.Context, table string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pkCs++
	return "id", f.pkErr
}

func (f *fakeRec) tables() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.countCs {
		out = append(out, c.table)
	}
	out = append(out, f.sampleCs...)
	for _, c := range f.rowCs {
		out = append(out, c.table)
	}
	return append(out, f.colTables...)
}

func (f *fakeRec) countCalls() []countCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.countCs)
}

func (f *fakeRec) rowCalls() []rowCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.rowCs)
}

func (f *fakeRec) set(fn func(f *fakeRec)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakeRec) columns(table string) ([]clients.ColumnInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.colTables = append(f.colTables, table)
	return slices.Clone(f.cols), nil
}

// recSource is the watcher's source with the reconcile reader.
type recSource struct {
	*fakeSource
	*fakeRec
}

// colSource also reads the catalog (clients.ColumnReader).
type colSource struct{ recSource }

func (c colSource) Columns(ctx context.Context, table string) ([]clients.ColumnInfo, error) {
	return c.fakeRec.columns(table)
}

// fakeWH is the sink warehouse.
type fakeWH struct {
	*fakeRec
	fresh    time.Time
	freshErr error
}

func (w *fakeWH) Freshness(ctx context.Context, table, ts string) (time.Time, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.fresh, w.freshErr
}

func (w *fakeWH) Row(ctx context.Context, table, pk, key string) (map[string]any, bool, error) {
	return nil, false, errors.New("fake: the watcher must never read a whole row")
}

type colWH struct{ *fakeWH }

func (c colWH) Columns(ctx context.Context, table string) ([]clients.ColumnInfo, error) {
	return c.fakeRec.columns(table)
}

// fakeDup is the Kafka client's clients.DupScanner.
type fakeDup struct {
	mu      sync.Mutex
	reports map[string]clients.DupReport
	err     error
	calls   []string
	since   []time.Time
	max     []int
}

func (d *fakeDup) DupScan(ctx context.Context, topic string, since time.Time, maxScan int) (clients.DupReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, topic)
	d.since = append(d.since, since)
	d.max = append(d.max, maxScan)
	if d.err != nil {
		return clients.DupReport{}, d.err
	}
	r, ok := d.reports[topic]
	if !ok {
		r = clients.DupReport{Topic: topic, Scanned: 100}
	}
	return r, nil
}

func (d *fakeDup) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.calls)
}

type dupKafka struct {
	*fakeKafka
	*fakeDup
}

// driftHarness is a harness whose pipelines have sink tables and whose
// clients carry the drift readers.
type driftHarness struct {
	*harness
	src *fakeRec
	snk *fakeRec
	wh  *fakeWH
	dup *fakeDup
}

type driftOpts struct {
	opts
	columns bool // both sides implement clients.ColumnReader
	noDup   bool
}

// sinkYAML gives a pipeline sink tables (same names as its source tables)
// and a timestamp column, plus extra watch settings.
func sinkYAML(tables, watch string) string {
	return "    sink_tables: " + tables + "\n    sink_ts_column: updated_at\n" + watch
}

func newDriftHarness(t *testing.T, o driftOpts) *driftHarness {
	t.Helper()
	if len(o.pipes) == 0 {
		o.pipes = []pipeSpec{{id: "orders", watch: sinkYAML("[public.t]", "")}}
	}
	h := newHarness(t, o.opts)
	dh := &driftHarness{harness: h, src: newFakeRec(), snk: newFakeRec(), dup: &fakeDup{reports: map[string]clients.DupReport{}}}
	dh.wh = &fakeWH{fakeRec: dh.snk, fresh: t0}
	at := t0.Add(-30 * time.Minute)
	dh.src.at, dh.snk.at = at, at
	for i := range driftSampleKeys {
		k := fmt.Sprintf("secret-key-%04d", i)
		dh.src.keys = append(dh.src.keys, k)
	}
	d := h.deps
	if o.columns {
		d.Source = colSource{recSource{h.src, dh.src}}
		d.Warehouse = colWH{dh.wh}
	} else {
		d.Source = recSource{h.src, dh.src}
		d.Warehouse = dh.wh
	}
	if !o.noDup {
		d.Kafka = dupKafka{h.kafka, dh.dup}
	}
	w, err := New(d)
	if err != nil {
		t.Fatal(err)
	}
	h.w, h.deps = w, d
	return dh
}

// alertsWith returns the created rooms whose evidence holds signal.
func (h *driftHarness) alertsWith(signal string) []openRec {
	var out []openRec
	for _, o := range h.rooms.created() {
		if strings.Contains(string(o.alert.Raw), `"signal":"`+signal+`"`) {
			out = append(out, o)
		}
	}
	return out
}

// ---- TC1, E1, E2: count_drift needs two failing checks ----

func TestCountDriftFiresOnTwoFailingChecks(t *testing.T) {
	h := newDriftHarness(t, driftOpts{})
	h.src.counts["public.t"], h.snk.counts["public.t"] = 1000, 1000
	h.step(0)
	if n := len(h.rooms.created()); n != 0 {
		t.Fatalf("1000/1000 opened %d rooms", n)
	}
	// 1 % once, then healthy again: no signal.
	h.snk.set(func(f *fakeRec) { f.counts["public.t"] = 990 })
	h.step(15 * time.Minute)
	h.snk.set(func(f *fakeRec) { f.counts["public.t"] = 1000 })
	h.step(15 * time.Minute)
	if n := len(h.rooms.created()); n != 0 {
		t.Fatalf("one failing check opened %d rooms\n%s", n, h.logs)
	}
	// 1 % twice in a row: one count_drift room.
	h.snk.set(func(f *fakeRec) { f.counts["public.t"] = 990 })
	h.step(15 * time.Minute)
	if n := len(h.rooms.created()); n != 0 {
		t.Fatal("fired on the first failing check")
	}
	h.step(15 * time.Minute)
	got := h.alertsWith(events.SignalCountDrift)
	if len(got) != 1 {
		t.Fatalf("want one count_drift room, got %d\n%s", len(h.rooms.created()), h.logs)
	}
	ev := signalEv(t, evidenceOf(t, got[0].alert.Raw), events.SignalCountDrift)
	if ev["severity"] != "sev2" || ev["missing"] != float64(10) || ev["source"] != float64(1000) || ev["sink"] != float64(990) ||
		ev["table"] != "public.t" || ev["topic"] != "orders.public.t" {
		t.Fatalf("evidence = %v", ev)
	}
	if got[0].alert.Severity != "sev2" || !strings.Contains(got[0].alert.Title, "count drift public.t") {
		t.Fatalf("alert = %+v", got[0].alert)
	}
	// A run between intervals reads nothing.
	n := len(h.src.countCalls())
	h.step(time.Minute)
	if len(h.src.countCalls()) != n {
		t.Fatal("drift checks ran before reconcile_interval")
	}
}

func TestCountDriftToleranceBoundaryAndResolve(t *testing.T) {
	h := newDriftHarness(t, driftOpts{})
	h.src.counts["public.t"], h.snk.counts["public.t"] = 1000, 995 // exactly 0.5 %
	h.step(0)
	h.step(15 * time.Minute)
	h.step(15 * time.Minute)
	if h.phase("orders", driftID(events.SignalCountDrift, "public.t")) != phaseOK {
		t.Fatal("a difference at the tolerance fired")
	}
	h.snk.set(func(f *fakeRec) { f.counts["public.t"] = 994 })
	h.step(15 * time.Minute)
	h.step(15 * time.Minute)
	if h.phase("orders", driftID(events.SignalCountDrift, "public.t")) != phaseFiring {
		t.Fatal("0.6 % on two checks did not fire")
	}
	h.snk.set(func(f *fakeRec) { f.counts["public.t"] = 1000 })
	h.step(15 * time.Minute)
	if h.phase("orders", driftID(events.SignalCountDrift, "public.t")) != phaseOK {
		t.Fatal("did not resolve when the counts agree")
	}
	if len(h.rooms.resolved()) != 1 {
		t.Fatalf("want one resolved alert, got %d", len(h.rooms.resolved()))
	}
}

func TestCountDriftToleranceFromOwners(t *testing.T) {
	h := newDriftHarness(t, driftOpts{opts: opts{pipes: []pipeSpec{{id: "orders",
		watch: sinkYAML("[public.t]", "    watch:\n      count_tolerance: 2%\n      reconcile_interval: 5m\n")}}}})
	h.src.counts["public.t"], h.snk.counts["public.t"] = 1000, 990
	h.step(0)
	h.step(5 * time.Minute)
	h.step(5 * time.Minute)
	if len(h.rooms.created()) != 0 {
		t.Fatal("1 % fired under a 2 % tolerance")
	}
	if n := len(h.src.countCalls()); n != 3 {
		t.Fatalf("reconcile_interval 5m: want 3 runs in 10 min, got %d", n)
	}
}

// ---- TC2: the window is shifted by the sink's lag ----

func TestCountDriftWindowShiftedBySinkLag(t *testing.T) {
	h := newDriftHarness(t, driftOpts{})
	h.wh.fresh = t0.Add(30 * time.Minute).Add(-10 * time.Minute)
	h.step(30 * time.Minute) // now 10:30, sink 10 min behind
	calls := h.src.countCalls()
	if len(calls) != 1 {
		t.Fatalf("want one source count, got %v", calls)
	}
	wantTo := t0.Add(-10 * time.Minute) // the closed hour ends at 10:00, minus the lag
	if c := calls[0]; !c.to.Equal(wantTo) || !c.from.Equal(wantTo.Add(-time.Hour)) || c.ts != "updated_at" {
		t.Fatalf("source window %v..%v (%s), want %v..%v", c.from, c.to, c.ts, wantTo.Add(-time.Hour), wantTo)
	}
	sc := h.snk.countCalls()
	if len(sc) != 1 || !sc[0].to.Equal(wantTo) || !sc[0].from.Equal(calls[0].from) {
		t.Fatalf("sink window %v, want the source's", sc)
	}
	// No lag: the window is the last closed hour itself.
	h.wh.mu.Lock()
	h.wh.fresh = t0.Add(45 * time.Minute)
	h.wh.mu.Unlock()
	h.step(15 * time.Minute)
	calls = h.src.countCalls()
	if c := calls[len(calls)-1]; !c.to.Equal(t0) {
		t.Fatalf("no lag: window ends %v, want %v", c.to, t0)
	}
}

// ---- TC3, E7: row_drift, keys never leave the watcher ----

func TestRowDriftMissingKeyAndNoKeyValues(t *testing.T) {
	h := newDriftHarness(t, driftOpts{opts: opts{evidenceSink: true}})
	h.snk.missing["secret-key-0017"] = true
	h.step(0)
	got := h.alertsWith(events.SignalRowDrift)
	if len(got) != 1 {
		t.Fatalf("want one row_drift room, got %d\n%s", len(h.rooms.created()), h.logs)
	}
	ev := signalEv(t, evidenceOf(t, got[0].alert.Raw), events.SignalRowDrift)
	if ev["missing"] != float64(1) || ev["sample"] != float64(driftSampleKeys) || ev["mismatched"] != float64(0) || ev["severity"] != "sev2" {
		t.Fatalf("evidence = %v", ev)
	}
	// The same keys were asked of both sides, timestamp columns only.
	for _, f := range []*fakeRec{h.src, h.snk} {
		rc := f.rowCalls()
		if len(rc) != 1 || rc[0].keys != driftSampleKeys || !slices.Equal(rc[0].cols, []string{"updated_at"}) || rc[0].pk != "id" {
			t.Fatalf("RowTimes calls %+v", rc)
		}
	}
	// A shifted timestamp is a mismatch.
	h.snk.set(func(f *fakeRec) { f.missing = map[string]bool{}; f.shifted["secret-key-0003"] = true })
	h.step(15 * time.Minute)
	h.step(time.Hour) // still firing join
	assertNoKeys(t, h)
}

func assertNoKeys(t *testing.T, h *driftHarness) {
	t.Helper()
	var all strings.Builder
	all.WriteString(h.logs.String())
	h.rooms.mu.Lock()
	for _, o := range h.rooms.opens {
		all.WriteString(o.alert.Title)
		all.Write(o.alert.Raw)
	}
	for _, p := range h.rooms.posts {
		all.WriteString(p.text)
		all.Write(p.ev)
	}
	h.rooms.mu.Unlock()
	if strings.Contains(all.String(), "secret-key") {
		t.Fatalf("a key value reached a log line, an alert or a post:\n%s", all.String())
	}
}

func TestRowDriftMismatchedTimestamp(t *testing.T) {
	h := newDriftHarness(t, driftOpts{})
	h.snk.shifted["secret-key-0001"] = true
	h.snk.shifted["secret-key-0002"] = true
	h.step(0)
	got := h.alertsWith(events.SignalRowDrift)
	if len(got) != 1 {
		t.Fatalf("want one row_drift room\n%s", h.logs)
	}
	ev := signalEv(t, evidenceOf(t, got[0].alert.Raw), events.SignalRowDrift)
	if ev["mismatched"] != float64(2) || ev["missing"] != float64(0) {
		t.Fatalf("evidence = %v", ev)
	}
	assertNoKeys(t, h)
}

func TestRowDriftSkipsRowsStillInFlight(t *testing.T) {
	h := newDriftHarness(t, driftOpts{})
	// Every sampled row is newer than now - lag - settle: nothing is judged.
	h.src.at = t0.Add(-time.Minute)
	h.snk.missing["secret-key-0000"] = true
	h.step(0)
	if len(h.rooms.created()) != 0 || h.phase("orders", driftID(events.SignalRowDrift, "public.t")) != phaseOK {
		t.Fatal("a row still in flight was judged missing")
	}
}

// ---- TC4: PII columns are never requested ----

func TestRowDriftNeverRequestsPIIColumns(t *testing.T) {
	h := newDriftHarness(t, driftOpts{columns: true})
	cols := []clients.ColumnInfo{
		{Name: "id", Type: "bigint"},
		{Name: "updated_at", Type: "timestamp with time zone"},
		{Name: "shipped_at", Type: "timestamp without time zone"},
		{Name: "email_verified_at", Type: "timestamp with time zone"},
		{Name: "birth_date", Type: "date"},
		{Name: "customer_name", Type: "text"},
		{Name: "note", Type: "text"},
	}
	h.src.cols, h.snk.cols = cols, cols
	h.step(0)
	want := []string{"updated_at", "shipped_at"}
	for _, f := range []*fakeRec{h.src, h.snk} {
		rc := f.rowCalls()
		if len(rc) != 1 || !slices.Equal(rc[0].cols, want) {
			t.Fatalf("RowTimes columns %+v, want %v", rc, want)
		}
	}
}

func TestPIIColumnNames(t *testing.T) {
	for _, tc := range []struct {
		name string
		pii  bool
	}{
		{"updated_at", false}, {"created_at", false}, {"shipped_at", false}, {"ts", false}, {"op_ts", false},
		{"email_verified_at", true}, {"birth_date", true}, {"dateofbirth", true}, {"dob", true},
		{"phone_confirmed_at", true}, {"last_login_ip", true}, {"card_expires_at", true}, {"name", true},
		{"PassportIssuedAt", true}, {"tax_filed_at", true}, {"timestamp", false}, {"pipeline_at", false},
	} {
		if got := piiColumn(tc.name); got != tc.pii {
			t.Errorf("piiColumn(%q) = %v, want %v", tc.name, got, tc.pii)
		}
	}
}

// ---- TC5: dup_keys and order_skew from the scan report ----

func TestDupKeysAndOrderSkewFromReport(t *testing.T) {
	h := newDriftHarness(t, driftOpts{})
	topic := "orders.public.t"
	h.dup.reports[topic] = clients.DupReport{Topic: topic, Scanned: 500, Duplicates: 0, Keys: 0}
	h.step(0)
	if len(h.rooms.created()) != 0 {
		t.Fatal("distinct positions fired")
	}
	h.dup.mu.Lock()
	h.dup.reports[topic] = clients.DupReport{Topic: topic, Scanned: 500, Duplicates: 3, Keys: 2, Capped: true, MaxScan: 50000,
		Findings: []clients.DupFinding{{Kind: clients.DupKindDuplicate, Key: "sha256:00112233deadbeef", Offset: 41}}}
	h.dup.mu.Unlock()
	h.step(15 * time.Minute)
	got := h.alertsWith(events.SignalDupKeys)
	if len(got) != 1 {
		t.Fatalf("want one dup_keys room, got %d\n%s", len(h.rooms.created()), h.logs)
	}
	raw := string(got[0].alert.Raw)
	ev := signalEv(t, evidenceOf(t, got[0].alert.Raw), events.SignalDupKeys)
	if ev["source"] != float64(500) || ev["missing"] != float64(3) || ev["sample"] != float64(2) || ev["topic"] != topic ||
		ev["severity"] != "sev3" || !strings.Contains(ev["detail"].(string), "capped") {
		t.Fatalf("evidence = %v", ev)
	}
	w := ev["window"].(map[string]any)
	if w["from"] != t0.Format(time.RFC3339) || w["to"] != t0.Add(15*time.Minute).Format(time.RFC3339) {
		t.Fatalf("window = %v, want the previous scan's start to this one", w)
	}
	if strings.Contains(raw, "sha256") || strings.Contains(raw, "deadbeef") {
		t.Fatalf("a fingerprint reached the evidence: %s", raw)
	}
	if strings.Contains(raw, `"signal":"order_skew"`) {
		t.Fatal("order_skew fired without regressions")
	}
	// The first scan started reconcile_interval back; the next one where the
	// last ended.
	h.dup.mu.Lock()
	since := slices.Clone(h.dup.since)
	h.dup.mu.Unlock()
	if !since[0].Equal(t0.Add(-15*time.Minute)) || !since[1].Equal(t0) {
		t.Fatalf("scan windows start %v", since)
	}

	// A regression fires order_skew into the same room.
	h.dup.mu.Lock()
	h.dup.reports[topic] = clients.DupReport{Topic: topic, Scanned: 400, OrderRegressions: 4, RegressionKeys: 1}
	h.dup.mu.Unlock()
	h.step(15 * time.Minute)
	if h.phase("orders", driftID(events.SignalOrderSkew, topic)) != phaseFiring {
		t.Fatal("order_skew did not fire")
	}
	posts := h.rooms.postsContaining("order skew")
	if len(posts) != 1 || len(h.rooms.created()) != 1 {
		t.Fatalf("order_skew should join the open room: posts %d rooms %d", len(posts), len(h.rooms.created()))
	}
	if h.phase("orders", driftID(events.SignalDupKeys, topic)) != phaseOK {
		t.Fatal("dup_keys did not clear on a scan without duplicates")
	}
}

func TestDupScanUndecodableNeverFires(t *testing.T) {
	r := dupReading(events.SignalDupKeys, clients.DupReport{Topic: "x", Scanned: 10, Undecodable: 10}, t0, t0)
	if r.v != vHold {
		t.Fatalf("all-undecodable scan: verdict %v, want hold", r.v)
	}
	r = dupReading(events.SignalOrderSkew, clients.DupReport{Topic: "x", Scanned: 10, OrderRegressions: 2, RegressionKeys: 2}, t0, t0)
	if r.v != vBreach || r.ev["mismatched"] != float64(2) || r.ev["sample"] != float64(2) || r.ev["missing"] != float64(0) {
		t.Fatalf("order_skew reading %+v", r)
	}
}

// ---- TC6: ErrNotSupported turns the check off with one INFO line ----

func TestNotSupportedTurnsDriftOffOnce(t *testing.T) {
	h := newDriftHarness(t, driftOpts{})
	h.src.countErr = fmt.Errorf("snowflake: %w", clients.ErrNotSupported)
	h.dup.err = fmt.Errorf("kafka: %w", clients.ErrNotSupported)
	h.snk.missing["secret-key-0001"] = true
	for range 4 {
		h.step(15 * time.Minute)
	}
	if n := h.logs.count("drift check off for this pipeline"); n != 2 {
		t.Fatalf("want two INFO lines (reconcile, dup scan), got %d\n%s", n, h.logs)
	}
	if n := h.logs.count("check failed"); n != 0 {
		t.Fatalf("ErrNotSupported was logged as a failure\n%s", h.logs)
	}
	if len(h.src.countCalls()) != 1 || h.dup.callCount() != 1 {
		t.Fatalf("readers called again after ErrNotSupported: counts %d scans %d", len(h.src.countCalls()), h.dup.callCount())
	}
	if len(h.rooms.created()) != 0 {
		t.Fatal("a check that is off opened a room")
	}
}

func TestMissingReadersTurnDriftOffOnce(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders", watch: sinkYAML("[public.t]", "")}}})
	h.steps(3, 15*time.Minute)
	if n := h.logs.count("drift check off for this pipeline"); n != 2 {
		t.Fatalf("want one line for the table checks and one for the scan, got %d\n%s", n, h.logs)
	}
}

// ---- E3: a read error leaves the state alone ----

func TestDriftReadErrorIsUnknown(t *testing.T) {
	h := newDriftHarness(t, driftOpts{})
	h.src.counts["public.t"], h.snk.counts["public.t"] = 1000, 900
	h.step(0)
	h.wh.mu.Lock()
	h.wh.freshErr = errors.New("warehouse down")
	h.wh.mu.Unlock()
	h.step(15 * time.Minute)
	h.step(15 * time.Minute)
	if h.phase("orders", driftID(events.SignalCountDrift, "public.t")) != phaseOK {
		t.Fatal("an error tick changed the state")
	}
	if n := h.logs.count("check failed, signal state unchanged"); n != 2 { // count and row, once each
		t.Fatalf("want one warning per signal, got %d\n%s", n, h.logs)
	}
	h.wh.mu.Lock()
	h.wh.freshErr = nil
	h.wh.mu.Unlock()
	h.step(15 * time.Minute)
	if h.phase("orders", driftID(events.SignalCountDrift, "public.t")) != phaseFiring {
		t.Fatal("the second failing check (around an error) did not fire")
	}
}

// ---- TC7: cooldown per (pipeline, table, kind) ----

func TestDriftCooldownPerTableAndKind(t *testing.T) {
	h := newDriftHarness(t, driftOpts{opts: opts{cooldown: 2 * time.Hour, pipes: []pipeSpec{{id: "orders", tables: "[public.a, public.b]",
		watch: sinkYAML("[public.a, public.b]", "")}}}})
	h.dup.err = clients.ErrNotSupported
	// A sampled key is missing (the fake answers it for both tables).
	h.snk.missing["secret-key-0005"] = true
	h.step(0)
	if n := len(h.rooms.created()); n != 1 {
		t.Fatalf("want one room, got %d\n%s", n, h.logs)
	}
	room := h.rooms.created()[0].roomID
	h.rooms.closeRoom(room, h.clk.Now())
	h.w.OnRoomClosed(room, "orders", DedupKey("orders"))
	// The drift clears, then comes back within the cooldown: no new room.
	h.snk.set(func(f *fakeRec) { f.missing = map[string]bool{} })
	h.step(15 * time.Minute)
	h.snk.set(func(f *fakeRec) { f.missing["secret-key-0005"] = true })
	h.step(15 * time.Minute)
	if n := len(h.rooms.created()); n != 1 {
		t.Fatalf("the same (pipeline, table, kind) re-opened within the cooldown: %d rooms", n)
	}
	// count_drift on public.b is another (table, kind): it opens a room.
	h.src.set(func(f *fakeRec) { f.counts["public.b"] = 1000 })
	h.step(15 * time.Minute)
	h.step(15 * time.Minute)
	got := h.alertsWith(events.SignalCountDrift)
	if len(got) != 1 {
		t.Fatalf("a different drift was muted by another's cooldown: %d rooms\n%s", len(h.rooms.created()), h.logs)
	}
	if strings.Contains(string(got[0].alert.Raw), `"signal":"row_drift"`) {
		t.Fatalf("the cooling row_drift was put in the new room's evidence: %s\n%s", got[0].alert.Raw, h.logs)
	}
}

// ---- TC8: reconcile: off ----

func TestReconcileOffTableIsNeverQueried(t *testing.T) {
	h := newDriftHarness(t, driftOpts{columns: true, opts: opts{pipes: []pipeSpec{{id: "orders", tables: "[public.a, public.b]",
		watch: sinkYAML("[public.a, public.b]", "    watch:\n      tables:\n        public.b: {reconcile: off}\n")}}}})
	h.steps(3, 15*time.Minute)
	for _, f := range []*fakeRec{h.src, h.snk} {
		tables := f.tables()
		if len(tables) == 0 {
			t.Fatal("public.a was not checked")
		}
		if slices.Contains(tables, "public.b") {
			t.Fatalf("public.b (reconcile: off) was queried: %v", tables)
		}
	}
}

// ---- scope ----

func TestDriftReadsCarryThePipelineScope(t *testing.T) {
	h := newDriftHarness(t, driftOpts{})
	h.step(0)
	if h.src.unscoped != 0 || h.snk.unscoped != 0 {
		t.Fatal("a reconcile read ran without the pipeline in ctx")
	}
	if len(h.src.countCalls()) == 0 {
		t.Fatal("nothing was read")
	}
}

func TestDriftPairs(t *testing.T) {
	for _, tc := range []struct {
		sources, sinks []string
		want           []tablePair
	}{
		{[]string{"public.orders"}, []string{"dw.orders_v2"}, []tablePair{{"public.orders", "dw.orders_v2"}}},
		{[]string{"public.a", "public.b"}, []string{"dw.b", "dw.c"}, []tablePair{{"public.b", "dw.b"}}},
		{[]string{"public.a", "s.a"}, []string{"dw.a", "x.a"}, nil}, // two sinks named a: cannot tell
	} {
		var got []tablePair
		for _, s := range tc.sources {
			if k := pairSink(s, tc.sources, tc.sinks); k != "" {
				got = append(got, tablePair{s, k})
			}
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("pairs of %v -> %v = %v, want %v", tc.sources, tc.sinks, got, tc.want)
		}
	}
}

func TestRotateBoundsWorkAndCoversAll(t *testing.T) {
	list := []int{0, 1, 2, 3, 4}
	next := 0
	seen := map[int]bool{}
	for range 3 {
		got := rotate(list, &next, 2)
		if len(got) != 2 {
			t.Fatalf("got %v", got)
		}
		for _, v := range got {
			seen[v] = true
		}
	}
	if len(seen) != 5 {
		t.Fatalf("rotation missed items: %v", seen)
	}
}

// ---- E6: each pipeline gets its own evidence ----

func TestDriftPerPipelineEvidence(t *testing.T) {
	h := newDriftHarness(t, driftOpts{opts: opts{pipes: []pipeSpec{
		{id: "orders", watch: sinkYAML("[public.t]", "")},
		{id: "billing", tables: "[public.inv]", watch: sinkYAML("[public.inv]", "")},
	}}})
	h.dup.reports["billing.public.inv"] = clients.DupReport{Topic: "billing.public.inv", Scanned: 9, Duplicates: 1, Keys: 1}
	h.step(0)
	got := h.alertsWith(events.SignalDupKeys)
	if len(got) != 1 || got[0].alert.PipelineID != "billing" || got[0].alert.DedupKey != DedupKey("billing") {
		t.Fatalf("want one dup_keys room for billing, got %+v", got)
	}
	if got[0].alert.Source != alertin.Watcher {
		t.Fatalf("source = %q", got[0].alert.Source)
	}
}

// ---- TC9: goroutines stop with ctx ----

func TestDriftChecksStopOnCancel(t *testing.T) {
	h := newDriftHarness(t, driftOpts{opts: opts{checkTimeout: 10 * time.Second}})
	reached := make(chan struct{}, 1)
	slow := &blockingWH{fakeWH: h.wh, reached: reached}
	d := h.deps
	d.Warehouse = slow
	w, err := New(d)
	if err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-reached:
	case <-time.After(2 * time.Second):
		t.Fatal("the drift check never reached the warehouse")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1 s of cancel")
	}
	waitFor(t, "goroutines to return to the start count", func() bool { return runtime.NumGoroutine() <= before })
}

// blockingWH's Freshness waits for its context, like a slow warehouse.
type blockingWH struct {
	*fakeWH
	reached chan struct{}
}

func (b *blockingWH) Freshness(ctx context.Context, table, ts string) (time.Time, error) {
	select {
	case b.reached <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return time.Time{}, ctx.Err()
}

// ---- logs: no drift line ever names a key ----

func TestDriftEvidenceDetailIsCapped(t *testing.T) {
	m := driftEvidence(events.DriftSignal{Kind: events.SignalDupKeys, Detail: strings.Repeat("é", 300)})
	if d := m["detail"].(string); len(d) > maxDriftDetail {
		t.Fatalf("detail is %d bytes", len(d))
	}
}
