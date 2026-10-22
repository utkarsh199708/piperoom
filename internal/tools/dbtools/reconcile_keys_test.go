package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// windowKeys is one observed WindowKeys call.
type windowKeys struct {
	table, key, ts string
	since, until   time.Time
	limit          int
}

// keySrc is countSrc plus the key reads: WindowKeys and the source's primary
// key column.
type keySrc struct {
	countSrc
	srcKeys []string
	total   int64 // 0 = len(srcKeys)
	keysErr error
	pk      string // "" = "id"
	pkErr   error
	got     windowKeys
	calls   int
	pkCalls int
}

func (f *keySrc) WindowKeys(_ context.Context, table, key, ts string, since, until time.Time, limit int) ([]string, int64, error) {
	f.got, f.calls = windowKeys{table, key, ts, since, until, limit}, f.calls+1
	if f.keysErr != nil {
		return nil, 0, f.keysErr
	}
	if f.total != 0 {
		return f.srcKeys, f.total, nil
	}
	return f.srcKeys, int64(len(f.srcKeys)), nil
}

func (f *keySrc) PrimaryKeyColumn(context.Context, string) (string, error) {
	f.pkCalls++
	if f.pk == "" && f.pkErr == nil {
		return "id", nil
	}
	return f.pk, f.pkErr
}

// keyWh is countWh plus WindowKeys and the sink's primary key column.
type keyWh struct {
	countWh
	whKeys  []string
	total   int64 // 0 = len(whKeys)
	keysErr error
	pk      string // "" = "id"
	pkErr   error
	got     windowKeys
	calls   int
}

func (f *keyWh) WindowKeys(_ context.Context, table, key, ts string, since, until time.Time, limit int) ([]string, int64, error) {
	f.got, f.calls = windowKeys{table, key, ts, since, until, limit}, f.calls+1
	if f.keysErr != nil {
		return nil, 0, f.keysErr
	}
	if f.total != 0 {
		return f.whKeys, f.total, nil
	}
	return f.whKeys, int64(len(f.whKeys)), nil
}

func (f *keyWh) PrimaryKeyColumn(context.Context, string) (string, error) {
	if f.pk == "" && f.pkErr == nil {
		return "id", nil
	}
	return f.pk, f.pkErr
}

// rcKeyDeps builds the owners of rcDeps and swaps in the key-capable fakes.
func rcKeyDeps(src *keySrc, wh *keyWh, k clients.KafkaReader) Deps {
	d := rcDeps(&src.countSrc, &wh.countWh, k)
	d.Src, d.Wh = src, wh
	return d
}

func keyStrings(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprint(i + 1)
	}
	return out
}

// TC3: counts equal -> no drift, and the keys say so too.
func TestReconcileKeysInSync(t *testing.T) {
	src := &keySrc{countSrc: countSrc{counts: []int64{3}}, srcKeys: []string{"1", "2", "3"}}
	wh := &keyWh{countWh: countWh{counts: []int64{3}}, whKeys: []string{"1", "2", "3"}}
	res, err := rcRun(t, rcKeyDeps(src, wh, nil), "orders", rcIn)
	if err != nil {
		t.Fatal(err)
	}
	keys := res.Data.(ReconcileData).Keys
	if keys == nil {
		t.Fatalf("no key diff: %s", res.Summary)
	}
	if keys.Source != 3 || keys.Sink != 3 || keys.Missing != 0 || keys.Extra != 0 || len(keys.MissingKeys) != 0 {
		t.Fatalf("keys = %+v", keys)
	}
	if keys.Column != "id" || keys.SinkColumn != "id" {
		t.Errorf("columns = %q/%q", keys.Column, keys.SinkColumn)
	}
	if !strings.Contains(res.Summary, "keys by id: source 3, sink 3; missing 0; extra 0") ||
		!strings.Contains(res.Summary, "the same 3 rows are in source and sink") {
		t.Errorf("summary:\n%s", res.Summary)
	}
	// One read per side over the same window, keyed by the discovered PK.
	if src.calls != 1 || wh.calls != 1 || src.got.table != "public.orders" || wh.got.table != "analytics.orders" ||
		src.got.key != "id" || src.got.ts != "updated_at" || wh.got.ts != "updated_at" || src.got.limit != reconcileKeyCap {
		t.Fatalf("source %+v, sink %+v", src.got, wh.got)
	}
	if !src.got.since.Equal(rcSince) || !src.got.until.Equal(rcSince.Add(30*time.Minute)) {
		t.Errorf("window = %s..%s", src.got.since, src.got.until)
	}
}

// TC2 basis: 30 rows in the source and none in the sink are reported with
// their exact count and a bounded list of keys.
func TestReconcileKeysMissingRows(t *testing.T) {
	src := &keySrc{countSrc: countSrc{counts: []int64{30}}, srcKeys: keyStrings(30)}
	wh := &keyWh{countWh: countWh{counts: []int64{0}}}
	res, err := rcRun(t, rcKeyDeps(src, wh, nil), "orders", rcIn)
	if err != nil {
		t.Fatal(err)
	}
	data := res.Data.(ReconcileData)
	keys := data.Keys
	if keys == nil || keys.Source != 30 || keys.Sink != 0 || keys.Missing != 30 || keys.Extra != 0 {
		t.Fatalf("keys = %+v", keys)
	}
	if len(keys.MissingKeys) != reconcileKeySample || keys.MissingKeys[0] != "1" {
		t.Fatalf("missing keys = %v", keys.MissingKeys)
	}
	for _, want := range []string{
		"keys by id: source 30, sink 0; missing 30 (e.g. 1, 10, 11, 12, 13, 14, 15, 16, 17, 18); extra 0",
		"30 source rows are not in the sink's window (missing, or not yet updated there)",
		"confirm one with warehouse_row",
	} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, res.Summary)
		}
	}
}

// Extra sink rows are reported too, with what they are (a stale or duplicated
// load, or a source row that left the window after a later update).
func TestReconcileKeysExtraRows(t *testing.T) {
	src := &keySrc{countSrc: countSrc{counts: []int64{2}}, srcKeys: []string{"1", "2"}}
	wh := &keyWh{countWh: countWh{counts: []int64{3}}, whKeys: []string{"1", "2", "3"}}
	res, err := rcRun(t, rcKeyDeps(src, wh, nil), "orders", rcIn)
	if err != nil {
		t.Fatal(err)
	}
	keys := res.Data.(ReconcileData).Keys
	if keys == nil || keys.Extra != 1 || strings.Join(keys.ExtraKeys, ",") != "3" || keys.Missing != 0 {
		t.Fatalf("keys = %+v", keys)
	}
	if !strings.Contains(res.Summary, "keys by id: source 2, sink 3; missing 0; extra 1 (e.g. 3)") ||
		!strings.Contains(res.Summary, "1 sink row is not among the source rows in this window") {
		t.Errorf("summary:\n%s", res.Summary)
	}
}

// TC4 of KAN-409: each row updated twice is three events per row. The event
// count must not turn into a gap: the key sets are equal, so no drift.
func TestReconcileKeysUpdatesAreNotAGap(t *testing.T) {
	srcKeys := keyStrings(100)
	src := &keySrc{countSrc: countSrc{counts: []int64{100, 100, 100, 100, 100, 100, 100, 100, 100, 100}}, srcKeys: srcKeys}
	wh := &keyWh{countWh: countWh{counts: []int64{100, 100, 100, 100, 100, 100, 100, 100, 100, 100}}, whKeys: srcKeys}
	// 3 events per row: the topic column reads 300 per bucket, the keys 100.
	k := &rateKafka{perBoundary: func(at time.Time) int64 {
		var total int64
		for i := 0; i < 10; i++ {
			if !rcSince.Add(time.Duration(i) * 3 * time.Minute).Before(at) {
				total += 300
			}
		}
		return total
	}}
	res, err := rcRun(t, rcKeyDeps(src, wh, k), "orders", rcIn)
	if err != nil {
		t.Fatal(err)
	}
	keys := res.Data.(ReconcileData).Keys
	if keys == nil || keys.Missing != 0 || keys.Extra != 0 || keys.Source != 100 {
		t.Fatalf("keys = %+v", keys)
	}
	if strings.Contains(res.Summary, "not in the sink") || strings.Contains(res.Summary, "not among the source") {
		t.Errorf("a 3x-event table reported a gap:\n%s", res.Summary)
	}
	if !strings.Contains(res.Summary, "missing 0; extra 0") || !strings.Contains(res.Summary, "no drift") {
		t.Errorf("summary:\n%s", res.Summary)
	}
}

// A window with more keys than the cap leaves the key section out with a note;
// the bucket table still answers (the production case: a busy 6 h window).
func TestReconcileKeysCapNote(t *testing.T) {
	src := &keySrc{countSrc: countSrc{counts: []int64{1}}, total: reconcileKeyCap + 1}
	res, err := rcRun(t, rcKeyDeps(src, &keyWh{}, nil), "orders", rcIn)
	if err != nil {
		t.Fatal(err)
	}
	if res.Data.(ReconcileData).Keys != nil || len(res.Data.(ReconcileData).Buckets) == 0 {
		t.Fatalf("data = %+v", res.Data)
	}
	if !strings.Contains(res.Summary, "bucket | source rows | topic events | sink rows") {
		t.Errorf("bucket table lost:\n%s", res.Summary)
	}
	if want := "keys not compared: 2001 source / 0 sink keys exceed the 2000-key cap; narrow since/until for a key diff"; !strings.Contains(res.Summary, want) {
		t.Errorf("summary lacks %q:\n%s", want, res.Summary)
	}

	src = &keySrc{countSrc: countSrc{counts: []int64{1}}, srcKeys: keyStrings(3)}
	res, err = rcRun(t, rcKeyDeps(src, &keyWh{total: 9000}, nil), "orders", rcIn)
	if err != nil {
		t.Fatal(err)
	}
	if want := "keys not compared: 3 source / 9000 sink keys exceed the 2000-key cap; narrow since/until for a key diff"; !strings.Contains(res.Summary, want) {
		t.Errorf("summary lacks %q:\n%s", want, res.Summary)
	}
}

// Clients without the key read (Snowflake, Databricks, the eval replay) leave
// the key section out with a note: the bucket table still answers.
func TestReconcileKeysUnavailable(t *testing.T) {
	d := rcDeps(&countSrc{counts: []int64{1}}, &countWh{counts: []int64{1}}, nil)
	res, err := rcRun(t, d, "orders", rcIn)
	if err != nil {
		t.Fatal(err)
	}
	if res.Data.(ReconcileData).Keys != nil || !strings.Contains(res.Summary, "keys not compared: the source client cannot list keys") {
		t.Fatalf("source without keys: %+v\n%s", res.Data, res.Summary)
	}
	// The source can list keys, the warehouse cannot.
	src := &keySrc{countSrc: countSrc{counts: []int64{1}}, srcKeys: []string{"1"}}
	d = rcKeyDeps(src, &keyWh{countWh: countWh{counts: []int64{1}}}, nil)
	d.Wh = &countWh{counts: []int64{1}} // no WindowKeys
	res, err = rcRun(t, d, "orders", rcIn)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Summary, "keys not compared: the warehouse client cannot list keys") {
		t.Errorf("summary:\n%s", res.Summary)
	}
}

// key_column must name the primary key; an accepted name is canonicalized to
// the discovered column, and a bad name is refused before I/O.
func TestReconcileKeysExplicitColumn(t *testing.T) {
	src := &keySrc{countSrc: countSrc{counts: []int64{1}}, srcKeys: []string{"a"}, pk: "order_id"}
	wh := &keyWh{countWh: countWh{counts: []int64{1}}, whKeys: []string{"a"}, pk: "order_id"}
	res, err := rcRun(t, rcKeyDeps(src, wh, nil), "orders", `{"table":"public.orders","since":"2026-10-14T03:00:00Z","until":"2026-10-14T03:30:00Z","key_column":"order_id"}`)
	if err != nil {
		t.Fatal(err)
	}
	if src.got.key != "order_id" || wh.got.key != "order_id" || src.pkCalls != 1 {
		t.Errorf("source %+v, pk calls %d", src.got, src.pkCalls)
	}
	if got := res.Data.(ReconcileData).Keys.Column; got != "order_id" {
		t.Errorf("column = %q", got)
	}
	_, err = rcRun(t, rcKeyDeps(src, wh, nil), "orders", `{"table":"public.orders","since":"2026-10-14T03:00:00Z","until":"2026-10-14T03:30:00Z","key_column":"id;drop"}`)
	if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "key_column") {
		t.Fatalf("bad key_column: %v", err)
	}
	if src.calls != 1 { // the first call only
		t.Errorf("a bad column reached the client: %d calls", src.calls)
	}
}

// key_column cannot name any other column: the whole window of that column
// would be read and up to ten values logged. The refusal happens before any
// key value is read (the pg_row rule).
func TestReconcileKeysRefusesNonPrimaryKeyColumn(t *testing.T) {
	src := &keySrc{countSrc: countSrc{counts: []int64{1}}, srcKeys: []string{"1"}, pk: "id"}
	wh := &keyWh{countWh: countWh{counts: []int64{1}}, whKeys: []string{"1"}, pk: "id"}
	_, err := rcRun(t, rcKeyDeps(src, wh, nil), "orders", `{"table":"public.orders","since":"2026-10-14T03:00:00Z","until":"2026-10-14T03:30:00Z","key_column":"email"}`)
	if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "must be the primary key column of public.orders") || !strings.Contains(err.Error(), `"id"`) {
		t.Fatalf("err = %v", err)
	}
	if src.pkCalls != 1 {
		t.Errorf("primary key not discovered before the refusal: %d calls", src.pkCalls)
	}
	if src.calls != 0 || wh.calls != 0 {
		t.Fatalf("key values were read: source %d, sink %d calls", src.calls, wh.calls)
	}

	// The sink side is checked too: a source key is not a sink key.
	src = &keySrc{countSrc: countSrc{counts: []int64{1}}, srcKeys: []string{"1"}, pk: "id"}
	wh = &keyWh{countWh: countWh{counts: []int64{1}}, whKeys: []string{"1"}, pk: "order_id"}
	_, err = rcRun(t, rcKeyDeps(src, wh, nil), "orders", `{"table":"public.orders","since":"2026-10-14T03:00:00Z","until":"2026-10-14T03:30:00Z","key_column":"id"}`)
	if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), `must be the primary key column of analytics.orders ("order_id")`) {
		t.Fatalf("sink key_column: %v", err)
	}
	if src.calls != 0 || wh.calls != 0 {
		t.Fatalf("key values were read: source %d, sink %d calls", src.calls, wh.calls)
	}
}

// Warehouse names fold by kind: a sink that reports "ID" accepts "id" on
// Snowflake and Databricks, while Postgres column names are case-sensitive.
func TestReconcileKeysColumnCaseByKind(t *testing.T) {
	for _, c := range []struct {
		kind, chosen, pk string
		ok               bool
	}{
		{"snowflake", "id", "ID", true},
		{"databricks", "id", "ID", true},
		{"", "id", "ID", false},
		{"postgres", "id", "ID", false},
	} {
		src := &keySrc{countSrc: countSrc{counts: []int64{1}}, srcKeys: []string{"a"}, pk: "id"}
		wh := &keyWh{countWh: countWh{counts: []int64{1}}, whKeys: []string{"a"}, pk: c.pk}
		d := rcKeyDeps(src, wh, nil)
		d.WarehouseKind = c.kind
		in := fmt.Sprintf(`{"table":"public.orders","since":"2026-10-14T03:00:00Z","until":"2026-10-14T03:30:00Z","key_column":%q}`, c.chosen)
		res, err := rcRun(t, d, "orders", in)
		if !c.ok {
			if !errors.Is(err, tools.ErrBadInput) || src.calls != 0 || wh.calls != 0 {
				t.Errorf("%q: err=%v, source %d, sink %d calls", c.kind, err, src.calls, wh.calls)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", c.kind, err)
			continue
		}
		// The catalog spelling is what the client reads.
		if got := res.Data.(ReconcileData).Keys.SinkColumn; got != c.pk {
			t.Errorf("%q: sink column = %q, want %q", c.kind, got, c.pk)
		}
		if wh.got.key != c.pk {
			t.Errorf("%q: WindowKeys key = %q, want %q", c.kind, wh.got.key, c.pk)
		}
	}
}

// The source's and the sink's primary keys are discovered separately.
func TestReconcileKeysDifferingColumns(t *testing.T) {
	src := &keySrc{countSrc: countSrc{counts: []int64{1}}, srcKeys: []string{"1"}}
	wh := &keyWh{countWh: countWh{counts: []int64{1}}, whKeys: []string{"1"}, pk: "order_id"}
	res, err := rcRun(t, rcKeyDeps(src, wh, nil), "orders", rcIn)
	if err != nil {
		t.Fatal(err)
	}
	found := res.Data.(ReconcileData).Keys
	if found.Column != "id" || found.SinkColumn != "order_id" || src.got.key != "id" || wh.got.key != "order_id" {
		t.Fatalf("keys = %+v; source %+v sink %+v", found, src.got, wh.got)
	}
	if !strings.Contains(res.Summary, "keys by id:") {
		t.Errorf("summary names the source column only:\n%s", res.Summary)
	}
}

// A table without a single-column primary key leaves the key section out; an
// explicit key_column is allowed only there (no primary key to compare
// against) and on a client that does not report one.
func TestReconcileKeysNoUsablePrimaryKey(t *testing.T) {
	for _, c := range []struct {
		err  error
		want string
	}{
		{clients.ErrNotFound, "public.orders has no primary key; pass key_column"},
		{clients.ErrNotSupported, "public.orders has a composite primary key"},
	} {
		src := &keySrc{countSrc: countSrc{counts: []int64{1}}, pkErr: c.err}
		res, err := rcRun(t, rcKeyDeps(src, &keyWh{countWh: countWh{counts: []int64{1}}}, nil), "orders", rcIn)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(res.Summary, c.want) || src.calls != 0 {
			t.Errorf("%v: %s (calls %d)", c.err, res.Summary, src.calls)
		}
	}
	// No primary key: the caller's column is the only row identity there is.
	src := &keySrc{countSrc: countSrc{counts: []int64{1}}, srcKeys: []string{"a"}, pkErr: clients.ErrNotFound}
	wh := &keyWh{countWh: countWh{counts: []int64{1}}, whKeys: []string{"a"}, pkErr: clients.ErrNotFound}
	res, err := rcRun(t, rcKeyDeps(src, wh, nil), "orders", `{"table":"public.orders","since":"2026-10-14T03:00:00Z","until":"2026-10-14T03:30:00Z","key_column":"order_id"}`)
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Data.(ReconcileData).Keys; got == nil || got.Column != "order_id" || got.SinkColumn != "order_id" {
		t.Fatalf("keys = %+v", res.Data)
	}
	// A composite key has no single column a key_column could name.
	src = &keySrc{countSrc: countSrc{counts: []int64{1}}, pkErr: clients.ErrNotSupported}
	_, err = rcRun(t, rcKeyDeps(src, &keyWh{countWh: countWh{counts: []int64{1}}}, nil), "orders", `{"table":"public.orders","since":"2026-10-14T03:00:00Z","until":"2026-10-14T03:30:00Z","key_column":"id"}`)
	if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "composite primary key") || src.calls != 0 {
		t.Fatalf("composite with key_column: err = %v, calls %d", err, src.calls)
	}
}

// A failed key read (or primary-key lookup) leaves the bucket table and adds a
// note; only a refused key_column fails the call.
func TestReconcileKeysReadErrorsAreNotes(t *testing.T) {
	cases := []struct {
		name string
		src  *keySrc
		wh   *keyWh
		want string
	}{
		{"source keys", &keySrc{countSrc: countSrc{counts: []int64{1}}, keysErr: errors.New("pg keys down")},
			&keyWh{countWh: countWh{counts: []int64{1}}}, "keys not compared: reading the source keys of public.orders failed (pg keys down)"},
		{"sink keys", &keySrc{countSrc: countSrc{counts: []int64{1}}, srcKeys: []string{"1"}},
			&keyWh{countWh: countWh{counts: []int64{1}}, keysErr: errors.New("wh down")}, "keys not compared: reading the sink keys of analytics.orders failed (wh down)"},
		{"source primary key", &keySrc{countSrc: countSrc{counts: []int64{1}}, pkErr: errors.New("catalog down")},
			&keyWh{countWh: countWh{counts: []int64{1}}}, "keys not compared: the primary key of public.orders could not be read (catalog down)"},
	}
	for _, c := range cases {
		res, err := rcRun(t, rcKeyDeps(c.src, c.wh, nil), "orders", rcIn)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if res.Data.(ReconcileData).Keys != nil {
			t.Errorf("%s: partial key diff reported", c.name)
		}
		if !strings.Contains(res.Summary, c.want) || !strings.Contains(res.Summary, "bucket | source rows | topic events | sink rows") {
			t.Errorf("%s: summary:\n%s", c.name, res.Summary)
		}
	}
}

// Scope is checked before any key read: another pipeline's table or no room
// pipeline never reaches a client.
func TestReconcileKeysScoped(t *testing.T) {
	src := &keySrc{countSrc: countSrc{counts: []int64{1}}, srcKeys: []string{"1"}}
	d := rcKeyDeps(src, &keyWh{countWh: countWh{counts: []int64{1}}}, nil)
	_, err := rcRun(t, d, "orders", `{"table":"public.a","since":"2026-10-14T03:00:00Z","until":"2026-10-14T03:30:00Z"}`)
	if !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("other pipeline's table: %v", err)
	}
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tl, _ := reg.Get("reconcile")
	if _, err := tl.Run(context.Background(), json.RawMessage(rcIn)); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("no scope: %v", err)
	}
	if src.calls != 0 || src.pkCalls != 0 {
		t.Fatalf("refused calls reached the client: %+v", src.got)
	}
}
