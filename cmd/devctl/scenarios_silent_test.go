package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/events"
)

// The silent scenarios run against p3World (scenarios_p3_fake_test.go): the
// exact SQL and compose calls, kfake for the topic. What no fake can show is
// `devctl health` staying green and Debezium's real placeholder; those are
// the integration checks in dev/scenarios.md (not run here: no Docker).

func TestSilentScenariosRegistered(t *testing.T) {
	want := map[string]string{
		"sink-filtered-rows":  events.SignalCountDrift,
		"sink-dropped-column": events.SignalRowDrift,
		"consumer-duplicates": events.SignalDupKeys,
		"toast-placeholder":   events.SignalRowDrift,
	}
	for name, sig := range want {
		sc, ok := scenarios[name]
		if !ok {
			t.Fatalf("scenario %q is not registered", name)
		}
		if sc.breakIt == nil || sc.fix == nil {
			t.Errorf("%s: break or fix missing", name)
		}
		if len(sc.signals) != 1 || sc.signals[0] != sig {
			t.Errorf("%s: signals = %v, want [%s]", name, sc.signals, sig)
		}
		if sc.alert != "" || sc.owners != "" {
			t.Errorf("%s: alert %q owners %q: the watcher opens the room on the baseline owners file", name, sc.alert, sc.owners)
		}
	}
}

// TC4: dev/scenarios.md documents each scenario with its watcher signal.
func TestSilentScenariosDocumented(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, "dev", "scenarios.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	for _, name := range []string{"sink-filtered-rows", "sink-dropped-column", "consumer-duplicates", "toast-placeholder"} {
		i := strings.Index(doc, "`"+name+"` —")
		if i < 0 {
			t.Fatalf("dev/scenarios.md does not document %s", name)
		}
		// the signal named in the section after the heading
		rest := doc[i:]
		if j := strings.Index(rest, "\n### "); j > 0 {
			rest = rest[:j]
		}
		if sig := scenarios[name].signals[0]; !strings.Contains(rest, sig) {
			t.Errorf("section of %s does not name its signal %s", name, sig)
		}
	}
}

func TestSinkFilteredRowsBreakAndFix(t *testing.T) {
	w := newP3World(t)
	s, logs := w.stack()
	ctx := w.ctx()

	if err := breakSinkFilteredRows(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.with(func() {
		if w.skipWhere != "status=backfill" || !w.consumerUp {
			t.Errorf("after break: build skip_where %q, consumer up %v", w.skipWhere, w.consumerUp)
		}
		if w.backfillSrc-w.backfillSink != backfillOrders {
			t.Errorf("source has %d backfill orders, sink %d: want %d missing", w.backfillSrc, w.backfillSink, backfillOrders)
		}
	})
	// TC5: break twice, fix once.
	if err := breakSinkFilteredRows(ctx, s); err != nil {
		t.Fatalf("second break: %v", err)
	}
	w.with(func() {
		if w.backfillSrc-w.backfillSink != 2*backfillOrders {
			t.Errorf("after two breaks %d missing, want %d", w.backfillSrc-w.backfillSink, 2*backfillOrders)
		}
	})
	if err := fixSinkFilteredRows(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	w.with(func() {
		if w.skipWhere != "" || w.backfillSrc != w.backfillSink {
			t.Errorf("after fix: skip_where %q source %d sink %d", w.skipWhere, w.backfillSrc, w.backfillSink)
		}
	})
	// E2: the second fix is a harmless replay.
	if err := fixSinkFilteredRows(ctx, s); err != nil {
		t.Fatalf("second fix: %v", err)
	}
	// The break leaves the stack running: lag 0, Stable, one member.
	if g, _ := w.group(ctx, ordersGroup); g.state != "Stable" || g.members != 1 || g.lag != 0 {
		t.Errorf("group after fix = %+v", g)
	}
}

// Error path: the knob does not reach the container, so the build loads the
// backfill orders; the break says so instead of claiming a broken stack.
func TestSinkFilteredRowsBreakNotSkipped(t *testing.T) {
	w := newP3World(t)
	s, _ := w.stack()
	s.composeFn = func(ctx context.Context, env map[string]string, args ...string) error {
		delete(env, "DEVCONSUMER_SKIP_WHERE")
		return w.compose(ctx, env, args...)
	}
	err := breakSinkFilteredRows(w.ctx(), s)
	if err == nil || !strings.Contains(err.Error(), "did not skip them") {
		t.Fatalf("break = %v", err)
	}
}

// Stack down: the group never becomes Stable; a clear error, no SQL written.
func TestSinkFilteredRowsStackDown(t *testing.T) {
	w := newP3World(t)
	s, _ := w.stack()
	s.groupFn = func(context.Context, string) (groupView, error) { return groupView{}, context.DeadlineExceeded }
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := breakSinkFilteredRows(ctx, s)
	if err == nil || !strings.Contains(err.Error(), "did not join the group") {
		t.Fatalf("break = %v", err)
	}
	w.with(func() {
		if w.backfillSrc != 0 {
			t.Errorf("break wrote %d rows although the consumer never joined", w.backfillSrc)
		}
	})
}

func TestSinkDroppedColumnBreakAndFix(t *testing.T) {
	w := newP3World(t)
	s, logs := w.stack()
	ctx := w.ctx()

	if err := breakSinkDroppedColumn(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.with(func() {
		if w.dropColumns != "updated_at" || w.nullTS != droppedOrders {
			t.Errorf("after break: drop_columns %q, %d NULL updated_at, want %d", w.dropColumns, w.nullTS, droppedOrders)
		}
	})
	if err := breakSinkDroppedColumn(ctx, s); err != nil {
		t.Fatalf("second break: %v", err)
	}
	w.with(func() {
		if w.nullTS != 2*droppedOrders {
			t.Errorf("after two breaks %d NULL, want %d", w.nullTS, 2*droppedOrders)
		}
	})
	if err := fixSinkDroppedColumn(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	w.with(func() {
		if w.dropColumns != "" || w.nullTS != 0 {
			t.Errorf("after fix: drop_columns %q, %d NULL", w.dropColumns, w.nullTS)
		}
	})
	if err := fixSinkDroppedColumn(ctx, s); err != nil {
		t.Fatalf("second fix: %v", err)
	}
}

// Error path: an empty sink has no timestamp to pin the rows to; the break
// refuses before it restarts anything.
func TestSinkDroppedColumnEmptySink(t *testing.T) {
	w := newP3World(t)
	w.sinkEmpty = true
	s, _ := w.stack()
	err := breakSinkDroppedColumn(w.ctx(), s)
	if err == nil || !strings.Contains(err.Error(), "no updated_at yet") {
		t.Fatalf("break = %v", err)
	}
	w.with(func() {
		if w.dropColumns != "" {
			t.Error("the build was restarted although the break refused")
		}
	})
}

// commitToEnd plays devconsumer at lag 0: order-sync's committed offsets are
// the end of every partition of the orders topic.
func commitToEnd(t *testing.T, w *p3World) {
	t.Helper()
	adm := kadm.NewClient(w.raw)
	ctx := w.ctx()
	ends, err := adm.ListEndOffsets(ctx, ordersTopic)
	if err != nil {
		t.Fatal(err)
	}
	if err := adm.CommitAllOffsets(ctx, ordersGroup, ends.Offsets()); err != nil {
		t.Fatal(err)
	}
}

func TestConsumerDuplicatesBreakAndFix(t *testing.T) {
	w := newP3World(t)
	s, logs := w.stack()
	ctx := w.ctx()

	if err := breakConsumerDuplicates(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	n, err := s.duplicateEvents(ctx)
	if err != nil || n != dupCopies {
		t.Fatalf("after break: %d duplicate events (err %v), want %d", n, err, dupCopies)
	}
	// The copies are of the newest event of ten distinct keys, so the topic
	// holds 2 x 10 records and each key's value is byte-identical.
	span, _, _ := s.topicSpan(ctx, ordersTopic)
	if span.records() != dupSourceOrders+dupCopies {
		t.Errorf("topic has %d records, want %d", span.records(), dupSourceOrders+dupCopies)
	}
	// TC5: break twice, fix once. The second break copies the copies.
	if err := breakConsumerDuplicates(ctx, s); err != nil {
		t.Fatalf("second break: %v", err)
	}
	if n, _ := s.duplicateEvents(ctx); n < 2*dupCopies {
		t.Errorf("after two breaks %d duplicates, want at least %d", n, 2*dupCopies)
	}

	commitToEnd(t, w) // devconsumer's commits at lag 0
	if err := fixConsumerDuplicates(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	if n, err := s.duplicateEvents(ctx); err != nil || n != 0 {
		t.Fatalf("after fix: %d duplicates (err %v)", n, err)
	}
	// health reads the newest record: the topic is never emptied.
	span, ok, err := s.topicSpan(ctx, ordersTopic)
	if err != nil || !ok || span.records() == 0 {
		t.Fatalf("topic after fix: %v %v %v", span, ok, err)
	}
	// E2: a second fix finds nothing to do and cuts nothing.
	before := span.records()
	if err := fixConsumerDuplicates(ctx, s); err != nil {
		t.Fatalf("second fix: %v", err)
	}
	if after, _, _ := s.topicSpan(ctx, ordersTopic); after.records() != before {
		t.Errorf("second fix changed the topic: %d -> %d records", before, after.records())
	}
	if !strings.Contains(logs.String(), "skipped") {
		t.Errorf("second fix did not say it skipped:\n%s", logs)
	}
}

// Error path: the consumer has not committed what the duplicates occupy, so
// the cut would delete records it still needs; the fix waits and fails, and
// the topic is untouched.
func TestConsumerDuplicatesFixBehindConsumer(t *testing.T) {
	w := newP3World(t)
	s, _ := w.stack()
	if err := breakConsumerDuplicates(w.ctx(), s); err != nil {
		t.Fatal(err)
	}
	before, _, _ := s.topicSpan(w.ctx(), ordersTopic)
	s.groupFn = func(context.Context, string) (groupView, error) {
		return groupView{state: "Stable", members: 1, lag: 7}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := fixConsumerDuplicates(ctx, s)
	if err == nil || !strings.Contains(err.Error(), "caught up before the topic is cut") {
		t.Fatalf("fix = %v", err)
	}
	if after, _, _ := s.topicSpan(w.ctx(), ordersTopic); after.records() != before.records() {
		t.Errorf("topic changed: %d -> %d records", before.records(), after.records())
	}
}

// Stack down: the consumer is stopped, so the break stops at its first check
// and writes nothing.
func TestConsumerDuplicatesBreakConsumerDown(t *testing.T) {
	w := newP3World(t)
	w.consumerUp = false
	s, _ := w.stack()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := breakConsumerDuplicates(ctx, s)
	if err == nil || !strings.Contains(err.Error(), "must be running and caught up") {
		t.Fatalf("break = %v", err)
	}
	if _, ok, _ := s.topicSpan(w.ctx(), ordersTopic); ok {
		if span, _, _ := s.topicSpan(w.ctx(), ordersTopic); span.records() != 0 {
			t.Errorf("the break wrote %d records", span.records())
		}
	}
}

func rec(key, value string) *kgo.Record {
	r := &kgo.Record{Key: []byte(key)}
	if value != "" {
		r.Value = []byte(value)
	}
	return r
}

func TestCountDuplicateEvents(t *testing.T) {
	tests := []struct {
		name string
		recs []*kgo.Record
		want int64
	}{
		{"none", []*kgo.Record{rec("a", "1"), rec("b", "1"), rec("a", "2")}, 0},
		{"one repeat", []*kgo.Record{rec("a", "1"), rec("b", "1"), rec("a", "1")}, 1},
		{"three copies", []*kgo.Record{rec("a", "1"), rec("a", "1"), rec("a", "1")}, 2},
		// key "ab"+value "c" is not key "a"+value "bc"
		{"boundary", []*kgo.Record{rec("ab", "c"), rec("a", "bc")}, 0},
		{"tombstones are not events", []*kgo.Record{rec("a", ""), rec("a", "")}, 0},
		{"empty", nil, 0},
	}
	for _, tt := range tests {
		if got := countDuplicateEvents(tt.recs); got != tt.want {
			t.Errorf("%s: %d, want %d", tt.name, got, tt.want)
		}
	}
}

func TestDuplicateCandidates(t *testing.T) {
	recs := []*kgo.Record{
		rec("a", "1"), rec("b", "1"), rec("a", "2"), rec("c", ""), rec("", "x"), rec("d", "1"), rec("b", "2"),
	}
	var got []string
	for _, r := range duplicateCandidates(recs, 10) {
		got = append(got, string(r.Key)+string(r.Value))
	}
	// the newest record of each key, in original order; tombstone and empty key skipped
	if want := "a2,d1,b2"; strings.Join(got, ",") != want {
		t.Errorf("candidates = %v, want %s", got, want)
	}
	if n := len(duplicateCandidates(recs, 2)); n != 2 {
		t.Errorf("limit 2 gave %d", n)
	}
	if duplicateCandidates(nil, 5) != nil {
		t.Error("nil input should give nil")
	}
}

func TestToastPlaceholderBreakAndFix(t *testing.T) {
	w := newP3World(t)
	s, logs := w.stack()
	ctx := w.ctx()

	if err := breakToastPlaceholder(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.with(func() {
		if w.riFull || w.placeholders != toastOrders {
			t.Errorf("after break: replica identity full %v, %d placeholder rows, want default and %d", w.riFull, w.placeholders, toastOrders)
		}
	})
	if err := breakToastPlaceholder(ctx, s); err != nil {
		t.Fatalf("second break: %v", err)
	}
	w.with(func() {
		if w.placeholders != 2*toastOrders {
			t.Errorf("after two breaks %d placeholder rows, want %d", w.placeholders, 2*toastOrders)
		}
	})
	if err := fixToastPlaceholder(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	w.with(func() {
		if !w.riFull || w.placeholders != 0 {
			t.Errorf("after fix: replica identity full %v, %d placeholder rows", w.riFull, w.placeholders)
		}
	})
	if err := fixToastPlaceholder(ctx, s); err != nil {
		t.Fatalf("second fix: %v", err)
	}
	if !strings.Contains(logs.String(), "skipped") {
		t.Errorf("second fix did not skip the rewrite:\n%s", logs)
	}
}

// The order matters: the replica identity is set before the large rows are
// written and updated, and the repair statement only ever contains integers.
func TestToastPlaceholderStatementOrder(t *testing.T) {
	w := newP3World(t)
	s, _ := w.stack()
	if err := breakToastPlaceholder(w.ctx(), s); err != nil {
		t.Fatal(err)
	}
	if err := fixToastPlaceholder(w.ctx(), s); err != nil {
		t.Fatal(err)
	}
	var seq []string
	w.with(func() {
		for _, q := range w.sql {
			switch {
			case q == sqlReplicaDefault:
				seq = append(seq, "default")
			case q == sqlInsertToast:
				seq = append(seq, "insert")
			case strings.HasPrefix(q, "UPDATE orders SET amount"):
				seq = append(seq, "touch")
			case q == sqlReplicaFull:
				seq = append(seq, "full")
			case strings.HasPrefix(q, "UPDATE orders SET status"):
				seq = append(seq, "repair")
			}
		}
	})
	if got, want := strings.Join(seq, " "), "default insert touch full repair"; got != want {
		t.Errorf("statements = %q, want %q", got, want)
	}
}

func TestSQLBuilders(t *testing.T) {
	if got := sqlToastRepair([]int64{1, 22}); got != "UPDATE orders SET status = status || '', updated_at = now() WHERE id IN (1,22)" {
		t.Errorf("repair = %s", got)
	}
	if got := sqlToastTouch(5, 7); !strings.HasSuffix(got, "WHERE id BETWEEN 5 AND 7") {
		t.Errorf("touch = %s", got)
	}
	q := sqlPinnedInsert(1760000000000123)
	if !strings.Contains(q, "'epoch'::timestamptz + (1760000000000123 * interval '1 microsecond')") || !strings.Contains(q, "generate_series(1, 20)") {
		t.Errorf("pinned = %s", q)
	}
	for _, in := range []string{"", " "} {
		if ids, err := parseIDList(in); err != nil || len(ids) != 0 {
			t.Errorf("parseIDList(%q) = %v, %v", in, ids, err)
		}
	}
	if ids, err := parseIDList("1, 2,3"); err != nil || len(ids) != 3 || ids[2] != 3 {
		t.Errorf("parseIDList = %v, %v", ids, err)
	}
	// anything that is not an integer is refused: the ids go back into SQL
	if _, err := parseIDList("1,2; DROP TABLE orders"); err == nil {
		t.Error("parseIDList accepted SQL")
	}
}
