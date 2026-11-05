package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"gopkg.in/yaml.v3"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// The recovery breaks (X2e) run against p3World (scenarios_p3_fake_test.go):
// the exact SQL and compose calls, kfake for the topics and the group. What
// no fake shows is the broker deleting segments: the log start read is a test
// seam (logStartFn), as retention-loss's committed-below-log-start read is.
// The integration checks in dev/scenarios.md §18-§19 are not run here (no
// Docker).

var recoveryScenarios = []string{"rows-lost-after-retention", "consumer-skipped-range"}

func TestRecoveryScenariosRegistered(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(filepath.Join(root, recoveryOwnersFile))
	if err != nil {
		t.Fatal(err)
	}
	p, ok := own.Pipeline("orders")
	if !ok || p.ReprocessTopic != reprocessTopic {
		t.Fatalf("%s: pipeline orders reprocess_topic = %q, want %s", recoveryOwnersFile, p.ReprocessTopic, reprocessTopic)
	}
	b, err := os.ReadFile(filepath.Join(root, "dev", "scenarios.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	for _, name := range recoveryScenarios {
		sc, ok := scenarios[name]
		if !ok || sc.breakIt == nil || sc.fix == nil {
			t.Fatalf("scenario %q not registered with a break and a fix", name)
		}
		if sc.owners != recoveryOwnersFile || len(sc.signals) != 0 {
			t.Errorf("%s: owners %q signals %v: lag is 0, so an alert opens the room on %s", name, sc.owners, sc.signals, recoveryOwnersFile)
		}
		var a struct {
			Title, Severity string
			PipelineID      string `json:"pipeline_id"`
			DedupKey        string `json:"dedup_key"`
		}
		if err := json.Unmarshal([]byte(sc.alert), &a); err != nil {
			t.Fatalf("alert of %q is not JSON: %v", name, err)
		}
		if a.Title == "" || a.Severity == "" || a.PipelineID != p.ID || a.DedupKey != "devctl-"+name {
			t.Errorf("alert of %q = %+v", name, a)
		}
		if !strings.Contains(doc, "`"+name+"` —") {
			t.Errorf("dev/scenarios.md does not document %s", name)
		}
	}
}

// dev/owners.recovery.yaml is dev/owners.yaml plus the reprocess_topic.
func TestOwnersRecoveryMatchesBaseline(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	read := func(name string) map[string]any {
		b, err := os.ReadFile(filepath.Join(root, "dev", name))
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := yaml.Unmarshal(b, &m); err != nil {
			t.Fatalf("dev/%s: %v", name, err)
		}
		return m
	}
	base, rec := read("owners.yaml"), read(filepath.Base(recoveryOwnersFile))
	pipe := func(m map[string]any) map[string]any { return m["pipelines"].([]any)[0].(map[string]any) }
	if _, ok := pipe(base)["reprocess_topic"]; ok {
		t.Errorf("baseline has reprocess_topic: it belongs in %s only", recoveryOwnersFile)
	}
	delete(pipe(rec), "reprocess_topic")
	if fmt.Sprint(base) != fmt.Sprint(rec) {
		t.Errorf("%s drifted from dev/owners.yaml beyond reprocess_topic:\nbase %v\nrec  %v", recoveryOwnersFile, base, rec)
	}
}

// cutRetention is the broker deleting every segment but the active ones: the
// log start of each partition is its end. It marks the fake's retention cut.
func (w *p3World) cutRetention(ctx context.Context, topic string) (map[int32]int64, error) {
	ends, err := kadm.NewClient(w.raw).ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	w.with(func() { w.retentionCut = true })
	out := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) { out[o.Partition] = o.Offset })
	return out, nil
}

// topicExists reads the topic through the code under test's own reader (a
// deleted topic answers UnknownTopicOrPartition).
func (w *p3World) topicExists(topic string) bool {
	s := &stack{cfg: config.Config{KafkaBrokers: strings.Join(w.addrs, ",")}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, ok, err := s.topicSpan(ctx, topic)
	if err != nil {
		w.t.Fatalf("offsets of %s: %v", topic, err)
	}
	return ok
}

func (w *p3World) missing(lo, hi int64) int {
	n := 0
	w.with(func() {
		for id := lo; id <= hi; id++ {
			if !w.loaded[id] {
				n++
			}
		}
	})
	return n
}

func TestRowsLostAfterRetentionBreakAndFix(t *testing.T) {
	w := newP3World(t)
	s, logs := w.stack()
	s.batchGap = 5 * time.Millisecond
	s.logStartFn = w.cutRetention
	ctx := w.ctx()

	if err := breakRowsLostAfterRetention(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	var lo, hi, last int64
	w.with(func() {
		lo, hi, last = w.lostLo, w.lostHi, w.lastBatchLo
		if !w.consumerUp || !w.signalExists || w.connCfg["signal.data.collection"] != "public.devctl_signal" {
			t.Errorf("after break: consumer up %v, signal table %v, connector config %v", w.consumerUp, w.signalExists, w.connCfg)
		}
	})
	if hi-lo+1 != lostBatches*lostBatchSize || last != hi-lostBatchSize+1 {
		t.Fatalf("lost orders %d..%d, last batch from %d", lo, hi, last)
	}
	// Everything but the last batch was deleted before order-sync read it.
	if got := w.missing(lo, hi); got != (lostBatches-1)*lostBatchSize {
		t.Errorf("%d orders missing, want %d", got, (lostBatches-1)*lostBatchSize)
	}
	if !w.topicExists(reprocessTopic) {
		t.Errorf("the break did not create %s", reprocessTopic)
	}
	if !strings.Contains(logs.String(), "missing_from_warehouse=40") {
		t.Errorf("log does not name the missing count:\n%s", logs)
	}

	if err := fixRowsLostAfterRetention(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	if got := w.missing(lo, hi); got != 0 {
		t.Errorf("after fix %d orders missing", got)
	}
	w.with(func() {
		if w.resnapshots != 1 || w.signalExists {
			t.Errorf("after fix: %d re-snapshot signals, signal table %v", w.resnapshots, w.signalExists)
		}
		if _, ok := w.connCfg["signal.data.collection"]; ok {
			t.Errorf("connector config not back to the baseline: %v", w.connCfg)
		}
	})
	if w.topicExists(reprocessTopic) {
		t.Errorf("the fix left %s", reprocessTopic)
	}
	// A second fix sends no signal: nothing is missing.
	if err := fixRowsLostAfterRetention(ctx, s); err != nil {
		t.Fatalf("second fix: %v", err)
	}
	w.with(func() {
		if w.resnapshots != 1 {
			t.Errorf("second fix sent another signal (%d)", w.resnapshots)
		}
	})
}

// Error path: the broker never deletes the old segments (retention not
// applied); the break says so instead of leaving a stack with nothing lost.
func TestRowsLostAfterRetentionNothingDeleted(t *testing.T) {
	w := newP3World(t)
	s, _ := w.stack()
	s.batchGap = 5 * time.Millisecond
	s.logStartFn = func(context.Context, string) (map[int32]int64, error) {
		return map[int32]int64{0: 0, 1: 0, 2: 0}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := breakRowsLostAfterRetention(ctx, s)
	if err == nil || !strings.Contains(err.Error(), "retention did not delete") {
		t.Fatalf("break = %v", err)
	}
}

func TestConsumerSkippedRangeBreakAndFix(t *testing.T) {
	w := newP3World(t)
	s, logs := w.stack()
	ctx := w.ctx()

	if err := breakConsumerSkippedRange(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	var lo, hi int64
	w.with(func() {
		lo, hi = w.skippedLo, w.skippedHi
		if !w.consumerUp || !w.committedAtEnd() {
			t.Errorf("after break: consumer up %v, committed at end %v", w.consumerUp, w.committedAtEnd())
		}
	})
	if got := w.missing(lo, hi); got != skippedOrders {
		t.Errorf("%d orders missing, want %d", got, skippedOrders)
	}
	if !w.topicExists(reprocessTopic) {
		t.Errorf("the break did not create %s", reprocessTopic)
	}
	if err := fixConsumerSkippedRange(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	if got := w.missing(lo, hi); got != 0 {
		t.Errorf("after fix %d orders missing", got)
	}
	if w.topicExists(reprocessTopic) {
		t.Errorf("the fix left %s", reprocessTopic)
	}
	if err := fixConsumerSkippedRange(ctx, s); err != nil {
		t.Fatalf("second fix: %v", err)
	}
}

// Error path: the offsets are back at the start when order-sync comes up
// (someone replayed the topic meanwhile), so it loads the orders; the break
// says the reset did not skip them.
func TestConsumerSkippedRangeNotSkipped(t *testing.T) {
	w := newP3World(t)
	s, _ := w.stack()
	s.composeFn = func(ctx context.Context, env map[string]string, args ...string) error {
		if slices.Contains(args, "up") {
			if err := s.resetGroupToStart(ctx, ordersGroup, ordersTopic); err != nil {
				return err
			}
		}
		return w.compose(ctx, env, args...)
	}
	err := breakConsumerSkippedRange(w.ctx(), s)
	if err == nil || !strings.Contains(err.Error(), "did not skip them") {
		t.Fatalf("break = %v", err)
	}
}

// Error path: order-sync is not running, so there is no committed position
// to lose or skip from; both breaks refuse before they write anything.
func TestRecoveryBreaksNeedRunningConsumer(t *testing.T) {
	for _, name := range recoveryScenarios {
		t.Run(name, func(t *testing.T) {
			w := newP3World(t)
			w.consumerUp = false
			s, _ := w.stack()
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			err := scenarios[name].breakIt(ctx, s)
			if err == nil || !strings.Contains(err.Error(), "order-sync is not running") {
				t.Fatalf("break = %v", err)
			}
			w.with(func() {
				if len(w.sql) != 0 {
					t.Errorf("SQL sent although the break refused: %v", w.sql)
				}
			})
		})
	}
}

// The signal row is the one pg_resnapshot_rows' writer stores: an
// incremental snapshot of public.orders with Debezium's additional-conditions.
func TestResnapshotSignalData(t *testing.T) {
	q := sqlResnapshotSignal(1301, 1340)
	i := strings.Index(q, "'{")
	j := strings.LastIndex(q, "}'")
	if i < 0 || j < i {
		t.Fatalf("no JSON literal in %s", q)
	}
	data := strings.ReplaceAll(q[i+1:j+1], "''", "'")
	var d struct {
		Collections []string `json:"data-collections"`
		Type        string   `json:"type"`
		Conditions  []struct {
			Collection string `json:"data-collection"`
			Filter     string `json:"filter"`
		} `json:"additional-conditions"`
	}
	if err := json.Unmarshal([]byte(data), &d); err != nil {
		t.Fatalf("data %s: %v", data, err)
	}
	if !slices.Equal(d.Collections, []string{"public.orders"}) || d.Type != "INCREMENTAL" || len(d.Conditions) != 1 ||
		d.Conditions[0].Collection != "public.orders" || d.Conditions[0].Filter != `"id" >= '1301' AND "id" <= '1340'` {
		t.Errorf("signal data = %+v", d)
	}
}
