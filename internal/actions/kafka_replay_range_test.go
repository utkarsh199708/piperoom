package actions

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

// ---- fakes and fixtures ----

const (
	rrTopic     = "pg.public.orders"
	rrReprocess = "orders-reprocess"
)

// fakeRangeReader serves a source topic's offsets and records from memory
// and offsets by time from ratesFn; it counts calls.
type fakeRangeReader struct {
	mu      sync.Mutex
	offs    map[string][]clients.PartitionOffsets
	recs    map[int32][]clients.Record
	perRead int
	ratesFn func(since time.Time, topic string) []clients.PartitionRate
	calls   int
}

func (f *fakeRangeReader) hit() {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
}

func (f *fakeRangeReader) DLQOffsets(_ context.Context, topics ...string) (map[string][]clients.PartitionOffsets, error) {
	f.hit()
	out := map[string][]clients.PartitionOffsets{}
	for _, t := range topics {
		if p, ok := f.offs[t]; ok {
			out[t] = p
		}
	}
	return out, nil
}

func (f *fakeRangeReader) ReadRecords(_ context.Context, topic string, partition int32, from int64, limit int) ([]clients.Record, error) {
	f.hit()
	if topic != rrTopic {
		return nil, fmt.Errorf("read of %s", topic)
	}
	if f.perRead > 0 {
		limit = min(limit, f.perRead)
	}
	var out []clients.Record
	for _, r := range f.recs[partition] {
		if r.Offset >= from && len(out) < limit {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeRangeReader) PartitionRates(_ context.Context, since time.Time, topics ...string) ([]clients.PartitionRate, error) {
	f.hit()
	var out []clients.PartitionRate
	for _, t := range topics {
		out = append(out, f.ratesFn(since, t)...)
	}
	return out, nil
}

func (f *fakeRangeReader) n() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

var _ clients.RateReader = (*fakeRangeReader)(nil)

// rangeReader: pg.public.orders has 3 partitions of offsets 0..299, the log
// of partition 2 starts at 50; each record carries a Connect error header
// and an application header. The reprocess topic exists and is empty.
func rangeReader() *fakeRangeReader {
	f := &fakeRangeReader{
		offs: map[string][]clients.PartitionOffsets{
			rrTopic:     {{Partition: 0, Start: 0, End: 300}, {Partition: 1, Start: 0, End: 300}, {Partition: 2, Start: 50, End: 300}},
			rrReprocess: {{Partition: 0}},
		},
		recs: map[int32][]clients.Record{},
	}
	for p := int32(0); p < 3; p++ {
		for o := int64(0); o < 300; o++ {
			if p == 2 && o < 50 {
				continue
			}
			f.recs[p] = append(f.recs[p], clients.Record{Topic: rrTopic, Partition: p, Offset: o,
				Key: fmt.Appendf(nil, "k-%d-%d", p, o), Value: fmt.Appendf(nil, "v-%d-%d", p, o),
				Headers: []clients.RecordHeader{{Key: "__connect.errors.topic", Value: []byte("x")}, {Key: "trace-id", Value: []byte("t")}}})
		}
	}
	return f
}

var rrPipeline = owners.Pipeline{
	ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"},
	Connectors: []string{"pg-orders-connector"}, ReprocessTopic: rrReprocess,
}

func rrCtx() context.Context { return tools.WithPipeline(context.Background(), rrPipeline) }

func rrParams(t *testing.T, override map[string]any) json.RawMessage {
	t.Helper()
	m := map[string]any{
		"topic": rrTopic, "to_topic": rrReprocess,
		"ranges": []map[string]any{{"partition": 1, "from_offset": 100, "to_offset": 199}},
	}
	for k, v := range override {
		if v == nil {
			delete(m, k)
		} else {
			m[k] = v
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// bulkWriter is a fakeProducer with ProduceAll, which the action prefers.
type bulkWriter struct {
	*fakeProducer
	bulk int
}

var _ clients.BulkProducer = (*bulkWriter)(nil)

func (b *bulkWriter) ProduceAll(ctx context.Context, topic string, recs []clients.Record) (int, error) {
	b.bulk++
	return b.Produce(ctx, topic, recs)
}

// written makes ratesFn report n records written to the reprocess topic.
func written(n int64) func(time.Time, string) []clients.PartitionRate {
	return func(_ time.Time, topic string) []clients.PartitionRate {
		return []clients.PartitionRate{{Topic: topic, Partition: 0, End: n, OffsetAtStart: 0, Records: n}}
	}
}

// ---- tests ----

func TestReplayRangeRegistersAsRecovery(t *testing.T) {
	reg := NewRegistry()
	if err := RegisterKafka(reg, KafkaDeps{}); err != nil {
		t.Fatal(err)
	}
	a, ok := reg.Get(replayRangeName)
	if !ok || ClassOf(a) != ClassRecovery || a.Layer() != "kafka" {
		t.Fatalf("registered=%v", ok)
	}
}

// TC1 (fakes) and E1: Validate, Preview, Execute, Verify; chunked reads keep
// order.
func TestReplayRangeHappyPath(t *testing.T) {
	f := rangeReader()
	f.perRead = 64
	f.ratesFn = written(100)
	w := &bulkWriter{fakeProducer: &fakeProducer{allowWrites: true, failAt: -1}}
	a := &replayRange{d: KafkaDeps{Records: f, Writer: w}}
	ctx, params := rrCtx(), rrParams(t, nil)

	if err := a.Validate(ctx, params); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := a.CheckScope(rrPipeline, params); err != nil {
		t.Fatalf("CheckScope: %v", err)
	}
	res, err := a.Preview(ctx, params)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if res.Count == nil || *res.Count != 100 || res.Unit != events.RecoveryUnitRecords || res.Reversible {
		t.Fatalf("preview count/unit = %v %q", res.Count, res.Unit)
	}
	for _, want := range []string{"partition 1: offsets 100 to 199 (100)", `"k-1-100"`, "will re-emit 100 records"} {
		if !strings.Contains(res.Expected, want) {
			t.Fatalf("Expected = %q, want %q", res.Expected, want)
		}
	}
	if !strings.Contains(res.Target, "→ reprocess topic "+rrReprocess) {
		t.Fatalf("Target = %q", res.Target)
	}

	out, err := a.Execute(ctx, params)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if w.topic != rrReprocess || len(w.produced) != 100 || w.bulk == 0 {
		t.Fatalf("produced %d to %q (bulk calls %d)", len(w.produced), w.topic, w.bulk)
	}
	for i, r := range w.produced {
		if want := fmt.Sprintf("k-1-%d", 100+i); string(r.Key) != want {
			t.Fatalf("record %d key %q, want %q (order)", i, r.Key, want)
		}
	}
	if !strings.Contains(out, "acked 100 records of 100 offsets") || !strings.Contains(out, "p1 100–199") {
		t.Fatalf("result %q", out)
	}

	v, err := a.Verify(ctx, params, time.Now())
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if v.Before != 100 || v.After != 0 || v.Unit != events.RecoveryUnitRecords || len(v.Detail) > 200 {
		t.Fatalf("Verify = %+v", v)
	}
	if events.RecoveryOutcomeFor(v.Before, v.After) != events.RecoveryRecovered {
		t.Fatalf("outcome %q", events.RecoveryOutcomeFor(v.Before, v.After))
	}
}

// TC2: a range below the log start is refused with the alternative named,
// and nothing is produced.
func TestReplayRangeBelowLogStart(t *testing.T) {
	f := rangeReader()
	w := &fakeProducer{allowWrites: true, failAt: -1}
	a := &replayRange{d: KafkaDeps{Records: f, Writer: w}}
	params := rrParams(t, map[string]any{"ranges": []map[string]any{{"partition": 2, "from_offset": 10, "to_offset": 60}}})
	ctx := rrCtx()
	_, perr := a.Preview(ctx, params)
	_, xerr := a.Execute(ctx, params)
	for name, err := range map[string]error{"Validate": a.Validate(ctx, params), "Preview": perr, "Execute": xerr} {
		if err == nil || !strings.Contains(err.Error(), "pg_resnapshot_rows") || !strings.Contains(err.Error(), "log start 50") {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if w.calls != 0 {
		t.Fatalf("%d produce calls", w.calls)
	}

	// A time window whose first record is the log start may reach into the
	// deleted part: refused too.
	f.ratesFn = func(since time.Time, topic string) []clients.PartitionRate {
		off := int64(50)
		if since.After(time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)) {
			off = 120
		}
		return []clients.PartitionRate{{Topic: topic, Partition: 2, End: 300, OffsetAtStart: off}}
	}
	win := rrParams(t, map[string]any{"ranges": nil, "from_time": "2026-10-01T00:00:00Z", "to_time": "2026-10-03T00:00:00Z", "partitions": []int{2}})
	if err := a.Validate(ctx, win); err == nil || !strings.Contains(err.Error(), "pg_resnapshot_rows") {
		t.Fatalf("window below log start: %v", err)
	}
	if w.calls != 0 {
		t.Fatalf("%d produce calls", w.calls)
	}
}

// TC3: over the cap, refused with the count; an offset range before any
// I/O.
func TestReplayRangeOverCap(t *testing.T) {
	f := rangeReader()
	a := &replayRange{d: KafkaDeps{Records: f}}
	params := rrParams(t, map[string]any{"ranges": []map[string]any{{"partition": 0, "from_offset": 0, "to_offset": 100_000}}})
	if err := a.Validate(rrCtx(), params); err == nil || !strings.Contains(err.Error(), "100001 records") {
		t.Fatalf("Validate: %v", err)
	}
	if f.n() != 0 {
		t.Fatalf("%d reads before refusing", f.n())
	}

	// A time window is counted after resolving it.
	f.offs[rrTopic] = []clients.PartitionOffsets{{Partition: 0, End: 200_000}}
	f.ratesFn = func(since time.Time, topic string) []clients.PartitionRate {
		off := int64(0)
		if since.Hour() == 12 {
			off = 150_001
		}
		return []clients.PartitionRate{{Topic: topic, Partition: 0, End: 200_000, OffsetAtStart: off}}
	}
	win := rrParams(t, map[string]any{"ranges": nil, "from_time": "2026-10-01T00:00:00Z", "to_time": "2026-10-01T12:00:00Z"})
	_, err := a.Preview(rrCtx(), win)
	if err == nil || !strings.Contains(err.Error(), "150001 records") {
		t.Fatalf("Preview window over cap: %v", err)
	}
}

// A time window resolves to [first offset at from_time, first offset at
// to_time) per partition; a partition with nothing in it is left out.
func TestReplayRangeTimeWindow(t *testing.T) {
	f := rangeReader()
	f.ratesFn = func(since time.Time, topic string) []clients.PartitionRate {
		if since.Hour() == 0 {
			return []clients.PartitionRate{{Topic: topic, Partition: 0, End: 300, OffsetAtStart: 10}, {Topic: topic, Partition: 1, End: 300, OffsetAtStart: 300}, {Topic: topic, Partition: 2, End: 300, OffsetAtStart: 60}}
		}
		return []clients.PartitionRate{{Topic: topic, Partition: 0, End: 300, OffsetAtStart: 20}, {Topic: topic, Partition: 1, End: 300, OffsetAtStart: 300}, {Topic: topic, Partition: 2, End: 300, OffsetAtStart: 65}}
	}
	w := &fakeProducer{allowWrites: true, failAt: -1}
	a := &replayRange{d: KafkaDeps{Records: f, Writer: w}}
	win := rrParams(t, map[string]any{"ranges": nil, "from_time": "2026-10-01T00:00:00Z", "to_time": "2026-10-01T06:00:00Z"})
	res, err := a.Preview(rrCtx(), win)
	if err != nil {
		t.Fatal(err)
	}
	if *res.Count != 15 || !strings.Contains(res.Expected, "partition 0: offsets 10 to 19 (10); partition 2: offsets 60 to 64 (5)") {
		t.Fatalf("count %d, Expected %q", *res.Count, res.Expected)
	}
	if _, err := a.Execute(rrCtx(), win); err != nil || len(w.produced) != 15 {
		t.Fatalf("Execute: %v, %d produced", err, len(w.produced))
	}

	// Without offsets by time: not supported, never a guess.
	b := &replayRange{d: KafkaDeps{Records: struct{ DLQRecordReader }{f}}}
	if err := b.Validate(rrCtx(), win); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("no RateReader: %v", err)
	}
	if _, err := b.Verify(rrCtx(), rrParams(t, nil), time.Now()); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("Verify without RateReader: %v", err)
	}
}

// TC4: Connect's __connect.errors.* headers are dropped, every other header,
// the key and the value are kept.
func TestReplayRangeHeaders(t *testing.T) {
	w := &fakeProducer{allowWrites: true, failAt: -1}
	a := &replayRange{d: KafkaDeps{Records: rangeReader(), Writer: w}}
	out, err := a.Execute(rrCtx(), rrParams(t, map[string]any{"ranges": []map[string]any{{"partition": 0, "from_offset": 5, "to_offset": 6}}}))
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range w.produced {
		if len(r.Headers) != 1 || r.Headers[0].Key != "trace-id" || string(r.Value) != fmt.Sprintf("v-0-%d", 5+i) {
			t.Fatalf("record %d = %+v", i, r)
		}
	}
	if !strings.Contains(out, "2 __connect.errors.* headers dropped") {
		t.Fatalf("result %q", out)
	}
}

// TC5, E2: a topic outside the pipeline, another target or no declared
// reprocess_topic is refused before any I/O.
func TestReplayRangeScope(t *testing.T) {
	noReprocess := rrPipeline
	noReprocess.ReprocessTopic = ""
	tests := []struct {
		name   string
		pl     owners.Pipeline
		params map[string]any
		want   error
	}{
		{"foreign topic", rrPipeline, map[string]any{"topic": "bill.public.invoices"}, tools.ErrOutOfScope},
		{"other target", rrPipeline, map[string]any{"to_topic": "pg.public.orders-copy"}, tools.ErrOutOfScope},
		{"no reprocess_topic", noReprocess, nil, tools.ErrBadInput},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := rangeReader()
			w := &fakeProducer{allowWrites: true, failAt: -1}
			a := &replayRange{d: KafkaDeps{Records: f, Writer: w}}
			ctx, params := tools.WithPipeline(context.Background(), tc.pl), rrParams(t, tc.params)
			_, perr := a.Preview(ctx, params)
			_, xerr := a.Execute(ctx, params)
			_, verr := a.Verify(ctx, params, time.Now())
			for name, err := range map[string]error{
				"CheckScope": a.CheckScope(tc.pl, params), "Validate": a.Validate(ctx, params),
				"Preview": perr, "Execute": xerr, "Verify": verr,
			} {
				if !errors.Is(err, tc.want) {
					t.Errorf("%s: %v, want %v", name, err, tc.want)
				}
			}
			if f.n() != 0 || w.calls != 0 {
				t.Fatalf("%d reads, %d produce calls before refusing", f.n(), w.calls)
			}
		})
	}

	// Preview and Execute fail closed without the room's pipeline.
	f := rangeReader()
	a := &replayRange{d: KafkaDeps{Records: f, Writer: &fakeProducer{allowWrites: true, failAt: -1}}}
	if _, err := a.Preview(context.Background(), rrParams(t, nil)); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("Preview without scope: %v", err)
	}
	if _, err := a.Execute(context.Background(), rrParams(t, nil)); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("Execute without scope: %v", err)
	}
	if f.n() != 0 {
		t.Fatalf("%d reads", f.n())
	}
}

func TestReplayRangeBadParams(t *testing.T) {
	a := &replayRange{d: KafkaDeps{Records: rangeReader(), now: func() time.Time { return time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC) }}}
	for name, override := range map[string]map[string]any{
		"unknown field":   {"count": 5},
		"no topic":        {"topic": nil},
		"same topic":      {"to_topic": rrTopic},
		"neither":         {"ranges": nil},
		"both":            {"from_time": "2026-10-01T00:00:00Z", "to_time": "2026-10-02T00:00:00Z"},
		"to before from":  {"ranges": []map[string]any{{"partition": 0, "from_offset": 9, "to_offset": 3}}},
		"missing offset":  {"ranges": []map[string]any{{"partition": 0, "from_offset": 9}}},
		"dup partition":   {"ranges": []map[string]any{{"partition": 0, "from_offset": 1, "to_offset": 2}, {"partition": 0, "from_offset": 5, "to_offset": 6}}},
		"partitions+rng":  {"partitions": []int{0}},
		"future to_time":  {"ranges": nil, "from_time": "2026-10-21T00:00:00Z", "to_time": "2026-10-23T00:00:00Z"},
		"reversed window": {"ranges": nil, "from_time": "2026-10-02T00:00:00Z", "to_time": "2026-10-01T00:00:00Z"},
		"bad time":        {"ranges": nil, "from_time": "yesterday", "to_time": "2026-10-01T00:00:00Z"},
	} {
		if err := a.Validate(rrCtx(), rrParams(t, override)); !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("%s: %v, want bad input", name, err)
		}
	}
	// Past the end of the partition: not written yet.
	err := a.Validate(rrCtx(), rrParams(t, map[string]any{"ranges": []map[string]any{{"partition": 0, "from_offset": 290, "to_offset": 300}}}))
	if err == nil || !strings.Contains(err.Error(), "not written yet") {
		t.Fatalf("past end: %v", err)
	}
	// A missing reprocess topic is named.
	f := rangeReader()
	delete(f.offs, rrReprocess)
	b := &replayRange{d: KafkaDeps{Records: f}}
	if err := b.Validate(rrCtx(), rrParams(t, nil)); !errors.Is(err, clients.ErrNotFound) || !strings.Contains(err.Error(), "create it first") {
		t.Fatalf("missing reprocess topic: %v", err)
	}
}

// TC6: the preview's keys are redacted as dlq_replay's; no raw key bytes
// anywhere in the preview.
func TestReplayRangePreviewRedactsKeys(t *testing.T) {
	const jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4ifQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
	f := rangeReader()
	f.recs[1][100].Key = []byte("user=alice@example.com")
	f.recs[1][101].Key = []byte("order-42|token=" + jwt)
	f.recs[1][102].Key = []byte{0, 0, 0, 0, 7, 2, 0x84}
	a := &replayRange{d: KafkaDeps{Records: f}}
	res, err := a.Preview(rrCtx(), rrParams(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res)
	for _, raw := range []string{"alice@example.com", "eyJhbGciOiJIUzI1NiIs", "SflKxwRJ", "v-1-100", "\\u0000\\u0000"} {
		if strings.Contains(string(b), raw) {
			t.Fatalf("preview shows %q: %s", raw, b)
		}
	}
	if !strings.Contains(res.Expected, "binary key, 7 bytes") {
		t.Fatalf("Expected = %q", res.Expected)
	}
}

// TC7, E5: a produce failure mid-way reports the acked count and fails the
// step; Verify then measures a partial repair.
func TestReplayRangeProduceFailure(t *testing.T) {
	f := rangeReader()
	f.ratesFn = written(30)
	w := &fakeProducer{allowWrites: true, failAt: 30}
	a := &replayRange{d: KafkaDeps{Records: f, Writer: w}}
	ctx, params := rrCtx(), rrParams(t, nil)
	_, err := a.Execute(ctx, params)
	if err == nil || !strings.Contains(err.Error(), "acked 30 of 100 records") || !strings.Contains(err.Error(), "NOT_ENOUGH_REPLICAS") {
		t.Fatalf("Execute: %v", err)
	}
	v, err := a.Verify(ctx, params, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if v.Before != 100 || v.After != 70 || events.RecoveryOutcomeFor(v.Before, v.After) != events.RecoveryPartial {
		t.Fatalf("Verify = %+v", v)
	}

	// Write mode off: Execute refuses, Verify still reads.
	off := &replayRange{d: KafkaDeps{Records: f}}
	if _, err := off.Execute(ctx, params); err == nil || !strings.Contains(err.Error(), "no Kafka write client") {
		t.Fatalf("no writer: %v", err)
	}
	if _, err := off.Verify(ctx, params, time.Time{}); err == nil {
		t.Fatal("Verify without executedAt")
	}
}
