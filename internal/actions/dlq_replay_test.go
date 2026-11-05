package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// ---- fakes ----

// fakeDLQReader serves offsets and records from memory and counts calls.
type fakeDLQReader struct {
	mu      sync.Mutex
	offs    map[string][]clients.PartitionOffsets
	recs    map[int32][]clients.Record // the DLQ's records per partition
	offErr  error
	readErr error
	// perRead caps one ReadRecords answer, to exercise the read loop.
	perRead int
	calls   int
	// block makes every call wait for ctx (a broker that does not answer).
	block bool
}

func (f *fakeDLQReader) DLQOffsets(ctx context.Context, topics ...string) (map[string][]clients.PartitionOffsets, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.offErr != nil {
		return nil, f.offErr
	}
	out := map[string][]clients.PartitionOffsets{}
	for _, t := range topics {
		if p, ok := f.offs[t]; ok {
			out[t] = p
		}
	}
	return out, nil
}

func (f *fakeDLQReader) ReadRecords(ctx context.Context, _ string, partition int32, from int64, limit int) ([]clients.Record, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.readErr != nil {
		return nil, f.readErr
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

func (f *fakeDLQReader) n() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// fakeProducer is a KafkaWriter that records produced records; it refuses unless allowWrites,
// like every write fake here, and can fail at record failAt.
type fakeProducer struct {
	allowWrites bool
	failAt      int // -1: never
	topic       string
	produced    []clients.Record
	calls       int
}

func (f *fakeProducer) ResetGroupOffsets(context.Context, string, clients.OffsetTarget) (int, error) {
	return 0, errors.New("not used")
}

func (f *fakeProducer) Produce(_ context.Context, topic string, recs []clients.Record) (int, error) {
	f.calls++
	if !f.allowWrites {
		return 0, errWriteRefused
	}
	f.topic = topic
	for i, r := range recs {
		if i == f.failAt {
			return i, errors.New("NOT_ENOUGH_REPLICAS")
		}
		f.produced = append(f.produced, r)
	}
	return len(recs), nil
}

var (
	_ clients.KafkaWriter = (*fakeProducer)(nil)
	_ DLQRecordReader     = (*fakeDLQReader)(nil)
)

// ---- fixtures ----

const dlqReplayYAML = `
pipelines:
  - id: orders
    title: Orders CDC
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector, pg-orders-sink]
    consumer_groups: [order-sync]
    sink_tables: [analytics.orders]
    sink_ts_column: updated_at
    watch:
      dlq_topics:
        - {topic: orders-dlq, written_by: pg-orders-sink}
        - {topic: shared-dlq, written_by: order-sync, shared: true}
  - id: billing
    title: Billing
    topic_prefix: bill
    source_tables: [public.invoices]
    connectors: [bill-sink]
    consumer_groups: [bill-sync]
    sink_tables: [analytics.invoices]
    sink_ts_column: updated_at
    watch:
      dlq_topics:
        - {topic: shared-dlq, written_by: bill-sync, shared: true}
        - {topic: bill-dlq, written_by: bill-sink}
layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya
  kafka: arjun
  consumer: kabir
  warehouse: meera
`

func dlqPipeline(t *testing.T) owners.Pipeline {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(dlqReplayYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	p, ok := o.Pipeline("orders")
	if !ok {
		t.Fatal("no orders pipeline")
	}
	return p
}

const dlqTarget = "pg.public.orders"

// dlqRec is one DLQ record with Connect's error headers and an app header.
func dlqRec(part int32, off int64, key string) clients.Record {
	return clients.Record{
		Topic: "orders-dlq", Partition: part, Offset: off,
		Key: []byte(key), Value: []byte(fmt.Sprintf(`{"v":%d}`, off)),
		Headers: []clients.RecordHeader{
			{Key: "trace-id", Value: []byte("t-" + key)},
			{Key: "__connect.errors.topic", Value: []byte(dlqTarget)},
			{Key: "__connect.errors.exception.message", Value: []byte("boom")},
		},
	}
}

// newDLQFake: orders-dlq has one partition with offsets 10..14 (start 10,
// end 15); the target topic exists.
func newDLQFake() *fakeDLQReader {
	f := &fakeDLQReader{
		offs: map[string][]clients.PartitionOffsets{
			"orders-dlq": {{Partition: 0, Start: 10, End: 15}},
			"shared-dlq": {{Partition: 0, Start: 0, End: 2}},
			dlqTarget:    {{Partition: 0, Start: 0, End: 100}},
		},
		recs: map[int32][]clients.Record{},
	}
	for o := int64(10); o < 15; o++ {
		f.recs[0] = append(f.recs[0], dlqRec(0, o, fmt.Sprintf("k%d", o)))
	}
	return f
}

func dlqParams(t *testing.T, mods map[string]any) json.RawMessage {
	t.Helper()
	m := map[string]any{"dlq_topic": "orders-dlq", "to_topic": dlqTarget, "count": 3}
	for k, v := range mods {
		if v == nil {
			delete(m, k)
			continue
		}
		m[k] = v
	}
	return raw(t, m)
}

// ---- tests ----

func TestDLQReplayCheckScope(t *testing.T) {
	pl := dlqPipeline(t)
	a := &dlqReplay{}
	tests := []struct {
		name string
		mods map[string]any
		want string // "" = allowed
	}{
		{"in scope", nil, ""},
		{"DLQ of another pipeline", map[string]any{"dlq_topic": "bill-dlq"}, "watch.dlq_topics"},
		{"CDC topic as DLQ", map[string]any{"dlq_topic": "pg.public.orders", "to_topic": "pg.public.orders"}, "watch.dlq_topics"},
		{"target of another pipeline", map[string]any{"to_topic": "bill.public.invoices"}, "<topic_prefix>.<source_table>"},
		{"target is a DLQ", map[string]any{"to_topic": "shared-dlq"}, "CDC topic of this pipeline"},
		{"missing topics", map[string]any{"to_topic": nil}, "required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := a.CheckScope(pl, dlqParams(t, tc.mods))
			if tc.want == "" {
				if err != nil {
					t.Fatalf("CheckScope: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CheckScope = %v, want error containing %q", err, tc.want)
			}
			if tc.name != "missing topics" && !errors.Is(err, tools.ErrOutOfScope) {
				t.Fatalf("CheckScope = %v, want tools.ErrOutOfScope", err)
			}
		})
	}
}

func TestDLQReplayValidate(t *testing.T) {
	pl := dlqPipeline(t)
	multi := func(f *fakeDLQReader) {
		f.offs["orders-dlq"] = []clients.PartitionOffsets{{Partition: 0, Start: 10, End: 15}, {Partition: 1, Start: 0, End: 4}}
	}
	tests := []struct {
		name    string
		mods    map[string]any
		setup   func(*fakeDLQReader)
		noRead  bool // no Reader configured
		scoped  bool // ctx carries the room's pipeline
		want    string
		wantIs  error
		noCalls bool // refused before any I/O
	}{
		{name: "ok", mods: nil},
		{name: "ok scoped", scoped: true},
		{name: "count over 1000", mods: map[string]any{"count": 1001}, want: "over the 1000", noCalls: true},
		{name: "count zero", mods: map[string]any{"count": 0}, want: "want 1 to 1000", noCalls: true},
		{name: "unknown param", mods: map[string]any{"offset": 3}, want: "unknown field", noCalls: true},
		{name: "same topic", mods: map[string]any{"to_topic": "orders-dlq"}, want: "the DLQ itself", noCalls: true},
		{name: "negative from", mods: map[string]any{"from_offset": -1}, want: "negative from_offset", noCalls: true},
		{name: "out of scope target", scoped: true, mods: map[string]any{"to_topic": "bill.public.invoices"}, wantIs: tools.ErrOutOfScope, want: "must be a CDC topic of this pipeline (<topic_prefix>.<source_table>)", noCalls: true},
		{name: "out of scope dlq", scoped: true, mods: map[string]any{"dlq_topic": "bill-dlq"}, wantIs: tools.ErrOutOfScope, noCalls: true},
		{name: "no reader", noRead: true, want: "no Kafka client configured"},
		{name: "dlq missing", setup: func(f *fakeDLQReader) { delete(f.offs, "orders-dlq") }, wantIs: clients.ErrNotFound, want: "DLQ topic"},
		{name: "dlqTarget missing", setup: func(f *fakeDLQReader) { delete(f.offs, dlqTarget) }, wantIs: clients.ErrNotFound, want: "to_topic"},
		{name: "empty DLQ", setup: func(f *fakeDLQReader) { f.offs["orders-dlq"] = []clients.PartitionOffsets{{Start: 15, End: 15}} }, want: "is empty"},
		{name: "from below start", mods: map[string]any{"from_offset": 3}, want: "below the log start"},
		{name: "from at end", mods: map[string]any{"from_offset": 15}, want: "nothing left"},
		{name: "several partitions, none named", setup: multi, want: "has 2 partitions; set partition"},
		{name: "several partitions, one named", setup: multi, mods: map[string]any{"partition": 1, "from_offset": 0}},
		{name: "unknown partition", mods: map[string]any{"partition": 4}, wantIs: clients.ErrNotFound},
		{name: "broker down", setup: func(f *fakeDLQReader) { f.offErr = errors.New("dial tcp: connection refused") }, want: "connection refused"},
		{name: "timeout", setup: func(f *fakeDLQReader) { f.block = true }, wantIs: context.DeadlineExceeded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDLQFake()
			if tc.setup != nil {
				tc.setup(f)
			}
			d := KafkaDeps{Records: f, validateTimeout: 50 * time.Millisecond}
			if tc.noRead {
				d.Records = nil
			}
			a := &dlqReplay{d: d}
			ctx := context.Background()
			if tc.scoped {
				ctx = tools.WithPipeline(ctx, pl)
			}
			err := a.Validate(ctx, dlqParams(t, tc.mods))
			if tc.want == "" && tc.wantIs == nil {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
			} else {
				if err == nil {
					t.Fatal("Validate passed, want refused")
				}
				if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("Validate = %v, want it to contain %q", err, tc.want)
				}
				if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
					t.Fatalf("Validate = %v, want errors.Is %v", err, tc.wantIs)
				}
			}
			if tc.noCalls && f.n() != 0 {
				t.Fatalf("%d client calls, want none before the refusal", f.n())
			}
		})
	}
}

func TestDLQReplayValidateReadsNoRecords(t *testing.T) {
	f := newDLQFake()
	f.readErr = errors.New("Validate must not read records")
	a := &dlqReplay{d: KafkaDeps{Records: f}}
	if err := a.Validate(context.Background(), dlqParams(t, nil)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestDLQReplayPreview(t *testing.T) {
	pl := dlqPipeline(t)
	room := tools.WithPipeline(context.Background(), pl)
	f := newDLQFake()
	f.recs[0][0].Key = []byte("user=alice@example.com")
	f.recs[0][1].Key = []byte{0, 0, 0, 0, 7, 2, 0x84}
	// Write mode off: no writer at all, and the preview still works.
	a := &dlqReplay{d: KafkaDeps{Records: f}}
	res, err := a.Preview(room, dlqParams(t, nil))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	for _, want := range []string{
		"orders-dlq partition 0 offsets 10–12 → topic pg.public.orders",
	} {
		if !strings.Contains(res.Target, want) {
			t.Fatalf("Target = %q, want %q", res.Target, want)
		}
	}
	for _, want := range []string{
		"3 records copied to pg.public.orders in DLQ offset order",
		"next_from_offset will be 13",
		"binary key, 7 bytes",
		`"k12"`,
		"__connect.errors.* headers are dropped (6 on these records)",
		"duplicates if the cause was not fixed",
		"its end offset stays 15",
	} {
		if !strings.Contains(res.Expected, want) {
			t.Fatalf("Expected = %q\nwant it to contain %q", res.Expected, want)
		}
	}
	if strings.Contains(res.Expected, "alice@example.com") {
		t.Fatalf("Expected shows an unredacted key: %q", res.Expected)
	}
	if !strings.Contains(res.Current, "from_offset not set") || !strings.Contains(res.Current, "3 of the 3 records name pg.public.orders") {
		t.Fatalf("Current = %q", res.Current)
	}
	if res.Reversible {
		t.Fatal("a replay is not reversible")
	}
	for _, s := range []string{res.Target, res.Current, res.Expected} {
		if strings.Contains(s, `{"v":`) {
			t.Fatalf("preview carries a record value: %q", s)
		}
	}
}

func TestDLQReplayPreviewRefusals(t *testing.T) {
	pl := dlqPipeline(t)
	room := tools.WithPipeline(context.Background(), pl)
	tests := []struct {
		name   string
		ctx    context.Context
		mods   map[string]any
		setup  func(*fakeDLQReader)
		want   string
		wantIs error
		noIO   bool
	}{
		{name: "no scope", ctx: context.Background(), wantIs: tools.ErrNoScope, noIO: true},
		{name: "pipeline id only", ctx: tools.WithPipelineID(context.Background(), "orders"), wantIs: tools.ErrNoScope, noIO: true},
		{name: "out of scope", ctx: room, mods: map[string]any{"to_topic": "bill.public.invoices"}, wantIs: tools.ErrOutOfScope, noIO: true},
		{name: "origin header names another topic", ctx: room, setup: func(f *fakeDLQReader) {
			f.recs[0][1].Headers[1].Value = []byte("pg.public.customers")
		}, want: `came from topic "pg.public.customers"`},
		{name: "shared DLQ record without origin", ctx: room, mods: map[string]any{"dlq_topic": "shared-dlq"}, setup: func(f *fakeDLQReader) {
			f.recs[0] = []clients.Record{{Offset: 0, Key: []byte("a")}, {Offset: 1, Key: []byte("b")}}
		}, want: "shared with other pipelines"},
		{name: "read fails", ctx: room, setup: func(f *fakeDLQReader) { f.readErr = errors.New("fetch: NOT_LEADER") }, want: "NOT_LEADER"},
		{name: "only markers", ctx: room, setup: func(f *fakeDLQReader) { f.recs[0] = nil }, want: "nothing to replay"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDLQFake()
			if tc.setup != nil {
				tc.setup(f)
			}
			a := &dlqReplay{d: KafkaDeps{Records: f}}
			_, err := a.Preview(tc.ctx, dlqParams(t, tc.mods))
			if err == nil {
				t.Fatal("Preview passed, want refused")
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Preview = %v, want %q", err, tc.want)
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Fatalf("Preview = %v, want errors.Is %v", err, tc.wantIs)
			}
			if tc.noIO && f.n() != 0 {
				t.Fatalf("%d client calls before the refusal, want none", f.n())
			}
		})
	}
}

func TestDLQReplayExecute(t *testing.T) {
	pl := dlqPipeline(t)
	room := tools.WithPipeline(context.Background(), pl)
	f := newDLQFake()
	f.perRead = 2 // the action must loop over short reads, in order
	w := &fakeProducer{allowWrites: true, failAt: -1}
	a := &dlqReplay{d: KafkaDeps{Records: f, Writer: w}}
	out, err := a.Execute(room, dlqParams(t, map[string]any{"count": 4, "from_offset": 11}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if w.topic != dlqTarget || len(w.produced) != 4 {
		t.Fatalf("produced %d records to %q, want 4 to %s", len(w.produced), w.topic, dlqTarget)
	}
	for i, r := range w.produced {
		if r.Offset != int64(11+i) {
			t.Fatalf("record %d has DLQ offset %d, want %d (in order)", i, r.Offset, 11+i)
		}
		if string(r.Key) != fmt.Sprintf("k%d", 11+i) || string(r.Value) != fmt.Sprintf(`{"v":%d}`, 11+i) {
			t.Fatalf("record %d key/value changed: %q %q", i, r.Key, r.Value)
		}
		if len(r.Headers) != 1 || r.Headers[0].Key != "trace-id" {
			t.Fatalf("record %d headers = %+v, want only trace-id (connect headers dropped)", i, r.Headers)
		}
	}
	// The reader's records keep their headers: stripping copies.
	if len(f.recs[0][1].Headers) != 3 {
		t.Fatal("stripping changed the records read")
	}
	for _, want := range []string{"offsets 11 to 14", "acked 4 of 4", "8 __connect.errors.* headers dropped", "next_from_offset 15", "0 offsets left"} {
		if !strings.Contains(out, want) {
			t.Fatalf("Execute = %q, want %q", out, want)
		}
	}
	if strings.Contains(out, "k11") || strings.Contains(out, `"v"`) {
		t.Fatalf("Execute output carries record bytes: %q", out)
	}
}

func TestDLQReplayExecuteStopsAtFirstProduceError(t *testing.T) {
	room := tools.WithPipeline(context.Background(), dlqPipeline(t))
	w := &fakeProducer{allowWrites: true, failAt: 2}
	a := &dlqReplay{d: KafkaDeps{Records: newDLQFake(), Writer: w}}
	_, err := a.Execute(room, dlqParams(t, map[string]any{"count": 5}))
	if err == nil {
		t.Fatal("Execute passed, want the produce error")
	}
	for _, want := range []string{"acked 2 of 5", "offsets 10 to 11 copied", "next_from_offset 12", "NOT_ENOUGH_REPLICAS"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Execute = %v, want %q", err, want)
		}
	}
	if len(w.produced) != 2 {
		t.Fatalf("produced %d, want 2", len(w.produced))
	}
}

func TestDLQReplayExecuteRefusals(t *testing.T) {
	pl := dlqPipeline(t)
	room := tools.WithPipeline(context.Background(), pl)
	tests := []struct {
		name     string
		ctx      context.Context
		noWriter bool
		mods     map[string]any
		setup    func(*fakeDLQReader)
		want     string
		wantIs   error
		noIO     bool
	}{
		{name: "write mode off", ctx: room, noWriter: true, want: "PIPEROOM_WRITE_MODE is off", noIO: true},
		{name: "no scope", ctx: context.Background(), wantIs: tools.ErrNoScope, noIO: true},
		{name: "out of scope DLQ", ctx: room, mods: map[string]any{"dlq_topic": "bill-dlq"}, wantIs: tools.ErrOutOfScope, noIO: true},
		{name: "count over 1000", ctx: room, mods: map[string]any{"count": 5000}, want: "over the 1000", noIO: true},
		{name: "broker down", ctx: room, setup: func(f *fakeDLQReader) { f.offErr = errors.New("dial tcp: connection refused") }, want: "connection refused"},
		{name: "empty DLQ", ctx: room, setup: func(f *fakeDLQReader) { f.offs["orders-dlq"] = []clients.PartitionOffsets{{Start: 15, End: 15}} }, want: "is empty"},
		{name: "foreign origin", ctx: room, setup: func(f *fakeDLQReader) { f.recs[0][0].Headers[1].Value = []byte("other") }, want: "came from topic"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDLQFake()
			if tc.setup != nil {
				tc.setup(f)
			}
			w := &fakeProducer{allowWrites: true, failAt: -1}
			d := KafkaDeps{Records: f, Writer: w}
			if tc.noWriter {
				d.Writer = nil
			}
			a := &dlqReplay{d: d}
			_, err := a.Execute(tc.ctx, dlqParams(t, tc.mods))
			if err == nil {
				t.Fatal("Execute passed, want refused")
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Execute = %v, want %q", err, tc.want)
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Fatalf("Execute = %v, want errors.Is %v", err, tc.wantIs)
			}
			if w.calls != 0 {
				t.Fatalf("writer called %d times, want none", w.calls)
			}
			if tc.noIO && f.n() != 0 {
				t.Fatalf("%d reads before the refusal, want none", f.n())
			}
		})
	}
}

func TestDLQReplayExecuteTimeout(t *testing.T) {
	room := tools.WithPipeline(context.Background(), dlqPipeline(t))
	f := newDLQFake()
	f.block = true
	w := &fakeProducer{allowWrites: true, failAt: -1}
	a := &dlqReplay{d: KafkaDeps{Records: f, Writer: w}}
	ctx, cancel := context.WithTimeout(room, 50*time.Millisecond)
	defer cancel()
	_, err := a.Execute(ctx, dlqParams(t, nil))
	if !errors.Is(err, context.DeadlineExceeded) || w.calls != 0 {
		t.Fatalf("Execute = %v (writer calls %d), want deadline exceeded and no produce", err, w.calls)
	}
}

func TestDLQReplayReadStopsAtCount(t *testing.T) {
	f := newDLQFake()
	f.perRead = 1
	a := &dlqReplay{d: KafkaDeps{Records: f}}
	recs, err := a.read(context.Background(), dlqReplayParams{DLQTopic: "orders-dlq", Count: 2}, replaySpan{from: 10, start: 10, end: 15})
	if err != nil || len(recs) != 2 || recs[1].Offset != 11 {
		t.Fatalf("read = %d records, %v; want offsets 10, 11", len(recs), err)
	}
}

func TestKeyPreview(t *testing.T) {
	tests := []struct {
		key  []byte
		want string
	}{
		{nil, "null key"},
		{[]byte(`{"id":42}`), `"{\"id\":42}"`},
		{[]byte{0, 1, 2}, "binary key, 3 bytes"},
		{[]byte{0xff, 0xfe}, "binary key, 2 bytes"},
		{[]byte(strings.Repeat("a", 100)), `"` + strings.Repeat("a", 60) + `…"`},
	}
	for _, tc := range tests {
		if got := keyPreview(tc.key); got != tc.want {
			t.Errorf("keyPreview(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}
	if got := keyPreview([]byte("password=hunter2")); strings.Contains(got, "hunter2") {
		t.Errorf("keyPreview did not redact: %q", got)
	}
}

// TestKeyPreviewRedactsBeforeCut covers keys longer than the cap whose secret
// straddles rune 60: cutting first broke the pattern and left the head bare.
func TestKeyPreviewRedactsBeforeCut(t *testing.T) {
	const jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4ifQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
	tests := []struct {
		name   string
		key    string
		leaked []string // none of these may appear in the preview
	}{
		{"bare jwt", "order-" + jwt, []string{"eyJhbGciOiJIUzI1NiIs", "eyJzdWIi", "SflKxwRJ", "ssw5c"}},
		{"token jwt", "order-42|token=" + jwt, []string{"eyJhbGciOiJIUzI1NiIs", "eyJzdWIi", "SflKxwRJ", "ssw5c"}},
		{"password", strings.Repeat("0", 50) + "|password=Sup3rS3cretValueThatIsLong", []string{"Sup3", "S3cret", "IsLong"}},
		{"multibyte prefix", strings.Repeat("é", 40) + "|password=Sup3rS3cretValueThatIsLong", []string{"Sup3", "S3cret", "IsLong"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := keyPreview([]byte(tc.key))
			for _, l := range tc.leaked {
				if strings.Contains(got, l) {
					t.Errorf("keyPreview(%q) = %q, leaks %q", tc.key, got, l)
				}
			}
			s, err := strconv.Unquote(got)
			if err != nil {
				t.Fatalf("keyPreview = %q, not a quoted string: %v", got, err)
			}
			if !utf8.ValidString(s) || utf8.RuneCountInString(strings.TrimSuffix(s, "…")) > dlqKeyRunes {
				t.Errorf("keyPreview = %q, want valid UTF-8 of at most %d runes plus …", s, dlqKeyRunes)
			}
		})
	}
}

func TestDLQReplayRegister(t *testing.T) {
	reg := NewRegistry()
	if err := RegisterKafka(reg, KafkaDeps{}); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("kafka_reset_consumer_offsets"); !ok {
		t.Fatal("RegisterKafka no longer registers kafka_reset_consumer_offsets")
	}
	a, ok := reg.Get("dlq_replay")
	if !ok || a.Layer() != "kafka" {
		t.Fatalf("dlq_replay registered = %v, layer %q", ok, a.Layer())
	}
	if _, ok := a.(Scoped); !ok {
		t.Fatal("dlq_replay is not Scoped")
	}
	// Registered with write mode off (no clients): Validate refuses cleanly.
	if err := a.Validate(context.Background(), dlqParams(t, nil)); err == nil || !strings.Contains(err.Error(), "no Kafka client") {
		t.Fatalf("Validate without clients = %v", err)
	}
}
