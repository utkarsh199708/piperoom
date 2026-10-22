package kafka

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/dlq"
)

// fakeLister answers ListStart/EndOffsets from fixed tables and counts calls.
type fakeLister struct {
	starts, ends kadm.ListedOffsets
	stable       kadm.ListedOffsets // last stable offsets; nil means same as ends
	after        kadm.ListedOffsets // ListOffsetsAfterMilli; nil means same as starts
	stableCalls  atomic.Int32
	err          error
	calls        atomic.Int32
	lastTopics   []string
	sawDeadline  bool
}

func (f *fakeLister) ListStartOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	f.calls.Add(1)
	f.lastTopics = topics
	_, f.sawDeadline = ctx.Deadline()
	return f.starts, f.err
}

func (f *fakeLister) ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	f.calls.Add(1)
	return f.ends, f.err
}

func (f *fakeLister) ListCommittedOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	f.stableCalls.Add(1)
	if f.stable == nil {
		return f.ends, f.err
	}
	return f.stable, f.err
}

func (f *fakeLister) ListOffsetsAfterMilli(ctx context.Context, _ int64, topics ...string) (kadm.ListedOffsets, error) {
	f.calls.Add(1)
	if f.after == nil {
		return f.starts, f.err
	}
	return f.after, f.err
}

func listed(topic string, offs map[int32]int64) map[string]map[int32]kadm.ListedOffset {
	m := map[int32]kadm.ListedOffset{}
	for p, o := range offs {
		m[p] = kadm.ListedOffset{Topic: topic, Partition: p, Offset: o}
	}
	return map[string]map[int32]kadm.ListedOffset{topic: m}
}

func missing(topic string) map[string]map[int32]kadm.ListedOffset {
	return map[string]map[int32]kadm.ListedOffset{topic: {-1: {Topic: topic, Partition: -1, Err: kerr.UnknownTopicOrPartition}}}
}

func merge(ms ...map[string]map[int32]kadm.ListedOffset) kadm.ListedOffsets {
	out := kadm.ListedOffsets{}
	for _, m := range ms {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

func TestDLQOffsetsEmptySetSendsNothing(t *testing.T) {
	f := &fakeLister{}
	c := &Client{offs: f}
	for _, in := range [][]string{nil, {}, {""}, {"", ""}} {
		got, err := c.DLQOffsets(context.Background(), in...)
		if err != nil || got != nil {
			t.Fatalf("DLQOffsets(%q) = %v, %v; want nil, nil", in, got, err)
		}
	}
	if n := f.calls.Load(); n != 0 {
		t.Fatalf("%d kadm requests for an empty topic list, want 0 (kadm would list the whole cluster)", n)
	}
}

func TestDLQOffsets(t *testing.T) {
	f := &fakeLister{
		starts: merge(listed("a-dlq", map[int32]int64{0: 2, 1: 0}), missing("gone-dlq")),
		ends:   merge(listed("a-dlq", map[int32]int64{0: 10, 1: 5}), missing("gone-dlq")),
	}
	c := &Client{offs: f}
	got, err := c.DLQOffsets(context.Background(), "a-dlq", "gone-dlq", "a-dlq")
	if err != nil {
		t.Fatal(err)
	}
	if n := f.calls.Load(); n != 2 {
		t.Fatalf("%d requests, want 2 (start, end)", n)
	}
	if len(f.lastTopics) != 2 {
		t.Fatalf("topics sent = %v, want de-duplicated", f.lastTopics)
	}
	if !f.sawDeadline {
		t.Fatal("request context has no deadline")
	}
	if _, ok := got["gone-dlq"]; ok {
		t.Fatal("missing topic must be absent")
	}
	want := []clients.PartitionOffsets{{Partition: 0, Start: 2, End: 10}, {Partition: 1, Start: 0, End: 5}}
	if g := got["a-dlq"]; len(g) != 2 || g[0] != want[0] || g[1] != want[1] {
		t.Fatalf("a-dlq = %+v, want %+v", g, want)
	}
	for _, p := range got["a-dlq"] {
		if !p.LastTimestamp.IsZero() {
			t.Fatalf("LastTimestamp must be zero (no record fetched): %+v", p)
		}
	}
}

func TestDLQOffsetsErrors(t *testing.T) {
	boom := errors.New("boom")
	authErr := map[string]map[int32]kadm.ListedOffset{"t": {-1: {Partition: -1, Err: kerr.TopicAuthorizationFailed}}}
	partErr := map[string]map[int32]kadm.ListedOffset{"t": {0: {Partition: 0, Err: kerr.LeaderNotAvailable}}}
	tests := []struct {
		name string
		f    *fakeLister
		want error
	}{
		{"request error", &fakeLister{err: boom}, boom},
		{"authorization is not 'missing'", &fakeLister{starts: merge(authErr), ends: merge(authErr)}, nil},
		{"partition error", &fakeLister{starts: merge(partErr), ends: merge(partErr)}, kerr.LeaderNotAvailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{offs: tt.f}
			got, err := c.DLQOffsets(context.Background(), "t")
			if err == nil {
				t.Fatalf("want error, got %v", got)
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestDLQOffsetsCallerDeadlineKept(t *testing.T) {
	f := &fakeLister{starts: merge(listed("t", map[int32]int64{0: 0})), ends: merge(listed("t", map[int32]int64{0: 1}))}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := (&Client{offs: f}).DLQOffsets(ctx, "t"); err != nil || !f.sawDeadline {
		t.Fatalf("err=%v deadline=%v", err, f.sawDeadline)
	}
}

// fakeConsumer serves a fixed Fetches once per poll and records Close.
type fakeConsumer struct {
	polls  []kgo.Fetches
	closed atomic.Bool
	idx    int
}

func (f *fakeConsumer) PollFetches(ctx context.Context) kgo.Fetches {
	if f.idx < len(f.polls) {
		fs := f.polls[f.idx]
		f.idx++
		return fs
	}
	<-ctx.Done()
	return kgo.NewErrFetch(ctx.Err())
}

func (f *fakeConsumer) Close() { f.closed.Store(true) }

func fetchOf(topic string, recs ...*kgo.Record) kgo.Fetches {
	byPart := map[int32][]*kgo.Record{}
	var order []int32
	for _, r := range recs {
		if _, ok := byPart[r.Partition]; !ok {
			order = append(order, r.Partition)
		}
		byPart[r.Partition] = append(byPart[r.Partition], r)
	}
	ft := kgo.FetchTopic{Topic: topic}
	for _, p := range order {
		ft.Partitions = append(ft.Partitions, kgo.FetchPartition{Partition: p, Records: byPart[p]})
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{ft}}}
}

func rec(p int32, off int64, ts time.Time, hs ...kgo.RecordHeader) *kgo.Record {
	return &kgo.Record{Partition: p, Offset: off, Timestamp: ts, Headers: hs, Key: []byte("key"), Value: []byte("value")}
}

func hdr(k, v string) kgo.RecordHeader { return kgo.RecordHeader{Key: k, Value: []byte(v)} }

// peekClient wires a Client to fake offsets and a fake consumer; it returns
// the requested start offsets the consumer was opened with.
func peekClient(starts, ends kadm.ListedOffsets, cons *fakeConsumer, opened *map[string]map[int32]kgo.Offset) *Client {
	return &Client{
		offs: &fakeLister{starts: starts, ends: ends},
		consumerFn: func(opts []kgo.Opt) (pollConsumer, error) {
			if opened != nil {
				*opened = consumeModeOf(opts).parts
			}
			return cons, nil
		},
	}
}

func TestPeekDLQHeaderRules(t *testing.T) {
	now := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)
	long := strings.Repeat("é", 400)
	r := rec(0, 0, now,
		hdr("__connect.errors.exception.class.name", "org.apache.kafka.connect.errors.DataException"),
		hdr("__connect.errors.stage", "VALUE_CONVERTER\x00\x1b[31m"),
		hdr("__connect.errors.class.name", long),
		hdr("__connect.errors.exception.message", `Unexpected char at [Source: (byte[])"{"email":"a@b.com"}"; line: 1]`),
		hdr("__connect.errors.exception.stacktrace", "\n\norg.x.JsonParseException: bad {\"ssn\":\"123\"} here\n\tat org.x.Foo(Foo.java:1)\n\tat y"),
		hdr("password", "hunter2"),
		hdr("traceparent", "00-abc"),
		hdr("__connect.errorsX", "no"),
		hdr("__connect.errors.topic", "pg.public.orders"),
	)
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("d", r)}}
	c := peekClient(merge(listed("d", map[int32]int64{0: 0})), merge(listed("d", map[int32]int64{0: 1})), cons, nil)
	got, err := c.PeekDLQ(context.Background(), "d", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records", len(got))
	}
	h := got[0].Headers
	for k := range h {
		if !strings.HasPrefix(k, "__connect.errors.") || k == "__connect.errorsX" {
			t.Errorf("foreign header kept: %q", k)
		}
	}
	if h["__connect.errors.stage"] != "VALUE_CONVERTER[31m" {
		t.Errorf("control characters not removed: %q", h["__connect.errors.stage"])
	}
	if n := len([]rune(h["__connect.errors.class.name"])); n != dlq.MaxMessageRunes {
		t.Errorf("class.name = %d runes, want cap %d", n, dlq.MaxMessageRunes)
	}
	msg := h["__connect.errors.exception.message"]
	if strings.Contains(msg, "a@b.com") || strings.Contains(msg, "Source") {
		t.Errorf("message not redacted: %q", msg)
	}
	st := h["__connect.errors.exception.stacktrace"]
	if strings.Contains(st, "123") || strings.Contains(st, "Foo.java") || strings.Contains(st, "\n") || !strings.HasPrefix(st, "org.x.JsonParseException") {
		t.Errorf("stacktrace not first-line + redacted: %q", st)
	}
	if h["__connect.errors.topic"] != "pg.public.orders" {
		t.Errorf("topic header lost: %v", h)
	}
	if !cons.closed.Load() {
		t.Error("consumer not closed")
	}
}

func TestPeekDLQNoHeaders(t *testing.T) {
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("d", rec(0, 0, time.Now(), hdr("app", "x")))}}
	c := peekClient(merge(listed("d", map[int32]int64{0: 0})), merge(listed("d", map[int32]int64{0: 1})), cons, nil)
	got, err := c.PeekDLQ(context.Background(), "d", 1)
	if err != nil || len(got) != 1 || len(got[0].Headers) != 0 {
		t.Fatalf("got %+v, %v", got, err)
	}
}

// TestPeekDLQNewestWinsAcrossPartitions: p1 has the highest end offset but
// old records; p0 has few records, one of them much newer. The newest wins.
func TestPeekDLQNewestWinsAcrossPartitions(t *testing.T) {
	now := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)
	starts := merge(listed("d", map[int32]int64{0: 0, 1: 0}))
	ends := merge(listed("d", map[int32]int64{0: 3, 1: 9}))
	var recs []*kgo.Record
	for off := int64(4); off < 9; off++ {
		recs = append(recs, rec(1, off, now.Add(time.Duration(off)*time.Second)))
	}
	recs = append(recs, rec(0, 0, now), rec(0, 1, now), rec(0, 2, now.Add(time.Hour)))
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("d", recs...)}}
	var opened map[string]map[int32]kgo.Offset
	c := peekClient(starts, ends, cons, &opened)

	got, err := c.PeekDLQ(context.Background(), "d", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(opened["d"]) != 2 {
		t.Fatalf("consumer opened on %v, want both partitions", opened)
	}
	if len(got) != 5 || got[0].Partition != 0 || got[0].Offset != 2 {
		t.Fatalf("newest record must win, got %+v", got)
	}
	for i := 1; i < len(got); i++ {
		if got[i].Timestamp.After(got[i-1].Timestamp) {
			t.Fatalf("not newest first at %d: %+v", i, got)
		}
	}
}

func TestMergeNewestOrder(t *testing.T) {
	t0 := time.Unix(1000, 0)
	in := []clients.DLQRecord{
		{Partition: 0, Offset: 1, Timestamp: t0},
		{Partition: 0, Offset: 2, Timestamp: t0.Add(-time.Hour)}, // non-monotonic source time
		{Partition: 1, Offset: 7, Timestamp: t0},
	}
	got := mergeNewest(in, 10)
	// p0 stays offset-descending (2 before 1); across partitions by timestamp
	// of the head: p0@2 is old, p1@7 is newer, so p1 first.
	want := [][2]int64{{1, 7}, {0, 2}, {0, 1}}
	for i, w := range want {
		if int64(got[i].Partition) != w[0] || got[i].Offset != w[1] {
			t.Fatalf("order = %+v, want %v", got, want)
		}
	}
	if n := len(mergeNewest(in, 2)); n != 2 {
		t.Fatalf("cut to limit: %d", n)
	}

	// Equal timestamps across partitions: the lower partition wins, whatever
	// the input order.
	tie := []clients.DLQRecord{
		{Partition: 3, Offset: 50, Timestamp: t0},
		{Partition: 1, Offset: 5, Timestamp: t0},
		{Partition: 2, Offset: 99, Timestamp: t0},
	}
	got = mergeNewest(tie, 3)
	if got[0].Partition != 1 || got[1].Partition != 2 || got[2].Partition != 3 {
		t.Fatalf("tie-break order = %+v, want partitions 1,2,3", got)
	}
}

// fnConsumer lets a test script PollFetches.
type fnConsumer struct {
	poll   func(ctx context.Context) kgo.Fetches
	closed atomic.Bool
}

func (f *fnConsumer) PollFetches(ctx context.Context) kgo.Fetches { return f.poll(ctx) }
func (f *fnConsumer) Close()                                      { f.closed.Store(true) }

func clientWith(l *fakeLister, cons pollConsumer) *Client {
	return &Client{offs: l, consumerFn: func([]kgo.Opt) (pollConsumer, error) { return cons, nil }}
}

func controlRec(p int32, off int64) *kgo.Record {
	r := rec(p, off, time.Now())
	r.Attrs = kgo.NewRecordAttrs(kgo.RecordAttrsOpts{Control: true, Transactional: true})
	return r
}

// TestPeekDLQAbortMarkerPastEnd: a transaction open when the offsets were
// listed aborts afterwards; its marker lands past the planned end and must
// still finish the partition.
func TestPeekDLQAbortMarkerPastEnd(t *testing.T) {
	now := time.Now()
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("d",
		rec(0, 1, now), rec(0, 2, now.Add(time.Second)), // offsets 3..5 were aborted and are dropped
		controlRec(0, 6))}}
	c := peekClient(merge(listed("d", map[int32]int64{0: 0})), merge(listed("d", map[int32]int64{0: 4})), cons, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	got, err := c.PeekDLQ(ctx, "d", 3)
	if err != nil || len(got) != 2 {
		t.Fatalf("got %+v, %v", got, err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("waited for the deadline: the marker past end did not finish the partition")
	}
}

// TestPeekDLQPlansAgainstLastStableOffset: with LSO < HWM (an open
// transaction at the tail) the consumer starts and ends below the LSO.
func TestPeekDLQPlansAgainstLastStableOffset(t *testing.T) {
	now := time.Now()
	l := &fakeLister{
		starts: merge(listed("d", map[int32]int64{0: 0, 1: 0})),
		ends:   merge(listed("d", map[int32]int64{0: 10, 1: 6})),
		stable: merge(listed("d", map[int32]int64{0: 4, 1: 6})), // p0 has an open txn from offset 4
	}
	var opened map[string]map[int32]kgo.Offset
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("d",
		rec(0, 2, now), rec(0, 3, now.Add(time.Second)),
		rec(1, 5, now.Add(2*time.Second)))}}
	c := &Client{offs: l, consumerFn: func(opts []kgo.Opt) (pollConsumer, error) {
		opened = consumeModeOf(opts).parts
		return cons, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	got, err := c.PeekDLQ(ctx, "d", 2)
	if err != nil || len(got) != 2 {
		t.Fatalf("got %+v, %v", got, err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("waited for offsets above the last stable offset")
	}
	if len(opened["d"]) != 2 {
		t.Fatalf("opened %v", opened)
	}
	// DLQOffsets itself still reports the high watermark (watcher growth).
	offs, err := c.DLQOffsets(context.Background(), "d")
	if err != nil || offs["d"][0].End != 10 {
		t.Fatalf("DLQOffsets must keep the high watermark: %+v, %v", offs, err)
	}
}

func TestPeekDLQStableOffsetErrors(t *testing.T) {
	base := func() *fakeLister {
		return &fakeLister{
			starts: merge(listed("d", map[int32]int64{0: 0})),
			ends:   merge(listed("d", map[int32]int64{0: 2})),
		}
	}
	unused := &fakeConsumer{}
	l := base()
	l.stable = merge(missing("d"))
	if _, err := clientWith(l, unused).PeekDLQ(context.Background(), "d", 1); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("topic gone: %v", err)
	}
	l = base()
	l.stable = merge(listed("d", map[int32]int64{7: 1})) // partition 0 missing
	if _, err := clientWith(l, unused).PeekDLQ(context.Background(), "d", 1); err == nil {
		t.Fatal("missing partition in LSO response accepted")
	}
	l = base()
	l.stable = merge(map[string]map[int32]kadm.ListedOffset{"d": {0: {Partition: 0, Err: kerr.NotLeaderForPartition}}})
	if _, err := clientWith(l, unused).PeekDLQ(context.Background(), "d", 1); !errors.Is(err, kerr.NotLeaderForPartition) {
		t.Fatalf("partition error: %v", err)
	}
	if unused.closed.Load() {
		t.Fatal("consumer should not have been opened")
	}
}

// TestPeekDLQContextExpiresBetweenPolls exercises the ctx.Err check after a
// poll that returned records without an error fetch: with a record it is a
// sample, without one an error. It also checks the debug line is counts only.
func TestPeekDLQContextExpiresBetweenPolls(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)

	mk := func(withRecord bool) (*Client, context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cons := &fnConsumer{poll: func(context.Context) kgo.Fetches {
			cancel() // the deadline passes while the poll returns normally
			if withRecord {
				r := rec(0, 0, time.Now())
				r.Value = []byte("JUNKVAL-ctx")
				return fetchOf("d", r)
			}
			return kgo.Fetches{}
		}}
		l := &fakeLister{starts: merge(listed("d", map[int32]int64{0: 0})), ends: merge(listed("d", map[int32]int64{0: 2}))}
		return clientWith(l, cons), ctx, cancel
	}

	c, ctx, cancel := mk(true)
	got, err := c.PeekDLQ(ctx, "d", 2)
	cancel()
	if err != nil || len(got) != 1 {
		t.Fatalf("with a record: got %+v, %v; want the sample", got, err)
	}
	if !strings.Contains(logs.String(), "cut short") || !strings.Contains(logs.String(), "partitions_planned=1") ||
		strings.Contains(logs.String(), "JUNKVAL") {
		t.Fatalf("debug line: %q", logs.String())
	}

	c, ctx, cancel = mk(false)
	_, err = c.PeekDLQ(ctx, "d", 2)
	cancel()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("without a record: err = %v, want Canceled", err)
	}
}

func TestPlanPeek(t *testing.T) {
	parts := []clients.PartitionOffsets{
		{Partition: 2, Start: 0, End: 100},
		{Partition: 0, Start: 0, End: 2},
		{Partition: 1, Start: 5, End: 5}, // empty
		{Partition: 3, Start: 0, End: 3},
	}
	got := planPeek(parts, 5)
	want := []peekPart{{0, 0, 2}, {2, 95, 100}, {3, 0, 3}}
	if len(got) != len(want) {
		t.Fatalf("plan = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("plan = %+v, want %+v", got, want)
		}
	}
}

// TestPeekDLQControlRecordAtEnd: a transactional DLQ write leaves a commit
// marker at the last offset; it marks the partition done and is not returned.
func TestPeekDLQControlRecordAtEnd(t *testing.T) {
	now := time.Now()
	marker := rec(0, 3, now)
	marker.Attrs = kgo.NewRecordAttrs(kgo.RecordAttrsOpts{Control: true, Transactional: true})
	marker.Key, marker.Value = []byte("MARKERKEY"), []byte("MARKERVAL")
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("d",
		rec(0, 0, now), rec(0, 1, now.Add(time.Second)), rec(0, 2, now.Add(2*time.Second)), marker)}}
	c := peekClient(merge(listed("d", map[int32]int64{0: 0})), merge(listed("d", map[int32]int64{0: 4})), cons, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	got, err := c.PeekDLQ(ctx, "d", 5)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("waited for the deadline instead of finishing at the marker")
	}
	if len(got) != 3 || got[0].Offset != 2 {
		t.Fatalf("got %+v, want 3 data records", got)
	}
}

// TestPeekDLQDeadlineWithRecordsReturnsSample: records arrived, the last
// planned offset never does; the sample is returned.
func TestPeekDLQDeadlineWithRecordsReturnsSample(t *testing.T) {
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("d", rec(0, 0, time.Now()))}}
	c := peekClient(merge(listed("d", map[int32]int64{0: 0})), merge(listed("d", map[int32]int64{0: 2})), cons, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	got, err := c.PeekDLQ(ctx, "d", 2)
	if err != nil || len(got) != 1 {
		t.Fatalf("got %+v, %v; want the one record read", got, err)
	}
}

func TestPeekDLQDataLossIsSkipped(t *testing.T) {
	loss := kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "d", Partitions: []kgo.FetchPartition{
		{Partition: 0, Err: &kgo.ErrDataLoss{Topic: "d", Partition: 0}},
	}}}}}
	cons := &fakeConsumer{polls: []kgo.Fetches{loss, fetchOf("d", rec(0, 0, time.Now()))}}
	c := peekClient(merge(listed("d", map[int32]int64{0: 0})), merge(listed("d", map[int32]int64{0: 1})), cons, nil)
	got, err := c.PeekDLQ(context.Background(), "d", 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("got %+v, %v", got, err)
	}
}

// TestPeekDLQLeakInMessageErrorsAndLogs puts the junk inside the
// [Source: ...] tail of the message and the stack trace line, and checks the
// result, error text and log output for it.
func TestPeekDLQLeakInMessageErrorsAndLogs(t *testing.T) {
	const marker = "JUNKMSG-c3d1"
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)

	now := time.Now()
	r := rec(0, 0, now,
		hdr("__connect.errors.exception.message", `bad at [Source: (byte[])"{"email":"`+marker+`"}"; line: 1]`),
		hdr("__connect.errors.exception.stacktrace", `x.JsonParseException: Unexpected "`+marker+`" [Source: (byte[])"`+marker+`"]`+"\n\tat y"),
	)
	r.Value = []byte(marker)
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("d", r)}}
	c := peekClient(merge(listed("d", map[int32]int64{0: 0})), merge(listed("d", map[int32]int64{0: 1})), cons, nil)
	got, err := c.PeekDLQ(context.Background(), "d", 1)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(got)
	if strings.Contains(string(b), marker) {
		t.Fatalf("marker in result: %s", b)
	}

	// Failing paths: the error text must not carry record content either.
	// The same consumer first delivers a record carrying the marker, then fails.
	r2 := rec(0, 0, now, hdr("__connect.errors.exception.message", marker))
	r2.Key, r2.Value = []byte(marker), []byte(marker)
	bad := &fakeConsumer{polls: []kgo.Fetches{fetchOf("d", r2), kgo.NewErrFetch(errors.New("boom"))}}
	c2 := peekClient(merge(listed("d", map[int32]int64{0: 0})), merge(listed("d", map[int32]int64{0: 2})), bad, nil)
	if _, err := c2.PeekDLQ(context.Background(), "d", 1); err == nil || strings.Contains(err.Error(), marker) {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(logs.String(), marker) {
		t.Fatalf("marker in logs: %s", logs.String())
	}
}

func TestPeekDLQLimitNormalised(t *testing.T) {
	for in, want := range map[int]int{-3: 5, 0: 5, 1: 1, 20: 20, 21: 20, 1000: 20} {
		if got := normalizePeekLimit(in); got != want {
			t.Errorf("normalizePeekLimit(%d) = %d, want %d", in, got, want)
		}
	}
}

func TestPeekDLQMissingTopicIsNotFound(t *testing.T) {
	cons := &fakeConsumer{}
	called := false
	c := &Client{
		offs: &fakeLister{starts: merge(missing("gone")), ends: merge(missing("gone"))},
		consumerFn: func([]kgo.Opt) (pollConsumer, error) {
			called = true
			return cons, nil
		},
	}
	_, err := c.PeekDLQ(context.Background(), "gone", 5)
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if called {
		t.Fatal("consumer opened for a missing topic")
	}
}

func TestPeekDLQEmptyTopic(t *testing.T) {
	called := false
	c := &Client{
		offs: &fakeLister{starts: merge(listed("d", map[int32]int64{0: 7})), ends: merge(listed("d", map[int32]int64{0: 7}))},
		consumerFn: func([]kgo.Opt) (pollConsumer, error) {
			called = true
			return &fakeConsumer{}, nil
		},
	}
	got, err := c.PeekDLQ(context.Background(), "d", 5)
	if err != nil || len(got) != 0 || called {
		t.Fatalf("got %v, %v, consumer opened=%v", got, err, called)
	}
	if _, err := c.PeekDLQ(context.Background(), " ", 5); err == nil {
		t.Fatal("empty topic accepted")
	}
}

func TestPeekDLQStopsAt8MiB(t *testing.T) {
	now := time.Now()
	big := func(off int64) *kgo.Record {
		r := rec(0, off, now.Add(time.Duration(off)*time.Second))
		r.Value = make([]byte, 5<<20)
		return r
	}
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("d", big(0), big(1), big(2))}}
	c := peekClient(merge(listed("d", map[int32]int64{0: 0})), merge(listed("d", map[int32]int64{0: 3})), cons, nil)
	got, err := c.PeekDLQ(context.Background(), "d", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2 (stop once 8 MiB is passed)", len(got))
	}
	if !cons.closed.Load() {
		t.Error("consumer not closed")
	}
}

func TestPeekDLQDeadlineIsAnErrorAndClosesConsumer(t *testing.T) {
	// The fake never delivers the planned records, so the poll blocks until
	// the deadline.
	cons := &fakeConsumer{}
	c := peekClient(merge(listed("d", map[int32]int64{0: 0})), merge(listed("d", map[int32]int64{0: 2})), cons, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := c.PeekDLQ(ctx, "d", 2)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if !cons.closed.Load() {
		t.Error("consumer not closed on deadline")
	}
}

func TestPeekDLQFetchErrorIsReturned(t *testing.T) {
	cons := &fakeConsumer{polls: []kgo.Fetches{kgo.NewErrFetch(kerr.TopicAuthorizationFailed)}}
	c := peekClient(merge(listed("d", map[int32]int64{0: 0})), merge(listed("d", map[int32]int64{0: 1})), cons, nil)
	_, err := c.PeekDLQ(context.Background(), "d", 1)
	if !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Fatalf("err = %v", err)
	}
	if !cons.closed.Load() {
		t.Error("consumer not closed on error")
	}
}

// TestPeekDLQLeak seeds records whose key and value are junk markers and proves
// the result carries none of them in plain, base64 or hex form.
func TestPeekDLQLeak(t *testing.T) {
	const keyMarker, valMarker = "JUNKKEY-4f9a1c", "JUNKVAL-77be02"
	now := time.Now()
	var recs []*kgo.Record
	for off := int64(0); off < 3; off++ {
		r := rec(0, off, now.Add(time.Duration(off)*time.Second),
			hdr("__connect.errors.exception.class.name", "DataException"),
			hdr("__connect.errors.exception.message", "bad record"),
		)
		r.Key = []byte(keyMarker)
		r.Value = []byte(`{"email":"` + valMarker + `"}`)
		recs = append(recs, r)
	}
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("d", recs...)}}
	c := peekClient(merge(listed("d", map[int32]int64{0: 0})), merge(listed("d", map[int32]int64{0: 3})), cons, nil)
	got, err := c.PeekDLQ(context.Background(), "d", 3)
	if err != nil || len(got) != 3 {
		t.Fatalf("got %d, %v", len(got), err)
	}
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	out := string(b)
	for _, m := range []string{keyMarker, valMarker} {
		forms := map[string]string{
			"plain":      m,
			"base64":     base64.StdEncoding.EncodeToString([]byte(m)),
			"base64-url": base64.URLEncoding.EncodeToString([]byte(m)),
			"hex":        hex.EncodeToString([]byte(m)),
		}
		for name, f := range forms {
			if strings.Contains(out, f) {
				t.Errorf("%s of %q found in result JSON: %s", name, m, out)
			}
		}
	}
}

func TestConnectErrorHeadersInvalidUTF8AndSizes(t *testing.T) {
	got := connectErrorHeaders([]kgo.RecordHeader{{Key: "__connect.errors.stage", Value: []byte("A\xffB\x7f")}})
	if got["__connect.errors.stage"] != "AB" {
		t.Fatalf("got %q", got["__connect.errors.stage"])
	}
	if connectErrorHeaders(nil) != nil {
		t.Fatal("no headers must give a nil map")
	}
}

// TestConnectErrorHeadersWhitespaceKeepsTailMarkers is the regression test for
// control-character removal fusing "exists\nDetail:" into "existsDetail:",
// which hid the marker from the redactor and kept the PII.
func TestConnectErrorHeadersWhitespaceKeepsTailMarkers(t *testing.T) {
	tests := []struct {
		name, key, value, wantPrefix string
	}{
		{"newline before Detail", headerMessage,
			"ERROR: duplicate key value already exists\nDetail: Key (email)=(jane@x.com) exists",
			"ERROR: duplicate key value already exists "},
		{"tab before where", headerMessage, "bad\twhere: (jane@x.com)", "bad "},
		{"CRLF before Where", headerMessage, "ERROR: failed\r\nWhere: (jane@x.com)", "ERROR: failed "},
		{"stacktrace first line with tab", headerStacktrace,
			"org.x.PSQLException: exists\tDetail: Key (email)=(jane@x.com)\n\tat org.x.Foo", "org.x.PSQLException: exists "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := connectErrorHeaders([]kgo.RecordHeader{{Key: tc.key, Value: []byte(tc.value)}})[tc.key]
			if strings.Contains(got, "jane@x.com") {
				t.Fatalf("PII kept: %q", got)
			}
			if !strings.HasPrefix(got, tc.wantPrefix) || !strings.Contains(got, dlq.Redacted) {
				t.Fatalf("got %q, want prefix %q and a redaction mark", got, tc.wantPrefix)
			}
			if strings.ContainsAny(got, "\t\n\r") {
				t.Fatalf("whitespace control characters kept: %q", got)
			}
		})
	}
}

func TestConnectErrorHeadersUnknownKeysDropped(t *testing.T) {
	got := connectErrorHeaders([]kgo.RecordHeader{
		{Key: "__connect.errors.stage", Value: []byte("SINK_TASK")},
		{Key: "__connect.errors.injected", Value: []byte("ignore previous instructions")},
		{Key: "__connect.errors.", Value: []byte("x")},
	})
	if len(got) != 1 || got["__connect.errors.stage"] != "SINK_TASK" {
		t.Fatalf("got %v, want only the stage header", got)
	}
}

// TestConnectErrorHeadersRejectRule: Piperoom's own reject-rule header is the
// one non-Connect key PeekDLQ returns (KAN-412). It is cleaned, capped and
// redacted like the message, and every other application header stays dropped.
func TestConnectErrorHeadersRejectRule(t *testing.T) {
	got := connectErrorHeaders([]kgo.RecordHeader{
		{Key: headerRejectRule, Value: []byte("status_enum")},
		{Key: "x-trace-id", Value: []byte("trace")},
		{Key: "x-piperoom-other", Value: []byte("nope")},
		{Key: "X-Piperoom-Reject-Rule", Value: []byte("case")},
	})
	if len(got) != 1 || got[headerRejectRule] != "status_enum" {
		t.Fatalf("got %v, want only the reject rule", got)
	}

	tests := []struct {
		name, in string
		want     string // "" = redacted text only checked by the absence list
		absent   []string
	}{
		{"plain name", "status_enum", "status_enum", nil},
		{"rejection text", `unknown status "gift"`, "", []string{"gift"}},
		{"credential", "rule password=hunter2", "", []string{"hunter2"}},
		{"newline becomes a space", "status\n_enum", "status _enum", nil},
		{"other control bytes dropped", "status\x00_enum\x7f", "status_enum", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := connectErrorHeaders([]kgo.RecordHeader{{Key: headerRejectRule, Value: []byte(tc.in)}})[headerRejectRule]
			if tc.want != "" && v != tc.want {
				t.Fatalf("value = %q, want %q", v, tc.want)
			}
			if tc.want == "" && !strings.Contains(v, dlq.Redacted) {
				t.Fatalf("value %q carries no redaction mark", v)
			}
			for _, a := range tc.absent {
				if strings.Contains(v, a) {
					t.Fatalf("value %q keeps %q", v, a)
				}
			}
			if strings.ContainsAny(v, "\x00\n\r\t") {
				t.Fatalf("value %q keeps control characters", v)
			}
		})
	}

	long := connectErrorHeaders([]kgo.RecordHeader{{Key: headerRejectRule, Value: []byte(strings.Repeat("A", 10*1024))}})[headerRejectRule]
	if n := len([]rune(long)); n > dlq.MaxMessageRunes {
		t.Fatalf("value has %d runes, want at most %d", n, dlq.MaxMessageRunes)
	}
}

// TestNoGoroutineLeakFromRealConsumer builds and closes the real kgo consumer
// path (no broker is contacted until polled) and checks the goroutine count
// returns to its baseline.
func TestNoGoroutineLeakFromRealConsumer(t *testing.T) {
	c := &Client{brokers: []string{"127.0.0.1:1"}}
	runtime.GC()
	before := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		cons, err := c.newConsumer(map[string]map[int32]kgo.Offset{"d": {0: kgo.NewOffset().AtEnd().Relative(-1)}})
		if err != nil {
			t.Fatal(err)
		}
		cons.Close()
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		runtime.GC()
		if n := runtime.NumGoroutine(); n <= before+1 {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("goroutines: %d before, %d after closing consumers", before, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
