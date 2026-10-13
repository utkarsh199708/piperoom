package kafka

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
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
		consumerFn: func(p map[string]map[int32]kgo.Offset) (dlqConsumer, error) {
			if opened != nil {
				*opened = p
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

func TestPeekDLQLimitIsTotalAcrossPartitions(t *testing.T) {
	now := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)
	// p2 has the highest end offset (read first), p0 next, p1 smallest.
	starts := merge(listed("d", map[int32]int64{0: 0, 1: 0, 2: 0}))
	ends := merge(listed("d", map[int32]int64{0: 4, 1: 3, 2: 9}))
	var recs []*kgo.Record
	for off := int64(5); off < 9; off++ { // p2: 4 newest of limit 5... plan gives p2 5 records
		recs = append(recs, rec(2, off, now.Add(time.Duration(off)*time.Second)))
	}
	recs = append(recs, rec(2, 4, now.Add(4*time.Second)))
	recs = append(recs, rec(0, 3, now.Add(time.Hour))) // not planned: must be ignored
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("d", recs...)}}
	var opened map[string]map[int32]kgo.Offset
	c := peekClient(starts, ends, cons, &opened)

	got, err := c.PeekDLQ(context.Background(), "d", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(opened["d"]) != 1 {
		t.Fatalf("consumer opened on %d partitions, want only the newest-end partition (limit 5 fits in it): %v", len(opened["d"]), opened)
	}
	if _, ok := opened["d"][2]; !ok {
		t.Fatalf("partition 2 (highest end) not read: %v", opened)
	}
	if len(got) != 5 {
		t.Fatalf("got %d records, want 5", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].Timestamp.After(got[i-1].Timestamp) {
			t.Fatalf("not newest first at %d: %+v", i, got)
		}
	}
	if got[0].Offset != 8 {
		t.Errorf("newest = %d, want 8", got[0].Offset)
	}
}

func TestPlanPeek(t *testing.T) {
	parts := []clients.PartitionOffsets{
		{Partition: 0, Start: 0, End: 2},
		{Partition: 1, Start: 5, End: 5}, // empty
		{Partition: 2, Start: 0, End: 100},
		{Partition: 3, Start: 0, End: 3},
	}
	tests := []struct {
		limit int
		want  []peekPart
	}{
		{5, []peekPart{{2, 95, 100}}},
		{20, []peekPart{{2, 80, 100}}},
		{1, []peekPart{{2, 99, 100}}},
	}
	for _, tt := range tests {
		got := planPeek(parts, tt.limit)
		if len(got) != len(tt.want) || got[0] != tt.want[0] {
			t.Errorf("limit %d: %+v, want %+v", tt.limit, got, tt.want)
		}
	}
	// Spills over partitions in end-offset order, never more than limit total.
	small := []clients.PartitionOffsets{{Partition: 0, Start: 0, End: 2}, {Partition: 1, Start: 0, End: 3}, {Partition: 2, Start: 0, End: 1}}
	got := planPeek(small, 5)
	var sum int64
	for _, p := range got {
		sum += p.end - p.from
	}
	if sum != 5 || len(got) != 2 || got[0].partition != 1 || got[1].partition != 0 {
		t.Errorf("spill plan = %+v (sum %d)", got, sum)
	}
	// 64 partitions, limit 20 -> at most 20 records planned.
	var wide []clients.PartitionOffsets
	for i := int32(0); i < 64; i++ {
		wide = append(wide, clients.PartitionOffsets{Partition: i, Start: 0, End: 100})
	}
	sum = 0
	for _, p := range planPeek(wide, 20) {
		sum += p.end - p.from
	}
	if sum != 20 {
		t.Errorf("64 partitions planned %d records, want 20", sum)
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
		consumerFn: func(map[string]map[int32]kgo.Offset) (dlqConsumer, error) {
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
		consumerFn: func(map[string]map[int32]kgo.Offset) (dlqConsumer, error) {
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

// TestNoGoroutineLeakFromRealConsumer builds and closes the real kgo consumer
// path (no broker is contacted until polled) and checks the goroutine count
// returns to its baseline.
func TestNoGoroutineLeakFromRealConsumer(t *testing.T) {
	c := &Client{brokers: []string{"127.0.0.1:1"}}
	runtime.GC()
	before := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		cons, err := c.newDLQConsumer(map[string]map[int32]kgo.Offset{"d": {0: kgo.NewOffset().AtEnd().Relative(-1)}})
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
