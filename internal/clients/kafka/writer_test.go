package kafka

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// fakeGroups answers the group requests of a reset from fixed values and
// records what was committed.
type fakeGroups struct {
	classic     kadm.DescribedGroups
	classicErr  error
	consumer    kadm.DescribedConsumerGroups
	consumerErr error
	committed   kadm.OffsetResponses
	fetchErr    error
	// commitErrs gives a partition's commit answer; others succeed.
	commitErrs map[int32]error
	commitErr  error
	// commitBlocks makes CommitOffsets wait for ctx to end, like a broker
	// that never answers.
	commitBlocks bool

	consumerCalls, fetchCalls int
	commits                   []kadm.Offsets
}

func (f *fakeGroups) DescribeGroups(ctx context.Context, _ ...string) (kadm.DescribedGroups, error) {
	return f.classic, f.classicErr
}

func (f *fakeGroups) DescribeConsumerGroups(ctx context.Context, _ ...string) (kadm.DescribedConsumerGroups, error) {
	f.consumerCalls++
	return f.consumer, f.consumerErr
}

func (f *fakeGroups) FetchOffsets(ctx context.Context, _ string) (kadm.OffsetResponses, error) {
	f.fetchCalls++
	return f.committed, f.fetchErr
}

func (f *fakeGroups) CommitOffsets(ctx context.Context, _ string, os kadm.Offsets) (kadm.OffsetResponses, error) {
	f.commits = append(f.commits, os)
	if f.commitBlocks {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.commitErr != nil {
		return nil, f.commitErr
	}
	rs := kadm.OffsetResponses{}
	os.Each(func(o kadm.Offset) { rs.Add(kadm.OffsetResponse{Offset: o, Err: f.commitErrs[o.Partition]}) })
	return rs, nil
}

func committedOf(topic string, offs map[int32]int64) kadm.OffsetResponses {
	rs := kadm.OffsetResponses{}
	for p, o := range offs {
		rs.Add(kadm.OffsetResponse{Offset: kadm.Offset{Topic: topic, Partition: p, At: o}})
	}
	return rs
}

// unscoped is the explicit scope opt-out the Writer's writes need when no
// room pipeline is in ctx.
func unscoped() context.Context { return tools.WithUnscoped(context.Background()) }

func emptyGroup(g string) kadm.DescribedGroups {
	return kadm.DescribedGroups{g: {Group: g, State: "Empty"}}
}

func TestCheckTarget(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name string
		to   clients.OffsetTarget
		ok   bool
	}{
		{"earliest", clients.OffsetTarget{Earliest: true}, true},
		{"past timestamp", clients.OffsetTarget{Timestamp: now.Add(-time.Hour)}, true},
		{"both", clients.OffsetTarget{Earliest: true, Timestamp: now.Add(-time.Hour)}, false},
		{"neither", clients.OffsetTarget{}, false},
		{"future timestamp", clients.OffsetTarget{Timestamp: now.Add(time.Minute)}, false},
		{"before the epoch", clients.OffsetTarget{Timestamp: time.Unix(-10, 0)}, false},
	} {
		if err := checkTarget(tc.to, now); (err == nil) != tc.ok {
			t.Errorf("%s: err = %v, want ok=%v", tc.name, err, tc.ok)
		}
	}
}

func TestCheckGroupInactive(t *testing.T) {
	const g = "orders-sink"
	member := []kadm.DescribedGroupMember{{MemberID: "m1"}}
	for _, tc := range []struct {
		name         string
		f            fakeGroups
		active       bool
		wantErr      bool
		askConsumers bool
	}{
		{name: "classic empty", f: fakeGroups{classic: emptyGroup(g)}},
		{name: "classic stable with members", f: fakeGroups{classic: kadm.DescribedGroups{g: {State: "Stable", Members: member}}}, active: true},
		{name: "classic rebalancing, no member listed", f: fakeGroups{classic: kadm.DescribedGroups{g: {State: "PreparingRebalance"}}}, active: true},
		{
			name:         "KIP-848 group with a member",
			f:            fakeGroups{classic: kadm.DescribedGroups{g: {State: "Dead", Err: kerr.GroupIDNotFound}}, consumer: kadm.DescribedConsumerGroups{g: {State: "Stable", Members: []kadm.ConsumerGroupMember{{MemberID: "m"}}}}},
			active:       true,
			askConsumers: true,
		},
		{
			name:         "KIP-848 group empty (old broker answers Dead without error)",
			f:            fakeGroups{classic: kadm.DescribedGroups{g: {State: "Dead"}}, consumer: kadm.DescribedConsumerGroups{g: {State: "Empty"}}},
			askConsumers: true,
		},
		{
			name:         "unknown to both",
			f:            fakeGroups{classic: kadm.DescribedGroups{g: {Err: kerr.GroupIDNotFound}}, consumer: kadm.DescribedConsumerGroups{g: {Err: kerr.GroupIDNotFound}}},
			askConsumers: true,
		},
		{
			name:         "consumer describe unsupported fails closed",
			f:            fakeGroups{classic: kadm.DescribedGroups{g: {Err: kerr.GroupIDNotFound}}, consumerErr: errors.New("unknown request key")},
			wantErr:      true,
			askConsumers: true,
		},
		{
			name:         "consumer describe group error",
			f:            fakeGroups{classic: kadm.DescribedGroups{g: {Err: kerr.GroupIDNotFound}}, consumer: kadm.DescribedConsumerGroups{g: {Err: kerr.CoordinatorNotAvailable}}},
			wantErr:      true,
			askConsumers: true,
		},
		{name: "classic group error", f: fakeGroups{classic: kadm.DescribedGroups{g: {Err: kerr.CoordinatorNotAvailable}}}, wantErr: true},
		{name: "classic request error", f: fakeGroups{classicErr: errors.New("broker down")}, wantErr: true},
		{name: "missing from response", f: fakeGroups{classic: kadm.DescribedGroups{}}, wantErr: true},
	} {
		w := &Writer{c: &Client{}, groups: &tc.f}
		err := w.CheckGroupInactive(context.Background(), g)
		switch {
		case tc.active && !errors.Is(err, clients.ErrGroupActive):
			t.Errorf("%s: err = %v, want ErrGroupActive", tc.name, err)
		case tc.wantErr && (err == nil || errors.Is(err, clients.ErrGroupActive)):
			t.Errorf("%s: err = %v, want a non-ErrGroupActive error", tc.name, err)
		case !tc.active && !tc.wantErr && err != nil:
			t.Errorf("%s: err = %v, want nil", tc.name, err)
		}
		if asked := tc.f.consumerCalls > 0; asked != tc.askConsumers {
			t.Errorf("%s: consumer describe asked = %v, want %v", tc.name, asked, tc.askConsumers)
		}
	}
	if err := (&Writer{c: &Client{}, groups: &fakeGroups{}}).CheckGroupInactive(context.Background(), " "); err == nil {
		t.Error("empty group name accepted")
	}
}

// resetWriter wires a Writer to fake group, offset and metadata answers.
func resetWriter(f *fakeGroups, l *fakeLister) *Writer {
	return &Writer{c: &Client{offs: l}, groups: f}
}

func TestResetGroupOffsetsRefusesActiveGroupAndChangesNothing(t *testing.T) {
	f := &fakeGroups{
		classic:   kadm.DescribedGroups{"g": {State: "Stable", Members: []kadm.DescribedGroupMember{{MemberID: "m"}}}},
		committed: committedOf("t", map[int32]int64{0: 5}),
	}
	l := &fakeLister{starts: merge(listed("t", map[int32]int64{0: 0})), ends: merge(listed("t", map[int32]int64{0: 9}))}
	moved, err := resetWriter(f, l).ResetGroupOffsets(unscoped(), "g", clients.OffsetTarget{Earliest: true})
	if !errors.Is(err, clients.ErrGroupActive) || moved != 0 {
		t.Fatalf("moved=%d err=%v, want 0 and ErrGroupActive", moved, err)
	}
	if f.fetchCalls != 0 || len(f.commits) != 0 || l.calls.Load() != 0 {
		t.Fatalf("an active group was read or written: fetch=%d commits=%d lists=%d", f.fetchCalls, len(f.commits), l.calls.Load())
	}
}

func TestResetGroupOffsetsBadTargetSendsNothing(t *testing.T) {
	f := &fakeGroups{classic: emptyGroup("g")}
	for _, to := range []clients.OffsetTarget{{}, {Timestamp: time.Now().Add(time.Hour)}} {
		if _, err := resetWriter(f, &fakeLister{}).ResetGroupOffsets(unscoped(), "g", to); err == nil {
			t.Fatalf("target %+v accepted", to)
		}
	}
	if f.fetchCalls != 0 || len(f.commits) != 0 {
		t.Fatal("a bad target reached the broker")
	}
}

func TestPlanMoves(t *testing.T) {
	committed := map[string]map[int32]int64{"t": {0: 5, 1: 7}, "gone": {0: 3}}
	targets := merge(listed("t", map[int32]int64{0: 2, 1: -1, 2: 12}), missing("gone"))
	ends := merge(listed("t", map[int32]int64{0: 10, 1: 10, 2: 4}), missing("gone"))
	got, err := planMoves(committed, targets, ends)
	if err != nil {
		t.Fatal(err)
	}
	want := []OffsetMove{
		{Topic: "t", Partition: 0, Committed: 5, Target: 2, End: 10},
		{Topic: "t", Partition: 1, Committed: 7, Target: 10, End: 10}, // no record after the time: end
		{Topic: "t", Partition: 2, Committed: -1, Target: 4, End: 4},  // no commit yet; target past end: end
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("moves = %+v\nwant   %+v", got, want)
	}

	bad := merge(listed("t", map[int32]int64{0: 2}))
	bad["t"][0] = kadm.ListedOffset{Topic: "t", Partition: 0, Err: kerr.NotLeaderForPartition}
	if _, err := planMoves(committed, bad, ends); !errors.Is(err, kerr.NotLeaderForPartition) {
		t.Fatalf("target error: %v", err)
	}
	if _, err := planMoves(committed, targets, merge(listed("t", map[int32]int64{0: 10}))); err == nil {
		t.Fatal("missing end offset accepted")
	}
}

func TestResetGroupOffsetsEarliestCommitsOnlyMovedPartitions(t *testing.T) {
	f := &fakeGroups{classic: emptyGroup("g"), committed: committedOf("t", map[int32]int64{0: 5, 1: 2})}
	l := &fakeLister{
		starts: merge(listed("t", map[int32]int64{0: 2, 1: 2})),
		ends:   merge(listed("t", map[int32]int64{0: 10, 1: 10})),
	}
	moved, err := resetWriter(f, l).ResetGroupOffsets(unscoped(), "g", clients.OffsetTarget{Earliest: true})
	if err != nil || moved != 1 {
		t.Fatalf("moved=%d err=%v, want 1 nil", moved, err)
	}
	if len(f.commits) != 1 {
		t.Fatalf("commits = %d, want 1", len(f.commits))
	}
	want := kadm.Offsets{"t": {0: {Topic: "t", Partition: 0, At: 2, LeaderEpoch: -1}}}
	if !reflect.DeepEqual(f.commits[0], want) {
		t.Fatalf("committed %+v, want %+v", f.commits[0], want)
	}
	if !l.sawDeadline {
		t.Fatal("listing ran without a deadline")
	}
}

func TestResetGroupOffsetsToTimestamp(t *testing.T) {
	f := &fakeGroups{classic: emptyGroup("g"), committed: committedOf("t", map[int32]int64{0: 9, 1: 9})}
	l := &fakeLister{
		after: merge(listed("t", map[int32]int64{0: 7, 1: -1})),
		ends:  merge(listed("t", map[int32]int64{0: 10, 1: 10})),
	}
	moved, err := resetWriter(f, l).ResetGroupOffsets(unscoped(), "g", clients.OffsetTarget{Timestamp: time.Now().Add(-time.Hour)})
	if err != nil || moved != 2 {
		t.Fatalf("moved=%d err=%v, want 2 nil", moved, err)
	}
	if got := f.commits[0]; got["t"][0].At != 7 || got["t"][1].At != 10 {
		t.Fatalf("committed %+v, want p0=7, p1=10 (end)", got)
	}
}

func TestResetGroupOffsetsCommitAnswers(t *testing.T) {
	newF := func() *fakeGroups {
		return &fakeGroups{classic: emptyGroup("g"), committed: committedOf("t", map[int32]int64{0: 5, 1: 5})}
	}
	l := &fakeLister{starts: merge(listed("t", map[int32]int64{0: 0, 1: 0})), ends: merge(listed("t", map[int32]int64{0: 9, 1: 9}))}
	earliest := clients.OffsetTarget{Earliest: true}

	// A member joined after the check: the broker refuses an admin commit.
	f := newF()
	f.commitErrs = map[int32]error{0: kerr.UnknownMemberID, 1: kerr.UnknownMemberID}
	if moved, err := resetWriter(f, l).ResetGroupOffsets(unscoped(), "g", earliest); !errors.Is(err, clients.ErrGroupActive) || moved != 0 {
		t.Fatalf("joined member: moved=%d err=%v", moved, err)
	}

	f = newF()
	f.commitErrs = map[int32]error{1: kerr.CoordinatorLoadInProgress}
	moved, err := resetWriter(f, l).ResetGroupOffsets(unscoped(), "g", earliest)
	if moved != 1 || !errors.Is(err, kerr.CoordinatorLoadInProgress) || errors.Is(err, clients.ErrGroupActive) {
		t.Fatalf("partial: moved=%d err=%v, want 1 and the partition error", moved, err)
	}

	f = newF()
	f.commitErr = errors.New("connection reset")
	if moved, err := resetWriter(f, l).ResetGroupOffsets(unscoped(), "g", earliest); err == nil || moved != 0 || !strings.Contains(err.Error(), "outcome unknown") {
		t.Fatalf("request error: moved=%d err=%v, want 0 and an unknown outcome", moved, err)
	}
}

// TestResetGroupOffsetsCommitCutOff: a commit the deadline cuts off may
// have been applied; the error must say so, not report a plain failure.
func TestResetGroupOffsetsCommitCutOff(t *testing.T) {
	f := &fakeGroups{classic: emptyGroup("g"), committed: committedOf("t", map[int32]int64{0: 5}), commitBlocks: true}
	l := &fakeLister{starts: merge(listed("t", map[int32]int64{0: 0})), ends: merge(listed("t", map[int32]int64{0: 9}))}
	ctx, cancel := context.WithTimeout(unscoped(), 50*time.Millisecond)
	defer cancel()
	moved, err := resetWriter(f, l).ResetGroupOffsets(ctx, "g", clients.OffsetTarget{Earliest: true})
	if moved != 0 || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "outcome unknown, the commit may have been applied") {
		t.Fatalf("moved=%d err=%v, want 0 and an unknown outcome wrapping the deadline", moved, err)
	}
	if len(f.commits) != 1 {
		t.Fatalf("commits sent = %d, want 1", len(f.commits))
	}
}

func TestResetGroupOffsetsNoCommitsIsNotFound(t *testing.T) {
	f := &fakeGroups{classic: emptyGroup("g"), committed: kadm.OffsetResponses{}}
	_, err := resetWriter(f, &fakeLister{}).ResetGroupOffsets(unscoped(), "g", clients.OffsetTarget{Earliest: true})
	if !errors.Is(err, clients.ErrNotFound) || len(f.commits) != 0 {
		t.Fatalf("err=%v commits=%d, want ErrNotFound and none", err, len(f.commits))
	}
}

func TestResetGroupOffsetsAlreadyThere(t *testing.T) {
	f := &fakeGroups{classic: emptyGroup("g"), committed: committedOf("t", map[int32]int64{0: 0})}
	l := &fakeLister{starts: merge(listed("t", map[int32]int64{0: 0})), ends: merge(listed("t", map[int32]int64{0: 4}))}
	moved, err := resetWriter(f, l).ResetGroupOffsets(unscoped(), "g", clients.OffsetTarget{Earliest: true})
	if moved != 0 || err != nil || len(f.commits) != 0 {
		t.Fatalf("moved=%d err=%v commits=%d, want 0 nil none", moved, err, len(f.commits))
	}
}

func TestPreviousOffsets(t *testing.T) {
	rs := committedOf("b", map[int32]int64{1: 4, 0: 3})
	rs.Add(kadm.OffsetResponse{Offset: kadm.Offset{Topic: "a", Partition: 0, At: 9}})
	rs.Add(kadm.OffsetResponse{Offset: kadm.Offset{Topic: "a", Partition: 1, At: -1}}) // no commit
	w := &Writer{c: &Client{}, groups: &fakeGroups{committed: rs}}
	got, err := w.PreviousOffsets(context.Background(), "g")
	if err != nil {
		t.Fatal(err)
	}
	want := []CommittedOffset{{"a", 0, 9}, {"b", 0, 3}, {"b", 1, 4}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	rs.Add(kadm.OffsetResponse{Offset: kadm.Offset{Topic: "c", Partition: 0}, Err: kerr.UnstableOffsetCommit})
	if _, err := w.PreviousOffsets(context.Background(), "g"); !errors.Is(err, kerr.UnstableOffsetCommit) {
		t.Fatalf("partition error: %v", err)
	}
	w.groups = &fakeGroups{fetchErr: kerr.GroupIDNotFound}
	if _, err := w.PreviousOffsets(context.Background(), "g"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("unknown group: %v", err)
	}
}

// ---- produce ----

// produceLog is a fake kgo Produce: it acks every record unless failAt
// names its index, and keeps what it was given.
type produceLog struct {
	got    []*kgo.Record
	failAt int
}

func (p *produceLog) produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	p.got = append(p.got, r)
	if len(p.got)-1 == p.failAt {
		promise(r, kerr.NotEnoughReplicas)
		return
	}
	promise(r, nil)
}

func produceWriter(p *produceLog, topics kadm.TopicDetails) *Writer {
	return &Writer{c: &Client{meta: &fakeMeta{topics: topics}}, produceFn: p.produce}
}

func topicHere(topic string) kadm.TopicDetails {
	return kadm.TopicDetails{topic: {Topic: topic}}
}

func TestProduceInOrderStopsAtFirstFailure(t *testing.T) {
	p := &produceLog{failAt: 2}
	ts := time.Now().Add(-48 * time.Hour)
	recs := []clients.Record{
		{Topic: "dlq", Partition: 3, Offset: 40, Timestamp: ts, Key: []byte("k0"), Value: []byte("v0"), Headers: []clients.RecordHeader{{Key: "h", Value: []byte("x")}}},
		{Key: nil, Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}
	acked, err := produceWriter(p, topicHere("orders")).Produce(unscoped(), "orders", recs)
	if acked != 2 || !errors.Is(err, kerr.NotEnoughReplicas) {
		t.Fatalf("acked=%d err=%v, want 2 and the broker error", acked, err)
	}
	if len(p.got) != 3 {
		t.Fatalf("produced %d records, want 3 (none after the failure)", len(p.got))
	}
	first := p.got[0]
	if first.Topic != "orders" || first.Partition != 0 || !first.Timestamp.IsZero() {
		t.Fatalf("record 0 topic=%q partition=%d ts=%v: topic must be the argument, partition and time left to kgo", first.Topic, first.Partition, first.Timestamp)
	}
	if string(first.Key) != "k0" || string(first.Value) != "v0" || len(first.Headers) != 1 || first.Headers[0].Key != "h" || string(first.Headers[0].Value) != "x" {
		t.Fatalf("record 0 = %+v, want key, value and header kept", first)
	}
	if p.got[1].Key != nil {
		t.Fatal("a null key became non-null")
	}
	for i, r := range p.got {
		if !bytes.Equal(r.Value, recs[i].Value) {
			t.Fatalf("record %d out of order: %q", i, r.Value)
		}
	}
}

func TestProduceRefusesBeforeProducing(t *testing.T) {
	p := &produceLog{failAt: -1}
	// The cap counts key, value and headers together.
	for name, rec := range map[string]clients.Record{
		"value":         {Value: make([]byte, maxProduceRecordBytes+1)},
		"key and value": {Key: make([]byte, 20), Value: make([]byte, maxProduceRecordBytes-10)},
		"header value":  {Value: make([]byte, maxProduceRecordBytes-10), Headers: []clients.RecordHeader{{Key: "h", Value: make([]byte, 10)}}},
		"header key":    {Value: make([]byte, maxProduceRecordBytes), Headers: []clients.RecordHeader{{Key: "h"}}},
		"many headers":  {Headers: manyHeaders(1025, 1024)},
		"key alone":     {Key: make([]byte, maxProduceRecordBytes+1)},
	} {
		big := []clients.Record{{Value: []byte("ok")}, rec}
		if acked, err := produceWriter(p, topicHere("t")).Produce(unscoped(), "t", big); err == nil || acked != 0 {
			t.Fatalf("oversized %s: acked=%d err=%v", name, acked, err)
		}
	}
	if _, err := produceWriter(p, topicHere("t")).Produce(unscoped(), " ", []clients.Record{{}}); err == nil {
		t.Fatal("empty topic accepted")
	}
	gone := kadm.TopicDetails{"t": {Topic: "t", Err: kerr.UnknownTopicOrPartition}}
	if _, err := produceWriter(p, gone).Produce(unscoped(), "t", []clients.Record{{}}); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing topic: %v", err)
	}
	if _, err := produceWriter(p, kadm.TopicDetails{}).Produce(unscoped(), "t", []clients.Record{{}}); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("topic absent from metadata: %v", err)
	}
	if acked, err := produceWriter(p, nil).Produce(unscoped(), "t", nil); acked != 0 || err != nil {
		t.Fatalf("no records: acked=%d err=%v", acked, err)
	}
	if len(p.got) != 0 {
		t.Fatalf("%d records produced by refused calls", len(p.got))
	}
}

func manyHeaders(n, size int) []clients.RecordHeader {
	hs := make([]clients.RecordHeader, n)
	for i := range hs {
		hs[i] = clients.RecordHeader{Key: "h", Value: make([]byte, size-1)}
	}
	return hs
}

func TestProduceAtTheCap(t *testing.T) {
	p := &produceLog{failAt: -1}
	rec := clients.Record{Key: make([]byte, 10), Value: make([]byte, maxProduceRecordBytes-12), Headers: []clients.RecordHeader{{Key: "h", Value: []byte("v")}}}
	if acked, err := produceWriter(p, topicHere("t")).Produce(unscoped(), "t", []clients.Record{rec}); err != nil || acked != 1 {
		t.Fatalf("a record of exactly the cap: acked=%d err=%v", acked, err)
	}
}

// TestProduceAfterDeadline: a record kgo acks just after ctx ended counts;
// the next one is not sent; one kgo never answers is reported unknown.
func TestProduceAfterDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(unscoped())
	var sent int
	w := produceWriter(nil, topicHere("t"))
	w.produceFn = func(pctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
		sent++
		cancel()
		go func() {
			<-pctx.Done()
			promise(r, nil) // in flight when ctx ended; then acked
		}()
	}
	acked, err := w.Produce(ctx, "t", []clients.Record{{Value: []byte("a")}, {Value: []byte("b")}})
	if acked != 1 || !errors.Is(err, context.Canceled) || sent != 1 {
		t.Fatalf("acked=%d sent=%d err=%v, want 1 1 canceled", acked, sent, err)
	}

	ctx2, cancel2 := context.WithTimeout(unscoped(), 50*time.Millisecond)
	defer cancel2()
	w.produceFn = func(context.Context, *kgo.Record, func(*kgo.Record, error)) {} // never answers
	start := time.Now()
	acked, err = w.Produce(ctx2, "t", []clients.Record{{Value: []byte("a")}})
	if acked != 0 || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "outcome unknown") {
		t.Fatalf("acked=%d err=%v, want 0 and an unknown outcome", acked, err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("Produce took %s after its deadline", d)
	}
}

func TestNewWriterProducerSettings(t *testing.T) {
	w, err := NewWriter([]string{"127.0.0.1:1"}) // kgo dials lazily
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if v := w.c.cl.OptValue(kgo.ProducerBatchMaxBytes); v != int32(produceBatchMaxBytes) {
		t.Fatalf("ProducerBatchMaxBytes = %v", v)
	}
	if v := w.c.cl.OptValue(kgo.RequiredAcks); v != kgo.AllISRAcks() {
		t.Fatalf("RequiredAcks = %v, want all in-sync replicas", v)
	}
	if v := w.c.cl.OptValue(kgo.DisableIdempotentWrite); v != false {
		t.Fatalf("idempotent writes disabled: %v", v)
	}
	if v := w.c.cl.OptValue(kgo.AllowAutoTopicCreation); v != false {
		t.Fatalf("auto topic creation on: %v", v)
	}
}

// ---- whole-record reads ----

func readWriter(l *fakeLister, cons pollConsumer, opened *[]kgo.Opt) *Writer {
	return &Writer{c: &Client{offs: l, consumerFn: func(opts []kgo.Opt) (pollConsumer, error) {
		if opened != nil {
			*opened = opts
		}
		return cons, nil
	}}}
}

func dataRec(off int64, key, val string) *kgo.Record {
	return &kgo.Record{Topic: "dlq", Partition: 0, Offset: off, Key: []byte(key), Value: []byte(val), Timestamp: tsAt(off),
		Headers: []kgo.RecordHeader{hdr("__connect.errors.topic", "orders")}}
}

func TestReadRecords(t *testing.T) {
	l := &fakeLister{starts: merge(listed("dlq", map[int32]int64{0: 0})), ends: merge(listed("dlq", map[int32]int64{0: 6}))}
	ctrl := controlRec(0, 2)
	ctrl.Topic = "dlq"
	cons := &fakeConsumer{polls: []kgo.Fetches{
		fetchOf("dlq", dataRec(0, "k0", "v0"), dataRec(1, "k1", "v1")),
		fetchOf("dlq", ctrl, dataRec(3, "k3", "v3"), dataRec(4, "k4", "v4"), dataRec(5, "k5", "v5")),
	}}
	var opts []kgo.Opt
	got, err := readWriter(l, cons, &opts).ReadRecords(context.Background(), "dlq", 0, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	var offs []int64
	for _, r := range got {
		offs = append(offs, r.Offset)
	}
	if !reflect.DeepEqual(offs, []int64{0, 1, 3, 4, 5}) {
		t.Fatalf("offsets = %v, want the marker skipped", offs)
	}
	r := got[2]
	if r.Topic != "dlq" || string(r.Key) != "k3" || string(r.Value) != "v3" || !r.Timestamp.Equal(tsAt(3)) ||
		len(r.Headers) != 1 || string(r.Headers[0].Value) != "orders" {
		t.Fatalf("record = %+v, want the whole record", r)
	}
	if !cons.closed.Load() {
		t.Fatal("consumer not closed")
	}
	m := consumeModeOf(opts)
	if !m.readCommitted || !m.keepControl || m.parts["dlq"][0] != kgo.NewOffset().At(0) {
		t.Fatalf("consumer mode = %+v", m)
	}
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers("127.0.0.1:1")}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if !reflect.DeepEqual(cl.OptValue(kgo.ConsumeResetOffset), kgo.NoResetOffset()) {
		t.Fatal("the read may reset past a gap: ConsumeResetOffset is not NoResetOffset")
	}
}

func TestReadRecordsLimits(t *testing.T) {
	l := &fakeLister{starts: merge(listed("dlq", map[int32]int64{0: 0})), ends: merge(listed("dlq", map[int32]int64{0: 2000}))}
	var many []*kgo.Record
	for off := int64(0); off < 1200; off++ {
		many = append(many, dataRec(off, "k", "v"))
	}
	got, err := readWriter(l, &fakeConsumer{polls: []kgo.Fetches{fetchOf("dlq", many...)}}, nil).ReadRecords(context.Background(), "dlq", 0, 0, 5000)
	if err != nil || len(got) != maxReadRecords {
		t.Fatalf("len=%d err=%v, want the 1000 cap", len(got), err)
	}
	got, err = readWriter(l, &fakeConsumer{polls: []kgo.Fetches{fetchOf("dlq", many[10:20]...)}}, nil).ReadRecords(context.Background(), "dlq", 0, 10, 3)
	if err != nil || len(got) != 3 || got[0].Offset != 10 || got[2].Offset != 12 {
		t.Fatalf("got %d records from %v, err %v; want 10..12", len(got), got, err)
	}
}

func TestReadRecordsStopsAtStableOffset(t *testing.T) {
	l := &fakeLister{
		starts: merge(listed("dlq", map[int32]int64{0: 0})),
		ends:   merge(listed("dlq", map[int32]int64{0: 6})),
		stable: merge(listed("dlq", map[int32]int64{0: 2})), // a transaction is open at 2
	}
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("dlq", dataRec(0, "a", "a"), dataRec(1, "b", "b"))}}
	got, err := readWriter(l, cons, nil).ReadRecords(context.Background(), "dlq", 0, 0, 10)
	if err != nil || len(got) != 2 {
		t.Fatalf("len=%d err=%v, want the two committed records at once", len(got), err)
	}
}

func TestReadRecordsBoundsAndErrors(t *testing.T) {
	l := &fakeLister{starts: merge(listed("dlq", map[int32]int64{0: 5})), ends: merge(listed("dlq", map[int32]int64{0: 9}))}
	opened := false
	w := &Writer{c: &Client{offs: l, consumerFn: func([]kgo.Opt) (pollConsumer, error) {
		opened = true
		return &fakeConsumer{}, nil
	}}}
	ctx := context.Background()
	if _, err := w.ReadRecords(ctx, "dlq", 0, 3, 10); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("below the log start: %v", err)
	}
	if got, err := w.ReadRecords(ctx, "dlq", 0, 9, 10); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("at the end: %v %v, want an empty slice", got, err)
	}
	if _, err := w.ReadRecords(ctx, "dlq", 4, 5, 10); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("unknown partition: %v", err)
	}
	for _, in := range []struct {
		topic     string
		partition int32
		from      int64
		limit     int
	}{{"", 0, 0, 1}, {"dlq", -1, 0, 1}, {"dlq", 0, -1, 1}, {"dlq", 0, 5, 0}} {
		if _, err := w.ReadRecords(ctx, in.topic, in.partition, in.from, in.limit); err == nil {
			t.Fatalf("bad input %+v accepted", in)
		}
	}
	if opened {
		t.Fatal("a consumer was opened for a read with nothing to fetch")
	}

	w.c.offs = &fakeLister{starts: merge(missing("dlq")), ends: merge(missing("dlq"))}
	if _, err := w.ReadRecords(ctx, "dlq", 0, 0, 10); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing topic: %v", err)
	}
}

func TestReadRecordsFetchErrorKeepsPrefix(t *testing.T) {
	l := &fakeLister{starts: merge(listed("dlq", map[int32]int64{0: 0})), ends: merge(listed("dlq", map[int32]int64{0: 9}))}
	fail := fetchOf("dlq")
	fail[0].Topics[0].Partitions = []kgo.FetchPartition{{Partition: 0, Err: kerr.OffsetOutOfRange}}
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("dlq", dataRec(0, "a", "a")), fail}}
	got, err := readWriter(l, cons, nil).ReadRecords(context.Background(), "dlq", 0, 0, 10)
	if !errors.Is(err, kerr.OffsetOutOfRange) || len(got) != 1 || got[0].Offset != 0 {
		t.Fatalf("got %d records, err %v; want the prefix and the fetch error", len(got), err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	cons = &fakeConsumer{polls: []kgo.Fetches{fetchOf("dlq", dataRec(0, "a", "a"))}}
	w := readWriter(l, cons, nil)
	w.c.probeFn = func(context.Context, string, map[int32]readWindow) map[int32]int64 { return nil }
	got, err = w.ReadRecords(ctx, "dlq", 0, 0, 10)
	if !errors.Is(err, context.DeadlineExceeded) || len(got) != 1 {
		t.Fatalf("deadline: got %d records, err %v; want the prefix and the deadline", len(got), err)
	}
}

// ---- pipeline scope of the writes ----

// countMeta counts metadata requests (Produce's topic check is its first I/O).
type countMeta struct {
	fakeMeta
	n int
}

func (m *countMeta) ListTopics(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
	m.n++
	return m.fakeMeta.ListTopics(ctx, topics...)
}

// countGroups counts the describe requests of a reset (its first I/O).
type countGroups struct {
	fakeGroups
	n int
}

func (g *countGroups) DescribeGroups(ctx context.Context, groups ...string) (kadm.DescribedGroups, error) {
	g.n++
	return g.fakeGroups.DescribeGroups(ctx, groups...)
}

func scopePipeline() owners.Pipeline {
	return owners.Pipeline{ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"}, ConsumerGroups: []string{"order-sync"}}
}

// TestWriteScope: the writes fail closed like tools.ScopeFor. The room's
// pipeline decides; the opt-out lets everything through; no scope, or only a
// pipeline id, is ErrNoScope. Every refusal comes before any broker request.
func TestWriteScope(t *testing.T) {
	p := scopePipeline()
	bg := context.Background()
	for _, tc := range []struct {
		name  string
		ctx   context.Context
		group string
		topic string
		want  error // nil: allowed
	}{
		{"no scope", bg, "order-sync", "pg.public.orders", tools.ErrNoScope},
		{"pipeline id only", tools.WithPipelineID(bg, "orders"), "order-sync", "pg.public.orders", tools.ErrNoScope},
		{"pipeline id wins over the opt-out", tools.WithPipelineID(tools.WithUnscoped(bg), "orders"), "order-sync", "pg.public.orders", tools.ErrNoScope},
		{"unscoped", unscoped(), "any-group", "any.topic", nil},
		{"in scope", tools.WithPipeline(bg, p), "order-sync", "pg.public.orders", nil},
		{"out of scope", tools.WithPipeline(bg, p), "billing-sync", "pay.public.payments", tools.ErrOutOfScope},
		{"near miss", tools.WithPipeline(bg, p), "order-sync2", "x.pg.public.orders", tools.ErrOutOfScope},
		{"pipeline wins over the opt-out", tools.WithPipeline(tools.WithUnscoped(bg), p), "billing-sync", "pay.public.payments", tools.ErrOutOfScope},
	} {
		g := &countGroups{fakeGroups: fakeGroups{classic: kadm.DescribedGroups{tc.group: {State: "Empty"}}, committed: committedOf("pg.public.orders", map[int32]int64{0: 5})}}
		l := &fakeLister{starts: merge(listed("pg.public.orders", map[int32]int64{0: 0})), ends: merge(listed("pg.public.orders", map[int32]int64{0: 9}))}
		w := &Writer{c: &Client{offs: l}, groups: g}
		moved, err := w.ResetGroupOffsets(tc.ctx, tc.group, clients.OffsetTarget{Earliest: true})
		if tc.want == nil {
			if err != nil || moved != 1 {
				t.Errorf("%s: reset moved=%d err=%v, want it allowed", tc.name, moved, err)
			}
		} else if !errors.Is(err, tc.want) || g.n != 0 || g.fetchCalls != 0 || l.calls.Load() != 0 || len(g.commits) != 0 {
			t.Errorf("%s: reset err=%v, broker calls describe=%d fetch=%d list=%d commit=%d; want %v and none",
				tc.name, err, g.n, g.fetchCalls, l.calls.Load(), len(g.commits), tc.want)
		}

		meta := &countMeta{fakeMeta: fakeMeta{topics: topicHere(tc.topic)}}
		prod := &produceLog{failAt: -1}
		pw := &Writer{c: &Client{meta: meta}, produceFn: prod.produce}
		acked, err := pw.Produce(tc.ctx, tc.topic, []clients.Record{{Value: []byte("v")}})
		if tc.want == nil {
			if err != nil || acked != 1 {
				t.Errorf("%s: produce acked=%d err=%v, want it allowed", tc.name, acked, err)
			}
		} else if !errors.Is(err, tc.want) || meta.n != 0 || len(prod.got) != 0 {
			t.Errorf("%s: produce err=%v, metadata=%d produced=%d; want %v and no request", tc.name, err, meta.n, len(prod.got), tc.want)
		}
	}
}

// TestReadsAreNotScoped: the methods that change nothing run with no scope
// and with a pipeline the names are not part of; the action scopes them.
func TestReadsAreNotScoped(t *testing.T) {
	for _, ctx := range []context.Context{context.Background(), tools.WithPipeline(context.Background(), scopePipeline())} {
		w := &Writer{c: &Client{offs: &fakeLister{
			starts: merge(listed("other", map[int32]int64{0: 0})),
			ends:   merge(listed("other", map[int32]int64{0: 4})),
		}}, groups: &fakeGroups{classic: emptyGroup("other-group"), committed: committedOf("other", map[int32]int64{0: 2})}}
		if err := w.CheckGroupInactive(ctx, "other-group"); err != nil {
			t.Fatalf("CheckGroupInactive: %v", err)
		}
		if _, err := w.PreviousOffsets(ctx, "other-group"); err != nil {
			t.Fatalf("PreviousOffsets: %v", err)
		}
		if _, err := w.PlanGroupReset(ctx, "other-group", clients.OffsetTarget{Earliest: true}); err != nil {
			t.Fatalf("PlanGroupReset: %v", err)
		}
		if _, err := w.ReadRecords(ctx, "other", 0, 4, 1); err != nil {
			t.Fatalf("ReadRecords: %v", err)
		}
	}
}
