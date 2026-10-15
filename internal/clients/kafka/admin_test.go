package kafka

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
)

// consumeMode is what a kgo consumer built from a list of options does with
// transactions: whether it delivers markers and whether it reads committed
// records only.
type consumeMode struct {
	parts         map[string]map[int32]kgo.Offset
	keepControl   bool
	readCommitted bool
}

// consumeModeOf builds (never polls) a kgo client from opts and reads the
// options back, so a fake consumer behaves as the real one would.
func consumeModeOf(opts []kgo.Opt) consumeMode {
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers("127.0.0.1:1")}, opts...)...)
	if err != nil {
		return consumeMode{}
	}
	defer cl.Close()
	return modeOfClient(cl)
}

func modeOfClient(cl *kgo.Client) consumeMode {
	// kgo keeps the isolation level unexported: compare with a reference.
	ref, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	if err != nil {
		return consumeMode{}
	}
	defer ref.Close()
	parts, _ := cl.OptValue(kgo.ConsumePartitions).(map[string]map[int32]kgo.Offset)
	keep, _ := cl.OptValue(kgo.KeepControlRecords).(bool)
	return consumeMode{
		parts:         parts,
		keepControl:   keep,
		readCommitted: cl.OptValue(kgo.FetchIsolationLevel) == ref.OptValue(kgo.FetchIsolationLevel),
	}
}

// partLog is one partition as the broker holds it: records and transaction
// markers in offset order, the data records of aborted transactions, and the
// last stable offset (below it every transaction is decided).
type partLog struct {
	start   int64 // log start offset of a partition with no records
	recs    []*kgo.Record
	aborted map[int64]bool
	lso     int64
}

func (l partLog) startOffset() int64 {
	if len(l.recs) > 0 {
		return l.recs[0].Offset
	}
	return l.start
}

func (l partLog) hwm() int64 {
	if len(l.recs) > 0 {
		return l.recs[len(l.recs)-1].Offset + 1
	}
	return l.start
}

// decided is a partition with no open transaction: LSO = high watermark.
func decided(recs ...*kgo.Record) partLog {
	l := partLog{recs: recs}
	l.lso = l.hwm()
	return l
}

var (
	t0    = time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	epoch = time.Unix(0, 0).UTC()
)

// tsAt is the timestamp the test logs give the record at off.
func tsAt(off int64) time.Time { return t0.Add(time.Duration(off) * time.Second) }

func data(p int32, off int64, key string) *kgo.Record {
	return &kgo.Record{Topic: "t", Partition: p, Offset: off, Timestamp: tsAt(off), Key: []byte(key), Value: []byte("v")}
}

func txData(p int32, off int64, key string) *kgo.Record {
	r := data(p, off, key)
	r.Attrs = kgo.NewRecordAttrs(kgo.RecordAttrsOpts{Transactional: true})
	return r
}

// marker is a commit or abort marker; its key is the control key, never a
// user key.
func marker(p int32, off int64) *kgo.Record {
	r := data(p, off, "\x00\x00\x00\x00")
	r.Value = nil
	r.Attrs = kgo.NewRecordAttrs(kgo.RecordAttrsOpts{Control: true, Transactional: true})
	return r
}

// fakeTopic is a broker holding one topic. It answers the offset listings and
// opens logConsumers on it, so TopicOffsets and FindByKey run end to end.
type fakeTopic struct {
	name  string
	parts map[int32]partLog
	cons  []*logConsumer
}

func (f *fakeTopic) list(ctx context.Context, topics []string, off func(partLog) int64) (kadm.ListedOffsets, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := kadm.ListedOffsets{}
	for _, t := range topics {
		if t != f.name {
			out[t] = missing(t)[t]
			continue
		}
		m := map[int32]kadm.ListedOffset{}
		for p, l := range f.parts {
			m[p] = kadm.ListedOffset{Topic: t, Partition: p, Offset: off(l)}
		}
		out[t] = m
	}
	return out, nil
}

func (f *fakeTopic) ListStartOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	return f.list(ctx, topics, partLog.startOffset)
}

func (f *fakeTopic) ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	return f.list(ctx, topics, partLog.hwm)
}

func (f *fakeTopic) ListCommittedOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	return f.list(ctx, topics, func(l partLog) int64 { return l.lso })
}

func (f *fakeTopic) ListOffsetsAfterMilli(ctx context.Context, ms int64, topics ...string) (kadm.ListedOffsets, error) {
	return f.list(ctx, topics, func(l partLog) int64 {
		for _, r := range l.recs {
			if r.Timestamp.UnixMilli() >= ms {
				return r.Offset
			}
		}
		return l.hwm()
	})
}

func (f *fakeTopic) client() *Client {
	return &Client{offs: f, consumerFn: func(opts []kgo.Opt) (pollConsumer, error) {
		lc := &logConsumer{topic: f, mode: consumeModeOf(opts)}
		f.cons = append(f.cons, lc)
		return lc, nil
	}}
}

// logConsumer serves fakeTopic the way a kgo consumer built with mode would:
// from each requested offset, markers only with KeepControlRecords and, with
// ReadCommitted, only offsets below the LSO and no aborted record. It serves
// everything in one poll, then blocks until ctx ends (nothing new arrives).
type logConsumer struct {
	topic  *fakeTopic
	mode   consumeMode
	served bool
	closed bool
}

func (l *logConsumer) PollFetches(ctx context.Context) kgo.Fetches {
	var recs []*kgo.Record
	if !l.served {
		l.served = true
		for p, off := range l.mode.parts[l.topic.name] {
			pl := l.topic.parts[p]
			for _, r := range pl.recs {
				switch {
				case r.Offset < off.EpochOffset().Offset:
				case r.Attrs.IsControl() && !l.mode.keepControl:
				case l.mode.readCommitted && (r.Offset >= pl.lso || pl.aborted[r.Offset]):
				default:
					recs = append(recs, r)
				}
			}
		}
	}
	if len(recs) == 0 {
		<-ctx.Done()
		return kgo.NewErrFetch(ctx.Err())
	}
	return fetchOf(l.topic.name, recs...)
}

func (l *logConsumer) Close() { l.closed = true }

// quick runs fn under a deadline well above what a correct read needs: a read
// that waits for a record that never comes fails on it.
func quick(t *testing.T, fn func(ctx context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := fn(ctx); err != nil {
		t.Fatalf("after %s: %v", time.Since(start).Round(time.Millisecond), err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("took %s: the read waited for an offset that never arrives", d)
	}
}

func TestTopicOffsetsTransactionalTail(t *testing.T) {
	tests := []struct {
		name  string
		parts map[int32]partLog
		want  []clients.PartitionOffsets
	}{
		{
			name:  "no transactions",
			parts: map[int32]partLog{0: decided(data(0, 0, "a"), data(0, 1, "b"))},
			want:  []clients.PartitionOffsets{{Partition: 0, Start: 0, End: 2, LastTimestamp: tsAt(1)}},
		},
		{
			name:  "commit marker at the tail",
			parts: map[int32]partLog{0: decided(txData(0, 0, "a"), txData(0, 1, "b"), marker(0, 2))},
			want:  []clients.PartitionOffsets{{Partition: 0, Start: 0, End: 3, LastTimestamp: tsAt(2)}},
		},
		{
			name: "abort marker at the tail",
			parts: map[int32]partLog{0: {
				recs:    []*kgo.Record{data(0, 0, "a"), txData(0, 1, "x"), txData(0, 2, "y"), marker(0, 3)},
				aborted: map[int64]bool{1: true, 2: true},
				lso:     4,
			}},
			want: []clients.PartitionOffsets{{Partition: 0, Start: 0, End: 4, LastTimestamp: tsAt(3)}},
		},
		{
			name: "open transaction at the tail: newest committed record, high watermark kept",
			parts: map[int32]partLog{0: {
				recs: []*kgo.Record{data(0, 0, "a"), data(0, 1, "b"), txData(0, 2, "x"), txData(0, 3, "y")},
				lso:  2,
			}},
			want: []clients.PartitionOffsets{{Partition: 0, Start: 0, End: 4, LastTimestamp: tsAt(1)}},
		},
		{
			name:  "open transaction is all there is",
			parts: map[int32]partLog{0: {recs: []*kgo.Record{txData(0, 0, "x")}, lso: 0}},
			want:  []clients.PartitionOffsets{{Partition: 0, Start: 0, End: 1}},
		},
		{
			name: "empty partition next to a marker tail",
			parts: map[int32]partLog{
				0: decided(txData(0, 4, "a"), marker(0, 5)),
				1: {start: 7, lso: 7},
			},
			want: []clients.PartitionOffsets{
				{Partition: 0, Start: 4, End: 6, LastTimestamp: tsAt(5)},
				{Partition: 1, Start: 7, End: 7},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeTopic{name: "t", parts: tt.parts}
			var got []clients.PartitionOffsets
			quick(t, func(ctx context.Context) (err error) {
				got, err = f.client().TopicOffsets(ctx, "t")
				return err
			})
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("TopicOffsets = %+v, want %+v", got, tt.want)
			}
			for _, lc := range f.cons {
				if !lc.closed {
					t.Fatal("consumer not closed")
				}
			}
		})
	}
}

func TestTopicOffsetsErrors(t *testing.T) {
	boom := errors.New("connection refused")
	authErr := map[string]map[int32]kadm.ListedOffset{"t": {-1: {Partition: -1, Err: kerr.TopicAuthorizationFailed}}}
	partErr := map[string]map[int32]kadm.ListedOffset{"t": {0: {Partition: 0, Err: kerr.LeaderNotAvailable}}}
	ok := merge(listed("t", map[int32]int64{0: 0}))
	okEnd := merge(listed("t", map[int32]int64{0: 2}))
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name     string
		c        *Client
		ctx      context.Context
		want     error
		notFound bool
	}{
		{name: "broker request fails", c: &Client{offs: &fakeLister{err: boom}}, want: boom},
		{name: "unknown topic", c: (&fakeTopic{name: "other"}).client(), want: clients.ErrNotFound, notFound: true},
		{name: "authorization failure is not 'not found'", c: &Client{offs: &fakeLister{starts: merge(authErr), ends: merge(authErr)}}, want: kerr.TopicAuthorizationFailed},
		{name: "partition error", c: &Client{offs: &fakeLister{starts: merge(partErr), ends: okEnd}}, want: kerr.LeaderNotAvailable},
		{name: "end offset missing from response", c: &Client{offs: &fakeLister{starts: ok, ends: merge(listed("t", map[int32]int64{1: 2}))}}},
		{name: "last stable offset error", c: &Client{offs: &fakeLister{starts: ok, ends: okEnd,
			stable: merge(map[string]map[int32]kadm.ListedOffset{"t": {0: {Partition: 0, Err: kerr.NotLeaderForPartition}}})}},
			want: kerr.NotLeaderForPartition},
		{name: "deadline before the listing", c: (&fakeTopic{name: "t", parts: map[int32]partLog{0: decided(data(0, 0, "a"))}}).client(),
			ctx: expired, want: context.Canceled},
		{name: "record read times out", c: clientWith(&fakeLister{starts: ok, ends: okEnd}, &fakeConsumer{}), want: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.ctx
			if ctx == nil {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
				defer cancel()
			}
			got, err := tt.c.TopicOffsets(ctx, "t")
			if err == nil {
				t.Fatalf("want error, got %+v", got)
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if !tt.notFound && errors.Is(err, clients.ErrNotFound) {
				t.Fatalf("err = %v: a failure reported as 'not found'", err)
			}
		})
	}
}

func TestFindByKeyTransactions(t *testing.T) {
	keyed := func(p int32, off int64, key string) *kgo.Record {
		r := marker(p, off)
		r.Key = []byte(key)
		return r
	}
	tests := []struct {
		name    string
		parts   map[int32]partLog
		key     string
		maxScan int
		want    []clients.RecordRef
	}{
		{
			name:  "found",
			parts: map[int32]partLog{0: decided(data(0, 0, "a"), data(0, 1, "k"), data(0, 2, "b"))},
			key:   "k",
			want:  []clients.RecordRef{{Topic: "t", Partition: 0, Offset: 1, Timestamp: tsAt(1), Key: "k"}},
		},
		{
			name:  "found in a committed transaction",
			parts: map[int32]partLog{0: decided(txData(0, 0, "a"), txData(0, 1, "k"), marker(0, 2))},
			key:   "k",
			want:  []clients.RecordRef{{Topic: "t", Partition: 0, Offset: 1, Timestamp: tsAt(1), Key: "k"}},
		},
		{
			name:  "absent: the commit marker ends the window",
			parts: map[int32]partLog{0: decided(txData(0, 0, "a"), txData(0, 1, "b"), marker(0, 2))},
			key:   "k",
		},
		{
			name: "a record of an aborted transaction is not a match",
			parts: map[int32]partLog{0: {
				recs:    []*kgo.Record{data(0, 0, "a"), txData(0, 1, "k"), marker(0, 2)},
				aborted: map[int64]bool{1: true},
				lso:     3,
			}},
			key: "k",
		},
		{
			name:  "an open transaction at the tail is neither read nor waited for",
			parts: map[int32]partLog{0: {recs: []*kgo.Record{data(0, 0, "a"), txData(0, 1, "k")}, lso: 1}},
			key:   "k",
		},
		{
			name:  "a marker is never a match",
			parts: map[int32]partLog{0: decided(txData(0, 0, "a"), keyed(0, 1, "k"))},
			key:   "k",
		},
		{
			name:    "max_scan bounds the window",
			parts:   map[int32]partLog{0: decided(data(0, 0, "a"), data(0, 1, "b"), data(0, 2, "k"))},
			key:     "k",
			maxScan: 2,
		},
		{
			name: "partitions without anything readable are skipped",
			parts: map[int32]partLog{
				0: decided(data(0, 0, "a"), marker(0, 1)),
				1: {start: 3, lso: 3},
				2: {recs: []*kgo.Record{txData(2, 0, "k")}, lso: 0},
			},
			key: "k",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeTopic{name: "t", parts: tt.parts}
			var got []clients.RecordRef
			quick(t, func(ctx context.Context) (err error) {
				got, err = f.client().FindByKey(ctx, "t", tt.key, epoch, tt.maxScan)
				return err
			})
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("FindByKey = %+v, want %+v", got, tt.want)
			}
			for _, lc := range f.cons {
				if !lc.closed {
					t.Fatal("consumer not closed")
				}
			}
		})
	}
}

func TestFindByKeyErrors(t *testing.T) {
	boom := errors.New("connection refused")
	ok := merge(listed("t", map[int32]int64{0: 0}))
	okEnd := merge(listed("t", map[int32]int64{0: 2}))
	partErr := merge(map[string]map[int32]kadm.ListedOffset{"t": {0: {Partition: 0, Err: kerr.LeaderNotAvailable}}})
	tests := []struct {
		name     string
		c        *Client
		want     error
		notFound bool
	}{
		{name: "broker request fails", c: &Client{offs: &fakeLister{err: boom}}, want: boom},
		{name: "unknown topic", c: (&fakeTopic{name: "other"}).client(), want: clients.ErrNotFound, notFound: true},
		{name: "partition error is not a silent skip", c: &Client{offs: &fakeLister{after: partErr, ends: okEnd}}, want: kerr.LeaderNotAvailable},
		{name: "scan times out", c: clientWith(&fakeLister{starts: ok, ends: okEnd}, &fakeConsumer{}), want: context.DeadlineExceeded},
		{name: "fetch error", c: clientWith(&fakeLister{starts: ok, ends: okEnd},
			&fakeConsumer{polls: []kgo.Fetches{kgo.NewErrFetch(kerr.TopicAuthorizationFailed)}}), want: kerr.TopicAuthorizationFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			got, err := tt.c.FindByKey(ctx, "t", "k", epoch, 0)
			if err == nil {
				t.Fatalf("want error, got %+v", got)
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if !tt.notFound && errors.Is(err, clients.ErrNotFound) {
				t.Fatalf("err = %v: a failure reported as 'not found'", err)
			}
		})
	}
}

// TestOffsetsByTimeRefusePreEpoch: Kafka reads the timestamps -1 and -2 as
// "latest" and "earliest", so a since before 1970 never reaches the broker.
func TestOffsetsByTimeRefusePreEpoch(t *testing.T) {
	sent := errors.New("request sent")
	for _, since := range []time.Time{epoch.Add(-time.Millisecond), epoch.Add(-2 * time.Millisecond), {}} {
		l := &fakeLister{err: sent}
		if _, err := (&Client{offs: l}).FindByKey(context.Background(), "t", "k", since, 0); err == nil || errors.Is(err, sent) || l.calls.Load() != 0 {
			t.Errorf("FindByKey since %s: err = %v, %d requests; want refused before any request", since, err, l.calls.Load())
		}
		c := &Client{clusterFn: &fakeCluster{afterErr: sent}}
		if _, err := c.PartitionRates(context.Background(), since, "t"); err == nil || errors.Is(err, sent) {
			t.Errorf("PartitionRates since %s: err = %v; want refused before any request", since, err)
		}
	}
	if ms, err := afterMilli(epoch); err != nil || ms != 0 {
		t.Fatalf("afterMilli(epoch) = %d, %v; want 0", ms, err)
	}
}

func TestNormalizeMaxScan(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for in, want := range map[int]int{-1: defaultMaxScan, 0: defaultMaxScan, 1: 1, maxFindScan: maxFindScan, maxFindScan + 1: maxFindScan, maxInt: maxFindScan} {
		if got := normalizeMaxScan(in); got != want {
			t.Errorf("normalizeMaxScan(%d) = %d, want %d", in, got, want)
		}
	}
}

// TestFindByKeyHugeMaxScan: start + maxScan used to overflow into a window
// that ends before it starts, a false "not found".
func TestFindByKeyHugeMaxScan(t *testing.T) {
	f := &fakeTopic{name: "t", parts: map[int32]partLog{0: decided(data(0, 5, "a"), data(0, 6, "k"))}}
	var got []clients.RecordRef
	quick(t, func(ctx context.Context) (err error) {
		got, err = f.client().FindByKey(ctx, "t", "k", epoch, int(^uint(0)>>1))
		return err
	})
	if len(got) != 1 || got[0].Offset != 6 {
		t.Fatalf("got %+v, want the record at offset 6", got)
	}
}

// TestRecordReadsUseCommittedConsumer pins the real (non-seam) consumer every
// record read opens: committed records only, transaction markers delivered.
func TestRecordReadsUseCommittedConsumer(t *testing.T) {
	c := &Client{brokers: []string{"127.0.0.1:1"}}
	parts := map[string]map[int32]kgo.Offset{"t": {0: kgo.NewOffset().At(3)}}
	cons, err := c.newConsumer(parts)
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	cl, ok := cons.(*kgo.Client)
	if !ok {
		t.Fatalf("consumer is %T, want *kgo.Client", cons)
	}
	m := modeOfClient(cl)
	if !m.keepControl || !m.readCommitted || !reflect.DeepEqual(m.parts, parts) {
		t.Fatalf("consumer mode = %+v, want markers kept, read committed, partitions %v", m, parts)
	}
}

// fakeMeta answers the metadata, group and config requests from fixed values.
type fakeMeta struct {
	topics  kadm.TopicDetails
	groups  kadm.ListedGroups
	lags    kadm.DescribedGroupLags
	configs kadm.ResourceConfigs
	err     error // every request
	cfgErr  error // DescribeTopicConfigs only
}

func (f *fakeMeta) ListTopics(ctx context.Context, _ ...string) (kadm.TopicDetails, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.topics, f.err
}

func (f *fakeMeta) ListGroups(ctx context.Context, _ ...string) (kadm.ListedGroups, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.groups, f.err
}

func (f *fakeMeta) Lag(ctx context.Context, _ ...string) (kadm.DescribedGroupLags, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.lags, f.err
}

func (f *fakeMeta) DescribeTopicConfigs(ctx context.Context, _ ...string) (kadm.ResourceConfigs, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.cfgErr != nil {
		return nil, f.cfgErr
	}
	return f.configs, f.err
}

func TestTopicsAndConsumerGroups(t *testing.T) {
	m := &fakeMeta{
		topics: kadm.TopicDetails{
			"b": {Topic: "b"}, "a": {Topic: "a"}, "__consumer_offsets": {}, "_schemas": {}, "_connect_configs": {},
			"denied": {Topic: "denied", Err: kerr.TopicAuthorizationFailed},
		},
		groups: kadm.ListedGroups{"order-sync": {Group: "order-sync"}, "billing": {Group: "billing"}},
	}
	c := &Client{meta: m}
	topics, err := c.Topics(context.Background())
	if err != nil || !reflect.DeepEqual(topics, []string{"a", "b"}) {
		t.Fatalf("Topics = %v, %v; want [a b]", topics, err)
	}
	groups, err := c.ConsumerGroups(context.Background())
	if err != nil || !reflect.DeepEqual(groups, []string{"billing", "order-sync"}) {
		t.Fatalf("ConsumerGroups = %v, %v", groups, err)
	}
}

func TestGroupLag(t *testing.T) {
	lag := kadm.GroupLag{
		"b": {0: {Topic: "b", Partition: 0, Commit: kadm.Offset{At: 7}, End: kadm.ListedOffset{Offset: 9}, Lag: 2}},
		"a": {
			1: {Topic: "a", Partition: 1, Commit: kadm.Offset{At: 3}, End: kadm.ListedOffset{Offset: 8}, Lag: 5},
			0: {Topic: "a", Partition: 0, Commit: kadm.Offset{At: -1}, End: kadm.ListedOffset{Offset: 4}, Lag: 4},
			2: {Topic: "a", Partition: 2, Commit: kadm.Offset{At: -1}, End: kadm.ListedOffset{Offset: 6}, Lag: -1, Err: kerr.NotLeaderForPartition},
		},
	}
	m := &fakeMeta{lags: kadm.DescribedGroupLags{"g": {
		Group: "g", State: "Stable", Members: make([]kadm.DescribedGroupMember, 2), Lag: lag,
	}}}
	got, err := (&Client{meta: m}).GroupLag(context.Background(), "g")
	if err != nil {
		t.Fatal(err)
	}
	want := clients.GroupLag{Group: "g", State: "Stable", Members: 2, TotalLag: 11, Partitions: []clients.PartitionLag{
		{Topic: "a", Partition: 0, Committed: -1, End: 4, Lag: 4},
		{Topic: "a", Partition: 1, Committed: 3, End: 8, Lag: 5},
		{Topic: "b", Partition: 0, Committed: 7, End: 9, Lag: 2},
	}, Unknown: []clients.PartitionLag{
		{Topic: "a", Partition: 2, Committed: -1, End: 6, Lag: -1},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GroupLag = %+v, want %+v", got, want)
	}
}

// TestGroupLagUnknown: kadm's Lag swallows a ListOffsets or OffsetFetch shard
// error (a broker down) into the partition's Err. Such a partition used to be
// dropped, so a broker outage read as "total lag 0"; it must be listed in
// Unknown with the offsets that were read and Lag -1, and stay out of
// TotalLag.
func TestGroupLagUnknown(t *testing.T) {
	endErr := fmt.Errorf("end offsets: %w", kerr.NotLeaderForPartition)
	lag := kadm.GroupLag{"a": {
		0: {Topic: "a", Partition: 0, Commit: kadm.Offset{At: 5}, End: kadm.ListedOffset{Offset: 5}, Lag: 0},
		// End offset unreadable, commit read: Committed kept, End -1.
		1: {Topic: "a", Partition: 1, Commit: kadm.Offset{At: 40}, End: kadm.ListedOffset{Offset: -1, Err: endErr}, Lag: -1, Err: endErr},
		// Commit unreadable, end read: Committed -1, End kept.
		2: {Topic: "a", Partition: 2, Commit: kadm.Offset{At: 12}, End: kadm.ListedOffset{Offset: 90}, Lag: -1, Err: kerr.CoordinatorNotAvailable},
		// Both unreadable: kadm reports the commit error first.
		3: {Topic: "a", Partition: 3, Commit: kadm.Offset{At: 3}, End: kadm.ListedOffset{Offset: 8, Err: endErr}, Lag: -1, Err: kerr.CoordinatorNotAvailable},
	}}
	m := &fakeMeta{lags: kadm.DescribedGroupLags{"g": {Group: "g", State: "Stable", Lag: lag}}}
	got, err := (&Client{meta: m}).GroupLag(context.Background(), "g")
	if err != nil {
		t.Fatal(err)
	}
	want := clients.GroupLag{Group: "g", State: "Stable", TotalLag: 0,
		Partitions: []clients.PartitionLag{{Topic: "a", Partition: 0, Committed: 5, End: 5, Lag: 0}},
		Unknown: []clients.PartitionLag{
			{Topic: "a", Partition: 1, Committed: 40, End: -1, Lag: -1},
			{Topic: "a", Partition: 2, Committed: -1, End: 90, Lag: -1},
			{Topic: "a", Partition: 3, Committed: -1, End: -1, Lag: -1},
		}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GroupLag =\n %+v\nwant\n %+v", got, want)
	}
}

func TestMetadataErrors(t *testing.T) {
	boom := errors.New("connection refused")
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := kadm.ResourceConfigs{{Name: "t", Configs: []kadm.Config{{Key: "retention.ms", Value: kadm.StringPtr("1")}}}}
	topic := kadm.TopicDetails{"t": {Topic: "t", Partitions: kadm.PartitionDetails{0: {Replicas: []int32{1}}}}}
	calls := map[string]func(c *Client, ctx context.Context) error{
		"Topics":         func(c *Client, ctx context.Context) error { _, err := c.Topics(ctx); return err },
		"ConsumerGroups": func(c *Client, ctx context.Context) error { _, err := c.ConsumerGroups(ctx); return err },
		"GroupLag":       func(c *Client, ctx context.Context) error { _, err := c.GroupLag(ctx, "g"); return err },
		"DescribeTopicConfigs": func(c *Client, ctx context.Context) error {
			_, err := c.DescribeTopicConfigs(ctx, "t")
			return err
		},
	}
	tests := []struct {
		name, call string
		m          *fakeMeta
		ctx        context.Context
		want       error
	}{
		{"broker request fails", "Topics", &fakeMeta{err: boom}, nil, boom},
		{"broker request fails", "ConsumerGroups", &fakeMeta{err: boom}, nil, boom},
		{"broker request fails", "GroupLag", &fakeMeta{err: boom}, nil, boom},
		{"broker request fails", "DescribeTopicConfigs", &fakeMeta{err: boom}, nil, boom},
		{"deadline", "Topics", &fakeMeta{}, expired, context.Canceled},
		{"deadline", "GroupLag", &fakeMeta{}, expired, context.Canceled},
		{"unknown group", "GroupLag", &fakeMeta{lags: kadm.DescribedGroupLags{}}, nil, clients.ErrNotFound},
		{"describe error", "GroupLag", &fakeMeta{lags: kadm.DescribedGroupLags{"g": {Group: "g", DescribeErr: kerr.GroupAuthorizationFailed}}}, nil, kerr.GroupAuthorizationFailed},
		{"offset fetch error", "GroupLag", &fakeMeta{lags: kadm.DescribedGroupLags{"g": {Group: "g", FetchErr: kerr.CoordinatorNotAvailable}}}, nil, kerr.CoordinatorNotAvailable},
		{"unknown topic (request)", "DescribeTopicConfigs", &fakeMeta{cfgErr: kerr.UnknownTopicOrPartition}, nil, clients.ErrNotFound},
		{"unknown topic (resource)", "DescribeTopicConfigs", &fakeMeta{configs: kadm.ResourceConfigs{{Name: "t", Err: kerr.UnknownTopicOrPartition}}}, nil, clients.ErrNotFound},
		{"topic absent from configs", "DescribeTopicConfigs", &fakeMeta{configs: kadm.ResourceConfigs{}}, nil, clients.ErrNotFound},
		{"topic absent from metadata", "DescribeTopicConfigs", &fakeMeta{configs: cfg, topics: kadm.TopicDetails{}}, nil, clients.ErrNotFound},
		{"resource authorization", "DescribeTopicConfigs", &fakeMeta{configs: kadm.ResourceConfigs{{Name: "t", Err: kerr.TopicAuthorizationFailed}}, topics: topic}, nil, kerr.TopicAuthorizationFailed},
	}
	for _, tt := range tests {
		t.Run(tt.call+"/"+tt.name, func(t *testing.T) {
			ctx := tt.ctx
			if ctx == nil {
				ctx = context.Background()
			}
			err := calls[tt.call](&Client{meta: tt.m}, ctx)
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if tt.want != clients.ErrNotFound && errors.Is(err, clients.ErrNotFound) {
				t.Fatalf("err = %v: a failure reported as 'not found'", err)
			}
		})
	}
}

// TestAdminBrokerDown runs every admin read against a port nothing listens
// on: each one fails by the caller's deadline, never as "not found".
func TestAdminBrokerDown(t *testing.T) {
	c := unreachable(t)
	calls := map[string]func(ctx context.Context) error{
		"Topics":         func(ctx context.Context) error { _, err := c.Topics(ctx); return err },
		"TopicOffsets":   func(ctx context.Context) error { _, err := c.TopicOffsets(ctx, "t"); return err },
		"ConsumerGroups": func(ctx context.Context) error { _, err := c.ConsumerGroups(ctx); return err },
		"GroupLag":       func(ctx context.Context) error { _, err := c.GroupLag(ctx, "g"); return err },
		"FindByKey":      func(ctx context.Context) error { _, err := c.FindByKey(ctx, "t", "k", epoch, 0); return err },
		"DescribeTopicConfigs": func(ctx context.Context) error {
			_, err := c.DescribeTopicConfigs(ctx, "t")
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			start := time.Now()
			err := call(ctx)
			if err == nil || errors.Is(err, clients.ErrNotFound) {
				t.Fatalf("err = %v, want a broker failure", err)
			}
			if d := time.Since(start); d > 2*time.Second {
				t.Fatalf("returned after %s, long past the deadline", d)
			}
		})
	}
}
