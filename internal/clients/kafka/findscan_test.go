package kafka

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
)

// TestFindByKeyReportsMaxScanCut: a window cut by maxScan before the
// partition's readable end must say so; "not found" alone reads as "the key
// is absent" when the scan never reached the newer records.
func TestFindByKeyReportsMaxScanCut(t *testing.T) {
	var _ clients.KeyScanReader = (*Client)(nil)
	f := &fakeTopic{name: "t", parts: map[int32]partLog{
		0: decided(data(0, 0, "a"), data(0, 1, "b"), data(0, 2, "k")),
		1: decided(data(1, 0, "k")),
	}}
	var got clients.KeyScan
	quick(t, func(ctx context.Context) (err error) {
		got, err = f.client().FindByKeyScan(ctx, "t", "k", epoch, 2)
		return err
	})
	if !reflect.DeepEqual(got.Truncated, []int32{0}) {
		t.Fatalf("truncated = %v, want [0]", got.Truncated)
	}
	if len(got.Refs) != 1 || got.Refs[0].Partition != 1 {
		t.Fatalf("refs = %+v, want the match in partition 1", got.Refs)
	}

	// A window that reaches the readable end is not truncated, even when
	// maxScan equals the records left.
	quick(t, func(ctx context.Context) (err error) {
		got, err = f.client().FindByKeyScan(ctx, "t", "k", epoch, 3)
		return err
	})
	if len(got.Truncated) != 0 || len(got.Refs) != 2 {
		t.Fatalf("got %+v, want both matches and no truncation", got)
	}
}

// TestFindByKeyOpenTxnIsNotTruncation: an open transaction (LSO below the
// high watermark) ends the readable window; it is not a maxScan cut.
func TestFindByKeyOpenTxnIsNotTruncation(t *testing.T) {
	f := &fakeTopic{name: "t", parts: map[int32]partLog{
		0: {recs: []*kgo.Record{data(0, 0, "a"), data(0, 1, "k"), txData(0, 2, "x"), txData(0, 3, "y")}, lso: 2},
	}}
	var got clients.KeyScan
	quick(t, func(ctx context.Context) (err error) {
		got, err = f.client().FindByKeyScan(ctx, "t", "k", epoch, 2)
		return err
	})
	if len(got.Truncated) != 0 || len(got.Refs) != 1 {
		t.Fatalf("got %+v, want one match, no truncation", got)
	}
}

// TestFindByKeyScansWholeWindow: a match in the first poll used to end the
// scan, so a newer match later in the window was never seen and callers
// that take the last ref as the newest (topology trace) got a stale one.
func TestFindByKeyScansWholeWindow(t *testing.T) {
	l := &fakeLister{
		starts: merge(listed("t", map[int32]int64{0: 0})),
		ends:   merge(listed("t", map[int32]int64{0: 4})),
	}
	cons := &fakeConsumer{polls: []kgo.Fetches{
		fetchOf("t", data(0, 0, "k"), data(0, 1, "a")),
		fetchOf("t", data(0, 2, "k"), data(0, 3, "b")),
	}}
	var got clients.KeyScan
	quick(t, func(ctx context.Context) (err error) {
		got, err = clientWith(l, cons).FindByKeyScan(ctx, "t", "k", epoch, 0)
		return err
	})
	if len(got.Refs) != 2 || got.Refs[1].Offset != 2 || len(got.Truncated) != 0 {
		t.Fatalf("got %+v, want matches at offsets 0 and 2, newest last", got)
	}
}

// TestFindByKeyDeadlineKeepsMatches: when the scan deadline cuts a window
// after a match, the matches are returned with the unfinished partition
// reported as truncated instead of being thrown away with an error.
func TestFindByKeyDeadlineKeepsMatches(t *testing.T) {
	l := &fakeLister{
		starts: merge(listed("t", map[int32]int64{0: 0})),
		ends:   merge(listed("t", map[int32]int64{0: 10})),
	}
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("t", data(0, 0, "k"))}}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	got, err := clientWith(l, cons).FindByKeyScan(ctx, "t", "k", epoch, 0)
	if err != nil {
		t.Fatalf("err = %v, want the match found before the deadline", err)
	}
	if len(got.Refs) != 1 || !reflect.DeepEqual(got.Truncated, []int32{0}) {
		t.Fatalf("got %+v, want one match and partition 0 truncated", got)
	}
}

// TestFindByKeyOrdersByOffset: per partition by offset (producer timestamps can go
// backwards, the log cannot), partitions in order.
func TestFindByKeyOrdersByOffset(t *testing.T) {
	l := &fakeLister{
		starts: merge(listed("t", map[int32]int64{0: 0, 1: 0})),
		ends:   merge(listed("t", map[int32]int64{0: 3, 1: 1})),
	}
	late := data(0, 0, "k")
	late.Timestamp = tsAt(100) // CreateTime newer than the later record's
	cons := &fakeConsumer{polls: []kgo.Fetches{
		fetchOf("t", data(1, 0, "k"), late, data(0, 1, "a"), data(0, 2, "k")),
	}}
	var got clients.KeyScan
	quick(t, func(ctx context.Context) (err error) {
		got, err = clientWith(l, cons).FindByKeyScan(ctx, "t", "k", epoch, 0)
		return err
	})
	var order [][2]int64
	for _, r := range got.Refs {
		order = append(order, [2]int64{int64(r.Partition), r.Offset})
	}
	if want := [][2]int64{{0, 0}, {0, 2}, {1, 0}}; !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

// TestFindByKeyGapAtWindowEnd: compaction or an aborted transaction can leave
// no record at the window's last offset; the next record delivered lies past
// the window. It must still finish the partition, or the scan spins to its
// deadline and an absent key reads as "scan incomplete".
func TestFindByKeyGapAtWindowEnd(t *testing.T) {
	l := &fakeLister{
		starts: merge(listed("t", map[int32]int64{0: 0})),
		ends:   merge(listed("t", map[int32]int64{0: 10})),
	}
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("t", data(0, 0, "a"), data(0, 1, "b"), data(0, 5, "c"))}}
	var got clients.KeyScan
	quick(t, func(ctx context.Context) (err error) {
		got, err = clientWith(l, cons).FindByKeyScan(ctx, "t", "k", epoch, 3)
		return err
	})
	if len(got.Refs) != 0 || !reflect.DeepEqual(got.Truncated, []int32{0}) {
		t.Fatalf("got %+v, want not found with partition 0 cut by max_scan", got)
	}
}
