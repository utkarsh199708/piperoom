package kafka

import (
	"testing"
	"time"

	"piperoom/internal/clients"
)

// TestSortRefsOffsetWithinPartition: producer timestamps can go backwards
// (CreateTime is set by the client), so within one partition the log order,
// i.e. the offset, decides which occurrence is newest.
func TestSortRefsOffsetWithinPartition(t *testing.T) {
	now := time.Unix(10_000, 0)
	refs := []clients.RecordRef{
		{Partition: 0, Offset: 1, Timestamp: now.Add(-time.Hour)},
		{Partition: 0, Offset: 0, Timestamp: now},
	}
	SortRefs(refs)
	if refs[0].Offset != 0 || refs[1].Offset != 1 {
		t.Fatalf("order = %+v, want offsets 0, 1 (offset 1 newest)", refs)
	}
}

// TestSortRefsMergesPartitionsByTime: across partitions timestamps interleave
// the lists, but each partition keeps its offset order.
func TestSortRefsMergesPartitionsByTime(t *testing.T) {
	t0 := time.Unix(1000, 0)
	refs := []clients.RecordRef{
		{Partition: 1, Offset: 4, Timestamp: t0.Add(1 * time.Second)},
		{Partition: 0, Offset: 2, Timestamp: t0.Add(5 * time.Second)},
		{Partition: 0, Offset: 3, Timestamp: t0}, // clock went back
		{Partition: 1, Offset: 6, Timestamp: t0.Add(9 * time.Second)},
	}
	SortRefs(refs)
	var got []int64
	for _, r := range refs {
		got = append(got, r.Offset)
	}
	// p1/4 (1s) before p0/2 (5s); p0/3 must follow p0/2; p1/6 last.
	want := []int64{4, 2, 3, 6}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}
