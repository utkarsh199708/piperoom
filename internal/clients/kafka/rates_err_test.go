package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// TestPartitionRatesTopicErrors: kadm reports any topic-level metadata
// error with the partition -1 marker. Only UnknownTopicOrPartition means "no
// such topic"; an authorization failure was dropped, and the tool then said
// "not found". A failed after-time lookup read as zero records.
func TestPartitionRatesTopicErrors(t *testing.T) {
	marker := func(err error) kadm.ListedOffsets {
		return kadm.ListedOffsets{"t": {-1: {Topic: "t", Partition: -1, Err: err}}}
	}
	ok := listedOffs("t", map[int32]int64{0: 10})
	partErr := kadm.ListedOffsets{"t": {0: {Topic: "t", Partition: 0, Err: kerr.LeaderNotAvailable}}}
	for _, tt := range []struct {
		name        string
		after, ends kadm.ListedOffsets
		want        error
	}{
		{"auth failure on the topic", marker(kerr.TopicAuthorizationFailed), marker(kerr.TopicAuthorizationFailed), kerr.TopicAuthorizationFailed},
		{"after-time lookup failed", partErr, ok, kerr.LeaderNotAvailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeCluster{after: tt.after, ends: tt.ends}
			rs, err := (&Client{clusterFn: f}).PartitionRates(context.Background(), time.Now().Add(-time.Minute), "t")
			if !errors.Is(err, tt.want) {
				t.Fatalf("rates=%+v err=%v; want %v", rs, err, tt.want)
			}
		})
	}
	// An unknown topic is still just absent.
	f := &fakeCluster{after: marker(kerr.UnknownTopicOrPartition), ends: marker(kerr.UnknownTopicOrPartition)}
	if rs, err := (&Client{clusterFn: f}).PartitionRates(context.Background(), time.Now().Add(-time.Minute), "t"); err != nil || len(rs) != 0 {
		t.Fatalf("unknown topic: rates=%+v err=%v; want none, nil", rs, err)
	}
}
