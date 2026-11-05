package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"piperoom/internal/clients"
)

// TestPartitionRatesUnsupported: a broker that cannot look offsets up by time
// (old message format, old request version) answers clients.ErrNotSupported,
// with the broker error still in the chain, so kafka_throughput can fall back
// to lag alone (X6a TC3). Any other error is not ErrNotSupported.
func TestPartitionRatesUnsupported(t *testing.T) {
	ok := listedOffs("t", map[int32]int64{0: 10})
	partErr := func(err error) kadm.ListedOffsets {
		return kadm.ListedOffsets{"t": {0: {Topic: "t", Partition: 0, Err: err}}}
	}
	for _, tt := range []struct {
		name        string
		err         error
		unsupported bool
	}{
		{"old message format", kerr.UnsupportedForMessageFormat, true},
		{"old request version", kerr.UnsupportedVersion, true},
		{"leader election", kerr.LeaderNotAvailable, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeCluster{after: partErr(tt.err), ends: ok}
			_, err := (&Client{clusterFn: f}).PartitionRates(context.Background(), time.Now().Add(-time.Minute), "t")
			if !errors.Is(err, tt.err) {
				t.Fatalf("err = %v; want the broker error %v in the chain", err, tt.err)
			}
			if got := errors.Is(err, clients.ErrNotSupported); got != tt.unsupported {
				t.Fatalf("errors.Is(err, ErrNotSupported) = %v, want %v (err %v)", got, tt.unsupported, err)
			}
		})
	}
}
