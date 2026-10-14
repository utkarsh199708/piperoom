package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

type fakeCluster struct {
	md       kadm.Metadata
	mdErr    error
	cfgs     kadm.ResourceConfigs
	logs     kadm.DescribedAllLogDirs
	logsErr  error
	reass    kadm.ListPartitionReassignmentsResponses
	after    kadm.ListedOffsets
	ends     kadm.ListedOffsets
	afterErr error
	calls    []string
}

func (f *fakeCluster) Metadata(context.Context, ...string) (kadm.Metadata, error) {
	return f.md, f.mdErr
}
func (f *fakeCluster) DescribeTopicConfigs(context.Context, ...string) (kadm.ResourceConfigs, error) {
	return f.cfgs, nil
}
func (f *fakeCluster) DescribeAllLogDirs(context.Context, kadm.TopicsSet) (kadm.DescribedAllLogDirs, error) {
	return f.logs, f.logsErr
}
func (f *fakeCluster) ListPartitionReassignments(context.Context, kadm.TopicsSet) (kadm.ListPartitionReassignmentsResponses, error) {
	return f.reass, nil
}
func (f *fakeCluster) ListOffsetsAfterMilli(context.Context, int64, ...string) (kadm.ListedOffsets, error) {
	return f.after, f.afterErr
}
func (f *fakeCluster) ListEndOffsets(context.Context, ...string) (kadm.ListedOffsets, error) {
	return f.ends, nil
}

func str(s string) *string { return &s }

func healthFake() *fakeCluster {
	return &fakeCluster{
		md: kadm.Metadata{
			Cluster: "abc", Controller: 2,
			Brokers: kadm.BrokerDetails{{NodeID: 2, Host: "k2", Port: 9092}, {NodeID: 1, Host: "k1", Port: 9092}},
			Topics: kadm.TopicDetails{
				"pg.public.orders": {Topic: "pg.public.orders", Partitions: kadm.PartitionDetails{
					0: {Partition: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}},
					3: {Partition: 3, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1}, OfflineReplicas: []int32{2}},
				}},
				"gone": {Topic: "gone", Err: kerr.UnknownTopicOrPartition},
			},
		},
		cfgs: kadm.ResourceConfigs{{Name: "pg.public.orders", Configs: []kadm.Config{{Key: "min.insync.replicas", Value: str("2")}}}},
		logs: kadm.DescribedAllLogDirs{
			1: {"/d": {Broker: 1, Dir: "/d", TotalBytes: 400, UsableBytes: 300}},
			2: {"/d": {Broker: 2, Dir: "/d", TotalBytes: 400, UsableBytes: 8}},
		},
		reass: kadm.ListPartitionReassignmentsResponses{"pg.public.orders": {3: {AddingReplicas: []int32{3}}}},
	}
}

func TestClusterHealthUnderReplicated(t *testing.T) {
	c := &Client{clusterFn: healthFake()}
	h, err := c.ClusterHealth(context.Background(), "pg.public.orders", "gone")
	if err != nil {
		t.Fatal(err)
	}
	if h.ClusterID != "abc" || h.Controller != 2 || len(h.Brokers) != 2 || h.Brokers[0].NodeID != 1 || !h.Brokers[1].IsController {
		t.Fatalf("brokers/cluster: %+v", h)
	}
	if h.Brokers[1].LogDirTotalBytes != 400 || h.Brokers[1].LogDirUsableBytes != 8 {
		t.Fatalf("log dirs: %+v", h.Brokers[1])
	}
	if len(h.Partitions) != 2 {
		t.Fatalf("unknown topic must be left out: %+v", h.Partitions)
	}
	p3 := h.Partitions[1]
	if p3.Partition != 3 || len(p3.ISR) != 1 || p3.MinISR != 2 || !p3.Reassigning || len(p3.OfflineReplicas) != 1 {
		t.Fatalf("p3: %+v", p3)
	}
	if h.Partitions[0].Reassigning || h.Partitions[0].OfflineReplicas == nil {
		t.Fatalf("p0: %+v", h.Partitions[0])
	}
}

func TestClusterHealthLogDirsBestEffort(t *testing.T) {
	f := healthFake()
	f.logs, f.logsErr = nil, &kadm.AuthError{Err: kerr.ClusterAuthorizationFailed}
	c := &Client{clusterFn: f}
	h, err := c.ClusterHealth(context.Background(), "pg.public.orders")
	if err != nil {
		t.Fatalf("log dir denial must not fail the read: %v", err)
	}
	for _, b := range h.Brokers {
		if b.LogDirErr != LogDirNotPermitted || b.LogDirTotalBytes != -1 {
			t.Fatalf("broker %d: %+v", b.NodeID, b)
		}
	}
	if len(h.Partitions) != 2 {
		t.Fatal("partitions lost")
	}
}

func TestClusterHealthUnreportedSizes(t *testing.T) {
	f := healthFake()
	f.logs = kadm.DescribedAllLogDirs{1: {"/d": {Broker: 1, Dir: "/d", TotalBytes: -1, UsableBytes: -1}}}
	h, err := (&Client{clusterFn: f}).ClusterHealth(context.Background(), "pg.public.orders")
	if err != nil {
		t.Fatal(err)
	}
	if h.Brokers[0].LogDirTotalBytes != -1 || h.Brokers[0].LogDirErr != "" {
		t.Fatalf("%+v", h.Brokers[0])
	}
	if h.Brokers[1].LogDirErr == "" {
		t.Fatal("broker without log dirs must say so")
	}
}

func TestClusterHealthMetadataError(t *testing.T) {
	f := healthFake()
	f.mdErr = errors.New("boom")
	if _, err := (&Client{clusterFn: f}).ClusterHealth(context.Background(), "x"); !errors.Is(err, f.mdErr) {
		t.Fatalf("err = %v", err)
	}
	f = healthFake()
	f.md.Topics["bad"] = kadm.TopicDetail{Topic: "bad", Err: kerr.TopicAuthorizationFailed}
	if _, err := (&Client{clusterFn: f}).ClusterHealth(context.Background(), "bad"); !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Fatalf("err = %v", err)
	}
}

func listedOffs(t string, m map[int32]int64) kadm.ListedOffsets {
	out := kadm.ListedOffsets{t: {}}
	for p, o := range m {
		out[t][p] = kadm.ListedOffset{Topic: t, Partition: p, Offset: o}
	}
	return out
}

func TestPartitionRates(t *testing.T) {
	f := &fakeCluster{
		after: listedOffs("t", map[int32]int64{0: 100, 1: 500, 2: -1}),
		ends:  listedOffs("t", map[int32]int64{0: 160, 1: 500, 2: 70}),
	}
	rs, err := (&Client{clusterFn: f}).PartitionRates(context.Background(), time.Unix(1000, 0), "t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 3 || rs[0].Records != 60 || rs[1].Records != 0 || rs[1].OffsetAtStart != 500 || rs[2].OffsetAtStart != -1 || rs[2].Records != 0 {
		t.Fatalf("%+v", rs)
	}
}

func TestPartitionRatesErrors(t *testing.T) {
	f := &fakeCluster{afterErr: errors.New("down")}
	if _, err := (&Client{clusterFn: f}).PartitionRates(context.Background(), time.Now(), "t"); !errors.Is(err, f.afterErr) {
		t.Fatalf("err = %v", err)
	}
	f = &fakeCluster{after: listedOffs("t", nil), ends: kadm.ListedOffsets{"t": {0: {Partition: 0, Err: kerr.NotLeaderForPartition}}}}
	if _, err := (&Client{clusterFn: f}).PartitionRates(context.Background(), time.Now(), "t"); !errors.Is(err, kerr.NotLeaderForPartition) {
		t.Fatalf("err = %v", err)
	}
	if rs, err := (&Client{clusterFn: f}).PartitionRates(context.Background(), time.Now()); err != nil || rs != nil {
		t.Fatalf("no topics: %v %v", rs, err)
	}
}
