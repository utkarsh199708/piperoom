package kafkatools

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// fakeCluster adds the optional F12a readers on top of the plain fakeKafka.
type fakeCluster struct {
	*fakeKafka
	health    clients.ClusterHealth
	healthErr error
	gotTopics []string
	rates     []clients.PartitionRate
	ratesErr  error
	gotSince  time.Time
	gotRate   []string
}

func (f *fakeCluster) ClusterHealth(_ context.Context, topics ...string) (clients.ClusterHealth, error) {
	f.gotTopics = topics
	return f.health, f.healthErr
}

func (f *fakeCluster) PartitionRates(_ context.Context, since time.Time, topics ...string) ([]clients.PartitionRate, error) {
	f.gotSince, f.gotRate = since, topics
	return f.rates, f.ratesErr
}

func f12Pipeline() context.Context {
	return tools.WithPipeline(context.Background(), owners.Pipeline{
		ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"},
	})
}

func newTool(t *testing.T, k clients.KafkaAdmin, name string) tools.Tool {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Kafka: k}); err != nil {
		t.Fatal(err)
	}
	tool, ok := reg.Get(name)
	if !ok || tool.Kind() != tools.KindRead {
		t.Fatalf("%s: registered=%v", name, ok)
	}
	return tool
}

func diskFullHealth() clients.ClusterHealth {
	return clients.ClusterHealth{
		ClusterID: "abc", Controller: 2,
		Brokers: []clients.BrokerHealth{
			{NodeID: 1, Host: "k1", Port: 9092, LogDirTotalBytes: 400e9, LogDirUsableBytes: 300e9},
			{NodeID: 2, Host: "k2", Port: 9092, IsController: true, LogDirTotalBytes: 400e9, LogDirUsableBytes: 12e9},
		},
		Partitions: []clients.PartitionHealth{
			{Topic: "pg.public.orders", Partition: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}, MinISR: 2},
			{Topic: "pg.public.orders", Partition: 3, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1}, OfflineReplicas: []int32{2}, MinISR: 2},
		},
	}
}

func TestKafkaClusterHealthSummary(t *testing.T) {
	f := &fakeCluster{fakeKafka: &fakeKafka{}, health: diskFullHealth()}
	r, err := newTool(t, f, "kafka_cluster_health").Run(f12Pipeline(), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"cluster abc (2 brokers, controller 2)",
		"broker 2 (k2) log dir 97% used (12.0 GB free of 400.0 GB)",
		"pg.public.orders: 2 partitions, 1 under-replicated, 1 under min ISR (min 2), 1 offline, 0 reassigning",
		"p3 ISR [1] of [1,2], leader 1",
		"NotEnoughReplicas is expected for writes to p3",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("summary missing %q:\n%s", want, r.Summary)
		}
	}
	if strings.Contains(r.Summary, "broker 1") {
		t.Errorf("healthy broker named:\n%s", r.Summary)
	}
	if len(f.gotTopics) != 1 || f.gotTopics[0] != "pg.public.orders" {
		t.Fatalf("topics asked = %v", f.gotTopics)
	}
	if _, ok := r.Data.(clients.ClusterHealth); !ok {
		t.Fatalf("data %T", r.Data)
	}
}

func TestKafkaClusterHealthLogDirsNotPermitted(t *testing.T) {
	h := diskFullHealth()
	for i := range h.Brokers {
		h.Brokers[i].LogDirErr, h.Brokers[i].LogDirTotalBytes, h.Brokers[i].LogDirUsableBytes = "not permitted", -1, -1
	}
	h.Partitions = h.Partitions[:1]
	f := &fakeCluster{fakeKafka: &fakeKafka{}, health: h}
	r, err := newTool(t, f, "kafka_cluster_health").Run(f12Pipeline(), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.Summary, "log dirs: not permitted") || strings.Contains(r.Summary, "NotEnoughReplicas") {
		t.Fatalf("summary:\n%s", r.Summary)
	}
}

func TestKafkaClusterHealthMissingTopicAndErrors(t *testing.T) {
	h := diskFullHealth()
	h.Partitions = nil
	f := &fakeCluster{fakeKafka: &fakeKafka{}, health: h}
	r, err := newTool(t, f, "kafka_cluster_health").Run(f12Pipeline(), []byte(`{}`))
	if err != nil || !strings.Contains(r.Summary, "pg.public.orders: not found on the cluster") {
		t.Fatalf("%v\n%s", err, r.Summary)
	}

	boom := errors.New("broker down")
	f.healthErr = boom
	if _, err := newTool(t, f, "kafka_cluster_health").Run(f12Pipeline(), []byte(`{}`)); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	// Unscoped: no topics to read.
	if _, err := newTool(t, f, "kafka_cluster_health").Run(tools.WithUnscoped(context.Background()), []byte(`{}`)); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("unscoped err = %v", err)
	}
	if _, err := newTool(t, f, "kafka_cluster_health").Run(f12Pipeline(), []byte(`{"x":`)); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("bad json err = %v", err)
	}
	// A client without the optional reader gets no tool at all, and a direct
	// call still refuses.
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Kafka: &fakeKafka{}}); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"kafka_cluster_health", "kafka_partition_rates"} {
		if _, ok := reg.Get(n); ok {
			t.Fatalf("%s registered for a client without the reader", n)
		}
	}
	if _, err := (&clusterHealth{d: Deps{Kafka: &fakeKafka{}}}).Run(f12Pipeline(), []byte(`{}`)); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("plain client err = %v", err)
	}
}

func rateFake(counts ...int64) *fakeCluster {
	f := &fakeCluster{fakeKafka: &fakeKafka{}}
	for i, c := range counts {
		f.rates = append(f.rates, clients.PartitionRate{Topic: "pg.public.orders", Partition: int32(i), End: 1000 + c, OffsetAtStart: 1000, Records: c})
	}
	return f
}

func TestKafkaPartitionRatesSkewVerdict(t *testing.T) {
	tests := []struct {
		name   string
		counts []int64
		want   string
	}{
		{"skewed", []int64{1204, 1310, 1300, 11002, 1250, 1280}, "→ skewed"},
		{"even", []int64{100, 110, 105, 98}, "→ even"},
		{"idle partitions", []int64{0, 0, 0, 500}, "most partitions received nothing"},
		{"no writes", []int64{0, 0}, "no partition received a write"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := rateFake(tt.counts...)
			r, err := newTool(t, f, "kafka_partition_rates").Run(f12Pipeline(), []byte(`{"window":"5m"}`))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(r.Summary, "pg.public.orders last 5m") || !strings.Contains(r.Summary, tt.want) {
				t.Fatalf("summary:\n%s", r.Summary)
			}
			if len(f.gotRate) != 1 || f.gotRate[0] != "pg.public.orders" {
				t.Fatalf("topics = %v", f.gotRate)
			}
			if d := time.Since(f.gotSince); d < 5*time.Minute || d > 5*time.Minute+5*time.Second {
				t.Fatalf("since is %s ago", d)
			}
		})
	}
	f := rateFake(1204, 1310, 1300, 11002, 1250, 1280)
	r, _ := newTool(t, f, "kafka_partition_rates").Run(f12Pipeline(), []byte(`{}`))
	if !strings.Contains(r.Summary, "17346 records (58/s); p3 11002 (63%)") {
		t.Fatalf("summary:\n%s", r.Summary)
	}
}

func TestKafkaPartitionRatesInputAndErrors(t *testing.T) {
	f := rateFake(1)
	tool := newTool(t, f, "kafka_partition_rates")
	for _, in := range []string{`{"window":"30s"}`, `{"window":"2h"}`, `{"window":"soon"}`, `{"table":`} {
		if _, err := tool.Run(f12Pipeline(), []byte(in)); !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("%s: err = %v", in, err)
		}
	}
	// A table of another pipeline is out of scope.
	if _, err := tool.Run(f12Pipeline(), []byte(`{"table":"public.payments"}`)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Errorf("other table: err = %v", err)
	}
	if _, err := tool.Run(tools.WithUnscoped(context.Background()), []byte(`{}`)); !errors.Is(err, tools.ErrBadInput) {
		t.Errorf("unscoped: err = %v", err)
	}
	f.ratesErr = errors.New("timeout")
	if _, err := tool.Run(f12Pipeline(), []byte(`{}`)); !errors.Is(err, f.ratesErr) {
		t.Errorf("client error lost: %v", err)
	}
	f.ratesErr, f.rates = nil, nil
	if _, err := tool.Run(f12Pipeline(), []byte(`{}`)); !errors.Is(err, clients.ErrNotFound) {
		t.Errorf("empty: err = %v", err)
	}
	if _, err := (&partitionRates{d: Deps{Kafka: &fakeKafka{}}}).Run(f12Pipeline(), []byte(`{}`)); !errors.Is(err, clients.ErrNotSupported) {
		t.Errorf("plain client err = %v", err)
	}
}
