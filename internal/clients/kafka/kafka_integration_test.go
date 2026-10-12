//go:build integration

package kafka

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/clients/schemareg"
	"piperoom/internal/testenv"
)

// Runs against the dev stack; testenv.Require skips when it is unreachable.

func newClient(t *testing.T) *Client {
	t.Helper()
	cfg := testenv.Require(t)
	sr := schemareg.New(cfg.SchemaRegistryURL, &http.Client{Timeout: 5 * time.Second})
	c, err := New(strings.Split(cfg.KafkaBrokers, ","), sr)
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestTopicsExcludesInternal(t *testing.T) {
	c := newClient(t)
	topics, err := c.Topics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("topics: %v", topics)
	for _, name := range topics {
		if strings.HasPrefix(name, "__") || strings.HasPrefix(name, "_connect_") || name == "_schemas" {
			t.Fatalf("internal topic leaked: %q", name)
		}
	}
	found := false
	for _, name := range topics {
		if name == "pg.public.orders" {
			found = true
		}
	}
	if !found {
		t.Fatalf("pg.public.orders missing from %v", topics)
	}
}

func TestTopicOffsets(t *testing.T) {
	c := newClient(t)
	offs, err := c.TopicOffsets(context.Background(), "pg.public.orders")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("offsets: %+v", offs)
	if len(offs) == 0 {
		t.Fatal("no partitions")
	}
	if offs[0].End <= offs[0].Start {
		t.Fatalf("end<=start: %+v", offs[0])
	}
	if offs[0].LastTimestamp.IsZero() {
		t.Fatalf("missing last timestamp: %+v", offs[0])
	}
}

func TestConsumerGroupsAndLag(t *testing.T) {
	c := newClient(t)
	groups, err := c.ConsumerGroups(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("groups: %v", groups)
	found := false
	for _, g := range groups {
		if g == "order-sync" {
			found = true
		}
	}
	if !found {
		t.Log("order-sync group not present; skipping lag check")
		return
	}
	lag, err := c.GroupLag(context.Background(), "order-sync")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("order-sync: state=%s members=%d totalLag=%d partitions=%d", lag.State, lag.Members, lag.TotalLag, len(lag.Partitions))
	if lag.Group != "order-sync" {
		t.Fatalf("group %q", lag.Group)
	}
}

func TestFindByKeyRealData(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	// After devctl reset, pg.public.orders holds 100 rows with Debezium Avro keys {"id": N}.
	refs, err := c.FindByKey(ctx, "pg.public.orders", "1", time.Now().Add(-24*time.Hour), 5000)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("found %d record(s) for id=1", len(refs))
	if len(refs) == 0 {
		t.Fatal("expected to find id=1 in pg.public.orders")
	}
	if refs[0].Key != "1" {
		t.Fatalf("decoded key %q, want 1", refs[0].Key)
	}
}

// TestFindByKeyAbsentKey proves an absent key returns (nil, nil) after scanning
// the whole window — not a "scan incomplete" deadline error.
func TestFindByKeyAbsentKey(t *testing.T) {
	c := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	refs, err := c.FindByKey(ctx, "pg.public.orders", "99999999", time.Now().Add(-24*time.Hour), 5000)
	if err != nil {
		t.Fatalf("absent key should scan cleanly, got: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("want 0 refs, got %+v", refs)
	}
	t.Logf("absent-key scan finished in %s", time.Since(start))
}

// TestDescribeTopicConfigs reads the real pipeline topic's key configs: what
// the retention scenario needs (retention.ms, cleanup.policy) plus shape.
func TestDescribeTopicConfigs(t *testing.T) {
	c := newClient(t)
	cfg, err := c.DescribeTopicConfigs(context.Background(), "pg.public.orders")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("pg.public.orders: %+v", cfg)
	if cfg.Topic != "pg.public.orders" {
		t.Fatalf("topic = %q", cfg.Topic)
	}
	if cfg.Partitions <= 0 || cfg.ReplicationFactor <= 0 {
		t.Fatalf("bad shape: %+v", cfg)
	}
	if cfg.RetentionMS == "" || cfg.RetentionBytes == "" || cfg.CleanupPolicy == "" || cfg.MinInsyncReplicas == "" {
		t.Fatalf("missing key configs: %+v", cfg)
	}
}

// TestDescribeTopicConfigsUnknownTopic proves an absent topic surfaces as
// clients.ErrNotFound, not a broker failure.
func TestDescribeTopicConfigsUnknownTopic(t *testing.T) {
	c := newClient(t)
	_, err := c.DescribeTopicConfigs(context.Background(), testenv.Name("it-missing"))
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("unknown topic: err = %v, want ErrNotFound", err)
	}
}

// TestTempTopicProduceOffsetsAndLag creates a temp topic, produces records,
// and verifies TopicOffsets, FindByKey and GroupLag (no-commit partition) on it.
func TestTempTopicProduceOffsetsAndLag(t *testing.T) {
	cfg := testenv.Require(t)
	brokers := strings.Split(cfg.KafkaBrokers, ",")
	c := newClient(t)

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	defer producer.Close()
	adm := kadm.NewClient(producer)

	topic := testenv.Name("it-kafka")
	resps, err := adm.CreateTopics(context.Background(), 1, 1, nil, topic)
	if err != nil {
		t.Fatalf("CreateTopics: %v", err)
	}
	if err := resps.Error(); err != nil {
		t.Fatalf("CreateTopics response: %v", err)
	}
	t.Cleanup(func() {
		if _, err := adm.DeleteTopics(context.Background(), topic); err != nil {
			t.Logf("DeleteTopics: %v", err)
		}
	})

	ctx := context.Background()
	rs := producer.ProduceSync(ctx,
		&kgo.Record{Topic: topic, Key: []byte("k1"), Value: []byte("v1")},
		&kgo.Record{Topic: topic, Key: []byte("k2"), Value: []byte("v2")},
		&kgo.Record{Topic: topic, Key: []byte("k3"), Value: []byte("v3")},
	)
	if err := rs.FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	offs, err := c.TopicOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(offs) != 1 || offs[0].Start != 0 || offs[0].End != 3 {
		t.Fatalf("offsets: %+v", offs)
	}
	if offs[0].LastTimestamp.IsZero() {
		t.Fatalf("missing last timestamp: %+v", offs[0])
	}

	refs, err := c.FindByKey(ctx, topic, "k2", time.Now().Add(-time.Hour), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].Key != "k2" {
		t.Fatalf("FindByKey k2: %+v", refs)
	}

	group := testenv.Name("it-group")
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer consumer.Close()

	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for n := 0; n < 3; {
		fetches := consumer.PollFetches(cctx)
		n += fetches.NumRecords()
		if cctx.Err() != nil {
			t.Fatalf("consume: %v", cctx.Err())
		}
	}

	var lag clients.GroupLag
	deadline := time.Now().Add(20 * time.Second)
	for {
		lag, err = c.GroupLag(ctx, group)
		if err == nil && len(lag.Partitions) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("GroupLag never populated: %v (lag %+v)", err, lag)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Logf("group %s: state=%s members=%d totalLag=%d", group, lag.State, lag.Members, lag.TotalLag)
	for _, p := range lag.Partitions {
		if p.Topic == topic && p.Committed != -1 {
			t.Fatalf("expected Committed=-1 (no commit), got %+v", p)
		}
	}
}
