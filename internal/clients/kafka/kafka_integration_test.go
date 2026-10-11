//go:build integration

package kafka

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients/schemareg"
	"piperoom/internal/testenv"
)

// Runs against the dev stack; fails (does not skip) if it is unreachable.

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
