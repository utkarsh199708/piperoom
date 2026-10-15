//go:build integration

package kafka

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClusterHealthIntegration(t *testing.T) {
	c := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h, err := c.ClusterHealth(ctx, "pg.public.orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Brokers) == 0 || len(h.Partitions) == 0 {
		t.Fatalf("brokers=%d partitions=%d", len(h.Brokers), len(h.Partitions))
	}
	for _, p := range h.Partitions {
		if len(p.ISR) == 0 || p.Leader < 0 {
			t.Errorf("partition %d unhealthy on the dev stack: %+v", p.Partition, p)
		}
	}
}

func TestPartitionRatesIntegration(t *testing.T) {
	c := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rs, err := c.PartitionRates(ctx, time.Now().Add(-time.Hour), "pg.public.orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) == 0 {
		t.Fatal("no partitions")
	}
	for _, r := range rs {
		if r.Records < 0 || r.Records > r.End {
			t.Errorf("implausible rate %+v", r)
		}
	}
}

func TestACLsIntegration(t *testing.T) {
	c := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	es, err := c.ACLs(ctx, []string{"pg.public.orders"}, nil)
	// The dev broker runs without an authorizer: a clean typed answer, or no entries.
	if err != nil && !errors.Is(err, ErrNoAuthorizer) && !errors.Is(err, ErrACLDescribeDenied) {
		t.Fatal(err)
	}
	t.Logf("acls: err=%v entries=%d", err, len(es))
}
