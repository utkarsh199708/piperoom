package topology

import (
	"context"
	"fmt"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// A newest sink timestamp ahead of now (clock skew, a TIMESTAMP without time
// zone written in another zone, one bad row) used to clamp to "stale for 0s"
// and show a stale sink as OK (QA sweep H5). A minute of skew still reads as
// fresh; beyond that the age is unknown and the sink warns.
func TestSinkHealthFutureTimestamp(t *testing.T) {
	th := DefaultThresholds()
	now := time.Now()
	if got, facts := sinkHealth(now.Add(30*time.Second), now, th); got != HealthOK || facts["stale_for"] != "0s" {
		t.Fatalf("within skew: %s %v, want ok and stale_for 0s", got, facts)
	}
	got, facts := sinkHealth(now.Add(3*time.Hour), now, th)
	if got != HealthWarn || facts["future_timestamp"] != "3.0h ahead" || facts["stale_for"] != "unknown" {
		t.Fatalf("3h ahead: %s %v, want warn, future_timestamp 3.0h ahead, stale_for unknown", got, facts)
	}
}

// A sink table or timestamp column that does not exist is a failure the room
// must see, not "unreachable" (QA sweep): the warehouse client reports it as
// clients.ErrNotFound.
func TestSinkNodeMissingTableFails(t *testing.T) {
	b := NewBuilder(
		testOwners(),
		&fakeConnect{},
		&fakeKafka{},
		&fakeSource{},
		&fakeWarehouse{freshErr: map[string]error{"analytics.orders": fmt.Errorf("warehouse: freshness of analytics.orders: %w", clients.ErrNotFound)}},
	)
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range g.Nodes {
		if n.Kind == KindSinkTable {
			if n.Health != HealthFail || n.Facts["missing"] != "true" || n.Facts["ts_column"] != "updated_at" {
				t.Fatalf("sink node = %+v, want fail with missing=true and the ts column", n)
			}
			return
		}
	}
	t.Fatalf("no sink node in %+v", g.Nodes)
}

// A Postgres 'infinity' newest timestamp (the warehouse client's year-9999
// stand-in) is named as such, not as a saturated "2562047.8h ahead" (#476).
func TestSinkHealthInfinity(t *testing.T) {
	inf := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	got, facts := sinkHealth(inf, time.Now(), DefaultThresholds())
	if got != HealthWarn || facts["future_timestamp"] != "infinity (a sentinel or bad value)" || facts["stale_for"] != "unknown" {
		t.Fatalf("infinity: %s %v, want warn, future_timestamp infinity, stale_for unknown", got, facts)
	}
}
