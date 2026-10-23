package watch

import (
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// mysqlPipe is the MySQL pipeline "shop" (source_kind: mysql) with a declared
// DLQ "shop-dlq". Its connector pg-shop gets a MySQL config (no slot.name).
func mysqlPipe() pipeSpec {
	return pipeSpec{id: "shop", tables: "[shop.orders]",
		watch: "    source_kind: mysql\n" + dlqWatch("shop", "shop-dlq", "", false)}
}

func (h *harness) setMySQLConfig(pipe string) {
	h.conn.mu.Lock()
	defer h.conn.mu.Unlock()
	h.conn.cfg["pg-"+pipe] = map[string]string{
		"connector.class":       "io.debezium.connector.mysql.MySqlConnector",
		"heartbeat.interval.ms": "10000",
	}
}

const slotOffLine = "watch: slot signals off for this pipeline"

// A MySQL pipeline reads no replication slots and logs one INFO line; its
// connector, lag and DLQ signals work as on Postgres (Q8p).
func TestMySQLPipelineReadsNoSlots(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{mysqlPipe()}, lag: true})
	h.setMySQLConfig("shop")
	h.kafka.set("shop-dlq", 0, 0)
	h.step(0)
	h.steps(3, time.Minute)
	if n := h.src.callCount(); n != 0 {
		t.Fatalf("ReplicationSlots called %d times for a MySQL pipeline", n)
	}
	if n := h.logs.count(slotOffLine); n != 1 {
		t.Fatalf("slot-off INFO lines = %d, want 1\n%s", n, h.logs)
	}
	if h.lag.callCount() == 0 {
		t.Fatal("consumer group lag not read for a MySQL pipeline")
	}

	h.kafka.add("shop-dlq", 5)
	h.breakConnector("shop", "binlog purged")
	h.steps(8, time.Minute)
	if h.phase("shop", owners.SignalConnectorDown) != phaseFiring {
		t.Fatalf("connector_down did not fire on a MySQL pipeline\n%s", h.logs)
	}
	if h.phase("shop", owners.SignalDLQGrowth) != phaseFiring {
		t.Fatalf("dlq_growth did not fire on a MySQL pipeline\n%s", h.logs)
	}
	for _, sig := range []string{owners.SignalSlotInactive, owners.SignalSlotWAL} {
		if h.phase("shop", sig) != phaseOK {
			t.Fatalf("%s left OK on a MySQL pipeline", sig)
		}
	}
	if n := h.src.callCount(); n != 0 {
		t.Fatalf("ReplicationSlots called %d times for a MySQL pipeline", n)
	}
	if n := h.logs.count(slotOffLine); n != 1 {
		t.Fatalf("slot-off INFO lines = %d after many ticks, want 1", n)
	}
	if h.w.Targets() != 1 {
		t.Fatalf("Targets = %d, want the MySQL pipeline watched", h.w.Targets())
	}
}

// In one client set with a Postgres pipeline, the slots are read for the
// Postgres one only: its slot_inactive fires, the MySQL pipeline's never does,
// even when a slot of its name is inactive.
func TestMySQLNextToPostgresKeepsPostgresSlots(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders"}, mysqlPipe()}})
	h.setMySQLConfig("shop")
	h.step(0)
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
	h.src.update("debezium_shop", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(10, time.Minute)
	if h.src.callCount() == 0 {
		t.Fatal("slots not read for the Postgres pipeline")
	}
	if h.phase("orders", owners.SignalSlotInactive) != phaseFiring {
		t.Fatalf("Postgres slot_inactive did not fire\n%s", h.logs)
	}
	if h.phase("shop", owners.SignalSlotInactive) != phaseOK {
		t.Fatal("slot_inactive fired on a MySQL pipeline")
	}
	if n := h.logs.count(slotOffLine); n != 1 {
		t.Fatalf("slot-off INFO lines = %d, want 1 (the MySQL pipeline only)", n)
	}
}

// The slot WAL forecast never samples a MySQL pipeline.
func TestMySQLPipelineHasNoSlotForecastSample(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{mysqlPipe()}})
	h.setMySQLConfig("shop")
	ts := h.w.targets(t.Context())
	if len(ts) != 1 {
		t.Fatalf("targets = %d", len(ts))
	}
	h.w.samples = newFakeSamples()
	// Even an observation that holds a slot row and a remembered slot name
	// gives no slot sample for the MySQL pipeline.
	h.w.pipe("shop").slot = "debezium_shop"
	obs := &observation{set: ts[0].set, slots: []clients.ReplicationSlot{{Name: "debezium_shop", RetainedWALBytes: 1 << 20}}}
	h.w.mu.Lock()
	jobs := h.w.planSamples(h.clk.Now(), ts, map[string]*observation{ts[0].set: obs})
	h.w.mu.Unlock()
	for _, j := range jobs {
		if j.metric == clients.WatchMetricSlotWALBytes {
			t.Fatalf("slot WAL sample planned for a MySQL pipeline: %+v", j)
		}
	}
}
