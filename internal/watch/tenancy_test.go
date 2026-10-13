package watch

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// tenancyOwners has three pipelines whose connectors share one name, as two
// teams' clusters can: pa (tenant a) and pb, pc (tenant b).
const tenancyOwners = `
tenants:
  a: {}
  b: {}
pipelines:
  - {id: pa, tenant: a, topic_prefix: pa, source_tables: [public.t], connectors: [cdc], watch: {signals: {slot_wal: {enabled: false}}},
     connections: {source_db_url: {env: A_SRC}, warehouse_db_url: {env: A_WH}, kafka_brokers: {env: A_K}, connect_url: {env: A_C}, schema_registry_url: {env: A_SR}}}
  - {id: pb, tenant: b, topic_prefix: pb, source_tables: [public.t], connectors: [cdc], watch: {signals: {slot_wal: {enabled: false}}},
     connections: {source_db_url: {env: B_SRC}, warehouse_db_url: {env: B_WH}, kafka_brokers: {env: B_K}, connect_url: {env: B_C}, schema_registry_url: {env: B_SR}}}
  - {id: pc, tenant: b, topic_prefix: pc, source_tables: [public.t], connectors: [cdc2], watch: {signals: {slot_wal: {enabled: false}}},
     connections: {source_db_url: {env: B_SRC}, warehouse_db_url: {env: B_WH}, kafka_brokers: {env: B_K}, connect_url: {env: B_C}, schema_registry_url: {env: B_SR}}}
`

// TestPerPipelineClients: each pipeline is checked with its own client set. A
// connector with the same name on two clusters is read on each, and pipelines
// of one set share one slot query per tick.
func TestPerPipelineClients(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(tenancyOwners), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	slot := func(s *fakeSource, name string) {
		s.update(name, func(r *clients.ReplicationSlot) { r.Active, r.WALStatus = true, "reserved" })
	}
	cfg := map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "slot.name": "debezium", "heartbeat.interval.ms": "10000"}

	srcA, srcB := &fakeSource{}, &fakeSource{}
	slot(srcA, "debezium")
	slot(srcB, "debezium")
	connA, connB := newFakeConnect(), newFakeConnect()
	connA.cfg["cdc"], connB.cfg["cdc"], connB.cfg["cdc2"] = cfg, cfg, cfg
	connA.setStatus("cdc", running("cdc"))
	connB.setStatus("cdc", failed("cdc", "boom"))
	connB.setStatus("cdc2", running("cdc2"))

	// The process-wide clients must not be used for any pipeline.
	shared := &fakeSource{}
	sets := map[string]PipelineClients{
		"pa": {Set: "pipeline pa", Source: srcA, Connect: connA},
		"pb": {Set: "b", Source: srcB, Connect: connB},
		"pc": {Set: "b", Source: srcB, Connect: connB},
	}
	clk, rooms, logs := newFakeClock(), newFakeRooms(), &logBuf{}
	w, err := New(Deps{
		Source: shared, Connect: newFakeConnect(), Owners: own, Rooms: rooms,
		ClientsFor: func(id string) PipelineClients { return sets[id] },
		Config:     config.Watch{Mode: config.WatchRooms, Interval: time.Minute, CheckTimeout: 200 * time.Millisecond, Cooldown: time.Hour},
		Clock:      clk,
		Log:        slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Jitter:     func(time.Duration) time.Duration { return 0 },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const ticks = 5
	for range ticks {
		clk.Advance(time.Minute)
		w.tick(context.Background())
	}

	created := rooms.created()
	if len(created) != 1 || created[0].alert.PipelineID != "pb" {
		t.Fatalf("rooms = %+v, want one for pb (its cluster's cdc FAILED)", created)
	}
	ph := func(p string) phase {
		w.mu.Lock()
		defer w.mu.Unlock()
		if ps := w.pipes[p]; ps != nil && ps.sigs[owners.SignalConnectorDown] != nil {
			return ps.sigs[owners.SignalConnectorDown].phase
		}
		return phaseOK
	}
	if ph("pa") != phaseOK || ph("pc") != phaseOK {
		t.Fatalf("pa=%v pc=%v: a connector read on another pipeline's cluster leaked in", ph("pa"), ph("pc"))
	}
	if n := shared.callCount(); n != 0 {
		t.Fatalf("the process-wide source was read %d times", n)
	}
	if a, b := srcA.callCount(), srcB.callCount(); a != ticks || b != ticks {
		t.Fatalf("slot queries: a=%d b=%d, want %d each (one per set per tick)", a, b, ticks)
	}
}

// TestNilSetClientsAreUnknown: a set with no Connect client leaves its
// pipelines unknown and opens nothing; the other set still works.
func TestNilSetClientsAreUnknown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(tenancyOwners), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	connB := newFakeConnect()
	connB.setStatus("cdc", failed("cdc", "boom"))
	connB.setStatus("cdc2", running("cdc2"))
	rooms, clk := newFakeRooms(), newFakeClock()
	w, err := New(Deps{
		Owners: own, Rooms: rooms,
		ClientsFor: func(id string) PipelineClients {
			if id == "pa" {
				return PipelineClients{Set: "a"}
			}
			return PipelineClients{Set: "b", Source: &fakeSource{}, Connect: connB}
		},
		Config: config.Watch{Mode: config.WatchRooms, Interval: time.Minute, CheckTimeout: 200 * time.Millisecond, Cooldown: time.Hour},
		Clock:  clk, Log: slog.New(slog.NewTextHandler(&logBuf{}, nil)), Jitter: func(time.Duration) time.Duration { return 0 },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for range 5 {
		clk.Advance(time.Minute)
		w.tick(context.Background())
	}
	created := rooms.created()
	if len(created) != 1 || created[0].alert.PipelineID != "pb" {
		t.Fatalf("rooms = %+v, want only pb", created)
	}
	w.mu.Lock()
	sig := w.pipes["pa"].sigs[owners.SignalConnectorDown]
	w.mu.Unlock()
	if sig == nil || sig.phase != phaseOK {
		t.Fatalf("pa with no client: phase %+v, want unknown (no fire)", sig)
	}
}

// TestPerSetDLQTopics: two clusters each have a DLQ topic called shared-dlq.
// Each set reads its own cluster once per tick and keeps its own ring, so
// records arriving on cluster A fire pa only, and cluster B's old backlog
// never looks new.
func TestPerSetDLQTopics(t *testing.T) {
	pipe := func(id, tenant, p string) string {
		return "  - id: " + id + "\n    tenant: " + tenant + "\n    topic_prefix: " + id + "\n    source_tables: [public.t]\n    connectors: [pg-" + id + "]\n    consumer_groups: [g-" + id + "]\n" +
			"    connections: {source_db_url: {env: " + p + "_SRC}, warehouse_db_url: {env: " + p + "_WH}, kafka_brokers: {env: " + p + "_K}, connect_url: {env: " + p + "_C}, schema_registry_url: {env: " + p + "_SR}}\n" +
			"    watch:\n      signals:\n        slot_inactive: {enabled: false}\n        slot_wal: {enabled: false}\n" +
			"      dlq_topics:\n        - {topic: shared-dlq, written_by: g-" + id + "}\n"
	}
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte("tenants:\n  a: {}\n  b: {}\npipelines:\n"+pipe("pa", "a", "A")+pipe("pb", "b", "B")), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	connA, connB := newFakeConnect(), newFakeConnect()
	connA.setStatus("pg-pa", running("pg-pa"))
	connB.setStatus("pg-pb", running("pg-pb"))
	kafkaA, kafkaB := newFakeKafka(), newFakeKafka()
	kafkaA.set("shared-dlq", 0, 0)
	kafkaB.set("shared-dlq", 0, 100) // an old backlog on cluster B
	sets := map[string]PipelineClients{
		"pa": {Set: "pipeline pa", Source: &fakeSource{}, Connect: connA, Kafka: kafkaA},
		"pb": {Set: "pipeline pb", Source: &fakeSource{}, Connect: connB, Kafka: kafkaB},
	}
	shared := newFakeKafka()
	clk, rooms, logs := newFakeClock(), newFakeRooms(), &logBuf{}
	w, err := New(Deps{
		Kafka: shared, Owners: own, Rooms: rooms,
		ClientsFor: func(id string) PipelineClients { return sets[id] },
		Config:     config.Watch{Mode: config.WatchRooms, Interval: time.Minute, CheckTimeout: 200 * time.Millisecond, Cooldown: time.Hour},
		Clock:      clk,
		Log:        slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Jitter:     func(time.Duration) time.Duration { return 0 },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tick := func() {
		clk.Advance(time.Minute)
		w.tick(context.Background())
	}
	tick() // baseline on both clusters
	kafkaA.add("shared-dlq", 3)
	for range 7 {
		tick()
	}
	created := rooms.created()
	if len(created) != 1 || created[0].alert.PipelineID != "pa" {
		t.Fatalf("rooms = %d (%v), want one for pa only\n%s", len(created), created, logs)
	}
	if n := len(kafkaA.callLists()); n != 8 {
		t.Fatalf("cluster A read %d times, want once per tick (8)", n)
	}
	if n := len(kafkaB.callLists()); n != 8 {
		t.Fatalf("cluster B read %d times, want once per tick (8)", n)
	}
	if n := len(shared.callLists()); n != 0 {
		t.Fatalf("the process-wide Kafka client was read %d times", n)
	}
	w.mu.Lock()
	_, a := w.dlqState[dlqStateKey("pipeline pa", "shared-dlq")]
	_, b := w.dlqState[dlqStateKey("pipeline pb", "shared-dlq")]
	_, bare := w.dlqState["shared-dlq"]
	w.mu.Unlock()
	if !a || !b || bare {
		t.Fatalf("rings: pa=%v pb=%v bare=%v, want one ring per set", a, b, bare)
	}
}
