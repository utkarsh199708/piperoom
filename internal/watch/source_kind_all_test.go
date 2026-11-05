package watch

import (
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// kindPipe is the pipeline "shop" with the given source_kind ("" = Postgres).
func kindPipe(kind string) pipeSpec {
	w := dlqWatch("shop", "shop-dlq", "", false)
	if kind != "" {
		w = "    source_kind: " + kind + "\n" + w
	}
	return pipeSpec{id: "shop", tables: "[shop.orders]", watch: w}
}

var sourceKindCases = []struct {
	name   string
	kind   string
	noSlot bool
}{
	{"postgres", "", false},
	{"postgres explicit", owners.SourceKindPostgres, false},
	{"mysql", owners.SourceKindMySQL, true},
	{"sqlserver", owners.SourceKindSQLServer, true},
	{"mongodb", owners.SourceKindMongoDB, true},
	{"oracle", owners.SourceKindOracle, true},
}

// Only a Postgres pipeline is a slot pipeline (KAN-541): the other kinds get
// no slot read (observe.go) and no slot signal.
func TestHasNoSlotByKind(t *testing.T) {
	for _, tc := range sourceKindCases {
		t.Run(tc.name, func(t *testing.T) {
			p := owners.Pipeline{ID: "shop", SourceKind: tc.kind}
			if got := hasNoSlot(p); got != tc.noSlot {
				t.Fatalf("hasNoSlot = %v, want %v", got, tc.noSlot)
			}
		})
	}
	if hasNoSlot(owners.Pipeline{ID: "b", Kind: "batch"}) {
		t.Fatal("a batch pipeline has no source database and is not a no-slot cdc pipeline")
	}
}

// observe.go: slot reads and slot signals per kind.
func TestSlotReadsByKind(t *testing.T) {
	for _, tc := range sourceKindCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, opts{pipes: []pipeSpec{kindPipe(tc.kind)}})
			h.step(0)
			h.steps(2, time.Minute)
			n := h.src.callCount()
			if tc.noSlot && n != 0 {
				t.Fatalf("ReplicationSlots called %d times for source_kind %q", n, tc.kind)
			}
			if !tc.noSlot && n == 0 {
				t.Fatalf("ReplicationSlots never called for a Postgres pipeline")
			}
			for _, ts := range h.w.targets(t.Context()) {
				on := ts.cfg.SlotInactive.Enabled || ts.cfg.SlotWAL.Enabled
				if tc.noSlot && on {
					t.Fatalf("slot signals on for source_kind %q", tc.kind)
				}
			}
		})
	}
}

// changes.go: the live DDL read is skipped when every target has no slot.
func TestAllNoSlotByKind(t *testing.T) {
	for _, tc := range sourceKindCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, opts{pipes: []pipeSpec{kindPipe(tc.kind)}})
			ts := h.w.targets(t.Context())
			if len(ts) != 1 {
				t.Fatalf("targets = %d", len(ts))
			}
			if got := allNoSlot(ts); got != tc.noSlot {
				t.Fatalf("allNoSlot = %v, want %v", got, tc.noSlot)
			}
		})
	}
	if allNoSlot(nil) {
		t.Fatal("allNoSlot(nil) must be false")
	}
}

// forecast.go: the slot WAL sample is planned for Postgres only.
func TestSlotForecastSampleByKind(t *testing.T) {
	for _, tc := range sourceKindCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, opts{pipes: []pipeSpec{kindPipe(tc.kind)}})
			ts := h.w.targets(t.Context())
			if len(ts) != 1 {
				t.Fatalf("targets = %d", len(ts))
			}
			h.w.samples = newFakeSamples()
			h.w.pipe("shop").slot = "debezium_shop"
			obs := &observation{set: ts[0].set, slots: []clients.ReplicationSlot{{Name: "debezium_shop", RetainedWALBytes: 1 << 20}}}
			h.w.mu.Lock()
			jobs := h.w.planSamples(h.clk.Now(), ts, map[string]*observation{ts[0].set: obs})
			h.w.mu.Unlock()
			planned := false
			for _, j := range jobs {
				if j.metric == clients.WatchMetricSlotWALBytes {
					planned = true
				}
			}
			if tc.noSlot && planned {
				t.Fatalf("slot WAL sample planned for source_kind %q", tc.kind)
			}
		})
	}
}
