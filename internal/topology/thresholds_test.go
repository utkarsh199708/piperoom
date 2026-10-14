package topology

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

func loadOwners(t *testing.T, yaml string) *owners.Owners {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	return o
}

const pipelineYAML = `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [c1]
%s
`

// The watcher fires on "retained >= fire"; pipeline_map must warn at the same point.
func TestSourceHealthWarnsAtThreshold(t *testing.T) {
	th := Thresholds{SlotRetainedWarnBytes: 100}
	for _, tc := range []struct {
		retained int64
		want     Health
	}{{99, HealthOK}, {100, HealthWarn}, {101, HealthWarn}} {
		slot := clients.ReplicationSlot{Name: "s", Active: true, RetainedWALBytes: tc.retained}
		if h, _ := sourceHealth(slot, th); h != tc.want {
			t.Errorf("retained %d: health = %v, want %v", tc.retained, h, tc.want)
		}
	}
}

func TestThresholdsForDefaults(t *testing.T) {
	o := loadOwners(t, fmt.Sprintf(pipelineYAML, ""))
	p, _ := o.Pipeline("orders")
	got := ThresholdsFor(DefaultThresholds(), p)
	if got != DefaultThresholds() {
		t.Fatalf("no watch block: ThresholdsFor = %+v, want the defaults %+v", got, DefaultThresholds())
	}
}

func TestThresholdsForUsesSlotWALFire(t *testing.T) {
	o := loadOwners(t, fmt.Sprintf(pipelineYAML, "    watch:\n      signals:\n        slot_wal: {fire: 1MiB, for: 30s}\n"))
	p, _ := o.Pipeline("orders")
	base := DefaultThresholds()
	base.GroupLagWarn = 77 // untouched fields must pass through
	got := ThresholdsFor(base, p)
	if got.SlotRetainedWarnBytes != 1<<20 {
		t.Fatalf("SlotRetainedWarnBytes = %d, want 1 MiB", got.SlotRetainedWarnBytes)
	}
	if got.GroupLagWarn != 77 || got.SinkStaleFail != 30*time.Minute {
		t.Fatalf("other thresholds changed: %+v", got)
	}
	if base.SlotRetainedWarnBytes != 1<<30 {
		t.Fatal("ThresholdsFor modified its argument")
	}

	// Same slot, same retained size: warn under the pipeline's number, not the default.
	slot := clients.ReplicationSlot{Name: "s", Active: true, RetainedWALBytes: 2 << 20}
	if h, _ := sourceHealth(slot, got); h != HealthWarn {
		t.Fatalf("2 MiB retained with fire 1 MiB: health = %v, want warn", h)
	}
	if h, _ := sourceHealth(slot, base); h != HealthOK {
		t.Fatalf("2 MiB retained with default threshold: health = %v, want ok", h)
	}
}
