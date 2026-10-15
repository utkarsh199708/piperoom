package owners

import (
	"strings"
	"testing"
	"time"
)

func TestWatchSlotStall(t *testing.T) {
	load := func(t *testing.T, signals string) (WatchConfig, error) {
		t.Helper()
		o, err := Load(writeTemp(t, withWatch("    watch:\n      signals:\n"+signals)))
		if err != nil {
			return WatchConfig{}, err
		}
		p, _ := o.Pipeline("orders")
		return p.Watch(), nil
	}
	t.Run("tuned", func(t *testing.T) {
		w, err := load(t, "        slot_stall: {stall_after: 10m, fire: 8MiB, growth: 256KiB, action: log}\n")
		if err != nil {
			t.Fatal(err)
		}
		s := w.SlotStall
		if s.StallAfter != 10*time.Minute || s.FireBytes != 8<<20 || s.GrowthBytes != 256<<10 || s.Action != ActionLog || !s.Enabled {
			t.Fatalf("slot_stall: %+v", s)
		}
	})
	t.Run("follows slot_wal", func(t *testing.T) {
		w, err := load(t, "        slot_wal: {enabled: false, action: log}\n        slot_stall: {stall_after: 1h}\n")
		if err != nil {
			t.Fatal(err)
		}
		if w.SlotStall.Enabled || w.SlotStall.Action != ActionLog || w.SlotStall.StallAfter != time.Hour || w.SlotStall.FireBytes != 64<<20 {
			t.Fatalf("slot_stall: %+v", w.SlotStall)
		}
	})
	t.Run("own enabled", func(t *testing.T) {
		w, err := load(t, "        slot_wal: {enabled: false}\n        slot_stall: {enabled: true}\n")
		if err != nil {
			t.Fatal(err)
		}
		if !w.SlotStall.Enabled || w.SlotWAL.Enabled {
			t.Fatalf("slot_stall %+v, slot_wal %+v", w.SlotStall, w.SlotWAL)
		}
	})
	for _, tc := range []struct{ name, yaml, want string }{
		{"for is not a key", "        slot_stall: {for: 5m}\n", `unknown field "for"`},
		{"bad stall_after", "        slot_stall: {stall_after: soon}\n", "stall_after"},
		{"zero fire", "        slot_stall: {fire: 0}\n", "greater than zero"},
		{"bad growth unit", "        slot_stall: {growth: 1GB}\n", "growth"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t, tc.yaml)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}
