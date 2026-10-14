package owners

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// withWatch returns validYAML with the given text as the orders pipeline's
// `watch:` block (already indented under the pipeline).
func withWatch(block string) string {
	return strings.Replace(validYAML, "    sink_ts_column: updated_at\n", "    sink_ts_column: updated_at\n"+block, 1)
}

func TestWatchDefaultsWhenAbsent(t *testing.T) {
	o, err := Load(writeTemp(t, validYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	w := p.Watch()
	want := DefaultWatch()
	if !reflect.DeepEqual(w, want) {
		t.Fatalf("Watch() = %+v, want defaults %+v", w, want)
	}
	if !w.Enabled || w.Slot != "" || w.WALHeadroomBytes != 0 {
		t.Fatalf("bad base defaults: %+v", w)
	}
	if w.ConnectorDown.For != 3*time.Minute || w.SlotInactive.For != 5*time.Minute || w.SlotWAL.For != 10*time.Minute {
		t.Fatalf("bad default for: %+v", w)
	}
	if w.SlotWAL.FireBytes != 1<<30 || w.SlotWAL.ClearBytes != 768<<20 || w.SlotWAL.EtaSev1 != 6*time.Hour {
		t.Fatalf("bad default slot_wal: %+v", w.SlotWAL)
	}
	for _, a := range []string{w.ConnectorDown.Action, w.SlotInactive.Action, w.SlotWAL.Action} {
		if a != ActionRoom {
			t.Fatalf("default action = %q, want room", a)
		}
	}
	// A pipeline built in code (no Load) also gets defaults.
	if !reflect.DeepEqual((Pipeline{ID: "x"}).Watch(), want) {
		t.Fatal("zero Pipeline should return defaults")
	}
}

func TestWatchFullBlock(t *testing.T) {
	o, err := Load(writeTemp(t, withWatch(`    watch:
      enabled: true
      slot: debezium_orders
      wal_headroom_bytes: 50GiB
      signals:
        connector_down: {for: 3m, action: log}
        slot_inactive:  {for: 90s}
        slot_wal:       {fire: 2GiB, clear: 1.5GiB, for: 15m, eta_sev1: 12h}
        # v2 keys: parsed, ignored
        freshness:      {sla: 30m, clear: 15m, for: 5m, interval: 10m, tz: UTC}
        consumer_lag:   {fire: 1000, clear: 500, for: 10m, members_expected: true}
        long_tx:        {age: 10m, for: 5m}
        compat_none:    {action: log}
        unreachable:    {for: 5m, action: log}
`)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	w := p.Watch()
	if !w.Enabled || w.Slot != "debezium_orders" || w.WALHeadroomBytes != 50<<30 {
		t.Fatalf("base: %+v", w)
	}
	if w.ConnectorDown.Action != ActionLog || w.ConnectorDown.For != 3*time.Minute {
		t.Fatalf("connector_down: %+v", w.ConnectorDown)
	}
	if w.SlotInactive.Action != ActionRoom || w.SlotInactive.For != 90*time.Second {
		t.Fatalf("slot_inactive: %+v", w.SlotInactive)
	}
	s := w.SlotWAL
	if s.FireBytes != 2<<30 || s.ClearBytes != 3<<29 || s.For != 15*time.Minute || s.EtaSev1 != 12*time.Hour {
		t.Fatalf("slot_wal: %+v", s)
	}
}

func TestWatchDisabled(t *testing.T) {
	o, err := Load(writeTemp(t, withWatch(`    watch:
      enabled: false
      signals:
        slot_wal: {enabled: false}
        slot_inactive: {enabled: true}
`)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	w := p.Watch()
	if w.Enabled {
		t.Fatal("enabled: false was ignored")
	}
	if w.SlotWAL.Enabled || !w.SlotInactive.Enabled || !w.ConnectorDown.Enabled {
		t.Fatalf("per-signal enabled wrong: %+v", w)
	}
	// Turning a signal off keeps its other defaults.
	if w.SlotWAL.FireBytes != 1<<30 {
		t.Fatalf("slot_wal lost defaults: %+v", w.SlotWAL)
	}
}

func TestWatchClearDerivedFromFire(t *testing.T) {
	o, err := Load(writeTemp(t, withWatch(`    watch:
      signals:
        slot_wal: {fire: 1MiB, for: 30s}
`)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	s := p.Watch().SlotWAL
	if s.FireBytes != 1<<20 || s.ClearBytes != 768<<10 {
		t.Fatalf("fire/clear = %d/%d, want 1MiB/768KiB", s.FireBytes, s.ClearBytes)
	}
}

// Only `clear` set: fire stays at its default, clear must sit at or below it.
func TestWatchClearOnly(t *testing.T) {
	o, err := Load(writeTemp(t, withWatch(`    watch:
      signals:
        slot_wal: {clear: 512MiB}
`)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	s := p.Watch().SlotWAL
	if s.FireBytes != 1<<30 || s.ClearBytes != 512<<20 {
		t.Fatalf("fire/clear = %d/%d, want 1GiB/512MiB", s.FireBytes, s.ClearBytes)
	}
}

// An empty `watch: {}` block is the same as no block.
func TestWatchEmptyBlock(t *testing.T) {
	for _, block := range []string{"    watch: {}\n", "    watch:\n      signals: {}\n", "    watch:\n"} {
		o, err := Load(writeTemp(t, withWatch(block)))
		if err != nil {
			t.Fatalf("Load(%q): %v", block, err)
		}
		p, _ := o.Pipeline("orders")
		if !reflect.DeepEqual(p.Watch(), DefaultWatch()) {
			t.Fatalf("%q: Watch() = %+v, want defaults", block, p.Watch())
		}
	}
}

func TestWatchBadValues(t *testing.T) {
	tests := []struct {
		name  string
		block string
		want  []string // substrings the error must contain
	}{
		{"bad size unit", "signals:\n        slot_wal: {fire: 1GB}\n", []string{`pipeline "orders"`, "watch.signals.slot_wal", "fire", `unknown unit "GB"`, "GiB"}},
		{"size no number", "signals:\n        slot_wal: {fire: GiB}\n", []string{"watch.signals.slot_wal", "fire"}},
		{"negative size", "signals:\n        slot_wal: {fire: -5MiB}\n", []string{"fire"}},
		{"zero size", "signals:\n        slot_wal: {fire: 0}\n", []string{"fire", "greater than zero"}},
		{"bad headroom unit", "wal_headroom_bytes: 50TB\n", []string{"watch.wal_headroom_bytes", "unknown unit"}},
		{"bad duration unit", "signals:\n        connector_down: {for: 3 minutes}\n", []string{"watch.signals.connector_down", "for", "duration"}},
		{"bare duration number", "signals:\n        slot_inactive: {for: 300}\n", []string{"watch.signals.slot_inactive", "for", "unit"}},
		{"zero duration", "signals:\n        slot_wal: {for: 0s}\n", []string{"for", "greater than zero"}},
		{"bad eta", "signals:\n        slot_wal: {eta_sev1: soon}\n", []string{"eta_sev1"}},
		{"bad action", "signals:\n        slot_inactive: {action: page}\n", []string{"watch.signals.slot_inactive", "action", "log or room"}},
		{"clear above fire", "signals:\n        slot_wal: {fire: 1GiB, clear: 2GiB}\n", []string{"clear", "above fire"}},
		{"unknown signal", "signals:\n        slot_wall: {fire: 1GiB}\n", []string{"watch.signals.slot_wall", "unknown signal"}},
		{"bad slot name", "slot: Debezium-Orders\n", []string{"watch.slot"}},
		{"slot name over 63 bytes", "slot: " + strings.Repeat("a", 64) + "\n", []string{"watch.slot"}},
		{"bad field in slot_wal", "signals:\n        slot_wal: {fier: 2GiB}\n", []string{"watch.signals.slot_wal", `unknown field "fier"`, "fire"}},
		{"slot_wal field on another signal", "signals:\n        connector_down: {fire: 2GiB}\n", []string{"watch.signals.connector_down", `unknown field "fire"`}},
		{"bad field in slot_inactive", "signals:\n        slot_inactive: {four: 5m}\n", []string{"watch.signals.slot_inactive", `unknown field "four"`}},
		{"bad field in watch block", "wal_headroom: 50GiB\n", []string{"watch", `unknown field "wal_headroom"`, "wal_headroom_bytes"}},
		{"fire too small to derive clear", "signals:\n        slot_wal: {fire: 3 bytes}\n", []string{"slot_wal", "clear"}},
		{"clear only, above default fire", "signals:\n        slot_wal: {clear: 2GiB}\n", []string{"clear", "above fire"}},
		{"fractional bytes", "wal_headroom_bytes: 0.5 bytes\n", []string{"whole number of bytes"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, withWatch("    watch:\n      "+tc.block)))
			if err == nil {
				t.Fatal("want an error")
			}
			for _, sub := range tc.want {
				if !strings.Contains(err.Error(), sub) {
					t.Errorf("error %q does not contain %q", err, sub)
				}
			}
		})
	}
}

func TestParseSize(t *testing.T) {
	tests := []struct {
		in   string
		want int64
		bad  bool
	}{
		{"4096", 4096, false},
		{"4096 bytes", 4096, false},
		{"1 byte", 1, false},
		{"10B", 10, false},
		{"64KiB", 64 << 10, false},
		{"512MiB", 512 << 20, false},
		{"1GiB", 1 << 30, false},
		{"1.5GiB", 3 << 29, false},
		{"2 gib", 2 << 30, false},
		{" 7MiB ", 7 << 20, false},
		{"1GB", 0, true},
		{"1MB", 0, true},
		{"1TiB", 1 << 40, false},
		{"0.5 bytes", 0, true},
		{"1.0000000001KiB", 0, true},
		{"9007199254740993", 9007199254740993, false}, // 2^53+1: a float64 would round it
		{"8191PiB", 0, true},
		{"9223372036854775807", 1<<63 - 1, false},
		{"9223372036854775808", 0, true},
		{"8388608TiB", 0, true}, // 2^63 bytes
		{"", 0, true},
		{"GiB", 0, true},
		{"-1MiB", 0, true},
		{"99999999999999999999GiB", 0, true},
	}
	for _, tc := range tests {
		got, err := ParseSize(tc.in)
		if tc.bad {
			if err == nil {
				t.Errorf("ParseSize(%q) = %d, want error", tc.in, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("ParseSize(%q) = %d, %v; want %d", tc.in, got, err, tc.want)
		}
	}
}

func TestParseDuration(t *testing.T) {
	for in, want := range map[string]time.Duration{"30s": 30 * time.Second, "3m": 3 * time.Minute, "6h": 6 * time.Hour, "1h30m": 90 * time.Minute} {
		if got, err := ParseDuration(in); err != nil || got != want {
			t.Errorf("ParseDuration(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "5", "3 minutes", "-1m", "0s", "1d"} {
		if got, err := ParseDuration(in); err == nil {
			t.Errorf("ParseDuration(%q) = %v, want error", in, got)
		}
	}
}
