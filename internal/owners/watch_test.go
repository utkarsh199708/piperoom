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
        long_tx:        {ticks: 4, min_growth: 128KiB, for: 2m, action: log}
        # v2 keys: parsed, ignored
        freshness:      {sla: 30m, clear: 15m, for: 5m, interval: 10m, tz: UTC}
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
	if lt := w.LongTx; !lt.Enabled || lt.Action != ActionLog || lt.For != 2*time.Minute || lt.Ticks != 4 || lt.MinGrowthBytes != 128<<10 {
		t.Fatalf("long_tx: %+v", lt)
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
		{"lag fire is a size", "signals:\n        consumer_lag: {fire: 1k}\n", []string{"watch.signals.consumer_lag", "fire", "whole number of records"}},
		{"lag fire zero", "signals:\n        consumer_lag: {fire: 0}\n", []string{"consumer_lag", "at least 1"}},
		{"lag clear zero", "signals:\n        consumer_lag: {clear: 0}\n", []string{"consumer_lag", "clear", "at least 1"}},
		{"lag clear above fire", "signals:\n        consumer_lag: {fire: 100, clear: 200}\n", []string{"consumer_lag", "above fire"}},
		{"lag clear above default fire", "signals:\n        consumer_lag: {clear: 5000}\n", []string{"consumer_lag", "above fire"}},
		{"lag members_expected not bool", "signals:\n        consumer_lag: {members_expected: maybe}\n", []string{"consumer_lag"}},
		{"lag bad field", "signals:\n        consumer_lag: {eta_sev1: 1h}\n", []string{"consumer_lag", `unknown field "eta_sev1"`}},
		{"flaps one", "signals:\n        group_unstable: {flaps: 1}\n", []string{"watch.signals.group_unstable", "flaps", "from 2 to 11"}},
		{"flaps above ring", "signals:\n        group_unstable: {flaps: 12}\n", []string{"group_unstable", "flaps"}},
		{"unstable bad window", "signals:\n        group_unstable: {window: 15}\n", []string{"group_unstable", "window", "unit"}},
		{"unstable bad field", "signals:\n        group_unstable: {fire: 3}\n", []string{"group_unstable", `unknown field "fire"`}},
		{"offset signal has no for", "signals:\n        offset_below_log_start: {for: 5m}\n", []string{"watch.signals.offset_below_log_start", `unknown field "for"`}},
		{"offset bad action", "signals:\n        offset_below_log_start: {action: page}\n", []string{"offset_below_log_start", "action"}},
		{"crashloop one restart", "signals:\n        connector_down: {crashloop_restarts: 1}\n", []string{"watch.signals.connector_down", "crashloop_restarts", "at least 2"}},
		{"crashloop restarts not a number", "signals:\n        connector_down: {crashloop_restarts: three}\n", []string{"crashloop_restarts"}},
		{"crashloop bad window", "signals:\n        connector_down: {crashloop_window: 0s}\n", []string{"crashloop_window", "greater than zero"}},
		{"crashloop key on another signal", "signals:\n        slot_inactive: {crashloop_restarts: 3}\n", []string{"slot_inactive", `unknown field "crashloop_restarts"`}},
		{"unknown signal lists new ones", "signals:\n        consumer_lagg: {}\n", []string{"unknown signal", "consumer_lag", "group_unstable", "offset_below_log_start"}},
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

func TestWatchConsumerSignalDefaults(t *testing.T) {
	w := DefaultWatch()
	if l := w.ConsumerLag; !l.Enabled || l.Action != ActionRoom || l.For != 10*time.Minute || l.FireLag != 1000 || l.ClearLag != 500 || !l.MembersExpected {
		t.Fatalf("consumer_lag defaults: %+v", l)
	}
	if g := w.GroupUnstable; !g.Enabled || g.Action != ActionRoom || g.For != 5*time.Minute || g.Flaps != 3 || g.Window != 15*time.Minute {
		t.Fatalf("group_unstable defaults: %+v", g)
	}
	if o := w.OffsetBelowLogStart; !o.Enabled || o.Action != ActionRoom || o.For != 0 {
		t.Fatalf("offset_below_log_start defaults: %+v", o)
	}
	if c := w.ConnectorCrashLoop; c.Restarts != 3 || c.Window != 15*time.Minute {
		t.Fatalf("crash-loop defaults: %+v", c)
	}
}

func TestWatchConsumerSignalsParsed(t *testing.T) {
	o, err := Load(writeTemp(t, withWatch(`    watch:
      signals:
        consumer_lag:           {fire: 5000, clear: 100, for: 2m, members_expected: false, action: log}
        group_unstable:         {for: 1m, flaps: 5, window: 30m}
        offset_below_log_start: {enabled: false}
        connector_down:         {for: 1m, crashloop_restarts: 4, crashloop_window: 1h}
`)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	w := p.Watch()
	if l := w.ConsumerLag; l.FireLag != 5000 || l.ClearLag != 100 || l.For != 2*time.Minute || l.MembersExpected || l.Action != ActionLog || !l.Enabled {
		t.Fatalf("consumer_lag: %+v", l)
	}
	if g := w.GroupUnstable; g.Flaps != 5 || g.Window != 30*time.Minute || g.For != time.Minute {
		t.Fatalf("group_unstable: %+v", g)
	}
	if w.OffsetBelowLogStart.Enabled || w.OffsetBelowLogStart.Action != ActionRoom {
		t.Fatalf("offset_below_log_start: %+v", w.OffsetBelowLogStart)
	}
	if w.ConnectorDown.For != time.Minute || w.ConnectorCrashLoop != (CrashLoop{Restarts: 4, Window: time.Hour}) {
		t.Fatalf("connector_down: %+v %+v", w.ConnectorDown, w.ConnectorCrashLoop)
	}
}

// A custom fire with no clear derives clear as half of fire, never 0.
func TestWatchLagClearDerivedFromFire(t *testing.T) {
	for fire, clear := range map[string]int64{"5000": 2500, "1": 1, "3": 1} {
		o, err := Load(writeTemp(t, withWatch("    watch:\n      signals:\n        consumer_lag: {fire: "+fire+"}\n")))
		if err != nil {
			t.Fatalf("fire %s: Load: %v", fire, err)
		}
		p, _ := o.Pipeline("orders")
		if got := p.Watch().ConsumerLag.ClearLag; got != clear {
			t.Errorf("fire %s: clear = %d, want %d", fire, got, clear)
		}
	}
}
