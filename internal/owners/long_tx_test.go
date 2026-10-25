package owners

import (
	"strings"
	"testing"
	"time"
)

func TestLongTxDefaults(t *testing.T) {
	lt := DefaultWatch().LongTx
	want := LongTxSignal{WatchSignal: WatchSignal{Enabled: true, Action: ActionRoom, For: 5 * time.Minute}, Ticks: 3, MinGrowthBytes: 64 << 10}
	if lt != want {
		t.Fatalf("DefaultWatch().LongTx = %+v, want %+v", lt, want)
	}
	if !strings.Contains(strings.Join(knownSignals, ","), SignalLongTx) {
		t.Fatal("long_tx is not a known signal")
	}
	for _, s := range v2Signals {
		if s == SignalLongTx {
			t.Fatal("long_tx is still in v2Signals (parsed and ignored)")
		}
	}
}

func TestLongTxParse(t *testing.T) {
	def := DefaultWatch().LongTx
	tests := []struct {
		name  string
		block string
		want  LongTxSignal
	}{
		{"empty block keeps defaults", "{}", def},
		{"off", "{enabled: false}", LongTxSignal{WatchSignal: WatchSignal{Action: ActionRoom, For: 5 * time.Minute}, Ticks: 3, MinGrowthBytes: 64 << 10}},
		{"all keys", "{enabled: true, action: log, for: 90s, ticks: 5, min_growth: 1MiB}",
			LongTxSignal{WatchSignal: WatchSignal{Enabled: true, Action: ActionLog, For: 90 * time.Second}, Ticks: 5, MinGrowthBytes: 1 << 20}},
		{"ticks at the minimum", "{ticks: 2}", LongTxSignal{WatchSignal: def.WatchSignal, Ticks: 2, MinGrowthBytes: def.MinGrowthBytes}},
		{"ticks at the maximum", "{ticks: 100}", LongTxSignal{WatchSignal: def.WatchSignal, Ticks: 100, MinGrowthBytes: def.MinGrowthBytes}},
		{"min_growth in bytes", "{min_growth: 4096}", LongTxSignal{WatchSignal: def.WatchSignal, Ticks: def.Ticks, MinGrowthBytes: 4096}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o, err := Load(writeTemp(t, withWatch("    watch:\n      signals:\n        long_tx: "+tc.block+"\n")))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			p, _ := o.Pipeline("orders")
			if got := p.Watch().LongTx; got != tc.want {
				t.Fatalf("LongTx = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestLongTxBadValues(t *testing.T) {
	tests := []struct {
		name  string
		block string
		want  []string
	}{
		{"typo in ticks", "{tick: 3}", []string{`pipeline "orders"`, "watch.signals.long_tx", `unknown field "tick"`, "ticks", "min_growth"}},
		{"slot_wal key on long_tx", "{fire: 1GiB}", []string{"watch.signals.long_tx", `unknown field "fire"`}},
		{"old age key", "{age: 10m, for: 5m}", []string{"watch.signals.long_tx", "age is not read", "ticks, min_growth and for"}},
		{"one tick", "{ticks: 1}", []string{"watch.signals.long_tx", "ticks", "from 2 to 100"}},
		{"too many ticks", "{ticks: 101}", []string{"ticks", "from 2 to 100"}},
		{"ticks not a number", "{ticks: three}", []string{"ticks", "whole number"}},
		{"ticks fraction", "{ticks: 2.5}", []string{"ticks", "whole number"}},
		{"min_growth bad unit", "{min_growth: 64KB}", []string{"watch.signals.long_tx", "min_growth", `unknown unit "KB"`}},
		{"min_growth zero", "{min_growth: 0}", []string{"min_growth", "greater than zero"}},
		{"min_growth negative", "{min_growth: -1KiB}", []string{"min_growth"}},
		{"bare for", "{for: 300}", []string{"watch.signals.long_tx", "for", "unit"}},
		{"zero for", "{for: 0s}", []string{"long_tx", "for", "greater than zero"}},
		{"bad action", "{action: page}", []string{"watch.signals.long_tx", "action", "log or room"}},
		{"enabled not a bool", "{enabled: maybe}", []string{"watch.signals.long_tx"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, withWatch("    watch:\n      signals:\n        long_tx: "+tc.block+"\n")))
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

// long_tx reads a replication slot, so it is off wherever the slot signals
// are: a non-Postgres source (and the key is refused there), a batch pipeline.
func TestLongTxOffWithoutSlot(t *testing.T) {
	o, err := Load(writeTemp(t, mysqlPipeline("")))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders-mysql")
	if p.Watch().LongTx.Enabled {
		t.Fatal("long_tx is on for a mysql pipeline")
	}
	if _, err := Load(writeTemp(t, mysqlPipeline("    watch: {signals: {long_tx: {enabled: false}}}\n"))); err == nil ||
		!strings.Contains(err.Error(), "watch.signals.long_tx: a mysql source has no replication slot") {
		t.Fatalf("long_tx on mysql: err = %v", err)
	}
	if (Pipeline{ID: "x", Kind: "batch"}).Watch().LongTx.Enabled {
		t.Fatal("long_tx is on for a batch pipeline")
	}
}
