package owners

import (
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
)

func TestReconcileDefaults(t *testing.T) {
	w := DefaultWatch()
	if w.ReconcileInterval != 15*time.Minute || w.CountTolerance != 0.005 || len(w.ReconcileOff) != 0 {
		t.Fatalf("defaults: interval %s, tolerance %v, off %v", w.ReconcileInterval, w.CountTolerance, w.ReconcileOff)
	}
	for name, s := range map[string]WatchSignal{"count_drift": w.CountDrift, "row_drift": w.RowDrift, "dup_keys": w.DupKeys, "order_skew": w.OrderSkew} {
		if !s.Enabled || s.Action != ActionRoom {
			t.Errorf("%s default = %+v, want enabled, room", name, s)
		}
	}
	if !w.Reconciles("public.orders") {
		t.Error("a table reconciles by default")
	}
	b := batchWatch()
	if b.CountDrift.Enabled || b.RowDrift.Enabled || b.DupKeys.Enabled || b.OrderSkew.Enabled {
		t.Errorf("batch watch has drift signals on: %+v", b)
	}
}

func TestReconcileBlockParsed(t *testing.T) {
	o, err := Load(writeTemp(t, withWatch(`    watch:
      reconcile_interval: 30m
      count_tolerance: 1.5%
      tables:
        analytics.orders: {reconcile: off}
        public.orders: {reconcile: on}
      signals:
        count_drift: {action: log}
        row_drift: {enabled: false}
        dup_keys: {action: room}
        order_skew: {enabled: true, action: log}
`)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	w := p.Watch()
	if w.ReconcileInterval != 30*time.Minute || w.CountTolerance != 0.015 {
		t.Fatalf("interval %s, tolerance %v", w.ReconcileInterval, w.CountTolerance)
	}
	if w.Reconciles("analytics.orders") || !w.Reconciles("public.orders") {
		t.Fatalf("ReconcileOff = %v, want analytics.orders only", w.ReconcileOff)
	}
	if w.CountDrift.Action != ActionLog || !w.CountDrift.Enabled || w.RowDrift.Enabled || w.DupKeys.Action != ActionRoom || w.OrderSkew.Action != ActionLog {
		t.Fatalf("signals: count %+v row %+v dup %+v order %+v", w.CountDrift, w.RowDrift, w.DupKeys, w.OrderSkew)
	}
	// Watch returns a copy: a caller cannot turn a table back on for others.
	w.ReconcileOff[0] = "x"
	if again := p.Watch(); again.Reconciles("analytics.orders") {
		t.Fatal("Watch shares ReconcileOff with its caller")
	}
}

func TestReconcileZeroTolerance(t *testing.T) {
	o, err := Load(writeTemp(t, withWatch("    watch:\n      count_tolerance: 0%\n")))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	if p.Watch().CountTolerance != 0 {
		t.Fatalf("tolerance = %v, want 0", p.Watch().CountTolerance)
	}
}

func TestReconcileBadValues(t *testing.T) {
	tests := []struct {
		name  string
		block string
		want  []string
	}{
		{"bare tolerance", "count_tolerance: 0.5\n", []string{`pipeline "orders"`, "watch.count_tolerance", "percentage"}},
		{"tolerance over 100", "count_tolerance: 150%\n", []string{"watch.count_tolerance", "0% to 100%"}},
		{"negative tolerance", "count_tolerance: -1%\n", []string{"watch.count_tolerance"}},
		{"NaN tolerance", "count_tolerance: NaN%\n", []string{"watch.count_tolerance"}},
		{"interval no unit", "reconcile_interval: 15\n", []string{"watch.reconcile_interval", "unit"}},
		{"interval too short", "reconcile_interval: 30s\n", []string{"watch.reconcile_interval", "at least 1m"}},
		{"table not in pipeline", "tables:\n        public.customers: {reconcile: off}\n", []string{"watch.tables.public.customers", "neither a source table nor a sink table"}},
		{"bad reconcile value", "tables:\n        public.orders: {reconcile: sometimes}\n", []string{"watch.tables.public.orders.reconcile", "off or on"}},
		{"typo in table block", "tables:\n        public.orders: {reconcil: off}\n", []string{"watch.tables[]", `unknown field "reconcil"`}},
		{"drift signal has no for", "signals:\n        count_drift: {for: 5m}\n", []string{"watch.signals.count_drift", `unknown field "for"`}},
		{"drift bad action", "signals:\n        row_drift: {action: page}\n", []string{"watch.signals.row_drift", "action"}},
		{"unknown signal lists drift ones", "signals:\n        count_drfit: {}\n", []string{"unknown signal", events.SignalCountDrift, events.SignalRowDrift, events.SignalDupKeys, events.SignalOrderSkew}},
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
