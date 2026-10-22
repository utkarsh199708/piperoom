package owners

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Settings of the X1 drift checks (count_drift, row_drift) in a pipeline's
// `watch:` block (docs/api.md, Watcher alerts):
//
//	watch:
//	  reconcile_interval: 15m
//	  count_tolerance: 0.5%
//	  tables:
//	    public.audit_log: {reconcile: off}

const (
	// DefaultReconcileInterval is how often the drift checks run.
	DefaultReconcileInterval = 15 * time.Minute
	// MinReconcileInterval bounds reconcile_interval: every check counts each
	// table pair on both sides, so it must not run like a health poll.
	MinReconcileInterval = time.Minute
	// DefaultCountTolerance is 0.5 %, as a fraction.
	DefaultCountTolerance = 0.005
)

// WatchTableSpec is one watch.tables entry as written in YAML.
type WatchTableSpec struct {
	// Reconcile is "off" to keep the drift checks off the table, or "on"
	// (the default).
	Reconcile string `yaml:"reconcile"`
}

// UnmarshalYAML rejects unknown keys: a typo such as `reconcil: off` would
// otherwise leave the checks reading a table someone asked them to skip.
func (s *WatchTableSpec) UnmarshalYAML(node *yaml.Node) error {
	if err := checkKeys(node, "watch.tables[]", "reconcile"); err != nil {
		return err
	}
	type plain WatchTableSpec
	return node.Decode((*plain)(s))
}

// percentRE is a non-negative decimal with a percent sign. The sign is
// required: a bare 0.5 could mean 0.5 % or 50 %.
var percentRE = regexp.MustCompile(`^\s*([0-9]+(?:\.[0-9]+)?)\s*%\s*$`)

// applyReconcile reads reconcile_interval and count_tolerance into w.
func applyReconcile(w *WatchConfig, spec *WatchSpec) error {
	if spec.ReconcileInterval != "" {
		d, err := ParseDuration(spec.ReconcileInterval)
		if err != nil {
			return fmt.Errorf("watch.reconcile_interval: %w", err)
		}
		if d < MinReconcileInterval {
			return fmt.Errorf("watch.reconcile_interval %q: must be at least %s (each check counts every table on both sides)", spec.ReconcileInterval, MinReconcileInterval)
		}
		w.ReconcileInterval = d
	}
	if spec.CountTolerance != "" {
		m := percentRE.FindStringSubmatch(spec.CountTolerance)
		if m == nil {
			return fmt.Errorf("watch.count_tolerance %q: want a percentage, like 0.5%%", spec.CountTolerance)
		}
		pct, err := strconv.ParseFloat(m[1], 64)
		if err != nil || pct > 100 {
			return fmt.Errorf("watch.count_tolerance %q: want a percentage from 0%% to 100%%", spec.CountTolerance)
		}
		w.CountTolerance = pct / 100
	}
	return nil
}

// resolveReconcileTables validates watch.tables into w.ReconcileOff. A key
// must be one of the pipeline's source tables or sink tables (after a
// sinks: block is merged), so a typo cannot silently turn nothing off.
func resolveReconcileTables(p Pipeline, w *WatchConfig) error {
	if p.WatchSpec == nil {
		return nil
	}
	for _, table := range sortedTableKeys(p.WatchSpec.Tables) {
		where := "watch.tables." + table
		if !slices.Contains(p.SourceTables, table) && !slices.Contains(p.SinkTables, table) {
			return fmt.Errorf("%s: %q is neither a source table nor a sink table of pipeline %q", where, table, p.ID)
		}
		switch v := p.WatchSpec.Tables[table].Reconcile; v {
		case "off", "false":
			w.ReconcileOff = append(w.ReconcileOff, table)
		case "", "on", "true":
		default:
			return fmt.Errorf("%s.reconcile %q: want off or on", where, v)
		}
	}
	return nil
}

func sortedTableKeys(m map[string]WatchTableSpec) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// Reconciles reports whether the drift checks may read table: false when
// the pipeline set `reconcile: off` on it. A source-to-sink pair is checked
// only when both of its tables reconcile.
func (w WatchConfig) Reconciles(table string) bool {
	return !slices.Contains(w.ReconcileOff, table)
}
