package owners

import (
	"errors"
	"fmt"

	"gopkg.in/yaml.v3"
)

// Recovery actions (X2): the optional `recovery:` block of a cdc pipeline.
//
//	recovery:
//	  max_rows: 250000   # pg_resnapshot_rows refuses a condition matching more source rows
const (
	// DefaultRecoveryMaxRows is pg_resnapshot_rows' cap when the pipeline
	// sets no recovery.max_rows (docs/api.md, X2 recovery actions).
	DefaultRecoveryMaxRows int64 = 100_000
	// MaxRecoveryMaxRows bounds recovery.max_rows: a re-emit of more rows is
	// a whole-table snapshot (pg_snapshot_signal), approved as such.
	MaxRecoveryMaxRows int64 = 10_000_000
)

// RecoverySpec is the raw `recovery:` block. Read the value with
// Pipeline.RecoveryMaxRows.
type RecoverySpec struct {
	// MaxRows is the most source rows one pg_resnapshot_rows step may
	// re-emit; nil = DefaultRecoveryMaxRows.
	MaxRows *RowCount `yaml:"max_rows"`
}

// RowCount is a number of rows written as a YAML integer: yaml.v3 would
// turn 1.5 into 1 without a word (see WholeDays).
type RowCount int64

// UnmarshalYAML accepts only a plain integer scalar.
func (n *RowCount) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!int" {
		return fmt.Errorf("line %d: recovery.max_rows: want a whole number of rows, got %q", node.Line, node.Value)
	}
	var v int64
	if err := node.Decode(&v); err != nil {
		return fmt.Errorf("line %d: recovery.max_rows: want a whole number of rows: %w", node.Line, err)
	}
	*n = RowCount(v)
	return nil
}

// UnmarshalYAML rejects unknown keys, like WatchSpec: a typo such as
// `max_row:` must stop the start, not silently keep the default.
func (s *RecoverySpec) UnmarshalYAML(node *yaml.Node) error {
	if err := checkKeys(node, "recovery", "max_rows"); err != nil {
		return err
	}
	type plain RecoverySpec // no methods, so Decode does not recurse into this one
	return node.Decode((*plain)(s))
}

// RecoveryMaxRows is the pipeline's recovery.max_rows, or
// DefaultRecoveryMaxRows when it sets none.
func (p Pipeline) RecoveryMaxRows() int64 {
	if p.RecoverySpec != nil && p.RecoverySpec.MaxRows != nil {
		return int64(*p.RecoverySpec.MaxRows)
	}
	return DefaultRecoveryMaxRows
}

// validateRecovery refuses a recovery: block on a batch pipeline (no
// recovery action reads one) and a max_rows outside 1..MaxRecoveryMaxRows.
func (p *Pipeline) validateRecovery() error {
	if p.RecoverySpec == nil {
		return nil
	}
	if p.isBatch() {
		return errors.New("recovery is a cdc field: no recovery action runs on a batch pipeline")
	}
	if n := p.RecoverySpec.MaxRows; n != nil && (*n < 1 || int64(*n) > MaxRecoveryMaxRows) {
		return fmt.Errorf("recovery.max_rows %d: want 1 to %d (leave it out for %d)", *n, MaxRecoveryMaxRows, DefaultRecoveryMaxRows)
	}
	return nil
}
