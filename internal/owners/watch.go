package owners

import (
	"fmt"
	"math/big"
	"regexp"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Watch actions: what a firing signal does.
const (
	// ActionRoom opens (or joins) a room when the watcher runs in mode "rooms".
	ActionRoom = "room"
	// ActionLog only writes a log line.
	ActionLog = "log"
)

// Signal names. The first three are active in v1; the rest are accepted in the
// file so one owners.yaml works across versions, but nothing reads them yet.
const (
	SignalConnectorDown = "connector_down"
	SignalSlotInactive  = "slot_inactive"
	SignalSlotWAL       = "slot_wal"
)

var v2Signals = []string{"freshness", "consumer_lag", "long_tx", "compat_none", "unreachable"}

// WatchSpec is the raw `watch:` block of a pipeline, as written in YAML. Use
// Pipeline.Watch for the validated values with defaults applied.
type WatchSpec struct {
	Enabled *bool `yaml:"enabled"`
	// Slot is the replication slot to watch; empty = the slot.name in the
	// connector's config.
	Slot string `yaml:"slot"`
	// WALHeadroomBytes is the free space WAL may use ("50GiB"); it turns on the
	// "disk full in N h" estimate. Kept as text so errors can name the field.
	WALHeadroomBytes string `yaml:"wal_headroom_bytes"`
	// Signals maps a signal name to its settings. Decoded in validate so v2
	// signals can be accepted and ignored.
	Signals map[string]yaml.Node `yaml:"signals"`
}

// UnmarshalYAML rejects unknown keys in the watch block: yaml.Unmarshal ignores
// them, so a typo such as `wal_headroom:` would silently keep the default.
func (s *WatchSpec) UnmarshalYAML(node *yaml.Node) error {
	if err := checkKeys(node, "watch", "enabled", "slot", "wal_headroom_bytes", "signals"); err != nil {
		return err
	}
	type plain WatchSpec // no methods, so Decode does not recurse into this one
	return node.Decode((*plain)(s))
}

// checkKeys fails when mapping node has a key outside allowed. A non-mapping
// node is left for the normal decoder to report.
func checkKeys(node *yaml.Node, where string, allowed ...string) error {
	if node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		k := node.Content[i]
		if !slices.Contains(allowed, k.Value) {
			return fmt.Errorf("line %d: %s: unknown field %q (allowed: %s)", k.Line, where, k.Value, strings.Join(allowed, ", "))
		}
	}
	return nil
}

// signalSpec is one v1 signal's raw settings. Sizes and durations stay text
// until validate, so an error can name the pipeline and field.
type signalSpec struct {
	Enabled *bool  `yaml:"enabled"`
	Action  string `yaml:"action"`
	For     string `yaml:"for"`
	Fire    string `yaml:"fire"`
	Clear   string `yaml:"clear"`
	EtaSev1 string `yaml:"eta_sev1"`
}

// WatchSignal holds the settings every signal shares.
type WatchSignal struct {
	Enabled bool
	// Action is ActionRoom or ActionLog.
	Action string
	// For is how long the condition must hold before the signal fires.
	For time.Duration
}

// SlotWALSignal is the slot_wal signal.
type SlotWALSignal struct {
	WatchSignal
	// FireBytes: retained WAL at or above this starts the timer.
	FireBytes int64
	// ClearBytes: retained WAL below this (for For) resolves it. <= FireBytes.
	ClearBytes int64
	// EtaSev1: a slot-invalidation estimate shorter than this makes it sev1.
	EtaSev1 time.Duration
}

// WatchConfig is a pipeline's watcher settings with defaults applied.
type WatchConfig struct {
	// Enabled is false when the pipeline opts out of the watcher.
	Enabled bool
	// Slot is the slot to watch; empty = take slot.name from the connector config.
	Slot string
	// WALHeadroomBytes is the operator's free-space figure; 0 = not set.
	WALHeadroomBytes int64

	ConnectorDown WatchSignal
	SlotInactive  WatchSignal
	SlotWAL       SlotWALSignal
}

// DefaultWatch returns the settings used when a pipeline has no `watch:` block.
func DefaultWatch() WatchConfig {
	return WatchConfig{
		Enabled:       true,
		ConnectorDown: WatchSignal{Enabled: true, Action: ActionRoom, For: 3 * time.Minute},
		SlotInactive:  WatchSignal{Enabled: true, Action: ActionRoom, For: 5 * time.Minute},
		SlotWAL: SlotWALSignal{
			WatchSignal: WatchSignal{Enabled: true, Action: ActionRoom, For: 10 * time.Minute},
			FireBytes:   1 << 30,   // 1 GiB, same as topology.DefaultThresholds
			ClearBytes:  768 << 20, // 768 MiB: 75% of fire, so a hovering value does not flap
			EtaSev1:     6 * time.Hour,
		},
	}
}

// Watch returns the validated watcher settings for the pipeline. A pipeline
// with no `watch:` block, or one built in code, gets DefaultWatch.
func (p Pipeline) Watch() WatchConfig {
	if p.watch != nil {
		return *p.watch
	}
	return DefaultWatch()
}

// resolveWatch validates spec and applies defaults. Errors name the field.
func resolveWatch(spec *WatchSpec) (WatchConfig, error) {
	w := DefaultWatch()
	if spec == nil {
		return w, nil
	}
	if spec.Enabled != nil {
		w.Enabled = *spec.Enabled
	}
	if spec.Slot != "" && !identRE.MatchString(spec.Slot) {
		return WatchConfig{}, fmt.Errorf("watch.slot %q: not a valid slot name (lower-case letters, digits, underscore)", spec.Slot)
	}
	w.Slot = spec.Slot
	if spec.WALHeadroomBytes != "" {
		n, err := ParseSize(spec.WALHeadroomBytes)
		if err != nil {
			return WatchConfig{}, fmt.Errorf("watch.wal_headroom_bytes: %w", err)
		}
		if n <= 0 {
			return WatchConfig{}, fmt.Errorf("watch.wal_headroom_bytes %q: must be greater than zero", spec.WALHeadroomBytes)
		}
		w.WALHeadroomBytes = n
	}

	for _, name := range sortedKeys(spec.Signals) {
		node := spec.Signals[name]
		if slices.Contains(v2Signals, name) {
			continue // parsed by the YAML decoder, ignored in v1
		}
		var s signalSpec
		switch name {
		case SignalConnectorDown, SignalSlotInactive, SignalSlotWAL:
		default:
			return WatchConfig{}, fmt.Errorf("watch.signals.%s: unknown signal (known: %s, %s, %s; later: %s)",
				name, SignalConnectorDown, SignalSlotInactive, SignalSlotWAL, strings.Join(v2Signals, ", "))
		}
		// node.Decode ignores unknown keys, so check them first: `{fier: 2GiB}`
		// must not silently keep the default.
		allowed := []string{"enabled", "action", "for"}
		if name == SignalSlotWAL {
			allowed = append(allowed, "fire", "clear", "eta_sev1")
		}
		if err := checkKeys(&node, "watch.signals."+name, allowed...); err != nil {
			return WatchConfig{}, err
		}
		if err := node.Decode(&s); err != nil {
			return WatchConfig{}, fmt.Errorf("watch.signals.%s: %w", name, err)
		}
		var err error
		switch name {
		case SignalConnectorDown:
			err = applySignal(&w.ConnectorDown, s)
		case SignalSlotInactive:
			err = applySignal(&w.SlotInactive, s)
		case SignalSlotWAL:
			err = applySlotWAL(&w.SlotWAL, s)
		}
		if err != nil {
			return WatchConfig{}, fmt.Errorf("watch.signals.%s: %w", name, err)
		}
	}
	return w, nil
}

func applySignal(dst *WatchSignal, s signalSpec) error {
	if s.Enabled != nil {
		dst.Enabled = *s.Enabled
	}
	if s.Action != "" {
		if s.Action != ActionRoom && s.Action != ActionLog {
			return fmt.Errorf("action %q: want %s or %s", s.Action, ActionLog, ActionRoom)
		}
		dst.Action = s.Action
	}
	if s.For != "" {
		d, err := ParseDuration(s.For)
		if err != nil {
			return fmt.Errorf("for: %w", err)
		}
		dst.For = d
	}
	return nil
}

func applySlotWAL(dst *SlotWALSignal, s signalSpec) error {
	if err := applySignal(&dst.WatchSignal, s); err != nil {
		return err
	}
	fireSet := false
	if s.Fire != "" {
		n, err := ParseSize(s.Fire)
		if err != nil {
			return fmt.Errorf("fire: %w", err)
		}
		if n <= 0 {
			return fmt.Errorf("fire %q: must be greater than zero", s.Fire)
		}
		dst.FireBytes = n
		fireSet = true
	}
	if s.Clear != "" {
		n, err := ParseSize(s.Clear)
		if err != nil {
			return fmt.Errorf("clear: %w", err)
		}
		if n <= 0 {
			return fmt.Errorf("clear %q: must be greater than zero", s.Clear)
		}
		dst.ClearBytes = n
	} else if fireSet {
		// A custom fire with no clear: derive clear so it never sits above fire.
		dst.ClearBytes = dst.FireBytes / 4 * 3
		if dst.ClearBytes <= 0 {
			return fmt.Errorf("fire %q is too small to derive clear (75%% rounds to 0): set clear explicitly", s.Fire)
		}
	}
	if dst.ClearBytes > dst.FireBytes {
		return fmt.Errorf("clear (%d bytes) must not be above fire (%d bytes)", dst.ClearBytes, dst.FireBytes)
	}
	if s.EtaSev1 != "" {
		d, err := ParseDuration(s.EtaSev1)
		if err != nil {
			return fmt.Errorf("eta_sev1: %w", err)
		}
		dst.EtaSev1 = d
	}
	return nil
}

func sortedKeys(m map[string]yaml.Node) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

var (
	// identRE is Postgres's rule for a replication slot name: lower-case
	// letters, digits and underscore, at most 63 bytes (NAMEDATALEN - 1).
	identRE = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)
	sizeRE  = regexp.MustCompile(`^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]*)\s*$`)
)

// ParseSize reads a byte size such as "1GiB", "768MiB", "64 KiB", "4096" or
// "4096 bytes". Units are binary (KiB, MiB, GiB, TiB); "GB" and "MB" are
// rejected because they are ambiguous. A bare number is bytes. The result must
// be a whole number of bytes that fits int64.
func ParseSize(s string) (int64, error) {
	m := sizeRE.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("size %q: want a number with a unit, like 1GiB, 512MiB, 64KiB or 4096 bytes", s)
	}
	var mult int64
	switch strings.ToLower(m[2]) {
	case "", "b", "byte", "bytes":
		mult = 1
	case "kib":
		mult = 1 << 10
	case "mib":
		mult = 1 << 20
	case "gib":
		mult = 1 << 30
	case "tib":
		mult = 1 << 40
	default:
		return 0, fmt.Errorf("size %q: unknown unit %q (use bytes, KiB, MiB, GiB or TiB)", s, m[2])
	}
	// Exact arithmetic: a float would round sizes above 2^53 bytes.
	r, ok := new(big.Rat).SetString(m[1])
	if !ok {
		return 0, fmt.Errorf("size %q: not a number", s)
	}
	r.Mul(r, new(big.Rat).SetInt64(mult))
	if !r.IsInt() {
		return 0, fmt.Errorf("size %q: is not a whole number of bytes", s)
	}
	if !r.Num().IsInt64() {
		return 0, fmt.Errorf("size %q: too large", s)
	}
	return r.Num().Int64(), nil
}

// ParseDuration reads a positive duration such as "30s", "3m" or "6h". A bare
// number is an error: the unit must be written.
func ParseDuration(s string) (time.Duration, error) {
	d, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("duration %q: want a number with a unit, like 30s, 3m or 6h", s)
	}
	if d <= 0 {
		return 0, fmt.Errorf("duration %q: must be greater than zero", s)
	}
	return d, nil
}
