package owners

import (
	"fmt"
	"math/big"
	"regexp"
	"slices"
	"strconv"
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

// Signal names. The first three are the v1 signals; dlq_growth and config_risk
// come from docs/design/dlq.md. The names in v2Signals are accepted in the
// file so one owners.yaml works across versions, but nothing reads them yet.
const (
	SignalConnectorDown = "connector_down"
	SignalSlotInactive  = "slot_inactive"
	SignalSlotWAL       = "slot_wal"
	SignalDLQGrowth     = "dlq_growth"
	SignalConfigRisk    = "config_risk"
)

// dlq_peek header modes (watch.dlq_peek.headers): how much Connect error-header
// text the dlq_peek tool may show per record (docs/design/dlq.md section 3.2).
const (
	// DLQHeadersCountsOnly shows partition, offset and timestamp only.
	DLQHeadersCountsOnly = "counts_only"
	// DLQHeadersClasses (the default) adds the fixed-vocabulary error headers.
	DLQHeadersClasses = "classes"
	// DLQHeadersMessages also shows the redacted exception message.
	DLQHeadersMessages = "messages"
)

// MinDLQWindow is the smallest dlq_growth window: a growth rate needs 30
// minutes of samples (internal/watch/rate.go minSpan).
const MinDLQWindow = 30 * time.Minute

var dlqHeaderModes = []string{DLQHeadersCountsOnly, DLQHeadersClasses, DLQHeadersMessages}

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
	// DLQTopics declares dead-letter topics the connector config cannot tell
	// the watcher about (consumer DLQs, or sink DLQs whose config is not readable).
	DLQTopics []DLQTopicSpec `yaml:"dlq_topics"`
	// DLQPeek holds the dlq_peek tool settings.
	DLQPeek DLQPeekSpec `yaml:"dlq_peek"`
}

// DLQTopicSpec is one watch.dlq_topics[] entry as written in YAML. It is
// validated in resolveDLQTopics and Owners.validateDLQ.
type DLQTopicSpec struct {
	Topic string `yaml:"topic"`
	// WrittenBy is a consumer group or connector of the same pipeline.
	WrittenBy string `yaml:"written_by"`
	// Shared must be true on every pipeline that declares the same topic.
	Shared bool `yaml:"shared"`
}

// UnmarshalYAML rejects unknown keys: a typo such as `writen_by:` would
// otherwise silently drop the writer.
func (s *DLQTopicSpec) UnmarshalYAML(node *yaml.Node) error {
	if err := checkKeys(node, "watch.dlq_topics[]", "topic", "written_by", "shared"); err != nil {
		return err
	}
	type plain DLQTopicSpec
	return node.Decode((*plain)(s))
}

// DLQPeekSpec is the raw watch.dlq_peek block.
type DLQPeekSpec struct {
	// Headers is counts_only, classes (default) or messages.
	Headers string `yaml:"headers"`
}

// UnmarshalYAML rejects unknown keys in the dlq_peek block.
func (s *DLQPeekSpec) UnmarshalYAML(node *yaml.Node) error {
	if err := checkKeys(node, "watch.dlq_peek", "headers"); err != nil {
		return err
	}
	type plain DLQPeekSpec
	return node.Decode((*plain)(s))
}

// UnmarshalYAML rejects unknown keys in the watch block: yaml.Unmarshal ignores
// them, so a typo such as `wal_headroom:` would silently keep the default.
func (s *WatchSpec) UnmarshalYAML(node *yaml.Node) error {
	if err := checkKeys(node, "watch", "enabled", "slot", "wal_headroom_bytes", "signals", "dlq_topics", "dlq_peek"); err != nil {
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
	Window  string `yaml:"window"` // dlq_growth only
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

// DLQSignal is the dlq_growth signal.
type DLQSignal struct {
	WatchSignal
	// FireRecords: this many new records in Window starts the timer. >= 1.
	FireRecords int
	// Window is how far back "new" looks. >= MinDLQWindow.
	Window time.Duration
}

// DLQTopic is a validated watch.dlq_topics[] declaration.
type DLQTopic struct {
	Topic     string
	WrittenBy string
	Shared    bool
}

// WatchConfig is a pipeline's watcher settings with defaults applied. It holds
// a slice (DLQTopics), so compare it with reflect.DeepEqual, not ==.
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
	DLQGrowth     DLQSignal
	// ConfigRisk logs by default: a bad config is a fact, not an incident.
	ConfigRisk WatchSignal

	// DLQTopics are the declared dead-letter topics, in file order.
	DLQTopics []DLQTopic
	// DLQPeekHeaders is one of the DLQHeaders* modes.
	DLQPeekHeaders string
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
		DLQGrowth: DLQSignal{
			WatchSignal: WatchSignal{Enabled: true, Action: ActionRoom, For: 5 * time.Minute},
			FireRecords: 1,
			Window:      time.Hour,
		},
		ConfigRisk:     WatchSignal{Enabled: true, Action: ActionLog, For: time.Minute},
		DLQPeekHeaders: DLQHeadersClasses,
	}
}

// Watch returns the validated watcher settings for the pipeline. A pipeline
// with no `watch:` block, or one built in code, gets DefaultWatch.
func (p Pipeline) Watch() WatchConfig {
	if p.watch != nil {
		w := *p.watch
		w.DLQTopics = slices.Clone(w.DLQTopics) // callers must not edit the shared slice
		return w
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
	if m := spec.DLQPeek.Headers; m != "" {
		if !slices.Contains(dlqHeaderModes, m) {
			return WatchConfig{}, fmt.Errorf("watch.dlq_peek.headers %q: want %s", m, strings.Join(dlqHeaderModes, ", "))
		}
		w.DLQPeekHeaders = m
	}

	for _, name := range sortedKeys(spec.Signals) {
		node := spec.Signals[name]
		if slices.Contains(v2Signals, name) {
			continue // parsed by the YAML decoder, ignored in v1
		}
		var s signalSpec
		switch name {
		case SignalConnectorDown, SignalSlotInactive, SignalSlotWAL, SignalDLQGrowth, SignalConfigRisk:
		default:
			return WatchConfig{}, fmt.Errorf("watch.signals.%s: unknown signal (known: %s, %s, %s, %s, %s; later: %s)",
				name, SignalConnectorDown, SignalSlotInactive, SignalSlotWAL, SignalDLQGrowth, SignalConfigRisk, strings.Join(v2Signals, ", "))
		}
		// node.Decode ignores unknown keys, so check them first: `{fier: 2GiB}`
		// must not silently keep the default.
		allowed := []string{"enabled", "action", "for"}
		if name == SignalSlotWAL {
			allowed = append(allowed, "fire", "clear", "eta_sev1")
		}
		if name == SignalDLQGrowth {
			allowed = append(allowed, "fire", "window")
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
		case SignalDLQGrowth:
			err = applyDLQ(&w.DLQGrowth, s)
		case SignalConfigRisk:
			err = applySignal(&w.ConfigRisk, s)
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

// applyDLQ parses fire as a record count, not a size: applySlotWAL's ParseSize
// would let `fire: 1k` through.
func applyDLQ(dst *DLQSignal, s signalSpec) error {
	if err := applySignal(&dst.WatchSignal, s); err != nil {
		return err
	}
	if s.Fire != "" {
		n, err := strconv.Atoi(s.Fire)
		if err != nil || n < 1 {
			return fmt.Errorf("fire %q: must be a whole number of records, at least 1", s.Fire)
		}
		dst.FireRecords = n
	}
	if s.Window != "" {
		d, err := ParseDuration(s.Window)
		if err != nil {
			return fmt.Errorf("window: %w", err)
		}
		if d < MinDLQWindow {
			return fmt.Errorf("window %q: must be at least 30m (a rate needs 30m of samples)", s.Window)
		}
		dst.Window = d
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
	// topicRE is Kafka's rule for a topic name.
	topicRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)
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

// resolveDLQTopics validates the pipeline's watch.dlq_topics into w. The
// checks that need other pipelines (CDC topic clash, sharing) are in
// Owners.validateDLQ.
func resolveDLQTopics(p Pipeline, w *WatchConfig) error {
	if p.WatchSpec == nil {
		return nil
	}
	for i, d := range p.WatchSpec.DLQTopics {
		where := fmt.Sprintf("watch.dlq_topics[%d]", i)
		// "." and ".." match the regexp but Kafka refuses them as topic names.
		if !topicRE.MatchString(d.Topic) || d.Topic == "." || d.Topic == ".." {
			return fmt.Errorf("%s: topic %q: not a valid Kafka topic name (letters, digits, '.', '_', '-', at most 249 characters)", where, d.Topic)
		}
		if d.WrittenBy == "" {
			return fmt.Errorf("%s: topic %q: written_by is required (a consumer group or connector of this pipeline)", where, d.Topic)
		}
		if !slices.Contains(p.ConsumerGroups, d.WrittenBy) && !slices.Contains(p.Connectors, d.WrittenBy) {
			return fmt.Errorf("%s: topic %q: written_by %q is neither a consumer group nor a connector of pipeline %q", where, d.Topic, d.WrittenBy, p.ID)
		}
		for _, prev := range w.DLQTopics {
			if prev.Topic == d.Topic {
				return fmt.Errorf("%s: topic %q is declared twice in this pipeline", where, d.Topic)
			}
		}
		w.DLQTopics = append(w.DLQTopics, DLQTopic{Topic: d.Topic, WrittenBy: d.WrittenBy, Shared: d.Shared})
	}
	return nil
}

// validateDLQ runs the DLQ checks that span pipelines: a DLQ topic must not be
// any pipeline's CDC topic, and a topic declared by several pipelines must be
// marked shared by all of them, so sharing is a written-down decision.
func (o *Owners) validateDLQ() error {
	cdc := map[string]bool{}
	for _, p := range o.Pipelines {
		for _, t := range p.SourceTables {
			cdc[p.TopicPrefix+"."+t] = true
		}
	}
	type decl struct {
		pipeline string
		shared   bool
	}
	byTopic := map[string][]decl{}
	var order []string
	for _, p := range o.Pipelines {
		if p.watch == nil {
			continue
		}
		for _, d := range p.watch.DLQTopics {
			if cdc[d.Topic] {
				return fmt.Errorf("pipeline %q: watch.dlq_topics: topic %q is a CDC topic; a DLQ must be a different topic", p.ID, d.Topic)
			}
			if _, ok := byTopic[d.Topic]; !ok {
				order = append(order, d.Topic)
			}
			byTopic[d.Topic] = append(byTopic[d.Topic], decl{p.ID, d.Shared})
		}
	}
	for _, topic := range order {
		ds := byTopic[topic]
		if len(ds) < 2 {
			continue
		}
		ids := make([]string, len(ds))
		allShared := true
		for i, d := range ds {
			ids[i] = strconv.Quote(d.pipeline)
			allShared = allShared && d.shared
		}
		if !allShared {
			return fmt.Errorf("watch.dlq_topics: %q is declared by %s: set shared: true on every declaration or give each pipeline its own DLQ",
				topic, strings.Join(ids, " and "))
		}
	}
	return nil
}
