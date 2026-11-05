package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// ---- connect_tune_throughput (X6c; docs/api.md "X6 performance actions") ----
//
// The one performance action: it changes only keys of TunableKeys, within
// their static bounds (CheckTunable, in Validate and again in Execute) and
// the target's (a sink's tasks.max at most the partitions it reads, a
// source's max.queue.size above its max.batch.size). Preview lists every key
// with its value now, the rollback the card offers; Stage tries it on the
// pipeline's staging Connect (only through the stage route); Verify compares
// the connector's throughput over the 15 min before the step with the 15 min
// from 2 min after it.

const tuneName = "connect_tune_throughput"

// Debezium's defaults, the value in force when a source config leaves the
// key unset (the queue/batch bound compares effective values).
const (
	defaultMaxBatchSize = 2048
	defaultMaxQueueSize = 8192
)

// Keys of the target-dependent bounds.
const (
	keyTasksMax     = "tasks.max"
	keyMaxBatchSize = "max.batch.size"
	keyMaxQueueSize = "max.queue.size"
)

// sampleNear is how far from a window's end a group_lag sample may lie: one
// sample interval, so a watcher that samples on time always has one.
const sampleNear = config.WatchSampleInterval

// minSampleSpan is the least time between the two samples a sink window is
// measured over.
const minSampleSpan = 5 * time.Minute

// TuningKafkaReader is the Kafka read connect_tune_throughput makes: a
// topic's partitions (clients.KafkaReader.TopicOffsets) and offsets by time
// (clients.RateReader; ErrNotSupported on a cluster without them). The
// reader client implements both; wiring hands the action a view with these
// two methods only.
type TuningKafkaReader interface {
	TopicOffsets(ctx context.Context, topic string) ([]clients.PartitionOffsets, error)
	clients.RateReader
}

// TuningDeps are the clients connect_tune_throughput reads and writes
// through. Any field may be nil: the action then refuses what needs it.
type TuningDeps struct {
	// Connect reads the connector's status and config (Validate, Preview,
	// Verify). Execute writes through it only when it is also a
	// clients.ConnectFixWriter (write mode on).
	Connect clients.ConnectReader
	// Kafka reads a sink's topic partitions (the tasks.max bound) and the
	// records written in Verify's windows. Read only.
	Kafka TuningKafkaReader
	// Samples reads the pipeline's group_lag watch samples, a sink's
	// consumed records in Verify; nil makes a sink tuning unverifiable.
	Samples clients.SampleReader
	// Staging is the pipeline's staging client set; only Stage uses it.
	Staging *Staging

	// now and timeout override time.Now and the 10 s call bound; tests only.
	now     func() time.Time
	timeout time.Duration
}

func (d TuningDeps) clock() time.Time {
	if d.now != nil {
		return d.now()
	}
	return time.Now()
}

func (d TuningDeps) callTimeout() time.Duration {
	if d.timeout > 0 {
		return d.timeout
	}
	return timeout
}

// RegisterTuning adds connect_tune_throughput to reg. It is separate from
// Register so the read clients it verifies with (Kafka rates, watch samples)
// are wired on their own, and so a harness that registers only Register's
// actions (evals, demo) offers no tuning.
func RegisterTuning(reg *Registry, d TuningDeps) error {
	return reg.Register(&tuneThroughput{d: d, stage: Deps{Staging: d.Staging}})
}

type tuneThroughput struct {
	d TuningDeps
	// stage holds Stage's staging set and timings (stage.go's Deps helpers).
	stage Deps
}

var (
	_ Classed  = (*tuneThroughput)(nil)
	_ Scoped   = (*tuneThroughput)(nil)
	_ Preview  = (*tuneThroughput)(nil)
	_ Verifier = (*tuneThroughput)(nil)
	_ Stager   = (*tuneThroughput)(nil)
)

func (a *tuneThroughput) Name() string  { return tuneName }
func (a *tuneThroughput) Layer() string { return "kafka-connect" }
func (a *tuneThroughput) Class() string { return ClassPerformance }

func (a *tuneThroughput) Describe() string {
	var keys []string
	for _, k := range TunableKeys() {
		who := "source, sink"
		if k.Connector != "" {
			who = k.Connector
		}
		keys = append(keys, fmt.Sprintf("%s %d-%d (%s)", k.Key, k.Min, k.Max, who))
	}
	return "Tunes one connector's throughput settings (performance class: judged by its throughput over the 15 min before the step against the 15 min from 2 min after it): params {connector, changes: {key: integer as text}}. Allowed: " +
		strings.Join(keys, "; ") + "; a sink's tasks.max at most the partitions of its topics, a source's max.queue.size above max.batch.size. " +
		"Propose it only after a kafka_throughput, connect_task_balance or debezium_snapshot_progress result shows the bottleneck, and cite that step's id (s_...) in the step description; never for a FAILED connector or task (that is a failure to fix, not to tune). Connect restarts the tasks; to undo, set the previous values."
}

func (a *tuneThroughput) ParamsSchema() map[string]any {
	props := map[string]any{}
	for _, k := range TunableKeys() {
		props[k.Key] = map[string]any{"type": "string", "description": fmt.Sprintf("integer %d-%d", k.Min, k.Max)}
	}
	return objectSchema(map[string]any{
		"connector": strProp("connector name"),
		"changes": map[string]any{
			"type":                 "object",
			"description":          "throughput keys to set, with their new integer values as text",
			"properties":           props,
			"additionalProperties": false,
		},
	}, "connector", "changes")
}

func (a *tuneThroughput) CheckScope(p owners.Pipeline, raw json.RawMessage) error {
	var params struct {
		Connector string `json:"connector"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return fmt.Errorf("actions: bad params: %w", err)
	}
	return inScopeConnector(p, params.Connector)
}

// tuningSet is parsed params: the connector and every key to set, each
// value in canonical base-10 form.
type tuningSet struct {
	connector string
	changes   map[string]string
}

func (s tuningSet) keys() []string { return sortedStringKeys(s.changes) }

// parseTuning decodes and checks params without I/O: every key must be in
// TunableKeys (for the type it applies to) with a value inside its static
// bounds. A value may be an integer or a string holding one. Errors wrap
// tools.ErrBadInput, ErrNotTunable or ErrTuningBounds and never echo a value.
func parseTuning(raw json.RawMessage) (tuningSet, error) {
	var in struct {
		Connector string                     `json:"connector"`
		Changes   map[string]json.RawMessage `json:"changes"`
	}
	if err := json.Unmarshal(raw, &in); err != nil || in.Connector == "" || len(in.Changes) == 0 {
		return tuningSet{}, fmt.Errorf("actions: %s: %w: connector and changes ({key: integer}) are required", tuneName, tools.ErrBadInput)
	}
	if n := len(TunableKeys()); len(in.Changes) > n {
		return tuningSet{}, fmt.Errorf("actions: %s: %w: changes has %d keys; the allow-list has %d", tuneName, tools.ErrBadInput, len(in.Changes), n)
	}
	set := tuningSet{connector: in.Connector, changes: make(map[string]string, len(in.Changes))}
	for _, k := range sortedRawKeys(in.Changes) {
		if len(k) > maxConfigKeyLen || !configKeyRe.MatchString(k) {
			return tuningSet{}, fmt.Errorf("actions: %s: %w: %q is not a config key name", tuneName, ErrNotTunable, shortText(k))
		}
		v := tuningValue(in.Changes[k])
		if err := CheckTunable(keyConnector(k), k, v); err != nil {
			return tuningSet{}, fmt.Errorf("actions: %s: %w", tuneName, err)
		}
		n, _ := strconv.ParseInt(v, 10, 64) // CheckTunable parsed it
		set.changes[k] = strconv.FormatInt(n, 10)
	}
	return set, nil
}

// sortedRawKeys returns the keys of m, sorted, so the first refusal is
// stable.
func sortedRawKeys(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// tuningValue reads one change's value: a JSON string or an integer token.
// Anything else comes back as "", which CheckTunable refuses as out of
// bounds.
func tuningValue(raw json.RawMessage) string {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		return n.String()
	}
	return ""
}

// keyConnector is the connector type to check key against before the
// connector's own type is known: the key's own (source when it applies to
// both), so only the static bounds and the allow-list are judged.
func keyConnector(key string) string {
	for _, k := range TunableKeys() {
		if k.Key == key && k.Connector != "" {
			return k.Connector
		}
	}
	return "source"
}

// checkType runs CheckTunable on every change for the connector's real type.
func (s tuningSet) checkType(connectorType string) error {
	for _, k := range s.keys() {
		if err := CheckTunable(connectorType, k, s.changes[k]); err != nil {
			return fmt.Errorf("actions: %s %s: %w", tuneName, s.connector, err)
		}
	}
	return nil
}

// failing describes a FAILED connector or task, "" when none is.
func failing(st clients.ConnectorStatus) string {
	if st.State == "FAILED" {
		return "the connector is FAILED"
	}
	for _, t := range st.Tasks {
		if t.State == "FAILED" {
			return fmt.Sprintf("task %d is FAILED", t.ID)
		}
	}
	return ""
}

// checkQueueBatch checks a source's max.queue.size above its max.batch.size,
// on the values the change leaves in force (the config's, else Debezium's
// defaults). Only a change of either key is checked.
func (s tuningSet) checkQueueBatch(cfg map[string]string) error {
	_, b := s.changes[keyMaxBatchSize]
	_, q := s.changes[keyMaxQueueSize]
	if !b && !q {
		return nil
	}
	batch, err := effectiveInt(s.changes, cfg, keyMaxBatchSize, defaultMaxBatchSize)
	if err != nil {
		return err
	}
	queue, err := effectiveInt(s.changes, cfg, keyMaxQueueSize, defaultMaxQueueSize)
	if err != nil {
		return err
	}
	if queue <= batch {
		return fmt.Errorf("actions: %s %s: %w: max.queue.size must be above max.batch.size (%d after the change), so it must be at least %d",
			tuneName, s.connector, ErrTuningBounds, batch, batch+1)
	}
	return nil
}

// effectiveInt is key's value after the change: the change's, else the
// config's, else def. A config value that is not an integer (a config
// provider reference) cannot be compared and is refused, never guessed.
func effectiveInt(changes, cfg map[string]string, key string, def int64) (int64, error) {
	v, ok := changes[key]
	if !ok {
		if v, ok = cfg[key]; !ok {
			return def, nil
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("actions: %s: %w: the connector's %s is not an integer, so the queue/batch bound cannot be checked", tuneName, ErrTuningBounds, key)
	}
	return n, nil
}

// check reads the connector's status and config and runs every check of a
// tuning on them: the allow-list for its type, not failing, the target
// bounds. Validate and Execute both call it, so the allow-list is checked
// right before the write too.
func (a *tuneThroughput) check(ctx context.Context, set tuningSet) (clients.ConnectorStatus, map[string]string, error) {
	if a.d.Connect == nil {
		return clients.ConnectorStatus{}, nil, fmt.Errorf("actions: %s: no Connect client: %w", tuneName, clients.ErrNotSupported)
	}
	st, err := a.d.Connect.Status(ctx, set.connector)
	if err != nil {
		return st, nil, fmt.Errorf("actions: %s: read the status of %s: %w", tuneName, set.connector, err)
	}
	if err := set.checkType(st.Type); err != nil {
		return st, nil, err
	}
	if why := failing(st); why != "" {
		return st, nil, fmt.Errorf("actions: %s %s: %s; a tuning is not a fix for a failing connector: find and fix the failure first", tuneName, set.connector, why)
	}
	cfg, err := a.d.Connect.Config(ctx, set.connector)
	if err != nil {
		return st, nil, fmt.Errorf("actions: %s: read the config of %s: %w", tuneName, set.connector, err)
	}
	if err := set.checkQueueBatch(cfg); err != nil {
		return st, nil, err
	}
	if v, ok := set.changes[keyTasksMax]; ok && st.Type == "sink" {
		if err := a.checkPartitions(ctx, set.connector, v, cfg); err != nil {
			return st, nil, err
		}
	}
	return st, cfg, nil
}

// checkPartitions refuses a sink's tasks.max above the partitions of the
// topics it reads: a task without a partition sits idle.
func (a *tuneThroughput) checkPartitions(ctx context.Context, connector, value string, cfg map[string]string) error {
	topics, err := sinkTopics(ctx, cfg)
	if err != nil {
		return fmt.Errorf("actions: %s %s: tasks.max bound: %w", tuneName, connector, err)
	}
	if a.d.Kafka == nil {
		return fmt.Errorf("actions: %s %s: no Kafka client to read the partitions tasks.max is bounded by: %w", tuneName, connector, clients.ErrNotSupported)
	}
	total := 0
	for _, t := range topics {
		parts, err := a.d.Kafka.TopicOffsets(ctx, t)
		if err != nil {
			return fmt.Errorf("actions: %s %s: read the partitions of %s: %w", tuneName, connector, t, err)
		}
		total += len(parts)
	}
	want, _ := strconv.Atoi(value)
	if want > total {
		return fmt.Errorf("actions: %s %s: %w: tasks.max must be at most %d, the partitions of the topics it reads (%s); %d tasks would sit idle",
			tuneName, connector, ErrTuningBounds, total, strings.Join(topics, ", "), want-total)
	}
	return nil
}

// sinkTopics is the topics a sink connector reads: its topics list, else the
// room pipeline's topics its topics.regex matches. With the room's pipeline
// in ctx a topic outside it is refused (tools.ErrOutOfScope) before any read.
func sinkTopics(ctx context.Context, cfg map[string]string) ([]string, error) {
	var topics []string
	for _, t := range strings.Split(cfg["topics"], ",") {
		if t = strings.TrimSpace(t); t != "" {
			topics = append(topics, t)
		}
	}
	p, scoped := tools.PipelineFrom(ctx)
	if len(topics) == 0 {
		re := strings.TrimSpace(cfg["topics.regex"])
		if re == "" || !scoped {
			return nil, fmt.Errorf("%w: the sink names no topics list (topics.regex needs the room's pipeline to resolve)", clients.ErrNotSupported)
		}
		rx, err := regexp.Compile("^(?:" + re + ")$")
		if err != nil {
			return nil, fmt.Errorf("%w: the sink's topics.regex does not compile here", clients.ErrNotSupported)
		}
		sc, err := tools.ScopeFor(ctx, nil)
		if err != nil {
			return nil, err
		}
		for _, t := range sc.Topics() {
			if rx.MatchString(t) {
				topics = append(topics, t)
			}
		}
		if len(topics) == 0 {
			return nil, fmt.Errorf("%w: the sink's topics.regex matches none of pipeline %s's topics", clients.ErrNotSupported, p.ID)
		}
		return topics, nil
	}
	if scoped {
		sc, err := tools.ScopeFor(ctx, nil)
		if err != nil {
			return nil, err
		}
		for _, t := range topics {
			if err := sc.CheckTopic(t); err != nil {
				return nil, fmt.Errorf("the sink reads a topic outside this pipeline: %w", err)
			}
		}
	}
	return topics, nil
}

func (a *tuneThroughput) Validate(ctx context.Context, raw json.RawMessage) error {
	set, err := parseTuning(raw)
	if err != nil {
		return err
	}
	if err := ctxConnectorScope(ctx, set.connector); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, a.d.callTimeout())
	defer cancel()
	_, _, err = a.check(ctx, set)
	return err
}

// plan splits the change set against the current config like
// configChangeSet.plan: the keys to write and the keys already at their
// value.
func (s tuningSet) plan(cfg map[string]string) (todo map[string]string, same []string) {
	return configChangeSet{connector: s.connector, changes: s.changes}.plan(cfg)
}

// currentInt is key's value now for a TuningChange: the stored value when it
// is a base-10 integer, else "" (unset, or a value that is not an integer,
// such as a config provider reference, which is never shown).
func currentInt(cfg map[string]string, key string) (string, bool) {
	v, ok := cfg[key]
	if !ok {
		return "", true
	}
	if _, err := strconv.ParseInt(v, 10, 64); err != nil {
		return "", false
	}
	return v, true
}

// shownInt renders key's value for a result: the integer, "unset", or "not
// an integer" (a config provider reference is never echoed).
func shownInt(cfg map[string]string, key string) string {
	v, isInt := currentInt(cfg, key)
	switch {
	case !isInt:
		return "not an integer"
	case v == "":
		return "unset"
	}
	return v
}

// rollback is the undo of set as a proposal step: every key back to its
// value now. ok is false when a key was unset or not an integer (the action
// cannot unset a key) or the old value is outside the allow-list's bounds.
func (s tuningSet) rollback(connectorType string, cfg map[string]string) (string, bool) {
	prev := make(map[string]string, len(s.changes))
	for _, k := range s.keys() {
		v, ok := cfg[k]
		if !ok || CheckTunable(connectorType, k, v) != nil {
			return "", false
		}
		prev[k] = v
	}
	b, _ := json.Marshal(map[string]any{"connector": s.connector, "changes": prev}) // strings only: cannot fail
	return tuneName + " " + string(b), true
}

func (a *tuneThroughput) Preview(ctx context.Context, raw json.RawMessage) (PreviewResult, error) {
	set, err := parseTuning(raw)
	if err != nil {
		return PreviewResult{}, previewErr(tuneName, err)
	}
	if err := ctxConnectorScope(ctx, set.connector); err != nil {
		return PreviewResult{}, previewErr(tuneName, err)
	}
	if a.d.Connect == nil {
		return PreviewResult{}, previewErr(tuneName, fmt.Errorf("no Connect client: %w", clients.ErrNotSupported))
	}
	ctx, cancel := context.WithTimeout(ctx, a.d.callTimeout())
	defer cancel()
	st, err := a.d.Connect.Status(ctx, set.connector)
	if err != nil {
		return PreviewResult{}, previewErr(tuneName, err)
	}
	if err := set.checkType(st.Type); err != nil {
		return PreviewResult{}, previewErr(tuneName, err)
	}
	cfg, err := a.d.Connect.Config(ctx, set.connector)
	if err != nil {
		return PreviewResult{}, previewErr(tuneName, err)
	}
	keys := set.keys()
	res := PreviewResult{Target: fmt.Sprintf("%s connector %s: %s", orUnknown(st.Type), set.connector, strings.Join(keys, ", "))}
	cur := make([]string, 0, len(keys))
	exp := make([]string, 0, len(keys))
	for _, k := range keys {
		now, isInt := currentInt(cfg, k)
		res.Tuning = append(res.Tuning, TuningChange{Key: k, Current: now, Proposed: set.changes[k]})
		switch {
		case !isInt:
			cur = append(cur, k+" = not an integer (not shown)")
		case now == "":
			cur = append(cur, k+" = unset")
		default:
			cur = append(cur, k+" = "+now)
		}
		exp = append(exp, k+" = "+set.changes[k])
	}
	res.Current = fmt.Sprintf("%s (GET /connectors/%s/config)", strings.Join(cur, "; "), set.connector)
	todo, same := set.plan(cfg)
	if len(todo) == 0 {
		res.Expected = fmt.Sprintf("no change: %s already set; no task restart", strings.Join(same, ", "))
		return res, nil
	}
	res.Expected = strings.Join(exp, "; ") + "; Connect restarts the tasks; judged by throughput over the 15 min before against the 15 min from 2 min after"
	res.Rollback, res.Reversible = set.rollback(st.Type, cfg)
	if d, err := validateMerged(ctx, a.d.Connect, cfg, todo); err != nil {
		res.DryRun = &DryRunResult{Source: sourceValidate, Skipped: dryRunText("validate failed: " + err.Error())}
	} else {
		res.DryRun = d
	}
	return res, nil
}

func (a *tuneThroughput) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	set, err := parseTuning(raw)
	if err != nil {
		return "", err
	}
	if err := ctxConnectorScope(ctx, set.connector); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, a.d.callTimeout())
	defer cancel()
	// Asserted here only: with write mode off the client is read-only, and
	// the agent must still be able to propose and preview.
	fw, ok := a.d.Connect.(clients.ConnectFixWriter)
	if !ok {
		return "", fmt.Errorf("actions: %s %s: the Connect client cannot write a config (clients.ConnectFixWriter); is write mode on?", tuneName, set.connector)
	}
	st, before, err := a.check(ctx, set)
	if err != nil {
		return "", err
	}
	todo, same := set.plan(before)
	if len(todo) == 0 {
		return fmt.Sprintf("%s %s already set; no change made", set.connector, strings.Join(same, ", ")), nil
	}
	moves := make([]string, 0, len(todo))
	for _, k := range sortedStringKeys(todo) {
		moves = append(moves, fmt.Sprintf("%s: %s → %s", k, shownInt(before, k), todo[k]))
	}
	if err := fw.UpdateConnectorConfigKeys(ctx, set.connector, todo); err != nil {
		return "", fmt.Errorf("actions: %s %s: %w", tuneName, set.connector, err)
	}
	// The write is done: later problems are a result, never an error, which
	// would invite a retry.
	head := set.connector + " " + strings.Join(moves, ", ")
	if rb, ok := set.rollback(st.Type, before); ok {
		head += "; rollback: " + rb
	}
	after, err := a.d.Connect.Config(ctx, set.connector)
	if err != nil {
		return fmt.Sprintf("%s (written; re-read of the config failed: %v)", head, err), nil
	}
	var off []string
	for _, k := range sortedStringKeys(todo) {
		if got := shownInt(after, k); got != todo[k] {
			off = append(off, fmt.Sprintf("%s = %s", k, got))
		}
	}
	if len(off) > 0 {
		return fmt.Sprintf("%s (written, but the re-read shows %s; check the connector config)", head, strings.Join(off, ", ")), nil
	}
	return head + "; Connect restarts the tasks; throughput is compared once the 15 min from 2 min after this step have passed", nil
}

// Stage applies the change set to the same-named connector on the staging
// Connect, watches it for the observe window and gives it back its whole
// previous config, like connect_update_connector_config's Stage. The
// allow-list and the queue/batch bound are checked again on staging's own
// type and config; a sink's partition bound was checked on production by
// Validate (staging may share its Kafka).
func (a *tuneThroughput) Stage(ctx context.Context, raw json.RawMessage) (StageResult, error) {
	set, err := parseTuning(raw)
	if err != nil {
		return StageResult{}, err
	}
	if err := stageScope(ctx, func(sc tools.Scope) error { return sc.CheckConnector(set.connector) }); err != nil {
		return StageResult{}, err
	}
	res := StageResult{Target: "connector " + set.connector}
	if a.stage.Staging == nil || a.stage.Staging.Connect == nil {
		res.Detail = events.StageNoStaging
		return res, nil
	}
	c := a.stage.Staging.Connect
	fw, canPatch := c.(clients.ConnectFixWriter)
	put, canPut := c.(clients.ConnectConfigPutter)
	if !canPatch || !canPut {
		return StageResult{}, fmt.Errorf("actions: stage %s: the staging Connect client cannot PATCH and PUT a connector config: %w", set.connector, clients.ErrNotSupported)
	}
	start := time.Now()
	present, err := hasConnector(ctx, c, set.connector)
	if err != nil {
		return StageResult{}, stagingErr("list staging connectors", err)
	}
	if !present {
		res.Detail = stageConnectorMissing
		return finish(res, start), nil
	}
	st, err := c.Status(ctx, set.connector)
	if err != nil {
		return StageResult{}, stagingErr("read the status of "+set.connector, err)
	}
	before, err := c.Config(ctx, set.connector)
	if err != nil {
		return StageResult{}, stagingErr("read the config of "+set.connector, err)
	}
	if err := errors.Join(set.checkType(st.Type), set.checkQueueBatch(before)); err != nil {
		res.DryRun = &DryRunResult{Source: "staging tuning guard", Text: err.Error()}
		return finish(res, start), nil
	}
	todo, same := set.plan(before)
	return a.stage.stageConnectorKeys(ctx, c, fw, put, set.connector, before, todo, same, res, start)
}

// ---- Verify: throughput before and after (X6) ----

// tuneWindow is one measured window: records per minute and, for a sink,
// the lag slope.
type tuneWindow struct {
	rate     int64
	slope    *int64
	from, to time.Time
}

// Verify measures the connector's throughput over the window before the step
// and the one after it (actions.PerformanceVerify*), from Kafka offsets by
// timestamp and, for a sink, the pipeline's group_lag samples. It reads only.
// An error wrapping clients.ErrNotSupported means the tuning cannot be
// measured (unverifiable); any other error is one failed call (the agent
// retries until its timeout), including the after window not closed yet or
// its group_lag sample not written yet.
func (a *tuneThroughput) Verify(ctx context.Context, raw json.RawMessage, executedAt time.Time) (VerifyResult, error) {
	set, err := parseTuning(raw)
	if err != nil {
		return VerifyResult{}, err
	}
	if _, ok := tools.PipelineFrom(ctx); !ok {
		return VerifyResult{}, fmt.Errorf("actions: verify %s: %w: Verify needs the room's pipeline in ctx", tuneName, tools.ErrNoScope)
	}
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		return VerifyResult{}, fmt.Errorf("actions: verify %s: %w", tuneName, err)
	}
	if err := sc.CheckConnector(set.connector); err != nil {
		return VerifyResult{}, fmt.Errorf("actions: verify %s: %w", tuneName, err)
	}
	if executedAt.IsZero() {
		return VerifyResult{}, fmt.Errorf("actions: verify %s: %w: no execution time", tuneName, tools.ErrBadInput)
	}
	afterFrom := executedAt.Add(PerformanceVerifySettle)
	afterTo := afterFrom.Add(PerformanceVerifyWindow)
	now := a.d.clock()
	if now.Before(afterTo) {
		return VerifyResult{}, fmt.Errorf("actions: verify %s: the after window closes at %s UTC", tuneName, afterTo.UTC().Format("15:04:05"))
	}
	rr := a.d.Kafka
	if a.d.Connect == nil || rr == nil {
		return VerifyResult{}, fmt.Errorf("actions: verify %s: no Connect client or no Kafka offsets by time: %w", tuneName, clients.ErrNotSupported)
	}
	ctx, cancel := context.WithTimeout(ctx, a.d.callTimeout())
	defer cancel()
	st, err := a.d.Connect.Status(ctx, set.connector)
	if err != nil {
		return VerifyResult{}, fmt.Errorf("actions: verify %s: read the status of %s: %w", tuneName, set.connector, err)
	}
	var before, after tuneWindow
	var topics []string
	switch st.Type {
	case "source":
		if topics = sc.Topics(); len(topics) == 0 {
			return VerifyResult{}, fmt.Errorf("actions: verify %s: pipeline %s has no topics to count: %w", tuneName, sc.Pipeline().ID, clients.ErrNotSupported)
		}
		if before, err = sourceWindow(ctx, rr, topics, executedAt.Add(-PerformanceVerifyWindow), executedAt); err == nil {
			after, err = sourceWindow(ctx, rr, topics, afterFrom, afterTo)
		}
	case "sink":
		before, after, topics, err = a.sinkWindows(ctx, rr, sc, set.connector, executedAt, now)
	default:
		return VerifyResult{}, fmt.Errorf("actions: verify %s: connector type %q: %w", tuneName, st.Type, clients.ErrNotSupported)
	}
	if err != nil {
		return VerifyResult{}, fmt.Errorf("actions: verify %s %s: %w", tuneName, set.connector, err)
	}
	return VerifyResult{
		Before: before.rate, After: after.rate, Unit: events.PerformanceUnitRecordsPerMinute,
		LagSlopeBefore: before.slope, LagSlopeAfter: after.slope,
		Detail: verifyDetail(st.Type, set.connector, topics, before, after),
	}, nil
}

// verifyDetail names the connector, its topics and the two windows, within
// VerifyResult.Detail's 200 bytes.
func verifyDetail(typ, connector string, topics []string, before, after tuneWindow) string {
	t := strings.Join(topics, ",")
	if len(t) > 60 {
		t = fmt.Sprintf("%d topics", len(topics))
	}
	hm := func(x time.Time) string { return x.UTC().Format("15:04") }
	return cutText(fmt.Sprintf("%s %s (%s): before %s-%s, after %s-%s UTC", typ, connector, t,
		hm(before.from), hm(before.to), hm(after.from), hm(after.to)), 200)
}

// perMinute is n records over span in whole records per minute.
func perMinute(n int64, span time.Duration) int64 {
	return int64(math.Round(float64(n) / span.Minutes()))
}

// sourceWindow is the records written to topics in [from, to), per minute.
func sourceWindow(ctx context.Context, rr clients.RateReader, topics []string, from, to time.Time) (tuneWindow, error) {
	n, err := recordsBetween(ctx, rr, topics, from, to)
	if err != nil {
		return tuneWindow{}, err
	}
	return tuneWindow{rate: perMinute(n, to.Sub(from)), from: from, to: to}, nil
}

// recordsBetween counts the records written to topics with a timestamp in
// [from, to): per partition the first offset at or after to less the first
// at or after from (the end offset when none is).
func recordsBetween(ctx context.Context, rr clients.RateReader, topics []string, from, to time.Time) (int64, error) {
	type tp struct {
		topic string
		part  int32
	}
	offsetsAt := func(at time.Time) (map[tp]int64, error) {
		rates, err := rr.PartitionRates(ctx, at, topics...)
		if err != nil {
			return nil, fmt.Errorf("offsets at %s: %w", at.UTC().Format(time.RFC3339), err)
		}
		out := make(map[tp]int64, len(rates))
		for _, r := range rates {
			off := r.OffsetAtStart
			if off < 0 {
				off = r.End
			}
			out[tp{r.Topic, r.Partition}] = off
		}
		return out, nil
	}
	start, err := offsetsAt(from)
	if err != nil {
		return 0, err
	}
	end, err := offsetsAt(to)
	if err != nil {
		return 0, err
	}
	var n int64
	for k, e := range end {
		if s, ok := start[k]; ok && e > s {
			n += e - s
		}
	}
	return n, nil
}

// sinkGroup is the consumer group Connect runs a sink connector under.
func sinkGroup(connector string, cfg map[string]string) string {
	if g := strings.TrimSpace(cfg["consumer.override.group.id"]); g != "" {
		return g
	}
	return "connect-" + connector
}

// sinkWindows measures a sink's consumed records over the two windows: the
// records written to its topics less the change of the pipeline's group_lag
// between two samples near each window's ends. group_lag is the total of
// the pipeline's declared groups, so it measures this sink only when the
// sink's group is the one group the pipeline declares.
func (a *tuneThroughput) sinkWindows(ctx context.Context, rr clients.RateReader, sc tools.Scope, connector string, executedAt, now time.Time) (before, after tuneWindow, topics []string, err error) {
	if a.d.Samples == nil {
		return before, after, nil, fmt.Errorf("no group_lag watch samples in this process (the watcher does not run here): %w", clients.ErrNotSupported)
	}
	cfg, err := a.d.Connect.Config(ctx, connector)
	if err != nil {
		return before, after, nil, fmt.Errorf("read the config: %w", err)
	}
	pl := sc.Pipeline()
	if g := sinkGroup(connector, cfg); len(pl.ConsumerGroups) != 1 || pl.ConsumerGroups[0] != g {
		return before, after, nil, fmt.Errorf("group_lag sums the pipeline's groups (%s), not only the sink's group %s: %w",
			strings.Join(pl.ConsumerGroups, ", "), g, clients.ErrNotSupported)
	}
	if topics, err = sinkTopics(ctx, cfg); err != nil {
		return before, after, nil, err
	}
	pts, err := a.d.Samples.Samples(ctx, pl.ID, clients.WatchMetricGroupLag, executedAt.Add(-PerformanceVerifyWindow-sampleNear))
	if err != nil {
		return before, after, nil, fmt.Errorf("read the group_lag samples: %w", err)
	}
	b1, ok1 := closestSample(pts, executedAt.Add(-sampleNear), executedAt, executedAt)
	var b0 clients.SamplePoint
	var ok0 bool
	if ok1 {
		b0, ok0 = closestSample(pts, executedAt.Add(-PerformanceVerifyWindow-sampleNear), b1.At.Add(-minSampleSpan), executedAt.Add(-PerformanceVerifyWindow))
	}
	if !ok0 || !ok1 {
		return before, after, nil, fmt.Errorf("no two group_lag samples cover the %s before the step (the watcher was off or young): %w",
			PerformanceVerifyWindow, clients.ErrNotSupported)
	}
	afterFrom := executedAt.Add(PerformanceVerifySettle)
	a0, ok0 := closestSample(pts, afterFrom, afterFrom.Add(sampleNear), afterFrom)
	var a1 clients.SamplePoint
	ok1 = false
	if ok0 {
		a1, ok1 = closestSample(pts, a0.At.Add(minSampleSpan), afterFrom.Add(PerformanceVerifyWindow+sampleNear), afterFrom.Add(PerformanceVerifyWindow))
	}
	if !ok0 || !ok1 {
		why := fmt.Errorf("no two group_lag samples cover the %s after the step yet", PerformanceVerifyWindow)
		if !now.Before(executedAt.Add(PerformanceVerifyTimeout)) {
			why = fmt.Errorf("no two group_lag samples cover the %s after the step: %w", PerformanceVerifyWindow, clients.ErrNotSupported)
		}
		return before, after, nil, why
	}
	if before, err = sinkWindow(ctx, rr, topics, b0, b1); err == nil {
		after, err = sinkWindow(ctx, rr, topics, a0, a1)
	}
	return before, after, topics, err
}

// closestSample is the sample with lo <= At <= hi closest to target.
func closestSample(pts []clients.SamplePoint, lo, hi, target time.Time) (clients.SamplePoint, bool) {
	var best clients.SamplePoint
	found := false
	dist := func(t time.Time) time.Duration { return max(t.Sub(target), target.Sub(t)) }
	for _, p := range pts {
		if p.At.Before(lo) || p.At.After(hi) {
			continue
		}
		if !found || dist(p.At) < dist(best.At) {
			best, found = p, true
		}
	}
	return best, found
}

// sinkWindow is a sink's consumed records per minute between two group_lag
// samples, and the lag's slope.
func sinkWindow(ctx context.Context, rr clients.RateReader, topics []string, s0, s1 clients.SamplePoint) (tuneWindow, error) {
	span := s1.At.Sub(s0.At)
	produced, err := recordsBetween(ctx, rr, topics, s0.At, s1.At)
	if err != nil {
		return tuneWindow{}, err
	}
	lagDelta := int64(math.Round(s1.Value - s0.Value))
	consumed := produced - lagDelta
	if consumed < 0 {
		return tuneWindow{}, fmt.Errorf("the lag grew by more than the records written between the samples (the groups or topics changed): %w", clients.ErrNotSupported)
	}
	slope := perMinute(lagDelta, span)
	return tuneWindow{rate: perMinute(consumed, span), slope: &slope, from: s0.At, to: s1.At}, nil
}
