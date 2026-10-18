package actions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// KafkaDeps holds the Kafka clients of the P3 Kafka actions. The action
// stays registered (and listed by GET /api/actions) with write mode off:
// Validate and Preview need only the read-only Planner, Execute alone needs
// the Writer, which wiring builds only with write mode on. A nil field makes
// the step that needs it refuse. Pass a nil interface, never a typed nil
// pointer.
type KafkaDeps struct {
	// Writer commits the reset (Execute only): a *kafka.Writer.
	Writer clients.KafkaWriter
	// Planner reads the group's state and planned moves: kafka.NewPlanner on
	// the reader client (write mode off or on), or the *kafka.Writer.
	Planner clients.GroupResetPlanner

	// validateTimeout overrides timeout for Validate's I/O; now overrides
	// time.Now. Tests only; zero values mean the defaults.
	validateTimeout time.Duration
	now             func() time.Time
}

// RegisterKafka adds the Kafka write actions to reg. It is separate from
// Register so the Kafka clients are wired on their own (write mode).
func RegisterKafka(reg *Registry, d KafkaDeps) error {
	return reg.Register(&resetConsumerOffsets{d: d})
}

func (d KafkaDeps) clock() time.Time {
	if d.now != nil {
		return d.now()
	}
	return time.Now()
}

var (
	_ Scoped  = (*resetConsumerOffsets)(nil)
	_ Preview = (*resetConsumerOffsets)(nil)
)

// ---- kafka_reset_consumer_offsets ----

const (
	resetName  = "kafka_reset_consumer_offsets"
	toEarliest = "earliest"
	toTime     = "timestamp"
	// maxListedPartitions caps the per-partition lines of Current and
	// Expected; Rollback always lists every partition.
	maxListedPartitions = 50
)

type resetConsumerOffsets struct{ d KafkaDeps }

func (a *resetConsumerOffsets) Name() string  { return resetName }
func (a *resetConsumerOffsets) Layer() string { return "consumer" }
func (a *resetConsumerOffsets) Describe() string {
	return "Moves a stopped consumer group's committed offsets back to the earliest offset or to the first record at or after a past timestamp, so it reprocesses (e.g. after a bad deploy). The group must have no active members: stop its consumers first. The reset moves every topic the group has committed offsets on, so topics must list exactly those topics, all of this pipeline. The preview lists, per partition, the committed offset now, the target and the lag change, and keeps the current offsets as the rollback point."
}

func (a *resetConsumerOffsets) ParamsSchema() map[string]any {
	return objectSchema(map[string]any{
		"group": strProp("consumer group; one of the pipeline's consumer_groups"),
		"topics": map[string]any{
			"type":        "array",
			"items":       map[string]any{"type": "string"},
			"minItems":    1,
			"description": "every topic the group has committed offsets on (the reset moves them all); each one a topic of this pipeline",
		},
		"to": map[string]any{"type": "string", "enum": []string{toEarliest, toTime}, "description": "earliest: the log start of each partition; timestamp: the first record at or after timestamp"},
		"timestamp": map[string]any{
			"type": "string", "format": "date-time",
			"description": "RFC 3339 time in the past; required when to is timestamp, refused otherwise",
		},
	}, "group", "topics", "to")
}

type resetOffsetsParams struct {
	Group     string   `json:"group"`
	Topics    []string `json:"topics"`
	To        string   `json:"to"`
	Timestamp string   `json:"timestamp,omitempty"`
}

// parse decodes and checks params without I/O and returns the client target.
// Unknown fields are refused: a misspelt or unsupported param (say,
// per-partition offsets) must not be dropped silently on a write.
func (a *resetConsumerOffsets) parse(raw json.RawMessage) (resetOffsetsParams, clients.OffsetTarget, error) {
	var p resetOffsetsParams
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, clients.OffsetTarget{}, fmt.Errorf("actions: %s: bad params: %w", resetName, err)
	}
	if strings.TrimSpace(p.Group) == "" {
		return p, clients.OffsetTarget{}, fmt.Errorf("actions: %s: group is required", resetName)
	}
	if len(p.Topics) == 0 {
		return p, clients.OffsetTarget{}, fmt.Errorf("actions: %s: topics is required: list every topic the group has committed offsets on", resetName)
	}
	seen := map[string]bool{}
	for _, t := range p.Topics {
		if strings.TrimSpace(t) == "" {
			return p, clients.OffsetTarget{}, fmt.Errorf("actions: %s: topics has an empty name", resetName)
		}
		if seen[t] {
			return p, clients.OffsetTarget{}, fmt.Errorf("actions: %s: topic %q is listed twice", resetName, t)
		}
		seen[t] = true
	}
	switch p.To {
	case toEarliest:
		if p.Timestamp != "" {
			return p, clients.OffsetTarget{}, fmt.Errorf("actions: %s: timestamp is only allowed with to=timestamp", resetName)
		}
		return p, clients.OffsetTarget{Earliest: true}, nil
	case toTime:
		if p.Timestamp == "" {
			return p, clients.OffsetTarget{}, fmt.Errorf("actions: %s: to=timestamp needs timestamp (RFC 3339)", resetName)
		}
		ts, err := time.Parse(time.RFC3339, p.Timestamp)
		if err != nil {
			return p, clients.OffsetTarget{}, fmt.Errorf("actions: %s: timestamp %q is not RFC 3339: %w", resetName, p.Timestamp, err)
		}
		if ts.UnixMilli() < 0 {
			return p, clients.OffsetTarget{}, fmt.Errorf("actions: %s: timestamp %s is before 1970", resetName, p.Timestamp)
		}
		// A future time resolves to the end of every partition: the group
		// would skip every unread record instead of reprocessing.
		if ts.After(a.d.clock()) {
			return p, clients.OffsetTarget{}, fmt.Errorf("actions: %s: timestamp %s is in the future; a reset to it would skip every unread record", resetName, p.Timestamp)
		}
		return p, clients.OffsetTarget{Timestamp: ts}, nil
	}
	return p, clients.OffsetTarget{}, fmt.Errorf("actions: %s: to %q is not one of earliest, timestamp", resetName, p.To)
}

func (a *resetConsumerOffsets) CheckScope(pl owners.Pipeline, raw json.RawMessage) error {
	var p resetOffsetsParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("actions: bad params: %w", err)
	}
	if p.Group == "" || len(p.Topics) == 0 {
		return fmt.Errorf("actions: %s: group and topics are required", resetName)
	}
	sc, err := tools.ScopeFor(tools.WithPipeline(context.Background(), pl), nil)
	if err != nil {
		return fmt.Errorf("actions: %w", err)
	}
	return checkResetNames(sc, p.Group, p.Topics)
}

// checkResetNames applies the Kafka writer's own scope rules (tools.Scope:
// consumer_groups, "<topic_prefix>.<source_table>") to the group and topics.
func checkResetNames(sc tools.Scope, group string, topics []string) error {
	if err := sc.CheckGroup(group); err != nil {
		return fmt.Errorf("actions: %w", err)
	}
	for _, t := range topics {
		if err := sc.CheckTopic(t); err != nil {
			return fmt.Errorf("actions: %w", err)
		}
	}
	return nil
}

// ctxScope scopes Preview and Execute, fail closed like the Kafka writer:
// the room's whole pipeline in ctx (tools.WithPipeline), or the explicit
// opt-out (tools.WithUnscoped; CLI and integration tests). Anything else,
// a bare ctx or a pipeline id only, wraps tools.ErrNoScope.
func ctxScope(ctx context.Context) (tools.Scope, error) {
	if _, ok := tools.PipelineFrom(ctx); !ok {
		sc, err := tools.ScopeFor(ctx, nil)
		if err == nil && sc.Unscoped() {
			return sc, nil
		}
		return tools.Scope{}, fmt.Errorf("actions: %s: %w: needs the room's pipeline in ctx", resetName, tools.ErrNoScope)
	}
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		return tools.Scope{}, fmt.Errorf("actions: %s: %w", resetName, err)
	}
	return sc, nil
}

func (a *resetConsumerOffsets) noPlanner() error {
	return fmt.Errorf("actions: %s: no Kafka client configured to read the group", resetName)
}

func (a *resetConsumerOffsets) noWriter() error {
	return fmt.Errorf("actions: %s: no Kafka write client: PIPEROOM_WRITE_MODE is off", resetName)
}

// checkInactive refuses a group with active members: its next commit would
// overwrite the reset.
func (a *resetConsumerOffsets) checkInactive(ctx context.Context, group string) error {
	err := a.d.Planner.CheckGroupInactive(ctx, group)
	switch {
	case errors.Is(err, clients.ErrGroupActive):
		return fmt.Errorf("actions: %s: consumer group %q has active members; stop its consumers first, then run the reset: %w", resetName, group, err)
	case err != nil:
		return fmt.Errorf("actions: %s: check group %q: %w", resetName, group, err)
	}
	return nil
}

// plan returns the client's planned moves, refusing a plan whose topics
// are not exactly p.Topics. The client resets every committed topic of the
// group, so on a group shared with another pipeline a topic outside the
// listed (scope-checked) ones would move too.
func (a *resetConsumerOffsets) plan(ctx context.Context, p resetOffsetsParams, to clients.OffsetTarget) ([]clients.OffsetMove, error) {
	moves, err := a.d.Planner.PlanGroupReset(ctx, p.Group, to)
	if err != nil {
		if errors.Is(err, clients.ErrNotFound) {
			return nil, fmt.Errorf("actions: %s: group %q has no committed offsets to reset: %w", resetName, p.Group, err)
		}
		return nil, fmt.Errorf("actions: %s: plan reset of %q: %w", resetName, p.Group, err)
	}
	listed := map[string]bool{}
	for _, t := range p.Topics {
		listed[t] = true
	}
	planned := map[string]bool{}
	var extra []string
	for _, m := range moves {
		if !planned[m.Topic] && !listed[m.Topic] {
			extra = append(extra, m.Topic)
		}
		planned[m.Topic] = true
	}
	if len(extra) > 0 {
		sort.Strings(extra)
		return nil, fmt.Errorf("actions: %s: group %q also has committed offsets on %s; the reset moves every committed topic of the group, so topics must list them (and they must be this pipeline's)", resetName, p.Group, strings.Join(extra, ", "))
	}
	for _, t := range p.Topics {
		if !planned[t] {
			return nil, fmt.Errorf("actions: %s: group %q has no committed offsets on topic %q (or the topic does not exist); nothing there to reset", resetName, p.Group, t)
		}
	}
	return moves, nil
}

// checkPlanScope checks every planned topic against the scope, on top of
// CheckScope's check of the listed ones.
func checkPlanScope(sc tools.Scope, group string, moves []clients.OffsetMove) error {
	topics := map[string]bool{}
	var list []string
	for _, m := range moves {
		if !topics[m.Topic] {
			topics[m.Topic] = true
			list = append(list, m.Topic)
		}
	}
	return checkResetNames(sc, group, list)
}

// Validate reads the target (≤ 10 s): the group must exist with committed
// offsets on exactly the listed topics and have no active member. It does
// not need a scope in ctx; when ctx carries the room's pipeline, the group
// and every planned topic are checked against it too.
func (a *resetConsumerOffsets) Validate(ctx context.Context, raw json.RawMessage) error {
	p, to, err := a.parse(raw)
	if err != nil {
		return err
	}
	if a.d.Planner == nil {
		return a.noPlanner()
	}
	t := timeout
	if a.d.validateTimeout > 0 {
		t = a.d.validateTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, t)
	defer cancel()
	if pl, ok := tools.PipelineFrom(ctx); ok {
		if err := a.CheckScope(pl, raw); err != nil {
			return err
		}
	}
	if err := a.checkInactive(ctx, p.Group); err != nil {
		return err
	}
	_, err = a.plan(ctx, p, to)
	return err
}

func (a *resetConsumerOffsets) Preview(ctx context.Context, raw json.RawMessage) (PreviewResult, error) {
	p, to, err := a.parse(raw)
	if err != nil {
		return PreviewResult{}, previewErr(resetName, err)
	}
	if a.d.Planner == nil {
		return PreviewResult{}, previewErr(resetName, a.noPlanner())
	}
	sc, err := ctxScope(ctx)
	if err != nil {
		return PreviewResult{}, previewErr(resetName, err)
	}
	if err := checkResetNames(sc, p.Group, p.Topics); err != nil {
		return PreviewResult{}, previewErr(resetName, err)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := a.checkInactive(ctx, p.Group); err != nil {
		return PreviewResult{}, previewErr(resetName, err)
	}
	moves, err := a.plan(ctx, p, to)
	if err != nil {
		return PreviewResult{}, previewErr(resetName, err)
	}
	if err := checkPlanScope(sc, p.Group, moves); err != nil {
		return PreviewResult{}, previewErr(resetName, err)
	}
	return PreviewResult{
		Target:     fmt.Sprintf("consumer group %s on %s", p.Group, strings.Join(p.Topics, ", ")),
		Current:    "group has no active members; committed offset (lag) per partition: " + listMoves(moves, currentText),
		Expected:   moveSummary(moves, to) + "; per partition: " + listMoves(moves, func(m clients.OffsetMove) string { return moveText(m, to) }),
		Reversible: false, // no action commits explicit offsets yet
		Rollback:   rollbackOffsets(moves),
	}, nil
}

func (a *resetConsumerOffsets) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, to, err := a.parse(raw)
	if err != nil {
		return "", err
	}
	if a.d.Writer == nil {
		return "", a.noWriter()
	}
	if a.d.Planner == nil {
		return "", a.noPlanner()
	}
	// Scope before any I/O; the writer checks the group again on its own.
	sc, err := ctxScope(ctx)
	if err != nil {
		return "", err
	}
	if err := checkResetNames(sc, p.Group, p.Topics); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := a.checkInactive(ctx, p.Group); err != nil {
		return "", err
	}
	before, err := a.plan(ctx, p, to)
	if err != nil {
		return "", err
	}
	if err := checkPlanScope(sc, p.Group, before); err != nil {
		return "", err
	}
	// The writer plans again and commits that plan. The group is inactive and
	// every committed topic was just checked; a commit on a new topic by an
	// admin in between is the one window left, and the after-read shows it.
	moved, err := a.d.Writer.ResetGroupOffsets(ctx, p.Group, to)
	if err != nil {
		hint := ""
		if errors.Is(err, clients.ErrGroupActive) {
			hint = "a consumer joined: stop the group's consumers first; "
		}
		return "", fmt.Errorf("actions: %s: reset %q (%d partitions moved): %s%s: %w", resetName, p.Group, moved, hint, rollbackOffsets(before), err)
	}
	head := fmt.Sprintf("reset group %s to %s: %d of %d partitions moved", p.Group, targetText(to), moved, len(before))
	// The commit is done: a failed re-read is reported, not an error, so the
	// agent does not retry a reset that happened.
	after, err := a.d.Planner.PlanGroupReset(ctx, p.Group, to)
	if err != nil {
		return fmt.Sprintf("%s; offsets before: %s; re-read failed: %v; %s", head, listMoves(before, func(m clients.OffsetMove) string { return offText(m.Committed) }), err, rollbackOffsets(before)), nil
	}
	return fmt.Sprintf("%s; committed before → after: %s; %s", head, beforeAfter(before, after), rollbackOffsets(before)), nil
}

// ---- rendering ----

func targetText(to clients.OffsetTarget) string {
	if to.Earliest {
		return "earliest"
	}
	return to.Timestamp.UTC().Format(time.RFC3339)
}

func offText(o int64) string {
	if o < 0 {
		return "none"
	}
	return fmt.Sprintf("%d", o)
}

func lagText(m clients.OffsetMove, at int64) string {
	if at < 0 {
		return "lag ?"
	}
	return fmt.Sprintf("lag %d", m.End-at)
}

func currentText(m clients.OffsetMove) string {
	return fmt.Sprintf("%s (%s)", offText(m.Committed), lagText(m, m.Committed))
}

// moveText is one partition's move: committed → target with the lag change.
// A timestamp target equal to the end offset is marked: no record is at or
// after the time there, so the partition keeps nothing to reprocess.
func moveText(m clients.OffsetMove, to clients.OffsetTarget) string {
	s := fmt.Sprintf("%s → %d, %s → %s", offText(m.Committed), m.Target, lagText(m, m.Committed), lagText(m, m.Target))
	if m.Committed >= 0 {
		s += fmt.Sprintf(" (%+d)", m.Committed-m.Target)
	}
	if m.Target == m.Committed {
		s += " (no change)"
	}
	if !to.Earliest && m.Target == m.End {
		s += " (no record at or after the time: end offset)"
	}
	return s
}

// listMoves renders moves grouped by topic as
// "t: p0 <f>, p1 <f>; t2: p0 <f>", at most maxListedPartitions of them.
func listMoves(moves []clients.OffsetMove, f func(clients.OffsetMove) string) string {
	var b strings.Builder
	topic := ""
	for i, m := range moves {
		if i == maxListedPartitions {
			fmt.Fprintf(&b, " … and %d more partitions (see rollback)", len(moves)-i)
			break
		}
		switch {
		case i == 0:
			fmt.Fprintf(&b, "%s: ", m.Topic)
		case m.Topic != topic:
			fmt.Fprintf(&b, "; %s: ", m.Topic)
		default:
			b.WriteString(", ")
		}
		topic = m.Topic
		fmt.Fprintf(&b, "p%d %s", m.Partition, f(m))
	}
	return b.String()
}

// moveSummary totals the plan: records the group will read again and,
// flagged, records it would skip (a target past the committed offset).
func moveSummary(moves []clients.OffsetMove, to clients.OffsetTarget) string {
	var change, replay, skip int64
	skipParts := 0
	for _, m := range moves {
		if m.Target != m.Committed {
			change++
		}
		if m.Committed < 0 {
			continue
		}
		switch d := m.Committed - m.Target; {
		case d > 0:
			replay += d
		case d < 0:
			skip -= d
			skipParts++
		}
	}
	s := fmt.Sprintf("%d of %d partitions move to %s; the group re-reads %d records", change, len(moves), targetText(to), replay)
	if skip > 0 {
		s += fmt.Sprintf("; WARNING: %d partitions move forward and skip %d unread records", skipParts, skip)
	}
	return s
}

// rollbackOffsets names every partition's committed offset before the reset:
// the rollback point (P3a). No action commits explicit offsets yet, so it says
// how to restore them by hand.
func rollbackOffsets(moves []clients.OffsetMove) string {
	var b strings.Builder
	b.WriteString("rollback offsets (committed before the reset; restore with kafka-consumer-groups --reset-offsets --from-file, no action sets explicit offsets): ")
	topic := ""
	for i, m := range moves {
		switch {
		case i == 0:
			fmt.Fprintf(&b, "%s: ", m.Topic)
		case m.Topic != topic:
			fmt.Fprintf(&b, "; %s: ", m.Topic)
		default:
			b.WriteString(", ")
		}
		topic = m.Topic
		fmt.Fprintf(&b, "p%d=%s", m.Partition, offText(m.Committed))
	}
	return b.String()
}

// beforeAfter pairs each partition's committed offset before with the one
// re-read after the reset (the after plan's Committed).
func beforeAfter(before, after []clients.OffsetMove) string {
	type tp struct {
		t string
		p int32
	}
	now := map[tp]int64{}
	for _, m := range after {
		now[tp{m.Topic, m.Partition}] = m.Committed
	}
	return listMoves(before, func(m clients.OffsetMove) string {
		a, ok := now[tp{m.Topic, m.Partition}]
		if !ok {
			return offText(m.Committed) + " → ?"
		}
		return offText(m.Committed) + " → " + offText(a)
	})
}
