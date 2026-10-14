package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// ---- kafka_topic_offsets ----

type topicOffsets struct{ d Deps }

func (t *topicOffsets) Name() string     { return "kafka_topic_offsets" }
func (t *topicOffsets) Kind() tools.Kind { return tools.KindRead }
func (t *topicOffsets) InputSchema() map[string]any {
	return objectSchema(map[string]any{"topic": strProp("topic name")}, "topic")
}
func (t *topicOffsets) Description() string {
	return "Per-partition start/end offsets, total record count and the newest record's age."
}

func (t *topicOffsets) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Topic string `json:"topic"`
	}
	if err := json.Unmarshal(input, &p); err != nil || p.Topic == "" {
		return tools.Result{}, fmt.Errorf("%w: topic is required", tools.ErrBadInput)
	}
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	if err := sc.CheckTopic(p.Topic); err != nil {
		return tools.Result{}, err
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	offs, err := t.d.Kafka.TopicOffsets(cctx, p.Topic)
	if err != nil {
		return tools.Result{}, err
	}
	var total int64
	var sb strings.Builder
	for i, o := range offs {
		if i > 0 {
			sb.WriteString("\n")
		}
		total += o.End - o.Start
		fmt.Fprintf(&sb, "partition %d: %d–%d (%d records)", o.Partition, o.Start, o.End, o.End-o.Start)
		if !o.LastTimestamp.IsZero() {
			fmt.Fprintf(&sb, ", newest %s", age(o.LastTimestamp))
		}
	}
	return res(sb.String(), map[string]any{
		"topic": p.Topic, "partitions": offs, "total_records": total,
	}), nil
}

func age(t time.Time) string {
	d := time.Since(t)
	if d < time.Minute {
		return "just now"
	}
	if d < time.Hour {
		return fmt.Sprintf("%d min ago", int(d.Minutes()))
	}
	return fmt.Sprintf("%.1f h ago", d.Hours())
}

// ---- kafka_list_groups ----

type listGroups struct{ d Deps }

func (t *listGroups) Name() string     { return "kafka_list_groups" }
func (t *listGroups) Kind() tools.Kind { return tools.KindRead }
func (t *listGroups) InputSchema() map[string]any {
	return objectSchema(map[string]any{"topic": strProp("optional topic: only groups reading this topic")})
}
func (t *listGroups) Description() string {
	return "Lists the pipeline's consumer groups with their state; optionally only those reading a given topic."
}

func (t *listGroups) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Topic string `json:"topic"`
	}
	if err := json.Unmarshal(input, &p); err != nil {
		return tools.Result{}, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	if p.Topic != "" {
		if err := sc.CheckTopic(p.Topic); err != nil {
			return tools.Result{}, err
		}
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	groups, err := t.d.Kafka.ConsumerGroups(cctx)
	if err != nil {
		return tools.Result{}, err
	}
	// Only this room's groups; the others get no GroupLag call either and are
	// only counted.
	all := len(groups)
	groups = sc.FilterGroups(groups)
	other := all - len(groups)
	type row struct {
		Group string `json:"group"`
		State string `json:"state"`
	}
	var rows []row
	for _, g := range groups {
		lag, err := t.d.Kafka.GroupLag(cctx, g)
		if err != nil {
			rows = append(rows, row{Group: g, State: "?"})
			continue
		}
		if p.Topic != "" && !groupReadsTopic(lag, p.Topic) {
			continue
		}
		rows = append(rows, row{Group: g, State: lag.State})
	}
	var sb strings.Builder
	for i, r := range rows {
		if i > 0 {
			sb.WriteString("\n")
		}
		fmt.Fprintf(&sb, "%s: %s", r.Group, r.State)
	}
	appendLine(&sb, tools.NotShown(other, "consumer group", "consumer groups"))
	return res(sb.String(), rows), nil
}

func groupReadsTopic(lag clients.GroupLag, topic string) bool {
	for _, pl := range lag.Partitions {
		if pl.Topic == topic {
			return true
		}
	}
	return false
}

// ---- kafka_consumer_lag ----

type consumerLag struct{ d Deps }

func (t *consumerLag) Name() string     { return "kafka_consumer_lag" }
func (t *consumerLag) Kind() tools.Kind { return tools.KindRead }
func (t *consumerLag) InputSchema() map[string]any {
	return objectSchema(map[string]any{"group": strProp("consumer group name")}, "group")
}
func (t *consumerLag) Description() string {
	return "Consumer group state, member count, total lag and the 5 worst partitions."
}

func (t *consumerLag) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Group string `json:"group"`
	}
	if err := json.Unmarshal(input, &p); err != nil || p.Group == "" {
		return tools.Result{}, fmt.Errorf("%w: group is required", tools.ErrBadInput)
	}
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	if err := sc.CheckGroup(p.Group); err != nil {
		return tools.Result{}, err
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	lag, anomalies, err := groupLag(cctx, t.d.Kafka, p.Group)
	if err != nil {
		if errors.Is(err, clients.ErrNotFound) {
			return res(fmt.Sprintf("not found: consumer group %q does not exist", p.Group),
				map[string]any{"group": p.Group, "found": false}), nil
		}
		return tools.Result{}, err
	}
	// Only the pipeline's topics: a group can read topics of other pipelines.
	parts := lag.Partitions
	other := 0
	if !sc.Unscoped() {
		parts = nil
		var total int64
		for _, pl := range lag.Partitions {
			if sc.TopicInScope(pl.Topic) {
				parts = append(parts, pl)
				total += pl.Lag
			} else {
				other++
			}
		}
		lag.TotalLag = total
		var kept []kafka.LagAnomaly
		for _, a := range anomalies {
			if sc.TopicInScope(a.Topic) {
				kept = append(kept, a)
			} else {
				other++
			}
		}
		anomalies = kept
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Lag > parts[j].Lag })
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s: %s, %d members, total lag %d", lag.Group, lag.State, lag.Members, lag.TotalLag)
	worst := parts
	if len(worst) > 5 {
		worst = worst[:5]
	}
	for _, pl := range worst {
		fmt.Fprintf(&sb, "\n  %s/%d: committed %d, end %d, lag %d", pl.Topic, pl.Partition, pl.Committed, pl.End, pl.Lag)
	}
	for _, a := range anomalies {
		appendLine(&sb, "  "+anomalyLine(a))
	}
	if line := tools.NotShown(other, "topic partition", "topic partitions"); line != "" {
		appendLine(&sb, line+"; the total lag above counts only this pipeline's partitions")
	}
	data := map[string]any{
		"group": lag.Group, "state": lag.State, "members": lag.Members,
		"total_lag": lag.TotalLag, "worst_partitions": worst,
	}
	if len(anomalies) > 0 {
		data["anomalies"] = anomalies
	}
	return res(sb.String(), data), nil
}

// lagReporter is the local adapter for GroupLagReport until
// clients.PartitionLag can carry commit anomalies (contract change request):
// the real client implements it; other KafkaAdmins get plain GroupLag.
type lagReporter interface {
	GroupLagReport(ctx context.Context, group string) (kafka.LagReport, error)
}

func groupLag(ctx context.Context, k clients.KafkaAdmin, group string) (clients.GroupLag, []kafka.LagAnomaly, error) {
	if r, ok := k.(lagReporter); ok {
		rep, err := r.GroupLagReport(ctx, group)
		if err != nil {
			return clients.GroupLag{}, nil, err
		}
		return rep.GroupLag, rep.Anomalies, nil
	}
	lag, err := k.GroupLag(ctx, group)
	return lag, nil, err
}

// anomalyLine explains one suspect commit in a sentence the agent can act on.
func anomalyLine(a kafka.LagAnomaly) string {
	where := fmt.Sprintf("%s/%d", a.Topic, a.Partition)
	switch a.Kind {
	case kafka.AnomalyTopicMissing:
		return fmt.Sprintf("WARNING %s: committed %d on a topic/partition that no longer exists (deleted or recreated); not counted in the lag", where, a.Committed)
	case kafka.AnomalyCommitBeyondEnd:
		return fmt.Sprintf("WARNING %s: committed %d is past the log end %d (topic recreated or truncated?); the lag of 0 shown for it is not real", where, a.Committed, a.End)
	case kafka.AnomalyCommitBelowLogStart:
		return fmt.Sprintf("WARNING %s: committed %d is below the log start %d: retention deleted %d unread records; the consumer will reset per auto.offset.reset", where, a.Committed, a.Start, a.Start-a.Committed)
	}
	return fmt.Sprintf("WARNING %s: offsets unavailable, not counted in the lag", where)
}

// ---- kafka_find_by_key ----

type findByKey struct{ d Deps }

func (t *findByKey) Name() string     { return "kafka_find_by_key" }
func (t *findByKey) Kind() tools.Kind { return tools.KindRead }
func (t *findByKey) InputSchema() map[string]any {
	return objectSchema(map[string]any{
		"topic":    strProp("topic name"),
		"key":      strProp("key to find (string form)"),
		"since":    strProp("RFC 3339 timestamp; scan from here"),
		"max_scan": map[string]any{"type": "integer", "description": "max records per partition (default 5000)"},
	}, "topic", "key", "since")
}
func (t *findByKey) Description() string {
	return "Finds records whose decoded key matches, scanning from the first offset at or after 'since'. Returns where each was found."
}

func (t *findByKey) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Topic   string `json:"topic"`
		Key     string `json:"key"`
		Since   string `json:"since"`
		MaxScan int    `json:"max_scan"`
	}
	if err := json.Unmarshal(input, &p); err != nil || p.Topic == "" || p.Key == "" || p.Since == "" {
		return tools.Result{}, fmt.Errorf("%w: topic, key and since are required", tools.ErrBadInput)
	}
	since, err := time.Parse(time.RFC3339, p.Since)
	if err != nil {
		return tools.Result{}, fmt.Errorf("%w: since must be RFC 3339: %v", tools.ErrBadInput, err)
	}
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	if err := sc.CheckTopic(p.Topic); err != nil {
		return tools.Result{}, err
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	refs, err := t.d.Kafka.FindByKey(cctx, p.Topic, p.Key, since, p.MaxScan)
	if errors.Is(err, clients.ErrNotFound) {
		return res(fmt.Sprintf("not found: topic %q does not exist", p.Topic),
			map[string]any{"topic": p.Topic, "key": p.Key, "found": false, "topic_exists": false}), nil
	}
	if err != nil {
		return tools.Result{}, err
	}
	// Oldest first, so the newest occurrence (latest update or tombstone) is
	// the one marked and, when the list is long, the one kept.
	sortRefsOldestFirst(refs)
	var sb strings.Builder
	if len(refs) == 0 {
		fmt.Fprintf(&sb, "not found: no record with key %q since %s", p.Key, p.Since)
	} else {
		fmt.Fprintf(&sb, "found %d record(s) with key %q, oldest first", len(refs), p.Key)
		shown := refs
		if len(shown) > maxRefsShown {
			fmt.Fprintf(&sb, "\n  (%d older not shown)", len(shown)-maxRefsShown)
			shown = shown[len(shown)-maxRefsShown:]
		}
		for i, r := range shown {
			fmt.Fprintf(&sb, "\n  partition %d, offset %d, %s", r.Partition, r.Offset, r.Timestamp.Format(time.RFC3339))
			if i == len(shown)-1 {
				sb.WriteString(" (newest)")
			}
		}
	}
	data := map[string]any{"topic": p.Topic, "key": p.Key, "found": len(refs) > 0, "records": refs}
	if len(refs) > 0 {
		data["newest"] = refs[len(refs)-1]
	}
	return res(sb.String(), data), nil
}

// maxRefsShown caps the matches listed in the summary; Data has them all.
const maxRefsShown = 10

// sortRefsOldestFirst puts the newest match last whatever order the client
// returned them in (ties: partition, then offset).
func sortRefsOldestFirst(refs []clients.RecordRef) {
	sort.SliceStable(refs, func(i, j int) bool {
		a, b := refs[i], refs[j]
		if !a.Timestamp.Equal(b.Timestamp) {
			return a.Timestamp.Before(b.Timestamp)
		}
		if a.Partition != b.Partition {
			return a.Partition < b.Partition
		}
		return a.Offset < b.Offset
	})
}

// ---- kafka_topic_config ----

// topicConfigAdmin is the local adapter for DescribeTopicConfigs until the
// method lands on clients.KafkaAdmin (contract change request in the PR): the
// real client (internal/clients/kafka) and the test fake implement it, and the
// tool reports a clear error when the injected client does not.
type topicConfigAdmin interface {
	DescribeTopicConfigs(ctx context.Context, topic string) (kafka.TopicConfig, error)
}

// topicConfig returns one pipeline topic's key configuration. The topic is
// scoped to the pipeline: "<topic_prefix>.<source_table>" (see the owners
// config), so the agent can only read topics the pipeline owns.
type topicConfig struct{ d Deps }

func (t *topicConfig) Name() string     { return "kafka_topic_config" }
func (t *topicConfig) Kind() tools.Kind { return tools.KindRead }
func (t *topicConfig) InputSchema() map[string]any {
	return objectSchema(map[string]any{
		"pipeline_id": strProp("pipeline id from owners.yaml"),
		"table":       strProp(`source table "schema.table"; optional when the pipeline has exactly one source table`),
	}, "pipeline_id")
}
func (t *topicConfig) Description() string {
	return "A pipeline topic's key configuration: retention.ms, retention.bytes, cleanup.policy, min.insync.replicas, partition count and replication factor."
}

func (t *topicConfig) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		PipelineID string `json:"pipeline_id"`
		Table      string `json:"table"`
	}
	if err := json.Unmarshal(input, &p); err != nil || p.PipelineID == "" {
		return tools.Result{}, fmt.Errorf("%w: pipeline_id is required", tools.ErrBadInput)
	}
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	if err := sc.CheckPipelineID(p.PipelineID); err != nil {
		return tools.Result{}, err
	}
	topic, err := pipelineTopic(t.d.Owners, sc.PipelineID(p.PipelineID), p.Table)
	if err != nil {
		return tools.Result{}, err
	}
	adm, ok := t.d.Kafka.(topicConfigAdmin)
	if !ok {
		return tools.Result{}, fmt.Errorf("kafkatools: kafka_topic_config: kafka client does not support DescribeTopicConfigs")
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	cfg, err := adm.DescribeTopicConfigs(cctx, topic)
	if err != nil {
		return tools.Result{}, fmt.Errorf("kafkatools: kafka_topic_config %s: %w", topic, err)
	}
	summary := fmt.Sprintf("%s: retention.ms=%s, retention.bytes=%s, cleanup.policy=%s, min.insync.replicas=%s, %d partitions, replication factor %d",
		cfg.Topic, cfg.RetentionMS, cfg.RetentionBytes, cfg.CleanupPolicy, cfg.MinInsyncReplicas, cfg.Partitions, cfg.ReplicationFactor)
	return res(summary, cfg), nil
}

// pipelineTopic resolves the topic for a pipeline's source table:
// "<topic_prefix>.<source_table>". table may be empty when the pipeline has
// exactly one source table. An unknown pipeline or a table the pipeline does
// not own is ErrBadInput (the agent's input); a missing owners config is a
// wiring fault and a plain error.
func pipelineTopic(o *owners.Owners, pipelineID, table string) (string, error) {
	if o == nil {
		return "", fmt.Errorf("kafkatools: owners not configured (wiring must pass Deps.Owners)")
	}
	p, ok := o.Pipeline(pipelineID)
	if !ok {
		return "", fmt.Errorf("%w: pipeline %q not found", tools.ErrBadInput, pipelineID)
	}
	if p.TopicPrefix == "" {
		return "", fmt.Errorf("%w: pipeline %q has no topic_prefix", tools.ErrBadInput, pipelineID)
	}
	switch {
	case table == "" && len(p.SourceTables) == 1:
		table = p.SourceTables[0]
	case table == "":
		return "", fmt.Errorf("%w: table is required: pipeline %q has %d source tables", tools.ErrBadInput, pipelineID, len(p.SourceTables))
	}
	if !slices.Contains(p.SourceTables, table) {
		return "", fmt.Errorf("%w: table %q is not a source table of pipeline %q", tools.ErrBadInput, table, pipelineID)
	}
	return p.TopicPrefix + "." + table, nil
}
