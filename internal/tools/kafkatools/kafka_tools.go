package kafkatools

import (
	"cmp"
	"context"
	"encoding/json"
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
	// An unreadable partition (clients.GroupLag.Unknown) is still read by the group.
	for _, pl := range slices.Concat(lag.Partitions, lag.Unknown) {
		if pl.Topic == topic {
			return true
		}
	}
	return false
}

// ---- kafka_consumer_lag ----

// maxUnknownShown caps the unreadable partitions listed in Data.
const maxUnknownShown = 20

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
	lag, err := t.d.Kafka.GroupLag(cctx, p.Group)
	if err != nil {
		return tools.Result{}, err
	}
	// Only the pipeline's topics: a group can read topics of other pipelines.
	parts, unknown := lag.Partitions, lag.Unknown
	other := 0
	if !sc.Unscoped() {
		parts, unknown = nil, nil
		var total int64
		for _, pl := range lag.Partitions {
			if sc.TopicInScope(pl.Topic) {
				parts = append(parts, pl)
				total += pl.Lag
			} else {
				other++
			}
		}
		for _, pl := range lag.Unknown {
			if sc.TopicInScope(pl.Topic) {
				unknown = append(unknown, pl)
			} else {
				other++
			}
		}
		lag.TotalLag = total
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Lag > parts[j].Lag })
	var sb strings.Builder
	// Unreadable partitions are not in the total: it is a lower bound then,
	// never "no lag" (clients.GroupLag.Unknown).
	total := fmt.Sprintf("total lag %d", lag.TotalLag)
	if len(unknown) > 0 {
		total = fmt.Sprintf("total lag at least %d", lag.TotalLag)
	}
	fmt.Fprintf(&sb, "%s: %s, %d members, %s", lag.Group, lag.State, lag.Members, total)
	worst := parts
	if len(worst) > 5 {
		worst = worst[:5]
	}
	for _, pl := range worst {
		committed := fmt.Sprintf("committed %d", pl.Committed)
		if pl.Committed < 0 {
			committed = "no committed offset" // kadm's -1 is not a position
		}
		fmt.Fprintf(&sb, "\n  %s/%d: %s, end %d, lag %d", pl.Topic, pl.Partition, committed, pl.End, pl.Lag)
	}
	if len(unknown) > 0 {
		names := make([]string, 0, min(len(unknown), 5))
		for _, pl := range unknown[:min(len(unknown), 5)] {
			names = append(names, fmt.Sprintf("%s/%d", pl.Topic, pl.Partition))
		}
		more := ""
		if len(unknown) > len(names) {
			more = fmt.Sprintf(" and %d more", len(unknown)-len(names))
		}
		fmt.Fprintf(&sb, "\n  lag unknown for %d partition(s) (committed or end offset could not be read, e.g. a broker down): %s%s",
			len(unknown), strings.Join(names, ", "), more)
	}
	if line := tools.NotShown(other, "topic partition", "topic partitions"); line != "" {
		appendLine(&sb, line+"; the total lag above counts only this pipeline's partitions")
	}
	data := map[string]any{
		"group": lag.Group, "state": lag.State, "members": lag.Members,
		"total_lag": lag.TotalLag, "worst_partitions": worst,
	}
	if len(unknown) > 0 {
		// Bounded like worst_partitions: a large group can lose every
		// partition in an outage; the count keeps the total.
		data["unknown_partitions"] = unknown[:min(len(unknown), maxUnknownShown)]
		data["unknown_count"] = len(unknown)
	}
	return res(sb.String(), data), nil
}

// ---- kafka_find_by_key ----

const (
	findDefaultMaxScan = 5000
	// findMaxScan is the client's cap too: the scan runs under the tool's
	// 10 s deadline, and a larger window rarely finishes inside it.
	findMaxScan = 20000
)

type findByKey struct{ d Deps }

func (t *findByKey) Name() string     { return "kafka_find_by_key" }
func (t *findByKey) Kind() tools.Kind { return tools.KindRead }
func (t *findByKey) InputSchema() map[string]any {
	return objectSchema(map[string]any{
		"topic": strProp("topic name"),
		"key":   strProp("key to find (string form)"),
		"since": strProp("RFC 3339 timestamp; scan from here"),
		// The range is in the description: strict tool use rejects
		// minimum/maximum, so findBounds enforces it.
		"max_scan": map[string]any{
			"type":        "integer",
			"description": "max records per partition, 1 to 20000; default 5000",
		},
	}, "topic", "key", "since")
}

// findBounds validates the model's numbers the way dlq_peek does: max_scan
// absent means the default, below 1 is bad input, above the cap is cut to
// it. A since before the Unix epoch becomes the epoch: Kafka reads the
// negative timestamps -1 and -2 as "latest" and "earliest".
func findBounds(maxScan *int, since time.Time) (int, time.Time, error) {
	if since.Before(time.Unix(0, 0)) {
		since = time.Unix(0, 0).UTC()
	}
	if maxScan == nil {
		return findDefaultMaxScan, since, nil
	}
	if *maxScan < 1 {
		return 0, since, fmt.Errorf("%w: max_scan must be between 1 and %d", tools.ErrBadInput, findMaxScan)
	}
	return min(*maxScan, findMaxScan), since, nil
}

func (t *findByKey) Description() string {
	return "Finds records whose decoded key matches, scanning from the first offset at or after 'since'. Returns where each was found."
}

func (t *findByKey) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Topic   string `json:"topic"`
		Key     string `json:"key"`
		Since   string `json:"since"`
		MaxScan *int   `json:"max_scan"`
	}
	if err := json.Unmarshal(input, &p); err != nil || p.Topic == "" || p.Key == "" || p.Since == "" {
		return tools.Result{}, fmt.Errorf("%w: topic, key and since are required", tools.ErrBadInput)
	}
	since, err := time.Parse(time.RFC3339, p.Since)
	if err != nil {
		return tools.Result{}, fmt.Errorf("%w: since must be RFC 3339: %v", tools.ErrBadInput, err)
	}
	maxScan, since, err := findBounds(p.MaxScan, since)
	if err != nil {
		return tools.Result{}, err
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
	scan, reported, err := findKey(cctx, t.d.Kafka, p.Topic, p.Key, since, maxScan)
	if err != nil {
		return tools.Result{}, err
	}
	refs := scan.Refs
	var sb strings.Builder
	switch {
	case len(refs) == 0 && !reported:
		fmt.Fprintf(&sb, "no record with key %q found since %s in the scanned window", p.Key, p.Since)
	case len(refs) == 0 && len(scan.Truncated) > 0:
		fmt.Fprintf(&sb, "not found in the scanned window: no record with key %q since %s", p.Key, p.Since)
	case len(refs) == 0:
		fmt.Fprintf(&sb, "not found: no record with key %q since %s", p.Key, p.Since)
	default:
		fmt.Fprintf(&sb, "found %d record(s) with key %q", len(refs), p.Key)
	}
	data := map[string]any{"topic": p.Topic, "key": p.Key, "found": len(refs) > 0, "records": refs}
	switch {
	case !reported:
		// clients.KeyScanReader: without it a cut scan looks complete, so
		// neither absence nor the newest match is stated as fact, and
		// "truncated" is left out rather than claimed false.
		newer := "newer "
		if len(refs) == 0 {
			newer = ""
		}
		fmt.Fprintf(&sb, "\nthis client cannot report whether the scan was cut (max_scan %d or the time limit): %smatches may exist past the scanned window", maxScan, newer)
		data["truncation_unknown"] = true
	case len(scan.Truncated) > 0:
		// Matches past the cut would be newer than any listed: say so before
		// the list can be read as complete.
		fmt.Fprintf(&sb, "\nthe scan stopped (max_scan %d or the time limit) before the end of partition(s) %v: newer matches may exist there; use a later since or a larger max_scan", maxScan, scan.Truncated)
		data["truncated"] = true
		data["truncated_partitions"] = scan.Truncated
	default:
		data["truncated"] = false
	}
	// The list goes after the notes, so a long one cannot push them past
	// MaxSummaryBytes.
	writeNewestRefs(&sb, refs)
	return res(sb.String(), data), nil
}

// findMaxListed caps the matches kafka_find_by_key lists in Summary: about 45
// bytes each, so the list stays well inside tools.MaxSummaryBytes. Data keeps
// every match.
const findMaxListed = 100

// writeNewestRefs lists at most findMaxListed matches, newest first: a hot
// CDC key can match hundreds of records, and the newest are the ones an
// investigation needs. A last line counts the older ones left out.
func writeNewestRefs(sb *strings.Builder, refs []clients.RecordRef) {
	byAge := slices.Clone(refs)
	slices.SortStableFunc(byAge, func(a, b clients.RecordRef) int {
		if c := b.Timestamp.Compare(a.Timestamp); c != 0 {
			return c
		}
		if a.Partition != b.Partition {
			return cmp.Compare(a.Partition, b.Partition)
		}
		return cmp.Compare(b.Offset, a.Offset)
	})
	for _, r := range byAge[:min(len(byAge), findMaxListed)] {
		fmt.Fprintf(sb, "\n  partition %d, offset %d, %s", r.Partition, r.Offset, r.Timestamp.Format(time.RFC3339))
	}
	if n := len(byAge) - findMaxListed; n > 0 {
		fmt.Fprintf(sb, "\n%d older match(es) not listed; Data.records has them all", n)
	}
}

// findKey uses clients.KeyScanReader when the client has it; reported is
// false for any other client, whose FindByKey result may be truncated.
func findKey(ctx context.Context, k clients.KafkaReader, topic, key string, since time.Time, maxScan int) (scan clients.KeyScan, reported bool, err error) {
	if ks, ok := k.(clients.KeyScanReader); ok {
		scan, err = ks.FindByKeyScan(ctx, topic, key, since, maxScan)
		return scan, true, err
	}
	refs, err := k.FindByKey(ctx, topic, key, since, maxScan)
	return clients.KeyScan{Refs: refs}, false, err
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
