package kafkatools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"piperoom/internal/clients"
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

func (t *topicOffsets) Run(_ context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Topic string `json:"topic"`
	}
	if err := json.Unmarshal(input, &p); err != nil || p.Topic == "" {
		return tools.Result{}, fmt.Errorf("%w: topic is required", tools.ErrBadInput)
	}
	cctx, cancel := ctx()
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
	return "Lists consumer groups with their state; optionally only those reading a given topic."
}

func (t *listGroups) Run(_ context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Topic string `json:"topic"`
	}
	if err := json.Unmarshal(input, &p); err != nil {
		return tools.Result{}, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	cctx, cancel := ctx()
	defer cancel()
	groups, err := t.d.Kafka.ConsumerGroups(cctx)
	if err != nil {
		return tools.Result{}, err
	}
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

func (t *consumerLag) Run(_ context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Group string `json:"group"`
	}
	if err := json.Unmarshal(input, &p); err != nil || p.Group == "" {
		return tools.Result{}, fmt.Errorf("%w: group is required", tools.ErrBadInput)
	}
	cctx, cancel := ctx()
	defer cancel()
	lag, err := t.d.Kafka.GroupLag(cctx, p.Group)
	if err != nil {
		return tools.Result{}, err
	}
	parts := lag.Partitions
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
	return res(sb.String(), map[string]any{
		"group": lag.Group, "state": lag.State, "members": lag.Members,
		"total_lag": lag.TotalLag, "worst_partitions": worst,
	}), nil
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

func (t *findByKey) Run(_ context.Context, input json.RawMessage) (tools.Result, error) {
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
	cctx, cancel := ctx()
	defer cancel()
	refs, err := t.d.Kafka.FindByKey(cctx, p.Topic, p.Key, since, p.MaxScan)
	if err != nil {
		return tools.Result{}, err
	}
	var sb strings.Builder
	if len(refs) == 0 {
		fmt.Fprintf(&sb, "not found: no record with key %q since %s", p.Key, p.Since)
	} else {
		fmt.Fprintf(&sb, "found %d record(s) with key %q", len(refs), p.Key)
		for _, r := range refs {
			fmt.Fprintf(&sb, "\n  partition %d, offset %d, %s", r.Partition, r.Offset, r.Timestamp.Format(time.RFC3339))
		}
	}
	return res(sb.String(), map[string]any{"topic": p.Topic, "key": p.Key, "found": len(refs) > 0, "records": refs}), nil
}
