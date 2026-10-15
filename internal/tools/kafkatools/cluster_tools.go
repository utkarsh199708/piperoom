package kafkatools

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/tools"
)

// diskWarnPct is the used-space percentage from which a broker's log dir is
// named in the summary line; below it the broker is only counted.
const diskWarnPct = 85

// maxBadPartitions bounds how many unhealthy partitions one topic line spells out.
const maxBadPartitions = 5

// ---- kafka_cluster_health ----

type clusterHealth struct{ d Deps }

func (t *clusterHealth) Name() string     { return "kafka_cluster_health" }
func (t *clusterHealth) Kind() tools.Kind { return tools.KindRead }
func (t *clusterHealth) InputSchema() map[string]any {
	return objectSchema(map[string]any{})
}
func (t *clusterHealth) Description() string {
	return "Broker and partition health for the pipeline's topics: brokers, controller, log-dir disk usage, " +
		"and per partition the leader, ISR vs replicas, min.insync.replicas, offline replicas and reassignments. " +
		"Use it for NotEnoughReplicas, a full broker disk, or an unavailable partition."
}

func (t *clusterHealth) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	if _, err := parse[struct{}](input); err != nil {
		return tools.Result{}, err
	}
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	topics := append(sc.Topics(), sc.DLQTopics()...)
	if len(topics) == 0 {
		return tools.Result{}, fmt.Errorf("%w: the room's pipeline has no topics (an unscoped call cannot read cluster health)", tools.ErrBadInput)
	}
	cr, ok := t.d.Kafka.(clients.ClusterReader)
	if !ok {
		return tools.Result{}, fmt.Errorf("kafkatools: kafka_cluster_health: %w", clients.ErrNotSupported)
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	h, err := cr.ClusterHealth(cctx, topics...)
	if err != nil {
		return tools.Result{}, fmt.Errorf("kafkatools: kafka_cluster_health: %w", err)
	}
	return res(clusterSummary(h, topics), h), nil
}

// clusterSummary renders the health snapshot for the model. topics is the
// requested set, so a topic the cluster does not know is named.
func clusterSummary(h clients.ClusterHealth, topics []string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "cluster %s (%d brokers, controller %d)", orUnknown(h.ClusterID), len(h.Brokers), h.Controller)
	var notPermitted, other int
	for _, b := range h.Brokers {
		switch {
		case b.LogDirErr == kafka.LogDirNotPermitted:
			notPermitted++
		case b.LogDirErr != "":
			other++
		case b.LogDirTotalBytes > 0 && b.LogDirUsableBytes >= 0:
			used := 100 - b.LogDirUsableBytes*100/b.LogDirTotalBytes
			if used >= diskWarnPct {
				fmt.Fprintf(&sb, "; broker %d (%s) log dir %d%% used (%s free of %s)",
					b.NodeID, b.Host, used, humanBytes(b.LogDirUsableBytes), humanBytes(b.LogDirTotalBytes))
			}
		}
	}
	switch {
	case notPermitted > 0:
		sb.WriteString("; log dirs: not permitted (needs DescribeConfigs on Cluster, docs/pilot/permissions.md)")
	case other > 0:
		sb.WriteString("; log dirs: unavailable")
	default:
		sb.WriteString("; log dirs below " + fmt.Sprint(diskWarnPct) + "% used")
	}

	byTopic := map[string][]clients.PartitionHealth{}
	for _, p := range h.Partitions {
		byTopic[p.Topic] = append(byTopic[p.Topic], p)
	}
	var verdicts []string
	for _, topic := range topics {
		parts, ok := byTopic[topic]
		if !ok {
			appendLine(&sb, topic+": not found on the cluster")
			continue
		}
		var under, belowMin, offline, reassigning int
		var bad []string
		for _, p := range parts {
			isUnder := len(p.ISR) < len(p.Replicas)
			isBelow := p.MinISR > 0 && len(p.ISR) < p.MinISR
			isOffline := p.Leader < 0 || len(p.OfflineReplicas) > 0
			if isUnder {
				under++
			}
			if isBelow {
				belowMin++
				verdicts = append(verdicts, fmt.Sprintf("p%d", p.Partition))
			}
			if isOffline {
				offline++
			}
			if p.Reassigning {
				reassigning++
			}
			if (isUnder || isBelow || isOffline) && len(bad) < maxBadPartitions {
				bad = append(bad, fmt.Sprintf("p%d ISR %s of %s, leader %s", p.Partition, ints(p.ISR), ints(p.Replicas), leader(p.Leader)))
			}
		}
		minISR := 0
		if len(parts) > 0 {
			minISR = parts[0].MinISR
		}
		line := fmt.Sprintf("%s: %d partitions, %d under-replicated, %d under min ISR", topic, len(parts), under, belowMin)
		if minISR > 0 {
			line += fmt.Sprintf(" (min %d)", minISR)
		} else {
			line += " (min.insync.replicas unknown)"
		}
		line += fmt.Sprintf(", %d offline, %d reassigning", offline, reassigning)
		if len(bad) > 0 {
			line += " [" + strings.Join(bad, "; ") + "]"
		}
		appendLine(&sb, line)
	}
	if len(verdicts) > 0 {
		slices.Sort(verdicts)
		appendLine(&sb, "NotEnoughReplicas is expected for writes to "+strings.Join(slices.Compact(verdicts), ", "))
	}
	return sb.String()
}

func orUnknown(s string) string {
	if s == "" {
		return "(id unknown)"
	}
	return s
}

func leader(n int32) string {
	if n < 0 {
		return "none"
	}
	return fmt.Sprint(n)
}

func ints(v []int32) string {
	parts := make([]string, len(v))
	for i, n := range v {
		parts[i] = fmt.Sprint(n)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func humanBytes(n int64) string {
	const unit = 1000
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "kMGTPE"[exp])
}
