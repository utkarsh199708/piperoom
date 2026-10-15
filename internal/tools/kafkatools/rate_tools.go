package kafkatools

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

const (
	defaultRateWindow = 5 * time.Minute
	minRateWindow     = time.Minute
	maxRateWindow     = time.Hour
	// skewRatio is max/median records per partition from which the summary says "skewed".
	skewRatio = 3.0
	// maxRatePartitions bounds how many partitions the summary lists.
	maxRatePartitions = 5
)

// ---- kafka_partition_rates ----

type partitionRates struct{ d Deps }

func (t *partitionRates) Name() string     { return "kafka_partition_rates" }
func (t *partitionRates) Kind() tools.Kind { return tools.KindRead }
func (t *partitionRates) InputSchema() map[string]any {
	return objectSchema(map[string]any{
		"table":  strProp(`source table "schema.table"; optional when the pipeline has exactly one source table`),
		"window": strProp(`look-back window as a Go duration, 1m to 1h (default "5m")`),
	})
}
func (t *partitionRates) Description() string {
	return "Records written per partition of a pipeline topic in the last few minutes (from offsets only, no record is read): " +
		"throughput, share per partition and skew. Use it to see a hot partition or a topic that stopped receiving writes."
}

func (t *partitionRates) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	p, err := parse[struct {
		Table  string `json:"table"`
		Window string `json:"window"`
	}](input)
	if err != nil {
		return tools.Result{}, err
	}
	window := defaultRateWindow
	if p.Window != "" {
		if window, err = time.ParseDuration(p.Window); err != nil || window < minRateWindow || window > maxRateWindow {
			return tools.Result{}, fmt.Errorf("%w: window must be a duration between 1m and 1h, e.g. \"5m\"", tools.ErrBadInput)
		}
	}
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	pl := sc.Pipeline()
	table := p.Table
	switch {
	case sc.Unscoped() || pl.TopicPrefix == "":
		return tools.Result{}, fmt.Errorf("%w: the room's pipeline has no topic_prefix (an unscoped call cannot resolve a topic)", tools.ErrBadInput)
	case table == "" && len(pl.SourceTables) == 1:
		table = pl.SourceTables[0]
	case table == "":
		return tools.Result{}, fmt.Errorf("%w: table is required: the pipeline has %d source tables", tools.ErrBadInput, len(pl.SourceTables))
	}
	if err := sc.CheckSourceTable(table); err != nil {
		return tools.Result{}, err
	}
	topic := pl.TopicPrefix + "." + table
	rr, ok := t.d.Kafka.(clients.RateReader)
	if !ok {
		return tools.Result{}, fmt.Errorf("kafkatools: kafka_partition_rates: %w", clients.ErrNotSupported)
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	rates, err := rr.PartitionRates(cctx, time.Now().Add(-window), topic)
	if err != nil {
		return tools.Result{}, fmt.Errorf("kafkatools: kafka_partition_rates %s: %w", topic, err)
	}
	if len(rates) == 0 {
		return tools.Result{}, fmt.Errorf("kafkatools: kafka_partition_rates %s: %w", topic, clients.ErrNotFound)
	}
	return res(rateSummary(topic, window, rates), map[string]any{
		"topic": topic, "window_seconds": int(window.Seconds()), "partitions": rates,
	}), nil
}

// rateSummary renders per-partition counts, the share of the busiest ones and
// a skew verdict (max against the median partition).
func rateSummary(topic string, window time.Duration, rates []clients.PartitionRate) string {
	var total int64
	counts := make([]int64, len(rates))
	for i, r := range rates {
		total += r.Records
		counts[i] = r.Records
	}
	sorted := slices.Clone(counts)
	slices.Sort(sorted)
	median := sorted[len(sorted)/2]
	if len(sorted)%2 == 0 {
		median = (sorted[len(sorted)/2-1] + sorted[len(sorted)/2]) / 2
	}
	max := sorted[len(sorted)-1]

	secs := window.Seconds()
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s last %s: %d records (%.0f/s)", topic, shortDur(window), total, float64(total)/secs)
	if total == 0 {
		sb.WriteString("; no partition received a write in the window")
		return sb.String()
	}
	byCount := slices.Clone(rates)
	sort.SliceStable(byCount, func(i, j int) bool { return byCount[i].Records > byCount[j].Records })
	var parts []string
	for i, r := range byCount {
		if i >= maxRatePartitions {
			parts = append(parts, fmt.Sprintf("… %d more", len(byCount)-i))
			break
		}
		s := fmt.Sprintf("p%d %d", r.Partition, r.Records)
		if i == 0 {
			s += fmt.Sprintf(" (%d%%)", r.Records*100/total)
		}
		parts = append(parts, s)
	}
	fmt.Fprintf(&sb, "; %s", strings.Join(parts, ", "))
	if len(rates) > 1 {
		fmt.Fprintf(&sb, "; median %d, max %d", median, max)
		switch {
		case median == 0:
			sb.WriteString(" → skewed: most partitions received nothing")
		case float64(max)/float64(median) >= skewRatio:
			fmt.Fprintf(&sb, ", max/median %.1fx → skewed", float64(max)/float64(median))
		default:
			fmt.Fprintf(&sb, ", max/median %.1fx → even", float64(max)/float64(median))
		}
	}
	return sb.String()
}

func shortDur(d time.Duration) string {
	if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", d/time.Hour)
	}
	return fmt.Sprintf("%dm", int(d.Minutes()))
}
