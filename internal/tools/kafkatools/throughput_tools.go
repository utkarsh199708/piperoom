package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/tools"
)

// ---- kafka_throughput (X6a) ----
//
// Produce rates come from offsets by record timestamp (clients.RateReader,
// the same read as kafka_partition_rates). Kafka keeps no history of a
// group's committed offsets, so a consume rate over a past window cannot be
// read from Kafka: it comes from the watcher's group_lag samples (the
// pipeline's total lag, one sample per config.WatchSampleInterval), as
// "records written to the groups' topics less the change of the lag" between
// two samples, the measure of a performance action's Verify (X6). It is
// therefore one number for the pipeline's groups, never per partition.

const (
	defaultThroughputWindow = 15 * time.Minute
	maxThroughputWindow     = 6 * time.Hour
	// lagSkewShare is the share of a topic's lag (for one group) above which
	// one partition is a skew finding.
	lagSkewShare = 0.5
	// minSkewLag is the topic lag below which a share is noise, not a finding.
	minSkewLag = 100
	// keepUpTolerance: consumption within 10 % of production is keeping up,
	// the X6 verdict's 10 % rule (events.PerformanceOutcomeFor).
	keepUpTolerance = 0.10
	// maxThroughputRows bounds the partitions listed in Data.
	maxThroughputRows = 100
	// maxThroughputLines bounds the topic and group lines of the Summary.
	maxThroughputLines = 5
	// sampleFreshness is how old the newest group_lag sample may be: two
	// missed samples mean the watcher is off or stuck.
	sampleFreshness = 2 * config.WatchSampleInterval
	// minSampleSpan is the least time between the two samples a consume rate
	// is measured over.
	minSampleSpan = 5 * time.Minute
)

// Consumption verdicts.
const (
	verdictFallingBehind = "falling_behind"
	verdictKeepingUp     = "keeping_up"
	verdictCatchingUp    = "catching_up"
	verdictIdle          = "idle"
)

const consumeScope = "pipeline: the consumed rate is the total of the pipeline's consumer groups, from the watcher's " +
	"group_lag samples; Kafka keeps no committed-offset history, so there is no per-partition consume rate"

type throughput struct{ d Deps }

func (t *throughput) Name() string     { return "kafka_throughput" }
func (t *throughput) Kind() tools.Kind { return tools.KindRead }
func (t *throughput) InputSchema() map[string]any {
	return objectSchema(map[string]any{
		"table":  strProp(`source table "schema.table"; optional, default every topic of the pipeline`),
		"window": strProp(`look-back window as a Go duration, 1m to 6h (default "15m")`),
	})
}
func (t *throughput) Description() string {
	return "Throughput of the pipeline's topics over a window: records written per partition (from offsets, no record " +
		"is read), each consumer group's lag per partition, and from the watcher's group_lag samples the consumed rate " +
		"against the produced rate (falling behind / keeping up) and the lag trend. Flags skew: a partition above 3x " +
		"the median rate, or one partition holding over half a topic's lag. Use it to tell a slow consumer from a hot " +
		"partition or a burst of writes."
}

// tpTopic is one topic's produce side over the window.
type tpTopic struct {
	Topic           string  `json:"topic"`
	Partitions      int     `json:"partitions"`
	Records         int64   `json:"records"`
	PerSecond       float64 `json:"per_second"`
	MedianPerSecond float64 `json:"median_per_second"`
	MaxPartition    int32   `json:"max_partition"`
	MaxPerSecond    float64 `json:"max_per_second"`
	Skewed          bool    `json:"skewed"`
}

// tpPartition is one partition's produce rate over the window.
type tpPartition struct {
	Topic     string  `json:"topic"`
	Partition int32   `json:"partition"`
	Records   int64   `json:"records"`
	PerSecond float64 `json:"per_second"`
}

// tpLagShare is, for one group and topic, the partition with the most lag.
type tpLagShare struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Lag       int64  `json:"lag"`
	TopicLag  int64  `json:"topic_lag"`
	SharePct  int    `json:"share_pct"`
	Skewed    bool   `json:"skewed"`
}

// tpGroup is one consumer group's current lag on the selected topics.
type tpGroup struct {
	Group   string `json:"group"`
	State   string `json:"state,omitempty"`
	Members int    `json:"members"`
	Lag     int64  `json:"lag"`
	// LagUnknown counts selected partitions whose lag could not be read, so
	// Lag is a lower bound when it is not zero.
	LagUnknown int          `json:"lag_unknown_partitions,omitempty"`
	Hot        []tpLagShare `json:"hot,omitempty"`
	Error      string       `json:"error,omitempty"`

	// topics are every topic the group reads, any pipeline's (consume rate).
	topics []string
}

// tpLagTrend is the change of the pipeline's group_lag between two samples.
type tpLagTrend struct {
	From           time.Time `json:"from"`
	To             time.Time `json:"to"`
	LagStart       int64     `json:"lag_start"`
	LagEnd         int64     `json:"lag_end"`
	SlopePerMinute float64   `json:"slope_per_minute"`
}

// tpConsumption is produced against consumed between the trend's samples.
type tpConsumption struct {
	Groups           []string `json:"groups"`
	Produced         int64    `json:"produced"`
	Consumed         int64    `json:"consumed"`
	ProducePerSecond float64  `json:"produce_per_second"`
	ConsumePerSecond float64  `json:"consume_per_second"`
	Verdict          string   `json:"verdict"`
}

func (t *throughput) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	p, err := parse[struct {
		Table  string `json:"table"`
		Window string `json:"window"`
	}](input)
	if err != nil {
		return tools.Result{}, err
	}
	window := defaultThroughputWindow
	if p.Window != "" {
		if window, err = time.ParseDuration(p.Window); err != nil || window < minRateWindow || window > maxThroughputWindow {
			return tools.Result{}, fmt.Errorf("%w: window must be a duration between 1m and 6h, e.g. \"15m\"", tools.ErrBadInput)
		}
	}
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	pl := sc.Pipeline()
	if sc.Unscoped() || pl.TopicPrefix == "" {
		return tools.Result{}, fmt.Errorf("%w: the room's pipeline has no topic_prefix (an unscoped call cannot resolve a topic)", tools.ErrBadInput)
	}
	topics := sc.Topics()
	if p.Table != "" {
		if err := sc.CheckSourceTable(p.Table); err != nil {
			return tools.Result{}, err
		}
		topics = []string{pl.TopicPrefix + "." + p.Table}
	}
	if len(topics) == 0 {
		return tools.Result{}, fmt.Errorf("%w: the room's pipeline has no source table", tools.ErrBadInput)
	}
	// The groups the watcher samples group_lag over (owners.yaml's declared
	// groups), so the lag read here and the samples are the same total.
	groups := pl.ConsumerGroups
	if t.d.Kafka == nil {
		return tools.Result{}, fmt.Errorf("kafkatools: kafka_throughput: no Kafka client: %w", clients.ErrNotSupported)
	}

	cctx, cancel := withTimeout(ctx)
	defer cancel()
	now := time.Now()
	data := map[string]any{"window_seconds": int(window.Seconds()), "consume_scope": consumeScope}

	rr, hasRates := t.d.Kafka.(clients.RateReader)
	var rates []clients.PartitionRate
	if hasRates {
		rates, err = rr.PartitionRates(cctx, now.Add(-window), topics...)
		switch {
		case errors.Is(err, clients.ErrNotSupported):
			hasRates = false
		case err != nil:
			return tools.Result{}, fmt.Errorf("kafkatools: kafka_throughput rates: %w", err)
		}
	}
	data["rates_available"] = hasRates

	gs := make([]tpGroup, 0, len(groups))
	for _, g := range groups {
		gl, err := t.d.Kafka.GroupLag(cctx, g)
		if err != nil {
			if cctx.Err() != nil {
				return tools.Result{}, fmt.Errorf("kafkatools: kafka_throughput lag of %s: %w", g, err)
			}
			gs = append(gs, tpGroup{Group: g, Error: err.Error()})
			continue
		}
		gs = append(gs, groupThroughput(g, gl, topics))
	}
	data["groups"] = gs

	var tps []tpTopic
	if hasRates {
		var parts []tpPartition
		tps, parts = produceSide(rates, topics, window)
		data["topics"] = tps
		data["partitions"], data["partitions_omitted"] = capRows(parts)
	}

	trend, cons, note, err := t.consumption(cctx, pl.ID, now, window, gs, rr, hasRates, sc)
	if err != nil {
		return tools.Result{}, err
	}
	if trend != nil {
		data["lag_trend"] = trend
	}
	if cons != nil {
		data["consumption"] = cons
	}
	if note != "" {
		data["consume_note"] = note
	}
	return res(throughputSummary(pl.ID, window, hasRates, tps, gs, trend, cons, note), data), nil
}

// groupThroughput keeps the group's partitions of the selected topics and
// finds, per topic, the partition with the largest share of the lag.
func groupThroughput(g string, gl clients.GroupLag, topics []string) tpGroup {
	out := tpGroup{Group: g, State: gl.State, Members: gl.Members}
	byTopic := map[string][]clients.PartitionLag{}
	seen := map[string]bool{}
	for _, pl := range gl.Partitions {
		if !seen[pl.Topic] {
			seen[pl.Topic] = true
			out.topics = append(out.topics, pl.Topic)
		}
		if slices.Contains(topics, pl.Topic) {
			byTopic[pl.Topic] = append(byTopic[pl.Topic], pl)
			out.Lag += pl.Lag
		}
	}
	for _, pl := range gl.Unknown {
		if !seen[pl.Topic] {
			seen[pl.Topic] = true
			out.topics = append(out.topics, pl.Topic)
		}
		if slices.Contains(topics, pl.Topic) {
			out.LagUnknown++
		}
	}
	for _, topic := range topics {
		parts := byTopic[topic]
		var total int64
		hot := -1
		for i, pl := range parts {
			total += pl.Lag
			if hot < 0 || pl.Lag > parts[hot].Lag {
				hot = i
			}
		}
		if total <= 0 {
			continue
		}
		share := float64(parts[hot].Lag) / float64(total)
		out.Hot = append(out.Hot, tpLagShare{
			Topic: topic, Partition: parts[hot].Partition, Lag: parts[hot].Lag, TopicLag: total,
			SharePct: int(share * 100),
			Skewed:   len(parts) > 1 && total >= minSkewLag && share > lagSkewShare,
		})
	}
	sort.SliceStable(out.Hot, func(i, j int) bool { return out.Hot[i].TopicLag > out.Hot[j].TopicLag })
	return out
}

// produceSide sums the window's records per topic (in the order of topics)
// and lists every partition, busiest first.
func produceSide(rates []clients.PartitionRate, topics []string, window time.Duration) ([]tpTopic, []tpPartition) {
	secs := window.Seconds()
	byTopic := map[string][]clients.PartitionRate{}
	parts := make([]tpPartition, 0, len(rates))
	for _, r := range rates {
		byTopic[r.Topic] = append(byTopic[r.Topic], r)
		parts = append(parts, tpPartition{Topic: r.Topic, Partition: r.Partition, Records: r.Records, PerSecond: float64(r.Records) / secs})
	}
	sort.SliceStable(parts, func(i, j int) bool { return parts[i].Records > parts[j].Records })
	var out []tpTopic
	for _, topic := range topics {
		rs := byTopic[topic]
		if len(rs) == 0 {
			continue
		}
		tt := tpTopic{Topic: topic, Partitions: len(rs)}
		counts := make([]int64, len(rs))
		hot := 0
		for i, r := range rs {
			tt.Records += r.Records
			counts[i] = r.Records
			if r.Records > rs[hot].Records {
				hot = i
			}
		}
		tt.MaxPartition, tt.MaxPerSecond = rs[hot].Partition, float64(rs[hot].Records)/secs
		slices.Sort(counts)
		median := float64(counts[len(counts)/2])
		if len(counts)%2 == 0 {
			median = float64(counts[len(counts)/2-1]+counts[len(counts)/2]) / 2
		}
		max := float64(counts[len(counts)-1])
		tt.PerSecond = float64(tt.Records) / secs
		tt.MedianPerSecond = median / secs
		tt.Skewed = len(rs) > 1 && max > 0 && (median == 0 || max > skewRatio*median)
		out = append(out, tt)
	}
	return out, parts
}

// capRows bounds the partition list of Data and returns how many it left out.
func capRows(parts []tpPartition) ([]tpPartition, int) {
	if len(parts) <= maxThroughputRows {
		return parts, 0
	}
	return parts[:maxThroughputRows], len(parts) - maxThroughputRows
}

// consumption reads the pipeline's group_lag samples for the lag trend and,
// when rates are available and every group reads only this pipeline's
// topics, the consumed records between the two samples. A reason it could
// not measure comes back as note; only an error of the rate read (the
// samples answered, so the clients should) or the deadline is an error.
func (t *throughput) consumption(ctx context.Context, pid string, now time.Time, window time.Duration,
	gs []tpGroup, rr clients.RateReader, hasRates bool, sc tools.Scope) (*tpLagTrend, *tpConsumption, string, error) {
	if len(gs) == 0 {
		return nil, nil, "no consumed rate: the pipeline declares no consumer group", nil
	}
	if t.d.Samples == nil {
		return nil, nil, "no consumed rate or lag trend: Kafka keeps no committed-offset history and this process has " +
			"no group_lag watch samples (the watcher does not run here)", nil
	}
	pts, err := t.d.Samples.Samples(ctx, pid, clients.WatchMetricGroupLag, now.Add(-window-config.WatchSampleInterval))
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, "", fmt.Errorf("kafkatools: kafka_throughput group_lag samples: %w", err)
		}
		return nil, nil, fmt.Sprintf("no consumed rate or lag trend: group_lag samples not read: %v", err), nil
	}
	s0, s1, why := pickSamples(pts, now, window)
	if why != "" {
		return nil, nil, "no consumed rate or lag trend: " + why, nil
	}
	span := s1.At.Sub(s0.At)
	trend := &tpLagTrend{
		From: s0.At.UTC(), To: s1.At.UTC(), LagStart: int64(s0.Value), LagEnd: int64(s1.Value),
		SlopePerMinute: (s1.Value - s0.Value) / span.Minutes(),
	}
	if !hasRates {
		return trend, nil, "no consumed rate: produce rates are not available from this cluster", nil
	}
	// Records to consume between the samples: each group's topics, counted
	// once per group, because group_lag sums every group's whole lag.
	var names, uniq []string
	perGroup := map[string]int{}
	for _, g := range gs {
		if g.Error != "" {
			return trend, nil, fmt.Sprintf("no consumed rate: the lag of group %s was not read", g.Group), nil
		}
		for _, topic := range g.topics {
			if !sc.TopicInScope(topic) {
				return trend, nil, fmt.Sprintf("no consumed rate: group %s also reads topics outside this pipeline, "+
					"which its group_lag samples include", g.Group), nil
			}
			if perGroup[topic] == 0 {
				uniq = append(uniq, topic)
			}
			perGroup[topic]++
		}
		names = append(names, g.Group)
	}
	cons := &tpConsumption{Groups: names}
	if len(uniq) > 0 {
		from, err := rr.PartitionRates(ctx, s0.At, uniq...)
		if err != nil {
			return nil, nil, "", fmt.Errorf("kafkatools: kafka_throughput rates since %s: %w", s0.At.UTC().Format(time.RFC3339), err)
		}
		to, err := rr.PartitionRates(ctx, s1.At, uniq...)
		if err != nil {
			return nil, nil, "", fmt.Errorf("kafkatools: kafka_throughput rates since %s: %w", s1.At.UTC().Format(time.RFC3339), err)
		}
		for _, r := range from {
			cons.Produced += r.Records * int64(perGroup[r.Topic])
		}
		for _, r := range to {
			cons.Produced -= r.Records * int64(perGroup[r.Topic])
		}
		cons.Produced = max(cons.Produced, 0)
	}
	cons.Consumed = cons.Produced - (trend.LagEnd - trend.LagStart)
	if cons.Consumed < 0 {
		return trend, nil, "no consumed rate: the lag grew by more than the records written between the samples " +
			"(the groups or their topics changed)", nil
	}
	cons.ProducePerSecond = float64(cons.Produced) / span.Seconds()
	cons.ConsumePerSecond = float64(cons.Consumed) / span.Seconds()
	cons.Verdict = consumeVerdict(cons.Produced, cons.Consumed)
	return trend, cons, "", nil
}

// pickSamples chooses the newest sample and the one closest to the window's
// start at least minSampleSpan before it, or says why it cannot.
func pickSamples(pts []clients.SamplePoint, now time.Time, window time.Duration) (s0, s1 clients.SamplePoint, why string) {
	if len(pts) < 2 {
		return s0, s1, fmt.Sprintf("%d group_lag sample(s) in the window, 2 needed (the watcher samples every %s)",
			len(pts), shortDur(config.WatchSampleInterval))
	}
	s1 = pts[len(pts)-1]
	if age := now.Sub(s1.At); age > sampleFreshness {
		return s0, s1, fmt.Sprintf("the newest group_lag sample is %s old (the watcher is off or stuck)", age.Round(time.Minute))
	}
	target := now.Add(-window)
	best := -1
	dist := func(i int) time.Duration { return max(pts[i].At.Sub(target), target.Sub(pts[i].At)) }
	for i := range pts[:len(pts)-1] {
		if s1.At.Sub(pts[i].At) < minSampleSpan {
			continue
		}
		if best < 0 || dist(i) < dist(best) {
			best = i
		}
	}
	if best < 0 {
		return s0, s1, fmt.Sprintf("no two group_lag samples %s apart", shortDur(minSampleSpan))
	}
	return pts[best], s1, ""
}

// consumeVerdict compares consumed with produced records under the 10 % rule.
func consumeVerdict(produced, consumed int64) string {
	if produced == 0 && consumed == 0 {
		return verdictIdle
	}
	tol := keepUpTolerance * float64(produced)
	switch d := float64(produced - consumed); {
	case d > tol:
		return verdictFallingBehind
	case -d > tol:
		return verdictCatchingUp
	}
	return verdictKeepingUp
}

// fmtRate renders records per second: one decimal below 10/s.
func fmtRate(perSec float64) string {
	if perSec < 10 {
		return fmt.Sprintf("%.1f/s", perSec)
	}
	return fmt.Sprintf("%.0f/s", perSec)
}

// throughputSummary leads with the findings (the bottleneck), then a line per
// topic, per group and for the samples.
func throughputSummary(pid string, window time.Duration, hasRates bool, tps []tpTopic, gs []tpGroup,
	trend *tpLagTrend, cons *tpConsumption, note string) string {
	var findings []string
	if cons != nil {
		vs := fmt.Sprintf("consumer rate %s vs produce %s", fmtRate(cons.ConsumePerSecond), fmtRate(cons.ProducePerSecond))
		switch cons.Verdict {
		case verdictFallingBehind:
			findings = append(findings, fmt.Sprintf("falling behind at %s: %s", fmtRate(cons.ProducePerSecond-cons.ConsumePerSecond), vs))
		case verdictCatchingUp:
			findings = append(findings, fmt.Sprintf("catching up at %s: %s", fmtRate(cons.ConsumePerSecond-cons.ProducePerSecond), vs))
		case verdictKeepingUp:
			findings = append(findings, "keeping up: "+vs)
		default:
			findings = append(findings, "idle: nothing written or consumed between the group_lag samples")
		}
	} else if trend != nil && trend.LagEnd != trend.LagStart {
		dir := "growing"
		if trend.LagEnd < trend.LagStart {
			dir = "shrinking"
		}
		findings = append(findings, fmt.Sprintf("lag %s %.0f/min", dir, math.Abs(trend.SlopePerMinute)))
	}
	for _, g := range gs {
		for _, h := range g.Hot {
			if h.Skewed {
				findings = append(findings, fmt.Sprintf("%s p%d holds %d%% of %s's lag", h.Topic, h.Partition, h.SharePct, g.Group))
			}
		}
	}
	for _, tt := range tps {
		switch {
		case !tt.Skewed:
		case tt.MedianPerSecond == 0:
			findings = append(findings, fmt.Sprintf("%s p%d gets the writes, most partitions none", tt.Topic, tt.MaxPartition))
		default:
			findings = append(findings, fmt.Sprintf("%s p%d at %s is %.1fx the median partition", tt.Topic, tt.MaxPartition,
				fmtRate(tt.MaxPerSecond), tt.MaxPerSecond/tt.MedianPerSecond))
		}
	}
	head := "no skew found"
	if len(findings) > 0 {
		head = strings.Join(findings, "; ")
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s last %s: %s", pid, shortDur(window), head)

	if !hasRates {
		appendLine(&sb, "rates not available from this cluster (no offsets by time); lag only")
	}
	for i, tt := range tps {
		if i == maxThroughputLines {
			appendLine(&sb, fmt.Sprintf("… %d more topics", len(tps)-i))
			break
		}
		verdict := "even"
		if tt.Skewed {
			verdict = "skewed"
		}
		appendLine(&sb, fmt.Sprintf("%s: %d records (%s) over %d partitions; median %s, max p%d %s → %s",
			tt.Topic, tt.Records, fmtRate(tt.PerSecond), tt.Partitions, fmtRate(tt.MedianPerSecond), tt.MaxPartition,
			fmtRate(tt.MaxPerSecond), verdict))
	}
	for i, g := range gs {
		if i == maxThroughputLines {
			appendLine(&sb, fmt.Sprintf("… %d more groups", len(gs)-i))
			break
		}
		if g.Error != "" {
			appendLine(&sb, fmt.Sprintf("%s: lag not read: %s", g.Group, g.Error))
			continue
		}
		lag := fmt.Sprintf("lag %d", g.Lag)
		if g.LagUnknown > 0 {
			lag = fmt.Sprintf("lag at least %d (%d partitions unreadable)", g.Lag, g.LagUnknown)
		}
		line := fmt.Sprintf("%s (%s, %d members): %s", g.Group, g.State, g.Members, lag)
		for j, h := range g.Hot {
			if j == 3 {
				line += fmt.Sprintf("; … %d more topics", len(g.Hot)-j)
				break
			}
			line += fmt.Sprintf("; %s p%d %d%% (%d)", h.Topic, h.Partition, h.SharePct, h.Lag)
		}
		appendLine(&sb, line)
	}
	if trend != nil {
		line := fmt.Sprintf("pipeline group_lag samples %s–%s UTC: lag %d → %d (%+.0f/min)",
			trend.From.Format("15:04"), trend.To.Format("15:04"), trend.LagStart, trend.LagEnd, trend.SlopePerMinute)
		if cons != nil {
			line += fmt.Sprintf("; produced %d, consumed %d", cons.Produced, cons.Consumed)
		}
		appendLine(&sb, line)
	}
	appendLine(&sb, note)
	return sb.String()
}
