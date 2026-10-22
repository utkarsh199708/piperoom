package actions

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

var (
	_ Scoped   = (*replayRange)(nil)
	_ Preview  = (*replayRange)(nil)
	_ Classed  = (*replayRange)(nil)
	_ Verifier = (*replayRange)(nil)
)

const (
	replayRangeName = "kafka_replay_range"
	// replayRangeMax caps the records of one proposal (docs/api.md, X2).
	replayRangeMax = 100_000
	// replayRangeMaxPartitions caps the ranges and partitions params.
	replayRangeMaxPartitions = 256
	// replayRangeExecuteTimeout bounds Execute's reads and produces under
	// the room's 30 s step timeout, so the step reports what it acked.
	replayRangeExecuteTimeout = 25 * time.Second
	// replayRangeChunk is one read and one produce (ReadRecords' cap).
	replayRangeChunk = 1000
	// replayVerifyLookback: the copies are stamped when produced, during
	// Execute, so before the action.executed time Verify gets; it counts
	// from that time less Execute's bound and a minute of clock skew.
	replayVerifyLookback = replayRangeExecuteTimeout + time.Minute
	// maxVerifyDetail is VerifyResult.Detail's cap.
	maxVerifyDetail = 200
)

type replayRange struct{ d KafkaDeps }

func (a *replayRange) Name() string  { return replayRangeName }
func (a *replayRange) Layer() string { return "kafka" }
func (a *replayRange) Class() string { return ClassRecovery }
func (a *replayRange) Describe() string {
	return "Recovery: copies the records of one CDC topic of this pipeline, as offset ranges per partition (last offset included) or a past time window [from_time, to_time) resolved to offsets, into the pipeline's reprocess_topic declared in owners.yaml, with key, value and headers (Kafka Connect's __connect.errors.* headers are dropped), produced in offset order per source partition (the target partition follows the key, so each key keeps its order). At most 100000 records per proposal. A range that starts below a partition's log start is refused: those records were deleted, re-emit the rows from the source with pg_resnapshot_rows instead. The CDC topic is not changed. Not idempotent: running it twice copies twice. Afterwards the agent counts the records that reached the reprocess topic."
}

func (a *replayRange) ParamsSchema() map[string]any {
	off := func(desc string) map[string]any {
		return map[string]any{"type": "integer", "minimum": 0, "description": desc}
	}
	return objectSchema(map[string]any{
		"topic":    strProp("the pipeline CDC topic (<topic_prefix>.<source_table>) to copy from"),
		"to_topic": strProp("the pipeline's reprocess_topic from owners.yaml; the only topic this action writes"),
		"ranges": map[string]any{
			"type": "array", "minItems": 1, "maxItems": replayRangeMaxPartitions,
			"items": objectSchema(map[string]any{
				"partition":   off("partition of topic"),
				"from_offset": off("first offset to copy"),
				"to_offset":   off("last offset to copy (included)"),
			}, "partition", "from_offset", "to_offset"),
			"description": "offset ranges to copy, at most one per partition; or set from_time and to_time instead",
		},
		"from_time": map[string]any{"type": "string", "format": "date-time", "description": "RFC 3339; with to_time, instead of ranges: copy the records timestamped in [from_time, to_time)"},
		"to_time":   map[string]any{"type": "string", "format": "date-time", "description": "RFC 3339, not in the future; the end of the window (excluded)"},
		"partitions": map[string]any{
			"type": "array", "items": map[string]any{"type": "integer", "minimum": 0}, "maxItems": replayRangeMaxPartitions,
			"description": "with from_time and to_time only: the partitions to copy; default all",
		},
	}, "topic", "to_topic")
}

// replayRangeSpec is one offset range; pointers so a missing field is
// refused, not read as offset 0.
type replayRangeSpec struct {
	Partition  *int32 `json:"partition"`
	FromOffset *int64 `json:"from_offset"`
	ToOffset   *int64 `json:"to_offset"`
}

type replayRangeParams struct {
	Topic      string            `json:"topic"`
	ToTopic    string            `json:"to_topic"`
	Ranges     []replayRangeSpec `json:"ranges,omitempty"`
	FromTime   string            `json:"from_time,omitempty"`
	ToTime     string            `json:"to_time,omitempty"`
	Partitions []int32           `json:"partitions,omitempty"`

	from, to time.Time // FromTime and ToTime, parsed
}

func replayBadInput(format string, args ...any) error {
	return fmt.Errorf("actions: %s: %w: %s", replayRangeName, tools.ErrBadInput, fmt.Sprintf(format, args...))
}

// parse decodes and checks params without I/O; unknown fields are refused.
// An offset-range request over the cap is refused here already.
func (a *replayRange) parse(raw json.RawMessage) (replayRangeParams, error) {
	var p replayRangeParams
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, replayBadInput("bad params: %v", err)
	}
	switch {
	case strings.TrimSpace(p.Topic) == "":
		return p, replayBadInput("topic is required: the pipeline CDC topic to copy from")
	case strings.TrimSpace(p.ToTopic) == "":
		return p, replayBadInput("to_topic is required: the pipeline's reprocess_topic")
	case p.Topic == p.ToTopic:
		return p, replayBadInput("to_topic %q is the topic copied from", p.ToTopic)
	}
	byTime := p.FromTime != "" || p.ToTime != ""
	switch {
	case byTime && len(p.Ranges) > 0:
		return p, replayBadInput("set either ranges or from_time and to_time, not both")
	case !byTime && len(p.Ranges) == 0:
		return p, replayBadInput("set ranges (offsets per partition) or from_time and to_time")
	case byTime:
		err := a.parseWindow(&p) // before p is read: return p, f(&p) has no set order
		return p, err
	case len(p.Partitions) > 0:
		return p, replayBadInput("partitions goes with from_time and to_time; each range names its partition")
	case len(p.Ranges) > replayRangeMaxPartitions:
		return p, replayBadInput("%d ranges: at most %d", len(p.Ranges), replayRangeMaxPartitions)
	}
	seen := map[int32]bool{}
	var total int64
	for i, r := range p.Ranges {
		switch {
		case r.Partition == nil || r.FromOffset == nil || r.ToOffset == nil:
			return p, replayBadInput("ranges[%d]: partition, from_offset and to_offset are required", i)
		case *r.Partition < 0 || *r.FromOffset < 0:
			return p, replayBadInput("ranges[%d]: negative partition or offset", i)
		case *r.ToOffset < *r.FromOffset:
			return p, replayBadInput("ranges[%d]: to_offset %d is below from_offset %d", i, *r.ToOffset, *r.FromOffset)
		case seen[*r.Partition]:
			return p, replayBadInput("ranges: partition %d is listed twice", *r.Partition)
		}
		seen[*r.Partition] = true
		total += *r.ToOffset - *r.FromOffset + 1
		if total > replayRangeMax {
			return p, overCap(total)
		}
	}
	return p, nil
}

func (a *replayRange) parseWindow(p *replayRangeParams) error {
	var err error
	if p.from, err = time.Parse(time.RFC3339, p.FromTime); err != nil {
		return replayBadInput("from_time %q: want RFC 3339", p.FromTime)
	}
	if p.to, err = time.Parse(time.RFC3339, p.ToTime); err != nil {
		return replayBadInput("to_time %q: want RFC 3339", p.ToTime)
	}
	switch {
	case !p.from.Before(p.to):
		return replayBadInput("from_time must be before to_time")
	case p.to.After(a.d.clock()):
		// Records still arriving in the window would change what was
		// previewed and approved.
		return replayBadInput("to_time %s is in the future: the window must have closed", p.ToTime)
	case len(p.Partitions) > replayRangeMaxPartitions:
		return replayBadInput("%d partitions: at most %d", len(p.Partitions), replayRangeMaxPartitions)
	}
	seen := map[int32]bool{}
	for _, n := range p.Partitions {
		if n < 0 || seen[n] {
			return replayBadInput("partitions: %d is negative or listed twice", n)
		}
		seen[n] = true
	}
	return nil
}

func overCap(n int64) error {
	return fmt.Errorf("actions: %s: the range holds %d records, over the %d one proposal may copy; split it into several proposals", replayRangeName, n, replayRangeMax)
}

func (a *replayRange) CheckScope(pl owners.Pipeline, raw json.RawMessage) error {
	var p replayRangeParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("actions: bad params: %w", err)
	}
	sc, err := tools.ScopeFor(tools.WithPipeline(context.Background(), pl), nil)
	if err != nil {
		return fmt.Errorf("actions: %w", err)
	}
	return checkReplayRangeNames(sc, p.Topic, p.ToTopic)
}

// checkReplayRangeNames: the source must be one of the pipeline's CDC topics
// and the target exactly its declared reprocess_topic, the one other topic
// the Kafka writer's scope lets it produce to.
func checkReplayRangeNames(sc tools.Scope, topic, toTopic string) error {
	if err := sc.CheckTopic(topic); err != nil {
		return fmt.Errorf("actions: %s: topic must be a CDC topic of this pipeline (<topic_prefix>.<source_table>): %w", replayRangeName, err)
	}
	if sc.Unscoped() {
		return nil
	}
	pl := sc.Pipeline()
	switch {
	case pl.ReprocessTopic == "":
		return replayBadInput("pipeline %q declares no reprocess_topic in owners.yaml: add a topic of its own for the consumer to re-read, then propose again", pl.ID)
	case toTopic != pl.ReprocessTopic:
		return fmt.Errorf("actions: %s: %w: to_topic %q is not the reprocess_topic %q of pipeline %q", replayRangeName, tools.ErrOutOfScope, toTopic, pl.ReprocessTopic, pl.ID)
	}
	return nil
}

// replaySpanRange is one partition's part of the copy.
type replaySpanRange struct {
	partition  int32
	start, end int64 // the partition's log start and end offset
	from, to   int64 // first and last offset to copy, both included
}

func (s replaySpanRange) offsets() int64 { return s.to - s.from + 1 }

// resolve reads offsets (never a record) and returns the spans to copy in
// partition order and their offset count. It refuses a missing topic,
// partition or reprocess topic, a range below the log start or past the
// end, an empty window and a count over the cap.
func (a *replayRange) resolve(ctx context.Context, p replayRangeParams) ([]replaySpanRange, int64, error) {
	offs, err := a.d.Records.DLQOffsets(ctx, p.Topic, p.ToTopic)
	if err != nil {
		return nil, 0, fmt.Errorf("actions: %s: read offsets of %s and %s: %w", replayRangeName, p.Topic, p.ToTopic, err)
	}
	parts := offs[p.Topic]
	if len(parts) == 0 {
		return nil, 0, fmt.Errorf("actions: %s: topic %q does not exist: %w", replayRangeName, p.Topic, clients.ErrNotFound)
	}
	if _, ok := offs[p.ToTopic]; !ok {
		return nil, 0, fmt.Errorf("actions: %s: reprocess topic %q does not exist; create it first: %w", replayRangeName, p.ToTopic, clients.ErrNotFound)
	}
	byPart := make(map[int32]clients.PartitionOffsets, len(parts))
	for _, po := range parts {
		byPart[po.Partition] = po
	}
	var spans []replaySpanRange
	if len(p.Ranges) > 0 {
		for _, r := range p.Ranges {
			po, ok := byPart[*r.Partition]
			if !ok {
				return nil, 0, fmt.Errorf("actions: %s: topic %q has no partition %d: %w", replayRangeName, p.Topic, *r.Partition, clients.ErrNotFound)
			}
			if *r.FromOffset < po.Start {
				return nil, 0, belowLogStart(p.Topic, po, *r.FromOffset)
			}
			if *r.ToOffset >= po.End {
				return nil, 0, fmt.Errorf("actions: %s: %s partition %d: to_offset %d is at or past the end offset %d: not written yet", replayRangeName, p.Topic, po.Partition, *r.ToOffset, po.End)
			}
			spans = append(spans, replaySpanRange{po.Partition, po.Start, po.End, *r.FromOffset, *r.ToOffset})
		}
	} else if spans, err = a.windowSpans(ctx, p, byPart); err != nil {
		return nil, 0, err
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].partition < spans[j].partition })
	var total int64
	for _, s := range spans {
		total += s.offsets()
	}
	switch {
	case total == 0:
		return nil, 0, fmt.Errorf("actions: %s: %s holds no record in the window: nothing to replay", replayRangeName, p.Topic)
	case total > replayRangeMax:
		return nil, 0, overCap(total)
	}
	return spans, total, nil
}

func belowLogStart(topic string, po clients.PartitionOffsets, from int64) error {
	return fmt.Errorf("actions: %s: %s partition %d: offsets %d to %d are below the log start %d and were deleted (retention or compaction), so Kafka cannot replay them; re-emit those rows from the source with pg_resnapshot_rows instead, and replay only from offset %d: %w",
		replayRangeName, topic, po.Partition, from, po.Start-1, po.Start, po.Start, clients.ErrNotFound)
}

// windowSpans resolves [from_time, to_time) to offsets per partition from
// offsets by time (clients.RateReader on the record reader).
func (a *replayRange) windowSpans(ctx context.Context, p replayRangeParams, byPart map[int32]clients.PartitionOffsets) ([]replaySpanRange, error) {
	rr, ok := a.d.Records.(clients.RateReader)
	if !ok {
		return nil, fmt.Errorf("actions: %s: this Kafka client cannot find offsets by time; give ranges with offsets: %w", replayRangeName, clients.ErrNotSupported)
	}
	firsts, err := rr.PartitionRates(ctx, p.from, p.Topic)
	if err != nil {
		return nil, fmt.Errorf("actions: %s: offsets of %s at from_time: %w", replayRangeName, p.Topic, err)
	}
	ends, err := rr.PartitionRates(ctx, p.to, p.Topic)
	if err != nil {
		return nil, fmt.Errorf("actions: %s: offsets of %s at to_time: %w", replayRangeName, p.Topic, err)
	}
	// atTime is the first offset at or after the time: the end offset when
	// no record is that new (and when the lookup gave none).
	atTime := func(rates []clients.PartitionRate, part int32, end int64) int64 {
		for _, r := range rates {
			if r.Topic == p.Topic && r.Partition == part && r.OffsetAtStart >= 0 {
				return min(r.OffsetAtStart, end)
			}
		}
		return end
	}
	want := make([]int32, 0, len(byPart))
	if len(p.Partitions) > 0 {
		for _, n := range p.Partitions {
			if _, ok := byPart[n]; !ok {
				return nil, fmt.Errorf("actions: %s: topic %q has no partition %d: %w", replayRangeName, p.Topic, n, clients.ErrNotFound)
			}
		}
		want = p.Partitions
	} else {
		for n := range byPart {
			want = append(want, n)
		}
	}
	var spans []replaySpanRange
	for _, n := range want {
		po := byPart[n]
		from, to := atTime(firsts, n, po.End), atTime(ends, n, po.End)
		if to <= from {
			continue // nothing timestamped in the window here
		}
		// The first retained record is in the window and older ones were
		// deleted: the window may begin in the deleted part. Refuse rather
		// than replay a window with a silent hole at its start.
		if from <= po.Start && po.Start > 0 {
			return nil, fmt.Errorf("actions: %s: %s partition %d: the window may begin below the log start %d, where records were deleted (retention or compaction), so Kafka cannot replay all of it; re-emit the rows from the source with pg_resnapshot_rows, or give ranges from offset %d to replay only what is retained: %w",
				replayRangeName, p.Topic, n, po.Start, po.Start, clients.ErrNotFound)
		}
		spans = append(spans, replaySpanRange{n, po.Start, po.End, from, to - 1})
	}
	return spans, nil
}

func (a *replayRange) noReader() error {
	return fmt.Errorf("actions: %s: no Kafka client configured to read the topic", replayRangeName)
}

// Validate reads offsets (≤ 10 s, never a record): the topic, its
// partitions and the reprocess topic must exist and the range must be
// retained, written and within the cap. With the room's pipeline in ctx the
// two topics are checked against it first, before any I/O.
func (a *replayRange) Validate(ctx context.Context, raw json.RawMessage) error {
	p, err := a.parse(raw)
	if err != nil {
		return err
	}
	if pl, ok := tools.PipelineFrom(ctx); ok {
		if err := a.CheckScope(pl, raw); err != nil {
			return err
		}
	}
	if a.d.Records == nil {
		return a.noReader()
	}
	t := timeout
	if a.d.validateTimeout > 0 {
		t = a.d.validateTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, t)
	defer cancel()
	_, _, err = a.resolve(ctx, p)
	return err
}

// scoped parses params and checks them against the scope in ctx, failing
// closed like the Kafka writer; it does no I/O.
func (a *replayRange) scoped(ctx context.Context, raw json.RawMessage) (replayRangeParams, error) {
	p, err := a.parse(raw)
	if err != nil {
		return p, err
	}
	sc, err := pipelineScope(ctx, replayRangeName)
	if err != nil {
		return p, err
	}
	if err := checkReplayRangeNames(sc, p.Topic, p.ToTopic); err != nil {
		return p, err
	}
	if a.d.Records == nil {
		return p, a.noReader()
	}
	return p, nil
}

// Preview shows the count per partition and the first keys, redacted as
// dlq_replay's are. It reads offsets and at most three records.
func (a *replayRange) Preview(ctx context.Context, raw json.RawMessage) (PreviewResult, error) {
	p, err := a.scoped(ctx, raw)
	if err != nil {
		return PreviewResult{}, previewErr(replayRangeName, err)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	spans, total, err := a.resolve(ctx, p)
	if err != nil {
		return PreviewResult{}, previewErr(replayRangeName, err)
	}
	first := spans[0]
	recs, err := a.d.Records.ReadRecords(ctx, p.Topic, first.partition, first.from, dlqPreviewKeys)
	if err != nil {
		return PreviewResult{}, previewErr(replayRangeName, fmt.Errorf("read %s partition %d from %d: %w", p.Topic, first.partition, first.from, err))
	}
	recs = inSpan(recs, first)
	keys := "none (the first offsets hold no record)"
	if len(recs) > 0 {
		keys = firstKeys(recs)
	}
	var cur, exp []string
	for i, s := range spans {
		if i == maxListedPartitions {
			cur = append(cur, fmt.Sprintf("and %d more partitions", len(spans)-i))
			exp = append(exp, fmt.Sprintf("and %d more partitions", len(spans)-i))
			break
		}
		cur = append(cur, fmt.Sprintf("partition %d holds offsets %d to %d", s.partition, s.start, s.end-1))
		exp = append(exp, fmt.Sprintf("partition %d: offsets %d to %d (%d)", s.partition, s.from, s.to, s.offsets()))
	}
	window := ""
	if !p.from.IsZero() {
		window = fmt.Sprintf(" (records timestamped from %s to before %s)", p.FromTime, p.ToTime)
	}
	count := total
	return PreviewResult{
		Target:  fmt.Sprintf("topic %s, %d partition(s), %d records → reprocess topic %s", p.Topic, len(spans), total, p.ToTopic),
		Current: fmt.Sprintf("%s: %s; reprocess topic %s exists", p.Topic, strings.Join(cur, "; "), p.ToTopic),
		Expected: fmt.Sprintf("will re-emit %d records%s to %s, in offset order per source partition (each key keeps its order): %s. First keys (redacted): %s. "+
			"Key, value and other headers are kept; the %s* headers are dropped. %s is not changed. "+
			"The count is offsets: one that holds no record (compaction, transaction markers) is skipped. "+
			"Not idempotent: running it again, or approving another proposal for the same range, copies the records again.",
			total, window, p.ToTopic, strings.Join(exp, "; "), keys, connectErrorsPrefix, p.Topic),
		Reversible: false, // a produced record cannot be taken back
		Count:      &count,
		Unit:       events.RecoveryUnitRecords,
	}, nil
}

// inSpan keeps the records of recs up to s.to.
func inSpan(recs []clients.Record, s replaySpanRange) []clients.Record {
	for i, r := range recs {
		if r.Offset > s.to {
			return recs[:i]
		}
	}
	return recs
}

func (a *replayRange) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	if a.d.Writer == nil {
		return "", fmt.Errorf("actions: %s: no Kafka write client: PIPEROOM_WRITE_MODE is off", replayRangeName)
	}
	// Scope before any I/O; the writer checks the target again on its own.
	p, err := a.scoped(ctx, raw)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, replayRangeExecuteTimeout)
	defer cancel()
	spans, total, err := a.resolve(ctx, p)
	if err != nil {
		return "", err
	}
	var acked int64
	dropped := 0
	for _, s := range spans {
		n, d, err := a.copySpan(ctx, p, s)
		acked += n
		dropped += d
		if err != nil {
			return "", fmt.Errorf("actions: %s: %s → %s: acked %d of %d records, stopped in partition %d (offsets %d to %d); a re-run copies the acked ones again: %w",
				replayRangeName, p.Topic, p.ToTopic, acked, total, s.partition, s.from, s.to, err)
		}
	}
	parts := make([]string, 0, min(len(spans), maxListedPartitions))
	for i, s := range spans {
		if i == maxListedPartitions {
			parts = append(parts, "…")
			break
		}
		parts = append(parts, fmt.Sprintf("p%d %d–%d", s.partition, s.from, s.to))
	}
	return fmt.Sprintf("replayed %s (%s) to %s: acked %d records of %d offsets, in offset order per source partition; %d %s* headers dropped; %s unchanged",
		p.Topic, strings.Join(parts, ", "), p.ToTopic, acked, total, dropped, connectErrorsPrefix, p.Topic), nil
}

// copySpan copies one partition's span chunk by chunk, each chunk acked
// before the next is read, so the partition's order holds in the target.
func (a *replayRange) copySpan(ctx context.Context, p replayRangeParams, s replaySpanRange) (acked int64, dropped int, err error) {
	next := s.from
	for next <= s.to {
		recs, err := a.d.Records.ReadRecords(ctx, p.Topic, s.partition, next, int(min(replayRangeChunk, s.to-next+1)))
		if err != nil {
			return acked, dropped, fmt.Errorf("read partition %d from %d: %w", s.partition, next, err)
		}
		for _, r := range recs {
			if r.Offset < next {
				return acked, dropped, fmt.Errorf("read partition %d: offset %d out of order (expected %d or later)", s.partition, r.Offset, next)
			}
		}
		recs = inSpan(recs, s)
		if len(recs) == 0 {
			return acked, dropped, nil // the rest of the span holds no record
		}
		out, d := stripConnectHeaders(recs)
		n, err := a.produce(ctx, p.ToTopic, out)
		acked += int64(n)
		dropped += d
		if err != nil {
			return acked, dropped, err
		}
		next = lastOffset(recs) + 1
	}
	return acked, dropped, nil
}

func (a *replayRange) produce(ctx context.Context, topic string, recs []clients.Record) (int, error) {
	var n int
	var err error
	// The pipelined produce (clients.BulkProducer) when the writer has it;
	// else Produce, a round trip per record (fakes).
	if bp, ok := a.d.Writer.(clients.BulkProducer); ok {
		n, err = bp.ProduceAll(ctx, topic, recs)
	} else {
		n, err = a.d.Writer.Produce(ctx, topic, recs)
	}
	return min(max(n, 0), len(recs)), err
}

// Verify counts what reached the reprocess topic: Before is the range's
// offset count (the preview's Count), After the part of it not yet written
// to the reprocess topic since the step began. It reads offsets only
// (DLQOffsets and offsets by time), never a record, so it is the same in
// write mode. Without offsets by time the error wraps
// clients.ErrNotSupported (unverifiable).
func (a *replayRange) Verify(ctx context.Context, raw json.RawMessage, executedAt time.Time) (VerifyResult, error) {
	if executedAt.IsZero() {
		return VerifyResult{}, fmt.Errorf("actions: %s: verify needs the time the step ran", replayRangeName)
	}
	p, err := a.scoped(ctx, raw)
	if err != nil {
		return VerifyResult{}, err
	}
	rr, ok := a.d.Records.(clients.RateReader)
	if !ok {
		return VerifyResult{}, fmt.Errorf("actions: %s: this Kafka client cannot count records by time: %w", replayRangeName, clients.ErrNotSupported)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, before, err := a.resolve(ctx, p)
	if err != nil {
		return VerifyResult{}, err
	}
	since := executedAt.Add(-replayVerifyLookback)
	rates, err := rr.PartitionRates(ctx, since, p.ToTopic)
	if err != nil {
		return VerifyResult{}, fmt.Errorf("actions: %s: count records in %s: %w", replayRangeName, p.ToTopic, err)
	}
	var written int64
	for _, r := range rates {
		if r.Topic == p.ToTopic {
			written += r.Records
		}
	}
	after := min(max(before-written, 0), before)
	detail := fmt.Sprintf("%d of %d records found in %s since %s (from %s)", min(written, before), before, p.ToTopic, since.UTC().Format(time.RFC3339), p.Topic)
	return VerifyResult{Before: before, After: after, Unit: events.RecoveryUnitRecords, Detail: cutText(detail, maxVerifyDetail)}, nil
}
