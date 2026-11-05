package actions

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// DLQRecordReader is what dlq_replay reads through (KafkaDeps.Records):
// whole records of one DLQ partition, and the partition bounds of the DLQ and
// the target topic. It changes nothing. kafka.NewReplayReader over the reader
// client implements it (write mode off or on), so Validate and Preview never
// need the writer, and Execute alone needs KafkaDeps.Writer.
type DLQRecordReader interface {
	clients.RecordReader
	DLQOffsets(ctx context.Context, topics ...string) (map[string][]clients.PartitionOffsets, error)
}

var (
	_ Scoped  = (*dlqReplay)(nil)
	_ Preview = (*dlqReplay)(nil)
)

const (
	dlqReplayName = "dlq_replay"
	// dlqMaxReplay caps count: one step copies at most this many records.
	dlqMaxReplay = 1000
	// dlqMaxReplayBytes bounds what one step holds in memory (key, value
	// and headers); a step stops reading there and reports where to go on.
	dlqMaxReplayBytes = 32 << 20
	// dlqExecuteTimeout bounds Execute's read and produce. It stays under
	// the room's 30 s step timeout so the step reports what it acked.
	dlqExecuteTimeout = 25 * time.Second
	// dlqPreviewKeys is how many record keys the preview shows (redacted).
	dlqPreviewKeys = 3
	// dlqKeyRunes caps one previewed key.
	dlqKeyRunes = 60
	// connectErrorsPrefix starts every header Kafka Connect adds to a
	// dead-lettered record; the replay drops them, keeps every other one.
	connectErrorsPrefix = "__connect.errors."
	// connectErrorsTopic names the topic the record was consumed from.
	connectErrorsTopic = connectErrorsPrefix + "topic"
)

type dlqReplay struct{ d KafkaDeps }

func (a *dlqReplay) Name() string  { return dlqReplayName }
func (a *dlqReplay) Layer() string { return "kafka" }
func (a *dlqReplay) Describe() string {
	return "Copies up to 1000 records of one dead-letter topic partition back to the pipeline's CDC topic they came from, in DLQ offset order, with their key, value and headers (Kafka Connect's __connect.errors.* headers are dropped). The DLQ is not changed. Fix the cause first: replayed records that hit the same cause are dead-lettered again, and records the sink already handled are duplicated. The step is not idempotent: running it twice copies twice. The result names the offsets copied and the from_offset that continues the replay in a next step."
}

func (a *dlqReplay) ParamsSchema() map[string]any {
	return objectSchema(map[string]any{
		"dlq_topic": strProp("dead-letter topic; one of the pipeline's watch.dlq_topics"),
		"to_topic":  strProp("the pipeline CDC topic (<topic_prefix>.<source_table>) the records are copied to"),
		"count": map[string]any{
			"type": "integer", "minimum": 1, "maximum": dlqMaxReplay,
			"description": "how many records to copy, at most 1000; fewer when the partition ends first",
		},
		"partition": map[string]any{
			"type": "integer", "minimum": 0,
			"description": "DLQ partition to replay (order holds per partition); required when the DLQ has more than one partition, 0 otherwise",
		},
		"from_offset": map[string]any{
			"type": "integer", "minimum": 0,
			"description": "first DLQ offset to copy; default the partition's log start at execution time. Set it to the next_from_offset a previous step reported to continue",
		},
	}, "dlq_topic", "to_topic", "count")
}

type dlqReplayParams struct {
	DLQTopic   string `json:"dlq_topic"`
	ToTopic    string `json:"to_topic"`
	Count      int    `json:"count"`
	Partition  *int32 `json:"partition,omitempty"`
	FromOffset *int64 `json:"from_offset,omitempty"`
}

func (p dlqReplayParams) partition() int32 {
	if p.Partition == nil {
		return 0
	}
	return *p.Partition
}

// parse decodes and checks params without I/O. Unknown fields are refused: a
// misspelt param (say, "offset") must not be dropped silently on a write.
func (a *dlqReplay) parse(raw json.RawMessage) (dlqReplayParams, error) {
	var p dlqReplayParams
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, fmt.Errorf("actions: %s: bad params: %w", dlqReplayName, err)
	}
	switch {
	case strings.TrimSpace(p.DLQTopic) == "":
		return p, fmt.Errorf("actions: %s: dlq_topic is required", dlqReplayName)
	case strings.TrimSpace(p.ToTopic) == "":
		return p, fmt.Errorf("actions: %s: to_topic is required: the pipeline CDC topic (<topic_prefix>.<source_table>) to copy the records to", dlqReplayName)
	case p.DLQTopic == p.ToTopic:
		return p, fmt.Errorf("actions: %s: to_topic %q is the DLQ itself", dlqReplayName, p.ToTopic)
	case p.Count < 1:
		return p, fmt.Errorf("actions: %s: count %d: want 1 to %d", dlqReplayName, p.Count, dlqMaxReplay)
	case p.Count > dlqMaxReplay:
		return p, fmt.Errorf("actions: %s: count %d is over the %d records one step may copy; replay in several steps, each from the next_from_offset the previous one reported", dlqReplayName, p.Count, dlqMaxReplay)
	case p.Partition != nil && *p.Partition < 0:
		return p, fmt.Errorf("actions: %s: negative partition %d", dlqReplayName, *p.Partition)
	case p.FromOffset != nil && *p.FromOffset < 0:
		return p, fmt.Errorf("actions: %s: negative from_offset %d", dlqReplayName, *p.FromOffset)
	}
	return p, nil
}

func (a *dlqReplay) CheckScope(pl owners.Pipeline, raw json.RawMessage) error {
	var p dlqReplayParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("actions: bad params: %w", err)
	}
	if p.DLQTopic == "" || p.ToTopic == "" {
		return fmt.Errorf("actions: %s: dlq_topic and to_topic are required", dlqReplayName)
	}
	sc, err := tools.ScopeFor(tools.WithPipeline(context.Background(), pl), nil)
	if err != nil {
		return fmt.Errorf("actions: %w", err)
	}
	return checkReplayNames(sc, p.DLQTopic, p.ToTopic)
}

// checkReplayNames: the DLQ must be declared in the pipeline's
// watch.dlq_topics (the dlq_peek rule) and the target must be one of its CDC
// topics, which is all the Kafka writer's own scope check lets it produce to.
func checkReplayNames(sc tools.Scope, dlqTopic, toTopic string) error {
	if err := sc.CheckDLQTopic(dlqTopic); err != nil {
		return fmt.Errorf("actions: %s: dlq_topic must be declared in the pipeline's watch.dlq_topics: %w", dlqReplayName, err)
	}
	if err := sc.CheckTopic(toTopic); err != nil {
		return fmt.Errorf("actions: %s: to_topic must be a CDC topic of this pipeline (<topic_prefix>.<source_table>): %w", dlqReplayName, err)
	}
	return nil
}

// dlqScope scopes Preview and Execute, fail closed like the Kafka writer:
// the room's whole pipeline in ctx (tools.WithPipeline), or the explicit
// opt-out (tools.WithUnscoped; CLI and integration tests). Anything else,
// a bare ctx or a pipeline id only, wraps tools.ErrNoScope.
func dlqScope(ctx context.Context) (tools.Scope, error) { return pipelineScope(ctx, dlqReplayName) }

// pipelineScope is dlqScope for the action named action (kafka_replay_range
// too).
func pipelineScope(ctx context.Context, action string) (tools.Scope, error) {
	if _, ok := tools.PipelineFrom(ctx); !ok {
		sc, err := tools.ScopeFor(ctx, nil)
		if err == nil && sc.Unscoped() {
			return sc, nil
		}
		return tools.Scope{}, fmt.Errorf("actions: %s: %w: needs the room's pipeline in ctx", action, tools.ErrNoScope)
	}
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		return tools.Scope{}, fmt.Errorf("actions: %s: %w", action, err)
	}
	return sc, nil
}

// sharedDLQ reports whether the pipeline declares dlqTopic shared with other
// pipelines (unknown, so false, when unscoped).
func sharedDLQ(sc tools.Scope, dlqTopic string) bool {
	if sc.Unscoped() {
		return false
	}
	for _, d := range sc.Pipeline().Watch().DLQTopics {
		if d.Topic == dlqTopic {
			return d.Shared
		}
	}
	return false
}

func (a *dlqReplay) noReader() error {
	return fmt.Errorf("actions: %s: no Kafka client configured to read the DLQ", dlqReplayName)
}

func (a *dlqReplay) noWriter() error {
	return fmt.Errorf("actions: %s: no Kafka write client: PIPEROOM_WRITE_MODE is off", dlqReplayName)
}

// replaySpan is the part of the DLQ partition a step reads.
type replaySpan struct {
	partition  int32
	partitions int   // how many partitions the DLQ has
	start, end int64 // the partition's log start and end offset
	from       int64 // first offset to copy
}

// bounds reads the DLQ's and the target's partition offsets (no record) and
// returns the span to copy. It refuses a DLQ or target that does not exist,
// a DLQ with several partitions and no partition param, a partition or
// from_offset outside the log, and an empty span.
func (a *dlqReplay) bounds(ctx context.Context, p dlqReplayParams) (replaySpan, error) {
	offs, err := a.d.Records.DLQOffsets(ctx, p.DLQTopic, p.ToTopic)
	if err != nil {
		return replaySpan{}, fmt.Errorf("actions: %s: read offsets of %s and %s: %w", dlqReplayName, p.DLQTopic, p.ToTopic, err)
	}
	parts, ok := offs[p.DLQTopic]
	if !ok || len(parts) == 0 {
		return replaySpan{}, fmt.Errorf("actions: %s: DLQ topic %q does not exist: %w", dlqReplayName, p.DLQTopic, clients.ErrNotFound)
	}
	if _, ok := offs[p.ToTopic]; !ok {
		return replaySpan{}, fmt.Errorf("actions: %s: to_topic %q does not exist: %w", dlqReplayName, p.ToTopic, clients.ErrNotFound)
	}
	// Order holds per partition only: replaying several partitions in one
	// step would interleave them, so the step names one.
	if p.Partition == nil && len(parts) > 1 {
		return replaySpan{}, fmt.Errorf("actions: %s: DLQ topic %q has %d partitions; set partition (0 to %d) and replay each partition in its own step, so records keep their order", dlqReplayName, p.DLQTopic, len(parts), len(parts)-1)
	}
	part := p.partition()
	var po *clients.PartitionOffsets
	for i := range parts {
		if parts[i].Partition == part {
			po = &parts[i]
		}
	}
	if po == nil {
		return replaySpan{}, fmt.Errorf("actions: %s: DLQ topic %q has no partition %d (it has %d): %w", dlqReplayName, p.DLQTopic, part, len(parts), clients.ErrNotFound)
	}
	sp := replaySpan{partition: part, partitions: len(parts), start: po.Start, end: po.End, from: po.Start}
	if p.FromOffset != nil {
		sp.from = *p.FromOffset
	}
	switch {
	case sp.start >= sp.end:
		return sp, fmt.Errorf("actions: %s: DLQ %s partition %d is empty (offsets %d to %d): nothing to replay", dlqReplayName, p.DLQTopic, part, sp.start, sp.end)
	case sp.from < sp.start:
		return sp, fmt.Errorf("actions: %s: from_offset %d is below the log start %d of DLQ %s partition %d: those records were deleted", dlqReplayName, sp.from, sp.start, p.DLQTopic, part)
	case sp.from >= sp.end:
		return sp, fmt.Errorf("actions: %s: from_offset %d is at or past the end offset %d of DLQ %s partition %d: nothing left to replay", dlqReplayName, sp.from, sp.end, p.DLQTopic, part)
	}
	return sp, nil
}

// read reads up to p.Count records of the span in offset order, at most
// dlqMaxReplayBytes. Any read error fails the whole read: a replay copies a
// prefix it read completely or nothing.
func (a *dlqReplay) read(ctx context.Context, p dlqReplayParams, sp replaySpan) ([]clients.Record, error) {
	var out []clients.Record
	next, size := sp.from, 0
	for len(out) < p.Count && next < sp.end && size < dlqMaxReplayBytes {
		recs, err := a.d.Records.ReadRecords(ctx, p.DLQTopic, sp.partition, next, p.Count-len(out))
		if err != nil {
			return nil, fmt.Errorf("actions: %s: read DLQ %s partition %d from %d: %w", dlqReplayName, p.DLQTopic, sp.partition, next, err)
		}
		if len(recs) == 0 {
			break
		}
		for _, r := range recs {
			if r.Offset < next {
				return nil, fmt.Errorf("actions: %s: read DLQ %s partition %d: offset %d out of order (expected %d or later)", dlqReplayName, p.DLQTopic, sp.partition, r.Offset, next)
			}
			if len(out) == p.Count || size >= dlqMaxReplayBytes {
				break
			}
			out = append(out, r)
			next = r.Offset + 1
			size += replayRecordBytes(r)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("actions: %s: DLQ %s partition %d has no record from offset %d (only markers or compacted offsets): nothing to replay", dlqReplayName, p.DLQTopic, sp.partition, sp.from)
	}
	return out, nil
}

func replayRecordBytes(r clients.Record) int {
	n := len(r.Key) + len(r.Value)
	for _, h := range r.Headers {
		n += len(h.Key) + len(h.Value)
	}
	return n
}

// checkOrigin refuses records whose Connect header names another topic than
// to_topic: they were consumed from somewhere else and must not be copied
// here. On a DLQ declared shared, every record must carry the header (others
// pipelines' records sit there too). It returns how many records carry it.
func checkOrigin(recs []clients.Record, toTopic string, shared bool) (int, error) {
	named := 0
	for _, r := range recs {
		origin, ok := headerValue(r.Headers, connectErrorsTopic)
		if !ok {
			if shared {
				return named, fmt.Errorf("actions: %s: DLQ offset %d has no %s header, and the DLQ is shared with other pipelines: its origin is unknown, so it is not replayed", dlqReplayName, r.Offset, connectErrorsTopic)
			}
			continue
		}
		named++
		if origin != toTopic {
			return named, fmt.Errorf("actions: %s: DLQ offset %d came from topic %q (its %s header), not to_topic %q; replay each origin to its own topic", dlqReplayName, r.Offset, clipOrigin(origin), connectErrorsTopic, toTopic)
		}
	}
	return named, nil
}

func headerValue(hs []clients.RecordHeader, key string) (string, bool) {
	for _, h := range hs {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// clipOrigin caps untrusted header text echoed in an error.
func clipOrigin(s string) string {
	s = strings.ToValidUTF8(s, "?")
	if utf8.RuneCountInString(s) > 80 {
		s = string([]rune(s)[:80]) + "…"
	}
	return s
}

// stripConnectHeaders returns the records to produce: key, value and every
// header kept, except Connect's __connect.errors.* ones (the failure context
// of the dead-lettering, not part of the record). It also counts the
// dropped headers. The inputs are not modified.
func stripConnectHeaders(recs []clients.Record) ([]clients.Record, int) {
	out := make([]clients.Record, len(recs))
	dropped := 0
	for i, r := range recs {
		out[i] = r
		out[i].Headers = nil
		for _, h := range r.Headers {
			if strings.HasPrefix(h.Key, connectErrorsPrefix) {
				dropped++
				continue
			}
			out[i].Headers = append(out[i].Headers, h)
		}
	}
	return out, dropped
}

// keyPreview renders one record key for approvers: text keys redacted
// (redact.Text) and then capped, binary keys (Avro, Protobuf) by size only.
// Redaction runs on the whole key before the cut: cutting first can break a
// secret's pattern (a JWT without its signature) and leave its head unmasked.
func keyPreview(k []byte) string {
	if k == nil {
		return "null key"
	}
	if !utf8.Valid(k) || strings.ContainsFunc(string(k), func(r rune) bool { return unicode.IsControl(r) && r != '\t' }) {
		return fmt.Sprintf("binary key, %d bytes", len(k))
	}
	s, _ := redact.Text(string(k))
	if utf8.RuneCountInString(s) > dlqKeyRunes {
		s = string([]rune(s)[:dlqKeyRunes]) + "…"
	}
	return strconv.Quote(s)
}

func firstKeys(recs []clients.Record) string {
	keys := make([]string, 0, dlqPreviewKeys)
	for i := 0; i < len(recs) && i < dlqPreviewKeys; i++ {
		keys = append(keys, fmt.Sprintf("@%d %s", recs[i].Offset, keyPreview(recs[i].Key)))
	}
	return strings.Join(keys, ", ")
}

// Validate reads the target (≤ 10 s): the DLQ partition must exist and hold
// records from from_offset on, the target topic must exist, and a DLQ with
// several partitions needs partition. It reads offsets only, never a record.
// It does not need a scope in ctx; when ctx carries the room's pipeline, the
// two topics are checked against it first, before any I/O.
func (a *dlqReplay) Validate(ctx context.Context, raw json.RawMessage) error {
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
	_, err = a.bounds(ctx, p)
	return err
}

func (a *dlqReplay) Preview(ctx context.Context, raw json.RawMessage) (PreviewResult, error) {
	p, err := a.parse(raw)
	if err != nil {
		return PreviewResult{}, previewErr(dlqReplayName, err)
	}
	sc, err := dlqScope(ctx)
	if err != nil {
		return PreviewResult{}, previewErr(dlqReplayName, err)
	}
	if err := checkReplayNames(sc, p.DLQTopic, p.ToTopic); err != nil {
		return PreviewResult{}, previewErr(dlqReplayName, err)
	}
	if a.d.Records == nil {
		return PreviewResult{}, previewErr(dlqReplayName, a.noReader())
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	sp, err := a.bounds(ctx, p)
	if err != nil {
		return PreviewResult{}, previewErr(dlqReplayName, err)
	}
	recs, err := a.read(ctx, p, sp)
	if err != nil {
		return PreviewResult{}, previewErr(dlqReplayName, err)
	}
	named, err := checkOrigin(recs, p.ToTopic, sharedDLQ(sc, p.DLQTopic))
	if err != nil {
		return PreviewResult{}, previewErr(dlqReplayName, err)
	}
	_, dropped := stripConnectHeaders(recs)
	first, lastOff := recs[0].Offset, lastOffset(recs)

	from := fmt.Sprintf("from_offset %d", sp.from)
	if p.FromOffset == nil {
		from = fmt.Sprintf("from_offset not set: the log start at execution time, now %d", sp.from)
	}
	current := fmt.Sprintf("DLQ %s partition %d of %d holds offsets %d to %d (%d at most); %s", p.DLQTopic, sp.partition, sp.partitions, sp.start, sp.end-1, sp.end-sp.start, from)
	if named > 0 {
		current += fmt.Sprintf("; %d of the %d records name %s as their origin (%s)", named, len(recs), p.ToTopic, connectErrorsTopic)
	}
	short := ""
	if len(recs) < p.Count {
		short = fmt.Sprintf(" (count is %d; only %d records are there or fit one step)", p.Count, len(recs))
	}
	expected := fmt.Sprintf("%d records copied to %s in DLQ offset order%s, offsets %d to %d; next_from_offset will be %d. "+
		"First keys (redacted): %s. Key, value and other headers are kept; the %s* headers are dropped (%d on these records). "+
		"The DLQ is not changed: its end offset stays %d, and %s's end offsets grow by %d. "+
		"Replayed records are duplicates if the cause was not fixed (they are dead-lettered again) or if the sink already handled them.",
		len(recs), p.ToTopic, short, first, lastOff, lastOff+1,
		firstKeys(recs), connectErrorsPrefix, dropped,
		sp.end, p.ToTopic, len(recs))
	return PreviewResult{
		Target:     fmt.Sprintf("DLQ %s partition %d offsets %d–%d → topic %s", p.DLQTopic, sp.partition, first, lastOff, p.ToTopic),
		Current:    current,
		Expected:   expected,
		Reversible: false, // a produced record cannot be taken back
	}, nil
}

func (a *dlqReplay) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, err := a.parse(raw)
	if err != nil {
		return "", err
	}
	if a.d.Writer == nil {
		return "", a.noWriter()
	}
	if a.d.Records == nil {
		return "", a.noReader()
	}
	// Scope before any I/O; the writer checks the target again on its own.
	sc, err := dlqScope(ctx)
	if err != nil {
		return "", err
	}
	if err := checkReplayNames(sc, p.DLQTopic, p.ToTopic); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, dlqExecuteTimeout)
	defer cancel()
	sp, err := a.bounds(ctx, p)
	if err != nil {
		return "", err
	}
	recs, err := a.read(ctx, p, sp)
	if err != nil {
		return "", err
	}
	if _, err := checkOrigin(recs, p.ToTopic, sharedDLQ(sc, p.DLQTopic)); err != nil {
		return "", err
	}
	out, dropped := stripConnectHeaders(recs)
	acked, perr := a.d.Writer.Produce(ctx, p.ToTopic, out)
	acked = min(max(acked, 0), len(recs))
	where := fmt.Sprintf("DLQ %s partition %d", p.DLQTopic, sp.partition)
	if perr != nil {
		// Stopped at the first record not acked: name exactly what was
		// copied, so a next step continues there and copies nothing twice.
		next := lastOffset(recs) + 1
		if acked < len(recs) {
			next = recs[acked].Offset
		}
		copied := "none copied"
		if acked > 0 {
			copied = fmt.Sprintf("offsets %d to %d copied", recs[0].Offset, recs[acked-1].Offset)
		}
		return "", fmt.Errorf("actions: %s: %s → %s: acked %d of %d (%s); next_from_offset %d: %w",
			dlqReplayName, where, p.ToTopic, acked, len(recs), copied, next, perr)
	}
	end := lastOffset(recs)
	left := max(sp.end-(end+1), 0)
	return fmt.Sprintf("replayed %s offsets %d to %d to %s: acked %d of %d, in order; %d %s* headers dropped; next_from_offset %d (%d offsets left in the partition)",
		where, recs[0].Offset, end, p.ToTopic, acked, len(recs), dropped, connectErrorsPrefix, end+1, left), nil
}

// lastOffset is the offset of the last record; recs is never empty here.
func lastOffset(recs []clients.Record) int64 { return recs[len(recs)-1].Offset }
