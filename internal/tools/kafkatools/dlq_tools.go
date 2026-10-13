package kafkatools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/dlq"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// ---- dlq_peek ----

const (
	dlqDefaultLimit = 5
	dlqMaxLimit     = 20

	// headerPrefix is the only header namespace the tool looks at. The client
	// already drops everything else; the tool checks again, because a reader
	// other than the real client (a fake, a future one) must not be able to
	// carry an application header into a summary.
	headerPrefix = "__connect.errors."
)

// Connect error header names (KIP-298), without the prefix.
const (
	hTopic      = "topic"
	hPartition  = "partition"
	hOffset     = "offset"
	hConnector  = "connector.name"
	hTask       = "task.id"
	hStage      = "stage"
	hClassName  = "class.name"
	hExcClass   = "exception.class.name"
	hExcMessage = "exception.message"
	hExcTrace   = "exception.stacktrace"
)

// identRE is the shape of every header value shown in "classes" mode: a Java
// class name, a stage constant, a connector name, a task id. Anything else
// (a value somebody wrote into a header to carry text) is shown as "?" and
// counted as "other", so the fixed-vocabulary promise holds even when the
// writer of the DLQ record is not Connect.
var identRE = regexp.MustCompile(`^[A-Za-z0-9_$][A-Za-z0-9_$.:\-]{0,119}$`)

type dlqPeek struct{ d Deps }

func (t *dlqPeek) Name() string     { return "dlq_peek" }
func (t *dlqPeek) Kind() tools.Kind { return tools.KindRead }
func (t *dlqPeek) InputSchema() map[string]any {
	return objectSchema(map[string]any{
		"connector": strProp("a sink connector of this pipeline: its dead-letter topic is read from its config"),
		"topic":     strProp("a dead-letter topic declared for this pipeline in owners.yaml (watch.dlq_topics); give exactly one of connector or topic"),
		"limit": map[string]any{
			"type": "integer", "minimum": 1, "maximum": dlqMaxLimit,
			"description": "newest records to show in total across partitions; default 5, at most 20",
		},
	})
}
func (t *dlqPeek) Description() string {
	return "Peeks a dead-letter topic: record count, arrival rate and, for the newest records, the Kafka Connect error headers (exception class, stage, original topic/partition/offset, connector and task; exception messages only when the pipeline opts in). Never record values or keys. Give `connector` for a sink's DLQ or `topic` for a DLQ declared in owners.yaml."
}

type dlqPeekInput struct {
	Connector string `json:"connector"`
	Topic     string `json:"topic"`
	Limit     *int   `json:"limit"`
}

// parseInput validates the input shape without any I/O.
func (t *dlqPeek) parseInput(raw json.RawMessage) (in dlqPeekInput, limit int, err error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		return in, 0, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	switch {
	case in.Connector != "" && in.Topic != "":
		return in, 0, fmt.Errorf("%w: give exactly one of connector or topic, not both", tools.ErrBadInput)
	case in.Connector == "" && in.Topic == "":
		return in, 0, fmt.Errorf("%w: give exactly one of connector or topic", tools.ErrBadInput)
	}
	limit = dlqDefaultLimit
	if in.Limit != nil {
		if *in.Limit < 1 {
			return in, 0, fmt.Errorf("%w: limit must be between 1 and %d", tools.ErrBadInput, dlqMaxLimit)
		}
		limit = min(*in.Limit, dlqMaxLimit)
	}
	return in, limit, nil
}

// dlqData is Result.Data. It is stored permanently and shown to every room
// member, so it holds counts, positions and timestamps only: no per-record
// list and no text taken from a record, in any header mode.
type dlqData struct {
	Topic               string          `json:"topic"`
	Writer              string          `json:"writer,omitempty"`
	Declared            bool            `json:"declared"`
	Exists              bool            `json:"exists"`
	Partitions          int             `json:"partitions"`
	Records             int64           `json:"records"`
	StartOffsets        map[int32]int64 `json:"start_offsets,omitempty"`
	EndOffsets          map[int32]int64 `json:"end_offsets,omitempty"`
	NewestTimestamp     *time.Time      `json:"newest_timestamp,omitempty"`
	Sampled             int             `json:"sampled"`
	ExceptionClassCount map[string]int  `json:"exception_class_counts,omitempty"`
	OtherWriters        int             `json:"other_writers"`
	ContextHeaders      *bool           `json:"context_headers,omitempty"`
	HeaderMode          string          `json:"header_mode"`
	NewInWindow         *int64          `json:"new_in_window,omitempty"`
	Risks               []string        `json:"risks,omitempty"`
}

func (t *dlqPeek) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	in, limit, err := t.parseInput(input)
	if err != nil {
		return tools.Result{}, err
	}
	// Scope before any client call (F11).
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	if in.Connector != "" {
		err = sc.CheckConnector(in.Connector)
	} else {
		err = sc.CheckDLQTopic(in.Topic)
	}
	if err != nil {
		return tools.Result{}, err
	}
	rd, ok := t.d.Kafka.(clients.DLQReader)
	if !ok {
		return tools.Result{}, errors.New("kafkatools: dlq_peek: Kafka client cannot read DLQ topics")
	}

	cctx, cancel := withTimeout(ctx)
	defer cancel()

	data := dlqData{HeaderMode: headerMode(sc), Writer: in.Connector}
	var risks []dlq.Risk
	var ec dlq.ErrorsConfig
	topic := in.Topic
	if in.Connector != "" {
		if t.d.Connect == nil {
			return tools.Result{}, errors.New("kafkatools: dlq_peek: Connect client is not configured")
		}
		cfg, err := t.d.Connect.Config(cctx, in.Connector)
		if err != nil {
			return tools.Result{}, fmt.Errorf("kafkatools: dlq_peek %s: %w", in.Connector, err)
		}
		ec = dlq.Parse(cfg)
		hdr := ec.ContextHeaders
		data.ContextHeaders = &hdr
		topic = ec.Topic
		if topic == "" {
			// Not an error: "this connector has no DLQ" is an answer.
			risks = dlq.Risks(ec, nil)
			data.Risks = dlq.Codes(risks)
			var sb strings.Builder
			if ec.TopicInvalid {
				fmt.Fprintf(&sb, "%s has errors.deadletterqueue.topic.name set to something that is not a valid Kafka topic name, so no DLQ can be read", in.Connector)
			} else {
				fmt.Fprintf(&sb, "%s has no DLQ topic (errors.deadletterqueue.topic.name is not set)", in.Connector)
			}
			writeRisks(&sb, risks)
			return res(sb.String(), data), nil
		}
	} else {
		data.Declared = true
		data.Writer = declaredWriter(sc, topic)
	}
	data.Topic = topic

	offs, err := rd.DLQOffsets(cctx, topic)
	if err != nil {
		return tools.Result{}, fmt.Errorf("kafkatools: dlq_peek %s: %w", topic, err)
	}
	parts, exists := offs[topic]
	exists = exists && len(parts) > 0
	data.Exists = exists
	if in.Connector != "" {
		// Risks come from the config already read plus the topic reading.
		risks = dlq.Risks(ec, &exists)
		data.Risks = dlq.Codes(risks)
	}
	if t.d.DLQStats != nil {
		if n, _, ok := t.d.DLQStats(topic); ok {
			data.NewInWindow = &n
		}
	}

	if !exists {
		var sb strings.Builder
		fmt.Fprintf(&sb, "%s does not exist", describeTopic(topic, data))
		if data.Declared {
			sb.WriteString(" (a declared consumer DLQ is created when the application first needs it)")
		} else {
			sb.WriteString(" (Connect creates a sink DLQ when the task starts: the worker could not create it)")
		}
		writeRisks(&sb, risks)
		return res(sb.String(), data), nil
	}

	data.Partitions = len(parts)
	data.StartOffsets = map[int32]int64{}
	data.EndOffsets = map[int32]int64{}
	for _, p := range parts {
		data.Records += p.End - p.Start
		data.StartOffsets[p.Partition] = p.Start
		data.EndOffsets[p.Partition] = p.End
	}

	var recs []clients.DLQRecord
	if data.Records > 0 {
		recs, err = rd.PeekDLQ(cctx, topic, limit)
		if err != nil {
			return tools.Result{}, fmt.Errorf("kafkatools: dlq_peek %s: %w", topic, err)
		}
	}
	recs, data.OtherWriters = filterWriters(recs, sc)
	if len(recs) > limit {
		recs = recs[:limit]
	}
	data.Sampled = len(recs)
	withHeaders := 0
	data.ExceptionClassCount = map[string]int{}
	for _, r := range recs {
		h := connectHeaders(r.Headers)
		if len(h) > 0 {
			withHeaders++
		}
		if c, ok := h[hExcClass]; ok {
			data.ExceptionClassCount[ident(c)]++
		}
		if data.NewestTimestamp == nil || r.Timestamp.After(*data.NewestTimestamp) {
			ts := r.Timestamp
			data.NewestTimestamp = &ts
		}
	}
	if len(data.ExceptionClassCount) == 0 {
		data.ExceptionClassCount = nil
	}
	if data.ContextHeaders == nil && len(recs) > 0 {
		hdr := withHeaders > 0
		data.ContextHeaders = &hdr
	}

	return res(t.summary(topic, data, recs, withHeaders, sc, risks), data), nil
}

// headerMode is the pipeline's dlq_peek.headers setting; unknown or unscoped
// gives the default ("classes").
func headerMode(sc tools.Scope) string {
	switch m := sc.Pipeline().Watch().DLQPeekHeaders; m {
	case owners.DLQHeadersCountsOnly, owners.DLQHeadersClasses, owners.DLQHeadersMessages:
		return m
	}
	return owners.DLQHeadersClasses
}

// declaredWriter returns written_by of a declared DLQ topic.
func declaredWriter(sc tools.Scope, topic string) string {
	for _, d := range sc.Pipeline().Watch().DLQTopics {
		if d.Topic == topic {
			return d.WrittenBy
		}
	}
	return ""
}

// connectHeaders keeps the __connect.errors.* headers, with the prefix removed.
func connectHeaders(h map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range h {
		if name, ok := strings.CutPrefix(k, headerPrefix); ok {
			out[name] = v
		}
	}
	return out
}

// filterWriters drops records whose connector.name header names a connector
// that is not in this pipeline (noise control for a shared DLQ topic, not a
// security boundary: the header is set by whoever wrote the record). Records
// without the header stay. Unscoped callers see everything.
func filterWriters(recs []clients.DLQRecord, sc tools.Scope) ([]clients.DLQRecord, int) {
	if sc.Unscoped() {
		return recs, 0
	}
	own := map[string]bool{}
	for _, c := range sc.Pipeline().Connectors {
		own[c] = true
	}
	kept := make([]clients.DLQRecord, 0, len(recs))
	other := 0
	for _, r := range recs {
		if c, ok := connectHeaders(r.Headers)[hConnector]; ok && !own[c] {
			other++
			continue
		}
		kept = append(kept, r)
	}
	return kept, other
}

// ident returns v when it has the shape of an identifier and "?" otherwise.
func ident(v string) string {
	if identRE.MatchString(v) {
		return v
	}
	return "?"
}

func describeTopic(topic string, d dlqData) string {
	if d.Writer != "" {
		return fmt.Sprintf("%s (DLQ of %s)", topic, d.Writer)
	}
	return topic + " (DLQ)"
}

func writeRisks(sb *strings.Builder, risks []dlq.Risk) {
	for _, r := range risks {
		fmt.Fprintf(sb, "\nrisk %s (%s): %s", r.Code, r.Sev, r.Text)
	}
}

// summary builds what the model reads. The fixed lines come first and the
// per-record lines last, so a truncation cuts records, not the counts or risks.
func (t *dlqPeek) summary(topic string, d dlqData, recs []clients.DLQRecord, withHeaders int, sc tools.Scope, risks []dlq.Risk) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s: %d records in %d partitions", describeTopic(topic, d), d.Records, d.Partitions)
	switch {
	case d.NewInWindow != nil:
		fmt.Fprintf(&sb, ", %d new in the watcher's window", *d.NewInWindow)
	default:
		sb.WriteString(", arrival rate unknown (no watcher in this process)")
	}
	if d.NewestTimestamp != nil {
		fmt.Fprintf(&sb, "; newest %s", d.NewestTimestamp.UTC().Format(time.RFC3339))
	}
	if d.OtherWriters > 0 {
		fmt.Fprintf(&sb, "\n%d sampled records written by other connectors were left out", d.OtherWriters)
	}
	if len(recs) == 0 {
		if d.Records > 0 {
			sb.WriteString("\nno records to show after filtering")
		}
		writeRisks(&sb, risks)
		return sb.String()
	}

	if len(d.ExceptionClassCount) > 0 {
		names := make([]string, 0, len(d.ExceptionClassCount))
		for n := range d.ExceptionClassCount {
			names = append(names, n)
		}
		sort.Slice(names, func(i, j int) bool {
			if d.ExceptionClassCount[names[i]] != d.ExceptionClassCount[names[j]] {
				return d.ExceptionClassCount[names[i]] > d.ExceptionClassCount[names[j]]
			}
			return names[i] < names[j]
		})
		parts := make([]string, 0, len(names))
		for _, n := range names {
			parts = append(parts, fmt.Sprintf("%s x%d", n, d.ExceptionClassCount[n]))
		}
		fmt.Fprintf(&sb, "\nexception classes in the sample of %d: %s", len(recs), strings.Join(parts, ", "))
	}
	switch d.HeaderMode {
	case owners.DLQHeadersCountsOnly:
		sb.WriteString("\nheaders: counts_only (owners.yaml watch.dlq_peek.headers); positions and timestamps only")
	case owners.DLQHeadersMessages:
		sb.WriteString("\nheaders: messages (owners.yaml); exception messages are redacted and capped")
	default:
		sb.WriteString("\nheaders: classes (set watch.dlq_peek.headers: messages in owners.yaml to see redacted exception messages)")
	}
	if withHeaders == 0 {
		sb.WriteString("\nno Connect error headers on these records; for a sink set errors.deadletterqueue.context.headers.enable=true (a proposable change)")
		if !sc.Unscoped() {
			sb.WriteString("; records of other connectors may be included")
		}
	}
	writeRisks(&sb, risks)

	fmt.Fprintf(&sb, "\nnewest %d:", len(recs))
	for _, r := range recs {
		sb.WriteString("\n  ")
		sb.WriteString(recordLine(r, d.HeaderMode))
	}
	return sb.String()
}

// recordLine renders one record for the mode. In counts_only it holds the
// partition, offset and timestamp. In classes it adds header values that pass
// identRE (or are numbers). Messages mode adds the redacted free text.
func recordLine(r clients.DLQRecord, mode string) string {
	line := fmt.Sprintf("p%d@%d %s", r.Partition, r.Offset, r.Timestamp.UTC().Format(time.RFC3339))
	if mode == owners.DLQHeadersCountsOnly {
		return line
	}
	h := connectHeaders(r.Headers)
	if v, ok := h[hExcClass]; ok {
		line += " " + ident(v)
	}
	if v, ok := h[hStage]; ok {
		line += " stage=" + ident(v)
	}
	if v, ok := h[hClassName]; ok {
		line += " class=" + ident(v)
	}
	if v, ok := h[hConnector]; ok {
		line += " connector=" + ident(v)
	}
	if v, ok := h[hTask]; ok {
		line += " task=" + numOrQ(v)
	}
	if orig := origin(h); orig != "" {
		line += " from " + orig
	}
	if mode == owners.DLQHeadersMessages {
		// The client redacts these already; redacting again is idempotent and
		// keeps a reader that did not redact from leaking record bytes.
		if v, ok := h[hExcMessage]; ok && strings.TrimSpace(v) != "" {
			line += fmt.Sprintf(" msg=%q", dlq.RedactMessage(v))
		}
		if v, ok := h[hExcTrace]; ok && strings.TrimSpace(v) != "" {
			first, _, _ := strings.Cut(v, "\n")
			line += fmt.Sprintf(" trace=%q", dlq.RedactMessage(first))
		}
	}
	return line
}

// origin renders the original topic/partition@offset, or "" when the headers
// do not carry a well-formed one.
func origin(h map[string]string) string {
	tp, ok := h[hTopic]
	if !ok || !dlq.ValidTopic(tp) {
		return ""
	}
	out := tp
	if p, ok := h[hPartition]; ok {
		out += "/" + numOrQ(p)
	}
	if o, ok := h[hOffset]; ok {
		out += "@" + numOrQ(o)
	}
	return out
}

// numOrQ returns v when it is a non-negative integer and "?" otherwise.
func numOrQ(v string) string {
	if n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 63); err == nil {
		return strconv.FormatUint(n, 10)
	}
	return "?"
}
