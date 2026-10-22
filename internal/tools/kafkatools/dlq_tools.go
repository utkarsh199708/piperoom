package kafkatools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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

	// headerPrefix is the Connect header namespace the tool reads with
	// connectHeaders (plus the single reject-rule header, read by rejectRule).
	// The client already drops everything else; the tool checks again, because
	// a reader other than the real client (a fake, a future one) must not be
	// able to carry an application header into a summary.
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

// hRejectRule is Piperoom's own DLQ header: the name of the rule that
// rejected the record ("status_enum"), never the rejected value. It sits
// outside Connect's prefix, so it is read with its own lookup rather than
// connectHeaders, and it is shown in every header mode: a rule name is fixed
// vocabulary, not record text (KAN-412).
const hRejectRule = "x-piperoom-reject-rule"

// Header values are written by whoever produced the DLQ record, so each field
// is checked against the narrowest shape its real values have. A value that
// fails is shown as "?": the fixed-vocabulary promise holds even when the
// writer is not Connect, and a digit string, hex or base64 cannot pass for a
// Java class name.
var (
	// fqcnRE is a Java fully qualified class name.
	fqcnRE = regexp.MustCompile(`^([A-Za-z_$][A-Za-z0-9_$]*\.)*[A-Za-z_$][A-Za-z0-9_$]*$`)
	// identRE is a plain name, for a connector name when there is no
	// pipeline to compare it with.
	identRE = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_$.\-]{0,119}$`)
	// ruleRE is a reject-rule name: an identifier of at most 64 characters,
	// starting with a letter. No spaces, quotes, angle brackets or dots, so a
	// record value (a card number, an e-mail address, markup) cannot pass for a
	// rule name and the most restrictive header mode stays free text (KAN-412).
	ruleRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)
)

const (
	maxClassNameLen = 200
	maxExcClassLen  = 120
)

// connectStages are the values of __connect.errors.stage
// (org.apache.kafka.connect.runtime.errors.Stage).
var connectStages = map[string]bool{
	"KEY_CONVERTER": true, "VALUE_CONVERTER": true, "HEADER_CONVERTER": true, "TRANSFORMATION": true,
	"TASK_PUT": true, "TASK_POLL": true, "KAFKA_PRODUCE": true, "KAFKA_CONSUME": true,
}

// className returns v when it is a Java class name, "?" otherwise.
func className(v string) string {
	if len(v) > maxClassNameLen || !fqcnRE.MatchString(v) {
		return "?"
	}
	return v
}

// operatorClassName is className plus the shape of a real operator class (a
// converter, a transformation): a package, then a capitalised class name. A
// bare word, hex or base64 string has no package and is refused. A dotted,
// capitalised name made up by a writer still passes; that is the residual the
// PR states.
func operatorClassName(v string) string {
	if className(v) == "?" {
		return "?"
	}
	i := strings.LastIndexByte(v, '.')
	if i < 1 || i == len(v)-1 {
		return "?"
	}
	if c := v[i+1]; c < 'A' || c > 'Z' {
		return "?"
	}
	return v
}

// excClassName is className plus the rule that a throwable's simple name ends
// in Exception, Error or Throwable.
func excClassName(v string) string {
	// Connect writes getClass().getName(), so a real value always has a
	// package; a bare word is not one. 120 is generous for a real name.
	if len(v) > maxExcClassLen || !strings.Contains(v, ".") || className(v) == "?" {
		return "?"
	}
	last := v[strings.LastIndexByte(v, '.')+1:]
	for _, suf := range []string{"Exception", "Error", "Throwable"} {
		if strings.HasSuffix(last, suf) {
			return v
		}
	}
	return "?"
}

// stageName returns v when it is one of Connect's stages, "?" otherwise.
func stageName(v string) string {
	if connectStages[v] {
		return v
	}
	return "?"
}

// rejectRule returns the reject-rule name of one record, or "" when the record
// carries no such header (absent is not an error: a Connect sink writes none).
// The writer is untrusted, so the value is redacted with the same rule as an
// exception message and then checked against ruleRE. A value that is not a
// plain name is shown as "?", so a writer that puts record text in this header
// cannot bring free text into a counts_only summary.
func rejectRule(h map[string]string) string {
	v := strings.TrimSpace(h[hRejectRule])
	if v == "" {
		return ""
	}
	// The client redacts already; redacting again is idempotent and keeps a
	// reader that did not redact from leaking record bytes.
	v = dlq.RedactMessage(v)
	if !ruleRE.MatchString(v) {
		return "?"
	}
	return v
}

type dlqPeek struct{ d Deps }

func (t *dlqPeek) Name() string     { return "dlq_peek" }
func (t *dlqPeek) Kind() tools.Kind { return tools.KindRead }
func (t *dlqPeek) InputSchema() map[string]any {
	return objectSchema(map[string]any{
		"connector": strProp("a sink connector of this pipeline: its dead-letter topic is read from its config"),
		"topic":     strProp("a dead-letter topic declared for this pipeline in owners.yaml (watch.dlq_topics); give exactly one of connector or topic"),
		// The range is in the description: strict tool use rejects
		// minimum/maximum, so parseInput enforces it.
		"limit": map[string]any{
			"type":        "integer",
			"description": "newest records to show in total across partitions, 1 to 20; default 5",
		},
	})
}
func (t *dlqPeek) Description() string {
	return "Peeks a dead-letter topic: record count, arrival rate and, for the newest records, the Kafka Connect error headers (exception class, stage, original topic/partition/offset, connector and task; exception messages only when the pipeline opts in) plus the value-free reject rule (x-piperoom-reject-rule, e.g. status_enum) when the consumer writes one. Never record values or keys. Give `connector` for a sink's DLQ or `topic` for a DLQ declared in owners.yaml."
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
	Window              string          `json:"window,omitempty"`
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
		// Scoped, CheckDLQTopic passed above, so the topic is declared.
		// Unscoped (CLI, dev) the declaration is not known.
		data.Declared = !sc.Unscoped()
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
		if n, w, ok := t.d.DLQStats(topic); ok {
			data.NewInWindow = &n
			data.Window = windowText(w)
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
			data.ExceptionClassCount[excClassName(c)]++
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

// connectorName shows a connector.name header only when it is one of the
// pipeline's connectors (scoped) or has the shape of a name (unscoped).
func connectorName(v string, sc tools.Scope) string {
	if sc.Unscoped() {
		if identRE.MatchString(v) {
			return v
		}
		return "?"
	}
	if sc.CheckConnector(v) == nil {
		return v
	}
	return "?"
}

// windowText renders the watcher's window ("1h", "90m").
func windowText(w time.Duration) string {
	switch {
	case w >= time.Hour && w%time.Hour == 0:
		return fmt.Sprintf("%dh", int(w/time.Hour))
	case w >= time.Minute && w%time.Minute == 0:
		return fmt.Sprintf("%dm", int(w/time.Minute))
	}
	return w.String()
}

// firstLine cuts s at the first line break of any kind (\r, \n, U+0085,
// U+2028, U+2029, and the vertical tab and form feed): a stack trace must not
// bring a second line into the summary through an unusual separator.
func firstLine(s string) string {
	if i := strings.IndexFunc(s, func(r rune) bool {
		switch r {
		case '\r', '\n', '\v', '\f', '\u0085', '\u2028', '\u2029':
			return true
		}
		return false
	}); i >= 0 {
		return s[:i]
	}
	return s
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
		fmt.Fprintf(&sb, ", %d new in the last %s", *d.NewInWindow, d.Window)
	default:
		sb.WriteString(", last hour: unknown (no watcher in this process)")
	}
	if d.NewestTimestamp != nil {
		fmt.Fprintf(&sb, "; newest %s", d.NewestTimestamp.UTC().Format(time.RFC3339))
	}
	if d.OtherWriters > 0 {
		// The record count and rate above are for the whole topic, and the
		// filter runs after PeekDLQ(limit): the newest records may all belong
		// to other connectors, leaving fewer than limit to show.
		fmt.Fprintf(&sb, "\n%d sampled records written by other connectors were left out; counts above are for the whole topic, which other connectors share", d.OtherWriters)
	}
	if !sc.Unscoped() {
		noName := 0
		for _, r := range recs {
			if _, ok := connectHeaders(r.Headers)[hConnector]; !ok {
				noName++
			}
		}
		if noName > 0 {
			fmt.Fprintf(&sb, "\n%d shown records have no connector.name header, so they may belong to another connector", noName)
		}
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
	hasRule := false
	for _, r := range recs {
		if rule := rejectRule(r.Headers); rule != "" && rule != "?" {
			hasRule = true
			break
		}
	}
	switch d.HeaderMode {
	case owners.DLQHeadersCountsOnly:
		if hasRule {
			sb.WriteString("\nheaders: counts_only (owners.yaml watch.dlq_peek.headers); positions, timestamps and the reject-rule name")
		} else {
			sb.WriteString("\nheaders: counts_only (owners.yaml watch.dlq_peek.headers); positions and timestamps only")
		}
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
		sb.WriteString(recordLine(r, d.HeaderMode, sc))
	}
	return sb.String()
}

// recordLine renders one record for the mode. In counts_only it holds the
// partition, offset and timestamp. In classes it adds header values that pass
// identRE (or are numbers). Messages mode adds the redacted free text. The
// value-free reject rule (x-piperoom-reject-rule) is shown in every mode,
// counts_only included, because naming the rule that fired is the answer the
// model needs and a rule name is not record text (KAN-412).
func recordLine(r clients.DLQRecord, mode string, sc tools.Scope) string {
	line := fmt.Sprintf("p%d@%d %s", r.Partition, r.Offset, r.Timestamp.UTC().Format(time.RFC3339))
	if rule := rejectRule(r.Headers); rule != "" {
		line += " rule=" + rule
	}
	if mode == owners.DLQHeadersCountsOnly {
		return line
	}
	h := connectHeaders(r.Headers)
	if v, ok := h[hExcClass]; ok {
		line += " " + excClassName(v)
	}
	if v, ok := h[hStage]; ok {
		line += " stage=" + stageName(v)
	}
	if v, ok := h[hClassName]; ok {
		line += " class=" + operatorClassName(v)
	}
	if v, ok := h[hConnector]; ok {
		line += " connector=" + connectorName(v, sc)
	}
	if v, ok := h[hTask]; ok {
		line += " task=" + numOrQ(v, maxTaskID)
	}
	if orig := origin(h, sc); orig != "" {
		line += " from " + orig
	}
	if mode == owners.DLQHeadersMessages {
		// The client redacts these already; redacting again is idempotent and
		// keeps a reader that did not redact from leaking record bytes.
		if v, ok := h[hExcMessage]; ok && strings.TrimSpace(v) != "" {
			line += fmt.Sprintf(" msg=%q", dlq.RedactMessage(v))
		}
		if v, ok := h[hExcTrace]; ok && strings.TrimSpace(v) != "" {
			line += fmt.Sprintf(" trace=%q", dlq.RedactMessage(firstLine(v)))
		}
	}
	return line
}

// origin renders the original topic/partition@offset, or "" when the headers
// do not carry a well-formed one.
func origin(h map[string]string, sc tools.Scope) string {
	tp, ok := h[hTopic]
	if !ok || !dlq.ValidTopic(tp) {
		return ""
	}
	// The topic is shown only when it is one of this pipeline's topics: the
	// header is writer-controlled and could name anything.
	out := "?"
	if sc.CheckTopic(tp) == nil {
		out = tp
	}
	if p, ok := h[hPartition]; ok {
		out += "/" + numOrQ(p, maxPartition)
	}
	if o, ok := h[hOffset]; ok {
		out += "@" + numOrQ(o, maxOffset)
	}
	return out
}

// numOrQ returns v when it is a non-negative integer no larger than max and
// "?" otherwise. The bound keeps a long digit string (a card number written
// into task.id or partition) out of the summary.
func numOrQ(v string, max uint64) string {
	if n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 63); err == nil && n <= max {
		return strconv.FormatUint(n, 10)
	}
	return "?"
}

// Bounds for the numeric headers. Offsets are not bounded below int64: a real
// offset can be any int64.
const (
	maxTaskID    = 9999
	maxPartition = math.MaxInt32
	maxOffset    = math.MaxInt64
)
