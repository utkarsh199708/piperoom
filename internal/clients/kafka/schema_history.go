package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/redact"
)

// Q8e: the tail of a Debezium schema history topic (docs/design/mysql-source.md
// §7.3). A MySQL (and later SQL Server, Oracle) connector keeps the DDL it has
// applied in this topic, so it is where a MySQL pipeline's DDL history lives.
// Each record value is JSON written by the connector: {"source": {...},
// "position": {file, pos, gtids, ...}, "ts_ms": ..., "databaseName": ...,
// "schemaName": ..., "ddl": "...", "tableChanges": [...]}. Only the fields
// below ever leave the client, cleaned and capped; tableChanges (the whole
// table schema, with column defaults that can hold literal values) never does.

// SchemaHistoryReader is optional on the Kafka client (asserted at use time,
// like ConnectClusterReader): the kafka_schema_history tool reads the newest
// records of the history topic an in-scope connector's config names. It is
// declared here, not in clients.go, until Q0 moves it (the Q8e contract
// change request). The caller scopes topic before the call; deadline 10 s.
type SchemaHistoryReader interface {
	// SchemaHistoryTail returns the newest records of topic, newest first, at
	// most limit (<= 0 means DefaultHistoryRecords, capped at
	// MaxHistoryRecords) and at most 8 MiB of record bytes read. An unknown
	// topic wraps clients.ErrNotFound. A deadline after at least one record
	// returns what was read with a nil error.
	SchemaHistoryTail(ctx context.Context, topic string, limit int) ([]SchemaHistoryRecord, error)
}

var _ SchemaHistoryReader = (*Client)(nil)

// Caps of a schema history read.
const (
	// DefaultHistoryRecords and MaxHistoryRecords bound SchemaHistoryTail.
	DefaultHistoryRecords = 10
	MaxHistoryRecords     = 20
	// MaxHistoryDDLBytes caps SchemaHistoryRecord.DDL: the statement is
	// untrusted text and can carry literals (DEFAULT values, comments).
	MaxHistoryDDLBytes = 200
	// maxHistoryValueBytes is the largest record value decoded; a larger one
	// (a huge tableChanges) is returned as Undecoded rather than parsed.
	maxHistoryValueBytes = 4 << 20
	// maxHistoryDDLScan bounds the DDL text redacted before the cut.
	maxHistoryDDLScan = 64 << 10
	// maxHistoryPositionKeys, maxHistoryPositionString cap Position.
	maxHistoryPositionKeys   = 16
	maxHistoryPositionKey    = 64
	maxHistoryPositionString = 1024
	maxHistoryNameBytes      = 128
)

// SchemaHistoryRecord is one schema history record as it may leave the
// client: where it is, when the connector wrote it, the database and schema
// it applies to, the source position it was read at (scalars only) and the
// start of its DDL, redacted. Everything but the Kafka coordinates is
// untrusted text written by the connector from the source's binlog.
type SchemaHistoryRecord struct {
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
	// TsMs is the record's ts_ms: when the connector applied the DDL.
	TsMs         int64  `json:"ts_ms,omitempty"`
	DatabaseName string `json:"database_name,omitempty"`
	SchemaName   string `json:"schema_name,omitempty"`
	// Position is the record's source position (MySQL: file, pos, gtids,
	// ts_sec, server_id, ...): scalar values only, strings cleaned and at
	// most 1 KiB; nested values are dropped and counted in PositionOmitted.
	Position        map[string]any `json:"position,omitempty"`
	PositionOmitted int            `json:"position_omitted,omitempty"`
	// DDL is the statement, control characters turned into spaces, redacted
	// (internal/redact) and cut to MaxHistoryDDLBytes; DDLCut says it was cut.
	DDL    string `json:"ddl,omitempty"`
	DDLCut bool   `json:"ddl_cut,omitempty"`
	// Undecoded marks a value that is not a JSON history record (or too large
	// to decode): only its coordinates are known.
	Undecoded bool `json:"undecoded,omitempty"`
}

// SchemaHistoryTail reads the newest records of a schema history topic. It
// plans like PeekDLQ (the tail of every partition, against the last stable
// offset) and decodes each value into the SchemaHistoryRecord fields only.
func (c *Client) SchemaHistoryTail(ctx context.Context, topic string, limit int) ([]SchemaHistoryRecord, error) {
	if strings.TrimSpace(topic) == "" {
		return nil, fmt.Errorf("kafka: schema history: empty topic")
	}
	switch {
	case limit <= 0:
		limit = DefaultHistoryRecords
	case limit > MaxHistoryRecords:
		limit = MaxHistoryRecords
	}
	ctx, cancel := withDefaultTimeout(ctx, dlqCallTimeout)
	defer cancel()

	offs, err := c.DLQOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: schema history %s: %w", topic, err)
	}
	parts, ok := offs[topic]
	if !ok {
		return nil, fmt.Errorf("kafka: schema history %s: %w", topic, clients.ErrNotFound)
	}
	parts, err = c.clampToStable(ctx, topic, parts)
	if err != nil {
		return nil, fmt.Errorf("kafka: schema history %s: %w", topic, err)
	}
	plan := planPeek(parts, limit)
	if len(plan) == 0 {
		return []SchemaHistoryRecord{}, nil
	}
	startAt := map[int32]kgo.Offset{}
	for _, pl := range plan {
		startAt[pl.partition] = kgo.NewOffset().At(pl.from)
	}
	cons, err := c.newConsumer(map[string]map[int32]kgo.Offset{topic: startAt})
	if err != nil {
		return nil, fmt.Errorf("kafka: schema history %s: consumer: %w", topic, err)
	}
	defer cons.Close()

	pollStart := time.Now()
	pr := c.newProber(topic)
	recs, err := collectHistory(ctx, cons, plan, func(w map[int32]readWindow) { pr.stalled(ctx, w) })
	c.record(ctx, pollStart, "kgo.PollFetches(topic, limit)", "topic="+topic, "limit="+strconv.Itoa(limit))
	pr.record(ctx)
	if err != nil {
		return nil, fmt.Errorf("kafka: schema history %s: %w", topic, err)
	}
	sort.Slice(recs, func(i, j int) bool {
		a, b := recs[i], recs[j]
		if !a.Timestamp.Equal(b.Timestamp) {
			return a.Timestamp.After(b.Timestamp)
		}
		if a.Partition != b.Partition {
			return a.Partition < b.Partition
		}
		return a.Offset > b.Offset
	})
	if len(recs) > limit {
		recs = recs[:limit]
	}
	return recs, nil
}

// collectHistory is collectPeek's loop for history records: poll until every
// planned partition reached its end, 8 MiB were read, or ctx ends; markers
// count toward "done" and are never returned. Each *kgo.Record is dropped
// after historyRecordFrom took the allowed fields.
func collectHistory(ctx context.Context, cons pollConsumer, plan []peekPart, probe func(map[int32]readWindow)) ([]SchemaHistoryRecord, error) {
	ends := map[int32]int64{}
	wait := map[int32]readWindow{}
	for _, pl := range plan {
		ends[pl.partition] = pl.end
		wait[pl.partition] = readWindow{from: pl.from, end: pl.end}
	}
	done := map[int32]bool{}
	var (
		out   []SchemaHistoryRecord
		total int
	)
	short := func(cause error) ([]SchemaHistoryRecord, error) {
		slog.DebugContext(ctx, "kafka: schema history read cut short",
			"partitions_planned", len(ends), "partitions_done", len(done), "records", len(out))
		if len(out) > 0 {
			return out, nil
		}
		return nil, fmt.Errorf("read incomplete after 0 records: %w", cause)
	}
	for len(done) < len(ends) {
		fetches := pollBounded(ctx, cons)
		if fetches.IsClientClosed() {
			return nil, errors.New("consumer closed")
		}
		for _, fe := range fetches.Errors() {
			var loss *kgo.ErrDataLoss
			if errors.As(fe.Err, &loss) {
				continue
			}
			if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
				return short(fe.Err)
			}
			return nil, fmt.Errorf("fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err)
		}
		for _, r := range fetches.Records() {
			end, planned := ends[r.Partition]
			if !planned {
				continue
			}
			if r.Offset+1 >= end {
				done[r.Partition] = true
				delete(wait, r.Partition)
			} else if w, ok := wait[r.Partition]; ok && r.Offset >= w.from {
				w.from = r.Offset + 1
				wait[r.Partition] = w
			}
			if r.Offset >= end || r.Attrs.IsControl() {
				continue
			}
			total += recordSize(r)
			out = append(out, historyRecordFrom(r))
			if total >= maxPeekBytes {
				return out, nil
			}
		}
		probe(wait)
		for p, w := range wait {
			if w.from >= w.end {
				done[p] = true
				delete(wait, p)
			}
		}
		if err := ctx.Err(); err != nil && len(done) < len(ends) {
			return short(err)
		}
	}
	return out, nil
}

// historyValue is the part of a history record value that is decoded; every
// other field (source, tableChanges) is skipped by the decoder.
type historyValue struct {
	Position     map[string]any `json:"position"`
	TsMs         json.Number    `json:"ts_ms"`
	DatabaseName string         `json:"databaseName"`
	SchemaName   string         `json:"schemaName"`
	DDL          string         `json:"ddl"`
}

// historyRecordFrom builds the DTO from one record. The key is never read.
func historyRecordFrom(r *kgo.Record) SchemaHistoryRecord {
	out := SchemaHistoryRecord{Partition: r.Partition, Offset: r.Offset, Timestamp: r.Timestamp}
	if len(r.Value) == 0 || len(r.Value) > maxHistoryValueBytes {
		out.Undecoded = true
		return out
	}
	var v historyValue
	dec := json.NewDecoder(bytes.NewReader(r.Value))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		out.Undecoded = true
		return out
	}
	if n, err := v.TsMs.Int64(); err == nil && n > 0 {
		out.TsMs = n
	}
	out.DatabaseName = cleanName(v.DatabaseName)
	out.SchemaName = cleanName(v.SchemaName)
	out.Position, out.PositionOmitted = historyPosition(v.Position)
	out.DDL, out.DDLCut = historyDDL(v.DDL)
	return out
}

// cleanName cleans a database or schema name for display.
func cleanName(s string) string {
	s = cleanLine(s)
	if len(s) > maxHistoryNameBytes {
		s = cutUTF8(s, maxHistoryNameBytes)
	}
	return s
}

// historyPosition keeps the scalar entries of a position map, cleaned and
// capped; it returns how many it dropped.
func historyPosition(m map[string]any) (map[string]any, int) {
	if len(m) == 0 {
		return nil, 0
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := map[string]any{}
	omitted := 0
	for _, k := range keys {
		if len(out) >= maxHistoryPositionKeys || len(k) > maxHistoryPositionKey || cleanLine(k) != k {
			omitted++
			continue
		}
		switch x := m[k].(type) {
		case json.Number:
			if len(x.String()) > 32 {
				omitted++
				continue
			}
			out[k] = x
		case string:
			s := cleanLine(x)
			if len(s) > maxHistoryPositionString {
				s = cutUTF8(s, maxHistoryPositionString)
			}
			s, _ = redact.Text(s)
			out[k] = s
		case bool:
			out[k] = x
		case nil:
			out[k] = nil
		default:
			omitted++
		}
	}
	return out, omitted
}

// historyDDL cleans, redacts and cuts a DDL statement. Redaction runs on
// the text before the cut, so a secret straddling the cut is masked whole.
func historyDDL(s string) (string, bool) {
	if s == "" {
		return "", false
	}
	cut := false
	if len(s) > maxHistoryDDLScan {
		s, cut = cutUTF8(s, maxHistoryDDLScan), true
	}
	s = strings.Join(strings.Fields(cleanLine(s)), " ")
	s = ddlPasswordRe.ReplaceAllString(s, "${1}'[masked]'")
	s, _ = redact.Text(s)
	if len(s) > MaxHistoryDDLBytes {
		s, cut = cutUTF8(s, MaxHistoryDDLBytes), true
	}
	return s, cut
}

// ddlPasswordRe finds the literal of a MySQL account statement's password
// (CREATE/ALTER USER ... IDENTIFIED [WITH plugin] BY|AS '...', SET PASSWORD [FOR u] =
// '...', PASSWORD('...')), which the generic redactor does not read as a
// credential; an unterminated literal (cut) is masked to the end.
var ddlPasswordRe = regexp.MustCompile(`(?i)(IDENTIFIED\s+(?:WITH\s+\S+\s+)?(?:BY|AS)\s+|PASSWORD\s*(?:FOR\s+\S+\s*)?(?:=\s*|\(\s*))(?:'(?:[^'\\]|\\.|'')*'|"(?:[^"\\]|\\.|"")*"|['"].*$)`)

// cleanLine makes text valid UTF-8 with every control character a space.
func cleanLine(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) {
			return ' '
		}
		return r
	}, strings.ToValidUTF8(s, "�"))
}

// cutUTF8 keeps at most n bytes of s on a rune boundary.
func cutUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
