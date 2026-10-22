package owners

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"piperoom/internal/dlq"
)

// DeclaredRetention is the optional per-pipeline `declared_retention:` block
// (X7, docs/design/lineage.md section 4): how long the data owner promised to
// keep each topic's records and each table's rows. The watcher compares it
// with the configured retention (X7d); the topology shows it as a tag. Values
// are topology.ParseRetention text ("7d", "36h", "infinite"); owners cannot
// import topology (topology imports owners), so validRetention below follows
// the same grammar and a watch test checks the two agree.
type DeclaredRetention struct {
	// Topics maps a Kafka topic name (a CDC or DLQ topic of this pipeline;
	// whether it is one is checked at run time) to its declared retention.
	Topics map[string]string `yaml:"topics"`
	// Tables maps one of the pipeline's source_tables or sink_tables to its
	// declared retention.
	Tables map[string]string `yaml:"tables"`
}

// MaxDeclaredRetention caps the entries of one pipeline's
// declared_retention (topics and tables together).
const MaxDeclaredRetention = 200

// maxRetention is topology.MaxRetention (100 years).
const maxRetention = 100 * 365 * 24 * time.Hour

// retentionInfinite is topology.RetentionInfinite.
const retentionInfinite = "infinite"

// validateDeclaredRetention checks the block: every value a retention, every
// topic key a valid Kafka topic name, every table key one of the pipeline's
// tables, at most MaxDeclaredRetention entries. Errors name the entry.
func (p *Pipeline) validateDeclaredRetention() error {
	d := p.DeclaredRetention
	if d == nil {
		return nil
	}
	if n := len(d.Topics) + len(d.Tables); n > MaxDeclaredRetention {
		return fmt.Errorf("declared_retention: %d entries, at most %d", n, MaxDeclaredRetention)
	}
	for _, topic := range slices.Sorted(maps.Keys(d.Topics)) {
		if !dlq.ValidTopic(topic) {
			return fmt.Errorf("declared_retention.topics %q: not a valid Kafka topic name", topic)
		}
		if err := validRetention(d.Topics[topic]); err != nil {
			return fmt.Errorf("declared_retention.topics %q: %w", topic, err)
		}
	}
	for _, table := range slices.Sorted(maps.Keys(d.Tables)) {
		if !slices.Contains(p.SourceTables, table) && !slices.Contains(p.SinkTables, table) {
			return fmt.Errorf("declared_retention.tables %q: not one of the pipeline's source_tables or sink_tables", table)
		}
		if err := validRetention(d.Tables[table]); err != nil {
			return fmt.Errorf("declared_retention.tables %q: %w", table, err)
		}
	}
	return nil
}

// DeclaredTopicRetention is the declared retention text of topic, if any.
func (p Pipeline) DeclaredTopicRetention(topic string) (string, bool) {
	if p.DeclaredRetention == nil {
		return "", false
	}
	v, ok := p.DeclaredRetention.Topics[topic]
	return v, ok
}

// DeclaredTableRetention is the declared retention text of table, if any.
func (p Pipeline) DeclaredTableRetention(table string) (string, bool) {
	if p.DeclaredRetention == nil {
		return "", false
	}
	v, ok := p.DeclaredRetention.Tables[table]
	return v, ok
}

// validRetention accepts what topology.ParseRetention accepts: "infinite",
// or a whole number >= 1 followed by d, h, m or s, at most 100 years.
func validRetention(s string) error {
	if s == retentionInfinite {
		return nil
	}
	bad := fmt.Errorf("retention %q: want <n>d, <n>h, <n>m or <n>s (n >= 1, at most 100 years), or %q", s, retentionInfinite)
	if len(s) < 2 {
		return bad
	}
	var unit time.Duration
	switch s[len(s)-1] {
	case 'd':
		unit = 24 * time.Hour
	case 'h':
		unit = time.Hour
	case 'm':
		unit = time.Minute
	case 's':
		unit = time.Second
	default:
		return bad
	}
	digits := s[:len(s)-1]
	for _, r := range digits {
		if r < '0' || r > '9' {
			return bad
		}
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || n < 1 || n > int64(maxRetention/unit) {
		return bad
	}
	return nil
}
