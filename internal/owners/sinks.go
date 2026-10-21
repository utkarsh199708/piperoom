package owners

import (
	"fmt"
	"strings"
)

// Per-sink writers (KAN-481).
//
// A topic can have more than one independent consumer: an application group
// that writes Postgres, a ClickHouse Kafka-engine table that fills a
// ClickHouse table, and so on. The legacy consumer_groups + sink_tables form
// links every group to every sink table, so with two sinks the map cannot say
// which writer feeds which table and the agent attributes a stalled sink to the
// wrong consumer. The optional sinks: block declares the pairing:
//
//	sinks:
//	  - table: analytics.orders
//	    warehouse: postgres
//	    writer: {kind: consumer_group, name: order-sync}
//	  - table: analytics_ch.orders
//	    warehouse: clickhouse
//	    writer: {kind: kafka_engine, name: analytics.orders_kafka}
//
// The legacy form still loads and means one sink per table, as before. A
// pipeline uses one form or the other: sinks: replaces sink_tables:. Extra
// consumer_groups: may still be listed beside sinks: (readers that write no
// sink); they are not paired with a table.

// Writer kinds of a sinks: entry.
const (
	// SinkWriterConsumerGroup is a Kafka consumer group that writes the sink
	// table itself (an application consumer, or a Connect sink connector's
	// group).
	SinkWriterConsumerGroup = "consumer_group"
	// SinkWriterKafkaEngine is a ClickHouse Kafka engine table that reads the
	// topic and fills the sink table through a materialized view.
	SinkWriterKafkaEngine = "kafka_engine"
)

// SinkWriter is what fills one sink table.
type SinkWriter struct {
	// Kind is SinkWriterConsumerGroup or SinkWriterKafkaEngine.
	Kind string `yaml:"kind"`
	// Name is the consumer group or the ClickHouse Kafka engine table
	// ("db.table").
	Name string `yaml:"name"`
}

// Sink is one sink table of a cdc pipeline and the writer that fills it.
// Warehouse is the sink's warehouse kind ("postgres", "clickhouse", ...);
// empty means the pipeline's warehouse kind. Load trims the values, lower-cases
// Warehouse and Kind, and defaults Warehouse to "clickhouse" for a
// kafka_engine writer.
type Sink struct {
	Table     string     `yaml:"table"`
	Warehouse string     `yaml:"warehouse"`
	Writer    SinkWriter `yaml:"writer"`
}

// ConsumerGroupWriter returns the consumer group that writes the sink, when the
// declared writer kind is SinkWriterConsumerGroup.
func (s Sink) ConsumerGroupWriter() (string, bool) {
	if s.Writer.Kind == SinkWriterConsumerGroup && s.Writer.Name != "" {
		return s.Writer.Name, true
	}
	return "", false
}

// KafkaEngineWriter returns the ClickHouse Kafka engine table that fills the
// sink, when the declared writer kind is SinkWriterKafkaEngine.
func (s Sink) KafkaEngineWriter() (string, bool) {
	if s.Writer.Kind == SinkWriterKafkaEngine && s.Writer.Name != "" {
		return s.Writer.Name, true
	}
	return "", false
}

// SinkTableNames returns the cdc pipeline's sink tables, in declaration order
// and without duplicates: the sinks: block when present, else sink_tables:.
// A pipeline built in code may set both; the union keeps a table once.
func (p Pipeline) SinkTableNames() []string {
	out := make([]string, 0, len(p.SinkTables)+len(p.Sinks))
	out = appendUnique(out, p.SinkTables...)
	for _, s := range p.Sinks {
		out = appendUnique(out, s.Table)
	}
	return out
}

// ConsumerGroupNames returns the pipeline's consumer groups, in declaration
// order and without duplicates: consumer_groups: plus the consumer_group
// writers declared in sinks:. Tools scope, the watcher and the topology all
// read the groups through it, so a sink writer is not invisible to them.
func (p Pipeline) ConsumerGroupNames() []string {
	out := make([]string, 0, len(p.ConsumerGroups)+len(p.Sinks))
	out = appendUnique(out, p.ConsumerGroups...)
	for _, s := range p.Sinks {
		if name, ok := s.ConsumerGroupWriter(); ok {
			out = appendUnique(out, name)
		}
	}
	return out
}

// SinkForTable returns the sinks: entry that declares table, if any.
func (p Pipeline) SinkForTable(table string) (Sink, bool) {
	for _, s := range p.Sinks {
		if s.Table == table {
			return s, true
		}
	}
	return Sink{}, false
}

// UsesSinkBlocks reports whether the pipeline declares its sinks with the
// per-sink sinks: form rather than the legacy consumer_groups + sink_tables
// lists.
func (p Pipeline) UsesSinkBlocks() bool { return len(p.Sinks) > 0 }

// validateSinks checks the sinks: block, normalizes it in place and merges its
// tables into SinkTables and its consumer-group writers into ConsumerGroups, so
// every reader of the legacy lists (tool scope, watcher, alert intake) sees the
// same objects. It refuses the block together with sink_tables: because two
// declarations of the same sinks could disagree; extra consumer_groups: are
// allowed (they read the topic but write no sink).
func (p *Pipeline) validateSinks() error {
	if len(p.Sinks) == 0 {
		return nil
	}
	if len(p.SinkTables) > 0 {
		return fmt.Errorf("sinks and sink_tables are both set: declare each sink table once, in sinks")
	}
	seen := make(map[string]bool, len(p.Sinks))
	for i := range p.Sinks {
		s := &p.Sinks[i]
		s.Table = strings.TrimSpace(s.Table)
		s.Warehouse = strings.ToLower(strings.TrimSpace(s.Warehouse))
		s.Writer.Kind = strings.ToLower(strings.TrimSpace(s.Writer.Kind))
		s.Writer.Name = strings.TrimSpace(s.Writer.Name)
		switch {
		case s.Table == "":
			return fmt.Errorf("sinks[%d]: table is required", i)
		case seen[s.Table]:
			return fmt.Errorf("sinks lists table %q twice", s.Table)
		}
		seen[s.Table] = true
		switch s.Writer.Kind {
		case SinkWriterConsumerGroup:
		case SinkWriterKafkaEngine:
			// The ClickHouse Kafka engine is a ClickHouse object, so its sink
			// is a ClickHouse table. A different warehouse there is a typo the
			// map would otherwise turn into a plausible-looking wrong chain.
			if s.Warehouse == "" {
				s.Warehouse = "clickhouse"
			} else if s.Warehouse != "clickhouse" {
				return fmt.Errorf("sinks[%d] (%s): writer kind %s needs warehouse clickhouse, got %q",
					i, s.Table, SinkWriterKafkaEngine, s.Warehouse)
			}
		default:
			return fmt.Errorf("sinks[%d] (%s): writer.kind %q: want %s or %s",
				i, s.Table, s.Writer.Kind, SinkWriterConsumerGroup, SinkWriterKafkaEngine)
		}
		if s.Writer.Name == "" {
			return fmt.Errorf("sinks[%d] (%s): writer.name is required", i, s.Table)
		}
	}
	tables := make([]string, 0, len(p.Sinks))
	for _, s := range p.Sinks {
		tables = append(tables, s.Table)
	}
	p.SinkTables = tables
	p.ConsumerGroups = p.ConsumerGroupNames()
	return nil
}

// appendUnique appends each name not already in out, preserving order.
func appendUnique(out []string, names ...string) []string {
	for _, n := range names {
		found := false
		for _, have := range out {
			if have == n {
				found = true
				break
			}
		}
		if !found {
			out = append(out, n)
		}
	}
	return out
}
