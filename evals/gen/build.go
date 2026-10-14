//go:build evalgen

package main

import (
	"fmt"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/postgres"
)

// Helpers that build world state the way the real systems report it.

const gib = int64(1) << 30
const mib = int64(1) << 20

// debezium is a Debezium Postgres source connector config with one table.
func debezium(host, db, table, prefix, slot string) map[string]string {
	return map[string]string{
		"connector.class":       "io.debezium.connector.postgresql.PostgresConnector",
		"database.hostname":     host,
		"database.port":         "5432",
		"database.dbname":       db,
		"database.user":         "debezium",
		"database.password":     "redacted-by-the-tool",
		"plugin.name":           "pgoutput",
		"publication.name":      "dbz_publication",
		"slot.name":             slot,
		"table.include.list":    table,
		"topic.prefix":          prefix,
		"key.converter":         "io.confluent.connect.avro.AvroConverter",
		"value.converter":       "io.confluent.connect.avro.AvroConverter",
		"decimal.handling.mode": "precise",
	}
}

// partitions builds a topic's offsets. Each range is {start, end}; every
// partition's newest record is newest ago.
func partitions(c clock, newest time.Duration, ranges ...[2]int64) []clients.PartitionOffsets {
	out := make([]clients.PartitionOffsets, len(ranges))
	for i, r := range ranges {
		out[i] = clients.PartitionOffsets{Partition: int32(i), Start: r[0], End: r[1], LastTimestamp: c.ago(newest)}
	}
	return out
}

// group builds a consumer group's lag. Each partition is {committed, end} of
// topic, numbered from 0; total lag is their sum.
func group(name, state string, members int, topic string, parts ...[2]int64) clients.GroupLag {
	g := clients.GroupLag{Group: name, State: state, Members: members}
	for i, p := range parts {
		lag := p[1] - p[0]
		g.Partitions = append(g.Partitions, clients.PartitionLag{Topic: topic, Partition: int32(i), Committed: p[0], End: p[1], Lag: lag})
		g.TotalLag += lag
	}
	return g
}

// addGroupTopic appends another topic's partitions to a group.
func addGroupTopic(g clients.GroupLag, topic string, parts ...[2]int64) clients.GroupLag {
	for i, p := range parts {
		lag := p[1] - p[0]
		g.Partitions = append(g.Partitions, clients.PartitionLag{Topic: topic, Partition: int32(i), Committed: p[0], End: p[1], Lag: lag})
		g.TotalLag += lag
	}
	return g
}

func topicConfig(topic string, partitions int, retention string) kafka.TopicConfig {
	return kafka.TopicConfig{
		Topic: topic, Partitions: partitions, ReplicationFactor: 3,
		RetentionMS: retention, RetentionBytes: "-1", CleanupPolicy: "delete", MinInsyncReplicas: "2",
	}
}

// currentLSN is the source's WAL insert position in every generated world.
// A slot's restart_lsn is retained bytes behind it, and its confirmed flush
// a little after that, so the LSNs move forward as retained WAL falls.
const currentLSN = int64(0x3B4C1D2E80)

func slot(name string, active bool, retained int64, walStatus string) clients.ReplicationSlot {
	restart := currentLSN - retained
	return clients.ReplicationSlot{
		Name: name, Plugin: "pgoutput", Active: active,
		RestartLSN: lsn(restart), ConfirmedFlushLSN: lsn(restart + 0x1F8),
		RetainedWALBytes: retained, WALStatus: walStatus,
	}
}

// lsn renders a WAL position the way Postgres prints pg_lsn.
func lsn(pos int64) string {
	return fmt.Sprintf("%X/%X", pos>>32, pos&0xFFFFFFFF)
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// postgresPub is one table's publication membership and the connector role's
// SELECT right on it.
type postgresPub struct {
	schema, table   string
	inPub, readable bool
}

type pubs []postgresPub

func (l pubs) tables() []postgres.PublicationTable {
	out := make([]postgres.PublicationTable, len(l))
	for i, p := range l {
		out[i] = postgres.PublicationTable{Schema: p.schema, Table: p.table, InPublication: p.inPub, Selectable: p.readable}
	}
	return out
}

// insertBeforeWildcards puts extra calls before the first wildcard call, so
// exact recordings come before the fallbacks in the file.
func insertBeforeWildcards(calls []call, extra ...call) []call {
	i := 0
	for i < len(calls) && !calls[i].wildcard {
		i++
	}
	out := append([]call{}, calls[:i]...)
	out = append(out, extra...)
	return append(out, calls[i:]...)
}

// ---- Avro schemas as Debezium registers them ----

func avroField(name, typ string) string { return fmt.Sprintf(`{"name":%q,"type":%s}`, name, typ) }

func optField(name, typ string) string {
	return fmt.Sprintf(`{"name":%q,"type":["null",%s],"default":null}`, name, typ)
}

const (
	avroLong   = `"long"`
	avroInt    = `"int"`
	avroString = `"string"`
	avroZoned  = `{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTimestamp"}`
)

func avroDecimal(precision, scale int) string {
	return fmt.Sprintf(`{"type":"bytes","scale":%d,"precision":%d,"connect.version":1,"connect.parameters":{"scale":"%d","connect.decimal.precision":"%d"},"connect.name":"org.apache.kafka.connect.data.Decimal","logicalType":"decimal"}`, scale, precision, scale, precision)
}

// envelope is a Debezium value schema: before/after of record Value with the
// given fields, plus source, op and ts_ms.
func envelope(topic string, fields ...string) string {
	return `{"type":"record","name":"Envelope","namespace":"` + topic + `","fields":[` +
		`{"name":"before","type":["null",{"type":"record","name":"Value","fields":[` + strings.Join(fields, ",") + `],"connect.name":"` + topic + `.Value"}],"default":null},` +
		`{"name":"after","type":["null","Value"],"default":null},` +
		`{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.postgresql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"db","type":"string"},{"name":"table","type":"string"},{"name":"lsn","type":["null","long"],"default":null}],"connect.name":"io.debezium.connector.postgresql.Source"}},` +
		`{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null}],"connect.name":"` + topic + `.Envelope"}`
}

func keySchema(topic string, fields ...string) string {
	return `{"type":"record","name":"Key","namespace":"` + topic + `","fields":[` + strings.Join(fields, ",") + `],"connect.name":"` + topic + `.Key"}`
}
