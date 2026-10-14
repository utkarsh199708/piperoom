// Package clients defines thin clients for the systems Piperoom inspects.
// Implementations live in sub-packages (connect, schemareg, kafka, postgres,
// warehouse). Tools and actions depend on these interfaces, so they can be
// tested with fakes.
//
// Reader / Writer split: every system has a *Reader interface with only the
// methods that read. A read-only tool (tools.KindRead), the topology builder,
// the watcher and discovery take Readers only, so the compiler keeps writes
// out of them. A type assertion from a Reader to a Writer or a *Client in such
// a package defeats this and fails review. Writers exist only for Kafka
// Connect and Schema Registry, and only package actions (and wiring, which
// builds them) may hold one. The full *Client
// interfaces are Reader + Writer, so one concrete client implements both.
// KafkaAdmin, SourceDB and Warehouse have no write methods: their Reader is
// the same method set under the name tools should use.
//
// CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
package clients

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned (wrapped) when the named object does not exist.
var ErrNotFound = errors.New("not found")

// ---- Kafka Connect (REST) ----

// ConnectReader is the read half of the Kafka Connect client.
type ConnectReader interface {
	ListConnectors(ctx context.Context) ([]string, error)
	Status(ctx context.Context, name string) (ConnectorStatus, error)
	Config(ctx context.Context, name string) (map[string]string, error)
}

// ConnectWriter is the write half: used only by actions after approval.
// (Resume and UpdateConfig are still optional adapter interfaces in package
// actions until a contract change request moves them here.)
type ConnectWriter interface {
	RestartTask(ctx context.Context, name string, task int) error
	RestartConnector(ctx context.Context, name string) error
}

// ConnectClient is the whole Kafka Connect client.
type ConnectClient interface {
	ConnectReader
	ConnectWriter
}

type ConnectorStatus struct {
	Name     string       `json:"name"`
	Type     string       `json:"type"`  // "source" | "sink"
	State    string       `json:"state"` // RUNNING | PAUSED | FAILED | UNASSIGNED | RESTARTING | STOPPED
	WorkerID string       `json:"worker_id"`
	Tasks    []TaskStatus `json:"tasks"`
}

type TaskStatus struct {
	ID       int    `json:"id"`
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
	Trace    string `json:"trace,omitempty"` // stack trace when FAILED
}

// ---- Schema Registry (Confluent REST API) ----

// SchemaRegistryReader is the read half of the Schema Registry client.
type SchemaRegistryReader interface {
	Subjects(ctx context.Context) ([]string, error)
	Versions(ctx context.Context, subject string) ([]int, error)
	// Schema returns one version; version -1 means latest.
	Schema(ctx context.Context, subject string, version int) (SchemaVersion, error)
	// Compatibility returns the subject level, falling back to the global level.
	Compatibility(ctx context.Context, subject string) (level string, subjectLevel bool, err error)
}

// SchemaRegistryWriter is the write half: used only by actions after approval.
type SchemaRegistryWriter interface {
	SetCompatibility(ctx context.Context, subject, level string) error
	Register(ctx context.Context, subject, schema string) (id int, err error)
}

// SchemaRegistryClient is the whole Schema Registry client.
type SchemaRegistryClient interface {
	SchemaRegistryReader
	SchemaRegistryWriter
}

type SchemaVersion struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	ID      int    `json:"id"`
	Schema  string `json:"schema"`
}

// ---- Kafka (admin + small reads) ----

// KafkaReader is the Kafka client as read-only tools take it. KafkaAdmin has
// no write method, so the two are the same method set.
type KafkaReader interface {
	Topics(ctx context.Context) ([]string, error)
	TopicOffsets(ctx context.Context, topic string) ([]PartitionOffsets, error)
	ConsumerGroups(ctx context.Context) ([]string, error)
	GroupLag(ctx context.Context, group string) (GroupLag, error)
	// FindByKey scans the topic from the first offset at or after since and
	// returns records whose key matches (string compare of the decoded key).
	// It stops at maxScan records per partition.
	FindByKey(ctx context.Context, topic, key string, since time.Time, maxScan int) ([]RecordRef, error)
}

// KafkaAdmin is the whole Kafka client (admin + small reads). It adds nothing
// to KafkaReader; a write method added here later must not go into KafkaReader.
type KafkaAdmin interface {
	KafkaReader
}

type PartitionOffsets struct {
	Partition     int32     `json:"partition"`
	Start         int64     `json:"start"`
	End           int64     `json:"end"`            // next offset to be written
	LastTimestamp time.Time `json:"last_timestamp"` // timestamp of record End-1; zero if empty
}

type GroupLag struct {
	Group      string         `json:"group"`
	State      string         `json:"state"` // Stable | Empty | Dead | PreparingRebalance | ...
	Members    int            `json:"members"`
	TotalLag   int64          `json:"total_lag"`
	Partitions []PartitionLag `json:"partitions"`
}

type PartitionLag struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Committed int64  `json:"committed"` // -1 if nothing committed
	End       int64  `json:"end"`
	Lag       int64  `json:"lag"`
}

type RecordRef struct {
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
	Key       string    `json:"key"`
}

// ---- Kafka: dead-letter topics (optional on a KafkaAdmin) ----

// DLQRecord is one record of a dead-letter topic with its key and value
// removed on purpose: where it is, when it was written, and the Kafka Connect
// error headers it carries. It is the only shape in which a DLQ record may
// leave the client (docs/pilot/data-handling.md, dlq_peek row).
type DLQRecord struct {
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
	// Headers keeps only headers whose key starts with "__connect.errors."
	// (Connect's DLQ context headers: topic, partition, offset, connector.name,
	// task.id, stage, class.name, exception.class.name, exception.message,
	// exception.stacktrace). Values are text with control characters removed
	// and at most 300 runes; exception.stacktrace is its first line only;
	// exception.message and that first line are redacted (dlq.RedactMessage:
	// "[Source:", "Detail:" and "Where:" tails, braces, brackets and quoted
	// strings removed) because converters and drivers quote record bytes in
	// them. Empty when the sink does not set
	// errors.deadletterqueue.context.headers.enable=true. Every other header
	// is dropped: an application header is never returned.
	Headers map[string]string `json:"headers,omitempty"`
}

// DLQReader is optional on a KafkaAdmin. The watcher's dlq_growth signal and
// the dlq_peek tool ask for it with a type assertion at use time; a
// KafkaAdmin without it leaves the signal "unknown" (one log line) and makes
// the tool return a clear error, so fakes and replay clients need not
// implement it.
type DLQReader interface {
	// DLQOffsets returns the start and end offset of every partition of each
	// topic, in two requests for all topics (ListStartOffsets, ListEndOffsets).
	// LastTimestamp is always zero: it never fetches a record (unlike
	// TopicOffsets). A topic that does not exist is absent from the result
	// and is NOT an error. No topics returns nil, nil without any request:
	// kadm lists the whole cluster for an empty topic list, and that must
	// never happen on a tick.
	DLQOffsets(ctx context.Context, topics ...string) (map[string][]PartitionOffsets, error)
	// PeekDLQ returns the newest records of topic, newest first, at most limit
	// in TOTAL across partitions (partitions are read newest end offset first
	// and the read stops at limit or at 8 MiB of record bytes), as DLQRecord:
	// never a value, never a key. limit <= 0 means 5; it is capped at 20.
	PeekDLQ(ctx context.Context, topic string, limit int) ([]DLQRecord, error)
}

// ---- Source Postgres (the CDC source) ----

// SourceReader is the source Postgres client as read-only tools take it.
type SourceReader interface {
	ReplicationSlots(ctx context.Context) ([]ReplicationSlot, error)
	// RecentDDL reads the DDL audit table written by an event trigger
	// (see dev/postgres-init). Newest first.
	RecentDDL(ctx context.Context, since time.Time, limit int) ([]DDLChange, error)
	// LongTransactions lists open transactions older than olderThan. Read-only
	// ones are included (they block DDL and VACUUM); Transaction.HasXID marks
	// writers, the only ones that hold the WAL horizon for a slot.
	LongTransactions(ctx context.Context, olderThan time.Duration) ([]Transaction, error)
	// Row reads one row by primary key. table is "schema.table".
	Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error)
}

// SourceDB is the whole source Postgres client. It adds nothing to
// SourceReader (the source role is read-only, docs/pilot/permissions.md).
type SourceDB interface {
	SourceReader
}

type ReplicationSlot struct {
	Name              string `json:"name"`
	Plugin            string `json:"plugin"`
	Active            bool   `json:"active"`
	RestartLSN        string `json:"restart_lsn"`
	ConfirmedFlushLSN string `json:"confirmed_flush_lsn"`
	RetainedWALBytes  int64  `json:"retained_wal_bytes"` // pg_current_wal_lsn() - restart_lsn
	WALStatus         string `json:"wal_status"`         // reserved | extended | unreserved | lost
	// SafeWALBytes is pg_replication_slots.safe_wal_size (PG13+): bytes of WAL
	// that can still be written before the slot becomes "unreserved". nil when
	// Postgres reports NULL (max_slot_wal_keep_size = -1, or the slot is lost).
	// It can be NEGATIVE once the slot is "unreserved" but not yet "lost": the
	// limit has been passed. Do not divide by it as if it were headroom.
	SafeWALBytes *int64 `json:"safe_wal_bytes,omitempty"`
}

type DDLChange struct {
	At             time.Time `json:"at"`
	CommandTag     string    `json:"command_tag"` // e.g. "ALTER TABLE"
	ObjectIdentity string    `json:"object_identity"`
	Query          string    `json:"query"`
	Username       string    `json:"username"`
}

// Transaction is one open transaction. It never carries query text: pg_stat_activity
// queries often hold literal row values, and the sessions belong to any pipeline.
type Transaction struct {
	PID             int           `json:"pid"`
	Username        string        `json:"username"`
	ApplicationName string        `json:"application_name"`
	State           string        `json:"state"`
	Age             time.Duration `json:"age"`
	// WaitEvent is "<wait_event_type>:<wait_event>", empty when not waiting.
	WaitEvent string `json:"wait_event"`
	// Query is at most the statement verb (the first keyword, upper case, e.g.
	// "UPDATE"), never the statement text.
	Query string `json:"query"`
	// HasXID is true when the backend holds a transaction id (backend_xid), so
	// it has written and holds the WAL horizon. False for read-only transactions.
	HasXID bool `json:"has_xid"`
}

// ---- Warehouse (Snowflake in production; Postgres in dev) ----

// WarehouseReader is the warehouse client as read-only tools take it.
type WarehouseReader interface {
	// Freshness returns max(tsColumn) of table. table is "schema.table".
	Freshness(ctx context.Context, table, tsColumn string) (time.Time, error)
	Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error)
}

// Warehouse is the whole warehouse client. It adds nothing to WarehouseReader.
type Warehouse interface {
	WarehouseReader
}
