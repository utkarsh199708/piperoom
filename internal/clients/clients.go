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
	"encoding/json"
	"errors"
	"time"
)

// ErrNotFound is returned (wrapped) when the named object does not exist.
var ErrNotFound = errors.New("not found")

// ErrNotSupported is returned by an optional read a client cannot do. (F12)
var ErrNotSupported = errors.New("clients: not supported by this client")

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
	// Trace is the connector-level stack trace Connect reports when the
	// connector itself fails (e.g. at startup, with no tasks); empty
	// otherwise. Untrusted text: condense and sanitize it before it reaches a
	// summary or tool Data. (#399)
	Trace string `json:"trace,omitempty"`
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
	// It stops at maxScan records per partition (<= 0 means the client
	// default). A deadline that cuts the scan before any match is an error,
	// never a false "not found"; a cut after a match returns the matches so
	// far with a nil error, so the result cannot tell "no newer match" from
	// "not scanned": a caller that must know uses KeyScanReader.
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
	LastTimestamp time.Time `json:"last_timestamp"` // timestamp at last stable offset-1 (End-1 unless a transaction is open; a marker counts); zero if none
}

type GroupLag struct {
	Group      string         `json:"group"`
	State      string         `json:"state"` // Stable | Empty | Dead | PreparingRebalance | ...
	Members    int            `json:"members"`
	TotalLag   int64          `json:"total_lag"`
	Partitions []PartitionLag `json:"partitions"`
	// Unknown lists the partitions whose committed offset or end offset could
	// not be read (e.g. a broker down); they are not in Partitions and not in
	// TotalLag, so a non-empty Unknown makes TotalLag a lower bound, never
	// "no lag". In an Unknown entry Lag is always -1, and End is -1 when the
	// end offset could not be read; Committed is the committed offset when it
	// was read, else -1. (#306)
	Unknown []PartitionLag `json:"unknown,omitempty"`
}

type PartitionLag struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Committed int64  `json:"committed"` // -1 if nothing committed
	End       int64  `json:"end"`
	Lag       int64  `json:"lag"`
}

// KeyScan is a key scan's result: the matching records and the partitions
// whose window the scan did not finish (maxScan, or the scan deadline after a
// match), where newer matches may exist. Refs are per partition in offset
// order (a partition's newest match is its last), partitions ascending, so
// the last Ref is not the newest match across partitions; Truncated is
// ascending, without duplicates. (#306)
type KeyScan struct {
	Refs      []RecordRef `json:"records"`
	Truncated []int32     `json:"truncated_partitions,omitempty"`
}

// KeyScanReader is FindByKey that also reports truncated partitions, so a
// caller never reads a cut scan as "the record is not there" (optional on a
// KafkaReader, asserted at use time). Its Refs are what FindByKey returns for
// the same arguments; a deadline that cuts the scan before any match is still
// an error. Without it (fakes, eval replay), a caller treats FindByKey's result
// as possibly truncated: it never states "not there", "newest match" or
// "consumed" as fact. (#306)
type KeyScanReader interface {
	FindByKeyScan(ctx context.Context, topic, key string, since time.Time, maxScan int) (KeyScan, error)
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

// ---- Kafka: cluster and partition reads (optional on a KafkaAdmin, F12) ----
//
// Asserted at use time like DLQReader. These interfaces are not embedded in
// KafkaAdmin, so fakes and the eval replay need not implement them.

// BrokerHealth is one broker from a metadata response plus its log dirs.
type BrokerHealth struct {
	NodeID       int32  `json:"node_id"`
	Host         string `json:"host"`
	Port         int32  `json:"port"`
	IsController bool   `json:"is_controller"`
	// LogDirTotalBytes/UsableBytes sum the broker's log directories; -1 when
	// the broker does not report them (Kafka < 3.3 or remote storage).
	LogDirTotalBytes  int64  `json:"log_dir_total_bytes"`
	LogDirUsableBytes int64  `json:"log_dir_usable_bytes"`
	LogDirErr         string `json:"log_dir_err,omitempty"`
}

// PartitionHealth is one partition of a pipeline topic from metadata.
type PartitionHealth struct {
	Topic           string  `json:"topic"`
	Partition       int32   `json:"partition"`
	Leader          int32   `json:"leader"` // -1 = no leader
	Replicas        []int32 `json:"replicas"`
	ISR             []int32 `json:"isr"`
	OfflineReplicas []int32 `json:"offline_replicas"`
	MinISR          int     `json:"min_isr"` // from min.insync.replicas; 0 = unknown
	Reassigning     bool    `json:"reassigning"`
}

// ClusterHealth is what ClusterReader.ClusterHealth returns for a set of topics.
type ClusterHealth struct {
	ClusterID  string            `json:"cluster_id"`
	Controller int32             `json:"controller"`
	Brokers    []BrokerHealth    `json:"brokers"`
	Partitions []PartitionHealth `json:"partitions"`
}

// ClusterReader reads broker and partition health for the given topics only
// (never the whole cluster's topics). Needs Describe on Cluster and the
// topics; log dirs need DescribeConfigs on Cluster and are best-effort.
type ClusterReader interface {
	ClusterHealth(ctx context.Context, topics ...string) (ClusterHealth, error)
}

// PartitionRate is records written to one partition in a window.
type PartitionRate struct {
	Topic         string `json:"topic"`
	Partition     int32  `json:"partition"`
	End           int64  `json:"end"`
	OffsetAtStart int64  `json:"offset_at_start"` // first offset with timestamp >= window start; -1 = none
	Records       int64  `json:"records"`         // End - OffsetAtStart, or 0
}

// RateReader reads per-partition write counts over a window from offsets
// alone (ListOffsetsAfterMilli + ListEndOffsets); it fetches no record.
type RateReader interface {
	PartitionRates(ctx context.Context, since time.Time, topics ...string) ([]PartitionRate, error)
}

// ACLEntry is one described ACL.
type ACLEntry struct {
	Principal    string `json:"principal"`
	Host         string `json:"host"`
	ResourceType string `json:"resource_type"` // TOPIC | GROUP | CLUSTER
	Resource     string `json:"resource"`
	Pattern      string `json:"pattern"` // LITERAL | PREFIXED
	Operation    string `json:"operation"`
	Permission   string `json:"permission"` // ALLOW | DENY
}

// ACLReader lists ACLs that touch the given topics and groups (any
// principal, any host), plus cluster ACLs. Needs Describe on Cluster.
type ACLReader interface {
	ACLs(ctx context.Context, topics, groups []string) ([]ACLEntry, error)
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

// ---- Warehouse: load history and counts (optional, F12) ----
//
// Asserted at use time; not embedded in Warehouse or SourceDB.

// LoadError is one failed or partial load into a sink table.
type LoadError struct {
	Table      string    `json:"table"`
	At         time.Time `json:"at"`
	Status     string    `json:"status"` // LOADED | LOAD_FAILED | PARTIALLY_LOADED
	Rows       int64     `json:"rows"`
	Errors     int64     `json:"errors"`
	FirstError string    `json:"first_error"`    // redacted, ≤ 300 runes; never a record value
	File       string    `json:"file,omitempty"` // file name only (no path, no URL)
}

// LoadErrorReader reads a sink table's load history. Snowflake implements it
// over INFORMATION_SCHEMA.COPY_HISTORY; the dev Postgres warehouse returns
// ErrNotSupported.
type LoadErrorReader interface {
	LoadErrors(ctx context.Context, table string, since time.Time, limit int) ([]LoadError, error)
}

// MinuteCount is rows per bucket for reconcile.
type MinuteCount struct {
	Bucket time.Time `json:"bucket"`
	Count  int64     `json:"count"`
}

// CountReader counts rows of table by tsColumn in buckets of width
// (optional on a SourceDB and a Warehouse). Counts only, never values.
type CountReader interface {
	CountByBucket(ctx context.Context, table, tsColumn string, since, until time.Time, width time.Duration) ([]MinuteCount, error)
}

// ---- Pipeline change log (Piperoom's own table, fed by the watcher, F8) ----
//
// Not a customer system: the room store implements both interfaces.

// Change kinds of PipelineChange.Kind.
const (
	ChangeDDL             = "ddl"              // a row of the source DDL audit table
	ChangeConnectorConfig = "connector_config" // connector config keys changed (values masked)
	ChangeConnectorState  = "connector_state"  // connector or task state transition
	ChangeSchemaVersion   = "schema_version"   // a subject's latest version went up (registered) or down (deleted)
)

// PipelineChange is one recorded change on a pipeline. Before and After hold
// only the changed keys (config), the two states (state), the version pair
// (schema) or nothing (DDL: the statement is in Summary, cut at 300 runes).
// A credential value is never stored: masked keys read "***". Actor is the
// DDL username or ""; Source is "watcher" or "ddl_audit".
type PipelineChange struct {
	ID          int64           `json:"id"`
	PipelineID  string          `json:"pipeline_id"`
	Kind        string          `json:"kind"`
	Object      string          `json:"object"` // "public.orders", "pg-orders-connector", "pg-orders-connector/0", "pg.public.orders-value"
	At          time.Time       `json:"at"`
	Actor       string          `json:"actor,omitempty"`
	Source      string          `json:"source"`
	Fingerprint string          `json:"fingerprint"` // dedup key: sha256 of kind|object|at|before|after
	Before      json.RawMessage `json:"before,omitempty"`
	After       json.RawMessage `json:"after,omitempty"`
	Summary     string          `json:"summary"` // one line, ≤ 300 runes, control characters removed
}

// ChangeReader lists changes of one pipeline, newest first, at most limit.
// kinds nil = every kind. Implemented by the room store (store.PG, memstore).
type ChangeReader interface {
	ListChanges(ctx context.Context, pipelineID string, since, until time.Time, kinds []string, limit int) ([]PipelineChange, error)
}

// ChangeWriter records a change. A row with the same (pipeline, kind, object,
// fingerprint) is not written again; inserted reports whether it was.
// It writes Piperoom's own table, never a customer system, so the Reader /
// Writer rule above does not apply: the watcher (F8c) may hold one.
type ChangeWriter interface {
	RecordChange(ctx context.Context, c PipelineChange) (inserted bool, err error)
}
