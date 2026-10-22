// Package clients defines thin clients for the systems Piperoom inspects.
// Implementations live in sub-packages (connect, schemareg, kafka, postgres,
// warehouse). Tools and actions depend on these interfaces, so they can be
// tested with fakes.
//
// Reader / Writer split: every system has a *Reader interface with only the
// methods that read. A read-only tool (tools.KindRead), the topology builder,
// the watcher and discovery take Readers only, so the compiler keeps writes
// out of them. A type assertion from a Reader to a Writer or a *Client in such
// a package defeats this and fails review. Writers exist for Kafka Connect,
// Schema Registry and, from the P wave, Kafka (KafkaWriter) and the source
// Postgres (SourceWriter); only package actions (and wiring, which builds
// them, and only when PIPEROOM_WRITE_MODE is on) may hold one. The full
// *Client interfaces are Reader + Writer, so one concrete client implements
// both. KafkaAdmin, SourceDB and Warehouse have no write methods: their
// Reader is the same method set under the name tools should use, and the P
// wave writers (ConnectFixWriter, KafkaWriter, SourceWriter) are separate
// interfaces, not embedded in the *Client ones, so fakes and the eval replay
// need not implement them.
//
// CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
package clients

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
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
// (Resume and the one-key UpdateConfig are still optional adapter interfaces
// in package actions; the P3 writes are ConnectFixWriter below.)
type ConnectWriter interface {
	RestartTask(ctx context.Context, name string, task int) error
	RestartConnector(ctx context.Context, name string) error
}

// ConnectFixWriter is the P3 part of the Kafka Connect write half (P0 item 7):
// implemented by internal/clients/connect (P3a-1) and asserted by the actions
// at use time, so the existing ConnectClient fakes need not grow. Only
// package actions may hold one. A later contract change may fold it into
// ConnectWriter once every fake has the methods.
type ConnectFixWriter interface {
	// PauseConnector pauses the connector and its tasks
	// (PUT /connectors/{name}/pause); connect_resume_connector undoes it.
	PauseConnector(ctx context.Context, name string) error
	// UpdateConnectorConfigKeys changes every key of changes in one request
	// (PATCH /connectors/{name}/config) and leaves the other keys as they
	// are. The action has refused secret-looking keys and values before.
	UpdateConnectorConfigKeys(ctx context.Context, name string, changes map[string]string) error
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

// ---- Kafka: writes and whole records (actions only, P3) ----
//
// Not embedded in KafkaAdmin: a read-only tool, the topology builder, the
// watcher and discovery can never reach them through a KafkaReader, and fakes
// and the eval replay need not implement them. Every write method requires the
// room's pipeline scope in ctx (tools.WithPipeline) and refuses a target
// outside it before any I/O (tools.ErrOutOfScope); without a scope it refuses
// with tools.ErrNoScope unless the caller opted out with tools.WithUnscoped
// (tests, CLI). Read helpers on the same type are scoped by the action's
// CheckScope instead.

// ErrGroupActive is returned (wrapped) by ResetGroupOffsets when the group
// has active members: committed offsets of a live group are overwritten by
// its next commit, so a reset would silently not happen.
var ErrGroupActive = errors.New("clients: consumer group has active members")

// OffsetTarget is where ResetGroupOffsets moves a group. Exactly one of
// Earliest and Timestamp is set; the action refuses both, neither, and a
// Timestamp in the future.
type OffsetTarget struct {
	Earliest  bool      `json:"earliest,omitempty"`
	Timestamp time.Time `json:"timestamp,omitzero"` // first offset at or after this time
}

// RecordHeader is one Kafka record header.
type RecordHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// Record is one whole Kafka record: key, value and headers included. Unlike
// DLQRecord it carries record bytes, so only package actions may hold one, and
// never put one into a log line, an event, a summary or tool Data
// (docs/pilot/data-handling.md). Produce ignores Topic, Partition, Offset and
// Timestamp: the topic argument wins, the producer's partitioner places each
// record by key as the original producer did, and the record gets the produce
// time, so a replayed record is not dropped at once by time-based retention.
type Record struct {
	Topic     string         `json:"topic"`
	Partition int32          `json:"partition"`
	Offset    int64          `json:"offset"`
	Timestamp time.Time      `json:"timestamp"`
	Key       []byte         `json:"key"`
	Value     []byte         `json:"value"`
	Headers   []RecordHeader `json:"headers,omitempty"`
}

// KafkaWriter is implemented by the Kafka client (internal/clients/kafka,
// P3a-2); wiring hands it to the actions only when write mode is on.
type KafkaWriter interface {
	// ResetGroupOffsets moves every partition of the group's committed topics to
	// the target; the group must have no active members (ErrGroupActive).
	ResetGroupOffsets(ctx context.Context, group string, to OffsetTarget) (moved int, err error)
	// Produce writes records to topic in order and returns how many were acked.
	// It stops at the first record that is not acked.
	Produce(ctx context.Context, topic string, recs []Record) (acked int, err error)
}

// GroupResetPlanner is implemented by the Kafka write client; an action's
// Validate and Preview hold it (never the writer) to refuse an active group
// and to show per-partition moves before anything is committed. Read-only.
// Optional: asserted at use time, like RecordReader; a nil or absent planner
// makes the action refuse at Validate. The Kafka client satisfies it from
// P3a-3, when its own OffsetMove becomes an alias of the type below; until
// then the assertion is false and the action refuses.
type GroupResetPlanner interface {
	CheckGroupInactive(ctx context.Context, group string) error
	PlanGroupReset(ctx context.Context, group string, to OffsetTarget) ([]OffsetMove, error)
}

// OffsetMove is one partition's planned move for a group reset.
type OffsetMove struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Committed int64  `json:"committed"` // the group's committed offset before the reset; -1 when nothing was committed
	Target    int64  `json:"target"`    // the offset the reset commits
	End       int64  `json:"end"`       // the high watermark at planning time; End-Target is the lag the reset leaves
}

// SnapshotSignaler sends one Debezium incremental-snapshot signal to the
// connector's Kafka signal topic (signal.kafka.topic). It builds the record
// itself, so it can only ever request an execute-snapshot of tables: key = the
// connector's topic.prefix, value {"type":"execute-snapshot","data":
// {"data-collections":[..],"type":"INCREMENTAL"}}, partition 0 of a
// single-partition topic. It is a write method: it requires the room's
// pipeline scope in ctx (connector in connectors, every table in
// source_tables, key equal to topic_prefix) and refuses before any I/O
// otherwise, as KafkaWriter does. Implemented by the Kafka write client
// (internal/clients/kafka, #620); optional, asserted by connect_snapshot_table,
// so fakes and the eval replay need not implement it. KafkaWriter.Produce is not
// used for signals: its scope allows CDC topics only. The Kafka client's own
// type becomes an alias of SnapshotSignal (A2 follow-up), as with OffsetMove.
type SnapshotSignaler interface {
	SendSnapshotSignal(ctx context.Context, signalTopic, connector, logicalName string, tables []string) (SnapshotSignal, error)
}

// SnapshotSignal is the record SendSnapshotSignal produced. ID is the signal id
// Debezium logs for it (the record offset).
type SnapshotSignal struct {
	ID        string `json:"id"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

// ---- X2: targeted re-snapshot over a Kafka signal topic (recovery) ----

// SnapshotConditionSignaler is SnapshotSignaler with a row condition: the same
// execute-snapshot record, plus Debezium's additional-conditions entry
// {"data-collection": t, "filter": additionalCondition} for every t in tables,
// so the connector re-emits only the rows that match (pg_resnapshot_rows,
// X2a, on a connector with signal.kafka.topic). additionalCondition follows
// the rule of SnapshotConditionWriter: built and checked by the action from
// typed params, never text from the model; the implementation runs
// ValidateSnapshotCondition before any I/O and refuses an empty condition
// (an unconditional snapshot is SendSnapshotSignal). Scope, key and partition
// rules are SnapshotSignaler's. Optional and separate from SnapshotSignaler,
// so the Kafka client, fakes and the eval replay compile without it; an
// action asserts it at use time and refuses before any I/O when it is
// absent. (X2)
type SnapshotConditionSignaler interface {
	SendSnapshotSignalWhere(ctx context.Context, signalTopic, connector, logicalName string, tables []string, additionalCondition string) (SnapshotSignal, error)
}

// RecordReader reads whole records for dlq_replay (P3a): at most limit
// records of one partition of topic from offset from, in offset order (fewer
// when the partition ends first or 32 MiB were read). It changes nothing, so
// an action's Preview may call it, but it returns keys and values: only
// package actions may assert it on a Kafka client, never a read-only tool,
// the topology builder or the watcher (that is what DLQReader's key-free
// DLQRecord is for).
// Optional, asserted at use time; implemented with KafkaWriter (P3a-2).
type RecordReader interface {
	ReadRecords(ctx context.Context, topic string, partition int32, from int64, limit int) ([]Record, error)
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

// ErrSlotActive is returned (wrapped) by DropReplicationSlot when the slot
// is active: dropping the slot of a running connector loses its position.
var ErrSlotActive = errors.New("clients: replication slot is active")

// ErrAlreadyPublished is returned (wrapped) by SourceWriter.AddPublicationTables
// when a table is already in the publication or the publication is FOR ALL
// TABLES; nothing is changed. The postgres client returns it from P3b-2;
// until then errors.Is against this sentinel is false for the writer's
// errors, so no action or fake branches on it before that lands.
var ErrAlreadyPublished = errors.New("clients: table is already in the publication")

// SourceWriter is implemented by the source Postgres client when write mode is
// on (P3b): a separate pool on SOURCE_DB_WRITE_URL with its own role, never
// the read-only SourceDB pool. No SOURCE_DB_WRITE_URL = no SourceWriter. Not
// embedded in SourceDB. Identifiers are checked against
// ^[a-zA-Z_][a-zA-Z0-9_]*$ and quoted with pgx.Identifier.Sanitize; no
// statement is built from untrusted text. Every write method requires the
// room's pipeline scope in ctx (tools.WithPipeline) and refuses a target
// outside it before any I/O (tools.ErrOutOfScope); without a scope it refuses
// with tools.ErrNoScope unless the caller opted out with tools.WithUnscoped
// (tests, CLI). Read helpers on the same type are scoped by the action's
// CheckScope instead.
type SourceWriter interface {
	// AddPublicationTables runs ALTER PUBLICATION … ADD TABLE for each of
	// tables ("schema.table").
	AddPublicationTables(ctx context.Context, publication string, tables []string) error
	// DropReplicationSlot drops an inactive slot (ErrSlotActive otherwise).
	DropReplicationSlot(ctx context.Context, slot string) error
	// InsertSnapshotSignal inserts one Debezium execute-snapshot row (id, type,
	// data) asking for an incremental snapshot of tables into signalTable (the
	// connector's signal.data.collection, "schema.table") and returns the row
	// id. connectorID names the connector the signal is for.
	InsertSnapshotSignal(ctx context.Context, signalTable, connectorID string, tables []string) (signalID string, err error)
}

// ---- X2: targeted re-snapshot over a signal table (recovery) ----

// SnapshotConditionWriter is implemented by the source Postgres client next to
// SourceWriter, on the same write pool and under the same rules (write mode
// on, the room's pipeline scope in ctx, identifiers checked and quoted,
// tools.ErrOutOfScope / tools.ErrNoScope before any I/O). It is a separate
// interface so that SourceWriter, its fakes and the eval replay compile
// unchanged; the recovery action pg_resnapshot_rows (X2a) asserts it at use
// time and refuses before any I/O when the writer lacks it. (X2)
type SnapshotConditionWriter interface {
	// InsertSnapshotSignalWhere inserts one execute-snapshot row like
	// SourceWriter.InsertSnapshotSignal, whose data also carries
	// "additional-conditions": [{"data-collection": t, "filter":
	// additionalCondition}] for every t in tables, so Debezium re-reads only
	// the matching rows, and returns the row id.
	//
	// additionalCondition is a SQL boolean expression that Debezium appends
	// to its snapshot SELECT, so it is executable text on the source
	// database. It is validated by the action (allow-listed grammar), never
	// passed raw from the model: the action builds it from typed params (an
	// identifier checked against ^[a-zA-Z_][a-zA-Z0-9_]*$ and quoted, the
	// table's primary key or a timestamp column, a comparison operator from a
	// fixed set, values bound as quoted literals with ' doubled), and the
	// model only ever supplies those params. The writer checks it again with
	// ValidateSnapshotCondition and refuses before any I/O when it fails
	// (ErrBadSnapshotCondition). The condition and its values are never
	// logged; errors name the table only.
	InsertSnapshotSignalWhere(ctx context.Context, signalTable, connectorID string, tables []string, additionalCondition string) (signalID string, err error)
}

// MaxSnapshotConditionBytes caps a re-snapshot's additionalCondition. One
// key range or time bound is far shorter; the cap also keeps the signal row
// under the 2 KiB data column Debezium's signal table documents. (X2)
const MaxSnapshotConditionBytes = 1024

// ErrBadSnapshotCondition is returned (wrapped) by ValidateSnapshotCondition
// and by every SnapshotConditionWriter / SnapshotConditionSignaler for a
// condition that fails it. Nothing was written. (X2)
var ErrBadSnapshotCondition = errors.New("clients: refused snapshot condition")

// ValidateSnapshotCondition is the writers' second line of defence behind the
// action's grammar. It is lexical and stricter than any SQL dialect a source
// speaks: it tracks '…' literals and "…" identifiers (a doubled quote stays
// inside), and refuses an empty condition, one over
// MaxSnapshotConditionBytes, a control character (NUL, newline and tab
// included) or a backslash anywhere, an unterminated quote, and ; -- /* # $
// or ` outside a quote. So a value such as '1; drop' bound as a literal
// passes, while a second statement, a comment, a dollar-quoted or
// backslash-escaped string does not. It does no I/O and returns
// ErrBadSnapshotCondition wrapped; the message never echoes the condition. (X2)
func ValidateSnapshotCondition(cond string) error {
	if cond == "" {
		return fmt.Errorf("%w: empty", ErrBadSnapshotCondition)
	}
	if len(cond) > MaxSnapshotConditionBytes {
		return fmt.Errorf("%w: %d bytes, at most %d", ErrBadSnapshotCondition, len(cond), MaxSnapshotConditionBytes)
	}
	var quote byte // 0 outside a quote, else ' or "
	for i := 0; i < len(cond); i++ {
		c := cond[i]
		if c < 0x20 || c == 0x7f || c == '\\' {
			return fmt.Errorf("%w: control character or backslash at byte %d", ErrBadSnapshotCondition, i)
		}
		if quote != 0 {
			if c == quote {
				if i+1 < len(cond) && cond[i+1] == quote {
					i++ // a doubled quote is an escaped one, not the end
					continue
				}
				quote = 0
			}
			continue
		}
		next := byte(0)
		if i+1 < len(cond) {
			next = cond[i+1]
		}
		switch {
		case c == '\'' || c == '"':
			quote = c
		case c == ';', c == '#', c == '$', c == '`',
			c == '-' && next == '-', c == '/' && next == '*':
			return fmt.Errorf("%w: %q outside a quote at byte %d", ErrBadSnapshotCondition, c, i)
		}
	}
	if quote != 0 {
		return fmt.Errorf("%w: unterminated quote", ErrBadSnapshotCondition)
	}
	return nil
}

// SetupSourceWriter holds the writes only a setup makes on the source
// database (docs/design/setup-agent.md §4 item 4): built only with write
// mode on, on the write pool (SOURCE_DB_WRITE_URL for Postgres), never the
// read-only SourceDB pool; not embedded in SourceDB or SourceWriter, so no
// existing fake implements it. Postgres implements it in U1-9 (A3), MySQL in
// U2a (A8), the other sources in A10's clients; a source with no equivalent
// of a method returns ErrNotSupported from it. The fail-closed scope rule of
// SourceWriter applies to every method (the room's pipeline scope in ctx,
// tools.ErrOutOfScope / tools.ErrNoScope before any I/O), and identifiers
// follow the same ^[a-zA-Z_][a-zA-Z0-9_]*$ plus Sanitize rule. In a setup
// room the implementation is internal/setup/remoteclients (A9), one T2 or T3
// op to the runner per call. No method deletes data or reads a secret. (U)
type SetupSourceWriter interface {
	// CreatePublication creates publication name FOR TABLE tables
	// ("schema.table"); an existing publication of that name is an error,
	// never altered (AddPublicationTables does that).
	CreatePublication(ctx context.Context, name string, tables []string) error
	// CreateCanaryTable creates the ladder's canary table ("schema.table":
	// an id, a nonce column and a timestamp) with the replica identity the
	// connector needs; it exists afterwards whether or not it did before.
	CreateCanaryTable(ctx context.Context, table string) error
	// InsertCanary inserts one row with nonce (16 random bytes, hex, from
	// the ladder runner, never from the model) into the canary table, so the
	// ladder can find the change event by key.
	InsertCanary(ctx context.Context, table, nonce string) error
	// CreateReadRole creates role with the grants of docs/pilot/permissions.md
	// on tables (SELECT, and the replication-slot reads the watcher needs)
	// and a password the implementation generates and stores in the runner's
	// secret store; it returns only that store's name. The password is in no
	// return value, log, event or prompt; it reaches the server only through
	// the runner's Credentials frame after the person's click and terminal
	// confirmation (design §3.7). An existing role is an error, never
	// re-passworded.
	CreateReadRole(ctx context.Context, role string, tables []string) (secretName string, err error)
	// AlterSystem sets one server setting (ALTER SYSTEM SET name = value, or
	// the source's equivalent) and reports whether the change needs a restart
	// to take effect (pg_settings.pending_restart). name must be in the
	// setup allow-list of design §2.5 (wal_level, max_replication_slots,
	// max_wal_senders, max_slot_wal_keep_size, wal_sender_timeout, and the
	// source's equivalents); any other name is refused before any I/O. It is
	// a T3 op: the room service creates and executes it only inside a
	// maintenance window.
	AlterSystem(ctx context.Context, name, value string) (pendingRestart bool, err error)
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

// ---- X1: silent corruption, source vs sink reconcile (optional) ----
//
// Asserted at use time like CountReader (rec, ok := src.(clients.Reconciler)),
// never embedded in SourceDB or Warehouse, so no existing client or fake has
// to grow. The source Postgres and the dev Postgres warehouse implement it
// (X1a, A3); other warehouses return ErrNotSupported from every method until
// their owners add it. The drift watcher signals (count_drift, row_drift;
// events.DriftSignal) are its first caller; a recovery action's Verify (X2,
// actions.Verifier) is the second.

// Caps and deadline of every Reconciler implementation. Callers and tests use
// these constants instead of their own numbers.
const (
	// ReconcileMaxSample caps SampleKeys' n: a larger n is cut to it.
	ReconcileMaxSample = 200
	// ReconcileMaxKeys caps RowTimes' keys: more is refused, nothing is read.
	ReconcileMaxKeys = 1000
	// ReconcileTimeout is the per-call deadline an implementation sets when
	// ctx has none (or a later one); the source's statement_timeout still
	// applies on top.
	ReconcileTimeout = 10 * time.Second
)

// ReconcileSampleSize is the number of keys SampleKeys reads for a requested
// n: n cut to ReconcileMaxSample, and 0 when n <= 0 (the implementation then
// refuses the call with an error wrapping tools.ErrBadInput). Implementations
// call it so the cap is the same everywhere.
func ReconcileSampleSize(n int) int {
	if n <= 0 {
		return 0
	}
	return min(n, ReconcileMaxSample)
}

// Reconciler reads the row evidence a drift check needs from one side of a
// pipeline (source or sink): counts, primary-key values and timestamps only.
// It never returns any other column value, and no method writes.
//
// Rules for every method:
//   - table is "schema.table"; table, pkColumn, tsColumn and every entry of
//     tsColumns must match ^[a-zA-Z_][a-zA-Z0-9_]*$ per part and are quoted
//     with pgx.Identifier{...}.Sanitize() (or the warehouse's equivalent).
//     A bad identifier is an error wrapping tools.ErrBadInput, before any I/O.
//   - Scope fails closed like SourceWriter: the pipeline in ctx
//     (tools.WithPipeline; the watcher sets it per pipeline, the agent per
//     room) must list table in source_tables (source) or sink_tables (sink),
//     else an error wrapping tools.ErrOutOfScope (tools.ErrNoScope with no
//     pipeline), before any I/O.
//   - Each call has a deadline of ReconcileTimeout (10 s) at most; a call cut
//     by it returns an error wrapping context.DeadlineExceeded.
//   - Times are compared and returned in UTC; tsColumn values that are NULL
//     are never counted, sampled or returned.
type Reconciler interface {
	// CountBetween counts the rows of table whose tsColumn is in the
	// half-open window [from, to). to before from is tools.ErrBadInput.
	// It returns a count only, never a value. Deadline 10 s.
	CountBetween(ctx context.Context, table, tsColumn string, from, to time.Time) (int64, error)
	// SampleKeys returns the primary-key values (as text) of the newest
	// rows of table with tsColumn >= since, newest first: at most
	// ReconcileSampleSize(n) keys, so n > ReconcileMaxSample (200) is cut
	// to 200 and n <= 0 is tools.ErrBadInput. Keys only, never another
	// column's value; a caller keeps them in memory and never logs them or
	// puts them in an event. Deadline 10 s.
	SampleKeys(ctx context.Context, table, pkColumn, tsColumn string, since time.Time, n int) ([]string, error)
	// RowTimes reads, for each key of keys (primary-key values as text), the
	// timestamp columns tsColumns of that row: key -> column -> time. It
	// selects only pkColumn and tsColumns, never another column, so no
	// non-timestamp value can be returned. A key with no row is absent from
	// the result; a NULL column is absent from its row's map; no keys is an
	// empty map and no I/O. More than ReconcileMaxKeys (1000) keys or an
	// empty tsColumns is tools.ErrBadInput before any I/O; a named column
	// that is not a date/time type is tools.ErrBadInput too, and no value of
	// it is returned. Deadline 10 s.
	RowTimes(ctx context.Context, table, pkColumn string, keys []string, tsColumns []string) (map[string]map[string]time.Time, error)
}

// ---- Batch pipelines: orchestrator and transform (P4) ----
//
// Optional readers, like ClusterReader: wiring holds nil when the system is
// not configured (AIRFLOW_URL, DBT_ARTIFACTS_DIR / DBT_CLOUD_URL), and a tool
// with a nil reader answers ErrNotSupported. Read-only by construction: there
// is no batch writer in this wave. Every text field below that the system
// wrote (notes, messages, log text) is untrusted: a tool wraps, redacts and
// scans it like every other read before it reaches a summary or the model.

// DAGRun is one run of an orchestrator DAG (Airflow dagRun). Note is the
// run's free-text note, written by whoever triggered or annotated the run:
// untrusted text.
type DAGRun struct {
	ID          string    `json:"id"`             // the run id (Airflow dag_run_id)
	State       string    `json:"state"`          // as the orchestrator reports it: queued | running | success | failed
	StartedAt   time.Time `json:"started_at"`     // zero while queued
	EndedAt     time.Time `json:"ended_at"`       // zero while not finished
	Note        string    `json:"note,omitempty"` // untrusted text
	LogicalDate time.Time `json:"logical_date"`   // the data interval the run is for (dag_schedule_gap, late-batch, backfill-overlap)
	RunType     string    `json:"run_type"`       // scheduled | manual | backfill | dataset_triggered
}

// TaskInstance is one try of one task in a DAG run.
type TaskInstance struct {
	TaskID    string    `json:"task_id"`
	Try       int       `json:"try"`   // Airflow try_number as reported; 0 when the task never ran (e.g. upstream_failed)
	State     string    `json:"state"` // as reported: success | failed | up_for_retry | running | skipped | upstream_failed | …
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
	Operator  string    `json:"operator"`  // e.g. "BashOperator"
	MapIndex  int       `json:"map_index"` // -1 for an unmapped task; the index of a mapped (dynamic) task instance
}

// DAG is one orchestrator DAG's schedule state (Airflow GET /api/v1/dags/{dag_id}).
type DAG struct {
	ID        string    `json:"id"`
	Schedule  string    `json:"schedule"`    // the schedule as the orchestrator shows it (cron, preset or timetable summary); "" when unscheduled
	Paused    bool      `json:"paused"`      // a paused DAG schedules no runs
	NextRunAt time.Time `json:"next_run_at"` // next scheduled run; zero when none
}

// DAGScheduleReader is optional next to OrchestratorReader, asserted at use
// time like ClusterReader: without it (nil, or a client that cannot) the
// dag_schedule_gap tool answers ErrNotSupported. An unknown DAG is
// ErrNotFound.
type DAGScheduleReader interface {
	DAG(ctx context.Context, dagID string) (DAG, error)
}

// OrchestratorReader reads DAG runs, task instances and task logs (Airflow
// REST API v1 in P4c). An unknown DAG, run or task is ErrNotFound.
type OrchestratorReader interface {
	// DAGRuns returns the newest runs of dagID first, at most limit.
	DAGRuns(ctx context.Context, dagID string, limit int) ([]DAGRun, error)
	// TaskInstances returns the task instances of one run.
	TaskInstances(ctx context.Context, dagID, runID string) ([]TaskInstance, error)
	// TaskLogTail returns at most maxBytes from the END of one try's log,
	// cut at a UTF-8 boundary. Untrusted text.
	TaskLogTail(ctx context.Context, dagID, runID, taskID string, try int, maxBytes int) (string, error)
}

// ModelResult is one node's result in a dbt run_results.json.
type ModelResult struct {
	Model   string  `json:"model"`             // the node's unique_id, e.g. "model.analytics.fct_revenue"
	Status  string  `json:"status"`            // as dbt reports it: success | error | fail | warn | skipped | pass
	Message string  `json:"message,omitempty"` // dbt's message; untrusted text (may quote SQL or data)
	Rows    int64   `json:"rows"`              // adapter_response.rows_affected; -1 when the adapter does not report it
	Seconds float64 `json:"seconds"`           // execution_time
}

// RunResults is the latest dbt run_results.json.
type RunResults struct {
	GeneratedAt time.Time     `json:"generated_at"` // metadata.generated_at
	Results     []ModelResult `json:"results"`
}

// Lineage is one model's direct parents and children from manifest.json, as
// unique_ids; never nil slices.
type Lineage struct {
	Model    string   `json:"model"`
	Parents  []string `json:"parents"`
	Children []string `json:"children"`
}

// TransformReader reads dbt artifacts (a local artifacts directory or dbt
// Cloud in P4d). An unknown model is ErrNotFound.
type TransformReader interface {
	RunResults(ctx context.Context) (RunResults, error)              // latest run_results.json
	ModelLineage(ctx context.Context, model string) (Lineage, error) // parents and children from manifest.json; model is a name as owners.yaml lists it, or a dbt unique_id; a short name that matches several nodes is an error (dbt.ErrAmbiguous), and the caller retries with the unique_id
}

// ModelSQL is one dbt model's SQL from manifest.json.
type ModelSQL struct {
	Model    string `json:"model"`              // unique_id
	Path     string `json:"path,omitempty"`     // original_file_path
	Language string `json:"language,omitempty"` // sql | python
	Compiled bool   `json:"compiled"`           // false: raw_code (dbt never compiled the model)
	SQL      string `json:"sql"`                // untrusted text
}

// TransformSQLReader is optional next to TransformReader, asserted at use
// time like ClusterReader: without it (nil, or a client that cannot) a tool
// answers ErrNotSupported. An unknown model, or a node that is not a model,
// is ErrNotFound.
type TransformSQLReader interface {
	ModelSQL(ctx context.Context, model string) (ModelSQL, error)
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

// ---- X4: external changes (deploy, migration, vendor incident) ----
//
// Many incidents start outside the pipeline: an application deploy, a schema
// migration run by Flyway, Liquibase or dbt, or a vendor's own incident. They
// are rows of the same change log (PipelineChange, ChangeReader/ChangeWriter),
// not a parallel concept, so the "what changed" tool, the retention loop and
// the room store treat them like every other kind. Their Before is empty and
// their After is an ExternalChange. Summary is the title (one line, untrusted
// text, cut like every Summary); Actor is who deployed or ran the migration.
//
// Object per kind: deploy → the service deployed (ExternalChange.Service);
// migration → the history table read ("public.flyway_schema_history",
// "public.databasechangelog"), the dbt project for dbt, or the service for a
// webhook; vendor_incident → the vendor component ("confluent-cloud",
// "msk", "aiven-kafka"). Readers and writers of these rows see timestamps and
// metadata only: never a migration script's contents, never a row value.

// External change kinds of PipelineChange.Kind. (X4)
const (
	ChangeDeploy         = "deploy"          // an application or service deploy (webhook)
	ChangeMigration      = "migration"       // an applied schema migration (Flyway, Liquibase, dbt, or webhook)
	ChangeVendorIncident = "vendor_incident" // a vendor status-page incident (webhook or status-page poller)
)

// Change sources of PipelineChange.Source. The first two predate X4. (X4)
const (
	ChangeSourceWatcher    = "watcher"    // internal/watch (F8c)
	ChangeSourceDDLAudit   = "ddl_audit"  // the source DDL audit table (F8)
	ChangeSourceWebhook    = "webhook"    // POST /api/changes/webhook (X4a): deploy, migration or vendor_incident
	ChangeSourceFlyway     = "flyway"     // flyway_schema_history in the source database (X4b)
	ChangeSourceLiquibase  = "liquibase"  // databasechangelog in the source database (X4b)
	ChangeSourceDBT        = "dbt"        // dbt run results (X4b)
	ChangeSourceStatuspage = "statuspage" // a pipeline's owners.yaml vendor_status_url poller (X4a)
)

// ChangeKinds returns every PipelineChange.Kind, the F8 kinds first then the
// external ones, as a fresh slice. The room store and the "what changed" tool
// validate kinds against it, so a new kind is added here once. (X4)
func ChangeKinds() []string {
	return []string{ChangeDDL, ChangeConnectorConfig, ChangeConnectorState, ChangeSchemaVersion,
		ChangeDeploy, ChangeMigration, ChangeVendorIncident}
}

// IsExternalChange reports whether kind is a change made outside the
// pipeline (deploy, migration, vendor_incident). The agent ranks these first
// inside the alert window (X4c). (X4)
func IsExternalChange(kind string) bool {
	switch kind {
	case ChangeDeploy, ChangeMigration, ChangeVendorIncident:
		return true
	}
	return false
}

// Vendor incident states of ExternalChange.Status (the statuspage.io set). (X4)
const (
	VendorInvestigating = "investigating"
	VendorIdentified    = "identified"
	VendorMonitoring    = "monitoring"
	VendorResolved      = "resolved"
)

// Caps of ExternalChange fields, checked by ExternalChange.Validate. (X4)
const (
	MaxExternalChangeURLBytes     = 1024
	MaxExternalChangeServiceBytes = 128
	MaxExternalChangeVersionBytes = 128
)

// ExternalChange is PipelineChange.After of the external kinds. Every field
// is untrusted text from the sender or the customer's database: clients show
// it as text, the agent sees it only inside its untrusted-data wrapper. (X4)
type ExternalChange struct {
	// URL links to the deploy run, pull request, migration run or incident
	// page; http or https, no user info, ≤ MaxExternalChangeURLBytes. "" when
	// the source has none (Flyway, Liquibase).
	URL string `json:"url,omitempty"`
	// Service is the application service as owners.yaml teams[].owns names it,
	// so the agent can name the owning team; "" when unknown.
	Service string `json:"service,omitempty"`
	// Version is the migration version (Flyway version, Liquibase id, dbt
	// invocation id); migration only.
	Version string `json:"version,omitempty"`
	// Status is one of the Vendor* states; vendor_incident only. "" = unknown.
	Status string `json:"status,omitempty"`
}

// Validate checks the caps and the URL rule. A writer (the webhook, the
// migration reader, the poller) calls it before RecordChange and refuses or
// drops the row on error; it never fixes a value up. (X4)
func (e ExternalChange) Validate() error {
	if e.URL != "" && len(e.URL) <= MaxExternalChangeURLBytes {
		u, err := url.Parse(e.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return errors.New("clients: external change url must be an absolute http or https URL")
		}
		if u.User != nil {
			return errors.New("clients: external change url must not carry user info")
		}
	}
	for _, f := range []struct {
		name, v string
		max     int
	}{{"service", e.Service, MaxExternalChangeServiceBytes}, {"version", e.Version, MaxExternalChangeVersionBytes}, {"url", e.URL, MaxExternalChangeURLBytes}} {
		if len(f.v) > f.max {
			return fmt.Errorf("clients: external change %s is over %d bytes", f.name, f.max)
		}
		if !utf8.ValidString(f.v) || strings.ContainsFunc(f.v, unicode.IsControl) {
			return fmt.Errorf("clients: external change %s holds control characters or invalid UTF-8", f.name)
		}
	}
	switch e.Status {
	case "", VendorInvestigating, VendorIdentified, VendorMonitoring, VendorResolved:
	default:
		return fmt.Errorf("clients: external change status %q is not a known vendor incident state", e.Status)
	}
	return nil
}

// PipelineChange is one recorded change on a pipeline. Before and After hold
// only the changed keys (config), the two states (state), the version pair
// (schema) or nothing (DDL: the statement is in Summary, cut at 300 runes).
// A credential value is never stored: masked keys read "***". Actor is the
// DDL username or ""; Source is one of the ChangeSource* constants.
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

// ---- X3: watch sample history (Piperoom's own table, fed by the watcher) ----
//
// Not a customer system: the room store implements both interfaces over the
// table watch_samples(pipeline_id, metric, at, value); its migration number
// 00018 (migrations/00018_watch_samples.sql) is reserved for A5 (X3a). Rows
// older than config.WatchSampleRetention are deleted by the store's
// retention job. A sample is one number per pipeline and metric: never a row
// value, a record, a config value or free text.
//
// Migration numbers: 00017 is reserved by docs/design/onboarding.md
// (proposed), 00018 here, 00019 and 00020 by the fleet block below. goose
// (migrations/migrate.go) refuses to apply a number below one a database
// already ran, so migrations merge in number order: a reserved number whose
// row is not ready is given up at merge time (the leads renumber in this
// comment in the same PR), never filled in later.

// Metric names of a watch sample (watch_samples.metric, ?metric= of
// GET /api/pipelines/{pid}/trends, events.ForecastSignal.Metric,
// clients.WatchSignal.Metric). (X3)
const (
	WatchMetricSlotWALBytes         = "slot_wal_bytes"          // WAL the replication slot retains; limit: watch.wal_headroom_bytes
	WatchMetricDLQRecords           = "dlq_records"             // records in the pipeline's dead-letter topics; no limit (trend only)
	WatchMetricGroupLag             = "group_lag"               // total consumer lag of the pipeline's groups; no limit (trend only)
	WatchMetricDBSizeBytes          = "db_size_bytes"           // pg_database_size of the source; limit: watch.db_size_limit
	WatchMetricTopicOldestUnreadAge = "topic_oldest_unread_age" // seconds, the oldest unconsumed record of any pipeline topic; limit: that topic's retention.ms
)

// WatchMetrics returns every metric name, in a stable order, as a fresh slice
// (like ChangeKinds). A name outside it is refused by the store and by the
// trends route (400 validation). (X3)
func WatchMetrics() []string {
	return []string{WatchMetricSlotWALBytes, WatchMetricDLQRecords, WatchMetricGroupLag,
		WatchMetricDBSizeBytes, WatchMetricTopicOldestUnreadAge}
}

// WatchMetricUnit returns the unit of a metric ("bytes", "records" or
// "seconds"), or "" for a name outside WatchMetrics(). (X3)
func WatchMetricUnit(metric string) string {
	switch metric {
	case WatchMetricSlotWALBytes, WatchMetricDBSizeBytes:
		return "bytes"
	case WatchMetricDLQRecords, WatchMetricGroupLag:
		return "records"
	case WatchMetricTopicOldestUnreadAge:
		return "seconds"
	}
	return ""
}

// MaxSamplesPerSeries caps what Samples returns for one pipeline and metric:
// one sample per 15 minutes for 7 days. (X3)
const MaxSamplesPerSeries = 7 * 24 * 4

// SamplePoint is one watch sample; a list of them, oldest first, is the body
// of GET /api/pipelines/{pid}/trends. (X3)
type SamplePoint struct {
	At    time.Time `json:"at"`
	Value float64   `json:"value"`
}

// SampleWriter appends one sample. A second sample with the same (pipeline,
// metric, at) is not written again and is not an error. An unknown metric,
// an empty pipeline id, or a NaN or infinite value is refused (an error, no
// row). It writes Piperoom's own table, never a customer system, so the
// watcher (X3a) may hold one, as it holds a ChangeWriter. (X3)
type SampleWriter interface {
	AppendSample(ctx context.Context, pipelineID, metric string, at time.Time, value float64) error
}

// SampleReader lists the samples of one pipeline and metric with At >= since,
// oldest first, at most MaxSamplesPerSeries (the newest ones when more
// match). None is an empty list, not an error; an unknown metric is an
// error. Callers check the pipeline's tenant before they call it. (X3)
type SampleReader interface {
	Samples(ctx context.Context, pipelineID, metric string, since time.Time) ([]SamplePoint, error)
}

// ---- Fleet view: cross-pipeline changes, watcher status, connector config versions (X5-0) ----
//
// Piperoom's own tables again, not customer systems: the room store (store.PG,
// memstore) implements every interface in this block. The watcher writes
// WatchStatus and ConnectorConfigVersion (the Reader / Writer rule above does
// not apply, as for ChangeWriter); the fleet routes of docs/api.md ("Fleet
// view") only read. Nothing here carries a row value; a connector config value
// is stored only after actions.RedactConfig. Every method takes a context and
// the store bounds it at 10 s when the caller set no deadline. Tables, for A5:
// watch_status (migration 00019, X5a) and connector_config_versions (00020,
// X5c; not docs/design/onboarding.md's config_versions); the numbering rule is
// in the X3 block above.

// Caps of the fleet shapes, enforced by the store on write and by the routes
// on read.
const (
	MaxFleetChangesPage  = 200  // FleetChangeReader limit; the route's default page is 100
	MaxWatchSignals      = 32   // WatchStatus.Signals per pipeline
	MaxConfigKeys        = 1000 // keys of one ConnectorConfigVersion.Config, and of its ChangedKeys
	MaxConfigVersionList = 200  // ConfigVersionReader.ConfigVersions limit
)

// ChangeCursor is a position in the fleet change order (At, then ID, both
// descending). The zero value is the newest end.
type ChangeCursor struct {
	At time.Time `json:"at"`
	ID int64     `json:"id"`
}

// FleetChangeReader lists the changes of several pipelines in one page, newest
// first by (At, ID), at most limit (1..MaxFleetChangesPage), only rows strictly
// after before in that order, with At >= since (zero = no bound). kinds nil =
// every kind; pipelineIDs empty = no rows (the caller passes the pipelines the
// user can see, never "all"). Optional next to ChangeReader, asserted at use
// time: without it the route merges ChangeReader pages per pipeline.
type FleetChangeReader interface {
	ListFleetChanges(ctx context.Context, pipelineIDs []string, since time.Time, kinds []string, before ChangeCursor, limit int) ([]PipelineChange, error)
}

// Watch status states of WatchSignal.State.
const (
	WatchFiring = "firing" // the signal fires now
	WatchAcked  = "acked"  // still fires, but the driver closed its room (docs/design/watcher.md)
)

// WatchSignal is one signal of a pipeline that is firing or acked; a signal in
// state ok is not listed. Node is the topology node id the signal is about
// ("connect:<connector>", "group:<group>", "dlq:<topic>"; "" when the watcher
// cannot name one). Metric and ETA are set only for Signal "forecast" (X3a):
// the sample metric and the time the forecast reaches its limit.
type WatchSignal struct {
	Signal   string     `json:"signal"`   // an owners.Signal* name or an X-wave events.Signal* name (count_drift … config_drift)
	State    string     `json:"state"`    // WatchFiring | WatchAcked
	Severity string     `json:"severity"` // "sev1".."sev4"
	Node     string     `json:"node,omitempty"`
	Since    time.Time  `json:"since"` // when it started firing
	Metric   string     `json:"metric,omitempty"`
	ETA      *time.Time `json:"eta,omitempty"`
}

// WatchStatus is the watcher's latest view of one pipeline: the row the fleet
// view reads instead of asking the watcher, which runs on one replica only
// (docs/design/replicas.md). Signals holds at most MaxWatchSignals entries and
// no evidence values.
type WatchStatus struct {
	PipelineID string        `json:"pipeline_id"`
	CheckedAt  time.Time     `json:"checked_at"` // the tick that produced it
	Signals    []WatchSignal `json:"signals"`
}

// WatchStatusWriter replaces the status row of s.PipelineID. The watcher calls
// it at most once per pipeline per tick; a failed write is logged by the
// watcher and never stops a tick. More than MaxWatchSignals signals is refused
// with an error, nothing written.
type WatchStatusWriter interface {
	PutWatchStatus(ctx context.Context, s WatchStatus) error
}

// WatchStatusReader returns the status rows of the given pipelines; a pipeline
// the watcher never checked has no row (not an error).
type WatchStatusReader interface {
	WatchStatuses(ctx context.Context, pipelineIDs []string) ([]WatchStatus, error)
}

// ConnectorConfigVersion is one config of a connector as the watcher saw it
// (X3b records, X5c stores). Config holds the values after
// actions.RedactConfig, applied once by the caller (actions.DigestConfig's
// Redacted; a raw config never crosses this interface), and Hash is
// actions.ConfigHash of that map, the same id as config_drift's hashes, so no
// secret value is stored or hashed. The store computes Hash itself (a
// caller's Hash is ignored) and refuses a Config in which an
// actions.LooksSecret key holds anything but actions.RedactedValue.
// ChangedKeys is actions.ChangedConfigKeys from the watcher's previous digest
// of the connector: names only, sorted, events.SecretKeyChanged for a
// credential key, so a change only to a secret value is a new version with
// the same Hash. It is empty when the watcher has no earlier digest in
// memory (its first poll after it starts); the diff route compares the
// stored forms either way. By is "proposal:<id>" when an executed Piperoom
// proposal produced the config, "" otherwise. Config is empty in list
// answers.
type ConnectorConfigVersion struct {
	PipelineID  string            `json:"pipeline_id"`
	Connector   string            `json:"connector"`
	Hash        string            `json:"hash"`
	At          time.Time         `json:"at"`
	By          string            `json:"by"`
	ChangedKeys []string          `json:"changed_keys"`
	Config      map[string]string `json:"config,omitempty"`
}

// ConfigVersionWriter appends v unless it would repeat the connector's latest
// version (same Hash and no ChangedKeys); inserted reports whether it did. A
// Config or ChangedKeys over MaxConfigKeys entries is refused with an error,
// nothing written.
type ConfigVersionWriter interface {
	RecordConfigVersion(ctx context.Context, v ConnectorConfigVersion) (inserted bool, err error)
}

// ConfigVersionReader reads stored versions. ConfigVersions lists one
// connector's versions newest first, at most limit (1..MaxConfigVersionList),
// Config empty. ConfigVersion returns the newest version with that Hash,
// Config filled; none is ErrNotFound.
type ConfigVersionReader interface {
	ConfigVersions(ctx context.Context, pipelineID, connector string, limit int) ([]ConnectorConfigVersion, error)
	ConfigVersion(ctx context.Context, pipelineID, connector, hash string) (ConnectorConfigVersion, error)
}
