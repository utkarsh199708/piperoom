// Package clients defines thin clients for the systems Piperoom inspects.
// Implementations live in sub-packages (connect, schemareg, kafka, postgres,
// warehouse). Tools and actions depend on these interfaces, so they can be
// tested with fakes.
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

type ConnectClient interface {
	ListConnectors(ctx context.Context) ([]string, error)
	Status(ctx context.Context, name string) (ConnectorStatus, error)
	Config(ctx context.Context, name string) (map[string]string, error)
	// Write — used only by actions after approval.
	RestartTask(ctx context.Context, name string, task int) error
	RestartConnector(ctx context.Context, name string) error
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

type SchemaRegistryClient interface {
	Subjects(ctx context.Context) ([]string, error)
	Versions(ctx context.Context, subject string) ([]int, error)
	// Schema returns one version; version -1 means latest.
	Schema(ctx context.Context, subject string, version int) (SchemaVersion, error)
	// Compatibility returns the subject level, falling back to the global level.
	Compatibility(ctx context.Context, subject string) (level string, subjectLevel bool, err error)
	// Write — used only by actions after approval.
	SetCompatibility(ctx context.Context, subject, level string) error
	Register(ctx context.Context, subject, schema string) (id int, err error)
}

type SchemaVersion struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	ID      int    `json:"id"`
	Schema  string `json:"schema"`
}

// ---- Kafka (admin + small reads) ----

type KafkaAdmin interface {
	Topics(ctx context.Context) ([]string, error)
	TopicOffsets(ctx context.Context, topic string) ([]PartitionOffsets, error)
	ConsumerGroups(ctx context.Context) ([]string, error)
	GroupLag(ctx context.Context, group string) (GroupLag, error)
	// FindByKey scans the topic from the first offset at or after since and
	// returns records whose key matches (string compare of the decoded key).
	// It stops at maxScan records per partition.
	FindByKey(ctx context.Context, topic, key string, since time.Time, maxScan int) ([]RecordRef, error)
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

// ---- Source Postgres (the CDC source) ----

type SourceDB interface {
	ReplicationSlots(ctx context.Context) ([]ReplicationSlot, error)
	// RecentDDL reads the DDL audit table written by an event trigger
	// (see dev/postgres-init). Newest first.
	RecentDDL(ctx context.Context, since time.Time, limit int) ([]DDLChange, error)
	// LongTransactions lists open transactions older than olderThan that hold a
	// transaction id (writers). Read-only transactions are left out: they pin
	// no WAL for a replication slot, so they are noise for CDC trouble.
	LongTransactions(ctx context.Context, olderThan time.Duration) ([]Transaction, error)
	// Row reads one row by primary key. table is "schema.table".
	Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error)
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
	// it has written. LongTransactions returns only such rows.
	HasXID bool `json:"has_xid"`
}

// ---- Warehouse (Snowflake in production; Postgres in dev) ----

type Warehouse interface {
	// Freshness returns max(tsColumn) of table. table is "schema.table".
	Freshness(ctx context.Context, table, tsColumn string) (time.Time, error)
	Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error)
}
