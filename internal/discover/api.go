// Package discover drafts an owners file from a live cluster: one pipeline per
// Debezium Postgres source connector, with its topic prefix, source tables,
// topics' consumer groups and notes for a human to review.
//
// It is read-only by type: the narrow interfaces below expose only read
// methods of the client contracts, and compile-time assertions prove the real
// clients satisfy them, so no write method is reachable from this package.
//
// Debezium property reference:
// https://debezium.io/documentation/reference/stable/connectors/postgresql.html
// (database.server.name became topic.prefix in 2.0:
// https://debezium.io/blog/2022/10/17/debezium-2-0-final-released/).
package discover

import (
	"context"
	"log/slog"
	"time"

	"piperoom/internal/clients"
)

// ConnectReader is the read-only part of clients.ConnectClient used here.
type ConnectReader interface {
	ListConnectors(ctx context.Context) ([]string, error)
	Config(ctx context.Context, name string) (map[string]string, error)
}

// KafkaReader is the read-only part of clients.KafkaAdmin used here.
type KafkaReader interface {
	Topics(ctx context.Context) ([]string, error)
	ConsumerGroups(ctx context.Context) ([]string, error)
	GroupLag(ctx context.Context, group string) (clients.GroupLag, error)
}

// SubjectLister is the read-only part of clients.SchemaRegistryClient used here.
type SubjectLister interface {
	Subjects(ctx context.Context) ([]string, error)
}

// SlotLister is the read-only part of clients.SourceDB used here.
type SlotLister interface {
	ReplicationSlots(ctx context.Context) ([]clients.ReplicationSlot, error)
}

var (
	_ ConnectReader = clients.ConnectClient(nil)
	_ KafkaReader   = clients.KafkaAdmin(nil)
	_ SubjectLister = clients.SchemaRegistryClient(nil)
	_ SlotLister    = clients.SourceDB(nil)
)

// Deps are the clients discovery reads from. Source may be nil: the slot
// check is then skipped.
type Deps struct {
	Connect  ConnectReader
	Kafka    KafkaReader
	Registry SubjectLister
	Source   SlotLister
}

// Options tune a run. Zero values take the defaults.
type Options struct {
	CallTimeout time.Duration // per client call; default 10 s
	MaxGroups   int           // consumer groups checked; default 1000
	Logger      *slog.Logger  // error details; default slog.Default()
}

const (
	defaultCallTimeout = 10 * time.Second
	defaultMaxGroups   = 1000
)

// Pipeline is one drafted owners.yaml pipeline. Lists are sorted and
// de-duplicated. Notes are fixed text plus names only, never config values.
type Pipeline struct {
	ID             string
	Connector      string // connector name; also the draft title
	TopicPrefix    string
	SourceTables   []string
	ConsumerGroups []string
	Notes          []string
}

// Skipped is a connector that produced no pipeline, with a short reason.
type Skipped struct {
	Connector string
	Reason    string
}

// Result is a whole discovery run.
type Result struct {
	Pipelines []Pipeline // sorted by ID
	Skipped   []Skipped  // sorted by Connector
	Warnings  []string   // run-wide problems, sorted
}
