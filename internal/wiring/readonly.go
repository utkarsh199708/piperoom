package wiring

import (
	"context"
	"errors"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/schemareg"
)

// Read tools and the topology builder get only the read half of each client
// (clients.go, Reader/Writer split). Kafka, source Postgres and the warehouse
// have no write methods, so they pass through as is; their optional read
// interfaces (DLQReader, slot and DDL listers) keep working. Kafka Connect and
// Schema Registry do write, so the tools get a value that holds only the
// Reader: the write methods below refuse, and no type assertion reaches the
// concrete client's. The write halves go to package actions only.
//
// The adapters exist because kafkatools.Deps, dbtools.Deps,
// debeziumtools.Deps and topology.NewBuilder still take the whole *Client
// interfaces. When their owners switch them to Readers (no PR does yet),
// readConnect and readRegistry can hand over the Reader itself.

// ErrReadOnly is returned by a write method reached through a read-only client.
// Not to be confused with tools.ErrNotReadOnly, which a read-only registry
// returns for a tool whose Kind is not KindRead.
var ErrReadOnly = errors.New("wiring: write through a read-only client")

// readOnlyConnect is a ConnectReader dressed as a ConnectClient whose writes
// refuse.
type readOnlyConnect struct{ r clients.ConnectReader }

func (c readOnlyConnect) ListConnectors(ctx context.Context) ([]string, error) {
	return c.r.ListConnectors(ctx)
}
func (c readOnlyConnect) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	return c.r.Status(ctx, name)
}
func (c readOnlyConnect) Config(ctx context.Context, name string) (map[string]string, error) {
	return c.r.Config(ctx, name)
}
func (readOnlyConnect) RestartTask(context.Context, string, int) error { return ErrReadOnly }
func (readOnlyConnect) RestartConnector(context.Context, string) error { return ErrReadOnly }

// readOnlyRegistry is a SchemaRegistryReader dressed as a
// SchemaRegistryClient whose writes refuse.
type readOnlyRegistry struct{ r clients.SchemaRegistryReader }

func (s readOnlyRegistry) Subjects(ctx context.Context) ([]string, error) { return s.r.Subjects(ctx) }
func (s readOnlyRegistry) Versions(ctx context.Context, subject string) ([]int, error) {
	return s.r.Versions(ctx, subject)
}
func (s readOnlyRegistry) Schema(ctx context.Context, subject string, version int) (clients.SchemaVersion, error) {
	return s.r.Schema(ctx, subject, version)
}
func (s readOnlyRegistry) Compatibility(ctx context.Context, subject string) (string, bool, error) {
	return s.r.Compatibility(ctx, subject)
}
func (readOnlyRegistry) SetCompatibility(context.Context, string, string) error { return ErrReadOnly }
func (readOnlyRegistry) Register(context.Context, string, string) (int, error) {
	return 0, ErrReadOnly
}

// readConnect returns w's Connect client for read tools: nil when none is
// configured (the tools then say so), else read-only.
func readConnect(w *Wired) clients.ConnectClient {
	if w.Connect == nil {
		return nil
	}
	return readOnlyConnect{r: w.Connect}
}

// readRegistry returns w's Schema Registry client for read tools: nil when
// none is configured, else read-only.
func readRegistry(w *Wired) clients.SchemaRegistryClient {
	if w.Registry == nil {
		return nil
	}
	return readOnlyRegistry{r: w.Registry}
}

// With write mode off the actions get read-only clients too (writemode.go).
// Their Validate and Preview only read, but Preview also calls two dry-run
// endpoints that change nothing (actions.Preview: "the documented dry-run
// endpoints"): Connect's config validate and the Registry's compatibility
// check. The actions find them by a type assertion on their client, so the
// read-only value keeps each one exactly when the real client has it, and the
// proposal card shows the same dry-run verdict in both modes. These two
// interfaces mirror package actions' configValidator and compatibilityChecker;
// TestWriteModeOffKeepsDryRuns fails if they drift apart.
type (
	connectValidator interface {
		ValidateConfig(ctx context.Context, class string, cfg map[string]string) (connect.ValidateResult, error)
	}
	registryChecker interface {
		CheckCompatibility(ctx context.Context, subject, schema string) (ok bool, messages []string, err error)
	}
)

// The real clients have both dry-runs, so with write mode off the preview of
// a config change or a new schema keeps its verdict.
var (
	_ connectValidator = (*connect.Client)(nil)
	_ registryChecker  = (*schemareg.Client)(nil)
)

// readOnlyConnectDryRun is readOnlyConnect plus Connect's config validate.
type readOnlyConnectDryRun struct {
	readOnlyConnect
	v connectValidator
}

func (c readOnlyConnectDryRun) ValidateConfig(ctx context.Context, class string, cfg map[string]string) (connect.ValidateResult, error) {
	return c.v.ValidateConfig(ctx, class, cfg)
}

// readOnlyRegistryDryRun is readOnlyRegistry plus the compatibility check.
type readOnlyRegistryDryRun struct {
	readOnlyRegistry
	c registryChecker
}

func (s readOnlyRegistryDryRun) CheckCompatibility(ctx context.Context, subject, schema string) (bool, []string, error) {
	return s.c.CheckCompatibility(ctx, subject, schema)
}

// readOnlyActionConnect is w's Connect client for the actions with write mode
// off: nil when none is configured (as with write mode on), else read-only,
// with config validate when the client has it.
func readOnlyActionConnect(w *Wired) clients.ConnectClient {
	c := readConnect(w)
	ro, ok := c.(readOnlyConnect)
	if !ok {
		return c
	}
	if v, ok := ro.r.(connectValidator); ok {
		return readOnlyConnectDryRun{readOnlyConnect: ro, v: v}
	}
	return ro
}

// readOnlyActionRegistry is w's Schema Registry client for the actions with
// write mode off, never nil: noRegistry when none is configured (as with
// write mode on), else read-only, with the compatibility check when the
// client has it.
func readOnlyActionRegistry(w *Wired) clients.SchemaRegistryClient {
	ro, ok := readRegistry(w).(readOnlyRegistry)
	if !ok {
		return noRegistry{}
	}
	if c, ok := ro.r.(registryChecker); ok {
		return readOnlyRegistryDryRun{readOnlyRegistry: ro, c: c}
	}
	return ro
}
