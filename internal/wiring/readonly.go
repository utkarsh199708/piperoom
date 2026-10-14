package wiring

import (
	"context"
	"errors"

	"piperoom/internal/clients"
)

// Read tools and the topology builder get only the read half of each client
// (clients.go, Reader/Writer split). Kafka, source Postgres and the warehouse
// have no write methods, so they pass through as is; their optional read
// interfaces (DLQReader, slot and DDL listers) keep working. Kafka Connect and
// Schema Registry do write, so the tools get a value that holds only the
// Reader: the write methods below refuse, and no type assertion reaches the
// concrete client's. The write halves go to package actions only.
//
// The adapters exist because kafkatools.Deps and dbtools.Deps (A2/A3) and
// topology.NewBuilder still take the whole *Client interfaces. Once R5c
// switches them to Readers, readConnect and readRegistry can hand over the
// Reader itself.

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
