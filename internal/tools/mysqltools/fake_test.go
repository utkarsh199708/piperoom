package mysqltools

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/mysql"
	"piperoom/internal/owners"
)

// fakeSrc is a MySQL source client: SourceDB (unused methods panic through
// the nil embedded interface) plus BinlogReader.
type fakeSrc struct {
	clients.SourceDB
	mu    sync.Mutex
	st    mysql.BinlogStatus
	err   error
	calls int
}

func (f *fakeSrc) BinlogStatus(context.Context) (mysql.BinlogStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.st, f.err
}

var _ mysql.BinlogReader = (*fakeSrc)(nil)

// pgOnlySrc has no BinlogReader, like the Postgres client.
type pgOnlySrc struct{ clients.SourceDB }

// fakeConnect is a ConnectClient with stored offsets.
type fakeConnect struct {
	mu      sync.Mutex
	configs map[string]map[string]string
	states  map[string]string
	offsets map[string][]connect.SourceOffset
	offErr  error
	calls   int
}

func (f *fakeConnect) count() {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
}

func (f *fakeConnect) ListConnectors(context.Context) ([]string, error) {
	f.count()
	return nil, errors.New("fake: not used")
}

func (f *fakeConnect) Status(_ context.Context, name string) (clients.ConnectorStatus, error) {
	f.count()
	s, ok := f.states[name]
	if !ok {
		return clients.ConnectorStatus{}, clients.ErrNotFound
	}
	return clients.ConnectorStatus{Name: name, Type: "source", State: s}, nil
}

func (f *fakeConnect) Config(_ context.Context, name string) (map[string]string, error) {
	f.count()
	c, ok := f.configs[name]
	if !ok {
		return nil, clients.ErrNotFound
	}
	return c, nil
}

func (f *fakeConnect) RestartTask(context.Context, string, int) error {
	return errors.New("fake: no writes")
}
func (f *fakeConnect) RestartConnector(context.Context, string) error {
	return errors.New("fake: no writes")
}

func (f *fakeConnect) SourceOffsets(_ context.Context, name string) ([]connect.SourceOffset, error) {
	f.count()
	if f.offErr != nil {
		return nil, f.offErr
	}
	return f.offsets[name], nil
}

var (
	_ clients.ConnectClient      = (*fakeConnect)(nil)
	_ connect.SourceOffsetReader = (*fakeConnect)(nil)
)

// noOffsetsConnect hides SourceOffsets.
type noOffsetsConnect struct{ clients.ConnectReader }

const ownersYAML = `
pipelines:
  - id: orders-mysql
    source_kind: mysql
    topic_prefix: shop
    source_tables: [shop.orders, shop.order_items]
    connectors: [orders-mysql, orders-mysql-2]
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
layers:
  mysql: dana
  postgres: priya
`

func loadOwners(t *testing.T) *owners.Owners {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(ownersYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	return o
}

func pipeline(t *testing.T, o *owners.Owners, id string) owners.Pipeline {
	t.Helper()
	p, ok := o.Pipeline(id)
	if !ok {
		t.Fatalf("no pipeline %s", id)
	}
	return p
}

var testNow = time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

func mysqlCfg(serverID string) map[string]string {
	return map[string]string{
		"connector.class":    mysqlConnectorClass,
		"database.server.id": serverID,
		"database.user":      "debezium",
		"database.password":  "hunter2",
	}
}
