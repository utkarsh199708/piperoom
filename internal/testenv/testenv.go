//go:build integration

// Package testenv helps integration tests use the dev stack (dev/docker-compose.yml).
// Start it with `go run ./cmd/devctl up` (or `reset` for a clean state).
package testenv

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/config"
	"piperoom/internal/stacklock"
)

// Dep is one service of the dev stack an integration test needs.
type Dep int

// Dev stack services an integration test can depend on.
const (
	Postgres  Dep = iota + 1 // source Postgres (SOURCE_DB_URL)
	Warehouse                // dev warehouse Postgres (WAREHOUSE_DB_URL)
	RoomDB                   // room service database (PIPEROOM_DB_URL)
	Kafka                    // KAFKA_BROKERS
	Registry                 // SCHEMA_REGISTRY_URL
	Connect                  // CONNECT_URL
)

func (d Dep) String() string {
	switch d {
	case Postgres:
		return "postgres"
	case Warehouse:
		return "warehouse"
	case RoomDB:
		return "room-db"
	case Kafka:
		return "kafka"
	case Registry:
		return "schema-registry"
	case Connect:
		return "connect"
	}
	return fmt.Sprintf("dep(%d)", int(d))
}

// allDeps is the fallback for callers whose needs cannot be inferred: the
// whole stack must be up (the old behaviour, where Connect stood in for it).
var allDeps = []Dep{Postgres, Warehouse, Kafka, Registry, Connect}

// inferred maps a caller's package directory to the services it uses, so the
// existing Require(t) callers stop skipping just because Connect (the slowest
// service to start) is down. Keys are matched as suffixes of the caller's dir.
var inferred = []struct {
	dir  string
	deps []Dep
}{
	{"internal/clients/postgres", []Dep{Postgres}},
	{"internal/clients/warehouse", []Dep{Warehouse}},
	{"internal/clients/kafka", []Dep{Kafka}},
	{"internal/clients/schemareg", []Dep{Registry}},
	{"internal/clients/connect", []Dep{Connect}},
	{"internal/tools/dbtools", []Dep{Postgres, Warehouse}},
	{"internal/testenv", nil}, // its own test only checks the helpers
}

// Require returns the dev config, or skips the test when a dev-stack service
// it needs is not reachable or another agent holds the shared stack lock.
// With deps it checks exactly those services. Without deps it infers them
// from the calling test's package (see inferred) and falls back to the whole
// stack for packages it does not know.
func Require(t *testing.T, deps ...Dep) config.Config {
	t.Helper()
	if len(deps) == 0 {
		_, file, _, ok := runtime.Caller(1)
		deps = inferDeps(file, ok)
		if root, _, _ := strings.Cut(t.Name(), "/"); needsPipeline[root] && !hasDep(deps, Connect) {
			deps = append(append([]Dep(nil), deps...), Connect)
		}
	}
	return require(t, deps)
}

// needsPipeline lists Require(t) callers in inferred packages whose fixtures
// (slot, publication, pg.public.* topics) are created by the running Debezium
// connector, so they also need Connect. Callers should move to explicit deps
// (Require(t, testenv.Postgres, testenv.Connect)); this keeps them correct
// until they do.
var needsPipeline = map[string]bool{
	"TestReplicationSlotsIntegration":                        true,
	"TestPublicationTablesIntegration":                       true,
	"TestPublicationTablesSourceNotInPublicationIntegration": true,
	"TestTopicsExcludesInternal":                             true,
	"TestTopicOffsets":                                       true,
	"TestConsumerGroupsAndLag":                               true,
	"TestFindByKeyRealData":                                  true,
	"TestDescribeTopicConfigs":                               true,
	"TestPgPublicationTablesIntegration":                     true,
}

func hasDep(deps []Dep, d Dep) bool {
	for _, x := range deps {
		if x == d {
			return true
		}
	}
	return false
}

// RequirePostgres skips unless the source Postgres is reachable.
func RequirePostgres(t *testing.T) config.Config { t.Helper(); return require(t, []Dep{Postgres}) }

// RequireWarehouse skips unless the dev warehouse Postgres is reachable.
func RequireWarehouse(t *testing.T) config.Config { t.Helper(); return require(t, []Dep{Warehouse}) }

// RequireRoomDB skips unless the room service database is reachable.
func RequireRoomDB(t *testing.T) config.Config { t.Helper(); return require(t, []Dep{RoomDB}) }

// RequireKafka skips unless a Kafka broker is reachable.
func RequireKafka(t *testing.T) config.Config { t.Helper(); return require(t, []Dep{Kafka}) }

// RequireRegistry skips unless the Schema Registry is reachable.
func RequireRegistry(t *testing.T) config.Config { t.Helper(); return require(t, []Dep{Registry}) }

// RequireConnect skips unless Kafka Connect is reachable.
func RequireConnect(t *testing.T) config.Config { t.Helper(); return require(t, []Dep{Connect}) }

func inferDeps(file string, ok bool) []Dep {
	if !ok || file == "" {
		return allDeps
	}
	dir := filepath.ToSlash(filepath.Dir(filepath.ToSlash(file)))
	for _, e := range inferred {
		if dir == e.dir || strings.HasSuffix(dir, "/"+e.dir) {
			return e.deps
		}
	}
	return allDeps
}

func require(t *testing.T, deps []Dep) config.Config {
	t.Helper()
	lk := stacklock.New(stacklock.Path())
	if msg := lockConflict(lk, os.Getenv(stackOwner)); msg != "" {
		t.Skipf("testenv: %s", msg)
	}

	cfg := config.Load()
	for _, d := range deps {
		addr, err := depAddr(cfg, d)
		if err != nil {
			t.Fatalf("testenv: %v", err)
		}
		if addr == "" {
			t.Skipf("testenv: %s is not configured", d)
		}
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			t.Skipf("testenv: %s not reachable at %s (run `go run ./cmd/devctl up`): %v", d, addr, err)
		}
		_ = conn.Close()
		if dsn := pgURL(cfg, d); dsn != "" {
			// A port that answers may be another Postgres (CI has only the room
			// database on 5432): the test's own database must accept a login.
			if err := pingPG(dsn); err != nil {
				t.Skipf("testenv: %s at %s does not accept a connection: %v", d, addr, err)
			}
		}
	}
	return cfg
}

// pgURL is the Postgres URL behind d, or "" when d is not a Postgres.
func pgURL(cfg config.Config, d Dep) string {
	switch d {
	case Postgres:
		return cfg.SourceDBURL
	case Warehouse:
		return cfg.WarehouseDBURL
	case RoomDB:
		return cfg.RoomDBURL
	}
	return ""
}

func pingPG(dsn string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer c.Close(context.Background())
	return c.Ping(ctx)
}

// depAddr returns the host:port to probe for d, or "" when d is switched off.
func depAddr(cfg config.Config, d Dep) (string, error) {
	switch d {
	case Postgres:
		return urlHost("SOURCE_DB_URL", cfg.SourceDBURL, "5432")
	case Warehouse:
		if cfg.WarehouseKind != "" && cfg.WarehouseKind != "postgres" {
			return "", nil // a Snowflake DSN is not a dev-stack service
		}
		return urlHost("WAREHOUSE_DB_URL", cfg.WarehouseDBURL, "5432")
	case RoomDB:
		return urlHost("PIPEROOM_DB_URL", cfg.RoomDBURL, "5432")
	case Kafka:
		b, _, _ := strings.Cut(cfg.KafkaBrokers, ",")
		return strings.TrimSpace(b), nil
	case Registry:
		if cfg.SchemaRegistryURL == "" {
			return "", nil
		}
		return urlHost("SCHEMA_REGISTRY_URL", cfg.SchemaRegistryURL, "")
	case Connect:
		return urlHost("CONNECT_URL", cfg.ConnectURL, "")
	}
	return "", fmt.Errorf("unknown dependency %s", d)
}

func urlHost(env, raw, defPort string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("bad %s %q", env, redactURL(raw))
	}
	if u.Port() != "" {
		return u.Host, nil
	}
	port := defPort
	if port == "" {
		port = "80"
		if u.Scheme == "https" {
			port = "443"
		}
	}
	return net.JoinHostPort(u.Hostname(), port), nil
}

// redactURL drops userinfo so a DB password never lands in test output.
func redactURL(raw string) string {
	if u, err := url.Parse(raw); err == nil {
		return u.Redacted()
	}
	return "<unparseable>"
}

// Name returns a unique name like "it_orders_3fa9c1" so parallel tests do not collide.
func Name(prefix string) string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return "it_" + prefix + "_" + hex.EncodeToString(b)
}

// WaitFor polls cond every 500 ms until it returns true, an error, or the timeout.
func WaitFor(t *testing.T, timeout time.Duration, cond func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, err := cond()
		if err != nil {
			t.Fatalf("testenv: wait: %v", err)
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("testenv: condition not met within %s", timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
