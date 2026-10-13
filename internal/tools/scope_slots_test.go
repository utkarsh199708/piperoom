package tools

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/owners"
)

// configs is a ConfigReader over fixed connector configs that records every
// call; a name it does not know gives err (or a not-found error).
type configs struct {
	cfg   map[string]map[string]string
	err   error
	calls []string
}

func (c *configs) Config(_ context.Context, name string) (map[string]string, error) {
	c.calls = append(c.calls, name)
	if c.err != nil {
		return nil, c.err
	}
	cfg, ok := c.cfg[name]
	if !ok {
		return nil, errors.New("connector not found")
	}
	return cfg, nil
}

func slotScope(t *testing.T, connectors ...string) Scope {
	t.Helper()
	o := scopeTestOwners()
	o.Pipelines[0].Connectors = connectors
	sc, err := ScopeFor(WithPipelineID(context.Background(), "orders"), o)
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

const pgClass = "io.debezium.connector.postgresql.PostgresConnector"

func TestSlotsFromConnectorConfigs(t *testing.T) {
	cr := &configs{cfg: map[string]map[string]string{
		"src-a": {"connector.class": pgClass, "slot.name": "orders_slot"},
		"src-b": {"connector.class": pgClass}, // Debezium's default slot
		"sink":  {"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector", "topics": "pg.public.orders"},
		"dup":   {"connector.class": pgClass, "slot.name": "orders_slot"},
	}}
	got, err := slotScope(t, "src-a", "src-b", "sink", "dup").Slots(context.Background(), cr)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"orders_slot", "debezium"}; !slices.Equal(got, want) {
		t.Fatalf("Slots = %v, want %v", got, want)
	}
	if len(cr.calls) != 4 {
		t.Fatalf("every connector config must be read, calls = %v", cr.calls)
	}
}

func TestSlotsNoPostgresSourceIsEmpty(t *testing.T) {
	cr := &configs{cfg: map[string]map[string]string{"sink": {"connector.class": "SomeSink"}}}
	got, err := slotScope(t, "sink").Slots(context.Background(), cr)
	if err != nil || len(got) != 0 {
		t.Fatalf("Slots = %v, %v; want none and no error", got, err)
	}
}

// Fail closed: a config that cannot be read, a bad slot.name or no Connect
// client is an error with no slot names, never a fallback to all slots or to
// the default slot (which can belong to another pipeline).
func TestSlotsFailClosed(t *testing.T) {
	down := errors.New("connect: dial tcp: connection refused")
	t.Run("config unreadable", func(t *testing.T) {
		cr := &configs{err: down}
		got, err := slotScope(t, "src-a").Slots(context.Background(), cr)
		if !errors.Is(err, down) || got != nil {
			t.Fatalf("Slots = %v, %v; want the read error and no slots", got, err)
		}
		if !strings.Contains(err.Error(), "src-a") || !strings.Contains(err.Error(), "watch.slot") {
			t.Fatalf("error must name the connector and the way out: %v", err)
		}
	})
	t.Run("second config unreadable", func(t *testing.T) {
		cr := &configs{cfg: map[string]map[string]string{"src-a": {"connector.class": pgClass, "slot.name": "orders_slot"}}}
		got, err := slotScope(t, "src-a", "gone").Slots(context.Background(), cr)
		if err == nil || got != nil {
			t.Fatalf("Slots = %v, %v; want an error and no partial list", got, err)
		}
	})
	t.Run("invalid slot.name", func(t *testing.T) {
		cr := &configs{cfg: map[string]map[string]string{"src-a": {"connector.class": pgClass, "slot.name": "Bad-Slot; drop"}}}
		got, err := slotScope(t, "src-a").Slots(context.Background(), cr)
		if err == nil || got != nil {
			t.Fatalf("Slots = %v, %v; want an error, never the default slot", got, err)
		}
	})
	t.Run("no Connect client", func(t *testing.T) {
		got, err := slotScope(t, "src-a").Slots(context.Background(), nil)
		if err == nil || got != nil {
			t.Fatalf("Slots = %v, %v; want an error", got, err)
		}
	})
	t.Run("unscoped", func(t *testing.T) {
		sc, _ := ScopeFor(WithUnscoped(context.Background()), nil)
		if _, err := sc.Slots(context.Background(), &configs{}); err == nil {
			t.Fatal("Slots unscoped must be an error: unscoped callers do not filter")
		}
	})
}

// owners.yaml watch.slot names the slot without Connect: the slot stays
// visible while Connect is down, and the connector configs are not read.
func TestSlotsDeclaredInOwnersNeedNoConnect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owners.yaml")
	yaml := `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    watch:
      slot: orders_declared
layers:
  postgres: priya
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := ScopeFor(WithPipelineID(context.Background(), "orders"), o)
	if err != nil {
		t.Fatal(err)
	}
	cr := &configs{err: errors.New("connect down")}
	got, err := sc.Slots(context.Background(), cr)
	if err != nil || !slices.Equal(got, []string{"orders_declared"}) {
		t.Fatalf("Slots = %v, %v; want [orders_declared]", got, err)
	}
	if len(cr.calls) != 0 {
		t.Fatalf("Connect was called: %v", cr.calls)
	}
}

func TestNotShown(t *testing.T) {
	if NotShown(0, "slots") != "" || NotShown(-1, "slots") != "" {
		t.Fatal("NotShown must be empty for n <= 0")
	}
	if got, want := NotShown(3, "replication slots"), "3 other replication slots not shown (not part of this room's pipeline)"; got != want {
		t.Fatalf("NotShown = %q, want %q", got, want)
	}
}
