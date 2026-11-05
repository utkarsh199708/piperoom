//go:build integration

package connprobe

import (
	"context"
	"encoding/json"
	"testing"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// These tests only read the dev stack (docker compose -f dev/docker-compose.yml
// up -d): they create, change and delete nothing.

func devTarget(k Kind) Target {
	return Target{
		Kind: k,
		Config: config.Config{
			KafkaBrokers:      "localhost:29092",
			ConnectURL:        "http://localhost:8083",
			SchemaRegistryURL: "http://localhost:8081",
		},
		Pipelines: []owners.Pipeline{{
			ID:             "orders",
			TopicPrefix:    "pg",
			SourceTables:   []string{"public.orders"},
			Connectors:     []string{"pg-orders-connector"},
			ConsumerGroups: []string{"order-sync"},
		}},
	}
}

func runDev(t *testing.T, k Kind) Result {
	t.Helper()
	r := Run(context.Background(), devTarget(k), devGuard())
	b, _ := json.Marshal(r)
	t.Logf("%s", b)
	return r
}

func wantStatus(t *testing.T, r Result, id string, want ...string) {
	t.Helper()
	for _, c := range r.Checks {
		if c.ID != id {
			continue
		}
		for _, w := range want {
			if c.Status == w {
				return
			}
		}
		t.Errorf("check %s: status %s, want one of %v (problem %q)", id, c.Status, want, c.Problem)
		return
	}
	t.Errorf("check %s missing", id)
}

func TestIntegrationKafka(t *testing.T) {
	r := runDev(t, KindKafka)
	if !r.OK {
		t.Fatalf("kafka probe not ok")
	}
	wantStatus(t, r, "config", StatusOK)
	wantStatus(t, r, "reach", StatusOK)
	wantStatus(t, r, "login", StatusSkip) // PLAINTEXT: no SASL
	wantStatus(t, r, "topics", StatusOK)
	wantStatus(t, r, "offsets", StatusOK)
	// order-sync is active while the dev consumer runs; not found is a warning.
	wantStatus(t, r, "groups", StatusOK, StatusWarn)
	// The dev broker has no authorizer.
	wantStatus(t, r, "acls", StatusSkip)
}

func TestIntegrationConnect(t *testing.T) {
	r := runDev(t, KindConnect)
	if !r.OK {
		t.Fatalf("connect probe not ok")
	}
	for _, id := range []string{"config", "reach", "login", "connectors", "config_read"} {
		wantStatus(t, r, id, StatusOK)
	}
}

func TestIntegrationSchemaRegistry(t *testing.T) {
	r := runDev(t, KindSchemaRegistry)
	if !r.OK {
		t.Fatalf("schema registry probe not ok")
	}
	for _, id := range []string{"config", "reach", "login"} {
		wantStatus(t, r, id, StatusOK)
	}
	// The value subject exists once the connector has written a change.
	wantStatus(t, r, "subjects", StatusOK, StatusWarn)
	wantStatus(t, r, "compatibility", StatusOK, StatusWarn)
}

func TestIntegrationMissingObjects(t *testing.T) {
	tg := devTarget(KindKafka)
	tg.Pipelines[0].SourceTables = []string{"public.it_no_such_table"}
	tg.Pipelines[0].ConsumerGroups = []string{"it_no_such_group"}
	tg.Pipelines[0].Connectors = []string{"it-no-such-connector"}
	r := Run(context.Background(), tg, devGuard())
	wantStatus(t, r, "topics", StatusFail)
	wantStatus(t, r, "offsets", StatusSkip)
	wantStatus(t, r, "groups", StatusWarn)

	tg.Kind = KindConnect
	r = Run(context.Background(), tg, devGuard())
	wantStatus(t, r, "connectors", StatusFail)
	wantStatus(t, r, "config_read", StatusSkip)

	tg.Kind = KindSchemaRegistry
	r = Run(context.Background(), tg, devGuard())
	wantStatus(t, r, "subjects", StatusWarn)
}
