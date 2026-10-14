//go:build integration

package integrations

import (
	"context"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/integrations/spec"
)

// These run the real checks against the dev stack (dev/docker-compose.yml).
// They read only; nothing is created, so no cleanup is needed.

func devDeps(t *testing.T) Deps {
	t.Helper()
	g, err := NewGuard("", nil)
	if err != nil {
		t.Fatal(err)
	}
	return Deps{Guard: g}.withDefaults()
}

func TestIntegrationPostgresSource(t *testing.T) {
	cfg := config.Load()
	if !strings.Contains(cfg.SourceDBURL, "localhost") {
		t.Skip("SOURCE_DB_URL is not the dev stack")
	}
	v := vals(map[string]string{"host": "localhost", "port": "5432", "database": "orders", "user": "piperoom",
		"sslmode": "disable", "tables": "public.orders"}, map[string]string{"password": "piperoom-dev"})
	checks := devDeps(t).postgresKinds()[0].Test(context.Background(), v)
	for _, id := range []string{"reach", "login", "version", "primary", "slots_visible"} {
		wantStatus(t, checks, id, spec.CheckPass)
	}
	// The dev user owns the tables, so the read-only check must say so.
	wantStatus(t, checks, "read_only:public.orders", spec.CheckFail)

	client, closeFn, err := devDeps(t).postgresKinds()[0].Build(context.Background(), v)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	if _, err := client.(clients.SourceDB).ReplicationSlots(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationKafka(t *testing.T) {
	v := vals(map[string]string{"brokers": "localhost:29092", "protocol": "PLAINTEXT"}, nil)
	checks := devDeps(t).kafkaKinds()[0].Test(context.Background(), v)
	for _, id := range []string{"reach", "metadata", "groups"} {
		wantStatus(t, checks, id, spec.CheckPass)
	}
	wantStatus(t, checks, "read_only", spec.CheckSkipped)
}

func TestIntegrationConnectAndRegistry(t *testing.T) {
	d := devDeps(t)
	checks := d.connectKind().Test(context.Background(), vals(map[string]string{"url": "http://localhost:8083"}, nil))
	wantStatus(t, checks, "api", spec.CheckPass)
	checks = d.registryKind().Test(context.Background(), vals(map[string]string{"url": "http://localhost:8081"}, nil))
	wantStatus(t, checks, "api", spec.CheckPass)
}
