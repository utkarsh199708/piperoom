//go:build integration

package setuptools

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients/connect"
	"piperoom/internal/testenv"
)

// TestSinkConfigPassesConnectValidate is the board's "a sink config passes
// Q4a's validate on the dev stack": the builder's Debezium JDBC sink config
// (a sink to the dev Postgres, upserts and deletes keyed on the record key,
// one Debezium SMT) is sent to the dev Connect's
// PUT /connector-plugins/{class}/config/validate through the Q4a
// ConfigValidator client, with the ${secret:} reference materialised the way
// the runner does. Validate creates nothing, so there is nothing to clean up.
func TestSinkConfigPassesConnectValidate(t *testing.T) {
	cfg := testenv.Require(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	b := builder(t)
	host := strings.TrimSpace(os.Getenv("PIPEROOM_TEST_CONNECT_DB_HOST"))
	if host == "" {
		// Inside the Connect container the dev Postgres is the compose
		// service, not localhost.
		host = "postgres"
	}
	sink, err := b.SinkConfig(SinkOptions{
		Sink:   "debezium-jdbc-sink",
		Topics: []string{testenv.Name("sink")},
		Config: map[string]string{
			"connection.url":      "jdbc:postgresql://" + host + ":5432/orders",
			"connection.username": "piperoom",
			"connection.password": "${secret:sink_password}",
			"insert.mode":         "upsert",
			"primary.key.mode":    "record_key",
			"delete.enabled":      "true",
			"schema.evolution":    "basic",
			"tasks.max":           "1",
		},
		Transforms: []Transform{{Alias: "route", Class: "jdbc-collection-name-transformation"}},
	})
	if err != nil {
		t.Fatalf("SinkConfig: %v", err)
	}
	// The runner materialises ${secret:} before Connect sees the config; the
	// dev stack's password stands in for the stored secret.
	sink["connection.password"] = "piperoom-dev"
	sink["name"] = testenv.Name("sinkcfg")

	c := connect.New(cfg.ConnectURL, &http.Client{Timeout: 30 * time.Second})
	vr, err := c.ValidateConfig(ctx, sink["connector.class"], sink)
	if err != nil {
		t.Fatalf("Connect validate: %v", err)
	}
	if vr.ErrorCount != 0 {
		t.Fatalf("Connect validate: %d errors: %v", vr.ErrorCount, vr.Errors)
	}
}
