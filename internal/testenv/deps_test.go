//go:build integration

package testenv

import (
	"reflect"
	"testing"

	"piperoom/internal/config"
)

func TestInferDeps(t *testing.T) {
	tests := []struct {
		file string
		ok   bool
		want []Dep
	}{
		{"/src/piperoom/internal/clients/postgres/integration_test.go", true, []Dep{Postgres}},
		{"C:/src/piperoom/internal/clients/warehouse/integration_test.go", true, []Dep{Warehouse}},
		{"/r/internal/clients/kafka/kafka_integration_test.go", true, []Dep{Kafka}},
		{"/r/internal/clients/schemareg/x_test.go", true, []Dep{Registry}},
		{"/r/internal/clients/connect/x_test.go", true, []Dep{Connect}},
		{"/r/internal/tools/dbtools/x_test.go", true, []Dep{Postgres, Warehouse}},
		{"/r/internal/topology/integration_test.go", true, allDeps},
		{"/r/internal/clients/postgresx/x_test.go", true, allDeps},
		{"", false, allDeps},
	}
	for _, tc := range tests {
		if got := inferDeps(tc.file, tc.ok); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("inferDeps(%q) = %v, want %v", tc.file, got, tc.want)
		}
	}
}

func TestDepAddr(t *testing.T) {
	cfg := config.Config{
		SourceDBURL:       "postgres://u:secret@db.local/orders?sslmode=disable",
		WarehouseDBURL:    "postgres://u:p@wh:6543/warehouse",
		KafkaBrokers:      " k1:9092 ,k2:9092",
		SchemaRegistryURL: "https://sr.example",
		ConnectURL:        "http://localhost:8083",
	}
	tests := []struct {
		d    Dep
		want string
	}{
		{Postgres, "db.local:5432"},
		{Warehouse, "wh:6543"},
		{Kafka, "k1:9092"},
		{Registry, "sr.example:443"},
		{Connect, "localhost:8083"},
	}
	for _, tc := range tests {
		got, err := depAddr(cfg, tc.d)
		if err != nil || got != tc.want {
			t.Errorf("depAddr(%s) = %q, %v; want %q", tc.d, got, err, tc.want)
		}
	}
	cfg.SchemaRegistryURL = ""
	if got, err := depAddr(cfg, Registry); got != "" || err != nil {
		t.Errorf("registry off: %q %v", got, err)
	}
	cfg.WarehouseKind = "snowflake"
	if got, _ := depAddr(cfg, Warehouse); got != "" {
		t.Errorf("snowflake warehouse probed at %q", got)
	}
	cfg.SourceDBURL = "::bad"
	if _, err := depAddr(cfg, Postgres); err == nil {
		t.Error("bad url accepted")
	}
}

// TestGateIgnoresConnect: with Connect pointed at a closed port a
// Postgres-only test must still run; before the fix every test skipped.
func TestGateIgnoresConnect(t *testing.T) {
	t.Setenv("CONNECT_URL", "http://127.0.0.1:1")
	var pgSkipped, connectSkipped bool
	t.Run("postgres", func(t *testing.T) {
		defer func() { pgSkipped = t.Skipped() }()
		RequirePostgres(t)
	})
	t.Run("connect", func(t *testing.T) {
		defer func() { connectSkipped = t.Skipped() }()
		RequireConnect(t)
	})
	if !connectSkipped {
		t.Error("RequireConnect did not skip with Connect down")
	}
	if pgSkipped {
		t.Skip("source Postgres itself is not reachable; cannot check the gate")
	}
}
