package room

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"piperoom/internal/owners"
)

// The mysql source layer owners.yaml accepts (owners.KnownLayers) is accepted
// by the room too: a users.yaml that lists it passes ValidateUsers, a nudge
// may name it, and its owner sees it in LayersFor and may export the room's
// pipeline. Before, the room checked events.Layers, which has no mysql, so
// the server refused to start on a valid MySQL owners file.
func TestMySQLLayerKnownToRoom(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "owners.yaml")
	if err := os.WriteFile(path, []byte(`
pipelines:
  - id: orders-mysql
    source_kind: mysql
    topic_prefix: shop
    source_tables: [shop.orders]
    connectors: [orders-mysql]
layers:
  mysql: dana
  kafka-connect: arjun
  schema-registry: arjun
  kafka: arjun
  consumer: arjun
  warehouse: arjun
`), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	users, err := ParseUsers([]byte(`
users:
  - id: dana
    name: Dana
    layers: [mysql]
  - id: arjun
    name: Arjun
    layers: [kafka-connect, schema-registry, kafka, consumer, warehouse]
`))
	if err != nil {
		t.Fatalf("ParseUsers: %v", err)
	}
	if err := ValidateUsers(users, own); err != nil {
		t.Fatalf("ValidateUsers refused the mysql layer: %v", err)
	}
	if err := ValidateUsers(users, nil); err != nil {
		t.Fatalf("ValidateUsers (users only) refused the mysql layer: %v", err)
	}
	if !isKnownLayer("mysql") || isKnownLayer("db2") {
		t.Fatal("isKnownLayer must agree with owners.KnownLayers")
	}

	s := &Service{owners: own, users: users}
	if got := s.knownLayers(); !slices.Contains(got, "mysql") || slices.Contains(got, "postgres") {
		t.Fatalf("knownLayers = %v, want the MySQL pipeline's layers", got)
	}
	dana, _ := users.Get("dana")
	if got := s.LayersFor(dana); !slices.Equal(got, []string{"mysql"}) {
		t.Fatalf("LayersFor(dana) = %v, want [mysql]", got)
	}
	if g, ok := s.canGradeShadow("orders-mysql", dana); !ok {
		t.Fatalf("dana owns the mysql layer of orders-mysql: canGradeShadow = %q, %v", g, ok)
	}
	if got := s.pipelineLayers("orders-mysql"); len(got) == 0 || got[0] != "mysql" {
		t.Fatalf("pipelineLayers = %v, want mysql first", got)
	}
}
