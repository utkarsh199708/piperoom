// Package config reads settings from environment variables.
//
// CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
package config

import "os"

// Config holds every setting. Defaults match dev/docker-compose.yml.
type Config struct {
	HTTPAddr          string // PIPEROOM_HTTP_ADDR (loopback by default: no real login yet, security F1)
	RoomDBURL         string // PIPEROOM_DB_URL (room service state + event log)
	SourceDBURL       string // SOURCE_DB_URL (the CDC source Postgres)
	WarehouseDBURL    string // WAREHOUSE_DB_URL (dev warehouse; Postgres)
	KafkaBrokers      string // KAFKA_BROKERS (comma separated)
	ConnectURL        string // CONNECT_URL
	SchemaRegistryURL string // SCHEMA_REGISTRY_URL
	OwnersFile        string // OWNERS_FILE
	UsersFile         string // USERS_FILE
	Model             string // PIPEROOM_MODEL
	Effort            string // PIPEROOM_EFFORT
	// The Anthropic SDK reads ANTHROPIC_API_KEY itself; it is never stored here.
}

// Load reads the environment and applies dev defaults.
func Load() Config {
	return Config{
		HTTPAddr:          get("PIPEROOM_HTTP_ADDR", "127.0.0.1:8090"),
		RoomDBURL:         get("PIPEROOM_DB_URL", "postgres://piperoom:piperoom-dev@localhost:5432/piperoom?sslmode=disable"),
		SourceDBURL:       get("SOURCE_DB_URL", "postgres://piperoom:piperoom-dev@localhost:5432/orders?sslmode=disable"),
		WarehouseDBURL:    get("WAREHOUSE_DB_URL", "postgres://piperoom:piperoom-dev@localhost:5432/warehouse?sslmode=disable"),
		KafkaBrokers:      get("KAFKA_BROKERS", "localhost:29092"),
		ConnectURL:        get("CONNECT_URL", "http://localhost:8083"),
		SchemaRegistryURL: get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		OwnersFile:        get("OWNERS_FILE", "dev/owners.yaml"),
		UsersFile:         get("USERS_FILE", "dev/users.yaml"),
		Model:             get("PIPEROOM_MODEL", "claude-opus-5-5"),
		Effort:            get("PIPEROOM_EFFORT", "high"),
	}
}

func get(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
