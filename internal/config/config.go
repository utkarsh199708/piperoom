// Package config reads settings from environment variables.
//
// CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

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

	// Kafka connection security (internal/clients/netsec). Empty = PLAINTEXT, no login (dev stack).
	// Passwords are secrets: never log them; wiring wraps them in netsec.Secret.
	KafkaSecurityProtocol string // KAFKA_SECURITY_PROTOCOL: PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
	KafkaSASLMechanism    string // KAFKA_SASL_MECHANISM: PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
	KafkaSASLUsername     string // KAFKA_SASL_USERNAME
	KafkaSASLPassword     string // KAFKA_SASL_PASSWORD (secret)
	KafkaTLSCAFile        string // KAFKA_TLS_CA_FILE (PEM; empty = system roots)
	KafkaTLSCertFile      string // KAFKA_TLS_CERT_FILE (PEM client cert, mutual TLS)
	KafkaTLSKeyFile       string // KAFKA_TLS_KEY_FILE
	KafkaTLSServerName    string // KAFKA_TLS_SERVER_NAME (empty = broker host)

	// Schema Registry login. TLS is on when SCHEMA_REGISTRY_URL is https.
	SchemaRegistryUsername    string // SCHEMA_REGISTRY_USERNAME (Confluent Cloud: API key)
	SchemaRegistryPassword    string // SCHEMA_REGISTRY_PASSWORD (Confluent Cloud: API secret; secret)
	SchemaRegistryTLSCAFile   string // SCHEMA_REGISTRY_TLS_CA_FILE
	SchemaRegistryTLSCertFile string // SCHEMA_REGISTRY_TLS_CERT_FILE
	SchemaRegistryTLSKeyFile  string // SCHEMA_REGISTRY_TLS_KEY_FILE

	// Kafka Connect login. TLS is on when CONNECT_URL is https.
	ConnectUsername    string // CONNECT_USERNAME
	ConnectPassword    string // CONNECT_PASSWORD (secret)
	ConnectTLSCAFile   string // CONNECT_TLS_CA_FILE
	ConnectTLSCertFile string // CONNECT_TLS_CERT_FILE
	ConnectTLSKeyFile  string // CONNECT_TLS_KEY_FILE

	// Slack mirror of rooms. Off unless both token and channel are set.
	SlackBotToken  string // SLACK_BOT_TOKEN (secret)
	SlackChannelID string // SLACK_CHANNEL_ID
	PublicURL      string // PIPEROOM_PUBLIC_URL: base URL for room links in Slack; empty = no links

	// Warehouse kind. "postgres" (default) uses WAREHOUSE_DB_URL as a Postgres URL;
	// "snowflake" uses it as a gosnowflake DSN.
	WarehouseKind string // WAREHOUSE_KIND: postgres | snowflake

	// Signed alert sources (internal/alertin). A source with an empty secret is
	// disabled: its route answers 401. All secrets; never log them.
	AlertmanagerToken    string // ALERTMANAGER_TOKEN
	GrafanaAlertSecret   string // GRAFANA_ALERT_SECRET
	PagerDutyAlertSecret string // PAGERDUTY_ALERT_SECRET
	PagerDutyServices    string // PAGERDUTY_SERVICES: "SVCID=pipeline,SVCID2=pipeline2"
	DatadogAlertToken    string // DATADOG_ALERT_TOKEN
	GenericAlertSecret   string // GENERIC_ALERT_SECRET (HMAC for /api/alerts/generic)
	DefaultAlertPipeline string // DEFAULT_ALERT_PIPELINE: used when an alert names no pipeline

	// Agent cost guard: at most this many agent runs at once across all rooms;
	// further rooms wait in a queue. 0 or empty = default 3.
	MaxAgents string // PIPEROOM_MAX_AGENTS

	// Built-in watcher (internal/watch). Kept as strings like the other fields;
	// ParseWatch turns them into typed values and names the env var on error.
	WatchMode         string // PIPEROOM_WATCH_MODE: off | log | rooms (default log = shadow mode)
	WatchInterval     string // PIPEROOM_WATCH_INTERVAL: Go duration, default 60s, minimum 15s
	WatchCheckTimeout string // PIPEROOM_WATCH_CHECK_TIMEOUT: per-check deadline, default 10s (max 10s = the pilot role's statement_timeout)
	WatchCooldown     string // PIPEROOM_WATCH_COOLDOWN: quiet time after a watcher room is closed, default 60m

	// WebDir, when set, makes the server serve the built web UI (web/dist) at /.
	WebDir string // PIPEROOM_WEB_DIR

	// Model endpoint. "anthropic" (default) calls the Anthropic API with
	// ANTHROPIC_API_KEY; "bedrock" uses AWS credentials from the default chain in
	// AWSRegion (PIPEROOM_MODEL must then be a Bedrock model or inference-profile
	// id, e.g. an India "in." profile); "vertex" uses Google ADC.
	ModelProvider string // PIPEROOM_MODEL_PROVIDER: anthropic | bedrock | vertex
	AWSRegion     string // AWS_REGION (bedrock)
	VertexProject string // VERTEX_PROJECT_ID (vertex)
	VertexRegion  string // VERTEX_REGION (vertex)
	// TriageModel, when set, runs the first investigation turns on a cheaper
	// model; empty = always Model.
	TriageModel string // PIPEROOM_TRIAGE_MODEL
	// RoomTokenBudget caps model tokens (input + output) per room; when reached
	// the agent stops and says so. Empty = default 2000000; "0" = no cap.
	RoomTokenBudget string // PIPEROOM_ROOM_TOKEN_BUDGET

	// RetentionDays deletes closed rooms' events older than this many days.
	// Empty or "0" = keep forever.
	RetentionDays string // PIPEROOM_RETENTION_DAYS
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

		KafkaSecurityProtocol: get("KAFKA_SECURITY_PROTOCOL", ""),
		KafkaSASLMechanism:    get("KAFKA_SASL_MECHANISM", ""),
		KafkaSASLUsername:     get("KAFKA_SASL_USERNAME", ""),
		KafkaSASLPassword:     get("KAFKA_SASL_PASSWORD", ""),
		KafkaTLSCAFile:        get("KAFKA_TLS_CA_FILE", ""),
		KafkaTLSCertFile:      get("KAFKA_TLS_CERT_FILE", ""),
		KafkaTLSKeyFile:       get("KAFKA_TLS_KEY_FILE", ""),
		KafkaTLSServerName:    get("KAFKA_TLS_SERVER_NAME", ""),

		SchemaRegistryUsername:    get("SCHEMA_REGISTRY_USERNAME", ""),
		SchemaRegistryPassword:    get("SCHEMA_REGISTRY_PASSWORD", ""),
		SchemaRegistryTLSCAFile:   get("SCHEMA_REGISTRY_TLS_CA_FILE", ""),
		SchemaRegistryTLSCertFile: get("SCHEMA_REGISTRY_TLS_CERT_FILE", ""),
		SchemaRegistryTLSKeyFile:  get("SCHEMA_REGISTRY_TLS_KEY_FILE", ""),

		ConnectUsername:    get("CONNECT_USERNAME", ""),
		ConnectPassword:    get("CONNECT_PASSWORD", ""),
		ConnectTLSCAFile:   get("CONNECT_TLS_CA_FILE", ""),
		ConnectTLSCertFile: get("CONNECT_TLS_CERT_FILE", ""),
		ConnectTLSKeyFile:  get("CONNECT_TLS_KEY_FILE", ""),

		SlackBotToken:  get("SLACK_BOT_TOKEN", ""),
		SlackChannelID: get("SLACK_CHANNEL_ID", ""),
		PublicURL:      get("PIPEROOM_PUBLIC_URL", ""),

		WarehouseKind: get("WAREHOUSE_KIND", "postgres"),

		AlertmanagerToken:    get("ALERTMANAGER_TOKEN", ""),
		GrafanaAlertSecret:   get("GRAFANA_ALERT_SECRET", ""),
		PagerDutyAlertSecret: get("PAGERDUTY_ALERT_SECRET", ""),
		PagerDutyServices:    get("PAGERDUTY_SERVICES", ""),
		DatadogAlertToken:    get("DATADOG_ALERT_TOKEN", ""),
		GenericAlertSecret:   get("GENERIC_ALERT_SECRET", ""),
		DefaultAlertPipeline: get("DEFAULT_ALERT_PIPELINE", ""),

		MaxAgents: get("PIPEROOM_MAX_AGENTS", ""),

		WatchMode:         get("PIPEROOM_WATCH_MODE", DefaultWatchMode),
		WatchInterval:     get("PIPEROOM_WATCH_INTERVAL", DefaultWatchInterval),
		WatchCheckTimeout: get("PIPEROOM_WATCH_CHECK_TIMEOUT", DefaultWatchCheckTimeout),
		WatchCooldown:     get("PIPEROOM_WATCH_COOLDOWN", DefaultWatchCooldown),

		WebDir: get("PIPEROOM_WEB_DIR", ""),

		ModelProvider:   get("PIPEROOM_MODEL_PROVIDER", "anthropic"),
		AWSRegion:       get("AWS_REGION", ""),
		VertexProject:   get("VERTEX_PROJECT_ID", ""),
		VertexRegion:    get("VERTEX_REGION", ""),
		TriageModel:     get("PIPEROOM_TRIAGE_MODEL", ""),
		RoomTokenBudget: get("PIPEROOM_ROOM_TOKEN_BUDGET", ""),

		RetentionDays: get("PIPEROOM_RETENTION_DAYS", ""),
	}
}

// Watcher defaults and limits.
const (
	DefaultWatchMode         = "log"
	DefaultWatchInterval     = "60s"
	DefaultWatchCheckTimeout = "10s"
	DefaultWatchCooldown     = "60m"

	// MinWatchInterval keeps the watcher from hammering the source database.
	MinWatchInterval = 15 * time.Second
	// MaxWatchCheckTimeout must not exceed the pilot role's statement_timeout
	// (docs/pilot/permissions.md), or a hung check would outlive the server's cap.
	MaxWatchCheckTimeout = 10 * time.Second
)

// WatchMode says what the watcher does when a signal fires.
type WatchMode string

// Watcher modes.
const (
	// WatchOff does not start the watcher.
	WatchOff WatchMode = "off"
	// WatchLog checks and logs transitions but opens no rooms (shadow mode).
	WatchLog WatchMode = "log"
	// WatchRooms checks, logs, and opens or joins rooms.
	WatchRooms WatchMode = "rooms"
)

// Watch holds the typed watcher settings.
type Watch struct {
	Mode         WatchMode
	Interval     time.Duration
	CheckTimeout time.Duration
	Cooldown     time.Duration
}

// ParseWatch validates the PIPEROOM_WATCH_* fields. The error names the env var
// so the operator knows what to fix. Empty fields mean the default.
func ParseWatch(c Config) (Watch, error) {
	var w Watch

	mode := strings.ToLower(strings.TrimSpace(c.WatchMode))
	if mode == "" {
		mode = DefaultWatchMode
	}
	switch WatchMode(mode) {
	case WatchOff, WatchLog, WatchRooms:
		w.Mode = WatchMode(mode)
	default:
		return Watch{}, fmt.Errorf("config: PIPEROOM_WATCH_MODE=%q: want off, log or rooms", c.WatchMode)
	}

	var err error
	if w.Interval, err = parseWatchDuration("PIPEROOM_WATCH_INTERVAL", c.WatchInterval, DefaultWatchInterval); err != nil {
		return Watch{}, err
	}
	if w.Interval < MinWatchInterval {
		return Watch{}, fmt.Errorf("config: PIPEROOM_WATCH_INTERVAL=%q: must be at least %s", c.WatchInterval, MinWatchInterval)
	}
	if w.CheckTimeout, err = parseWatchDuration("PIPEROOM_WATCH_CHECK_TIMEOUT", c.WatchCheckTimeout, DefaultWatchCheckTimeout); err != nil {
		return Watch{}, err
	}
	if w.CheckTimeout > MaxWatchCheckTimeout {
		return Watch{}, fmt.Errorf("config: PIPEROOM_WATCH_CHECK_TIMEOUT=%q: must be at most %s (the source role's statement_timeout)", c.WatchCheckTimeout, MaxWatchCheckTimeout)
	}
	if w.Cooldown, err = parseWatchDuration("PIPEROOM_WATCH_COOLDOWN", c.WatchCooldown, DefaultWatchCooldown); err != nil {
		return Watch{}, err
	}
	return w, nil
}

// parseWatchDuration parses a positive Go duration; empty uses def.
func parseWatchDuration(name, v, def string) (time.Duration, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		v = def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q: not a duration like 30s or 5m: %w", name, v, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("config: %s=%q: must be greater than zero", name, v)
	}
	return d, nil
}

func get(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
