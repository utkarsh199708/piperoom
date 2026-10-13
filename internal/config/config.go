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
	HTTPAddr          string // PIPEROOM_HTTP_ADDR (loopback by default; dev mode off loopback needs PIPEROOM_DEV_ALLOW_NONLOOPBACK)
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
	PublicURL      string // PIPEROOM_PUBLIC_URL: base URL for Slack room links, the WS origin, and (auth) the OIDC redirect URI and Host allow-list

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

	// RetentionDays deletes whole closed rooms (events, proposals, approvals,
	// hand-offs) whose closed_at is older than this many days. Open rooms are
	// never deleted. Empty or "0" = keep forever.
	RetentionDays string // PIPEROOM_RETENTION_DAYS

	// Login (docs/design/auth.md §6). Raw strings; the auth package parses and
	// checks them at start-up. Not wired until auth PR 2.
	AuthMode            string // PIPEROOM_AUTH_MODE: dev (default) | oidc
	DevAllowNonLoopback string // PIPEROOM_DEV_ALLOW_NONLOOPBACK: "1" lets dev mode listen off loopback (replaces --allow-remote)
	AllowedHosts        string // PIPEROOM_ALLOWED_HOSTS: comma list of extra Host values for a proxy that rewrites Host
	TrustedProxies      string // PIPEROOM_TRUSTED_PROXIES: comma list of CIDRs; X-Forwarded-For read only from these (oidc mode only)
	OIDCIssuer          string // PIPEROOM_OIDC_ISSUER
	OIDCClientID        string // PIPEROOM_OIDC_CLIENT_ID
	OIDCClientSecret    string // PIPEROOM_OIDC_CLIENT_SECRET (secret, never logged)
	OIDCPublicClient    string // PIPEROOM_OIDC_PUBLIC_CLIENT: "1" allows an empty secret (PKCE only)
	OIDCScopes          string // PIPEROOM_OIDC_SCOPES (space separated)
	OIDCGroupsClaim     string // PIPEROOM_OIDC_GROUPS_CLAIM: "" = no group rights
	AuthViewerGroups    string // PIPEROOM_AUTH_VIEWER_GROUPS: comma list; viewer role
	SessionIdle         string // PIPEROOM_SESSION_IDLE: Go duration
	SessionMax          string // PIPEROOM_SESSION_MAX: Go duration
	SoloUser            string // PIPEROOM_SOLO_USER: "" = off

	// Shadow mode (docs/design/shadow.md). Which pipelines are shadow is set
	// per pipeline in owners.yaml (shadow: block); these are the numbers.
	// Kept as strings like the watcher fields; ParseShadow types them and
	// names the env var on error.
	ShadowGrace       string // PIPEROOM_SHADOW_GRACE: Go duration the run may keep going after close; default 0 = cancel at close
	ShadowTokenBudget string // PIPEROOM_SHADOW_TOKEN_BUDGET: tokens per shadow room; default 500000; "0" = use PIPEROOM_ROOM_TOKEN_BUDGET (there is no uncapped shadow run)
	ShadowMaxTurns    string // PIPEROOM_SHADOW_MAX_TURNS: model turns per shadow run; default 20

	// Tenancy (docs/design/tenancy.md). Tenants, their members and each
	// pipeline's connections live in owners.yaml; this only states which
	// model the operator expects, so a lost tenants: block stops the start
	// instead of silently showing every room to everyone.
	TenancyMode string // PIPEROOM_TENANCY_MODE: auto (default) | single | teams
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

		AuthMode:            get("PIPEROOM_AUTH_MODE", DefaultAuthMode),
		DevAllowNonLoopback: get("PIPEROOM_DEV_ALLOW_NONLOOPBACK", ""),
		AllowedHosts:        get("PIPEROOM_ALLOWED_HOSTS", ""),
		TrustedProxies:      get("PIPEROOM_TRUSTED_PROXIES", ""),
		OIDCIssuer:          get("PIPEROOM_OIDC_ISSUER", ""),
		OIDCClientID:        get("PIPEROOM_OIDC_CLIENT_ID", ""),
		OIDCClientSecret:    get("PIPEROOM_OIDC_CLIENT_SECRET", ""),
		OIDCPublicClient:    get("PIPEROOM_OIDC_PUBLIC_CLIENT", ""),
		OIDCScopes:          get("PIPEROOM_OIDC_SCOPES", DefaultOIDCScopes),
		OIDCGroupsClaim:     getSet("PIPEROOM_OIDC_GROUPS_CLAIM", DefaultOIDCGroupsClaim),
		AuthViewerGroups:    get("PIPEROOM_AUTH_VIEWER_GROUPS", ""),
		SessionIdle:         get("PIPEROOM_SESSION_IDLE", DefaultSessionIdle),
		SessionMax:          get("PIPEROOM_SESSION_MAX", DefaultSessionMax),
		SoloUser:            get("PIPEROOM_SOLO_USER", ""),

		ShadowGrace:       get("PIPEROOM_SHADOW_GRACE", DefaultShadowGrace),
		ShadowTokenBudget: get("PIPEROOM_SHADOW_TOKEN_BUDGET", DefaultShadowTokenBudget),
		ShadowMaxTurns:    get("PIPEROOM_SHADOW_MAX_TURNS", DefaultShadowMaxTurns),

		TenancyMode: get("PIPEROOM_TENANCY_MODE", TenancyAuto),
	}
}

// Tenancy modes (docs/design/tenancy.md section 4).
const (
	// TenancyAuto follows owners.yaml: a tenants: block turns the tenant
	// filter on (teams); no block keeps one tenant (single).
	TenancyAuto = "auto"
	// TenancySingle requires owners.yaml to have no tenants: block. Every
	// signed-in user sees every room, as before tenancy.
	TenancySingle = "single"
	// TenancyTeams requires a tenants: block. A user sees only the rooms of
	// pipelines in their tenants.
	TenancyTeams = "teams"
)

// ParseTenancyMode validates PIPEROOM_TENANCY_MODE. Empty means auto. The
// caller compares the result with owners.yaml; this function does no I/O.
func ParseTenancyMode(c Config) (string, error) {
	m := strings.ToLower(strings.TrimSpace(c.TenancyMode))
	switch m {
	case "":
		return TenancyAuto, nil
	case TenancyAuto, TenancySingle, TenancyTeams:
		return m, nil
	}
	return "", fmt.Errorf("config: PIPEROOM_TENANCY_MODE=%q: want auto, single or teams", c.TenancyMode)
}

// Login defaults (docs/design/auth.md §6).
const (
	DefaultAuthMode        = "dev"
	DefaultOIDCScopes      = "openid email profile"
	DefaultOIDCGroupsClaim = "groups"
	DefaultSessionIdle     = "8h"
	DefaultSessionMax      = "12h"
)

// Shadow defaults.
const (
	DefaultShadowGrace       = "0"
	DefaultShadowTokenBudget = "500000"
	DefaultShadowMaxTurns    = "20"
)

// Shadow holds the typed shadow settings.
type Shadow struct {
	// Grace is how long a shadow run may keep going after the room closes.
	// 0 = cancel at close, as a live run is.
	Grace time.Duration
	// TokenBudget caps model tokens per shadow room; 0 = use the room
	// default (PIPEROOM_ROOM_TOKEN_BUDGET), as agent.Options.TokenBudget 0 does.
	TokenBudget int64
	// MaxTurns caps model turns per shadow run.
	MaxTurns int
}

// ParseShadow validates the PIPEROOM_SHADOW_* fields. Empty fields mean the
// default. Grace accepts "0" and any non-negative Go duration; TokenBudget
// is a whole number >= 1, or "0" for the room default; MaxTurns is a whole number >= 1.
func ParseShadow(c Config) (Shadow, error) {
	var sh Shadow
	g := strings.TrimSpace(c.ShadowGrace)
	if g == "" {
		g = DefaultShadowGrace
	}
	if g != "0" {
		d, err := time.ParseDuration(g)
		if err != nil {
			return Shadow{}, fmt.Errorf("config: PIPEROOM_SHADOW_GRACE=%q: not a duration like 0, 30s or 10m: %w", c.ShadowGrace, err)
		}
		if d < 0 {
			return Shadow{}, fmt.Errorf("config: PIPEROOM_SHADOW_GRACE=%q: must not be negative", c.ShadowGrace)
		}
		sh.Grace = d
	}
	var err error
	if sh.TokenBudget, err = parseWholeNumber("PIPEROOM_SHADOW_TOKEN_BUDGET", c.ShadowTokenBudget, DefaultShadowTokenBudget, true); err != nil {
		return Shadow{}, err
	}
	n, err := parseWholeNumber("PIPEROOM_SHADOW_MAX_TURNS", c.ShadowMaxTurns, DefaultShadowMaxTurns, false)
	if err != nil {
		return Shadow{}, err
	}
	sh.MaxTurns = int(n)
	return sh, nil
}

// parseWholeNumber parses a non-negative whole number; empty uses def. With
// zeroOK, "0" is allowed (no cap); otherwise the value must be >= 1.
func parseWholeNumber(name, v, def string, zeroOK bool) (int64, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		v = def
	}
	var n int64
	for _, r := range v {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("config: %s=%q: want a whole number", name, v)
		}
		if n > (1<<62)/10 {
			return 0, fmt.Errorf("config: %s=%q: too large", name, v)
		}
		n = n*10 + int64(r-'0')
	}
	if n == 0 && !zeroOK {
		return 0, fmt.Errorf("config: %s=%q: must be at least 1", name, v)
	}
	return n, nil
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

// getSet is get, except that a variable set to "" keeps "" (for settings
// where empty means off, like PIPEROOM_OIDC_GROUPS_CLAIM). Compose files must
// not pass such a variable as ${VAR:-} (that sets ""); use ${VAR-default} or
// leave it out.
func getSet(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func get(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
