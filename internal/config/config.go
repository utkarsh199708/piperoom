// Package config reads settings from environment variables.
//
// CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
package config

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Config holds every setting. Defaults match dev/docker-compose.yml.
type Config struct {
	HTTPAddr          string // PIPEROOM_HTTP_ADDR (loopback by default; dev mode off loopback needs PIPEROOM_DEV_ALLOW_NONLOOPBACK)
	RoomDBURL         string // PIPEROOM_DB_URL (room service state + event log)
	SourceDBURL       string // SOURCE_DB_URL (the CDC source Postgres)
	SourceDBWriteURL  string // SOURCE_DB_WRITE_URL (P0 item 7): the source Postgres as a separate WRITE role for the pg write actions, never the read-only SOURCE_DB_URL role; empty = no writer (those actions refuse at Execute: "no write credentials"; #606); secret
	WarehouseDBURL    string // WAREHOUSE_DB_URL (DSN for the selected warehouse kind)
	KafkaBrokers      string // KAFKA_BROKERS (comma separated)
	ConnectURL        string // CONNECT_URL
	SchemaRegistryURL string // SCHEMA_REGISTRY_URL; set to "" or "off" (any case) = no Schema Registry, and Load stores ""
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
	// SlackPageNoDriver is PIPEROOM_SLACK_PAGE_NO_DRIVER split on commas:
	// the on-call Slack member IDs the bridge pages on control.lease_expired
	// (slackbridge.PagingOptions.NoDriver, C12 #130; cmd/server builds the
	// options, A5 follow-up). Empty means no paging on lease expiry. Load
	// only splits and trims; cmd/server calls ParseSlackMemberIDs at start
	// and refuses to run on a value that is not a Slack member ID.
	SlackPageNoDriver []string
	PublicURL         string // PIPEROOM_PUBLIC_URL: base URL for Slack room links, the WS origin, and (auth) the OIDC redirect URI and Host allow-list

	// Warehouse kind. "postgres" (default) uses WAREHOUSE_DB_URL as a Postgres URL;
	// "snowflake" uses it as a gosnowflake DSN; "bigquery" and "databricks"
	// (P0 item 8, readers in P4e) use it as that driver's DSN; "clickhouse"
	// uses it as a clickhouse-go/v2 DSN. internal/wiring validates the value
	// and refuses a kind it has no reader for yet.
	WarehouseKind string // WAREHOUSE_KIND: postgres | snowflake | bigquery | databricks | clickhouse

	// Batch pipelines (P0 item 8): the orchestrator and transform readers.
	// An empty URL or directory turns that reader off, and its tools answer
	// clients.ErrNotSupported. ParseAirflow and ParseDBT type them.
	AirflowURL      string // AIRFLOW_URL: base URL of the Airflow REST API v1 (http or https, no user info)
	AirflowUsername string // AIRFLOW_USERNAME: basic auth, with AIRFLOW_PASSWORD
	AirflowPassword string // AIRFLOW_PASSWORD (secret)
	AirflowToken    string // AIRFLOW_TOKEN: bearer token instead of basic auth (secret)
	DBTArtifactsDir string // DBT_ARTIFACTS_DIR: directory holding run_results.json and manifest.json; or dbt Cloud below, not both
	DBTCloudURL     string // DBT_CLOUD_URL: dbt Cloud base URL, e.g. https://cloud.getdbt.com
	DBTCloudToken   string // DBT_CLOUD_TOKEN: read-only service token (secret)
	DBTCloudAccount string // DBT_CLOUD_ACCOUNT_ID: the {a} of /api/v2/accounts/{a}/runs/
	DBTCloudJobID   string // DBT_CLOUD_JOB_ID: the job whose latest run's artifacts are read

	// AutoApproveDelay is how long a proposal whose every step is on its
	// pipeline's owners.yaml auto_approve: list waits, cancellable, before the
	// policy approves and runs it (P2). ParseAutoApproveDelay.
	AutoApproveDelay string // PIPEROOM_AUTO_APPROVE_DELAY: Go duration before a policy-approved proposal runs; default "60s"; "0" turns the policy off even when owners.yaml lists actions

	// Signed alert sources (internal/alertin). A source with an empty secret is
	// disabled: its route answers 401. All secrets; never log them.
	AlertmanagerToken    string // ALERTMANAGER_TOKEN
	GrafanaAlertSecret   string // GRAFANA_ALERT_SECRET
	PagerDutyAlertSecret string // PAGERDUTY_ALERT_SECRET
	PagerDutyServices    string // PAGERDUTY_SERVICES: "SVCID=pipeline,SVCID2=pipeline2"
	DatadogAlertToken    string // DATADOG_ALERT_TOKEN
	GenericAlertSecret   string // GENERIC_ALERT_SECRET (HMAC for /api/alerts/generic)
	DefaultAlertPipeline string // DEFAULT_ALERT_PIPELINE: used when an alert names no pipeline

	// Agent cost guard: at most this many agent runs working at once on THIS
	// replica, across all its rooms; further rooms wait in a queue (round-robin
	// across pipelines, FIFO within one). 0 or empty = default 3. With several
	// replicas the total is replicas x this, unless MaxAgentsGlobal is set.
	MaxAgents string // PIPEROOM_MAX_AGENTS
	// MaxAgentsGlobal caps agent runs working at once across ALL replicas,
	// counted from the live room leases in Postgres (docs/design/replicas.md
	// section 4). 0 or empty = off (only the per-replica cap applies). Ignored
	// with the in-memory store (--demo), which is one process.
	MaxAgentsGlobal string // PIPEROOM_MAX_AGENTS_GLOBAL

	// ReplicaID names this server process in room leases, executor claims and
	// leader logs (docs/design/replicas.md section 2). Empty = the host name
	// (in Kubernetes, the pod name). The server always appends a random
	// per-process suffix, so a restarted container never mistakes the old
	// process's leases or claims for its own.
	ReplicaID string // PIPEROOM_REPLICA_ID

	// ResumeOnTakeover: when a replica takes over the agent lease of a
	// replica that stopped, "1" resumes an agent that was working (a fresh
	// run, at most 3 resumes per room per hour); empty or "0" (default) pauses
	// it as a restart does, until someone steers or mentions @agent
	// (docs/design/replicas.md section 4.3).
	ResumeOnTakeover string // PIPEROOM_RESUME_ON_TAKEOVER

	// Built-in watcher (internal/watch). Kept as strings like the other fields;
	// ParseWatch turns them into typed values and names the env var on error.
	WatchMode         string // PIPEROOM_WATCH_MODE: off | log | rooms (default log = shadow mode)
	WatchInterval     string // PIPEROOM_WATCH_INTERVAL: Go duration, default 60s, minimum 15s
	WatchCheckTimeout string // PIPEROOM_WATCH_CHECK_TIMEOUT: per-check deadline, default 10s (max 10s = the pilot role's statement_timeout)
	WatchCooldown     string // PIPEROOM_WATCH_COOLDOWN: quiet time after a watcher room is closed, default 60m
	WatchMaxOpenRooms string // PIPEROOM_WATCH_MAX_OPEN_ROOMS: rooms the watcher keeps open at once; default 6, min 1 (F6; ParseWatchMaxOpenRooms)

	// Triage pass (F6): one bounded first look on a cheap model before the
	// full investigation, outside the agent pool. Not PIPEROOM_TRIAGE_MODEL,
	// which runs the first turns of the investigation itself. ParseTriagePass
	// types them and names the env var on error.
	TriagePass        string // PIPEROOM_TRIAGE_PASS: on | off, default on
	TriagePassModel   string // PIPEROOM_TRIAGE_PASS_MODEL: default "claude-haiku-5-5"
	TriagePassTimeout string // PIPEROOM_TRIAGE_PASS_TIMEOUT: Go duration, default 40s, max 90s

	// PostmortemModel says whether a closed room without a fresh hand-off
	// summary gets one model call for the post-mortem prose (F2):
	// auto (default) | off (events-only draft). ParsePostmortemModel.
	PostmortemModel string // PIPEROOM_POSTMORTEM_MODEL

	// ChangesRetentionDays deletes pipeline_changes rows older than this many
	// days (F8); "0" = keep. Rows per pipeline are capped at 5000 regardless.
	ChangesRetentionDays string // PIPEROOM_CHANGES_RETENTION_DAYS: default 30

	// Debezium metrics (F12): the Prometheus text endpoint of the JMX
	// exporter next to Kafka Connect. ParseDebeziumMetrics.
	DebeziumMetricsURL    string // DEBEZIUM_METRICS_URL: http(s)://host:port/metrics; empty = debezium_metrics says "not configured"
	DebeziumMetricsPrefix string // DEBEZIUM_METRICS_PREFIX: metric name prefix, default "debezium_metrics_"

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
	// the agent stops and says so. Empty = default 2000000; "0" = no cap. The
	// server keeps the count in the room database (docs/design/cost.md), so it
	// survives a restart.
	RoomTokenBudget string // PIPEROOM_ROOM_TOKEN_BUDGET

	// Model spend (docs/design/cost.md). Kept as strings like the shadow
	// fields; ParseCost types them and names the env var on error.
	ModelPrices              string // PIPEROOM_MODEL_PRICES: path of the JSON price table (model id -> USD per million tokens: input, output, cache_write, cache_read); empty = no table, so cost is omitted, never guessed
	PipelineDailyTokenBudget string // PIPEROOM_PIPELINE_DAILY_TOKEN_BUDGET: model tokens per pipeline per UTC day, kept in the room database; empty or "0" = off
	PipelineDailyCostUSD     string // PIPEROOM_PIPELINE_DAILY_COST_USD: estimated USD per pipeline per UTC day, kept in the room database; empty or "0" = off; needs PIPEROOM_MODEL_PRICES

	// RetentionDays deletes whole closed rooms (events, proposals, approvals,
	// hand-offs) whose closed_at is older than this many days. Open rooms are
	// never deleted. Empty or "0" = keep forever. A pipeline's owners.yaml
	// retention_days overrides it for that pipeline; when this value is set too,
	// the shorter of the two wins (docs/design/retention.md section 3).
	RetentionDays string // PIPEROOM_RETENTION_DAYS

	// Room limits (docs/design/retention.md). Kept as strings like the
	// watcher fields; ParseLimits types them and names the env var on error.
	MaxOpenDays       string // PIPEROOM_MAX_OPEN_DAYS: whole days a room may stay open before the server closes it; default 14; "0" = off
	RoomMaxEvents     string // PIPEROOM_ROOM_MAX_EVENTS: events in one room log; default 20000
	RoomMaxBytes      string // PIPEROOM_ROOM_MAX_BYTES: stored payload bytes of one room log; default 256MiB
	EventMaxDataBytes string // PIPEROOM_EVENT_MAX_DATA_BYTES: tool_result.data (and action.executed.output) bytes of one event; default 64KiB
	ArchiveDir        string // PIPEROOM_ARCHIVE_DIR: absolute directory; when set, retention writes the room's export there before a purge, and an admin delete does when asked (?archive=true); empty = off

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

	// Logging of the long-running commands (cmd/server first). Raw strings;
	// ParseLog types them and names the env var on error.
	LogLevel  string // PIPEROOM_LOG_LEVEL: debug | info (default) | warn | error
	LogFormat string // PIPEROOM_LOG_FORMAT: text (default) | json

	// DevAck is the operator's written acknowledgement that dev login (no
	// password: anyone who reaches the port picks any user) is served on a
	// non-loopback address. See ParseDevAck. Never baked into an image.
	DevAck string // PIPEROOM_DEV_ACK: "" (default) | DevAckValue

	// FakeAgent swaps the real agent for the scripted one of internal/room/demo
	// on a server wired to a real stack, so a full-stack test can run the demo
	// story without a model or an API key. Dev only: see ParseFakeAgent.
	FakeAgent string // PIPEROOM_FAKE_AGENT: "" (default, the real agent) | FakeAgentScripted

	// WriteMode is PIPEROOM_WRITE_MODE: "off" (default) makes the executor
	// refuse to queue or run any step, so an approved proposal is recorded but
	// never executed and `action.executed` is never emitted; "on" wires the
	// write clients. --demo forces "on" (fake clients only).
	WriteMode string // PIPEROOM_WRITE_MODE: off | on

	// Setup rooms (docs/design/setup-agent.md §3.8, §4 item 6; U wave).
	// Server side. Raw strings; ParseSetup types them and refuses
	// SetupEnabled "on" while write mode is off.
	SetupEnabled      string // PIPEROOM_SETUP_ENABLED: off (default) | on; "on" requires PIPEROOM_WRITE_MODE=on
	SetupProdPatterns string // PIPEROOM_SETUP_PROD_PATTERNS: comma list of host globs, kubectl context names and cloud account ids that class a target production
	SetupTokenBudget  string // PIPEROOM_SETUP_TOKEN_BUDGET: model tokens per setup room; default 2000000
	SetupLocalDelay   string // PIPEROOM_SETUP_LOCAL_DELAY: countdown before a T1 setup step runs (policy setup_local); default 20s; "0" = every T1 step waits for a click
	SetupPluginHosts  string // PIPEROOM_SETUP_PLUGIN_HOSTS: comma list of hosts plugin_install may download from; default repo1.maven.org
	SecretsDir        string // PIPEROOM_SECRETS_DIR: absolute directory where registered read credentials land (<dir>/<pipeline>/<name>, 0600); required when setup is on; secret files, never logged
	OwnersDir         string // OWNERS_DIR: directory of per-pipeline owners files (<id>.yaml) a registration writes and the server hot-reloads; "" = registration writes nothing on this server (standalone writes next to the project)

	// Runner side (cmd/setup). The runner reads these; the server ignores
	// them. Only names of files and endpoints: the secret store's contents
	// are never in the environment of anything the runner starts.
	SetupSecretsFile string // PIPEROOM_SETUP_SECRETS_FILE: the runner's secret store; default <project dir>/.piperoom/secrets.env (0600)
	SetupServerURL   string // PIPEROOM_SETUP_SERVER_URL: the server origin, same as --server; https unless loopback
	SetupCode        string // PIPEROOM_SETUP_CODE: the pairing code, same as --code; single use, never logged
}

// LogValue keeps a Config out of the log: it holds database URLs with
// passwords, the alert HMAC secrets, the OIDC client secret and the runner's
// pairing code (PIPEROOM_SETUP_CODE), so slog prints a fixed marker instead
// of the fields. A command that wants to log settings logs the typed values
// ParseX returns, which hold no secret. (U0)
func (c Config) LogValue() slog.Value {
	return slog.StringValue("config.Config{redacted: holds secrets}")
}

// Load reads the environment and applies dev defaults.
func Load() Config {
	c := Config{
		HTTPAddr:          get("PIPEROOM_HTTP_ADDR", "127.0.0.1:8090"),
		RoomDBURL:         get("PIPEROOM_DB_URL", "postgres://piperoom:piperoom-dev@localhost:5432/piperoom?sslmode=disable"),
		SourceDBURL:       get("SOURCE_DB_URL", "postgres://piperoom:piperoom-dev@localhost:5432/orders?sslmode=disable"),
		SourceDBWriteURL:  get("SOURCE_DB_WRITE_URL", ""),
		WarehouseDBURL:    get("WAREHOUSE_DB_URL", "postgres://piperoom:piperoom-dev@localhost:5432/warehouse?sslmode=disable"),
		KafkaBrokers:      get("KAFKA_BROKERS", "localhost:29092"),
		ConnectURL:        get("CONNECT_URL", "http://localhost:8083"),
		SchemaRegistryURL: registryURL(getSet("SCHEMA_REGISTRY_URL", "http://localhost:8081")),
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

		AirflowURL:      get("AIRFLOW_URL", ""),
		AirflowUsername: get("AIRFLOW_USERNAME", ""),
		AirflowPassword: get("AIRFLOW_PASSWORD", ""),
		AirflowToken:    get("AIRFLOW_TOKEN", ""),
		DBTArtifactsDir: get("DBT_ARTIFACTS_DIR", ""),
		DBTCloudURL:     get("DBT_CLOUD_URL", ""),
		DBTCloudToken:   get("DBT_CLOUD_TOKEN", ""),
		DBTCloudAccount: get("DBT_CLOUD_ACCOUNT_ID", ""),
		DBTCloudJobID:   get("DBT_CLOUD_JOB_ID", ""),

		AutoApproveDelay: get("PIPEROOM_AUTO_APPROVE_DELAY", DefaultAutoApproveDelay),

		AlertmanagerToken:    get("ALERTMANAGER_TOKEN", ""),
		GrafanaAlertSecret:   get("GRAFANA_ALERT_SECRET", ""),
		PagerDutyAlertSecret: get("PAGERDUTY_ALERT_SECRET", ""),
		PagerDutyServices:    get("PAGERDUTY_SERVICES", ""),
		DatadogAlertToken:    get("DATADOG_ALERT_TOKEN", ""),
		GenericAlertSecret:   get("GENERIC_ALERT_SECRET", ""),
		DefaultAlertPipeline: get("DEFAULT_ALERT_PIPELINE", ""),

		MaxAgents:        get("PIPEROOM_MAX_AGENTS", ""),
		MaxAgentsGlobal:  get("PIPEROOM_MAX_AGENTS_GLOBAL", ""),
		ReplicaID:        get("PIPEROOM_REPLICA_ID", ""),
		ResumeOnTakeover: get("PIPEROOM_RESUME_ON_TAKEOVER", ""),

		WatchMode:         get("PIPEROOM_WATCH_MODE", DefaultWatchMode),
		WatchInterval:     get("PIPEROOM_WATCH_INTERVAL", DefaultWatchInterval),
		WatchCheckTimeout: get("PIPEROOM_WATCH_CHECK_TIMEOUT", DefaultWatchCheckTimeout),
		WatchCooldown:     get("PIPEROOM_WATCH_COOLDOWN", DefaultWatchCooldown),
		WatchMaxOpenRooms: get("PIPEROOM_WATCH_MAX_OPEN_ROOMS", DefaultWatchMaxOpenRooms),

		TriagePass:        get("PIPEROOM_TRIAGE_PASS", DefaultTriagePass),
		TriagePassModel:   get("PIPEROOM_TRIAGE_PASS_MODEL", DefaultTriagePassModel),
		TriagePassTimeout: get("PIPEROOM_TRIAGE_PASS_TIMEOUT", DefaultTriagePassTimeout),

		PostmortemModel: get("PIPEROOM_POSTMORTEM_MODEL", PostmortemModelAuto),

		ChangesRetentionDays: get("PIPEROOM_CHANGES_RETENTION_DAYS", DefaultChangesRetentionDays),

		DebeziumMetricsURL:    get("DEBEZIUM_METRICS_URL", ""),
		DebeziumMetricsPrefix: get("DEBEZIUM_METRICS_PREFIX", DefaultDebeziumMetricsPrefix),

		WebDir: get("PIPEROOM_WEB_DIR", ""),

		ModelProvider:   get("PIPEROOM_MODEL_PROVIDER", "anthropic"),
		AWSRegion:       get("AWS_REGION", ""),
		VertexProject:   get("VERTEX_PROJECT_ID", ""),
		VertexRegion:    get("VERTEX_REGION", ""),
		TriageModel:     get("PIPEROOM_TRIAGE_MODEL", ""),
		RoomTokenBudget: get("PIPEROOM_ROOM_TOKEN_BUDGET", ""),

		ModelPrices:              get("PIPEROOM_MODEL_PRICES", ""),
		PipelineDailyTokenBudget: get("PIPEROOM_PIPELINE_DAILY_TOKEN_BUDGET", ""),
		PipelineDailyCostUSD:     get("PIPEROOM_PIPELINE_DAILY_COST_USD", ""),

		RetentionDays: get("PIPEROOM_RETENTION_DAYS", ""),

		MaxOpenDays:       get("PIPEROOM_MAX_OPEN_DAYS", DefaultMaxOpenDays),
		RoomMaxEvents:     get("PIPEROOM_ROOM_MAX_EVENTS", DefaultRoomMaxEvents),
		RoomMaxBytes:      get("PIPEROOM_ROOM_MAX_BYTES", DefaultRoomMaxBytes),
		EventMaxDataBytes: get("PIPEROOM_EVENT_MAX_DATA_BYTES", DefaultEventMaxDataBytes),
		ArchiveDir:        get("PIPEROOM_ARCHIVE_DIR", ""),

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

		LogLevel:  get("PIPEROOM_LOG_LEVEL", DefaultLogLevel),
		LogFormat: get("PIPEROOM_LOG_FORMAT", DefaultLogFormat),

		DevAck:    get("PIPEROOM_DEV_ACK", ""),
		FakeAgent: get("PIPEROOM_FAKE_AGENT", ""),

		WriteMode: get("PIPEROOM_WRITE_MODE", "off"),

		SetupEnabled:      get("PIPEROOM_SETUP_ENABLED", "off"),
		SetupProdPatterns: get("PIPEROOM_SETUP_PROD_PATTERNS", ""),
		SetupTokenBudget:  get("PIPEROOM_SETUP_TOKEN_BUDGET", DefaultSetupTokenBudget),
		SetupLocalDelay:   get("PIPEROOM_SETUP_LOCAL_DELAY", DefaultSetupLocalDelay),
		SetupPluginHosts:  get("PIPEROOM_SETUP_PLUGIN_HOSTS", DefaultSetupPluginHosts),
		SecretsDir:        get("PIPEROOM_SECRETS_DIR", ""),
		OwnersDir:         get("OWNERS_DIR", ""),

		SetupSecretsFile: get("PIPEROOM_SETUP_SECRETS_FILE", ""),
		SetupServerURL:   get("PIPEROOM_SETUP_SERVER_URL", ""),
		SetupCode:        get("PIPEROOM_SETUP_CODE", ""),
	}
	c.SlackPageNoDriver = splitList(get("PIPEROOM_SLACK_PAGE_NO_DRIVER", ""))
	return c
}

// Log defaults.
const (
	DefaultLogLevel  = "info"
	DefaultLogFormat = LogFormatText
)

// Log formats (PIPEROOM_LOG_FORMAT).
const (
	// LogFormatText is slog.NewTextHandler: key=value lines for a terminal.
	LogFormatText = "text"
	// LogFormatJSON is slog.NewJSONHandler: one JSON object per line for a
	// log collector (Cloud Logging, Loki, CloudWatch).
	LogFormatJSON = "json"
)

// Log holds the typed log settings. The command builds its handler from it
// (slog.HandlerOptions{Level: Level}); this package creates no logger.
type Log struct {
	Level  slog.Level
	Format string // LogFormatText | LogFormatJSON
}

// ParseLog validates PIPEROOM_LOG_LEVEL and PIPEROOM_LOG_FORMAT. Both are
// trimmed and case-insensitive; empty means the default. Level is one of
// debug, info, warn, error (no numeric offsets such as "info+2": a level the
// operator cannot read back from the log line is a typo, not a setting).
// The error names the env var. A debug line follows the same rule as every
// other: no secrets and no record values (AGENTS.md section 7).
func ParseLog(c Config) (Log, error) {
	var l Log
	switch lv := strings.ToLower(strings.TrimSpace(c.LogLevel)); lv {
	case "", DefaultLogLevel:
		l.Level = slog.LevelInfo
	case "debug":
		l.Level = slog.LevelDebug
	case "warn":
		l.Level = slog.LevelWarn
	case "error":
		l.Level = slog.LevelError
	default:
		return Log{}, fmt.Errorf("config: PIPEROOM_LOG_LEVEL=%q: want debug, info, warn or error", c.LogLevel)
	}
	switch f := strings.ToLower(strings.TrimSpace(c.LogFormat)); f {
	case "":
		l.Format = DefaultLogFormat
	case LogFormatText, LogFormatJSON:
		l.Format = f
	default:
		return Log{}, fmt.Errorf("config: PIPEROOM_LOG_FORMAT=%q: want text or json", c.LogFormat)
	}
	return l, nil
}

// DevAckValue is the only accepted value of PIPEROOM_DEV_ACK. It is a sentence,
// not "1", so that it cannot be set by accident or by a copied default, and so
// that whoever sets it has read what it means.
const DevAckValue = "dev-login-has-no-password"

// ParseDevAck validates PIPEROOM_DEV_ACK and reports whether it is given.
// Empty is false; DevAckValue (exact, after trimming spaces) is true; any
// other value is an error, so a "1" or "true" stops the start instead of
// silently counting as no acknowledgement.
//
// The rule it serves (enforced by cmd/server, R9): in dev auth mode on a
// non-loopback PIPEROOM_HTTP_ADDR the server needs BOTH
// PIPEROOM_DEV_ALLOW_NONLOOPBACK=1 and PIPEROOM_DEV_ACK=DevAckValue. The
// Docker image sets only the first, so an image started as-is off loopback
// refuses to start until the operator adds the acknowledgement in their own
// deployment (deploy/.env, Helm values). Loopback, --demo and oidc mode do not
// need it; in oidc mode it is ignored with one warning, like the switch.
func ParseDevAck(c Config) (bool, error) {
	switch strings.TrimSpace(c.DevAck) {
	case "":
		return false, nil
	case DevAckValue:
		return true, nil
	}
	return false, fmt.Errorf("config: PIPEROOM_DEV_ACK: want exactly %q (or unset)", DevAckValue)
}

// ParseWriteMode validates PIPEROOM_WRITE_MODE and reports whether write mode
// is on. "off" (the default, also when empty) is false and "on" is true; any
// other value is an error, so a "true", "1" or typo stops the start instead of
// silently counting as either.
func ParseWriteMode(c Config) (bool, error) {
	switch strings.TrimSpace(c.WriteMode) {
	case "", "off":
		return false, nil
	case "on":
		return true, nil
	}
	return false, fmt.Errorf("config: PIPEROOM_WRITE_MODE=%q: want off or on", c.WriteMode)
}

// FakeAgentScripted is the only accepted value of PIPEROOM_FAKE_AGENT.
const FakeAgentScripted = "scripted"

// ParseFakeAgent validates PIPEROOM_FAKE_AGENT and reports whether the server
// runs the scripted agent instead of the real one. Empty is false. "scripted"
// is true only in dev auth mode with PIPEROOM_DEV_ACK=DevAckValue and with
// write mode off: a scripted agent shows people an investigation that did not
// happen, so it must never reach a deployment with real users, and its
// proposals must never run against real systems. Anything else is an error
// naming the variable, so a typo stops the start instead of running the real
// agent unnoticed.
func ParseFakeAgent(c Config) (bool, error) {
	switch v := strings.TrimSpace(c.FakeAgent); v {
	case "":
		return false, nil
	case FakeAgentScripted:
	default:
		return false, fmt.Errorf("config: PIPEROOM_FAKE_AGENT=%q: want %q (or unset)", c.FakeAgent, FakeAgentScripted)
	}
	if mode := strings.TrimSpace(c.AuthMode); mode != "" && mode != DefaultAuthMode {
		return false, fmt.Errorf("config: PIPEROOM_FAKE_AGENT=%s needs PIPEROOM_AUTH_MODE=%s, not %q", FakeAgentScripted, DefaultAuthMode, mode)
	}
	ack, err := ParseDevAck(c)
	if err != nil {
		return false, err
	}
	if !ack {
		return false, fmt.Errorf("config: PIPEROOM_FAKE_AGENT=%s needs PIPEROOM_DEV_ACK=%s", FakeAgentScripted, DevAckValue)
	}
	write, err := ParseWriteMode(c)
	if err != nil {
		return false, err
	}
	if write {
		return false, fmt.Errorf("config: PIPEROOM_FAKE_AGENT=%s needs PIPEROOM_WRITE_MODE=off", FakeAgentScripted)
	}
	return true, nil
}

// slackMemberID is the shape of a Slack member ID ("U0…" for users, "W0…"
// for Enterprise Grid users), which is what the paging allow-list holds.
var slackMemberID = regexp.MustCompile(`^[UW][A-Z0-9]{2,}$`)

// ParseSlackMemberIDs validates a list of Slack member IDs from the env var
// name (Config.SlackPageNoDriver from PIPEROOM_SLACK_PAGE_NO_DRIVER): each
// must match ^[UW][A-Z0-9]{2,}$. It returns a copy with duplicates removed,
// in order, so a repeated ID pages once. An empty list is fine (no paging).
func ParseSlackMemberIDs(name string, ids []string) ([]string, error) {
	out := make([]string, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if !slackMemberID.MatchString(id) {
			return nil, fmt.Errorf("config: %s: %q is not a Slack member ID (want U… or W…, letters and digits)", name, id)
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out, nil
}

// splitList splits a comma-separated env value, trims each item and drops
// empty ones, so "a, b,,c" and "" read as expected.
func splitList(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
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

// Cost holds the typed spend settings (docs/design/cost.md).
type Cost struct {
	// PricesFile is the PIPEROOM_MODEL_PRICES path; "" = no price table, so
	// every cost is omitted and no cost budget can be set.
	PricesFile string
	// PipelineDailyTokens caps model tokens per pipeline per UTC day; 0 = off.
	PipelineDailyTokens int64
	// PipelineDailyCostUSD caps the estimated cost per pipeline per UTC day,
	// in USD; 0 = off.
	PipelineDailyCostUSD float64
}

// MaxPipelineDailyCostUSD bounds PIPEROOM_PIPELINE_DAILY_COST_USD, so the cap
// in millionths of a USD (the unit the spend tables keep) fits an int64.
const MaxPipelineDailyCostUSD = 1_000_000_000

// ParseCost validates the spend fields. Empty means off (no table, no cap).
// PIPEROOM_PIPELINE_DAILY_TOKEN_BUDGET is a whole number, "0" = off.
// PIPEROOM_PIPELINE_DAILY_COST_USD is a plain decimal such as 25 or 12.50
// with at most six decimals, "0" = off; a value above 0 needs
// PIPEROOM_MODEL_PRICES. The price file itself is read by the agent package
// at start-up.
func ParseCost(c Config) (Cost, error) {
	out := Cost{PricesFile: strings.TrimSpace(c.ModelPrices)}
	var err error
	if out.PipelineDailyTokens, err = parseWholeNumber("PIPEROOM_PIPELINE_DAILY_TOKEN_BUDGET", c.PipelineDailyTokenBudget, "0", true); err != nil {
		return Cost{}, err
	}
	if out.PipelineDailyCostUSD, err = parseUSD("PIPEROOM_PIPELINE_DAILY_COST_USD", c.PipelineDailyCostUSD); err != nil {
		return Cost{}, err
	}
	if out.PipelineDailyCostUSD > 0 && out.PricesFile == "" {
		return Cost{}, fmt.Errorf("config: PIPEROOM_PIPELINE_DAILY_COST_USD=%q needs PIPEROOM_MODEL_PRICES: without a price table there is no cost to count", c.PipelineDailyCostUSD)
	}
	return out, nil
}

// maxUSDDecimals is the finest amount PIPEROOM_PIPELINE_DAILY_COST_USD takes:
// one millionth of a USD, the unit the spend tables keep.
const maxUSDDecimals = 6

// parseUSD parses a plain non-negative decimal amount (digits, at most one
// point with digits on both sides, at most six decimals); empty is 0.
// Exponents, signs, NaN, Inf and amounts finer than the stored millionth are
// refused so a typo cannot turn into an unexpected cap.
func parseUSD(name, v string) (float64, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, nil
	}
	digits, points, decimals := 0, 0, 0
	for i, r := range v {
		switch {
		case r >= '0' && r <= '9':
			digits++
			if points > 0 {
				decimals++
			}
		case r == '.' && points == 0 && i > 0 && i < len(v)-1:
			points++
		default:
			return 0, fmt.Errorf("config: %s=%q: want a plain amount in USD such as 25 or 12.50", name, v)
		}
	}
	if decimals > maxUSDDecimals {
		return 0, fmt.Errorf("config: %s=%q: at most %d decimal places (millionths of a USD)", name, v, maxUSDDecimals)
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || digits == 0 {
		return 0, fmt.Errorf("config: %s=%q: want a plain amount in USD such as 25 or 12.50", name, v)
	}
	if f > MaxPipelineDailyCostUSD {
		return 0, fmt.Errorf("config: %s=%q: too large (at most %d)", name, v, MaxPipelineDailyCostUSD)
	}
	return f, nil
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

// Room limit defaults and bounds (docs/design/retention.md).
const (
	DefaultMaxOpenDays       = "14"
	DefaultRoomMaxEvents     = "20000"
	DefaultRoomMaxBytes      = "256MiB"
	DefaultEventMaxDataBytes = "64KiB"

	// MaxMaxOpenDays bounds PIPEROOM_MAX_OPEN_DAYS (ten years).
	MaxMaxOpenDays = 3650
	// MinRoomMaxEvents keeps a room able to hold its opening events and a
	// short investigation.
	MinRoomMaxEvents = 100
	// MaxRoomMaxEvents keeps the cap plus its 2% reserve under the audit
	// export's 500000-event limit, so every room can still be exported and
	// archived (PIPEROOM_ARCHIVE_DIR).
	MaxRoomMaxEvents = 490000
	MinRoomMaxBytes  = 1 << 20 // 1 MiB
	MaxRoomMaxBytes  = 16 << 30
	MinEventMaxData  = 1 << 10 // 1 KiB
	MaxEventMaxData  = 16 << 20
)

// Limits holds the typed room limits.
type Limits struct {
	// MaxOpenDays is how long a room may stay open before the server closes
	// it; 0 = no limit.
	MaxOpenDays int
	// RoomMaxEvents and RoomMaxBytes cap one room's event log (count, and
	// the sum of stored payload sizes). Human, system and close events may go
	// 2% over them; agent events may not.
	RoomMaxEvents int64
	RoomMaxBytes  int64
	// EventMaxDataBytes caps tool_result.data and action.executed.output of
	// one event; larger values are stored truncated.
	EventMaxDataBytes int64
	// ArchiveDir is where retention writes a room's export before it purges
	// it, and where an admin delete writes one when asked (?archive=true);
	// "" = no archive. Always an absolute, cleaned path.
	ArchiveDir string
}

// ParseLimits validates the room limit fields (PIPEROOM_MAX_OPEN_DAYS,
// PIPEROOM_ROOM_MAX_EVENTS, PIPEROOM_ROOM_MAX_BYTES,
// PIPEROOM_EVENT_MAX_DATA_BYTES, PIPEROOM_ARCHIVE_DIR). Empty fields mean the
// default. Byte sizes are a whole number of bytes, optionally followed by KiB,
// MiB or GiB ("64KiB", "268435456"). The error names the env var. Whether
// ArchiveDir exists and is writable is checked by the server at start, not here.
func ParseLimits(c Config) (Limits, error) {
	var l Limits
	days, err := parseWholeNumber("PIPEROOM_MAX_OPEN_DAYS", c.MaxOpenDays, DefaultMaxOpenDays, true)
	if err != nil {
		return Limits{}, err
	}
	if days > MaxMaxOpenDays {
		return Limits{}, fmt.Errorf("config: PIPEROOM_MAX_OPEN_DAYS=%q: at most %d", c.MaxOpenDays, MaxMaxOpenDays)
	}
	l.MaxOpenDays = int(days)

	if l.RoomMaxEvents, err = parseWholeNumber("PIPEROOM_ROOM_MAX_EVENTS", c.RoomMaxEvents, DefaultRoomMaxEvents, false); err != nil {
		return Limits{}, err
	}
	if l.RoomMaxEvents < MinRoomMaxEvents || l.RoomMaxEvents > MaxRoomMaxEvents {
		return Limits{}, fmt.Errorf("config: PIPEROOM_ROOM_MAX_EVENTS=%q: want %d to %d", c.RoomMaxEvents, MinRoomMaxEvents, MaxRoomMaxEvents)
	}
	if l.RoomMaxBytes, err = parseByteSize("PIPEROOM_ROOM_MAX_BYTES", c.RoomMaxBytes, DefaultRoomMaxBytes); err != nil {
		return Limits{}, err
	}
	if l.RoomMaxBytes < MinRoomMaxBytes || l.RoomMaxBytes > MaxRoomMaxBytes {
		return Limits{}, fmt.Errorf("config: PIPEROOM_ROOM_MAX_BYTES=%q: want 1MiB to 16GiB", c.RoomMaxBytes)
	}
	if l.EventMaxDataBytes, err = parseByteSize("PIPEROOM_EVENT_MAX_DATA_BYTES", c.EventMaxDataBytes, DefaultEventMaxDataBytes); err != nil {
		return Limits{}, err
	}
	if l.EventMaxDataBytes < MinEventMaxData || l.EventMaxDataBytes > MaxEventMaxData {
		return Limits{}, fmt.Errorf("config: PIPEROOM_EVENT_MAX_DATA_BYTES=%q: want 1KiB to 16MiB", c.EventMaxDataBytes)
	}
	if l.EventMaxDataBytes > l.RoomMaxBytes {
		return Limits{}, fmt.Errorf("config: PIPEROOM_EVENT_MAX_DATA_BYTES=%q: must not be larger than PIPEROOM_ROOM_MAX_BYTES=%q", c.EventMaxDataBytes, c.RoomMaxBytes)
	}

	if dir := strings.TrimSpace(c.ArchiveDir); dir != "" {
		if !filepath.IsAbs(dir) {
			return Limits{}, fmt.Errorf("config: PIPEROOM_ARCHIVE_DIR=%q: want an absolute path", c.ArchiveDir)
		}
		l.ArchiveDir = filepath.Clean(dir)
	}
	return l, nil
}

// parseByteSize parses a whole number of bytes with an optional KiB, MiB or
// GiB suffix; empty uses def. Zero is refused.
func parseByteSize(name, v, def string) (int64, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		v = def
	}
	mult := int64(1)
	for _, u := range []struct {
		suffix string
		mult   int64
	}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}} {
		if strings.HasSuffix(v, u.suffix) {
			v, mult = strings.TrimSuffix(v, u.suffix), u.mult
			break
		}
	}
	if v == "" || strings.TrimSpace(v) != v {
		return 0, fmt.Errorf("config: %s: want a size like 65536, 64KiB or 256MiB", name)
	}
	n, err := parseWholeNumber(name, v, "", false)
	if err != nil {
		return 0, fmt.Errorf("config: %s: want a size like 65536, 64KiB or 256MiB: %w", name, err)
	}
	if n > (1<<62)/mult {
		return 0, fmt.Errorf("config: %s=%q: too large", name, v)
	}
	return n * mult, nil
}

// Watcher defaults and limits.
const (
	// DefaultWatchMode stays "log" (shadow) for now. F6 moves it to "rooms";
	// that flip lands with F6d, which owns the tests that pin "log"
	// (cmd/server/watch_test.go) and the setup.md wording.
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

// ---- F wave (F2, F6, F8, F12) ----

// DefaultWatchMaxOpenRooms is the watcher's open-room cap (F6). It is not
// PIPEROOM_MAX_AGENTS: a room beyond the agent pool still opens and queues.
const (
	DefaultWatchMaxOpenRooms = "6"
	// MaxWatchMaxOpenRooms bounds a typo, not a real limit.
	MaxWatchMaxOpenRooms = 1000
)

// ParseWatchMaxOpenRooms validates PIPEROOM_WATCH_MAX_OPEN_ROOMS: a whole
// number from 1 to MaxWatchMaxOpenRooms; empty = 6. The error names the var.
func ParseWatchMaxOpenRooms(c Config) (int, error) {
	n, err := parseWholeNumber("PIPEROOM_WATCH_MAX_OPEN_ROOMS", c.WatchMaxOpenRooms, DefaultWatchMaxOpenRooms, false)
	if err != nil {
		return 0, err
	}
	if n > MaxWatchMaxOpenRooms {
		return 0, fmt.Errorf("config: PIPEROOM_WATCH_MAX_OPEN_ROOMS=%q: at most %d", c.WatchMaxOpenRooms, MaxWatchMaxOpenRooms)
	}
	return int(n), nil
}

// Triage pass defaults and limits (F6).
const (
	DefaultTriagePass        = "on"
	DefaultTriagePassModel   = "claude-haiku-5-5"
	DefaultTriagePassTimeout = "40s"
	// MaxTriagePassTimeout keeps the first look a first look: the room shows
	// a pending triage line until it ends.
	MaxTriagePassTimeout = 90 * time.Second
)

// TriagePass holds the typed triage-pass settings.
type TriagePass struct {
	On      bool
	Model   string
	Timeout time.Duration
}

// ParseTriagePass validates PIPEROOM_TRIAGE_PASS (on | off, any case),
// PIPEROOM_TRIAGE_PASS_MODEL (a model id without spaces) and
// PIPEROOM_TRIAGE_PASS_TIMEOUT (a Go duration, at most 90s). Empty fields
// mean the default. The error names the env var.
func ParseTriagePass(c Config) (TriagePass, error) {
	var t TriagePass
	switch v := strings.ToLower(strings.TrimSpace(c.TriagePass)); v {
	case "", "on":
		t.On = true
	case "off":
	default:
		return TriagePass{}, fmt.Errorf("config: PIPEROOM_TRIAGE_PASS=%q: want on or off", c.TriagePass)
	}
	t.Model = strings.TrimSpace(c.TriagePassModel)
	if t.Model == "" {
		t.Model = DefaultTriagePassModel
	}
	if strings.ContainsAny(t.Model, " \t\r\n") {
		return TriagePass{}, fmt.Errorf("config: PIPEROOM_TRIAGE_PASS_MODEL=%q: want a model id without spaces", c.TriagePassModel)
	}
	var err error
	if t.Timeout, err = parseWatchDuration("PIPEROOM_TRIAGE_PASS_TIMEOUT", c.TriagePassTimeout, DefaultTriagePassTimeout); err != nil {
		return TriagePass{}, err
	}
	if t.Timeout > MaxTriagePassTimeout {
		return TriagePass{}, fmt.Errorf("config: PIPEROOM_TRIAGE_PASS_TIMEOUT=%q: must be at most %s", c.TriagePassTimeout, MaxTriagePassTimeout)
	}
	return t, nil
}

// Post-mortem model modes (PIPEROOM_POSTMORTEM_MODEL, F2).
const (
	// PostmortemModelAuto makes one model call at close only when the room
	// has no hand-off summary newer than its last proposal, action and hypothesis.
	PostmortemModelAuto = "auto"
	// PostmortemModelOff never calls the model: the draft is events-only.
	PostmortemModelOff = "off"
)

// ParsePostmortemModel validates PIPEROOM_POSTMORTEM_MODEL (auto | off, any
// case; empty = auto). Any other value refuses start; the error names the var.
func ParsePostmortemModel(c Config) (string, error) {
	switch v := strings.ToLower(strings.TrimSpace(c.PostmortemModel)); v {
	case "", PostmortemModelAuto:
		return PostmortemModelAuto, nil
	case PostmortemModelOff:
		return PostmortemModelOff, nil
	default:
		return "", fmt.Errorf("config: PIPEROOM_POSTMORTEM_MODEL=%q: want auto or off", c.PostmortemModel)
	}
}

// Pipeline change log retention (F8).
const (
	DefaultChangesRetentionDays = "30"
	// MaxChangesRetentionDays bounds a typo (ten years).
	MaxChangesRetentionDays = 3650
	// MaxChangesPerPipeline is the row cap per pipeline the store keeps
	// whatever the retention.
	MaxChangesPerPipeline = 5000
)

// ParseChangesRetentionDays validates PIPEROOM_CHANGES_RETENTION_DAYS: a whole
// number of days, "0" = keep (the per-pipeline row cap still applies); empty
// = 30. The error names the env var.
func ParseChangesRetentionDays(c Config) (int, error) {
	n, err := parseWholeNumber("PIPEROOM_CHANGES_RETENTION_DAYS", c.ChangesRetentionDays, DefaultChangesRetentionDays, true)
	if err != nil {
		return 0, err
	}
	if n > MaxChangesRetentionDays {
		return 0, fmt.Errorf("config: PIPEROOM_CHANGES_RETENTION_DAYS=%q: at most %d", c.ChangesRetentionDays, MaxChangesRetentionDays)
	}
	return int(n), nil
}

// DefaultDebeziumMetricsPrefix is the JMX exporter's usual prefix for
// Debezium MBeans (F12).
const DefaultDebeziumMetricsPrefix = "debezium_metrics_"

// DebeziumMetrics holds the typed Debezium metrics settings. URL "" = not configured.
type DebeziumMetrics struct {
	URL    string
	Prefix string
}

// ParseDebeziumMetrics validates DEBEZIUM_METRICS_URL (empty, or an absolute
// http or https URL with a host and no user info: credentials do not belong
// in a URL that is logged) and DEBEZIUM_METRICS_PREFIX (a Prometheus metric
// name prefix: letters, digits, '_' and ':', not starting with a digit;
// empty = the default). The error names the env var.
func ParseDebeziumMetrics(c Config) (DebeziumMetrics, error) {
	d := DebeziumMetrics{URL: strings.TrimSpace(c.DebeziumMetricsURL), Prefix: strings.TrimSpace(c.DebeziumMetricsPrefix)}
	if d.URL != "" {
		u, err := url.Parse(d.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return DebeziumMetrics{}, fmt.Errorf("config: DEBEZIUM_METRICS_URL: want an http(s)://host:port/metrics URL")
		}
		if u.User != nil {
			return DebeziumMetrics{}, fmt.Errorf("config: DEBEZIUM_METRICS_URL: must not carry user info")
		}
	}
	if d.Prefix == "" {
		d.Prefix = DefaultDebeziumMetricsPrefix
	}
	for i, r := range d.Prefix {
		ok := r == '_' || r == ':' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (i > 0 && r >= '0' && r <= '9')
		if !ok {
			return DebeziumMetrics{}, fmt.Errorf("config: DEBEZIUM_METRICS_PREFIX=%q: want a metric name prefix like debezium_metrics_", c.DebeziumMetricsPrefix)
		}
	}
	return d, nil
}

// ---- P wave (P2, P3, P4; PIPEROOM_WRITE_MODE of P1 is next to ParseDevAck) ----

// DefaultAutoApproveDelay is the policy countdown (P0 item 2): long enough
// for someone in the room to read the card and cancel.
const DefaultAutoApproveDelay = "60s"

// MaxAutoApproveDelay bounds PIPEROOM_AUTO_APPROVE_DELAY: a countdown longer
// than an hour leaves a fix pending past any incident's attention span, so it
// is a typo, not a policy.
const MaxAutoApproveDelay = time.Hour

// ParseAutoApproveDelay validates PIPEROOM_AUTO_APPROVE_DELAY: "0" turns the
// auto-approve policy off (0 is returned), anything else must be a Go
// duration above zero and at most MaxAutoApproveDelay; empty = 60s. "0s" is
// refused with a pointer to "0", so off is always spelled one way. The error
// names the env var.
func ParseAutoApproveDelay(c Config) (time.Duration, error) {
	v := strings.TrimSpace(c.AutoApproveDelay)
	switch v {
	case "0":
		return 0, nil
	case "":
		v = DefaultAutoApproveDelay
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: PIPEROOM_AUTO_APPROVE_DELAY=%q: not a duration like 60s or 2m: %w", v, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("config: PIPEROOM_AUTO_APPROVE_DELAY=%q: must be greater than zero; use \"0\" to turn the policy off", v)
	}
	if d > MaxAutoApproveDelay {
		return 0, fmt.Errorf("config: PIPEROOM_AUTO_APPROVE_DELAY=%q: at most %s", v, MaxAutoApproveDelay)
	}
	return d, nil
}

// Airflow holds the typed Airflow settings (P0 item 8). URL "" = off.
// Password and Token are secrets: never log them.
type Airflow struct {
	URL      string
	Username string
	Password string
	Token    string
}

// ParseAirflow validates AIRFLOW_URL (empty, or an absolute http or https URL
// with a host and no user info) and the login: basic auth (AIRFLOW_USERNAME
// and AIRFLOW_PASSWORD, both) or AIRFLOW_TOKEN, not both. A URL with neither
// is allowed (an Airflow behind a proxy that authenticates). A login needs an
// https URL: over http:// the password or token would travel in clear text
// (the internal/clients/netsec rule). A login without a URL is an error, so a
// forgotten URL is not read as "off". The error names the env var and never
// quotes a secret.
func ParseAirflow(c Config) (Airflow, error) {
	a := Airflow{
		URL:      strings.TrimSpace(c.AirflowURL),
		Username: strings.TrimSpace(c.AirflowUsername),
		Password: c.AirflowPassword,
		Token:    strings.TrimSpace(c.AirflowToken),
	}
	if a.URL == "" {
		if a.Username != "" || a.Password != "" || a.Token != "" {
			return Airflow{}, fmt.Errorf("config: AIRFLOW_USERNAME, AIRFLOW_PASSWORD or AIRFLOW_TOKEN is set but AIRFLOW_URL is empty")
		}
		return Airflow{}, nil
	}
	scheme, err := checkHTTPURL("AIRFLOW_URL", a.URL)
	if err != nil {
		return Airflow{}, err
	}
	if (a.Username == "") != (a.Password == "") {
		return Airflow{}, fmt.Errorf("config: AIRFLOW_USERNAME and AIRFLOW_PASSWORD go together: set both or neither")
	}
	if a.Username != "" && a.Token != "" {
		return Airflow{}, fmt.Errorf("config: AIRFLOW_TOKEN and AIRFLOW_USERNAME/AIRFLOW_PASSWORD: set one login, not both")
	}
	if scheme == "http" {
		switch {
		case a.Token != "":
			return Airflow{}, fmt.Errorf("config: AIRFLOW_TOKEN with an http:// AIRFLOW_URL sends the token in clear text; use an https:// URL")
		case a.Username != "":
			return Airflow{}, fmt.Errorf("config: AIRFLOW_USERNAME/AIRFLOW_PASSWORD with an http:// AIRFLOW_URL sends the password in clear text; use an https:// URL")
		}
	}
	return a, nil
}

// DBT holds the typed dbt settings (P0 item 8): an artifacts directory or dbt
// Cloud, never both; all empty = off. CloudToken is a secret.
type DBT struct {
	ArtifactsDir string
	CloudURL     string
	CloudToken   string
	CloudAccount string
	CloudJobID   string
}

// ParseDBT validates the dbt settings: DBT_ARTIFACTS_DIR (an absolute path)
// or dbt Cloud (DBT_CLOUD_URL, an https URL without user info, plus
// DBT_CLOUD_TOKEN, DBT_CLOUD_ACCOUNT_ID and DBT_CLOUD_JOB_ID, the two ids
// whole numbers), not both. An http:// DBT_CLOUD_URL is refused: the token
// goes with every request and would travel in clear text. Whether the
// directory exists is the reader's check at use, not this one. The error
// names the env var.
func ParseDBT(c Config) (DBT, error) {
	d := DBT{
		ArtifactsDir: strings.TrimSpace(c.DBTArtifactsDir),
		CloudURL:     strings.TrimSpace(c.DBTCloudURL),
		CloudToken:   strings.TrimSpace(c.DBTCloudToken),
		CloudAccount: strings.TrimSpace(c.DBTCloudAccount),
		CloudJobID:   strings.TrimSpace(c.DBTCloudJobID),
	}
	cloud := d.CloudURL != "" || d.CloudToken != "" || d.CloudAccount != "" || d.CloudJobID != ""
	if d.ArtifactsDir != "" {
		if cloud {
			return DBT{}, fmt.Errorf("config: DBT_ARTIFACTS_DIR and DBT_CLOUD_*: set one dbt source, not both")
		}
		if !filepath.IsAbs(d.ArtifactsDir) {
			return DBT{}, fmt.Errorf("config: DBT_ARTIFACTS_DIR=%q: want an absolute path", c.DBTArtifactsDir)
		}
		d.ArtifactsDir = filepath.Clean(d.ArtifactsDir)
		return d, nil
	}
	if !cloud {
		return DBT{}, nil
	}
	if d.CloudURL == "" || d.CloudToken == "" || d.CloudAccount == "" || d.CloudJobID == "" {
		return DBT{}, fmt.Errorf("config: dbt Cloud needs DBT_CLOUD_URL, DBT_CLOUD_TOKEN, DBT_CLOUD_ACCOUNT_ID and DBT_CLOUD_JOB_ID together")
	}
	scheme, err := checkHTTPURL("DBT_CLOUD_URL", d.CloudURL)
	if err != nil {
		return DBT{}, err
	}
	if scheme == "http" {
		return DBT{}, fmt.Errorf("config: DBT_CLOUD_TOKEN with an http:// DBT_CLOUD_URL sends the token in clear text; use an https:// URL")
	}
	if _, err := parseWholeNumber("DBT_CLOUD_ACCOUNT_ID", d.CloudAccount, "", false); err != nil {
		return DBT{}, err
	}
	if _, err := parseWholeNumber("DBT_CLOUD_JOB_ID", d.CloudJobID, "", false); err != nil {
		return DBT{}, err
	}
	return d, nil
}

// ---- U wave (setup rooms; docs/design/setup-agent.md §3.8, §4 item 6) ----

// Setup defaults.
const (
	// DefaultSetupTokenBudget is the model tokens one setup room may use.
	DefaultSetupTokenBudget = "2000000"
	// DefaultSetupLocalDelay is the countdown before a T1 setup step runs on
	// a local-class target (policy setup_local): long enough to read the
	// card and cancel.
	DefaultSetupLocalDelay = "20s"
	// DefaultSetupPluginHosts is where plugin_install may download from; the
	// digest it verifies comes from the knowledge index, never from the host.
	DefaultSetupPluginHosts = "repo1.maven.org"
	// MaxSetupLocalDelay bounds PIPEROOM_SETUP_LOCAL_DELAY like
	// MaxAutoApproveDelay bounds the auto-approve countdown.
	MaxSetupLocalDelay = time.Hour
)

// Setup holds the typed server-side setup settings. Enabled false means no
// setup route, no runner endpoint, no setup action and no remote writer is
// built, whatever the other fields say.
type Setup struct {
	Enabled      bool
	ProdPatterns []string      // host globs, kubectl context names, cloud account ids; nil when none
	TokenBudget  int64         // > 0
	LocalDelay   time.Duration // 0 = every T1 step waits for a click
	PluginHosts  []string      // never empty when Enabled
	SecretsDir   string        // absolute; "" only when !Enabled
	OwnersDir    string        // absolute or ""
}

// ParseSetup validates the setup variables. PIPEROOM_SETUP_ENABLED is "off"
// (the default, also when empty) or "on"; anything else is an error naming
// the variable. "on" is refused while ParseWriteMode(c) is false, with an
// error that names both variables: with write mode off nothing in the tree
// may reach a writer, and setup is all writes. With setup on,
// PIPEROOM_SECRETS_DIR must be set and absolute (a replica that cannot write
// to it refuses to start, which the server checks after this); OWNERS_DIR,
// when set, must be absolute. The other variables are checked whether or not
// setup is on, so a typo is caught before the day setup is switched on.
func ParseSetup(c Config) (Setup, error) {
	var s Setup
	switch strings.TrimSpace(c.SetupEnabled) {
	case "", "off":
	case "on":
		s.Enabled = true
	default:
		return Setup{}, fmt.Errorf("config: PIPEROOM_SETUP_ENABLED=%q: want off or on", c.SetupEnabled)
	}
	if s.Enabled {
		on, err := ParseWriteMode(c)
		if err != nil {
			return Setup{}, err
		}
		if !on {
			return Setup{}, fmt.Errorf("config: PIPEROOM_SETUP_ENABLED=on requires PIPEROOM_WRITE_MODE=on (setup executes writes through the runner; write mode is %q)", c.WriteMode)
		}
	}
	var err error
	if s.TokenBudget, err = parseWholeNumber("PIPEROOM_SETUP_TOKEN_BUDGET", c.SetupTokenBudget, DefaultSetupTokenBudget, false); err != nil {
		return Setup{}, err
	}
	if s.LocalDelay, err = parseSetupLocalDelay(c.SetupLocalDelay); err != nil {
		return Setup{}, err
	}
	s.ProdPatterns = splitList(c.SetupProdPatterns)
	for _, p := range s.ProdPatterns {
		if strings.ContainsAny(p, " \t\r\n") {
			return Setup{}, fmt.Errorf("config: PIPEROOM_SETUP_PROD_PATTERNS: entry %q contains white space", p)
		}
	}
	hosts := c.SetupPluginHosts
	if strings.TrimSpace(hosts) == "" {
		hosts = DefaultSetupPluginHosts
	}
	s.PluginHosts = splitList(hosts)
	for _, h := range s.PluginHosts {
		if strings.ContainsAny(h, "/:@ \t") {
			return Setup{}, fmt.Errorf("config: PIPEROOM_SETUP_PLUGIN_HOSTS: entry %q is not a host name", h)
		}
	}
	s.SecretsDir = strings.TrimSpace(c.SecretsDir)
	if s.SecretsDir != "" && !filepath.IsAbs(s.SecretsDir) {
		return Setup{}, fmt.Errorf("config: PIPEROOM_SECRETS_DIR: want an absolute directory")
	}
	if s.Enabled && s.SecretsDir == "" {
		return Setup{}, fmt.Errorf("config: PIPEROOM_SETUP_ENABLED=on requires PIPEROOM_SECRETS_DIR (where registered read credentials are written)")
	}
	s.OwnersDir = strings.TrimSpace(c.OwnersDir)
	if s.OwnersDir != "" && !filepath.IsAbs(s.OwnersDir) {
		return Setup{}, fmt.Errorf("config: OWNERS_DIR: want an absolute directory")
	}
	return s, nil
}

// parseSetupLocalDelay is ParseAutoApproveDelay's rule for
// PIPEROOM_SETUP_LOCAL_DELAY: "0" = off (every T1 step waits for a click),
// else a Go duration above zero and at most MaxSetupLocalDelay; empty = 20s.
func parseSetupLocalDelay(v string) (time.Duration, error) {
	v = strings.TrimSpace(v)
	switch v {
	case "0":
		return 0, nil
	case "":
		v = DefaultSetupLocalDelay
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: PIPEROOM_SETUP_LOCAL_DELAY=%q: not a duration like 20s or 2m: %w", v, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("config: PIPEROOM_SETUP_LOCAL_DELAY=%q: must be greater than zero; use \"0\" so every T1 step waits for a click", v)
	}
	if d > MaxSetupLocalDelay {
		return 0, fmt.Errorf("config: PIPEROOM_SETUP_LOCAL_DELAY=%q: at most %s", v, MaxSetupLocalDelay)
	}
	return d, nil
}

// SetupRunner holds the typed runner-side settings (cmd/setup). Code is the
// pairing code: it is used once and must never be logged or written to the
// runner's op log.
type SetupRunner struct {
	SecretsFile string // "" = <project dir>/.piperoom/secrets.env
	ServerURL   string // "" = --server must be given (or --standalone)
	Code        string // "" = --code must be given (or --standalone)
}

// ParseSetupRunner validates the runner variables. PIPEROOM_SETUP_SERVER_URL,
// when set, must be an http(s) URL without user info (the runner itself
// refuses plain http to a non-loopback host, the netsec rule);
// PIPEROOM_SETUP_CODE, when set, must be exactly 8 characters (the alphabet
// is checked by the runner against protocol.CodeAlphabet). The error never
// quotes the code.
func ParseSetupRunner(c Config) (SetupRunner, error) {
	r := SetupRunner{
		SecretsFile: strings.TrimSpace(c.SetupSecretsFile),
		ServerURL:   strings.TrimSpace(c.SetupServerURL),
		Code:        strings.TrimSpace(c.SetupCode),
	}
	if r.ServerURL != "" {
		if _, err := checkHTTPURL("PIPEROOM_SETUP_SERVER_URL", r.ServerURL); err != nil {
			return SetupRunner{}, err
		}
	}
	if r.Code != "" && len(r.Code) != 8 {
		return SetupRunner{}, fmt.Errorf("config: PIPEROOM_SETUP_CODE: want the 8-character pairing code the Set up button showed")
	}
	return r, nil
}

// checkHTTPURL refuses anything but an absolute http(s) URL with a host and
// no user info (credentials do not belong in a URL that may be logged) and
// returns its scheme ("http" or "https", lower case) so the caller can refuse
// a login over plain http. The error names the env var but never quotes the
// value.
func checkHTTPURL(name, v string) (string, error) {
	u, err := url.Parse(v)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("config: %s: want an http(s)://host URL", name)
	}
	if u.User != nil {
		return "", fmt.Errorf("config: %s: must not carry user info", name)
	}
	return u.Scheme, nil
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

// registryURL maps "off" to "": a pipeline without a Schema Registry says so
// explicitly, instead of every client and map chasing an unreachable default.
func registryURL(v string) string {
	v = strings.TrimSpace(v)
	if strings.EqualFold(v, "off") {
		return ""
	}
	return v
}

func get(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
