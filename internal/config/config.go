// Package config reads settings from environment variables.
//
// CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
package config

import (
	"fmt"
	"log/slog"
	"net"
	"net/netip"
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
	SourceDBURL       string // SOURCE_DB_URL (the CDC source database: postgres:// or mysql://; a pipeline's source_kind must match the scheme, Q8)
	SourceDBWriteURL  string // SOURCE_DB_WRITE_URL (P0 item 7): the source database (postgres:// or mysql://, matching source_kind, Q8) as a separate WRITE role for the pg write actions, never the read-only SOURCE_DB_URL role; empty = no writer (those actions refuse at Execute: "no write credentials"; #606); secret
	WarehouseDBURL    string // WAREHOUSE_DB_URL (DSN for the selected warehouse kind)
	KafkaBrokers      string // KAFKA_BROKERS (comma separated)
	ConnectURL        string // CONNECT_URL (CONNECT_KIND=rest only)
	ConnectKind       string // CONNECT_KIND: rest (default) | msk | confluent | aiven (ParseConnectKind; no caller yet, inert until wiring and cmd/discover call it: msk.md PR 2, U13)
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

	// X4: ChangesWebhookSecret signs POST /api/changes/webhook with the
	// generic alert scheme (X-Piperoom-Signature: t=<unix ts>,v1=<hex
	// HMAC-SHA256("<ts>.<body>")>, ±5 min). Comma-separated for rotation,
	// like GENERIC_ALERT_SECRET. Empty = the route answers 401. Secret; never
	// log it. Limits: ChangesWebhookMaxBodyBytes, ChangesWebhookPerMinute.
	ChangesWebhookSecret string // CHANGES_WEBHOOK_SECRET

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
	// X3: how far ahead a forecast signal looks (ParseWatchForecast).
	WatchForecastHorizon string // PIPEROOM_WATCH_FORECAST_HORIZON: Go duration, default 48h, 1h..168h

	// Triage pass (F6): one bounded first look on a cheap model before the
	// full investigation, outside the agent pool. Not PIPEROOM_TRIAGE_MODEL,
	// which runs the first turns of the investigation itself. ParseTriagePass
	// types them and names the env var on error.
	TriagePass        string // PIPEROOM_TRIAGE_PASS: on | off, default on
	TriagePassModel   string // PIPEROOM_TRIAGE_PASS_MODEL: empty = claude-haiku-5-5 on a Claude provider, else the role's main model (ParseModelRoles; ParseTriagePass)
	TriagePassTimeout string // PIPEROOM_TRIAGE_PASS_TIMEOUT: Go duration, default 40s, max 90s

	// PostmortemModel says whether a closed room without a fresh hand-off
	// summary gets one model call for the post-mortem prose (F2):
	// auto (default) | off (events-only draft). ParsePostmortemModel.
	// PostmortemMode is its name since V0; PostmortemModel is the deprecated
	// alias (design model-providers 15, decision 5). Both empty = auto.
	PostmortemModel string // PIPEROOM_POSTMORTEM_MODEL (deprecated alias of PIPEROOM_POSTMORTEM_MODE)
	PostmortemMode  string // PIPEROOM_POSTMORTEM_MODE: auto | off

	// ChangesRetentionDays deletes pipeline_changes rows older than this many
	// days (F8); "0" = keep. Rows per pipeline are capped at 5000 regardless.
	ChangesRetentionDays string // PIPEROOM_CHANGES_RETENTION_DAYS: default 30

	// Debezium metrics (F12): the Prometheus text endpoint of the JMX
	// exporter next to Kafka Connect. ParseDebeziumMetrics.
	DebeziumMetricsURL    string // DEBEZIUM_METRICS_URL: http(s)://host:port/metrics; empty = debezium_metrics says "not configured"
	DebeziumMetricsPrefix string // DEBEZIUM_METRICS_PREFIX: metric name prefix, default "debezium_metrics_"

	// X7: the optional OpenLineage emitter (X7b; mapping in
	// docs/design/lineage.md). Empty URL = off: no goroutine, no request.
	// ParseOpenLineage. The API key is a secret; never log it.
	OpenLineageURL       string // OPENLINEAGE_URL: the full endpoint events are POSTed to, e.g. http://marquez:5000/api/v1/lineage
	OpenLineageNamespace string // OPENLINEAGE_NAMESPACE: the job namespace, default "piperoom"
	OpenLineageAPIKey    string // OPENLINEAGE_API_KEY: sent as "Authorization: Bearer <key>"; empty = no header (secret)

	// Q7: the forge of the pull-request fix (docs/design/fix-pr.md §4, §5.6).
	// FORGE_KIND "" = off: no forge client, no forge tool, no fix-PR action.
	// ParseForge and ParseFixPRApplyTimeout type them. The App's private key
	// lives in a file; only its path is here.
	ForgeKind               string // FORGE_KIND: "" (off, the default) | "github"
	GitHubAPIURL            string // GITHUB_API_URL: default https://api.github.com; GitHub Enterprise Server: https://<host>/api/v3
	GitHubAppID             string // GITHUB_APP_ID
	GitHubAppInstallationID string // GITHUB_APP_INSTALLATION_ID
	GitHubAppPrivateKeyFile string // GITHUB_APP_PRIVATE_KEY_FILE: path to the App's PEM private key (secret file)
	FixPRApplyTimeout       string // PIPEROOM_FIXPR_APPLY_TIMEOUT: how long after a merge the agent waits for the change to reach the system; default "2h", 10m to 168h

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
	// model; empty = always Model. Always on the main provider: the triage
	// turns share the investigation's history.
	TriageModel string // PIPEROOM_TRIAGE_MODEL

	// Model providers (V0; docs/design/model-providers.md section 9). Kept as
	// strings like the other groups; ParseModelRoles and ParseModelKnobs type
	// them and name the env var on error. Keys are never here: adapters read
	// PIPEROOM_MODEL_API_KEY / PIPEROOM_MODEL_API_KEY_FILE (EnvModelAPIKey,
	// EnvModelAPIKeyFile) or the provider's standard key variable themselves.
	// On anthropic, bedrock and vertex none of these changes anything unless set.
	ModelFlavor                  string // PIPEROOM_MODEL_FLAVOR: provider openai only; openai (default) | azure | deepseek | qwen | mistral | groq | together | openrouter | xai | ollama | vllm | generic
	ModelBaseURL                 string // PIPEROOM_MODEL_BASE_URL: providers openai and gemini only; empty = the flavor's default (ParseBaseURL)
	ModelContextTokens           string // PIPEROOM_MODEL_CONTEXT_TOKENS: the main model's context window; empty = the adapter's table, else 128000
	ModelCaps                    string // PIPEROOM_MODEL_CAPS: comma list for generic, ollama and vllm hosts: parallel_tools, structured_output, reasoning
	ModelUnverified              string // PIPEROOM_MODEL_UNVERIFIED: on | off (default); on allows a model not in supported.json on a new provider
	ModelMaxConcurrency          string // PIPEROOM_MODEL_MAX_CONCURRENCY: requests in flight per endpoint, new providers only; empty = PIPEROOM_MAX_AGENTS
	ModelTPM                     string // PIPEROOM_MODEL_TPM: optional tokens-per-minute budget per endpoint; empty = none
	ModelProxy                   string // PIPEROOM_MODEL_PROXY: on | off (default); on sends model requests through HTTPS_PROXY with the target check
	GeminiBackend                string // PIPEROOM_GEMINI_BACKEND: api (default) | vertex (Google ADC, VERTEX_PROJECT_ID, VERTEX_REGION)
	ModelAllowPrivateHTTP        string // PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP: on | off (default); on allows plain http to RFC 1918, ULA and loopback hosts only
	ModelAckCNEndpoint           string // PIPEROOM_MODEL_ACK_CN_ENDPOINT: must be "yes" to start with an endpoint operated in the People's Republic of China
	ModelPricingStrict           string // PIPEROOM_MODEL_PRICING_STRICT: all | non-claude (default) | off; off with PIPEROOM_PIPELINE_DAILY_COST_USD refuses start
	ModelNeutraliseControlTokens string // PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS: all (default) | non-claude; there is no off
	ModelPromptVariant           string // PIPEROOM_MODEL_PROMPT_VARIANT: reserved, not implemented; any non-empty value refuses start
	TriagePassProvider           string // PIPEROOM_TRIAGE_PASS_PROVIDER: first look; empty = PIPEROOM_MODEL_PROVIDER
	SummaryProvider              string // PIPEROOM_SUMMARY_PROVIDER: hand-off and post-mortem writer; empty = PIPEROOM_MODEL_PROVIDER
	SummaryModel                 string // PIPEROOM_SUMMARY_MODEL: empty = PIPEROOM_MODEL (required when the provider differs)
	JudgeProvider                string // PIPEROOM_JUDGE_PROVIDER: hand-off and evals judge; empty = PIPEROOM_MODEL_PROVIDER
	JudgeModel                   string // PIPEROOM_JUDGE_MODEL: empty = PIPEROOM_MODEL (required when the provider differs)
	TriagePassBaseURL            string // PIPEROOM_TRIAGE_PASS_BASE_URL: only when the first look's provider differs from the main one
	SummaryBaseURL               string // PIPEROOM_SUMMARY_BASE_URL: only when the summary provider differs from the main one
	JudgeBaseURL                 string // PIPEROOM_JUDGE_BASE_URL: only when the judge provider differs from the main one
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

	// HypothesisGuard is PIPEROOM_HYPOTHESIS_GUARD: off | log | enforce
	// (default log). The agent's checks that refuse a "nothing to fix" verdict
	// while the watcher still fires and a "ruled out" claim no cited read
	// supports; log only records what would be refused. (H1)
	HypothesisGuard string // PIPEROOM_HYPOTHESIS_GUARD: off | log | enforce

	// Autonomy is PIPEROOM_AUTONOMY: the AU wave's earned auto-approve.
	// ParseAutonomy types it; write mode off makes it off whatever it says. (AU1)
	Autonomy string // PIPEROOM_AUTONOMY: on (default) | off

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
		ConnectKind:       get("CONNECT_KIND", DefaultConnectKind),
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
		ChangesWebhookSecret: get("CHANGES_WEBHOOK_SECRET", ""), // X4

		MaxAgents:        get("PIPEROOM_MAX_AGENTS", ""),
		MaxAgentsGlobal:  get("PIPEROOM_MAX_AGENTS_GLOBAL", ""),
		ReplicaID:        get("PIPEROOM_REPLICA_ID", ""),
		ResumeOnTakeover: get("PIPEROOM_RESUME_ON_TAKEOVER", ""),

		WatchMode:         get("PIPEROOM_WATCH_MODE", DefaultWatchMode),
		WatchInterval:     get("PIPEROOM_WATCH_INTERVAL", DefaultWatchInterval),
		WatchCheckTimeout: get("PIPEROOM_WATCH_CHECK_TIMEOUT", DefaultWatchCheckTimeout),
		WatchCooldown:     get("PIPEROOM_WATCH_COOLDOWN", DefaultWatchCooldown),
		WatchMaxOpenRooms: get("PIPEROOM_WATCH_MAX_OPEN_ROOMS", DefaultWatchMaxOpenRooms),
		// X3
		WatchForecastHorizon: get("PIPEROOM_WATCH_FORECAST_HORIZON", DefaultWatchForecastHorizon),

		TriagePass: get("PIPEROOM_TRIAGE_PASS", DefaultTriagePass),
		// No default here since V0: ParseModelRoles (and ParseTriagePass) give
		// claude-haiku-5-5 on a Claude provider and can tell "unset" elsewhere.
		TriagePassModel:   get("PIPEROOM_TRIAGE_PASS_MODEL", ""),
		TriagePassTimeout: get("PIPEROOM_TRIAGE_PASS_TIMEOUT", DefaultTriagePassTimeout),

		// Empty default (ParsePostmortemModel reads "" as auto), so a set alias
		// can be told from an unset one (V0).
		PostmortemModel: get("PIPEROOM_POSTMORTEM_MODEL", ""),
		PostmortemMode:  get("PIPEROOM_POSTMORTEM_MODE", ""),

		ChangesRetentionDays: get("PIPEROOM_CHANGES_RETENTION_DAYS", DefaultChangesRetentionDays),

		DebeziumMetricsURL:    get("DEBEZIUM_METRICS_URL", ""),
		DebeziumMetricsPrefix: get("DEBEZIUM_METRICS_PREFIX", DefaultDebeziumMetricsPrefix),

		// X7
		OpenLineageURL:       get("OPENLINEAGE_URL", ""),
		OpenLineageNamespace: get("OPENLINEAGE_NAMESPACE", DefaultOpenLineageNamespace),
		OpenLineageAPIKey:    get("OPENLINEAGE_API_KEY", ""),

		// Q7
		ForgeKind:               get("FORGE_KIND", ""),
		GitHubAPIURL:            get("GITHUB_API_URL", DefaultGitHubAPIURL),
		GitHubAppID:             get("GITHUB_APP_ID", ""),
		GitHubAppInstallationID: get("GITHUB_APP_INSTALLATION_ID", ""),
		GitHubAppPrivateKeyFile: get("GITHUB_APP_PRIVATE_KEY_FILE", ""),
		FixPRApplyTimeout:       get("PIPEROOM_FIXPR_APPLY_TIMEOUT", ""),

		WebDir: get("PIPEROOM_WEB_DIR", ""),

		ModelProvider:   get("PIPEROOM_MODEL_PROVIDER", "anthropic"),
		AWSRegion:       get("AWS_REGION", ""),
		VertexProject:   get("VERTEX_PROJECT_ID", ""),
		VertexRegion:    get("VERTEX_REGION", ""),
		TriageModel:     get("PIPEROOM_TRIAGE_MODEL", ""),
		RoomTokenBudget: get("PIPEROOM_ROOM_TOKEN_BUDGET", ""),

		// V0
		ModelFlavor:                  get("PIPEROOM_MODEL_FLAVOR", ""),
		ModelBaseURL:                 get("PIPEROOM_MODEL_BASE_URL", ""),
		ModelContextTokens:           get("PIPEROOM_MODEL_CONTEXT_TOKENS", ""),
		ModelCaps:                    get("PIPEROOM_MODEL_CAPS", ""),
		ModelUnverified:              get("PIPEROOM_MODEL_UNVERIFIED", ""),
		ModelMaxConcurrency:          get("PIPEROOM_MODEL_MAX_CONCURRENCY", ""),
		ModelTPM:                     get("PIPEROOM_MODEL_TPM", ""),
		ModelProxy:                   get("PIPEROOM_MODEL_PROXY", ""),
		GeminiBackend:                get("PIPEROOM_GEMINI_BACKEND", ""),
		ModelAllowPrivateHTTP:        get("PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP", ""),
		ModelAckCNEndpoint:           get("PIPEROOM_MODEL_ACK_CN_ENDPOINT", ""),
		ModelPricingStrict:           get("PIPEROOM_MODEL_PRICING_STRICT", ""),
		ModelNeutraliseControlTokens: get("PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS", ""),
		ModelPromptVariant:           get("PIPEROOM_MODEL_PROMPT_VARIANT", ""),
		TriagePassProvider:           get("PIPEROOM_TRIAGE_PASS_PROVIDER", ""),
		SummaryProvider:              get("PIPEROOM_SUMMARY_PROVIDER", ""),
		SummaryModel:                 get("PIPEROOM_SUMMARY_MODEL", ""),
		JudgeProvider:                get("PIPEROOM_JUDGE_PROVIDER", ""),
		JudgeModel:                   get("PIPEROOM_JUDGE_MODEL", ""),
		TriagePassBaseURL:            get("PIPEROOM_TRIAGE_PASS_BASE_URL", ""),
		SummaryBaseURL:               get("PIPEROOM_SUMMARY_BASE_URL", ""),
		JudgeBaseURL:                 get("PIPEROOM_JUDGE_BASE_URL", ""),

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

		HypothesisGuard: get("PIPEROOM_HYPOTHESIS_GUARD", DefaultHypothesisGuard),
		Autonomy:        get("PIPEROOM_AUTONOMY", "on"),

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

// DefaultHypothesisGuard is the guard mode when PIPEROOM_HYPOTHESIS_GUARD is
// unset: check and log, never refuse, until a pilot week of logs says enforce
// is safe. (H1)
const DefaultHypothesisGuard = "log"

// ParseHypothesisGuard validates PIPEROOM_HYPOTHESIS_GUARD. Empty is the
// default; off, log and enforce are accepted; anything else is an error naming
// the variable, so a typo stops the start instead of silently turning the guard
// off or on. (H1)
func ParseHypothesisGuard(c Config) (string, error) {
	switch v := strings.TrimSpace(c.HypothesisGuard); v {
	case "":
		return DefaultHypothesisGuard, nil
	case "off", "log", "enforce":
		return v, nil
	}
	return "", fmt.Errorf("config: PIPEROOM_HYPOTHESIS_GUARD=%q: want off, log or enforce", c.HypothesisGuard)
}

// FakeAgentScripted is the only accepted value of PIPEROOM_FAKE_AGENT.
const FakeAgentScripted = "scripted"

// ParseFakeAgent validates PIPEROOM_FAKE_AGENT and reports whether the server
// runs the scripted agent instead of the real one. Empty is false. "scripted"
// is true only in dev auth mode with PIPEROOM_DEV_ACK=DevAckValue, with write
// mode off and with no Slack bot token: a scripted agent shows people an
// investigation that did not happen, so it must never reach a deployment with
// real users or a real channel, and its proposals must never run against real
// systems. Anything else is an error naming the variable, so a typo stops the
// start instead of running the real agent unnoticed.
//
// cmd/server reads it only outside --demo and --quickstart (--demo already
// runs the scripted agent and forces write mode on). When it is true the
// server replaces every model path, not only the runner: the hand-off and
// post-mortem writers are the scripted ones, triage is off and no model
// endpoint is built.
func ParseFakeAgent(c Config) (bool, error) {
	switch v := strings.TrimSpace(c.FakeAgent); v {
	case "":
		return false, nil
	case FakeAgentScripted:
	default:
		return false, fmt.Errorf("config: PIPEROOM_FAKE_AGENT=%q: want %q (or unset)", c.FakeAgent, FakeAgentScripted)
	}
	if mode := strings.ToLower(strings.TrimSpace(c.AuthMode)); mode != "" && mode != DefaultAuthMode {
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
	if strings.TrimSpace(c.SlackBotToken) != "" {
		return false, fmt.Errorf("config: PIPEROOM_FAKE_AGENT=%s needs SLACK_BOT_TOKEN unset", FakeAgentScripted)
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

// ---- X3: watch sample history and forecasts ----

// Sample history and forecast settings. Only the horizon is configurable:
// the sample period, retention, fit window and fit threshold are fixed by the
// X3a design, and the trends route and the web chart (X3d) assume them. (X3)
const (
	// WatchSampleInterval is the least time between two samples of one
	// pipeline and metric, whatever PIPEROOM_WATCH_INTERVAL is.
	WatchSampleInterval = 15 * time.Minute
	// WatchSampleRetention is how long watch_samples rows are kept; it is
	// also the largest ?days= of GET /api/pipelines/{pid}/trends.
	WatchSampleRetention = 7 * 24 * time.Hour
	// WatchForecastFitWindow is the history the least-squares line uses.
	WatchForecastFitWindow = 24 * time.Hour
	// WatchForecastMinR2 is the least goodness of fit that may fire: a noisy
	// series says nothing about when it reaches a limit.
	WatchForecastMinR2 = 0.6
	// WatchForecastMinSamples is the fewest points a fit may use (2 h).
	WatchForecastMinSamples = 8

	DefaultWatchForecastHorizon = "48h"
	MinWatchForecastHorizon     = time.Hour
	// MaxWatchForecastHorizon: past a week a 24 h line says too little, and
	// the web shows no ETA badge beyond 7 d.
	MaxWatchForecastHorizon = 7 * 24 * time.Hour
)

// WatchForecast holds the typed forecast settings. (X3)
type WatchForecast struct {
	// Horizon: a forecast fires when the metric's ETA to its limit is less.
	Horizon time.Duration
}

// ParseWatchForecast validates PIPEROOM_WATCH_FORECAST_HORIZON: a Go
// duration from 1h to 168h; empty = 48h. The error names the env var. (X3)
func ParseWatchForecast(c Config) (WatchForecast, error) {
	h, err := parseWatchDuration("PIPEROOM_WATCH_FORECAST_HORIZON", c.WatchForecastHorizon, DefaultWatchForecastHorizon)
	if err != nil {
		return WatchForecast{}, err
	}
	if h < MinWatchForecastHorizon || h > MaxWatchForecastHorizon {
		return WatchForecast{}, fmt.Errorf("config: PIPEROOM_WATCH_FORECAST_HORIZON=%q: must be between %s and %s",
			c.WatchForecastHorizon, MinWatchForecastHorizon, MaxWatchForecastHorizon)
	}
	return WatchForecast{Horizon: h}, nil
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
		// The same default and refusal as ParseModelRoles: claude-haiku-5-5
		// on a Claude provider (today's value), the main model on a new one
		// (V0), and none for a first-look provider other than the main one
		// unless it is a Claude provider, so a first look moved to openai
		// never silently runs the main Claude model instead.
		p, main := firstLookProvider(c), mainProvider(c)
		if p != main && !claudeProvider(p) {
			return TriagePass{}, fmt.Errorf("config: PIPEROOM_TRIAGE_PASS_MODEL is required when PIPEROOM_TRIAGE_PASS_PROVIDER (%s) differs from PIPEROOM_MODEL_PROVIDER (%s)", p, main)
		}
		mainModel := strings.TrimSpace(c.Model)
		if mainModel == "" {
			mainModel = DefaultModel
		}
		t.Model = defaultFirstLookModel(p, mainModel)
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

// ParsePostmortemModel returns the post-mortem mode (auto | off, any case;
// empty = auto). It reads PIPEROOM_POSTMORTEM_MODE first and its deprecated
// alias PIPEROOM_POSTMORTEM_MODEL second; the two set to different values,
// or any other value, refuse start and the error names the var. The
// deprecation warning is ParseModelKnobs' (V0).
func ParsePostmortemModel(c Config) (string, error) {
	mode, _, err := parsePostmortemMode(c)
	return mode, err
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

// X4: limits of POST /api/changes/webhook (docs/api.md, External changes).
const (
	// ChangesWebhookMaxBodyBytes caps the body; a larger one is 413
	// too_large and nothing is stored.
	ChangesWebhookMaxBodyBytes = 4096
	// ChangesWebhookPerMinute is how many verified requests one pipeline may
	// send per minute; the next is 429 rate_limited.
	ChangesWebhookPerMinute = 60
	// VendorStatusPollInterval is how often an owners.yaml vendor_status_url
	// is fetched; an unreachable feed logs one warning per hour.
	VendorStatusPollInterval = 5 * time.Minute
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

// ---- X7: OpenLineage emitter (X7b; docs/design/lineage.md) ----

// OpenLineage emitter limits. Fixed, not configurable: docs/design/lineage.md
// and the emitter's tests state them. (X7)
const (
	DefaultOpenLineageNamespace = "piperoom"
	// OpenLineageEmitInterval is the least time between two topology
	// events of one pipeline; builds in between send nothing.
	OpenLineageEmitInterval = 15 * time.Minute
	// OpenLineageTimeout is the deadline of one POST, retries apart.
	OpenLineageTimeout = 10 * time.Second
	// OpenLineageMaxRetries is how many times a failed POST (network error,
	// 429 or 5xx) is retried with backoff before one warning is logged and
	// the event is dropped. A 4xx other than 429 is not retried.
	OpenLineageMaxRetries = 3
	// OpenLineageMaxEventBytes caps one event body. An event over it is sent
	// without its columnLineage and schema facets, then dropped if still over.
	OpenLineageMaxEventBytes = 1 << 20
	// OpenLineageMaxResponseBytes caps what is read of a response (rule 7).
	OpenLineageMaxResponseBytes = 4 << 20
)

// OpenLineage holds the typed emitter settings. URL "" = off.
type OpenLineage struct {
	URL       string
	Namespace string
	APIKey    string // secret: LogValue hides it
}

// LogValue keeps the API key out of the log.
func (o OpenLineage) LogValue() slog.Value {
	return slog.GroupValue(slog.String("url", o.URL), slog.String("namespace", o.Namespace),
		slog.Bool("api_key_set", o.APIKey != ""))
}

// ParseOpenLineage validates OPENLINEAGE_URL (empty = off; else an absolute
// http(s) URL with a host and no user info), OPENLINEAGE_NAMESPACE (1 to 128
// characters of letters, digits and . _ - : /, starting with a letter or
// digit; empty = "piperoom") and OPENLINEAGE_API_KEY: a key is only sent over
// https, or http to a loopback host, so http to any other host with a key is
// an error. The error names the env var and never quotes the key.
func ParseOpenLineage(c Config) (OpenLineage, error) {
	o := OpenLineage{
		URL:       strings.TrimSpace(c.OpenLineageURL),
		Namespace: strings.TrimSpace(c.OpenLineageNamespace),
		APIKey:    strings.TrimSpace(c.OpenLineageAPIKey),
	}
	if o.Namespace == "" {
		o.Namespace = DefaultOpenLineageNamespace
	}
	if !validOpenLineageNamespace(o.Namespace) {
		return OpenLineage{}, fmt.Errorf("config: OPENLINEAGE_NAMESPACE=%q: want 1 to 128 of letters, digits and . _ - : /, starting with a letter or digit", c.OpenLineageNamespace)
	}
	if o.URL == "" {
		return o, nil
	}
	scheme, err := checkHTTPURL("OPENLINEAGE_URL", o.URL)
	if err != nil {
		return OpenLineage{}, err
	}
	if scheme == "http" && o.APIKey != "" {
		u, _ := url.Parse(o.URL) // checkHTTPURL parsed it
		if !loopbackHost(u.Hostname()) {
			return OpenLineage{}, fmt.Errorf("config: OPENLINEAGE_URL: an API key is sent only over https (or http to a loopback host)")
		}
	}
	return o, nil
}

func validOpenLineageNamespace(ns string) bool {
	if ns == "" || len(ns) > 128 {
		return false
	}
	for i, r := range ns {
		alnum := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
		if !alnum && (i == 0 || !strings.ContainsRune("._-:/", r)) {
			return false
		}
	}
	return true
}

// ---- Q7: the forge of the pull-request fix (docs/design/fix-pr.md §5.6) ----

// Forge kinds of FORGE_KIND. (Q7)
const (
	ForgeKindOff    = ""
	ForgeKindGitHub = "github"
)

// DefaultGitHubAPIURL is GITHUB_API_URL's default: github.com's API. (Q7)
const DefaultGitHubAPIURL = "https://api.github.com"

// Bounds of PIPEROOM_FIXPR_APPLY_TIMEOUT (docs/design/fix-pr.md §14 D5). (Q7)
const (
	DefaultFixPRApplyTimeout = 2 * time.Hour
	MinFixPRApplyTimeout     = 10 * time.Minute
	MaxFixPRApplyTimeout     = 168 * time.Hour
)

// Forge is the parsed forge settings. Zero when FORGE_KIND is "". (Q7)
type Forge struct {
	Kind           string
	APIURL         string // validated, no trailing slash
	WebHost        string // github.com for api.github.com, else the API URL's host
	AppID          int64
	InstallationID int64
	PrivateKeyFile string
}

// ParseForge validates the forge settings. FORGE_KIND "" with any GITHUB_APP_*
// set is an error ("set FORGE_KIND=github"); "github" needs positive integer
// ids and a key file path; the API URL is https unless the host is loopback
// (the netsec rule), with no user info, query or fragment. It does not read
// the key file (wiring does, and refuses a key that is not PEM RSA). (Q7)
//
// GITHUB_API_URL alone does not turn anything on: GitHub Actions sets it in
// every job, so it is read only when FORGE_KIND is github. WebHost is lower
// case and keeps a port (a GitHub Enterprise Server on :8443 serves its web
// pages there too); cmd/server compares every owners.yaml repos[].url host
// with it at start.
func ParseForge(c Config) (Forge, error) {
	kind := strings.ToLower(strings.TrimSpace(c.ForgeKind))
	appID := strings.TrimSpace(c.GitHubAppID)
	instID := strings.TrimSpace(c.GitHubAppInstallationID)
	keyFile := strings.TrimSpace(c.GitHubAppPrivateKeyFile)
	switch kind {
	case ForgeKindOff:
		if appID != "" || instID != "" || keyFile != "" {
			return Forge{}, fmt.Errorf("config: GITHUB_APP_* is set but FORGE_KIND is empty: set FORGE_KIND=github (or unset GITHUB_APP_ID, GITHUB_APP_INSTALLATION_ID and GITHUB_APP_PRIVATE_KEY_FILE)")
		}
		return Forge{}, nil
	case ForgeKindGitHub:
	default:
		return Forge{}, fmt.Errorf("config: FORGE_KIND=%q: want github, or empty for off", c.ForgeKind)
	}
	f := Forge{Kind: kind, PrivateKeyFile: keyFile}
	var err error
	if f.AppID, err = positiveID("GITHUB_APP_ID", appID); err != nil {
		return Forge{}, err
	}
	if f.InstallationID, err = positiveID("GITHUB_APP_INSTALLATION_ID", instID); err != nil {
		return Forge{}, err
	}
	if keyFile == "" {
		return Forge{}, fmt.Errorf("config: GITHUB_APP_PRIVATE_KEY_FILE: required with FORGE_KIND=github (the path of the App's PEM private key)")
	}
	raw := strings.TrimSpace(c.GitHubAPIURL)
	if raw == "" {
		raw = DefaultGitHubAPIURL
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.Hostname() == "" || (u.Scheme != "https" && u.Scheme != "http") || u.Opaque != "" {
		return Forge{}, fmt.Errorf("config: GITHUB_API_URL: want an https://host URL such as %s or https://<host>/api/v3", DefaultGitHubAPIURL)
	}
	if u.User != nil {
		return Forge{}, fmt.Errorf("config: GITHUB_API_URL: must not carry user info")
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#") {
		return Forge{}, fmt.Errorf("config: GITHUB_API_URL: must not carry a query or fragment")
	}
	if u.Scheme == "http" && !loopbackHost(u.Hostname()) {
		return Forge{}, fmt.Errorf("config: GITHUB_API_URL: must be https (plain http only to a loopback host)")
	}
	host := strings.ToLower(u.Host)
	f.APIURL = u.Scheme + "://" + host + strings.TrimRight(u.EscapedPath(), "/")
	f.WebHost = host
	if host == "api.github.com" {
		f.WebHost = "github.com"
	}
	return f, nil
}

// positiveID parses a GitHub App or installation id: a base-10 integer
// above zero. The error names the variable.
func positiveID(name, v string) (int64, error) {
	if v == "" {
		return 0, fmt.Errorf("config: %s: required with FORGE_KIND=github", name)
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("config: %s=%q: want a positive integer id", name, v)
	}
	return n, nil
}

// ParseFixPRApplyTimeout: "" = 2h; a Go duration from 10m to 168h; else an
// error naming the variable. (Q7)
func ParseFixPRApplyTimeout(c Config) (time.Duration, error) {
	v := strings.TrimSpace(c.FixPRApplyTimeout)
	if v == "" {
		return DefaultFixPRApplyTimeout, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: PIPEROOM_FIXPR_APPLY_TIMEOUT=%q: not a duration like 2h or 90m: %w", v, err)
	}
	if d < MinFixPRApplyTimeout || d > MaxFixPRApplyTimeout {
		return 0, fmt.Errorf("config: PIPEROOM_FIXPR_APPLY_TIMEOUT=%q: want %s to %s", v, MinFixPRApplyTimeout, MaxFixPRApplyTimeout)
	}
	return d, nil
}

// loopbackHost reports whether host is localhost or a loopback IP.
func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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

// ---- V wave (V0; docs/design/model-providers.md sections 9, 10.3 and 15) ----

// Model provider values of PIPEROOM_MODEL_PROVIDER and PIPEROOM_<ROLE>_PROVIDER.
// They equal llm.Provider* (internal/agent/llm); config stays a leaf package.
const (
	ModelProviderAnthropic       = "anthropic"
	ModelProviderBedrock         = "bedrock"
	ModelProviderVertex          = "vertex"
	ModelProviderOpenAI          = "openai"
	ModelProviderGemini          = "gemini"
	ModelProviderBedrockConverse = "bedrock-converse"
)

// Key variables of the main provider. Adapters read them at construction;
// they are never stored in Config, logged or put in an error.
const (
	EnvModelAPIKey     = "PIPEROOM_MODEL_API_KEY"
	EnvModelAPIKeyFile = "PIPEROOM_MODEL_API_KEY_FILE"
)

// DefaultModel is the main model when PIPEROOM_MODEL is unset.
const DefaultModel = "claude-opus-5-5"

// modelFlavors are the PIPEROOM_MODEL_FLAVOR values with their default base
// URL ("" = none: PIPEROOM_MODEL_BASE_URL is required) (design section 3).
var modelFlavors = map[string]string{
	"openai":     "https://api.openai.com/v1",
	"azure":      "",
	"deepseek":   "https://api.deepseek.com/v1",
	"qwen":       "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
	"mistral":    "https://api.mistral.ai/v1",
	"groq":       "https://api.groq.com/openai/v1",
	"together":   "https://api.together.xyz/v1",
	"openrouter": "https://openrouter.ai/api/v1",
	"xai":        "https://api.x.ai/v1",
	"ollama":     "http://localhost:11434/v1",
	"vllm":       "http://localhost:8000/v1",
	"generic":    "",
}

// claudeProvider reports whether p is anthropic, bedrock or vertex, where
// every model id is accepted as today.
func claudeProvider(p string) bool {
	return p == ModelProviderAnthropic || p == ModelProviderBedrock || p == ModelProviderVertex
}

func knownProvider(p string) bool {
	return claudeProvider(p) || p == ModelProviderOpenAI || p == ModelProviderGemini || p == ModelProviderBedrockConverse
}

// mainProvider is PIPEROOM_MODEL_PROVIDER normalised; empty is anthropic.
func mainProvider(c Config) string {
	if p := strings.ToLower(strings.TrimSpace(c.ModelProvider)); p != "" {
		return p
	}
	return ModelProviderAnthropic
}

func firstLookProvider(c Config) string {
	if p := strings.ToLower(strings.TrimSpace(c.TriagePassProvider)); p != "" {
		return p
	}
	return mainProvider(c)
}

// defaultFirstLookModel is the first-look model when
// PIPEROOM_TRIAGE_PASS_MODEL is unset: claude-haiku-5-5 on a Claude provider
// (and on an unknown one, which start refuses anyway), as before V0; the
// main model on a new provider.
func defaultFirstLookModel(provider, mainModel string) string {
	if knownProvider(provider) && !claudeProvider(provider) {
		return mainModel
	}
	return DefaultTriagePassModel
}

// BaseURL is a validated model endpoint base URL (ParseBaseURL).
type BaseURL struct {
	// URL is the parsed URL: scheme, host and path only.
	URL *url.URL
	// Host is the lower-case host name or address, without port or brackets.
	Host string
	// PrivateHTTP is true for plain http to a host that is not loopback,
	// allowed only by PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP=on. The adapter's
	// dialer must then refuse every dialed address that is not RFC 1918, ULA
	// or loopback (ModelPrivateHTTPAddr), since a name is resolved at dial time.
	PrivateHTTP bool
}

// String returns the whole URL. ParseBaseURL refuses userinfo, a query and a
// fragment, but the path is kept and some gateways put an account id or a
// token in it, so logs, errors and API responses use Redacted.
func (b BaseURL) String() string {
	if b.URL == nil {
		return ""
	}
	return b.URL.String()
}

// Redacted returns scheme://host[:port] only, the form that may be logged or
// shown.
func (b BaseURL) Redacted() string {
	if b.URL == nil {
		return ""
	}
	return b.URL.Scheme + "://" + b.URL.Host
}

// metadataHostNames are refused by name before any lookup.
var metadataHostNames = map[string]bool{
	"metadata.google.internal": true,
	"metadata":                 true,
}

var (
	awsIMDSv6     = netip.MustParseAddr("fd00:ec2::254")
	alibabaMeta   = netip.MustParseAddr("100.100.100.200")
	ipv4Broadcast = netip.MustParseAddr("255.255.255.255")
	thisNetwork   = netip.MustParsePrefix("0.0.0.0/8")
	nat64Prefix   = netip.MustParsePrefix("64:ff9b::/96")
	// IPv6 forms that carry an IPv4 address a host with the matching
	// translation or tunnel reaches: IPv4-compatible (::/96, deprecated),
	// local-use NAT64 (RFC 8215) and 6to4 (2002::/16, the IPv4 address in
	// bits 16-47).
	ipv4CompatPrefix = netip.MustParsePrefix("::/96")
	nat64LocalPrefix = netip.MustParsePrefix("64:ff9b:1::/48")
	sixToFourPrefix  = netip.MustParsePrefix("2002::/16")
	ulaPrefix        = netip.MustParsePrefix("fc00::/7")
	rfc1918Prefixs   = []netip.Prefix{
		netip.MustParsePrefix("10.0.0.0/8"),
		netip.MustParsePrefix("172.16.0.0/12"),
		netip.MustParsePrefix("192.168.0.0/16"),
	}
)

// ModelAddrDenied reports whether a model endpoint may never be reached at
// addr: link-local (169.254.0.0/16 and fe80::/10, every cloud's metadata
// service and ECS task metadata), AWS IMDS over IPv6 (fd00:ec2::254), Alibaba
// Cloud metadata (100.100.100.200), multicast, broadcast, unspecified and
// 0.0.0.0/8; IPv4-mapped, IPv4-compatible, NAT64 (well-known and local-use)
// and 6to4 forms are checked as the IPv4 address they carry, and a local-use
// NAT64 or 6to4 address whose IPv4 address is denied is denied whatever its
// other bits. It is the metadata deny list of design 10.3: no setting changes
// it, and the adapters' dialers apply it to every dialed address.
func ModelAddrDenied(addr netip.Addr) bool {
	addr = addr.Unmap().WithZone("")
	if addr.Is6() {
		b := addr.As16()
		switch {
		case nat64Prefix.Contains(addr), nat64LocalPrefix.Contains(addr):
			addr = netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]})
		case sixToFourPrefix.Contains(addr):
			addr = netip.AddrFrom4([4]byte{b[2], b[3], b[4], b[5]})
		case ipv4CompatPrefix.Contains(addr) && !addr.IsLoopback() && !addr.IsUnspecified():
			addr = netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]})
		}
	}
	switch {
	case !addr.IsValid(),
		addr.IsLinkLocalUnicast(), addr.IsLinkLocalMulticast(), addr.IsMulticast(), addr.IsInterfaceLocalMulticast(),
		addr.IsUnspecified(),
		addr == awsIMDSv6, addr == alibabaMeta, addr == ipv4Broadcast,
		addr.Is4() && thisNetwork.Contains(addr):
		return true
	}
	return false
}

// ModelPrivateHTTPAddr reports whether plain http may reach addr under
// PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP=on: loopback, RFC 1918 or ULA, and never
// an address ModelAddrDenied refuses.
func ModelPrivateHTTPAddr(addr netip.Addr) bool {
	addr = addr.Unmap().WithZone("")
	if ModelAddrDenied(addr) {
		return false
	}
	if addr.IsLoopback() || (addr.Is6() && ulaPrefix.Contains(addr)) {
		return true
	}
	for _, p := range rfc1918Prefixs {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// numericHost reports a host that a resolver may read as an IPv4 address in a
// non-canonical form (2852039166, 0xA9FEA9FE, 0251.0376.0251.0376): its last
// label is all digits or 0x-hex. A real top-level domain never is.
func numericHost(h string) bool {
	last := h[strings.LastIndexByte(h, '.')+1:]
	if last == "" {
		return false
	}
	if strings.HasPrefix(last, "0x") {
		return true
	}
	for _, r := range last {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ParseBaseURL validates a model endpoint base URL taken from env var name
// (design 10.3). https is required; plain http only to localhost, 127.0.0.0/8
// or ::1, or, with allowPrivateHTTP (PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP=on), to
// an RFC 1918 or ULA address or a host name (the dialer then checks what it
// resolves to, BaseURL.PrivateHTTP). User info, a query and a fragment are
// refused; the path is kept. The metadata deny list (ModelAddrDenied, and the
// names metadata.google.internal and metadata) applies always, whatever the
// scheme or knob. The error names the env var and the reason, never the value,
// which an operator may have pasted with a key in it.
func ParseBaseURL(name, v string, allowPrivateHTTP bool) (BaseURL, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return BaseURL{}, fmt.Errorf("config: %s: empty", name)
	}
	if strings.ContainsAny(v, "?#") {
		return BaseURL{}, fmt.Errorf("config: %s: must not carry a query or a fragment", name)
	}
	u, err := url.Parse(v)
	if err != nil || u.Opaque != "" {
		return BaseURL{}, fmt.Errorf("config: %s: want an https://host[/path] URL", name)
	}
	if u.User != nil || strings.Contains(u.Host, "@") {
		return BaseURL{}, fmt.Errorf("config: %s: must not carry user info", name)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "https" && scheme != "http" {
		return BaseURL{}, fmt.Errorf("config: %s: want an https://host[/path] URL", name)
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "" {
		return BaseURL{}, fmt.Errorf("config: %s: want an https://host[/path] URL", name)
	}
	if p := u.Port(); p != "" {
		if n, err := strconv.Atoi(p); err != nil || n < 1 || n > 65535 {
			return BaseURL{}, fmt.Errorf("config: %s: bad port", name)
		}
	}
	if metadataHostNames[host] {
		return BaseURL{}, fmt.Errorf("config: %s: a cloud metadata host is never a model endpoint", name)
	}
	addr, aerr := netip.ParseAddr(host)
	isIP := aerr == nil
	if !isIP && numericHost(host) {
		return BaseURL{}, fmt.Errorf("config: %s: write an IPv4 address in dotted decimal", name)
	}
	if isIP && addr.Zone() != "" {
		return BaseURL{}, fmt.Errorf("config: %s: an IPv6 zone is not allowed", name)
	}
	if isIP && ModelAddrDenied(addr) {
		return BaseURL{}, fmt.Errorf("config: %s: a link-local, metadata, multicast, broadcast or unspecified address is never a model endpoint", name)
	}
	out := BaseURL{URL: &url.URL{Scheme: scheme, Host: u.Host, Path: u.Path, RawPath: u.RawPath}, Host: host}
	if scheme == "http" {
		loopback := host == "localhost" || (isIP && addr.Unmap().IsLoopback())
		switch {
		case loopback:
		case !allowPrivateHTTP:
			return BaseURL{}, fmt.Errorf("config: %s: plain http only to localhost, 127.0.0.0/8 or ::1 (PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP=on also allows RFC 1918 and ULA hosts); use https", name)
		case isIP && !ModelPrivateHTTPAddr(addr):
			return BaseURL{}, fmt.Errorf("config: %s: plain http only to loopback, RFC 1918 or ULA addresses; use https", name)
		default:
			out.PrivateHTTP = true
		}
	}
	return out, nil
}

// ModelRole is the endpoint and model of one model role.
type ModelRole struct {
	Provider string // a ModelProvider* value
	Model    string // the model id as the provider names it
	// Flavor is the OpenAI-compatible flavor when Provider is openai
	// (PIPEROOM_MODEL_FLAVOR, shared by every openai role), else "".
	Flavor string
	// BaseURL is the endpoint override; nil = the provider's or flavor's
	// default (FlavorDefaultBaseURL) or, on a Claude provider, the SDK's.
	BaseURL *BaseURL
}

// ModelRoles is every model role (design section 9).
type ModelRoles struct {
	Main ModelRole
	// Triage is PIPEROOM_TRIAGE_MODEL, on Main's provider and endpoint (the
	// triage turns share the investigation's history); "" = always Main.Model.
	Triage    string
	FirstLook ModelRole // PIPEROOM_TRIAGE_PASS_PROVIDER / _MODEL / _BASE_URL
	Summary   ModelRole // PIPEROOM_SUMMARY_PROVIDER / _MODEL / _BASE_URL
	Judge     ModelRole // PIPEROOM_JUDGE_PROVIDER / _MODEL / _BASE_URL
}

// FlavorDefaultBaseURL returns the default base URL of an OpenAI-compatible
// flavor, "" when the flavor has none (azure, generic) or is unknown.
func FlavorDefaultBaseURL(flavor string) string { return modelFlavors[flavor] }

// ParseModelRoles validates the provider and model of every role. Each role
// falls back to the main provider and model; the first look's model defaults
// to claude-haiku-5-5 on a Claude provider and to the main model on a new
// one; a role on a provider other than the main one must name its model
// (except a first look on a Claude provider). PIPEROOM_MODEL_FLAVOR is for
// provider openai only (default openai); PIPEROOM_MODEL_BASE_URL and
// PIPEROOM_<ROLE>_BASE_URL for openai and gemini only, and a role's base URL
// only when its provider differs from the main one (otherwise it shares the
// main endpoint). azure and generic need a base URL. A Claude deployment that
// sets none of the V0 variables parses to exactly today's values. It never
// reads a key and never makes a network call; the error names the env var.
func ParseModelRoles(c Config) (ModelRoles, error) {
	allowPrivate, err := parseOnOff("PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP", c.ModelAllowPrivateHTTP)
	if err != nil {
		return ModelRoles{}, err
	}
	var r ModelRoles
	main := mainProvider(c)
	if !knownProvider(main) {
		return ModelRoles{}, fmt.Errorf("config: PIPEROOM_MODEL_PROVIDER=%q: want anthropic, bedrock, vertex, openai, gemini or bedrock-converse", c.ModelProvider)
	}
	r.Main.Provider = main
	if r.Main.Model, err = modelID("PIPEROOM_MODEL", c.Model, DefaultModel); err != nil {
		return ModelRoles{}, err
	}
	flavor := strings.ToLower(strings.TrimSpace(c.ModelFlavor))
	if flavor != "" {
		if _, ok := modelFlavors[flavor]; !ok {
			return ModelRoles{}, fmt.Errorf("config: PIPEROOM_MODEL_FLAVOR=%q: want one of openai, azure, deepseek, qwen, mistral, groq, together, openrouter, xai, ollama, vllm, generic", c.ModelFlavor)
		}
	}
	if r.Main.BaseURL, err = roleBaseURL("PIPEROOM_MODEL_BASE_URL", c.ModelBaseURL, main, allowPrivate); err != nil {
		return ModelRoles{}, err
	}
	if r.Main.Flavor, err = roleFlavor("PIPEROOM_MODEL_PROVIDER", main, flavor, r.Main.BaseURL); err != nil {
		return ModelRoles{}, err
	}
	if flavor != "" && main != ModelProviderOpenAI && !anyRoleOn(c, ModelProviderOpenAI) {
		return ModelRoles{}, fmt.Errorf("config: PIPEROOM_MODEL_FLAVOR is for provider openai only")
	}
	if r.Triage, err = modelID("PIPEROOM_TRIAGE_MODEL", c.TriageModel, ""); err != nil {
		return ModelRoles{}, err
	}
	for _, role := range []struct {
		dst                      *ModelRole
		prefix                   string
		provider, model, baseURL string
		firstLook                bool
	}{
		{&r.FirstLook, "PIPEROOM_TRIAGE_PASS", c.TriagePassProvider, c.TriagePassModel, c.TriagePassBaseURL, true},
		{&r.Summary, "PIPEROOM_SUMMARY", c.SummaryProvider, c.SummaryModel, c.SummaryBaseURL, false},
		{&r.Judge, "PIPEROOM_JUDGE", c.JudgeProvider, c.JudgeModel, c.JudgeBaseURL, false},
	} {
		p := strings.ToLower(strings.TrimSpace(role.provider))
		if p == "" {
			p = main
		}
		if !knownProvider(p) {
			return ModelRoles{}, fmt.Errorf("config: %s_PROVIDER=%q: want anthropic, bedrock, vertex, openai, gemini or bedrock-converse", role.prefix, role.provider)
		}
		def := r.Main.Model
		switch {
		case role.firstLook:
			def = defaultFirstLookModel(p, r.Main.Model)
			if p != main && !claudeProvider(p) {
				def = ""
			}
		case p != main:
			def = ""
		}
		m, err := modelID(role.prefix+"_MODEL", role.model, def)
		if err != nil {
			return ModelRoles{}, err
		}
		if m == "" {
			return ModelRoles{}, fmt.Errorf("config: %s_MODEL is required when %s_PROVIDER (%s) differs from PIPEROOM_MODEL_PROVIDER (%s)", role.prefix, role.prefix, p, main)
		}
		out := ModelRole{Provider: p, Model: m}
		if p == main {
			if strings.TrimSpace(role.baseURL) != "" {
				return ModelRoles{}, fmt.Errorf("config: %s_BASE_URL is only for a role whose provider differs from PIPEROOM_MODEL_PROVIDER; this role uses the main endpoint", role.prefix)
			}
			out.Flavor, out.BaseURL = r.Main.Flavor, r.Main.BaseURL
		} else {
			if out.BaseURL, err = roleBaseURL(role.prefix+"_BASE_URL", role.baseURL, p, allowPrivate); err != nil {
				return ModelRoles{}, err
			}
			if out.Flavor, err = roleFlavor(role.prefix+"_PROVIDER", p, flavor, out.BaseURL); err != nil {
				return ModelRoles{}, err
			}
		}
		*role.dst = out
	}
	return r, nil
}

// anyRoleOn reports whether a single-shot role names provider p.
func anyRoleOn(c Config, p string) bool {
	for _, v := range []string{c.TriagePassProvider, c.SummaryProvider, c.JudgeProvider} {
		if strings.ToLower(strings.TrimSpace(v)) == p {
			return true
		}
	}
	return false
}

// modelID trims v, applies def when empty and refuses whitespace inside.
func modelID(name, v, def string) (string, error) {
	m := strings.TrimSpace(v)
	if m == "" {
		m = def
	}
	if strings.ContainsAny(m, " \t\r\n") {
		return "", fmt.Errorf("config: %s=%q: want a model id without spaces", name, v)
	}
	return m, nil
}

// roleBaseURL validates a base URL for a role on provider p; "" = default.
func roleBaseURL(name, v, p string, allowPrivate bool) (*BaseURL, error) {
	if strings.TrimSpace(v) == "" {
		return nil, nil
	}
	if p != ModelProviderOpenAI && p != ModelProviderGemini {
		return nil, fmt.Errorf("config: %s is for providers openai and gemini only (this role's provider is %s)", name, p)
	}
	b, err := ParseBaseURL(name, v, allowPrivate)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

// roleFlavor returns the flavor of a role on provider p: openai only,
// default openai; azure and generic need a base URL.
func roleFlavor(providerVar, p, flavor string, base *BaseURL) (string, error) {
	if p != ModelProviderOpenAI {
		return "", nil
	}
	if flavor == "" {
		flavor = "openai"
	}
	if base == nil && modelFlavors[flavor] == "" {
		return "", fmt.Errorf("config: %s=openai with PIPEROOM_MODEL_FLAVOR=%s needs a base URL (PIPEROOM_MODEL_BASE_URL or the role's _BASE_URL)", providerVar, flavor)
	}
	return flavor, nil
}

// PIPEROOM_MODEL_PRICING_STRICT values (design 8.2, decision 4).
const (
	PricingStrictAll       = "all"
	PricingStrictNonClaude = "non-claude"
	PricingStrictOff       = "off"
)

// PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS values (design 10.1, decision 6).
// There is no off: the untrusted-data wrapper always applies.
const (
	NeutraliseAll       = "all"
	NeutraliseNonClaude = "non-claude"
)

// PIPEROOM_GEMINI_BACKEND values.
const (
	GeminiBackendAPI    = "api"
	GeminiBackendVertex = "vertex"
)

// MaxModelContextTokens bounds PIPEROOM_MODEL_CONTEXT_TOKENS (a typo guard;
// no model today has more than a few million).
const MaxModelContextTokens = 100_000_000

// modelCaps are the PIPEROOM_MODEL_CAPS values: each turns a capability on,
// none can lower a check.
var modelCaps = map[string]bool{"parallel_tools": true, "structured_output": true, "reasoning": true}

// ModelKnobs holds the typed model settings other than the roles.
type ModelKnobs struct {
	Roles                   ModelRoles
	ContextTokens           int64    // 0 = the adapter's table, else 128000
	Caps                    []string // sorted as given, deduplicated
	Unverified              bool
	MaxConcurrency          int64 // 0 = PIPEROOM_MAX_AGENTS
	TPM                     int64 // 0 = no budget
	Proxy                   bool
	GeminiBackend           string // GeminiBackend*
	AllowPrivateHTTP        bool
	AckCNEndpoint           bool
	PricingStrict           string // PricingStrict*
	NeutraliseControlTokens string // Neutralise*
	PostmortemMode          string // PostmortemModelAuto | PostmortemModelOff
	// Warnings are start-up notes for the caller to log once at Warn (a
	// deprecated variable name); never a reason to refuse start.
	Warnings []string
}

// ParseModelKnobs validates the model settings of design section 9 and the
// owner rulings of section 15, after ParseModelRoles (whose result it
// carries). It refuses start on: PIPEROOM_MODEL_PRICING_STRICT=off with
// PIPEROOM_PIPELINE_DAILY_COST_USD set; a PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS
// other than all or non-claude; any PIPEROOM_MODEL_PROMPT_VARIANT (reserved,
// not implemented); an endpoint operated in the People's Republic of China
// without PIPEROOM_MODEL_ACK_CN_ENDPOINT=yes; PIPEROOM_POSTMORTEM_MODE and its
// deprecated alias PIPEROOM_POSTMORTEM_MODEL set to different values. No knob
// widens the metadata deny list (design 15.1). The error names the env var.
func ParseModelKnobs(c Config) (ModelKnobs, error) {
	var k ModelKnobs
	var err error
	if c.ModelPromptVariant != "" {
		return ModelKnobs{}, fmt.Errorf("config: PIPEROOM_MODEL_PROMPT_VARIANT is reserved, not implemented: unset it (one prompt for every model in this release)")
	}
	if k.Roles, err = ParseModelRoles(c); err != nil {
		return ModelKnobs{}, err
	}
	if k.AllowPrivateHTTP, err = parseOnOff("PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP", c.ModelAllowPrivateHTTP); err != nil {
		return ModelKnobs{}, err
	}
	if k.Unverified, err = parseOnOff("PIPEROOM_MODEL_UNVERIFIED", c.ModelUnverified); err != nil {
		return ModelKnobs{}, err
	}
	if k.Proxy, err = parseOnOff("PIPEROOM_MODEL_PROXY", c.ModelProxy); err != nil {
		return ModelKnobs{}, err
	}
	if strings.TrimSpace(c.ModelContextTokens) != "" {
		if k.ContextTokens, err = parseWholeNumber("PIPEROOM_MODEL_CONTEXT_TOKENS", c.ModelContextTokens, "", false); err != nil {
			return ModelKnobs{}, err
		}
		if k.ContextTokens > MaxModelContextTokens {
			return ModelKnobs{}, fmt.Errorf("config: PIPEROOM_MODEL_CONTEXT_TOKENS=%q: at most %d", c.ModelContextTokens, MaxModelContextTokens)
		}
	}
	if strings.TrimSpace(c.ModelMaxConcurrency) != "" {
		if k.MaxConcurrency, err = parseWholeNumber("PIPEROOM_MODEL_MAX_CONCURRENCY", c.ModelMaxConcurrency, "", false); err != nil {
			return ModelKnobs{}, err
		}
		if k.MaxConcurrency > 10000 {
			return ModelKnobs{}, fmt.Errorf("config: PIPEROOM_MODEL_MAX_CONCURRENCY=%q: at most 10000", c.ModelMaxConcurrency)
		}
	}
	if strings.TrimSpace(c.ModelTPM) != "" {
		if k.TPM, err = parseWholeNumber("PIPEROOM_MODEL_TPM", c.ModelTPM, "", false); err != nil {
			return ModelKnobs{}, err
		}
	}
	seen := map[string]bool{}
	for _, cp := range splitList(strings.ToLower(c.ModelCaps)) {
		if !modelCaps[cp] {
			return ModelKnobs{}, fmt.Errorf("config: PIPEROOM_MODEL_CAPS: %q: want parallel_tools, structured_output or reasoning (a capability can only be turned on)", cp)
		}
		if !seen[cp] {
			seen[cp] = true
			k.Caps = append(k.Caps, cp)
		}
	}
	switch v := strings.ToLower(strings.TrimSpace(c.GeminiBackend)); v {
	case "", GeminiBackendAPI:
		k.GeminiBackend = GeminiBackendAPI
	case GeminiBackendVertex:
		k.GeminiBackend = GeminiBackendVertex
	default:
		return ModelKnobs{}, fmt.Errorf("config: PIPEROOM_GEMINI_BACKEND=%q: want api or vertex", c.GeminiBackend)
	}
	switch v := strings.ToLower(strings.TrimSpace(c.ModelPricingStrict)); v {
	case "", PricingStrictNonClaude:
		k.PricingStrict = PricingStrictNonClaude
	case PricingStrictAll:
		k.PricingStrict = PricingStrictAll
	case PricingStrictOff:
		capUSD, err := parseUSD("PIPEROOM_PIPELINE_DAILY_COST_USD", c.PipelineDailyCostUSD)
		if err != nil {
			return ModelKnobs{}, err
		}
		if capUSD > 0 {
			return ModelKnobs{}, fmt.Errorf("config: PIPEROOM_MODEL_PRICING_STRICT=off with PIPEROOM_PIPELINE_DAILY_COST_USD set: a cost cap would count an unpriced model as free; use non-claude or all, or unset the cap")
		}
		k.PricingStrict = PricingStrictOff
	default:
		return ModelKnobs{}, fmt.Errorf("config: PIPEROOM_MODEL_PRICING_STRICT=%q: want all, non-claude or off", c.ModelPricingStrict)
	}
	switch v := strings.ToLower(strings.TrimSpace(c.ModelNeutraliseControlTokens)); v {
	case "", NeutraliseAll:
		k.NeutraliseControlTokens = NeutraliseAll
	case NeutraliseNonClaude:
		k.NeutraliseControlTokens = NeutraliseNonClaude
	default:
		return ModelKnobs{}, fmt.Errorf("config: PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS=%q: want all or non-claude (there is no off)", c.ModelNeutraliseControlTokens)
	}
	switch v := strings.ToLower(strings.TrimSpace(c.ModelAckCNEndpoint)); v {
	case "":
	case "yes":
		k.AckCNEndpoint = true
	default:
		return ModelKnobs{}, fmt.Errorf("config: PIPEROOM_MODEL_ACK_CN_ENDPOINT=%q: want yes or unset", c.ModelAckCNEndpoint)
	}
	if !k.AckCNEndpoint {
		for _, role := range []struct {
			name string
			r    ModelRole
		}{{"PIPEROOM_MODEL", k.Roles.Main}, {"PIPEROOM_TRIAGE_PASS", k.Roles.FirstLook}, {"PIPEROOM_SUMMARY", k.Roles.Summary}, {"PIPEROOM_JUDGE", k.Roles.Judge}} {
			if host := ChinaOperatedEndpoint(role.r); host != "" {
				return ModelKnobs{}, fmt.Errorf("config: %s: endpoint %s is operated in the People's Republic of China: set PIPEROOM_MODEL_ACK_CN_ENDPOINT=yes to acknowledge where incident data goes", role.name, host)
			}
		}
	}
	var deprecated bool
	if k.PostmortemMode, deprecated, err = parsePostmortemMode(c); err != nil {
		return ModelKnobs{}, err
	}
	if deprecated {
		k.Warnings = append(k.Warnings, "PIPEROOM_POSTMORTEM_MODEL is deprecated: rename it to PIPEROOM_POSTMORTEM_MODE (same values, auto or off)")
	}
	return k, nil
}

// ChinaOperatedEndpoint returns the host of r's endpoint when it is operated
// in the People's Republic of China (DeepSeek's API, the DashScope China
// endpoint, other PRC-operated model APIs on non-.cn domains, any .cn host),
// else "". The list is best effort: an IP literal or a provider it does not
// name passes, so the acknowledgement is a reminder, not a compliance
// control. Only openai and gemini roles have a configurable endpoint; the
// deepseek flavor's default host counts.
func ChinaOperatedEndpoint(r ModelRole) string {
	host := ""
	switch {
	case r.BaseURL != nil:
		host = r.BaseURL.Host
	case r.Provider == ModelProviderOpenAI:
		if u, err := url.Parse(modelFlavors[r.Flavor]); err == nil {
			host = strings.ToLower(u.Hostname())
		}
	}
	if host == "" {
		return ""
	}
	for _, d := range []string{"deepseek.com", "dashscope.aliyuncs.com", "moonshot.ai", "minimax.chat", "minimaxi.com", "volces.com", "baichuan-ai.com", "tencentcloudapi.com"} {
		if host == d || strings.HasSuffix(host, "."+d) {
			return host
		}
	}
	if strings.HasSuffix(host, ".cn") {
		return host
	}
	return ""
}

// parsePostmortemMode reads PIPEROOM_POSTMORTEM_MODE first and the
// deprecated PIPEROOM_POSTMORTEM_MODEL second; deprecated is true when only
// the old name carried the value. Both set to different values refuse.
func parsePostmortemMode(c Config) (mode string, deprecated bool, err error) {
	norm := func(name, raw string) (string, error) {
		switch v := strings.ToLower(strings.TrimSpace(raw)); v {
		case "":
			return "", nil
		case PostmortemModelAuto, PostmortemModelOff:
			return v, nil
		default:
			return "", fmt.Errorf("config: %s=%q: want auto or off", name, raw)
		}
	}
	newV, err := norm("PIPEROOM_POSTMORTEM_MODE", c.PostmortemMode)
	if err != nil {
		return "", false, err
	}
	oldV, err := norm("PIPEROOM_POSTMORTEM_MODEL", c.PostmortemModel)
	if err != nil {
		return "", false, err
	}
	switch {
	case newV != "" && oldV != "" && newV != oldV:
		return "", false, fmt.Errorf("config: PIPEROOM_POSTMORTEM_MODE=%q and the deprecated PIPEROOM_POSTMORTEM_MODEL=%q disagree: unset PIPEROOM_POSTMORTEM_MODEL", c.PostmortemMode, c.PostmortemModel)
	case newV != "":
		return newV, oldV != "", nil
	case oldV != "":
		return oldV, true, nil
	}
	return PostmortemModelAuto, false, nil
}

// parseOnOff parses on | off (any case); empty is off.
func parseOnOff(name, v string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "off":
		return false, nil
	case "on":
		return true, nil
	}
	return false, fmt.Errorf("config: %s=%q: want on or off", name, v)
}

// ---- AU wave (AU1) ----

// ParseAutonomy validates PIPEROOM_AUTONOMY and reports whether earned
// autonomy (events.PolicyEarned) may run: "on" (the default, also when
// empty) or "off", any case; anything else is an error naming the variable,
// so the server refuses to start. With write mode off it is false whatever
// the variable says (no autonomy of any kind without writes); a bad
// PIPEROOM_WRITE_MODE is that error. "off" disables the whole wave: no
// autonomy.granted is written and no policy-earned proposal is scheduled.
func ParseAutonomy(c Config) (bool, error) {
	var on bool
	switch v := strings.ToLower(strings.TrimSpace(c.Autonomy)); v {
	case "", "on":
		on = true
	case "off":
	default:
		return false, fmt.Errorf("config: PIPEROOM_AUTONOMY=%q: want on or off", c.Autonomy)
	}
	write, err := ParseWriteMode(c)
	if err != nil {
		return false, err
	}
	return on && write, nil
}

// Connect backend kinds (CONNECT_KIND). Confluent and Aiven clients get their
// credentials per pipeline from owners.yaml connections: (docs/design/tenancy.md
// §3.2); there are no server-wide CONFLUENT_* variables.
const (
	DefaultConnectKind   = "rest"
	ConnectKindREST      = "rest"
	ConnectKindMSK       = "msk"
	ConnectKindConfluent = "confluent"
	ConnectKindAiven     = "aiven"
)

// ParseConnectKind validates CONNECT_KIND and the settings that go with it.
// Empty means rest. "msk" talks to the AWS MSK Connect API with the default
// AWS credential chain in AWS_REGION instead of the Connect REST API, so the
// REST login variables must be empty: a value there means the operator thinks
// they are used. The error names the env var. "confluent" and "aiven" take
// their endpoint and keys per pipeline, so there is nothing to check here.
func ParseConnectKind(c Config) (string, error) {
	kind := strings.ToLower(strings.TrimSpace(c.ConnectKind))
	if kind == "" {
		kind = DefaultConnectKind
	}
	switch kind {
	case ConnectKindREST, ConnectKindConfluent, ConnectKindAiven:
		return kind, nil
	case ConnectKindMSK:
		if strings.TrimSpace(c.AWSRegion) == "" {
			return "", fmt.Errorf("config: CONNECT_KIND=msk needs AWS_REGION")
		}
		restVars := []struct{ name, value string }{
			{"CONNECT_USERNAME", c.ConnectUsername},
			{"CONNECT_PASSWORD", c.ConnectPassword},
			{"CONNECT_TLS_CA_FILE", c.ConnectTLSCAFile},
			{"CONNECT_TLS_CERT_FILE", c.ConnectTLSCertFile},
			{"CONNECT_TLS_KEY_FILE", c.ConnectTLSKeyFile},
		}
		for _, v := range restVars {
			if v.value != "" {
				return "", fmt.Errorf("config: %s is set but CONNECT_KIND=msk uses the AWS SDK, not the Connect REST API; unset it", v.name)
			}
		}
		return kind, nil
	default:
		return "", fmt.Errorf("config: CONNECT_KIND=%q: want rest, msk, confluent or aiven", c.ConnectKind)
	}
}
