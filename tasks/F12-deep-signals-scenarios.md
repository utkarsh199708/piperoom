# F12 — Deep signals and new scenarios

Owners: F12a A2 (`internal/clients/kafka/**`, `internal/clients/netsec/**`, `internal/tools/kafkatools/**`) ·
F12b A3 (`internal/clients/postgres/**`, `internal/clients/warehouse/**`, `internal/tools/dbtools/**`, new
`internal/clients/prom/**` and `internal/tools/debeziumtools/**` → A3) · F12c C7 (`evals/scenarios/*.yaml`,
`evals/scenarios/README.md`) · F12d C6 (`evals/**`) + C1 (`.github/**`) · contract additions by the lead (F12-0;
optional client interfaces, config vars, `docs/pilot/data-handling.md` rows).
Model: **Opus 5.5** for F12a/F12b (new client code against real APIs; every call must be read-only, bounded and
verified with `go doc`); **Sonnet 5.5** for F12c/F12d (format work against fixed tool output).
Base: `main` after F0 and the R wave. Read `AGENTS.md` §4 rule 9 (**no invented APIs**), `docs/pilot/permissions.md`
(Kafka ACL table, lines 140-160), `evals/README.md` (scenario format, phases), `evals/scenarios/README.md`.

## Goal
Half of the incidents an on-call sees are diagnosable only by inference today: a stuck snapshot, a Snowflake load
error, a rotated SASL credential, a broker on a full disk (platform review §2 rows 6, 8, 12, 13). Seven small read
tools give the agent sight of those layers, four scenarios grade it on them, and a checked-in baseline turns
"the evals pass" into a number a pilot can be told.

## Evidence
- 21 read tools, none for broker health, partition throughput, ACLs, cert expiry, warehouse load errors,
  Debezium snapshot metrics or a counts reconciliation (`agent-platform-review.md` §1 "Read tools" row, §2).
- `kafka_consumer_lag` is point-in-time (`internal/tools/kafkatools/kafka_tools.go:168`); skew and rate are
  invisible (§2 row 10). `kafka_topic_config` reads `min.insync.replicas` but nothing reads ISR (row 13).
- `warehouse_freshness` is `SELECT max(ts)` (`internal/clients/snowflake/snowflake.go:67`); a sink task FAILED on
  `invalid identifier RISK_SCORE` leaves no trace a tool can read (row 8).
- No report is checked in; CI runs `evalrun -validate` only (`.github/workflows/ci.yml:51-52`); "N of M pass"
  cannot be stated (gap 16). 20 default scenarios all on one pipeline shape (gap 17).

## Scope — what to build
1. **F12-0 contract** (lead): optional reader interfaces in `clients.go`, `DEBEZIUM_METRICS_URL` +
   `DEBEZIUM_METRICS_PREFIX` in `config.go`, data-handling rows. §Contract.
2. **F12a** (A2): `kafka_cluster_health`, `kafka_partition_rates`, `kafka_acls`, `tls_cert_expiry` as KindRead tools
   with the client methods behind them; `permissions.md` grants listed in the PR.
3. **F12b** (A3): `warehouse_load_errors`, `reconcile`, `debezium_metrics`; a Prometheus text-format scraper with
   no new dependency.
4. **F12c** (C7): `snapshot-stuck`, `snowflake-sink-column-missing`, `sasl-credential-rotated`,
   `broker-disk-notenoughreplicas` scenarios with recordings in the exact tool formats, each with a trap.
5. **F12d** (C6 + C1): `evals/baselines/<date>-<label>.json|.md`, `evalrun -baseline-dir`, a manual CI job that runs
   the model and compares against the newest baseline.

## Out of scope
`logs_search` (own design), `pg_role_grants`, CloudWatch `FreeStorageSpace`, BigQuery/Databricks, new write
actions (`heartbeat.interval.ms` allow-list is its own A2 PR), watcher signals on the new reads (`cert_expiry`,
`under_min_isr`: after F6), Kafka-side `connect_offsets` (Connect 3.5 `GET /connectors/{n}/offsets`: A2 follow-up).

## Contract (F12-0 — exact diffs)
`internal/clients/clients.go` — optional on a `KafkaAdmin` (asserted at use time like `DLQReader`; fakes and the
eval replay need not implement them):
```go
// ---- Kafka: cluster and partition reads (optional on a KafkaAdmin) ----

// BrokerHealth is one broker from a metadata response plus its log dirs.
type BrokerHealth struct {
	NodeID       int32  `json:"node_id"`
	Host         string `json:"host"`
	Port         int32  `json:"port"`
	IsController bool   `json:"is_controller"`
	// LogDirTotalBytes/UsableBytes sum the broker's log directories; -1 when
	// the broker does not report them (Kafka < 3.3 or remote storage).
	LogDirTotalBytes  int64 `json:"log_dir_total_bytes"`
	LogDirUsableBytes int64 `json:"log_dir_usable_bytes"`
	LogDirErr         string `json:"log_dir_err,omitempty"`
}

// PartitionHealth is one partition of a pipeline topic from metadata.
type PartitionHealth struct {
	Topic           string  `json:"topic"`
	Partition       int32   `json:"partition"`
	Leader          int32   `json:"leader"` // -1 = no leader
	Replicas        []int32 `json:"replicas"`
	ISR             []int32 `json:"isr"`
	OfflineReplicas []int32 `json:"offline_replicas"`
	MinISR          int     `json:"min_isr"` // from min.insync.replicas; 0 = unknown
	Reassigning     bool    `json:"reassigning"`
}

// ClusterHealth is what ClusterReader.ClusterHealth returns for a set of topics.
type ClusterHealth struct {
	ClusterID  string            `json:"cluster_id"`
	Controller int32             `json:"controller"`
	Brokers    []BrokerHealth    `json:"brokers"`
	Partitions []PartitionHealth `json:"partitions"`
}

// ClusterReader reads broker and partition health for the given topics only
// (never the whole cluster's topics). Needs Describe on Cluster and the
// topics; log dirs need DescribeConfigs on Cluster and are best-effort.
type ClusterReader interface {
	ClusterHealth(ctx context.Context, topics ...string) (ClusterHealth, error)
}

// PartitionRate is records written to one partition in a window.
type PartitionRate struct {
	Topic         string `json:"topic"`
	Partition     int32  `json:"partition"`
	End           int64  `json:"end"`
	OffsetAtStart int64  `json:"offset_at_start"` // first offset with timestamp >= window start; -1 = none
	Records       int64  `json:"records"`         // End - OffsetAtStart, or 0
}

// RateReader reads per-partition write counts over a window from offsets
// alone (ListOffsetsAfterMilli + ListEndOffsets); it fetches no record.
type RateReader interface {
	PartitionRates(ctx context.Context, since time.Time, topics ...string) ([]PartitionRate, error)
}

// ACLEntry is one described ACL.
type ACLEntry struct {
	Principal    string `json:"principal"`
	Host         string `json:"host"`
	ResourceType string `json:"resource_type"` // TOPIC | GROUP | CLUSTER
	Resource     string `json:"resource"`
	Pattern      string `json:"pattern"` // LITERAL | PREFIXED
	Operation    string `json:"operation"`
	Permission   string `json:"permission"` // ALLOW | DENY
}

// ACLReader lists ACLs that touch the given topics and groups (any
// principal, any host), plus cluster ACLs. Needs Describe on Cluster.
type ACLReader interface {
	ACLs(ctx context.Context, topics, groups []string) ([]ACLEntry, error)
}

// ---- Warehouse: load history (optional on a Warehouse) ----

// LoadError is one failed or partial load into a sink table.
type LoadError struct {
	Table      string    `json:"table"`
	At         time.Time `json:"at"`
	Status     string    `json:"status"` // LOADED | LOAD_FAILED | PARTIALLY_LOADED
	Rows       int64     `json:"rows"`
	Errors     int64     `json:"errors"`
	FirstError string    `json:"first_error"` // redacted, ≤ 300 runes; never a record value
	File       string    `json:"file,omitempty"` // file name only (no path, no URL)
}

// LoadErrorReader reads a sink table's load history. Snowflake implements it
// over INFORMATION_SCHEMA.COPY_HISTORY; the dev Postgres warehouse returns
// ErrNotSupported.
type LoadErrorReader interface {
	LoadErrors(ctx context.Context, table string, since time.Time, limit int) ([]LoadError, error)
}

// MinuteCount is rows per bucket for reconcile.
type MinuteCount struct {
	Bucket time.Time `json:"bucket"`
	Count  int64     `json:"count"`
}

// CountReader counts rows of table by tsColumn in buckets of width
// (optional on a SourceDB and a Warehouse). Counts only, never values.
type CountReader interface {
	CountByBucket(ctx context.Context, table, tsColumn string, since, until time.Time, width time.Duration) ([]MinuteCount, error)
}

// ErrNotSupported is returned by an optional read a client cannot do.
var ErrNotSupported = errors.New("clients: not supported by this client")
```
`internal/config/config.go`: `DebeziumMetricsURL string // DEBEZIUM_METRICS_URL: Prometheus text endpoint of the
JMX exporter (http(s)://host:port/metrics); empty = debezium_metrics says "not configured"` and
`DebeziumMetricsPrefix string // DEBEZIUM_METRICS_PREFIX: metric name prefix, default "debezium_metrics_"`.
`AGENTS.md` §2: rows `internal/clients/prom/**`, `internal/tools/debeziumtools/**` → A3. `docs/pilot/data-handling.md`:
one row per new tool (what leaves the system: counts, states, hostnames, cert subjects, masked error text).

## Ownership and files
| File | Owner | New/mod |
|---|---|---|
| `internal/clients/clients.go`, `internal/config/config.go`, `AGENTS.md`, `docs/pilot/{data-handling,permissions}.md` | lead | mod |
| `internal/clients/kafka/{cluster.go,rates.go,acls.go}` (+tests, integration tests); `internal/clients/netsec/certs.go` (`CertExpiry`); `internal/tools/kafkatools/{cluster_tools.go,rate_tools.go,acl_tools.go,cert_tools.go}` + tests; `kafkatools.Register` | A2 (F12a) | new/mod |
| `internal/clients/snowflake/loads.go`, `internal/clients/warehouse/{loads,counts}.go`, `internal/clients/postgres/counts.go`; `internal/tools/dbtools/{loads.go,reconcile.go}`; `internal/clients/prom/{scrape.go,parse.go}`; `internal/tools/debeziumtools/{tool.go,metrics.go}`; `internal/wiring/wiring.go` (one registration line each, A4 approves) | A3 (F12b) | new/mod |
| `evals/scenarios/{snapshot-stuck,snowflake-sink-column-missing,sasl-credential-rotated,broker-disk-notenoughreplicas}.yaml`, `evals/scenarios/README.md` | C7 (F12c) | new/mod |
| `evals/baselines/README.md`, `evals/baselines/2026-10-main.{json,md}`, `evals/cmd/evalrun/main.go` (`-baseline-dir`), `evals/compare.go` | C6 (F12d) | new/mod |
| `.github/workflows/evals.yml` | C1 (F12d) | new |

## Dependencies and merge order
F12-0 → F12a and F12b in parallel → F12c (recordings must name live tools: `-validate` rejects unknown tool names,
so the four files land **after** the tools they record; C7 may open the PR early with the new recordings commented
out and `evidence_tools` on existing tools, then finish) → F12d baseline run after F12c (the baseline must include
the new scenarios). F3's `Provenance` field, if merged first, is filled by every new client call (`kind: kafka`,
`sql`, `http`). No contract file is edited outside F12-0.

## Design details
### F12a — Kafka depth (A2). Verified with `go doc github.com/twmb/franz-go/pkg/kadm.<Symbol>` on kadm v1.19.0:
`Client.Metadata(ctx, topics...)` → `Metadata{Cluster, Controller, Brokers BrokerDetails(kgo.BrokerMetadata
{NodeID, Host, Port}), Topics TopicDetails}`; `PartitionDetail{Leader, Replicas, ISR, OfflineReplicas, Err}`;
`Client.DescribeAllLogDirs(ctx, TopicsSet)` → `DescribedAllLogDirs map[int32]DescribedLogDirs`,
`DescribedLogDir{Broker, Dir, TotalBytes, UsableBytes (-1 unreported), Err}`;
`Client.ListPartitionReassignments(ctx, TopicsSet)`; `Client.DescribeTopicConfigs(ctx, topics...)` → `ResourceConfigs`
(`Config{Key, Value *string}` → `min.insync.replicas`); `Client.ListOffsetsAfterMilli(ctx, millis, topics...)` and
`Client.ListEndOffsets(ctx, topics...)` → `ListedOffsets` (`ListedOffset{Offset (-1 none), Err}`);
`Client.DescribeACLs(ctx, *ACLBuilder)` → `DescribeACLsResults[].Described []DescribedACL{Principal, Host, Type,
Name, Pattern, Operation, Permission}`; builder `kadm.NewACLs().Topics(t...).Groups(g...).Clusters().
Operations(kadm.OpAny)` with `.Allow().AllowHosts()` and `.Deny().DenyHosts()` set **empty** (= any principal,
any host; `ValidateDescribe` must pass; **there is no `DescribeCluster`/`DescribeLogDirs` method: do not use them**).
- `ClusterHealth(ctx, topics...)`: one `Metadata(topics)`; `DescribeTopicConfigs(topics)` for `min.insync.replicas`;
  `DescribeAllLogDirs(nil-set)` best-effort (error → `LogDirErr` per broker, never a tool error);
  `ListPartitionReassignments(set of the topics)` best-effort. Topics only from `Scope.Topics()` (+ DLQ topics).
- `kafka_cluster_health {}`: summary `cluster abc (3 brokers, controller 2); broker 2 log dir 97% used (12.1 GB of
  400 GB usable)`; then per topic `pg.public.orders: 6 partitions, 2 under-replicated (p3 ISR [1] of [1,2,3]),
  1 under min ISR (min 2), 0 offline, 0 reassigning`; `Data` = `ClusterHealth`. Verdict line: `NotEnoughReplicas is
  expected for writes to p3` when ISR < MinISR. Hostnames are shown (they are the operator's own), never ACL
  principals of other pipelines.
- `PartitionRates(ctx, since, topics...)`: `ListOffsetsAfterMilli(since)` + `ListEndOffsets`; records = end − first.
  `kafka_partition_rates {table, window?: "5m" (1m..1h)}`: summary `pg.public.orders last 5m: 18 420 records
  (61/s); p3 11 002 (60%), p0 1 204, … median 1 310, max/median 8.4x → skewed`; the group's assignment is not read
  (member→partition lives in `DescribedGroup.Members[].Assigned`, left for a follow-up). No record is fetched.
- `kafka_acls {principal?}`: filters the described entries to the pipeline's topics (literal or a prefix that
  matches), groups and cluster; optional `principal` (`User:…`, ≤ 200 chars, `^[A-Za-z0-9:_.@/-]+$`) narrows.
  Summary `User:debezium: ALLOW READ topic pg.* (PREFIXED), ALLOW DESCRIBE cluster; no WRITE on pg.public.orders`
  with a verdict `missing: WRITE on topic pg.public.orders for the connector principal` only when a `principal` was
  given. A `ClusterAuthorizationFailed` from the broker → summary `cannot describe ACLs: the Piperoom principal
  lacks Describe on Cluster (docs/pilot/permissions.md)`, not an error status. Principals of other pipelines'
  resources are never returned (filter first, then render).
- `netsec.CertExpiry(ctx, addr string, cfg *tls.Config) (CertInfo, error)`: `tls.Dialer{Config: clone with
  InsecureSkipVerify: true (we read, we do not trust)}` with a 5 s timeout, `ConnectionState().PeerCertificates[0]`
  → `{Subject CN, Issuer CN, NotAfter, DNSNames (≤ 5), DaysLeft, Verified (a second check with the configured
  roots)}`; the handshake sends no credential (no client cert from the config: `Certificates: nil`). Targets are
  **only configured endpoints**: `KAFKA_BROKERS` hosts (when `KAFKA_SECURITY_PROTOCOL` uses TLS), `CONNECT_URL`,
  `SCHEMA_REGISTRY_URL` (https only), `WAREHOUSE_DB_URL` host for Snowflake (`<account>.snowflakecomputing.com:443`).
  `tls_cert_expiry {}`: summary one line per endpoint `broker kafka-1:9093 expires in 12 d (2026-10-26) issuer R3
  ✔ chain ok`; `expired 3 d ago` is the verdict; plain-text endpoints say `no TLS configured`.
Grants to document (`permissions.md`): `Describe` on Cluster (metadata, ACL describe), `DescribeConfigs` on Cluster
for log dirs (optional; the tool says `log dirs: not permitted` without it). Timeouts: every call under the tools'
10 s with `withTimeout`; `ClusterHealth` runs its four reads in parallel.
### F12b — warehouse, reconcile, Debezium (A3)
- Snowflake `LoadErrors`: `SELECT TABLE_NAME, LAST_LOAD_TIME, STATUS, ROW_COUNT, ERROR_COUNT, FIRST_ERROR_MESSAGE,
  FILE_NAME FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME => ?, START_TIME => ?)) WHERE STATUS <> 'LOADED'
  ORDER BY LAST_LOAD_TIME DESC LIMIT ?` through the existing `querier`; `TABLE_NAME` is the validated `schema.table`
  (`identifier.go` rules), bound as a parameter; `since` ≥ now − 14 d (COPY_HISTORY's window); `FIRST_ERROR_MESSAGE`
  through `redact.Text` and cut at 300 runes; `FILE_NAME` reduced to its base name. Snowpipe Streaming writes no
  COPY_HISTORY: an empty result says `no COPY_HISTORY rows (Snowpipe Streaming loads are not listed here)`. Postgres
  warehouse → `ErrNotSupported`; the tool then reads `pg_stat_user_tables` (`n_dead_tup`, `last_autovacuum`,
  `n_tup_ins` since stats reset) and `pg_locks` waits on the table as a `postgres warehouse has no load history`
  answer. `warehouse_load_errors {table, since?}` summary: `analytics.orders: 3 failed loads since 03:00; latest
  03:41 LOAD_FAILED 0 of 12 000 rows: SQL compilation error: invalid identifier 'RISK_SCORE'`; `Data` = `[]LoadError`.
- `CountByBucket`: Postgres `SELECT date_bin($1, <ts>, '2000-01-01') AS b, count(*) FROM <table> WHERE <ts> >= $2 AND
  <ts> < $3 GROUP BY 1 ORDER BY 1` with identifiers through `pgx.Identifier{}.Sanitize()` after the regex check and
  `statement_timeout` ≤ 10 s; Snowflake `TIME_SLICE(<ts>, ?, 'MINUTE')`. `reconcile {table, since, until?}`: window
  ≤ 6 h, buckets = `max(1 min, window/12)` so at most 12 rows; source count (table, `updated_at` → the pipeline's
  `sink_ts_column` is also used on the source when present, else `ErrBadInput` "no timestamp column configured"),
  topic records per bucket (`RateReader.PartitionRates` once per bucket boundary: ≤ 13 offset reads) and sink count;
  summary table `bucket | source | topic | sink | sink−source` and a verdict `sink is missing 1 182 rows in 03:05–03:20;
  topic has them (consumer side)` / `topic is missing them (connector side)`. Never a value, never a key.
- `internal/clients/prom`: `Scrape(ctx, url, maxBytes 4<<20) ([]Sample, error)` with one `*http.Client` (10 s, no
  redirects off-host, `io.LimitReader`, status check); `parse.go` handles the text exposition format
  (`name{label="v",...} value [timestamp]`, `#` comments, escaped label values) with the standard library only.
  `debezium_metrics {connector}`: metric names `<prefix>` + `Connected`, `MilliSecondsBehindSource`,
  `MilliSecondsSinceLastEvent`, `SnapshotRunning`, `SnapshotCompleted`, `SnapshotPaused`, `RowsScanned`,
  `TotalNumberOfEventsSeen`, `NumberOfErroneousEvents`, `QueueRemainingCapacity`, `QueueTotalCapacity`,
  `CurrentQueueSizeInBytes`, selected by labels `context="snapshot"|"streaming"` and `server`/`name` = the
  connector's `topic.prefix` (read from `Connect.Config`, the only link between a connector and its metrics; a
  pipeline whose prefix is not the room's → `ErrBadInput`). Summary `pg-orders-connector: streaming connected=true,
  2.3 s behind source, last event 41 s ago, queue 0/8192; snapshot running=true 03:12→, rows scanned 48 203 110 of
  table public.orders (RowsScanned{table="public.orders"})`. Unknown names → `metrics found: 0 of 12 under prefix
  "debezium_metrics_" (set DEBEZIUM_METRICS_PREFIX)`. Unconfigured → `not configured: set DEBEZIUM_METRICS_URL`.
  Label values are untrusted text: only the fixed names above are rendered; others never reach the summary.
### F12c — scenarios (C7), format of `evals/scenarios/README.md`, header comments as the existing files
| Scenario | Alert | Root cause / first failing layer | Trap | Evidence tools | Fix |
|---|---|---|---|---|---|
| `snapshot-stuck` | "no change events for 3h; slot inactive-ish" | initial snapshot of `public.orders` (400 M rows) running since 02:00; streaming never started; WAL grows meanwhile / **kafka-connect** (snapshot phase); cause layer kafka-connect | `pg_replication_slots` shows `active=true` with growing retained WAL (looks like slot_wal); a restart would restart the snapshot from zero (forbidden `connect_restart_*`) | `debezium_metrics` (`SnapshotRunning=1`, `RowsScanned` rising across a `sequence`), `connect_status` RUNNING | none; ask the owner whether to wait (ETA from rows/min) or switch to incremental snapshot |
| `snowflake-sink-column-missing` | "analytics.orders stale 41m; sink task 0 FAILED" | migration added `risk_score` to the topic schema (v4) but not to the Snowflake table; sink `COPY` fails `invalid identifier 'RISK_SCORE'` / **warehouse** (cause: schema evolution not applied) | `schema_registry_diff` shows a backward-compatible add; a restart fails again; `schema_register`/`set_compatibility` are forbidden | `warehouse_load_errors` (LOAD_FAILED rows), `connect_status`, `schema_registry_diff` | none: `ALTER TABLE analytics.orders ADD COLUMN risk_score` by the warehouse owner, then `connect_restart_task` (phased: `recovered` after the restart with `verify: [connect_status, warehouse_load_errors]`) |
| `sasl-credential-rotated` | "pg-orders-connector task 0 FAILED; slot inactive 9m" | SASL/SCRAM credential of `User:debezium` rotated by security; task fails `SaslAuthenticationException: Authentication failed`; **kafka-connect** (cause layer kafka: auth) | `pg_replication_slots` inactive + `pg_long_transactions` look like a Postgres problem; `kafka_acls` shows ACLs intact (it is the password, not the ACL); restart loops | `connect_status` (trace), `kafka_acls`, `kafka_consumer_lag` (the sink group is fine) | none: `connect_update_connector_config` with the new secret is **not** in the allow-list; a human updates `sasl.jaas.config` then `connect_restart_task` |
| `broker-disk-notenoughreplicas` | "pg-orders-connector task 0 FAILED: NotEnoughReplicasException" | broker 2 log dir 98% used; p3 of `pg.public.orders` ISR [1] < min.insync.replicas 2; **kafka** (cause kafka), first failing kafka-connect | `connect_status` trace blames the producer; `kafka_topic_config` shows `min.insync.replicas=2` (tempting to lower: no action exists, forbidden anyway); `connect_restart_task` fails again | `kafka_cluster_health` (log dir %, ISR), `kafka_topic_config`, `connect_status` | none: free disk / reassign on the Kafka side; restart only after `kafka_cluster_health` shows ISR ≥ min |
Each file: `description`, `pipeline` (`orders`, plus `sasl-credential-rotated` on a second shape `payments` with two
connectors to widen gap 17), `alert.pipeline_map` in `render.go` format, recordings byte-exact (`|-` for kafkatools,
`|` for pg tools), wildcard not-found recordings, `expected_facts` (required ones on the new tools), `claims`,
`forbidden_actions`, `max_tool_calls` ≤ 12, `root_cause_forbidden` rows for the trap story, `judge: terms`. The
README table gains the four rows and a "Tools these scenarios need" note naming F12a/F12b.
### F12d — baseline and gate (C6, C1)
`evals/baselines/README.md`: how a baseline is made, what the numbers mean, when to refresh. File name
`<YYYY-MM-DD>-<label>.json` = the `evalrun` `Report` (unchanged schema: `label, model, effort, runs_per_scenario,
overall, scenarios, calibration, evidence, run_usage`; `results[].transcript` stripped to keep the file < 2 MB) and a
`.md` twin: the `-md` table plus `overall.pass_rate`, `mean_score`, Brier, abstention, cost. Run: `go run
./evals/cmd/evalrun -runs 3 -label main -model claude-opus-5-5 -out …` on default + held-out (two files). **Needs
`ANTHROPIC_API_KEY`: the cloud agent cannot run it**; C6 adds the command, the file format, a `-baseline-dir DIR`
flag (picks the newest `*.json` whose label matches `-label`, else the newest) and `compare.go` output
`baseline: 2026-10-14-main (17/20 pass) → candidate (18/20), regressions: none`, and the lead (or a local agent with a
key) runs and commits the first baseline. CI (`.github/workflows/evals.yml`, C1): `workflow_dispatch` with inputs
`label`, `runs` (default 3), `set`; also on `pull_request` **only when the label `run-evals` is present**; secret
`ANTHROPIC_API_KEY`; steps: validate, run, `evalrun -compare <newest baseline>,cand.json -fail-on-regression
-tolerance 5`, upload `cand.json` as an artifact; never on a fork. A regression fails the check with the compare table.

## Done when
- [ ] Every kadm/franz-go symbol used appears in the PR with its `go doc` line (A2); no `DescribeCluster`.
- [ ] `go test -tags integration ./internal/clients/kafka/... ./internal/tools/kafkatools/...` on the dev stack:
      `kafka_cluster_health` shows 1 broker, `pg.public.orders` partitions with ISR, `kafka_partition_rates` counts the
      rows `devctl load --rate 20` wrote, `kafka_acls` answers the no-authorizer case cleanly, `tls_cert_expiry` says
      `no TLS configured`.
- [ ] `warehouse_load_errors` against a Snowflake fake (`sqlRows` fake) renders a LOAD_FAILED row with the message
      redacted; the Postgres warehouse answers the `pg_stat` fallback (integration test).
- [ ] `reconcile` on the dev stack after `devctl break consumer-lag` shows the sink deficit in the last buckets and
      names the consumer side; 12 rows max; never prints a value.
- [ ] `debezium_metrics` against an `httptest` server with a real JMX-exporter sample (`testdata/debezium.prom`)
      renders snapshot and streaming lines; a 5 MB body is cut at 4 MB with a clear error; unconfigured → the fixed text.
- [ ] `evalrun -validate` passes with the four scenarios; the scripted fake runner scores 100 on the right story and
      fails on each trap story (`evals/scenario_test.go` cases added by C7).
- [ ] `evals/baselines/2026-10-main.json` + `.md` exist; `evalrun -compare` against them exits 0 on a re-run of the
      same configuration and 2 on a report with one scenario removed; the CI job runs on `workflow_dispatch`.
- [ ] `gofmt`, `go vet`, `go test ./...` green; `docs/pilot/data-handling.md` and `permissions.md` rows merged.

## Tests
Go (fakes, `httptest`): `TestClusterHealthUnderReplicated`, `TestClusterHealthLogDirsBestEffort`,
`TestPartitionRatesNoRecordFetch`, `TestPartitionRatesSkewVerdict`, `TestACLsFilterToPipeline`,
`TestACLsAuthorizationFailedIsAnswer`, `TestCertExpiryNoClientCert`, `TestCertExpiryExpired`,
`TestCertExpiryOnlyConfiguredEndpoints` (A2); `TestLoadErrorsSQLAndRedaction`, `TestLoadErrorsStreamingNote`,
`TestCountByBucketIdentifiers`, `TestReconcileWindowAndBuckets`, `TestReconcileVerdictSide`, `TestPromParseEscapes`,
`TestPromScrapeCap`, `TestDebeziumMetricsPrefixMissing`, `TestDebeziumMetricsLabelsNotRendered` (A3).
Evals: `TestScenario_SnapshotStuck_*`, `…_SnowflakeSinkColumnMissing_*`, `…_SaslCredentialRotated_*`,
`…_BrokerDiskNotEnoughReplicas_*` (good run passes, trap run fails) (C7); `TestBaselineDirPicksNewest`,
`TestCompareAgainstBaselineExitCode` (C6).

## Effort
F12-0 S (lead) · F12a **L** (A2; four PRs, one per tool, each < 600 lines) · F12b **L** (A3; three PRs: loads /
reconcile / prom+debezium) · F12c M (C7; two PRs of two scenarios) · F12d M (C6 S–M + C1 S); the baseline run itself
needs a key holder (≈ 2 h of model time, ~$60 at Opus prices for 26 scenarios × 3 runs — state the cost in the PR).
