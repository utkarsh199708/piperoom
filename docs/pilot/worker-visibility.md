# Connect worker visibility: logs and metrics

For the engineer who lets Piperoom read the Kafka Connect workers' **log lines** (from Grafana Loki) and **JMX
metrics** (from the Prometheus JMX exporter). Both are read-only. They answer the questions `connect_status`
cannot: a task that is `RUNNING` but skips records, a DLQ that fails to produce, a worker group that keeps
rebalancing, a `FAILED` task whose trace was cut.

Checked against the Q batch 2 integration branch at `fe54acc`. Each claim names the file it comes from; if
a file changes, update the claim and its citation in the same PR.

## Status on this release: written, not yet wired

| Piece | State | Where |
|---|---|---|
| Loki reader and tool `connect_worker_logs` (Q3c) | code and unit tests exist; **not registered** in the agent's tools yet. Registering it is the wiring row Q3e | `internal/clients/workerlogs/`, `internal/tools/logtools/logtools.go` (`Register`: "Registering it in the agent's registry is the wiring row's decision (Q3e)") |
| Tool `connect_worker_metrics` (Q3b) | code exists; **not registered** yet, for the same reason | `internal/tools/debeziumtools/worker.go` (`RegisterWorkerMetrics`) |
| `WORKER_LOGS_LOKI_URL`, `WORKER_LOGS_SELECTOR`, `WORKER_LOGS_TOKEN`, `CONNECT_METRICS_PREFIX` | **proposed only**: the Q0 contract item names them, but they are not in `internal/config/config.go` yet, so a server does not read them today | `tasks/Q-board.md` (Q0 item 3, `config.go` paragraph); `grep WORKER_LOGS internal/config/config.go` finds nothing |
| `DEBEZIUM_METRICS_URL` | live: already read by the server for `debezium_snapshot_progress` and `debezium_metrics` | [setup.md](setup.md#1-configure-the-environment) (`config.ParseDebeziumMetrics`) |
| Dev stack: exporter rules and the `logs` profile (Q3d) | in the repo; never seen healthy on a running stack | `dev/README.md` ("Worker metrics and the `logs` profile", "Not run yet") |

Until Q3e and the config change land, set nothing below on a pilot server: the variables are ignored. Prepare
the token and the exporter rules now so the wiring PR can be turned on with one restart.

## 1. Settings (proposed)

What the code takes, and the variable each value is proposed to come from:

| Variable (proposed) | What the code does with it | Code |
|---|---|---|
| `WORKER_LOGS_LOKI_URL` | Loki's base URL, without `/loki/api/v1` (a gateway path prefix is kept). No query string or fragment. Unset means the tool is still registered and answers "not configured" with no request made. | `workerlogs.New`; `logtools.Deps.Logs` ("Nil means WORKER_LOGS_LOKI_URL is not set") |
| `WORKER_LOGS_SELECTOR` | A LogQL **stream selector only**: `{` matchers `}` with `=`, `!=`, `=~`, `!~` and quoted values, at most 1024 bytes. A pipeline stage (`|= "x"`, `| json`) or a metric query is refused at start. The model never sets it. | `workerlogs.CheckSelector` (`selector.go`) |
| `WORKER_LOGS_TOKEN` (secret) | Sent as `Authorization: Bearer <token>` to the base URL's own scheme and host only. Over `http://` it is refused unless the host is loopback. | `workerlogs.New`, type `headers` (`workerlogs.go`) |
| `CONNECT_METRICS_PREFIX` | The metric name prefix of Connect's MBeans on the `DEBEZIUM_METRICS_URL` endpoint; empty means `kafka_connect_`. Must match `^[a-zA-Z_:][a-zA-Z0-9_:]*$`. | `debeziumtools.RegisterWorkerMetrics`, `DefaultWorkerMetricsPrefix` (`worker.go`) |

The Loki client also takes three options that **no proposed variable covers yet**. A Grafana Cloud or a
multi-tenant Loki needs one of the first two, so the config change should add them (flagged to the leads):

| Option | What it does | Code |
|---|---|---|
| `WithBasicAuthUser(user)` | Sends the token as the **password** of HTTP basic auth for `user` instead of as a bearer token (Grafana Cloud: the stack's Loki user id). Basic auth is refused over `http://`. | `workerlogs.go`; `internal/clients/netsec/http.go` |
| `WithOrgID(id)` | Sends `X-Scope-OrgID: <id>`, the tenant of a multi-tenant Loki; at most 128 printable characters. | `workerlogs.go` |
| `WithTLS(...)` | CA bundle, client certificate and server name (the `netsec` TLS settings the other clients use). | `workerlogs.go`; `internal/clients/netsec` |

For the dev stack the values are `WORKER_LOGS_LOKI_URL=http://localhost:3100`,
`WORKER_LOGS_SELECTOR={service="connect"}` and `DEBEZIUM_METRICS_URL=http://localhost:9404/metrics`
(`dev/README.md`, the table under "Worker metrics and the `logs` profile").

## 2. The Loki read token (least privilege)

Piperoom makes **one kind of request**: `GET <base>/loki/api/v1/query_range` with `query`, `start`, `end`,
`limit` and `direction=backward` (`WorkerLogs` in `internal/clients/workerlogs/workerlogs.go`). It never
pushes, deletes, tails or reads labels, series, rules or the ruler. Give it a credential that can do that one
read and nothing else. The Loki endpoint is described in Grafana's
[Loki HTTP API reference](https://grafana.com/docs/loki/latest/reference/loki-http-api/).

**The selector is not the boundary; the token is.** `WORKER_LOGS_SELECTOR` picks which streams the tool reads,
and the model cannot change it (`CheckSelector`, `buildQuery` in `selector.go`). But anyone holding the token
can query every stream the token can read. Scope the token to the Connect workers' logs:

**Grafana Cloud.** Create an access policy for the stack with the **`logs:read`** scope only (no `logs:write`,
no metrics, traces or alerts scopes) and a token for it
([access policies](https://grafana.com/docs/grafana-cloud/security-and-account-management/authentication-and-permissions/access-policies/)).
Grafana Cloud Logs takes basic auth: the user is the stack's Loki user id, the password is the token (the option's own doc comment in `workerlogs.go`). That is
the `WithBasicAuthUser` option above, and it needs an `https://` URL (`netsec/http.go` refuses basic auth over
`http://`). If your Connect logs share a stack with application logs, the token can read those too: ship the
Connect workers' logs to a stack (or a label-restricted policy, where your plan offers one) that holds only
them.

**Self-hosted Loki.** Loki has no users or tokens of its own: with `auth_enabled: true` it only reads the tenant
from `X-Scope-OrgID`, and authentication is left to a reverse proxy or gateway in front of it
([Loki authentication](https://grafana.com/docs/loki/latest/operations/authentication/),
[multi-tenancy](https://grafana.com/docs/loki/latest/operations/multi-tenancy/)). Least privilege there means:

- Put the gateway in front of Loki, have it check the bearer token, and allow that token only
  `GET /loki/api/v1/query_range` (and the gateway's own health path, if it has one).
- Ship the Connect workers' logs into their **own tenant**, and have the gateway pin that tenant for this token
  (or pass it with `WithOrgID`). The token can then read nothing but Connect logs.
- Serve the gateway over `https://`. A bearer token over `http://` is refused unless the host is loopback
  (`workerlogs.New`).
- The dev stack's Loki runs with `auth_enabled: false` and listens on host loopback only (`dev/logs/loki.yml`,
  `dev/docker-compose.yml` service `loki`). Do not copy that to a shared host.

What Piperoom does with the token: it goes only to the configured host (a redirect to another scheme or host is
refused, at most 3 redirects; `CheckRedirect` in `workerlogs.New`); a `401` or `403` becomes `ErrAuth` and the tool
says "Loki refused the read token (check WORKER_LOGS_TOKEN)" without the token (`StatusError.Is`;
`logtools.go` `Run`); a transport error keeps the host only, not the URL with the query (`WorkerLogs`).

Shipping the logs is your side. The dev stack's Promtail config (`dev/logs/promtail.yml`) shows a working shape:
it tails only the `connect` container, joins a stack trace to the line that logged it (`multiline` stage,
first line starts with a date), and labels each entry `service`, `container` and `level`. The reader reads the
level from the `level` or `detected_level` label, else from the line, and the worker name from the `pod`,
`instance`, `host`, `hostname` or `container` label (`workerOf`, `normLevel`, `lineLevel` in `workerlogs.go`).
Ship one entry per log event with its stack trace: a trace split into one entry per line loses the
`Caused by:` lines the summary shows.

## 3. The JMX exporter rules

`connect_worker_metrics` reads Connect's own MBeans from the same Prometheus endpoint as the Debezium tools
(`DEBEZIUM_METRICS_URL`, through `internal/clients/prom`), so a worker already exporting Debezium metrics needs
only more rules, not another endpoint (`RegisterWorkerMetrics` in `internal/tools/debeziumtools/worker.go`).

**Names the tool expects** (prefix + MBean type + attribute, `-` turned into `_`; the exporter's rule-less naming,
`worker.go` comment above `famTaskErrors`):

| MBean (Kafka's [Connect monitoring](https://kafka.apache.org/documentation/#connect_monitoring)) | Metric names read | Code |
|---|---|---|
| `kafka.connect:type=task-error-metrics,connector=*,task=*` | `<prefix>task_error_metrics_` + `total_record_errors`, `total_record_failures`, `total_records_skipped`, `total_retries`, `total_errors_logged`, `deadletterqueue_produce_requests`, `deadletterqueue_produce_failures`, `last_error_timestamp`, labels `connector`, `task` | `taskErrorAttrs` |
| `kafka.connect:type=connect-worker-metrics,connector=*` | `<prefix>connect_worker_metrics_connector_{total,running,failed,paused,unassigned,restarting}_task_count`, label `connector` | `connectorAttrs` |
| `kafka.connect:type=connect-worker-metrics` | `<prefix>connect_worker_metrics_` + `task_startup_failure_total`, `task_startup_attempts_total`, `connector_startup_failure_total`, `connector_startup_attempts_total`, `task_count`, `connector_count` | `workerAttrs` |
| `kafka.connect:type=connect-worker-rebalance-metrics` | `<prefix>connect_worker_rebalance_metrics_` + `rebalancing`, `completed_rebalances_total`, `epoch`, `rebalance_avg_time_ms`, `rebalance_max_time_ms`, `time_since_last_rebalance_ms` | `rebalanceAttrs` |

If one of the five families the diagnosis rests on (`total_record_errors`, `total_records_skipped`,
`deadletterqueue_produce_failures`, `task_startup_failure_total`, `rebalancing`) is absent for every connector,
the tool names it as "the exporter does not export ..." instead of reading a missing number as `0`
(`requiredFamilies`, and the summary text in `worker.go`).

**Rules to copy.** `dev/connect/jmx-exporter.yml` holds working rules: copy its "Kafka Connect" block (the last
six rules) into your exporter config next to your Debezium rules. Things it gets right that a hand-written
config often does not:

- **No catch-all rule.** Only matched attributes are exported, so the endpoint stays small and nothing but numbers
  and the status words leave the worker (file header comment).
- **Status rules first.** `connector-metrics` and `connector-task-metrics` `status` are strings; they are exported
  as `kafka_connect_connector_status` / `kafka_connect_connector_task_status` with value `1` and the status in a
  label, and they come before the numeric rules because a string attribute matched by a numeric rule is
  dropped (comment above the status rules).
- **Exporter 1.x drops a trailing `_total`** (`..._task_startup_failure`); the tool accepts both forms
  (`dev/README.md`, "Not run yet" paragraph).
- The dev image pins `jmx_prometheus_javaagent` 1.0.1 by checksum and loads it with `KAFKA_OPTS`
  (`dev/connect/Dockerfile`, `dev/docker-compose.yml` service `connect`). The rules are unit-tested against sample
  MBean names (`cmd/devctl/jmx_exporter_test.go`).

If your exporter uses another prefix (a `lowercaseOutputName`, or rules that rename the domain), set the
proposed `CONNECT_METRICS_PREFIX` to it. The exporter project's own rule syntax is in its
[README](https://github.com/prometheus/jmx_exporter).

**Endpoint access.** The metrics URL takes no user info (`DEBEZIUM_METRICS_URL` row in
[setup.md](setup.md#1-configure-the-environment)). The exporter has no login of its own in the dev stack
(`dev/README.md`: "host loopback; no login"); keep it on a network only the Piperoom server reaches.

## 4. What reaches the model

Both tools are already in [data-handling.md](data-handling.md); this section adds the order of the protections,
not new rows.

- **Metrics** carry numbers only. `connect_worker_metrics` keeps no label text from the endpoint other than a
  numeric task id, and no record or error text
  ([data-handling.md §2](data-handling.md#2-what-each-tool-exposes), row `connect_worker_metrics`;
  `WorkerReport` in `worker.go`).
- **Log lines are free text** written by the worker. A converter, SMT or sink error can echo a record value or a
  SQL statement, and the worker logs host names ([data-handling.md §2](data-handling.md#2-what-each-tool-exposes),
  row `connect_worker_logs`, and the "Worker log lines are free text" note under it). Answer the go-live question
  in [data-handling.md §6](data-handling.md#6-questions-to-answer-before-go-live) for log lines too.

What `connect_worker_logs` does to each line before the model sees it, in this order (`Run` and `summarize` in
`internal/tools/logtools/logtools.go`):

1. **Scope.** The connector must be one of the room pipeline's (`sc.CheckConnector`) before any request; the model
   picks only connector, task, level (`DEBUG` to `ERROR`), how far back (at most 168 h) and a line count; each
   value is escaped as a LogQL string literal (`buildQuery` in `selector.go`).
2. **Caps.** Loki is asked for at most 200 lines; the answer is read to 4 MB (`MaxLines`, `MaxBodyBytes` in
   `workerlogs.go`). Each entry is cut to 16 KiB, keeping its start, and the newest entries that fit 64 KiB are
   kept (`MaxLineBytes`, `MaxTextBytes`). The call times out after 10 s (`callTimeout`).
3. **Sanitize.** Invalid UTF-8 replaced, ANSI sequences dropped, control characters other than tab and newline
   turned into spaces (`sanitize`).
4. **Redact.** `redact.Text` masks secret-looking values (N5, `internal/redact`); the counts by kind go in
   `Data.secrets_masked` and the summary. Redaction is by pattern: a customer email or an amount in an error
   message is **not** masked.
5. **Injection scan.** `injectscan.Scan` (N6, `internal/injectscan`) runs on every kept line; findings go in
   `Data.injection` (line, kind, offset, no excerpt) and add a warning to the summary telling the model not to
   follow the text.
6. **Fence.** The summary shows the newest entries (first line plus at most two `Caused by:` lines, each cut to
   400 bytes) between `untrusted-log-<random>` markers; the agent's own untrusted-data wrapper then wraps the whole
   tool result as for every tool ([data-handling.md §3](data-handling.md#3-prompt-injection-and-untrusted-text);
   `internal/agent/prompt.go`).

The provenance receipt of the call, stored in the room log, holds the Loki host, the path and the full LogQL
query (selector plus the connector name) and the start time, not the token (`tools.Record` in `WorkerLogs`).
A Loki error body is cut to 200 bytes with control characters removed before it reaches an error or the log
(`StatusError.Error`).

## Review checklist

- [ ] The Loki credential can only read, and only the Connect workers' logs (own Grafana Cloud stack or
      label-restricted policy with `logs:read`; or a self-hosted tenant pinned by the gateway).
- [ ] The Loki URL is `https://` (or loopback), and the selector names the Connect workers' streams only.
- [ ] Your log shipper keeps a stack trace in one entry and sets a `level` label.
- [ ] Your exporter config has the Connect rules from `dev/connect/jmx-exporter.yml`, no catch-all, and the
      metrics endpoint is reachable only from the Piperoom server.
- [ ] You checked a day of your Connect workers' WARN and ERROR lines for record values before the pilot, and the
      data-handling go-live questions cover them.
- [ ] On your release, Q3e has registered the tools and `internal/config/config.go` has the variables; until
      then this page is preparation only.
