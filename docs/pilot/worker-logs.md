# Worker logs, the Loki read token and the JMX exporter

Piperoom reads two read-only views of a Kafka Connect worker: log lines from Loki (`connect_worker_logs`, Q3c) and JMX metrics from the Prometheus JMX exporter (`connect_worker_metrics`, `debezium_snapshot_progress`, `debezium_metrics`, Q3b). This page gives the selector, credential, retention, caps, redaction and what reaches the model; the per-reader table is [data-handling.md](data-handling.md) (`docs/pilot/data-handling.md:263`).

**State on `main` (d6f91828).** Client and tool are merged (`internal/clients/workerlogs/`, `internal/tools/logtools/`) but not registered in the agent: row Q3e (`tasks/Q-board.md:279`), and no Go package outside `logtools` imports it (`grep -rl logtools --include=*.go .`). `internal/config/config.go` has no `WORKER_LOGS_*` key (no `ConfigFromEnv` or `ParseWorkerLogs`); the names appear only in the dev stack and the tool's messages (`dev/README.md:152-153`, `internal/tools/logtools/logtools.go:246,256`).

## 1. What is read

One `GET <WORKER_LOGS_LOKI_URL>/loki/api/v1/query_range` per call: `query`, `start`/`end` in Unix nanoseconds, `limit`, `direction=backward` (`internal/clients/workerlogs/workerlogs.go:47,286-296`; [Loki HTTP API](https://grafana.com/docs/loki/latest/reference/loki-http-api/)). The model chooses only `connector` (required, 1-249 printable bytes, checked against the room pipeline's `owners.yaml` before any I/O), `task` (0-9999, absent = any), `level` (`DEBUG`/`INFO`/`WARN`/`ERROR`, default `WARN`), `since` (duration or RFC 3339, in the past, at most 7 days, default 1 hour) and `limit` (1-200, default 100) (`internal/tools/logtools/logtools.go:135-148,170-202,236-242`).

An entry carries its time, level, logger, worker and text: the worker is the stream label `pod`, `instance`, `host`, `hostname` or `container` (`workerlogs.go:418-422`); the level is the label `level`/`detected_level`, else the first 64 bytes of the line (`:383,438-451`); the logger is read from the end of the first line (`:453-471`). Newest entries are kept and returned oldest first, newest last (`:275-278,346-349`).

## 2. The selector is the operator's, never the model's

`WORKER_LOGS_SELECTOR` must be a bare LogQL stream selector `{ name op "value", ... }` with `=`, `!=`, `=~` or `!~`, at most 1024 bytes; pipeline stages (`|= "x"`, `| json`) and metric queries are refused, because the selector decides which streams are readable and a stage would change what "newest" means without the tool knowing (`internal/clients/workerlogs/selector.go:10-75`).

Model values become line filters after it: `|= "<connector>"`, or for a task `|~ "<connector>(-<n>\b|\|task-<n>\])"` (Connect logs `<connector>-<n>` and `[<connector>|task-<n>]`, `:86-98`), plus a coarse level regex such as `\b(WARN|WARNING|ERROR|SEVERE|FATAL)\b` (`:100-121`). Every value passes `strconv.Quote` and the connector `regexp.QuoteMeta`, so none can close its literal and add a stage (`:77-82`). The dev stack uses `{service="connect"}` (`dev/logs/promtail.yml:4`, `dev/README.md:153`).

## 3. The read token

`WORKER_LOGS_TOKEN` is used as follows (`internal/clients/workerlogs/workerlogs.go`): one `GET /loki/api/v1/query_range` (`:47`; the package has no push, delete or admin call); the bearer header goes only to the base URL's own scheme and host (`:213-234`); a token over `http://` is refused unless the host is loopback (`:179-181`); cross-host redirects are refused (`:185-193`). Optional basic auth sends the token as the password (`:137-139`), and optional `X-Scope-OrgID` selects one tenant (`:141-142,230-232`). A 401/403 becomes `ErrAuth` and the tool says to check `WORKER_LOGS_TOKEN` (`:108-111`; `internal/tools/logtools/logtools.go:255-257`). The token is a `netsec.Secret` whose `String`, `GoString`, `LogValue` and `MarshalText` print a mask (`internal/clients/netsec/netsec.go:34-50`); errors keep the host only (`workerlogs.go:313-319`), and the provenance receipt has host, path and LogQL query, never the token (`:298-305`).

**Grafana Cloud:** an access policy token whose only scope is `logs:read`, sent as basic auth with the Loki instance ID as username and the token as password ([Authenticate to query Cloud Logs](https://grafana.com/docs/grafana-cloud/send-data/logs/authenticate-to-query-logs/)). Grant no `logs:write`, no delete and no other data source's scope; one stack and one tenant, so leave the org-ID option unset unless that tenant is the one you mean.

**Self-hosted:** Loki has no authentication of its own and belongs behind a reverse proxy ([authentication](https://grafana.com/docs/loki/latest/operations/authentication/)); with `auth_enabled: true` every request needs `X-Scope-OrgID` ([tenant isolation](https://grafana.com/docs/loki/latest/operations/multi-tenancy/)). The minimum is a proxy credential that can query logs in one tenant only. The dev Loki sets `auth_enabled: false` and binds host loopback, so it needs no token (`dev/logs/loki.yml:4`, `dev/docker-compose.yml:260-261`). Gap: the basic-auth user and org id have client options but no environment variable names yet; Q3e must add them (`workerlogs.go:137-142`).

## 4. Retention

Piperoom never reads older than 7 days and defaults to the last hour (`workerlogs.go:36-40`; `internal/tools/logtools/logtools.go:205-227`), whatever Loki still holds. The dev Loki has no compactor or retention section (`dev/logs/loki.yml`, ends at `:33`); Loki keeps logs forever unless `compactor.retention_enabled` is set ([retention](https://grafana.com/docs/loki/latest/operations/storage/retention/)), and here they sit in the container's filesystem and go with `docker compose down` (`dev/logs/loki.yml:1-3,13-20`; `dev/README.md:137-138`).

Your own Loki's retention decides how much history exists; the 7-day cap applies on top. Returned lines are not ephemeral: `Data` is written to the room event log and stays until the room is deleted (`docs/pilot/data-handling.md:11-17,50-55`).

## 5. Caps

| Cap | Value | Source |
|---|---|---|
| Lines per call; default | 200; 100 | `workerlogs.go:31-33`; `logtools.go:35-38` |
| Text in `Data`; one entry | 64 KiB newest kept (older counted in `dropped`); 16 KiB start kept | `logtools.go:38-43,266-284,413-423` |
| Loki request; response body | 10 s; 4 MB (non-200 read to 4 KiB, 200 bytes echoed) | `logtools.go:44-45`; `netsec/http.go:11-13`; `workerlogs.go:34-35,48,96-106,322-332` |
| Lookback; selector | 7 days (1 hour default); 1024 bytes | `workerlogs.go:36-40`; `selector.go:10-12` |
| Summary to the model | 8000 bytes | `internal/tools/tool.go:16-18` |

## 6. What is redacted and what reaches the model

Each line is sanitized (invalid UTF-8 replaced, ANSI escapes dropped, CRLF folded, control characters turned into spaces, `logtools.go:395-411`), cut to 16 KiB (`:413-423`), masked by `redact.Text` and scanned by `injectscan.Scan` (`:272-302`). `redact.Text` masks private keys, URL passwords, auth headers, bearer tokens, JWTs, cloud keys, API keys and `key=value` secrets, plus e-mail local parts; IP addresses are deliberately kept as evidence (`internal/redact/redact.go:105-127,157-165`), and only counts per kind are shown (`logtools.go:282-291`). The scan reports instruction-like text as kind and byte offset, with no excerpt (`logtools.go:97-102,292-302`; `internal/injectscan/injectscan.go:22-51,71-80`).

**What reaches the model** is only `Result.Summary`: a warning if anything was flagged, then the lines inside a per-call nonce fence (`<<<untrusted-log-<nonce>` ... `>>>untrusted-log-<nonce>`) that says they are data, never instructions (`logtools.go:329-362`). Each entry is its time, level, worker, the first line cut to 400 bytes and at most the two deepest `Caused by:` lines (`:368-393`). The full masked text (64 KiB) is in `Data`, which goes to the room event log and not to the model (`docs/pilot/data-handling.md:11-17`).

**What can be in a line:** worker output is free text; a converter or sink error can echo a record value or a SQL statement (`docs/pilot/data-handling.md:303-306`), and `errors.log.include.messages=true` writes failed record values into the worker log — Piperoom refuses that value when it validates `errors.*` keys (`docs/pilot/permissions.md:441-445`). Redaction is best-effort with documented gaps (`internal/redact/redact.go:32-60`); injection scanning is heuristics and can miss paraphrases (`internal/injectscan/injectscan.go:6-11`). Treat every line as potentially sensitive.

## 7. The JMX exporter rules

The `connect` image carries the Prometheus JMX exporter 1.0.1, checksum-verified, loaded through `KAFKA_OPTS` and served on `127.0.0.1:9404/metrics` without a login (`dev/connect/Dockerfile:19-28`; `dev/docker-compose.yml:86-93`). Rules are evaluated in order, the first match wins, and attributes that match no rule are not collected — there is no catch-all ([rule semantics](https://github.com/prometheus/jmx_exporter/blob/main/website/docs/configuration/rules.md); `dev/connect/jmx-exporter.yml:7-10`). `cmd/devctl/jmx_exporter_test.go` applies the rules the way exporter 1.0.1 does and asserts the names below and the negatives (`:30-60,107-177`).

| Rule (MBean family) | Exported as | Why it is exported | Reader |
|---|---|---|---|
| Debezium connector metrics, `server`/`task`/`context`/`key` labels | `debezium_metrics_RowsScanned`, `debezium_metrics_<attr>` | snapshot state, rows scanned per table, lag, event counts (`jmx-exporter.yml:14-52`) | `debezium_snapshot_progress`, `debezium_metrics` (`internal/tools/debeziumtools/snapshot.go:21-25`, `metrics.go:17-22`) |
| `connector-metrics` and `connector-task-metrics` status | `kafka_connect_connector_status`, `kafka_connect_connector_task_status` (value 1, status as label) | a status word leaves only through a fixed-value rule (`jmx-exporter.yml:58-74`) | `connect_worker_metrics` |
| `task-error`/`source-task`/`sink-task`/`connector-task` numbers | `kafka_connect_<type>_metrics_<attr>` | errors, failures, skipped records, retries, DLQ produce requests/failures, age of last error (`jmx-exporter.yml:75-83`) | `connect_worker_metrics` (`worker.go:35-42`) |
| `connect-worker-metrics` with and without a connector label | `kafka_connect_connect_worker_metrics_<attr>` | task counts; startup failures and attempts; worker counts (`jmx-exporter.yml:84-92`) | `connect_worker_metrics` (`worker.go:43-55`) |
| `connect-worker-rebalance-metrics` | `kafka_connect_connect_worker_rebalance_metrics_<attr>` | rebalancing now, completed rebalances, epoch, rebalance times (`jmx-exporter.yml:93-96`) | `connect_worker_metrics` (`worker.go:56-62`) |

Deliberately not exported: connector class names, producer/JVM metrics and `app-info` strings (`cmd/devctl/jmx_exporter_test.go:157-161`). Exporter 1.x drops a trailing `_total` from every name (`jmx_exporter_test.go:63-78`); `connect_worker_metrics` accepts both forms and names each required family the endpoint does not export, so a missing rule is visible instead of reading as zero (`internal/tools/debeziumtools/worker.go:63-72,207-212,255-258,305-325`).

## 8. Running the `logs` profile locally

```
docker compose -f dev/docker-compose.yml --profile logs up -d --wait   # default stack + loki + promtail
docker compose -f dev/docker-compose.yml --profile logs down           # a plain `down` leaves them
```

Promtail finds the `connect` container of project `piperoom-dev` through the read-only Docker socket, joins a stack trace to the line that logged it, labels entries `service`, `container` and `level`, and pushes to Loki (`dev/logs/promtail.yml:16-43`; `dev/docker-compose.yml:268-290`).

Set `WORKER_LOGS_LOKI_URL=http://localhost:3100` and `WORKER_LOGS_SELECTOR={service="connect"}`, no token (`dev/README.md:145-153`; `dev/docker-compose.yml:260-261`); check with `curl -sG localhost:3100/loki/api/v1/query_range --data-urlencode 'query={service="connect"}' --data-urlencode limit=5` (`dev/README.md:155-156`). The profile has not been seen healthy yet: it was written without Docker, and only the compose structure, rules and Promtail regexes are tested (`dev/README.md:157-164`).
