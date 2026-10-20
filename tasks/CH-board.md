# ClickHouse warehouse wave

Each row is a task; a row split across agents is delivered as one PR per agent.
CH-0 updates the contracts and ownership before the client or its consumers
merge. The ClickHouse client is a reader; `WAREHOUSE_DB_URL` is a clickhouse-go/v2
DSN when `WAREHOUSE_KIND=clickhouse`.

| ID | Agent | Work | Done when | After |
|---|---|---|---|---|
| CH-0 | leads | Contract and ownership update: config comments, C3 client ownership, and this board | `[CH-0]` PR merged; `go build ./...`, `node docs/process/ship-workflow.dryrun.mjs`, and JSON parsing of `docs/process/ownership.json` pass; config comments name the DSN selected by `WAREHOUSE_KIND` without changing behavior | — |
| CH-ci-a | A1 | Add an opt-in `clickhouse` profile to `dev/docker-compose.yml` with its init SQL under `dev/clickhouse/` (`dev/**`), and `devctl clickhouse init` (`cmd/devctl/**`), which starts the profile and creates the sink table, Kafka engine source and materialized view for `pg.public.orders` (PR #702). The init creates an admin user and a reader user whose settings profile is `readonly=2`, never `readonly=1`: the client sends `readonly=2` and `max_execution_time` with every query, and `readonly=1` refuses any query that sets a setting | The profile starts a ClickHouse server; the reader user can run the client's queries with those settings and nothing that writes; `devctl clickhouse init` builds the Kafka engine hop | CH-0 |
| CH-ci-b | C1 | In the fullstack job of `.github/workflows/ci.yml`, start the `clickhouse` profile (its init creates the users) and export `CLICKHOUSE_INTEGRATION_DSN` (the `readonly=2` reader) and `CLICKHOUSE_INTEGRATION_ADMIN_DSN` to the integration test run (PR #703) | The CI integration job supplies the ClickHouse server and both DSNs, so the ClickHouse client integration test runs against a real server; `.github/scripts/fail-on-skips.sh` stays unchanged and still fails the job on a skip | CH-ci-a |
| CH-1 | C3 | ClickHouse warehouse client in `internal/clients/clickhouse/**` (PR #697 and review fixes) | The client implements its assigned warehouse readers, unit tests pass, and live integration tests pass against ClickHouse in CI; PR #697 has both required approvals | CH-0, CH-ci-b |
| CH-2 | A4 | Wire `WAREHOUSE_KIND=clickhouse`, `bigquery` and `databricks` in `internal/wiring/warehouse.go` beside the existing `postgres` and `snowflake` kinds (owner ruling 2026-10-20: wire every client that exists, so config and wiring accept the same kinds), and add all three to the per-pipeline `warehouse_kind` literal lists in `internal/wiring/connections.go` (`literalOK` and `literalWant`) | Each configured kind creates its own reader from `WAREHOUSE_DB_URL` with secrets redacted from errors; `postgres` and `snowflake` keep working; unknown kinds still fail clearly; `owners.yaml` accepts all five literals; a wiring test fails if a kind config documents has no reader | CH-1 |
| CH-3 | A3 | Add the Kafka engine and materialized-view hop to pipeline topology | A topology fixture for Kafka engine → materialized view → warehouse produces the expected nodes and edges without changing existing CDC or batch topologies; topology tests pass | CH-2 |
| CH-4 | C7 | Add 3–5 ClickHouse incident scenarios to `evals/scenarios/` | All new scenarios pass `evalrun -validate`; each names an expected cause and forbidden actions, and `evals/scenarios/README.md` lists them | CH-2 |
| CH-5 | C14 | Document the ClickHouse pilot in `docs/pilot/` | Pilot setup explains the selected-kind DSN, minimum read grants, supported behavior, and known query-log/engine limitations; every operational claim points to its source | CH-2 |

CH-ci-a starts after CH-0, and CH-ci-b after CH-ci-a. CH-1 waits on CH-ci-b:
its integration test needs the ClickHouse server and the
`CLICKHOUSE_INTEGRATION_DSN` and `CLICKHOUSE_INTEGRATION_ADMIN_DSN` variables
that CH-ci-b supplies, and without them `.github/scripts/fail-on-skips.sh` fails
on the test's skip. CH-2 starts after CH-1 lands; CH-3, CH-4, and CH-5 can then
proceed in parallel.
