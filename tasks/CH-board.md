# ClickHouse warehouse wave

Each row is a task; a row split across agents is delivered as one PR per agent.
CH-0 updates the contracts and ownership before the client or its consumers
merge. The ClickHouse client is a reader; `WAREHOUSE_DB_URL` is a clickhouse-go/v2
DSN when `WAREHOUSE_KIND=clickhouse`.

| ID | Agent | Work | Done when | After |
|---|---|---|---|---|
| CH-0 | leads | Contract and ownership update: config comments, C3 client ownership, and this board | `[CH-0]` PR merged; `go build ./...`, `node docs/process/ship-workflow.dryrun.mjs`, and JSON parsing of `docs/process/ownership.json` pass; config comments name the DSN selected by `WAREHOUSE_KIND` without changing behavior | — |
| CH-ci | A1 + C1 | A1 adds a `clickhouse` profile to `dev/docker-compose.yml`; C1 adds ClickHouse to the CI integration job in `.github/**` | The dev profile starts a ClickHouse server; the CI integration job runs the ClickHouse client integration tests against a real server and reports failures instead of passing on a skip | CH-0 |
| CH-1 | C3 | ClickHouse warehouse client in `internal/clients/clickhouse/**` (PR #697 and review fixes) | The client implements its assigned warehouse readers, unit tests pass, and live integration tests pass against ClickHouse in CI; PR #697 has both required approvals | CH-0 |
| CH-2 | A4 | Wire `WAREHOUSE_KIND=clickhouse` in `internal/wiring/warehouse.go`, alongside the existing BigQuery and Databricks clients | Configured ClickHouse creates the ClickHouse reader; BigQuery and Databricks wiring remains available; unsupported kinds still fail clearly; wiring tests cover all supported kinds | CH-ci, CH-1 |
| CH-3 | A3 | Add the Kafka engine and materialized-view hop to pipeline topology | A topology fixture for Kafka engine → materialized view → warehouse produces the expected nodes and edges without changing existing CDC or batch topologies; topology tests pass | CH-2 |
| CH-4 | C7 | Add 3–5 ClickHouse incident scenarios to `evals/scenarios/` | All new scenarios pass `evalrun -validate`; each names an expected cause and forbidden actions, and `evals/scenarios/README.md` lists them | CH-2 |
| CH-5 | C14 | Document the ClickHouse pilot in `docs/pilot/` | Pilot setup explains the selected-kind DSN, minimum read grants, supported behavior, and known query-log/engine limitations; every operational claim points to its source | CH-2 |

CH-ci and CH-1 can proceed in parallel after CH-0. CH-2 starts after both land;
CH-3, CH-4, and CH-5 can then proceed in parallel.
