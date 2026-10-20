# Batch profile: Airflow + dbt (P4g)

The `batch` compose profile adds one service, `airflow`, to the dev stack. It runs the batch pipeline
`daily-revenue` (`dev/owners.yaml`):

```
DAG revenue_daily:  check_source  ->  dbt_run
                                        stg_orders (view)  ->  fct_revenue (table)
warehouse db:       analytics.orders ---------------------->  analytics.fct_revenue
```

Without `--profile batch` nothing here starts; the default services are unchanged.

## Start

```powershell
docker compose -f dev/docker-compose.yml --profile batch up -d --build --wait
```

Linux only: `export AIRFLOW_UID=$(id -u)` first, so dbt can write the bind-mounted `dev/batch/artifacts`
(Docker Desktop on Windows and macOS needs nothing). The first `--build` installs dbt into the image; after
that the service is healthy once the API answers for `revenue_daily` (target < 3 min). The DAG is unpaused
at creation, so one run starts right away (`catchup=False`) and then daily.

`go test -tags integration -run TestBatchProfile ./internal/testenv/` does the same against a running dev stack,
waits for one successful `revenue_daily` run, checks `analytics.fct_revenue` and the artifacts, logs the
timings, and afterwards removes the `airflow` service, drops `analytics.stg_orders` and `analytics.fct_revenue`
and deletes the two artifacts (the base stack stays up).

Build cost: the image build (pull `apache/airflow`, pip install dbt) is not cached between CI runs; the test
logs `compose up --build airflow took ...` so the integration job shows what it adds. If it is large, the job
can move to its own CI job with a build cache (C1's `.github/**`).

## What is where

| What | Value |
|---|---|
| Image | `piperoom-dev/airflow:2.10.5-dbt1.9.0`, built from `apache/airflow:2.10.5-python3.12` + `dbt-postgres==1.9.0` in `/opt/dbt-venv` |
| Airflow UI and REST API v1 | `http://127.0.0.1:8080` (loopback only); `/api/v1/...` takes **no login** (`AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.default`, plus `AUTH_ROLE_PUBLIC = "Viewer"` in `dev/batch/airflow/webserver_config.py`: since Airflow 2.8 every API call is also authorized, and an anonymous caller with no public role gets 403), because the Airflow client refuses to send a password or token over `http://` (#587). The UI login is `admin` with the password `airflow standalone` prints in the container log |
| Airflow metadata | SQLite inside the container (standalone mode); `down` forgets the run history |
| DAG | `dev/batch/dags/revenue_daily.py` |
| dbt project | `dev/batch/dbt/analytics` (`profiles.yml` targets database `warehouse`, schema `analytics`) |
| dbt artifacts | `dev/batch/artifacts` on the host = `/opt/airflow/dbt-artifacts` in the container (`DBT_TARGET_PATH`): `run_results.json`, `manifest.json` |

## Point Piperoom at it

Run the server on the host as for the default stack, plus the batch reader env vars (the `daily-revenue`
pipeline is already in `dev/owners.yaml`, and `dev/users.yaml` already names its `orchestrator` and
`transform` owners):

```powershell
$env:AIRFLOW_URL = "http://127.0.0.1:8080"
$env:DBT_ARTIFACTS_DIR = (Resolve-Path dev/batch/artifacts).Path   # must be absolute
```

The server refuses to start when the users and owners files disagree in either direction, so
`dev/users.yaml` and `dev/owners.yaml` go together (`TestUsersBatchMatchesOwners` in `cmd/devctl` runs
that check on them). Leave `AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD` and `AIRFLOW_TOKEN` unset (config refuses
a login over `http://`). `devctl break dag-failed`, `dbt-model-broken` and `late-batch` exercise the three
batch failure modes (dev/scenarios.md §13).
