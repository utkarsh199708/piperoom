# Piperoom

Piperoom is a shared incident room for CDC/Kafka data pipelines (Postgres → Debezium →
Kafka → consumers → warehouse). When an alert fires, it opens a room where an AI agent
(Claude) investigates with read-only tools while engineers watch live, comment, and steer.
Only one person — the **driver** — steers the agent at a time; everyone else suggests. The
agent can only propose a fix, and a fix runs only once every layer owner it touches has
approved it. At shift change, the agent writes a hand-off summary and the driver role moves
to the next person.

A UI mockup of the target room view is linked from `AGENTS.md`.

## Repo map

```
cmd/
  devctl/        local pipeline control (lock/unlock/up/down/reset/status/load/break/fix)
  devconsumer/   sample order-sync consumer used by the dev stack and scenarios
  discover/      drafts an owners file from a live cluster (read-only)
  investigate/   CLI entry point for a one-shot agent investigation
evals/           agent eval harness and scenarios (cmd/evalrun)
dev/
  docker-compose.yml   Postgres, Kafka, Schema Registry, Connect, devconsumer
  scenarios.md         the 4 observed failure scenarios devctl can break/fix
  owners.yaml, users.yaml   pipeline layer ownership and dev users
internal/
  actions/       action implementations an approved proposal runs (the only writes)
  agent/         the Claude investigation loop and its contract
  clients/       Postgres, warehouse, Connect, Schema Registry, and Kafka clients
  owners/        who owns which pipeline layer, for approvals
  slackbridge/   mirrors a room into one Slack thread (wired in cmd/server)
  stacklock/      the shared dev-stack lock devctl uses
  testenv/       integration-test helpers (skip when the stack is unreachable or locked)
  tools/         read-only tools the agent calls, grouped by area
  topology/      the pipeline graph used to localize a fault
  watch/         the built-in watcher: polls Connect and the source DB and opens a room when a pipeline is in trouble (PIPEROOM_WATCH_MODE)
  events/        the append-only room event log types
  config/        environment variable loading (read config only through config.Load())
web/             the React room UI
docs/            API contract (docs/api.md), this repo's collaboration rules, runbook
tasks/           one task file per agent, read before writing code
AGENTS.md        rules for coding agents working on this repo — read it first
```

See `AGENTS.md` for the full architecture diagram, team roles, and file ownership, and
`docs/collab.md` for how agents and the three reviewing leads work together.

## Quickstart (5 minutes)

Read-only: Piperoom investigates and proposes fixes, but nothing executes. You need Go 1.27
(plus Node 24 for the UI) or Docker, and your Kafka Connect, Kafka, Schema Registry, source
Postgres and warehouse, with the read-only roles of `docs/pilot/permissions.md`. `discover`
reads Connect, Kafka and the Registry. The server pings the source Postgres and the warehouse
at start and will not start if either is down; it connects to Connect, Kafka and the Registry
only when a tool first uses them. Point the standard variables at them (`CONNECT_URL`,
`KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `SOURCE_DB_URL`, `WAREHOUSE_DB_URL`); unset, they
default to the local dev stack below. Set `ANTHROPIC_API_KEY` for the agent.

```sh
make quickstart                                          # Go: discover, build the UI, start
docker compose -f deploy/docker-compose.quickstart.yml up --build   # or Docker (host network)
```

`cmd/discover` writes `owners.yaml` with every layer owned by you (an existing file is kept;
`FORCE=1` rediscovers), then `server --quickstart` starts with in-memory rooms and dev login on
127.0.0.1 only, and prints the URL to open. The room says fixes are suggestions: approvals are
recorded, nothing runs. Send a test alert with the `curl` in `docs/pilot/quickstart.md`.

**Batch pipelines (Airflow + dbt → warehouse).** There is no `--kind` flag: the kind is a field of
`owners.yaml`, and `cmd/discover` only finds CDC pipelines, so you write the batch pipeline by hand.
Add a pipeline with `kind: batch`, its `dags` and/or `models`, and the three batch layers
(`orchestrator`, `transform`, `warehouse`); the file shape is in `docs/pilot/setup.md` (§ batch).
Then set the readers' variables (`AIRFLOW_URL` with `AIRFLOW_USERNAME` and `AIRFLOW_PASSWORD`, or
`AIRFLOW_TOKEN`; `DBT_ARTIFACTS_DIR`, or `DBT_CLOUD_URL` with its token, account and job; and
`WAREHOUSE_DB_URL` with `WAREHOUSE_KIND`, as in `docs/pilot/setup.md`) and start with that file:

```sh
make quickstart QUICKSTART_OWNERS=owners.yaml   # an existing file is reused, discover is skipped
```

The batch tools are read-only, like the rest of the quickstart. The Airflow, dbt and warehouse
roles are in `docs/pilot/permissions.md`.

Write mode is a separate, full-server setup: `PIPEROOM_WRITE_MODE=on` plus the Connect write
role in `docs/pilot/permissions.md` (`--quickstart` refuses it); see `docs/pilot/setup.md`.

## Running the dev stack

Requires Docker. From the repo root — `devctl` shares one dev stack across everyone working
locally, so every mutating command needs the shared lock first (`docs/runbook.md` has the
full story):

```sh
go run ./cmd/devctl lock --owner <tag>          # take the shared stack lock
go run ./cmd/devctl reset --owner <tag>         # start clean: stack up, connector registered, devconsumer running
go run ./cmd/devctl status                      # one-screen health: connector, replication slot, consumer group, warehouse
go run ./cmd/devctl load --rate 20 --owner <tag>        # generate traffic in a second terminal
go run ./cmd/devctl break schema-change --owner <tag>   # trigger a failure scenario
go run ./cmd/devctl fix schema-change --owner <tag>     # apply its real-world fix
go run ./cmd/devctl down --owner <tag>          # stop the stack
go run ./cmd/devctl unlock --owner <tag>        # release the lock when you're done
```

See `dev/scenarios.md` for what each of the 4 scenarios does and looks like, and
`docs/runbook.md` for day-to-day operation, the stack lock, and common errors.

## Running tests

```sh
go test ./...                              # unit tests — no Docker or network needed
go test -tags integration ./internal/...   # integration tests — needs the dev stack running
make evals                                 # validate eval scenarios — no model calls, no API key, no network
cd web && npm ci && npm run lint && npm run build && npx vitest run
```

CI (`.github/workflows/ci.yml`) runs on every pull request and on push to `main`: the Go
job (gofmt, vet including the build-tagged files, build, unit tests, and the eval scenario
check `go run ./evals/cmd/evalrun -validate`), the integration tests against a room-only
Postgres (`internal/store` and `internal/room` run for real; tests that need the rest of the
dev stack skip), the race detector, govulncheck, the deploy manifests (Helm, Terraform, no
dev auth in the image) and the web job. The OIDC end-to-end test runs only by hand
(`workflow_dispatch`). Live eval runs call the model and need `ANTHROPIC_API_KEY`; see
`evals/README.md`.

### Web tooling: npm install scripts

`web/node_modules` has one package, `esbuild`, that needs its `postinstall` script to fetch
the right platform binary; nothing else in the dependency tree does. CI does not grant a
blanket `--ignore-scripts=false` for that — it disables every install script
(`npm ci --ignore-scripts`) and then runs only esbuild's install script by hand
(`node node_modules/esbuild/install.js`). Do the same locally if you ever run with
`--ignore-scripts` turned on in your own `.npmrc`; a plain `npm ci` (scripts enabled) also
works fine for local development.

## Pilot onboarding: draft the owners file

`OWNERS_FILE` (default `dev/owners.yaml`) tells the server which pipelines exist and who
owns each layer; `USERS_FILE` (default `dev/users.yaml`) lists the user ids it may name.
For a new cluster, draft the owners file instead of writing it by hand:

```sh
go run ./cmd/discover -out owners.draft.yaml              # or: make discover-draft
go run ./cmd/discover -out owners.draft.yaml -source-db   # also check replication slots in SOURCE_DB_URL
go run ./cmd/discover -out -                              # print the draft to stdout
go run ./cmd/discover -out owners.draft.yaml -force       # overwrite an existing draft
```

`discover` only reads: Kafka Connect (`CONNECT_URL`), Kafka (`KAFKA_BROKERS`), the Schema
Registry (`SCHEMA_REGISTRY_URL`) and, with `-source-db`, the source Postgres
(`SOURCE_DB_URL`), with the credential and TLS settings from `internal/config/config.go`
(`KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_*`, `KAFKA_TLS_*`, `CONNECT_*`,
`SCHEMA_REGISTRY_*`). It never writes to the cluster. `-timeout` (default `2m`) bounds the
whole run. Without `-force` it refuses to overwrite an existing `-out` file.

The draft has one pipeline per Debezium Postgres source connector, in the
`dev/owners.yaml` shape: `id`, `topic_prefix`, `source_tables`, `connectors` and
`consumer_groups` are filled from the cluster; notes (e.g. "source_tables guessed from
existing topics", "slot … inactive") sit in comments above each pipeline, and skipped
connectors and warnings are listed at the end. A human then fills in every `TODO`:

- `title` — a human-friendly name;
- `sink_tables` (warehouse `schema.table`s) and `sink_ts_column`;
- `source_tables` when the draft left them empty;
- each entry under `layers` — a user id from `USERS_FILE`.

Review it, then copy it into the file `OWNERS_FILE` points at by hand. `discover` never
writes `OWNERS_FILE` itself. Re-running it is covered in `docs/runbook.md`.

## Slack bridge

The server starts the bridge when `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID` are both set
(`cmd/server/slack.go`, `internal/config/config.go`); with neither set it logs
`slack bridge disabled` and runs without it. Setting only one of the two is a start-up
error, not "off": the server exits with
`level=ERROR msg="server: failed" err="server: slack bridge: slackbridge: token and channel id must be set together"`.
`PIPEROOM_PUBLIC_URL`, when set, adds a link to the room in each thread's first message.

The bridge mirrors each incident room into one thread in one Slack channel. It is
**outbound only**: nothing typed in Slack reaches a room, and approvals happen only in
Piperoom.

- **Mirrored:** room opened (severity, title, pipeline id, link to the room when a public
  URL is set), agent hypotheses, proposals, approvals and rejections, executed actions,
  hand-offs created and accepted, room closed.
- **Never mirrored:** tool calls and tool `Data`, the raw alert payload, agent status and
  messages, comments, steering, control events, pinned traces.

Slack app setup:

1. Create a Slack app with a bot token that has only the **`chat:write`** scope.
2. Use a **private** channel — Slack reaches more people than the room does, and the thread
   repeats proposals and hand-offs. Invite the bot to it.
3. Note the channel **ID** (like `C0123ABC`, from the channel details), not its `#name`;
   the bridge rejects names.

Give the server the token and channel ID (and the public web URL for room links) through
the env vars above. Operations (token rotation, turning it off, error codes) are in
`docs/runbook.md`.

## Running a pilot (Docker, login)

`deploy/README.md` is the guide: one image, one compose file, its own Postgres. The image and the
compose file start in **oidc** mode (real single sign-on through your identity provider) and refuse
to start, naming the missing variable, until `PIPEROOM_PUBLIC_URL`, `PIPEROOM_OIDC_ISSUER`,
`PIPEROOM_OIDC_CLIENT_ID` and `PIPEROOM_OIDC_CLIENT_SECRET` are set. Passwordless **dev** login
(pick a user, no password) is only for a laptop, a VPN or an SSH tunnel, and exists only if you ask
for it with `-f deploy/docker-compose.dev-auth.yml`. A TLS reverse proxy is supported in oidc mode
only. The setup, the settings, who may log in and what each
role can do, and the troubleshooting table are in
[deploy/README.md](deploy/README.md#single-sign-on-oidc). The pilot limits are in
[docs/pilot/README.md](docs/pilot/README.md).

## How the team works

Ten coding agents own separate parts of the codebase and raise pull requests; three Claude
leads review every PR — architecture/completeness, code review for local agents, and code
review for cloud agents — and only one of them merges to `main`. Roles, ownership, and the
contract files nobody but the leads may edit are in `AGENTS.md` §2; the day-to-day
collaboration protocol is in `docs/collab.md`.
