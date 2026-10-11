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
  slackbridge/   mirrors a room into one Slack thread (library; not wired into the server yet)
  stacklock/      the shared dev-stack lock devctl uses
  testenv/       integration-test helpers (skip when the stack is unreachable or locked)
  tools/         read-only tools the agent calls, grouped by area
  topology/      the pipeline graph used to localize a fault
  events/        the append-only room event log types
  config/        environment variable loading (read config only through config.Load())
web/             the React room UI
docs/            API contract (docs/api.md), this repo's collaboration rules, runbook
tasks/           one task file per agent, read before writing code
AGENTS.md        rules for coding agents working on this repo — read it first
```

See `AGENTS.md` for the full architecture diagram, team roles, and file ownership, and
`docs/collab.md` for how agents and the three reviewing leads work together.

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

CI (`.github/workflows/ci.yml`) runs the Go unit-test job (including the eval scenario
check, `go run ./evals/cmd/evalrun -validate`) and the web job on every pull request and on
push to `main`; integration tests need the dev stack and are not run in CI. Live eval runs
call the model and need `ANTHROPIC_API_KEY`; see `evals/README.md`.

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

**Status:** the `internal/slackbridge` library is merged; wiring it into `cmd/server` is
pending in PR #24. On `main` today nothing reads `SLACK_BOT_TOKEN`, `SLACK_CHANNEL_ID` or
`PIPEROOM_PUBLIC_URL`, so setting them has no effect yet. This section describes what the
bridge does so the Slack side can be prepared.

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

Once PR #24 lands, the token, channel ID and public web URL will be given to the server
through the env vars above; the token and channel must be set together. Operations
(token rotation, turning it off, error codes) are in `docs/runbook.md`.

## How the team works

Ten coding agents own separate parts of the codebase and raise pull requests; three Claude
leads review every PR — architecture/completeness, code review for local agents, and code
review for cloud agents — and only one of them merges to `main`. Roles, ownership, and the
contract files nobody but the leads may edit are in `AGENTS.md` §2; the day-to-day
collaboration protocol is in `docs/collab.md`.
