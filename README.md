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
  devctl/        local pipeline control (up/down/reset/status/load/break/fix)
  devconsumer/   sample order-sync consumer used by the dev stack and scenarios
  investigate/   CLI entry point for a one-shot agent investigation
dev/
  docker-compose.yml   Postgres, Kafka, Schema Registry, Connect, devconsumer
  scenarios.md         the 4 observed failure scenarios devctl can break/fix
  owners.yaml, users.yaml   pipeline layer ownership and dev users
internal/
  agent/         the Claude investigation loop and its contract
  clients/       Postgres, warehouse, (and in-progress) Connect/Schema Registry/Kafka clients
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
`docs/collab.md` for how agents and the two reviewing leads work together.

## Running the dev stack

Requires Docker. From the repo root:

```sh
go run ./cmd/devctl reset    # start clean: stack up, connector registered, devconsumer running
go run ./cmd/devctl status   # one-screen health: connector, replication slot, consumer group, warehouse
go run ./cmd/devctl load --rate 20   # generate traffic in a second terminal
go run ./cmd/devctl break schema-change   # trigger a failure scenario
go run ./cmd/devctl fix schema-change     # apply its real-world fix
go run ./cmd/devctl down     # stop the stack
```

See `dev/scenarios.md` for what each of the 4 scenarios does and looks like, and
`docs/runbook.md` for day-to-day operation and common errors.

## Running tests

```sh
go test ./...                              # unit tests — no Docker or network needed
go test -tags integration ./internal/...   # integration tests — needs the dev stack running
cd web && npm ci && npm run lint && npm run build && npx vitest run
```

CI (`.github/workflows/ci.yml`) runs the Go unit-test job and the web job on every pull
request and on push to `main`; integration tests need the dev stack and are not run in CI.

### Web tooling: npm install scripts

`web/node_modules` has one package, `esbuild`, that needs its `postinstall` script to fetch
the right platform binary; nothing else in the dependency tree does. CI does not grant a
blanket `--ignore-scripts=false` for that — it disables every install script
(`npm ci --ignore-scripts`) and then runs only esbuild's install script by hand
(`node node_modules/esbuild/install.js`). Do the same locally if you ever run with
`--ignore-scripts` turned on in your own `.npmrc`; a plain `npm ci` (scripts enabled) also
works fine for local development.

## How the team works

Six coding agents own separate parts of the codebase and raise pull requests; two Claude
leads review every PR (code review and architecture/completeness), and only one of them
merges to `main`. Roles, ownership, and the contract files nobody but the leads may edit are
in `AGENTS.md` §2; the day-to-day collaboration protocol is in `docs/collab.md`.
