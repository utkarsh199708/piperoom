# Developing Piperoom

How the repository is laid out, how to run the dev stack and the tests, and what CI checks. Coding agents read
[`AGENTS.md`](../AGENTS.md) first: it holds the architecture, the file ownership and the coding rules.

## Repository map

```
cmd/
  server/        the room service: HTTP API, WebSocket gateway, agent runner, executor, watcher
  piperoom/      umbrella binary: piperoom server | discover | investigate | setup
  setup/         piperoom-setup: the setup runner (pairs with a server, or --standalone)
  discover/      drafts an owners file from a live cluster (read-only)
  investigate/   CLI entry point for a one-shot agent investigation
  devctl/        local pipeline control (lock/unlock/up/down/reset/status/load/break/fix)
  devconsumer/   sample order-sync consumer used by the dev stack and scenarios
  loadtest/      capacity and isolation runs against a real server
internal/
  room/          the incident room: folds the append-only event log, drivers, proposals, approvals
  httpapi/       the room service over HTTP, exactly per docs/api.md
  ws/            WebSocket gateway: replay events after seq N, then live
  store/         events.Store on Postgres (the room_events log)
  agent/         the investigation loop, its contract and the model adapters (agent/llm)
  tools/         read-only tools the agent calls, grouped by area
  actions/       action implementations an approved proposal runs (the only writes)
  clients/       Postgres, MySQL, warehouse, Connect, Schema Registry, Kafka, Airflow, dbt, ... clients
  topology/      the pipeline graph used to localize a fault
  owners/        who owns which pipeline layer, for approvals
  watch/         the built-in watcher: polls Connect and the source DB and opens a room when a pipeline is in trouble (PIPEROOM_WATCH_MODE)
  alertin/       verifies alert webhooks (Alertmanager, Grafana, PagerDuty, Datadog, generic) and maps them to a pipeline
  auth/          login settings (dev login, OIDC) and sessions
  setup/         the setup runner, its typed operation catalogue, targets and sources
  slackbridge/   mirrors a room into one Slack thread (wired in cmd/server)
  redact/        masks secrets in untrusted text before it reaches the model
  injectscan/    flags prompt-injection text inside untrusted data
  events/        the append-only room event log types
  config/        environment variable loading (read config only through config.Load())
  wiring/        constructs the real stack clients and tool registries
  stacklock/     the shared dev-stack lock devctl uses
  testenv/       integration-test helpers (skip when the stack is unreachable or locked)
evals/           agent eval harness and scenarios (cmd/evalrun)
dev/
  docker-compose.yml   Postgres, Kafka, Schema Registry, Connect, devconsumer
  scenarios.md         the failure scenarios devctl can break/fix
  owners.yaml, users.yaml   pipeline layer ownership and dev users
web/             the React room UI
deploy/          the pilot image's compose files, Helm chart, Terraform
migrations/      the room database schema
docs/            API contract (docs/api.md), design docs, pilot guides, runbook
tasks/           one task file per agent and the wave boards, read before writing code
AGENTS.md        rules for coding agents working on this repo — read it first
```

See [`AGENTS.md`](../AGENTS.md) for the full architecture diagram, team roles, and file ownership, and
[`docs/collab.md`](collab.md) for how agents and the reviewing leads work together.

## The dev stack

Requires Docker. From the repo root — `devctl` shares one dev stack across everyone working
locally, so every mutating command needs the shared lock first ([`docs/runbook.md`](runbook.md) has the
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

See [`dev/scenarios.md`](../dev/scenarios.md) for what each scenario does and looks like,
[`docs/demo/schema-change.md`](demo/schema-change.md) for the scripted live demo, and
[`docs/runbook.md`](runbook.md) for day-to-day operation, the stack lock, and common errors.

## Running tests

```sh
go test ./...                              # unit tests — no Docker or network needed
go test -tags integration ./internal/...   # integration tests — needs the dev stack running
make evals                                 # validate eval scenarios — no model calls, no API key, no network
cd web && npm ci && npm run lint && npm run build && npx vitest run
```

## Continuous integration

CI (`.github/workflows/ci.yml`) runs on every pull request and on push to `main`: the Go
job (gofmt, vet including the build-tagged files, build, unit tests, and the eval scenario
check `go run ./evals/cmd/evalrun -validate`), the integration tests against a room-only
Postgres (`internal/store` and `internal/room` run for real; tests that need the rest of the
dev stack skip), the race detector, govulncheck, the deploy manifests (Helm, Terraform, no
dev auth in the image) and the web job. The OIDC end-to-end test runs only by hand
(`workflow_dispatch`). Live eval runs call the model and need `ANTHROPIC_API_KEY`; see
[`evals/README.md`](../evals/README.md). CI also checks the supported-model list of the other providers: every row of
`internal/agent/llm/supported.json` needs a 3-run baseline that passes `evalrun -gate` against the Claude
reference, and no `sunset` may have passed. A model of another provider (OpenAI and compatible hosts, Gemini,
Bedrock) is gated by dispatching `evals-live` with its `provider`, `flavor` and `model`; the provider's key is a
secret of the `evals` environment and a missing one skips the run. See "Gate run of a model provider" in
[`docs/runbook.md`](runbook.md); the published model table is `docs/pilot/models.md` (added by V14).

The setup runner has its own workflows. `setup-matrix.yml` runs on every pull request and on
push to `main`: the setup unit tests, the path table tests of `internal/setup/ops` and the
Docker endpoint detection on ubuntu, macOS and Windows, plus the Compose integration on
ubuntu (and on macOS when the runner has Colima; otherwise it is a documented skip).
`setup-nightly.yml` runs the Compose integration on the owner's Windows machine (a
self-hosted runner, label `piperoom-r1`, Docker Desktop in Linux-container mode), schedule
and manual only: it never runs on a pull request, so it is not a required check and a
checkout without that runner never shows it as pending. Its `go test -json` export is kept
as an artifact for 30 days. The repository is public, so before registering that runner set
"Require approval for all external contributors" in the Actions settings and run it under a
dedicated unprivileged Windows account (see the workflow's header).

### Web tooling: npm install scripts

`web/node_modules` has one package, `esbuild`, that needs its `postinstall` script to fetch
the right platform binary; nothing else in the dependency tree does. CI does not grant a
blanket `--ignore-scripts=false` for that — it disables every install script
(`npm ci --ignore-scripts`) and then runs only esbuild's install script by hand
(`node node_modules/esbuild/install.js`). Do the same locally if you ever run with
`--ignore-scripts` turned on in your own `.npmrc`; a plain `npm ci` (scripts enabled) also
works fine for local development.

## How the team works

Coding agents own separate parts of the codebase and raise pull requests; three Claude
leads review every PR — architecture/completeness, code review for local agents, and code
review for cloud agents — and only one of them merges to `main`. Roles, ownership, and the
contract files nobody but the leads may edit are in [`AGENTS.md`](../AGENTS.md) §2; the day-to-day
collaboration protocol is in [`docs/collab.md`](collab.md), and the no-human-approval ship workflow for board rows
is [`docs/process/ship-workflow.md`](process/ship-workflow.md).
