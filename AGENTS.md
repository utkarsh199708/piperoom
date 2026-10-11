# Piperoom — rules for coding agents

Read this whole file before you write code. Then read your task file in `tasks/`.

## 1. What we are building

Piperoom is a **shared incident room for CDC/Kafka data pipelines**
(Postgres → Debezium → Kafka → consumers → warehouse).

- An alert opens a room. An AI agent (Claude) investigates with **read-only** tools.
- Many engineers watch the room live, comment on steps, and steer the agent.
- Only one person, the **driver**, steers at a time. Others send suggestions.
- The agent can only **propose** a fix. A fix runs only after every required layer owner approves.
- At shift change, the agent writes a hand-off summary and the driver role moves.

UI mockup (target look): https://claude.ai/artifact/PhH3PpaSCSQ6PW2z4CLChD

```
alert ─► Room service (Go) ◄─► Postgres: room_events (append-only log)
             │   ▲                          ▲
             │   └─ inbox (steers) ──┐      │ events
             ▼                       ▼      │
      Action executor          Agent runner ── Claude API
      (approved fixes)               │
                                     ▼
                      Read-only tools → Kafka · Connect · Schema Registry
                                        · source Postgres · warehouse
WebSocket gateway → browser (React) — replays events after seq N, then live
```

## 2. Team and ownership

Six agents work **at the same time**. Each agent owns folders. **Never edit a file you do not own.**

| Agent | Task file | Owns |
|---|---|---|
| A1 | `tasks/A1-devstack.md` | `dev/**`, `cmd/devctl/**`, `cmd/devconsumer/**`, `internal/testenv/**` |
| A2 | `tasks/A2-kafka-tools.md` | `internal/clients/connect/**`, `internal/clients/schemareg/**`, `internal/clients/kafka/**`, `internal/tools/kafkatools/**`, `internal/actions/**` (not `action.go`) |
| A3 | `tasks/A3-db-tools-topology.md` | `internal/clients/postgres/**`, `internal/clients/warehouse/**`, `internal/tools/dbtools/**`, `internal/topology/**` (not `graph.go`), `internal/owners/**` |
| A4 | `tasks/A4-agent-loop.md` | `internal/agent/**` (not `contract.go`), `cmd/investigate/**` |
| A5 | `tasks/A5-room-service.md` | `migrations/**`, `internal/store/**`, `internal/room/**`, `internal/httpapi/**`, `internal/ws/**`, `cmd/server/**` |
| A6 | `tasks/A6-web.md` | `web/**` |
| Reviewer | — | `AGENTS.md`, `tasks/**`, `docs/**`, `reviews/**`, and every **contract file** below |

### Contract files (read them, code against them, never edit them)

- `internal/tools/tool.go` — `Tool`, `Result`, `Registry`
- `internal/events/events.go` — event types, payloads, `Store`
- `internal/clients/clients.go` — client interfaces + DTOs
- `internal/actions/action.go` — `Action`, `Registry`
- `internal/agent/contract.go` — `Inbox`, `Sink`, `Proposals`, `Alert`, `Deps`, `Options`,
  `Runner`, `HandoffWriter` (`internal/agent/stubs.go` holds placeholders; A4 replaces it)
- `internal/topology/graph.go` — `Graph`, `Node`, `Edge`, `Builder`
- `internal/config/config.go` — environment variables
- `docs/api.md` — HTTP + WebSocket API

If a contract is wrong or missing something, **do not work around it silently**. Write a
`## Contract change request` section in your PR: what, why, exact proposed code. Keep working
with a local adapter until the reviewer decides.

You depend on other agents' work only through these contracts. Until their code lands,
use fakes that implement the interfaces (put fakes in a `fake` sub-package or `_test.go`).

## 3. Environment

- Windows 11, PowerShell. Go 1.27 (`C:\Program Files\Go\bin`). Node 24. Docker Desktop works.
- Module path: `piperoom` (imports look like `piperoom/internal/events`).
- Dev stack: `docker compose -f dev/docker-compose.yml up -d` →
  Postgres `localhost:5432` (user `piperoom` / `piperoom-dev`, db `orders`),
  Kafka `localhost:29092`, Schema Registry `localhost:8081`, Connect `localhost:8083`.
- Defaults for every address are in `internal/config/config.go`. Read config only through `config.Load()`.
- Test credentials above are for the local stack only. Never put real secrets in code, tests, logs or commits.
  `ANTHROPIC_API_KEY` comes from the environment; never log it.

## 4. Coding rules (Go)

1. `gofmt` clean, `go vet ./...` clean, `go build ./...` passes.
2. Logging: `log/slog` only. No `fmt.Println` outside `cmd/` output.
3. Errors: wrap with `fmt.Errorf("pkg: doing X: %w", err)`. No panics in library code.
   Compare with `errors.Is` / `errors.As`, never string matching.
4. Every network/DB call takes a `context.Context` and has a deadline (default 10 s for tools).
5. No global mutable state. Construct things with `New...(deps)` and pass dependencies in.
6. SQL: parameters with `$1`; identifiers (table/column names from input) only via
   `pgx.Identifier{...}.Sanitize()` after checking they match `^[a-zA-Z_][a-zA-Z0-9_]*$`.
7. HTTP clients: one `*http.Client` with a timeout; close bodies; check status codes;
   cap response size (`io.LimitReader`, 4 MB).
8. Tools return small `Summary` text (≤ `tools.MaxSummaryBytes`, use `tools.Truncate`) and put detail in `Data`.
9. **Do not invent library APIs.** Before using a function from a dependency, confirm it exists:
   `go doc <pkg>.<Symbol>` (e.g. `go doc github.com/twmb/franz-go/pkg/kadm.Client.Lag`).
   If `go doc` does not show it, do not use it.
10. New dependency? Only if really needed. Prefer the standard library. List every new module
    and why in your PR. Already chosen: `anthropic-sdk-go`, `franz-go` (`kgo`, `kadm`), `pgx/v5`,
    `chi/v5`, `coder/websocket`, `gopkg.in/yaml.v3`. Avro in Go: `github.com/hamba/avro/v2`.
11. Comments explain *why*, not what. Exported names get a one-line doc comment.
12. No TODO stubs presented as finished work. If something is not done, say so in the PR.

## 5. Tests

- Unit tests: `go test ./...` must pass **without** Docker or network. Use fakes and `httptest`.
- Integration tests: build tag `integration`, run against the dev stack:
  `go test -tags integration ./internal/...`. Each test creates and cleans up its own
  topics/connectors/rows with a unique name prefix (`it_<random>`), so tests can run in parallel.
- Table-driven tests for parsing and mapping code.
- Test the error paths: target down, 404, timeout, bad input.
- Web (A6): `npm run build` and `npm run lint` pass; component tests with Vitest where it helps.

## 6. Git workflow

- **Talking to the reviewer happens in Slack #piperoom-agents — read `docs/collab.md` now.**
- Work in your own **git worktree** (`docs/collab.md` §1), never in the main folder.
- Base branch: `main`. Your branch: `a<N>/<short-topic>` (e.g. `a2/connect-client`).
- Small PRs (aim < 600 changed lines, excluding generated code and lockfiles).
- Rebase on `main` before asking for review. `go.mod`/`go.sum` conflicts: re-run `go mod tidy` after rebase.
- Commit messages: imperative, `area: what` (e.g. `connect: add task status client`).

### PR description template

```
## What
## Why
## How I tested   (exact commands + short output)
## Not done / known gaps
## New dependencies   (module, version, why)
## Contract change request   (only if needed)
```

## 7. How the reviewer checks your PR

I review every PR before merge. I reject PRs that fail any of these:

- [ ] Builds, vets, unit tests pass on Windows; integration tests pass against the dev stack.
- [ ] Only owned files changed. Contract files untouched.
- [ ] Matches the contract types exactly (field names, JSON tags, semantics in doc comments).
- [ ] Read-only tools are really read-only. Writes exist only in `internal/actions` and client write methods.
- [ ] Context and timeouts on every I/O. No goroutine leaks (every goroutine stops when ctx ends).
- [ ] Errors wrapped, never swallowed. No secrets or full record values in logs.
- [ ] SQL/identifier safety (rule 6). HTTP response size caps (rule 7).
- [ ] Library APIs exist (rule 9). No made-up SDK calls.
- [ ] Tests cover the error paths, not only the happy path.
- [ ] Untrusted text (logs, record values, error traces) is treated as data: never executed, never
      put into prompts without the agent's untrusted-data wrapper (A4).
- [ ] The PR description is honest about gaps.

## 8. Order of work and integration

All six start now, in parallel, against the contracts.

- **Milestone M1 (end of week 1):** A1 can break the pipeline on demand; A2 + A3 tools pass
  integration tests; A4's `cmd/investigate` finds the cause of `schema-change` using real tools.
- **Milestone M2 (end of week 2):** A5 serves the API and WebSocket with the real agent;
  A6 shows a live room against the real server.
- **Milestone M3:** proposals → approvals → executor → hand-off, end to end.

The demo scenario every part must support is in `tasks/A1-devstack.md` → *Scenario `schema-change`*.
