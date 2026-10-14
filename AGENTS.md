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

One team, clear roles:

| Who | Slack tag | Does | Never does |
|---|---|---|---|
| **Claude Lead 1 — Architecture** | `[LEAD-ARCH]` | completeness vs. task file and milestones, architecture and contract fit, integration runs, answers questions, **the only one who merges** | write feature code |
| **Claude Lead 2 — Code review** | `[LEAD-REVIEW]` | line-by-line code review, runs build/vet/unit/integration tests, security and error paths (§7) | merge, write feature code |
| **Qoder agents** | `[A1]`…`[A6]` | **write all the code**, raise PRs, fix review comments | merge, touch `main`, edit contract files |
| **Claude cloud agents** (Sonnet 5, cloud routines on GitHub) | `[C1]`…`[C4]` | follow-up tasks that need no local stack; open GitHub PRs | merge, push to `main`, edit contract files |
| **Claude Lead 3 — Cloud lead** | `[LEAD-CLOUD]` | leads the cloud agents (C1…): watches their cloud sessions, relays their status to Slack, code-reviews their GitHub PRs (instead of Lead 2), starts round-N fix runs | merge, write feature code |

A PR is merged only when **both** leads approve: `[LEAD-REVIEW] APPROVED` (code) and
`[LEAD-ARCH] APPROVED` (architecture + completeness). Then Lead 1 merges.
For cloud-agent (C*) PRs the code approval comes from `[LEAD-CLOUD]` instead of `[LEAD-REVIEW]`.
Each agent owns folders. **Never edit a file you do not own.**

| Agent | Task file | Owns |
|---|---|---|
| A1 (first version by Claude, branch `a1/devstack`; Qoder owns follow-ups) | `tasks/A1-devstack.md` | `dev/**`, `cmd/devctl/**`, `cmd/devconsumer/**`, `internal/testenv/**` |
| A2 | `tasks/A2-kafka-tools.md` | `internal/clients/connect/**`, `internal/clients/schemareg/**`, `internal/clients/kafka/**`, `internal/tools/kafkatools/**`, `internal/actions/**` (not `action.go`) |
| A3 | `tasks/A3-db-tools-topology.md` | `internal/clients/postgres/**`, `internal/clients/warehouse/**`, `internal/tools/dbtools/**`, `internal/tools/scope.go`, `internal/topology/**` (not `graph.go`), `internal/owners/**`, `internal/clients/pgconv/**` |
| A4 | `tasks/A4-agent-loop.md` | `internal/agent/**` (not `contract.go`), `cmd/investigate/**`, `internal/wiring/**` (shared client/tool wiring) |
| A5 | `tasks/A5-room-service.md` | `migrations/**`, `internal/store/**`, `internal/room/**`, `internal/httpapi/**`, `internal/ws/**`, `cmd/server/**`, `internal/auth/**` (A5 created it in auth PR 2, `e6dc2a1`) |
| A6 | `tasks/A6-web.md` | `web/**` |
| A7 | `docs/design/watcher.md` (watcher), `docs/design/dlq.md` | `internal/watch/**`, `internal/dlq/**` |
| C1 (cloud) | `tasks/C1-ci-repo.md` | `.github/**`, `README.md`, `docs/runbook.md`, `Makefile` |
| C2 (cloud) | `tasks/C2-agent-memory.md` | done — merged `9519d23`; `internal/agent/**` belongs to A4 again |
| C3 (cloud) | `tasks/C3-snowflake.md` | `internal/clients/snowflake/**` |
| C4 (cloud) | `tasks/C4-security.md` | `docs/security/**` (no code changes) |
| C5 (cloud) | `tasks/C5-alert-ingest.md` | `internal/alertin/**` |
| C6 (cloud) | `tasks/C6-agent-evals.md` | `evals/**` except `evals/scenarios/*.yaml` (other than `schema-change.yaml`) and `evals/scenarios/README.md` |
| C7 (cloud) | `tasks/C7-incident-scenarios.md` | `evals/scenarios/*.yaml` (not `schema-change.yaml`), `evals/scenarios/README.md` |
| C8 (cloud) | `tasks/C8-secure-clients.md` | done — merged `7c7bdac`; client folders back to A2, `internal/clients/netsec/**` to A2 |
| C9 (cloud) | `tasks/C9-security-design.md` | `docs/design/**` (no code changes) |
| C10 (cloud) | `tasks/C10-pipeline-discovery.md` | `cmd/discover/**`, `internal/discover/**` |
| C11 (cloud) | `tasks/C11-runbook-tool.md` | `internal/tools/runbooks/**` |
| C12 (cloud) | `tasks/C12-slack-bridge.md` | `internal/slackbridge/**` |
| C13 (cloud) | `tasks/C13-prompt-audit.md` | `docs/audits/**` — delivered as PR #12 |
| C14 (cloud) | `tasks/C14-pilot-onboarding.md` | `docs/pilot/**` (no code changes) |
| Claude leads | — | `AGENTS.md`, `tasks/**`, `docs/**`, `reviews/**`, and every **contract file** below |

Cloud sessions started from claude.ai/code are bound to their own `claude/...` branch; that is accepted. Their PR
title starts with `[C<N>]`.

### Earlier waves: E, T, N, S (all merged by 2026-10-14)

These tags had no task file; their scope was in the dispatch message and the design doc named. A tag that created a
package owns it for follow-ups. A tag that only extended other agents' folders owns nothing now: those folders stay
with the owners above, who carry any follow-up.

| Tag | PR | Topic | Owns now |
|---|---|---|---|
| E1 | #109 | topology + prompt: one dependency graph | nothing (edited A2, A3, A4, A6 folders) |
| E2 | #110 | evals: evidence scoring, calibration, abstention | `evals/score.go`, `evals/calibration.go` (`docs/design/cost.md`) |
| E3 | #119 | evals: phased replay, held-out set | `evals/gen/**`, `evals/scenarios/heldout/**` |
| E4 | #117 | usage telemetry, durable spend limits | nothing (A4, A5, A6, C6 folders); design `docs/design/cost.md` |
| T1 | #112 | tenancy | nothing (A3, A4, A5, A7 folders); design `docs/design/tenancy.md` |
| T2 | #107 | tool scope for db/cluster-wide metadata | nothing (A2, A3, A7 folders) |
| T3 | #115 | auth: approval lifecycle, display-name impersonation | nothing (`internal/auth/**` is A5's; also A5 folders) |
| T4 | #118 | retention, caps, deletion, archive | nothing (A1, A3, A5 folders); design `docs/design/retention.md` |
| N1 | #92 | runbooks: BM25 search | nothing (`internal/tools/runbooks/**` stays C11) |
| N2 | #94 | evals: token matching, LLM judge | nothing (C6, C7 folders) |
| N3 | #91 | trace normalise + fingerprint | `internal/textnorm/**` |
| N4 | #100 | `similar_incidents` tool | `internal/tools/similarincidents/**`, `internal/textsearch/**` |
| N5 | #96 | redact secrets in untrusted tool text | `internal/redact/**` |
| N6 | #95 | injection scan of untrusted tool data | `internal/injectscan/**` |
| N7 | #93 | hand-off summary checker | nothing (`internal/agent/**` stays A4) |
| N8 | #97 | evals: shadow agreement scoring | nothing (C6 folders) |
| S1 | #121 | replicas: leader, room leases, executor | nothing (A5 folders, `deploy/helm/**`); design `docs/design/replicas.md` |
| S2 | #122, #123 | load-test harness, multi-replica runs | `cmd/loadtest/**`; `internal/loadtest/**` (created by O5 in #77, extended by S2; the O wave has no row); its runs live in `reviews/capacity-2026-10/` (leads' folder) |

### R wave (dispatched 2026-10-14)

PR titles start with `[R<N>]` (e.g. `[R5b] wiring: tools get Reader clients`). An R task is done by the agent named
here, **inside that agent's own folders above**; a task that needs a file outside them asks in its thread (§4 of
`docs/collab.md`), it does not edit it. R0 (the leads) changes the contracts first; "Uses" names the R0 PR 1 items
(#138) each task codes against.

| Task | Agent | Folders | Uses (R0 PR 1 item) |
|---|---|---|---|
| R0 | leads (`[LEAD-ARCH]`) | contract files, `AGENTS.md`, `docs/**`, the `concurrency:` block of `.github/workflows/ci.yml` | — |
| R1 | local (run on the Windows dev machine, not a cloud session) | as dispatched | — |
| R2 | A1 | A1 folders (`dev/**`, `cmd/devctl/**`, `cmd/devconsumer/**` except its `Dockerfile`, `internal/testenv/**`) and `docs/demo/**` | 6 (`PIPEROOM_DEV_ACK`, log vars in the dev stack) |
| R3 | A6 | `web/**` | 1, 2, 3, 4, 5 |
| R4 | A5 | A5 folders | 1, 2, 4 |
| R5a | A5 | A5 folders (`cmd/server`, `internal/room` registries) | 9 |
| R5b | A4 | `internal/agent/**`, `internal/wiring/**`, `cmd/investigate/**` | 7, 9 |
| R5c | A2 | A2 folders (`internal/tools/kafkatools/**`, `internal/actions/**` minus `action.go`) | 7, 9 |
| R5d | C5 (cloud) | `internal/alertin/**` | — |
| R6, R7 | C1 (cloud) | C1 folders: `.github/**` **except the `concurrency:` block of `ci.yml`** (R0 owns it), `Makefile`, `README.md`, `docs/runbook.md`. For R6 also `Dockerfile`, `cmd/devconsumer/Dockerfile`, `deploy/**` and the `go` line of `go.mod` (no baked dev auth, OIDC default, Go 1.27.2: #127). R7's #133 also edits `internal/tools/dbtools/integration_test.go` (A3): ask A3 in thread | 6, 8 (ldflags and version build arg in `Makefile`, CI, `Dockerfile`) |
| R8a | C6 (cloud) | `evals/**` (C6 rows above) | — |
| R8b | A4 | A4 folders | — |
| R9 | A5 | A5 folders | 6, 8 |
| R10 | A6 | `web/**` | — |
| R11 | A5 (driver leases, suggestion decline), C12 (Slack paging, `internal/slackbridge/**`), A4 (tool `Data` cap, A4 folders) | as listed | A5: 3, 5. C12's #130 does not use 5 yet: paging on `control.lease_expired` is a follow-up C12 PR after the A5 R11 PR |
| R12 | A6 + A1 | `web/**` (A6), A1 folders (A1) | — |

A task split between two agents (R11, R12) is one PR per agent, titled as usual (`[R11] slackbridge: ...`,
`[R11] agent: ...`); the folders tell the agents apart.

### Contract files (read them, code against them, never edit them)

- `internal/tools/tool.go` — `Tool`, `Result`, `Registry`
- `internal/events/events.go` — event types, payloads, `Store`
- `internal/clients/clients.go` — client interfaces + DTOs
- `internal/actions/action.go` — `Action`, `Registry`
- `internal/agent/contract.go` — `Inbox`, `Sink`, `Proposals`, `Alert`, `Deps`, `Options`,
  `Runner`, `HandoffWriter` (`internal/agent/stubs.go` holds placeholders; A4 replaces it)
- `internal/topology/graph.go` — `Graph`, `Node`, `Edge`, `Builder`
- `internal/config/config.go` — environment variables
- `internal/version/version.go` — build version (`-ldflags -X`), reported by `/healthz`
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
- **Only Lead 1 (Claude, `[LEAD-ARCH]`) merges into `main`**, after both leads approve. Agents never commit, merge, rebase onto, reset or
  push `main`. You raise a PR: commit on your branch, then post `[A<N>] READY FOR REVIEW` (local repo)
  or open a pull request (if the repo is on GitHub). A git hook rejects any change to `main` from agents.
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

## 7. How the leads check your PR

Lead 2 (code review) checks every item below; Lead 1 (architecture) checks the task file's
"Done when", the milestone, and fit with the contracts and other parts. A PR that fails any item
gets `CHANGES REQUESTED`:

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

### Merge order (R wave)

Lead 1 merges in this order. A PR whose predecessor has not merged waits even if both leads approved it; its
author rebases (merges `main` into the branch for a cloud PR) once the predecessor lands.

0. **Done:** PR #101 (`[D2]` Kafka client `DLQOffsets` / `PeekDLQ`), merged 2026-10-14. Nothing waits on it.
1. **R0 PR 1** (#138, contracts). First, alone. It also carries the `gofmt` fix of `internal/clients/kafka/dlq.go`
   that makes `main` green again; every R PR needs it.
2. **R4, R5b, R9** (A5 routes and events; A4 read-only wiring; A5 log/dev-ack/version). Any order among them; they
   touch different packages. R5a and R5c may merge with or after R5b.
3. Only after **all** of step 2: **#146** (R3, calls R4's routes) and **#147** (the A5 R11 PR: decline and leases, built on
   R4's hand-off and lease code in `internal/room`), and any later PR that uses items 1–5.
4. Everything else after R0 PR 1, in any order, unless its thread says it waits on another PR: R1, R2, R5d, R6,
   R7, R8a, R8b, R10, R12, and the R3/R11 PRs that use none of items 1–5 (#135 web verified badge, #130 slackbridge,
   #136 agent Data cap).

R0 PR 2 (process: this section, the CI `concurrency:` fix, `docs/pilot/decision.md`) has no code dependency and may
merge at any time; merging it before R6/R7 keeps their `ci.yml` diffs off the `concurrency:` block.

