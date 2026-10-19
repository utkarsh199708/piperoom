# P1 — Read-only by default, one-command quickstart, a 60-line setup

Owners: P1a A5 · P1b C10 · P1c C1 · P1d C14 · P1e A6 · P1f A1.
Model: **Sonnet 5.5** for P1b, P1c, P1d, P1f (well-specified, mostly plumbing and docs); **Opus 5.5** for P1a
(the write-mode gate sits in the executor path) and P1e (the first screen a new user sees).
Plan: `docs/product/fit-2026-10.md` §1.3, §2 row 1. Uses P0 items 1, 3 (`kind` default only).

## Goal
A platform engineer with a Connect URL gets a live room in under five minutes:

```
piperoom quickstart --connect http://connect:8083        # or --source-db postgres://…
```
discovers the pipelines, writes `owners.yaml` with every layer owned by the current user, starts the server on
loopback in dev auth with the in-memory store and **no write mode**, prints the URL and opens it. The room
shows "Fixes are suggestions until write mode is on" and every approval records as usual but nothing executes.
`docs/pilot/setup.md` becomes a quickstart of about 60 lines and a reference that nobody has to read first.

## Evidence
- `docs/pilot/setup.md`: 704 lines; `internal/config/config.go`: 56 `PIPEROOM_*` variables.
- "Read-only" today means *do not grant write rights at Connect* (`setup.md` §7, line 665): the server has no
  switch, so a misconfigured role executes. P0 item 1 adds the switch.
- `cmd/discover` already drafts `owners.yaml` from Connect, Kafka, the Registry and `-source-db`, and never
  writes to the cluster (`cmd/discover/main.go:1-7`); it does not start anything.
- `cmd/server --demo` already runs the in-memory store (`cmd/server/main.go:255`) and dev auth on loopback needs
  no `PIPEROOM_DEV_ACK` (`cmd/server/authsetup.go`).
- `F7` is already answered by the per-pipeline `connections:` block (`internal/owners/tenancy.go`); the pilot
  docs still list it as open (`docs/pilot/README.md` F7).

## Scope — what to build

1. **P1a (A5): write mode in the server.** `cmd/server/main.go` reads `cfg.WriteMode` (P0 item 1). `off`:
   `wiring` is asked for read-only clients only (it already builds them, `internal/wiring/readonly.go`); the
   actions registry the executor holds is empty and the one the agent *proposes from* is the full registry
   (the agent must still propose; see `wiring.RegisterTools`'s comment, `wiring.go:294`). On the last approval
   the executor emits no `action.executed`; instead the proposal stays `approved` and the snapshot's
   `open_proposals[].blocked: "write_mode_off"` says why (`docs/api.md`, P0 item 1). `--demo` forces `on`.
   `/healthz` reports `write_mode`. `--quickstart` flag: implies `--demo`'s store choice (memstore) **without**
   the scripted agent, dev auth, loopback, `WriteMode=off`, `OWNERS_FILE` from `--owners` (default
   `./owners.yaml`), a one-line `users.yaml` written to a temp dir for the OS user name when `USERS_FILE` is
   unset; refuses any non-loopback `PIPEROOM_HTTP_ADDR`. On start it prints the URL once to stdout (allowed in
   `cmd/`) and logs everything else with `slog`.
2. **P1b (C10): `discover` writes a usable owners file.** `cmd/discover` gains `-write-owners <path>` (refuses to
   overwrite without `-force`, which exists), `-owner <user id>` (default: the OS user name; every layer of every
   pipeline gets `user:<id>`), `-users <path>` (writes a matching one-user `users.yaml`), and prints a summary
   table (pipelines, connectors, topics, sink tables found or missing). A connector whose sink table cannot be
   found gets `sink_tables: []` and a `# TODO` comment line, not a guess. The draft passes `owners.Load`
   (`internal/owners`) before it is written; a draft that does not is written as `<path>.draft` with the
   validation error at the top. No new dependency.
3. **P1c (C1): the one command and the short README.** A `cmd/piperoom/` launcher is **not** added (one binary per
   command is the repo's shape); instead `Makefile` gains `quickstart` (`go run ./cmd/discover -write-owners
   owners.yaml … && go run ./cmd/server --quickstart`), `deploy/docker-compose.quickstart.yml` runs the same two
   steps in the image (no room Postgres: `--quickstart` keeps rooms in memory; #618 ruling), and `README.md` §"Running the dev stack" is
   preceded by a §"Quickstart (5 minutes)" of at most 25 lines: prerequisites (Go or Docker, a reachable
   Connect), the command, what you see, how to turn write mode on (`PIPEROOM_WRITE_MODE=on` plus the Connect
   role from `permissions.md`). `.github/workflows/ci.yml` gains a job that runs `make quickstart` against the
   dev stack with a 5 minute budget and curls `/healthz` for `write_mode: "off"`.
4. **P1d (C14): docs.** `docs/pilot/setup.md` is split: `docs/pilot/quickstart.md` (≤ 60 lines: the P1c
   command, the three things to check in the UI, the one switch to write mode, the link to the reference) and
   `setup.md` kept as the reference with a new 10-line "Which variables matter first" table at the top (listen
   address, auth mode, owners and users files, `CONNECT_URL`, `KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`,
   `SOURCE_DB_URL`, `WAREHOUSE_DB_URL`, `PIPEROOM_WRITE_MODE`, `ANTHROPIC_API_KEY`); everything else moves under
   "Reference". `docs/pilot/README.md` F7 row becomes *fixed by `connections:`* with the citation;
   `decision.md` §4 names read-only (option B) as the default start and shadow as an opt-in toggle, not a phase.
   Every claim keeps its file citation (the pilot docs' own rule).
5. **P1e (A6): the read-only banner and the first-run screen.** With `snapshot.mode.write === false`: a
   persistent, dismiss-per-session banner under the room header, "Fixes are suggestions until write mode is on
   · how to turn it on" linking to the quickstart doc; the approval card's last button reads "Approve (will not
   run)" and, once every layer approved, the card shows "Approved · waiting for write mode" instead of the
   execution card. The empty room list gets a first-run panel: "No rooms yet. Send a test alert:" with the
   exact `curl` for `POST /api/alerts/webhook` (dev mode) copied from `docs/pilot/setup.md` §6, with the
   pipeline id filled from `GET /api/pipelines`. Mobile: the banner collapses to one line.
6. **P1f (A1): the quickstart path has an integration test.** `internal/testenv` gains `Quickstart(t)`: against
   the dev stack it runs `discover -write-owners` into a temp dir, starts `cmd/server --quickstart` on a free
   loopback port, waits for `/healthz`, posts the `schema-change` alert, and asserts a room opened, the agent
   ran (with `ANTHROPIC_API_KEY`; skipped otherwise), a proposal was created and `action.executed` was **not**
   emitted. `dev/README.md` (or `docs/demo/**`) gets the quickstart as the first thing to try.

## Out of scope
- A `piperoom.yaml` config file (P0 item 9). A single launcher binary. Any change to OIDC.
- Turning write mode on per layer: one switch for the process in this wave; per-layer is the next one.

## Ownership and files
| Sub-task | Agent | Files |
|---|---|---|
| P1a | A5 | `cmd/server/**`, `internal/room/**` (executor gate, snapshot `blocked`, `mode`), `internal/httpapi/**` (`/healthz`) |
| P1b | C10 | `cmd/discover/**`, `internal/discover/**` |
| P1c | C1 | `Makefile`, `README.md`, `deploy/docker-compose.quickstart.yml`, `.github/workflows/ci.yml` (not the `concurrency:` block) |
| P1d | C14 | `docs/pilot/**` |
| P1e | A6 | `web/**` |
| P1f | A1 | `internal/testenv/**`, `dev/**`, `docs/demo/**` |

## Dependencies and merge order
P0 → **P1a** → P1b and P1e (any order) → P1c (needs the flag and `-write-owners`) → P1f → P1d last (it
cites the merged behaviour). P1b and P1d may be coded in parallel with P1a against P0's text.

## Done when
- [ ] `PIPEROOM_WRITE_MODE` unset: a fully approved proposal never emits `action.executed`; the snapshot says
      `blocked: "write_mode_off"`; `/healthz` says `write_mode: "off"`. `=on`: today's behaviour. `--demo`: on.
- [ ] `make quickstart` from a clean clone against the dev stack prints a URL within 5 minutes; the UI shows
      the banner, the first-run panel, and a room after the printed `curl`.
- [ ] `discover -write-owners` output loads with `owners.Load` unchanged, or is written as `.draft` with the reason.
- [ ] `docs/pilot/quickstart.md` ≤ 60 lines; `setup.md` starts with the ten-variable table; F7 row updated.
- [ ] Web: Vitest for banner, button copy, "Approved · waiting for write mode", first-run panel; a11y: the
      banner is a `region` with a label, dismiss is a button.

## Tests
- A5: unit tests with the memstore and fake clients for both modes; a race run of `internal/room`.
- C10: `httptest` fakes of Connect, Kafka admin and the Registry; table tests for the summary and the `.draft`
  path; an `integration`-tagged run against the dev stack.
- C1: the CI job itself is the test; keep it under 5 minutes or mark it `continue-on-error` and say so.
- A1: `go test -tags integration ./internal/testenv/...` with and without `ANTHROPIC_API_KEY`.

## Effort
P1a 2 days · P1b 1.5 days · P1c 1 day · P1d 1 day · P1e 1.5 days · P1f 1 day.
