# Pilot decision: how the first customer pilot runs

Status: **proposed** by `[LEAD-ARCH]` (R0), 2026-10-14, against `main` @ `998c987`. The human (Utkarsh) decides;
the customer answers the open questions in section 5 before anything is installed on their network.

This page does not repeat the pilot docs. It weighs what the October reviews measured against what the pilot docs
promise, and proposes one shape for the first pilot. Sources are cited as `file` § section.

## 1. The question

What does the first customer run, on what, and what must be true before it starts? Three things are being decided
together:

1. **What the agent may do**: watch silently (shadow), investigate in live rooms, or also run approved fixes.
2. **How people log in and how many replicas**: dev login, OIDC, solo; one replica or several.
3. **The go-live gates**: which open findings block the start, and which only bound it.

## 2. What the evidence says

### 2.1 Ready

- **Correctness under load.** Zero gaps, zero duplicates, and no duplicate agent runs or executions in every
  capacity scenario, on one replica and on 2–3 (`reviews/capacity-2026-10.md` §1, §9.4). After the load-test fixes:
  1,000 sockets, p99 10.7 ms, 0 gaps, dups or reconnects (`reviews/load-test-2026-10.md` §Results).
- **Auth and approvals.** The audit fixed 7 bugs (A-PH-1, A-PH-3, A-OWN-1, A-USR-1/2, A-ID-3, A-ID-4). Approval
  forgery, CSRF, DNS rebinding, viewer writes, export access and open redirects were defended in tests
  (`reviews/security-audit-2026-10.md` §Summary). A-AP-3 and A-ID-5 have since been fixed (#89).
- **Read-only by construction on the customer side.** The source role is `SELECT` only, with
  `default_transaction_read_only = on`, `statement_timeout = '10s'` and `CONNECTION LIMIT 5`. Kafka needs no
  Write/Alter ACL. Read-only Connect and Registry is "a valid, safe pilot mode" (`docs/pilot/permissions.md`
  §Summary, §Source Postgres, §Kafka). Every read tool is scoped to the room's pipeline (F11), and every write
  action too (F10) (`docs/pilot/README.md` F-table).
- **Shadow mode is built.** Per-pipeline `shadow:` in `owners.yaml`: blind label, reveal, grade and metrics
  (`docs/design/shadow.md`; `GET /api/shadow/metrics` in `docs/api.md`). It proposes nothing and never runs
  actions. Its budget defaults to 500,000 tokens and 20 turns, against 2,000,000 tokens and 40 turns for a live
  room.

### 2.2 Not ready (open on `main` today)

| Item | Severity | Source | Blocks |
|---|---|---|---|
| hamba/avro GO-2026-5046/5047/5048, **reachable in production** through the Kafka key decode; `go.mod` still `v2.31.0` | medium–high | `reviews/vuln-report-2026-10.md` §Summary, §avro | any pilot that reads the customer's Kafka |
| Dev mode: "any client that can reach the API can act as any user, including a layer owner". The image no longer bakes in dev auth (`Dockerfile:65`); `PIPEROOM_DEV_ACK` makes dev mode an explicit act (`internal/config/config.go:219`) | F1 open in dev mode | `docs/pilot/README.md` F1 | any pilot on a shared network, until OIDC |
| A-ID-2 display-name impersonation: the server marks `verified`; the web badge merged with #135 (`7a8f90f`, gate G3) | Medium, mitigated | `docs/api.md` Verified people; audit §A-ID-2 | OIDC pilots with IdP-only (group) members: check the badge in the pilot UI |
| Capacity 6.1 (`Service.mu` held across every append, the write ceiling), 6.3 (`pool_max_conns` in the URL stops the server, so the pool cannot be raised), 6.4 (join-storm replay cliff) | HIGH, HIGH, MEDIUM | `reviews/capacity-2026-10.md` §6 | nothing at pilot scale; they bound it (2.3) |
| Numbers measured on a **shared 4-vCPU VM with loopback Postgres**; the managed-Postgres re-run ("most important") and a multi-hour soak are not done; replica **failover not tested under load** | — | same, §8, §9.4 | the scale we may promise, not the start |
| Coverage is low where pilot data flows: `internal/store` 8.3%, `clients/postgres` 7.8%, `clients/warehouse` 10.5%, `clients/kafka` 20.2% | — | `reviews/coverage-2026-10.md` | nothing; risk to watch |

Resolved since the 2026-10-14 draft (each checked against `main`, 2026-10-15; F7 and write mode on 2026-10-18):

- **F7, one set of connection settings per process.** Fixed by the per-pipeline `connections:` block in
  `owners.yaml`: such a pipeline gets its own clients, tools and actions, and two tenants may not share a variable or
  file unless every pipeline involved sets `shared_connections: true` (`internal/owners/tenancy.go`:
  `validateConnections`, `Owners.CheckSharedRefs`; `internal/wiring/stacks.go`: `openStacks`). One process still holds
  every secret it serves, so one deployment per customer stays the only hard boundary (`docs/pilot/README.md` F7).
- **No server switch for writes.** `PIPEROOM_WRITE_MODE` now exists and defaults to `off` (see section 4).

- **Go toolchain.** `go 1.27.3` at `go.mod:3`; `FROM golang:1.27.3-alpine` at `Dockerfile:23` and
  `cmd/devconsumer/Dockerfile:2`. This is gate G1.
- **gosnowflake GO-2026-6665.** `go.mod:16` has `github.com/snowflakedb/gosnowflake/v2 v2.3.0` (the fix is in v2.2.0;
  migration in `9a4fa1e`, #436). `55850be` (#475) removed the allowlist entry: `.github/govulncheck-allow.txt` now lists
  only GO-2026-5046/5047/5048. The `govulncheck` job passed on the latest completed `main` CI run, 38812713239 (head
  `a53b497`, which contains both commits). No `ParseDSN` mitigation is needed. `reviews/vuln-report-2026-10.md` stays
  as the dated record.

### 2.3 Supported scale we can state

One replica, 4 vCPU, default pool: up to 60 open rooms tested, 500 viewers in total, 25 room events/s,
`PIPEROOM_MAX_AGENTS` ≤ 10, and rooms up to 10,000 events. All of these are floors from a shared VM
(`reviews/capacity-2026-10.md` §7). For 2–3 replicas the review proposes 1,000 viewers per replica, with **no** rise
in write rate and with failover unmeasured (§9.4). A first pilot of one or two pipelines and a handful of engineers is
far inside the single-replica floor, so **replicas buy nothing for the first pilot and add an untested failover
path.**

### 2.4 Where the docs disagree with the reviews (fix before handing the docs to a customer)

1. `deploy/helm/README.md` says more replicas "add capacity". Capacity §9.2 says they did not raise the write rate.
2. Restart behaviour is described three ways: `deploy/README.md` §Day-2 says a new agent starts per open room;
   `docs/pilot/setup.md` and `docs/pilot/README.md` say the agent comes back idle; `deploy/helm/README.md` says rooms
   stay paused unless `PIPEROOM_RESUME_ON_TAKEOVER=1`.
3. "Shadow mode" means the watcher's `log` mode in `docs/pilot/setup.md` §1 and `deploy/README.md` §Watcher, and blind
   agent rooms in `docs/design/shadow.md`. No pilot page documents shadow rooms.
4. `docs/pilot/setup.md` §6c and `deploy/README.md` still call auth PR 4 future work; it merged at `8283a58`.
5. The capacity review asked the pilot docs to state the queue effect, the fixed DB pool and a scale statement
   (§5). `docs/pilot/setup.md` has none of them.
6. "Read-only" tools still decode untrusted Kafka keys with the vulnerable avro decoder. `docs/pilot/permissions.md`
   and `data-handling.md` do not mention this denial-of-service risk.

## 3. Options

| | A. Full live pilot | B. Live rooms, read-only | **C. Shadow first, then live** | D. Wait |
|---|---|---|---|---|
| Agent | investigates in live rooms, proposes, runs approved fixes | investigates live and proposes; write mode off (the default), so nothing executes | runs blind on every alert of 1–2 pipelines; humans work the incident as today, then label and grade | — |
| Customer risk | highest: writes to Connect/Registry; F1 and A-ID-2 matter most | low: reads only; people may act on a wrong hypothesis | **lowest**: reads only, nobody sees agent output until after close | none |
| What we learn | everything, but on the customer's production | usefulness in the room | **measured accuracy** (`layer_correct`, `root_cause`, time to first hypothesis vs human MTTR, from `GET /api/shadow/metrics`): the number the next sale needs (`docs/design/shadow.md` §1) | nothing new |
| Needs first | all of 2.2, `PIPEROOM_WRITE_MODE=on` and write credentials (`docs/pilot/permissions.md`) | gates G1–G5 below | gates G1–G5 below | — |
| Cost | 2M tokens/room cap | 2M tokens/room cap | 500k tokens/room cap, 20 turns | — |

A is ruled out for a first pilot: it puts unmeasured accuracy next to production writes. D wastes the window: the
blocking items are days of work, not weeks. B and C are not exclusive: B is what a new install does by default, and
C is one `owners.yaml` block on top of it, per pipeline (section 4).

## 4. Recommendation: read-only by default, shadow as a toggle, then write mode

**The default start is B: live rooms, read-only.**
- Write mode is off unless `PIPEROOM_WRITE_MODE=on`. Unset, empty or `off` all mean off; any other value stops the
  start (`internal/config/config.go`: `ParseWriteMode`; `cmd/server/writemode.go`: `writeModeSetup`).
- With it off the agent still proposes, and people still preview and approve. A fully approved proposal is never
  queued for the executor, nothing runs and no `action.executed` is written (`internal/room/service_executor.go`:
  `enqueueExecutionLocked`, which marks the proposal blocked; `executeProposal` refuses too). The room view shows
  `open_proposals[].blocked: "write_mode_off"` (`internal/room/writemode.go`: `RoomFor`), and `GET /healthz` reports
  `"write_mode": "off"` (`internal/httpapi/health.go`: `handleHealthz`).
- Grant read-only credentials as well (`docs/pilot/permissions.md`). The switch stops the executor; it does not take
  write rights away from a role that has them.
- One deployment for the customer (F7, `docs/pilot/README.md`). One replica (2.3). Postgres for the room DB.
- `PIPEROOM_AUTH_MODE=oidc` behind the customer's TLS proxy. No dev login on their network.
- Watcher in `log` for the first week, then `rooms` (`docs/pilot/setup.md` §1), or alerts from their
  Alertmanager/PagerDuty through the signed routes.

**Shadow is an opt-in toggle per pipeline, not a phase.**
- Turn it on with `shadow: {enabled: true}` on a pipeline in `owners.yaml` (`internal/owners/shadow.go`:
  `Pipeline.Shadow`). Every alert room of that pipeline is then a shadow room: the agent runs blind, proposes
  nothing, and its output is shown only after the room closes (`internal/room/shadow.go`: `pipelineShadow`;
  `docs/pilot/shadow-rooms.md`).
- Remove the block, or set `enabled: false`, and restart: new rooms are live. Rooms already open stay shadow
  (`docs/pilot/shadow-rooms.md`). The file is read at start-up only.
- Use it when the customer wants measured accuracy before anyone sees the agent: for example 1–2 pipelines for 2–4
  weeks, with a blind label and a grade from a pipeline member on every closed room. Other pipelines stay live and
  read-only at the same time. The run limits are `PIPEROOM_SHADOW_TOKEN_BUDGET` (500 000) and
  `PIPEROOM_SHADOW_MAX_TURNS` (20) (`internal/config/config.go`: `ParseShadow`).
- Turn it off for a pipeline once it meets the bar agreed in section 5, question 6.

**Then write mode** (`PIPEROOM_WRITE_MODE=on`), after the customer's approvers have used read-only rooms. The switch
is one per process today; per-layer write mode is not built (`tasks/P1-quickstart-readonly.md`, Out of scope). Until
it is, limit the first layer by credentials: grant write rights for Kafka Connect restarts first, the least risky
action (`docs/pilot/permissions.md`).

**Go-live gates for the first install** (each is a PR, owner in brackets):

| Gate | What | Owner | Status (2026-10-15) |
|---|---|---|---|
| G1 | Pin `go 1.27.2` in `go.mod` and `FROM golang:1.27.2-alpine`; CI green on it | C1, in R6 (#127) | done (a64382a) |
| G2 | Make GO-2026-5046/5047/5048 unreachable from untrusted Kafka record keys, per the vuln report | A2 | done in code (#222): `decodeKey` refuses keys over 64 KiB and key schemas with arrays, maps, recursion, large `fixed`, wide decimals, more than about 1,000 nodes or 32 levels, or schema text over 64 KiB; decodes with a frozen bounded config; recovers panics. hamba/avro v2.31.0 stays: moving to the `iskorotkov/avro` fork is a new third-party dependency and is the owner's call (section 5) |
| G3 | Web "unverified" badge for IdP-only people (A-ID-2), since the pilot runs OIDC | A6, in R3 (#135) | done (#135): every rendered person name and avatar goes through the badge and name neutralizer |
| G4 | Dev mode cannot be exposed by accident: `PIPEROOM_DEV_ACK` (R0 #138 item 6) enforced, and no dev auth baked into the image | A5 (R9, #145) + C1 (R6, #127) | done (eeeeb66, a64382a) |
| G5 | Pilot docs: fix the six disagreements in 2.4, add a shadow-rooms page and the 2.3 scale statement | C14 + leads | done (#219) |

Not gates for the first install, but due before the pilot grows past the scale in 2.3: the managed-Postgres capacity re-run (capacity §8.2) and capacity
6.3 (so the pool can be sized). The Snowflake item (GO-2026-6665) is closed: see 2.2.

## 5. Open questions

For the customer (from `docs/pilot/data-handling.md` §6, plus what shadow mode adds):

1. **Model data.** May alert titles, the pipeline map, tool summaries (which can include keys and timestamps),
   steers and the hand-off digest go to the model provider? Which provider and region: Anthropic, Bedrock (an `in.`
   profile for India) or Vertex?
2. **Who labels and grades.** Shadow numbers mean nothing without labels. Which pipeline members label every closed
   room, and within what time?
3. **Retention.** How long are rooms kept (`PIPEROOM_RETENTION_DAYS`, per-pipeline `retention_days`), and must
   rooms be archived (`PIPEROOM_ARCHIVE_DIR`)? Deleting a room does not recall what was sent to the provider or to
   Slack (`data-handling.md` §4).
4. **Slack.** Is a mirror wanted, and in which private channel? Shadow output never goes to Slack (`docs/api.md`
   Shadow mode).
5. **Revocation.** Who can turn the agent off quickly, and how? (`PIPEROOM_MAX_AGENTS=0` is not a supported "off";
   stopping the service is the off switch today.)

For us:

6. **The bar for turning shadow off on a pipeline.** Proposed: at least 20 closed, labelled shadow rooms, with `layer_correct`
   in at least 70% of them, and no room where the agent's hypothesis would have led to a harmful write. The 70% is a
   starting proposal, not derived from data; the evals (C6, `evals/`) should say whether it is realistic.
7. **Who is on call for Piperoom itself** during the pilot, and which channel the customer uses to reach them.
8. **Staging first?** `docs/pilot/setup.md` (intro) and `data-handling.md` §5 recommend a staging or replica
   source first. Do we require it, or accept production reads under the read-only role?
9. **Hardware.** The Terraform GCP target (e2-standard-2, Cloud SQL) is itself untested
   (`deploy/terraform/gcp/README.md` §Verification status). Do we run the capacity re-run on that exact shape and
   make it the pilot's?

## 6. What happens after this is decided

`[LEAD-ARCH]` turns the chosen option's gates into dispatch messages (one per owner, `docs/collab.md` §4). G4 is
already in the R wave (R9 #145, R6 #127), and so are G1 (R6 #127) and G3 (R3 #135). G2 and G5 are new tasks. As of 2026-10-15 all five
gates are merged (see the Status column). One owner decision stays open: whether to move from hamba/avro to the
`iskorotkov/avro` fork (G2 closed the advisories in our own code instead). This page is updated to **decided** with the date and the
option. It does not change any setting by itself.
