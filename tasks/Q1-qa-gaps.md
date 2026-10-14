# Q1 — fix the gaps of the failure-injection QA run (2026-10-15)

Source: product QA run on the real stack (main at 579154d, real server on :8090, real agent, dev stack, Postgres
17.11, Debezium 3.2). The contract changes every sub-task codes against are **Q0** (leads): see the `Q0` comments in
`internal/events/events.go`, `internal/agent/contract.go`, `internal/actions/action.go`,
`internal/clients/clients.go`, `internal/config/config.go`, and the "Q-wave" sections of `docs/api.md`.

What worked and must keep working: fast, evidence-cited diagnosis; honesty about tool limits; the prompt-injection
refusal; approval guards (wrong owner 403, missing params_hash 400, wrong hash 409, approve after close 409); stale
hand-offs voided by a newer one; hand-off summary quality; the post-mortem draft structure.

## The gaps (QA wording, shortened)

**High**

- **G1. The watcher can silence a real outage.** (a) A different incident right after a fix is ignored: a room closed
  at 04:09:50, a connector-auth break at 04:10:09 produced `watch: cooldown after a closed watcher room, staying quiet
  closed_ago=49s cooldown=1h`; three layers failing, no room, no agent, no notice. (b) An open room absorbs the
  incident: the watcher "joined an open room", an old manual room titled "test", as one system line; the agent,
  paused since a server restart, was not woken. (c) Closing a room while the signal still fires acks it, so the
  outage stays silent until the signal clears. Fix: key cooldown and ack to the incident (signal set), not the
  pipeline; re-raise when a new signal fires; join only recent rooms that came from an alert; wake the agent on
  join; escalate when a signal keeps firing with no open room.
- **G2. Nobody is told.** Watcher and alert rooms open with "No one is driving". Without Slack nobody is paged, and
  the agent knows the owner of the failing layer but can only write "needs a human". No agent action "ask the
  owner", no in-app notification, no email or webhook.
- **G3. Hidden failures are invisible to the agent.** Connect reports the task RUNNING while Debezium crash-loops
  on a retriable error (connector-auth; also the lz4 error of G17). The only evidence is the Connect worker log; the
  agent has no tool for it, and its Postgres tools do not return publication owner, table owner or the role's
  REPLICATION attribute. It proposed a restart that could not work. Fix: bounded, redacted worker-log tool; owner and
  role fields in `pg_publication_tables`.
- **G4. Hand-off fails with a bare 500.** One of three attempts: `room: handoff: write summary: ... model omitted
  what_broke` returned 500 internal_error after 9 s. No retry or repair, no usable message. Fix: validate and repair
  or retry once; typed error; allow a manual note.

**Medium**

- **G5. Approvals only cover the action's layer.** A `decimal.handling.mode=double` proposal for a money column asked
  only kafka-connect; the agent wrote "Meera and Kabir need to confirm" in prose, nothing enforced it. Add impacted
  layers that must approve; treat decimal precision loss as high risk.
- **G6. Verify-after-fix races Connect's async restart.** Right after the config change the verifier said "NOT
  recovered", then the agent corrected itself. The final agent message of that room was the literal text `<br>`. The
  same recovery was announced three times in 17 s. Fix: settle or poll with a timeout; dedupe; drop markup-only
  messages.
- **G7. "Seen before" and similar_incidents return junk.** A room titled "test", closing note "Closed by the QA
  run...", was offered as history in the UI and scored 1.48 and 0.92 in the agent's search. Require a resolution plus
  a hypothesis or label; skip rooms with few events.
- **G8. Closed rooms keep open proposals.** The UI of a closed room still says "Waiting for kafka-connect, 0 of 1
  approved" and "Proposals 1 open". The server refuses the approval (409) but the UI is wrong. Mark proposals
  withdrawn at close.
- **G9. The agent cannot withdraw or hold its own proposal.** It asked approvers to hold; the proposal stayed
  approvable.
- **G10. The post-mortem contradicts itself.** For a room fixed by a human outside Piperoom: "What was done: No fix
  was proposed", "Verification: No verification recorded", and "Still open"/"Risks" copied from the stale hand-off
  ("Restart the consumer", "lag keeps growing") although the resolution says it was fixed. Hand-off noise in the
  content ("Agent summary lines were truncated", "One handoff failed"). Build from the resolution and the events after
  the last hand-off; let people log what they did.
- **G11. Raw step ids in agent prose** (`s_01M4Y9X0ZPZ0NPJW00JGWVY8VP`); show "step 3" links.
- **G12. Layout bug.** At 1440 px a long proposal "risk" line runs under the right rail and overlaps the activity
  list. "Preview unavailable" for `connect_restart_task`.
- **G13. Cannot propose the heartbeat fix.** The connector-config allowlist has five keys; `heartbeat.interval.ms`
  is not one. Allow it with bounds; keep `heartbeat.action.query` human-only.
- **G14. Cost is invisible.** `cost_usd` is null and the post-mortem says "cost: not priced" until
  `PIPEROOM_MODEL_PRICES` is set. Ship a price table default and show an estimate in the room header.

**Low**

- **G15.** Injected alert text becomes the room title (200 characters, shown to everyone). Flag suspicious titles.
- **G16.** Only the driver can close, so the owner who fixed it must first be handed control.

**Dev stack and test tooling**

- **G17. The schema-change demo is not reliable.** After the break the task shows RUNNING but loops on `ERROR:
  compressed lz4 data is corrupt` (Postgres 17.11, logical decoding). The agent's fix and `devctl fix schema-change`
  (timed out after 60 s) both failed. A direct `pg_logical_slot_peek_binary_changes` worked. Maybe related to a
  connector restart on a slot whose restart_lsn was stuck (G18). Investigate.
- **G18. Baseline was already unhealthy.** `devctl health` showed `postgres FAIL retained 132.7 MB` because the
  slot's restart_lsn sat still for days on a quiet table while the shared Postgres kept writing; the watcher would
  not have flagged it (default fire 1 GiB).
- **G19. `devctl reset` wipes the room database** (`compose down -v`; the piperoom database lives in the pipeline's
  Postgres). Put the room DB on its own volume or service.

## Sub-tasks (wave 1)

Each sub-task stays inside the folders named; the lead merges them. A sub-task that needs a one-line change outside
its folders says so in its report instead of making it, unless the row allows it.

| Id | Gaps | Folders | Notes |
|---|---|---|---|
| Q1-watch | G1 (watcher side), G18 (signal) | `internal/watch/**` | incident-keyed cooldown/ack, escalate, slot restart_lsn stall signal |
| Q1-room-a | G5, G8, G9, G15, G16 (server) | `internal/room/**` except the Q1-room-b/-c files, `internal/store/**`, `migrations/**`, `internal/httpapi/**` (withdraw + close routes) | proposal lifecycle |
| Q1-room-b | G4, G10 (server) | `internal/room/service_handoff.go`, `internal/room/postmortem*.go`, `internal/room/service_postmortem.go`, the comment path, `internal/httpapi/**` (handoffs + comments handlers) | |
| Q1-room-c | G1 (room side), G2 (server) | `internal/room/service_watch.go`, new `internal/room/service_page.go`, the inbox, new `internal/notify/**`, `cmd/server/**`, `internal/httpapi/**` (page route) | |
| Q1-agent-a | G1, G2, G5, G9 (agent side) | `internal/agent/tools_room.go`, `run.go`, `prompt.go`, new files | |
| Q1-agent-b | G4, G6, G10, G14 (agent side) | `internal/agent/verify.go`, `handoff*.go`, `postmortem.go`, `usage.go`, `spend.go`, new files | |
| Q1-tools | G3, G5 (action), G12 (preview), G13 | `internal/clients/connect/**`, `internal/clients/postgres/**`, `internal/tools/kafkatools/**`, `internal/tools/dbtools/**`, `internal/actions/**` (not `action.go`), one registration line in `internal/wiring/**` | |
| Q1-web-a | G5, G8, G9, G12, G14, G16 (web) | `web/**` | |
| Q1-web-b | G2, G4, G7, G10 (web) | `web/**` | |
| Q1-small | G7 (tool), G11, G15 (web) | `internal/tools/similarincidents/**`, `web/**` (step links, title chip only) | |
| Q1-dev | G17, G18 (dev connector), G19, G3 (dev worker log file) | `dev/**`, `cmd/devctl/**`, `internal/testenv/**`, `docs/demo/**` | uses the real stack |

## Done when (every sub-task)

- `gofmt -l .` empty, `go vet ./...` and `go build ./...` clean, `go test ./...` passes for the packages touched
  (web: `npm run build`, `npm run lint`, `npm test`).
- New behaviour has table-driven unit tests, error paths included (AGENTS.md §5).
- AGENTS.md §4 rules hold: ctx + deadline on every I/O, wrapped errors, slog only, size caps, untrusted text wrapped.
- The report states what was done, what was not, and any contract question.
