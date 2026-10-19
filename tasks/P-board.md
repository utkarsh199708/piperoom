# P wave dispatch board: one row = one PR

Plan `docs/product/fit-2026-10.md`; details per sub-task in `tasks/P0-contracts.md`, `P1-…`, `P2-…`, `P3-…`,
`P4-…`. This board splits each sub-task into PRs an agent finishes in one or two sessions (target < 600 changed
lines, §6 of `AGENTS.md`). **ID** is the PR tag (`[P2b-1] room: approve-all route`). **Model**: the model the
agent runs on; Opus 5.5 where the PR decides who may do what, what the agent can see, or writes to a customer
system; Sonnet 5.5 otherwise. Cloud agents (C*) run Sonnet 5 unless the row says Opus. **Days** is one agent's
working days including tests and review fixes. **After** lists the rows that must be merged first (P0 always).

**Merged in round 1 (2026-10-18):** #583 (P0), #584 (P1a-1), #585 (P2a), #586 (P1b), #587 and #593 (P4c-1). Their
IDs below say "merged"; a row that lists them under After waits only on its other entries.

**Merged in round 2 (2026-10-18):** #589 (P1e-1), #590 (P2b-1), #591 (P3b-1), #592 (P3a-2), #588 (P4a-1), plus the
leads' #594 (contracts follow-up 1: `ErrAlreadyPublished`, `GroupResetPlanner`, the P3-0 and P4a-4 rows) and A5's
#595 (two flaky `internal/room` tests). Unblocked by round 2: P1e-2, P2c-1, P3-0 (new row), P4a-2, P4a-3, P4a-4. Still dispatchable from before (After all
merged, not merged themselves): P1a-2, P1a-3, P1d-2, P2b-2, P2b-3, P3h, P4d-1, P4e-1, P4e-2, P4g-1. Waiting: P3a-1,
P3a-3…P3a-5, P3b-2, P3b-3 on P3-0 (P3b-4 on P3-0 and P3a-5); P4b-1 and P4f-2 on P4a-4; P4c-2, P4f-1 and P4d-2 on
P4a-2 (P4d-2 also on P4d-1); P2b-4 on P2b-3.

**Leads follow-up 5 (2026-10-19):** new rows P3g-1r (C7: re-record the P3 scenarios live), P3a-5c and P3a-2b (A2),
P4b-3 (A4: batch tools in batch rooms), P4a-6 (A5), P4a-7 (A3), P4e-3 (C3: Databricks OAuth M2M), P4f-3 and P4f-4
(A6), P4i-4 (C14), P4i-5 (C1); P2c-3 gains the `cancel_until` wording. The executor claim rule of #638 is recorded in
`docs/design/replicas.md` §5, `docs/api.md` (approve row) and `docs/design/auth.md` §3.2.

**Leads follow-up 6 (2026-10-19):** merged #651 (P4b-3; it also wires P4c-2's and P4d-2's tools), #652 (P2b-4b),
#653 (P2c-2b), #654 (P3g-2), #655 (P3-W), #656 (C7: P3 fix labels), #657 (P3a-5c, P3a-2b), #658 (P4a-6), #659
(P3-W2). New rows: P3-W2e (C6: the live eval run that closes P3), P3g-1s (the Kafka signal-format check, #656 ruling) and the follow-up rows below Step 3; the
first-batch-action condition is recorded there and in `tasks/P0-contracts.md` item 8 note.

**Leads follow-up 7 (2026-10-19):** merged #661 (U1-0), #663 (P2c-3), #664 (P2c-2c), #665 and #673 (P1a-3b, P1a-3c),
#666 (P3g-1c), #667, #669, #670 (U0-IL1 to U0-IL3), #671 (P4b-4), #672 (P3g-2a), #674 (P3g-2b), #677 (P3-W2f),
#680 (P3-W2g), and, without a board row, #668 and #675 (C1: `evals-live` with the live judge, judge cap, transcripts,
infra exit code), #676 (C6: `evalrun` run errors and exit 3 on an API refusal), #678 (A2: `connect_config` shows
signal and publication keys) and #679 (C7: `missed-table` `connect_config` shows the signal table). Rows merged
in earlier rounds are now marked with their PR too.

## Step 0

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| P0 (merged #583) | leads | Opus 5.5 | 1 | contracts: write mode, solo ceremony, write clients, pipeline kinds, `GET /api/actions` | every item of `P0-contracts.md` in code and `docs/api.md`; all stubs and fakes compile; `go test ./...` green | — |

## Step 1 — P1 (quickstart, read-only) and P2 (solo ceremony) in parallel

### P1

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| P1a-1 (merged #584) | A5 | Opus 5.5 | 1.5 | server: `PIPEROOM_WRITE_MODE` gate, snapshot `mode.write` and `blocked`, `/healthz write_mode` | mode off: fully approved proposal never emits `action.executed`, snapshot says `blocked: "write_mode_off"`; `--demo` forces on; race run green | P0 |
| P1a-2 (merged #600, e94d1cc8) | A5 | Sonnet 5.5 | 1 | server: `--quickstart` flag (memstore, dev auth on loopback, temp one-user `users.yaml`, prints the URL) | `go run ./cmd/server --quickstart --owners x.yaml` serves a room list in < 10 s; non-loopback addr refused | P1a-1 |
| P1a-3 (merged #603, 70339d3b) | A4 | Opus 5.5 | 1 | wiring: read write mode; with it off build no write clients and register the actions over read-only clients (`wiring.StackOptions`), so an action holding a full client can never write | with `PIPEROOM_WRITE_MODE=off` no `*Writer` method is reachable from the action registry (test asserts the client types); P3a-1's wiring line lands after this; refuses `SOURCE_DB_WRITE_URL` equal to `SOURCE_DB_URL`; a pipeline with its own `connections:` block gets no `SourceWriter` until A3 adds a `source_db_write_url` key (secret: `env:`/`file:` only) and A4 maps it | P1a-1 |
| P3-0 (merged #599, cf8d2ce1) | A5 | Opus 5.5 | 0.5 | room: the executor runs every action's Validate and Execute with `tools.WithPipeline(ctx, roomPipeline)` (today only the preview sets it) | a test proves the pipeline is in the Execute ctx; the Postgres and Kafka writers fail closed without it from their first PRs (#591, #592), so no P3 action writes until this lands | P1a-1 |
| P1b (merged #586) | C10 | Sonnet 5 | 1.5 | discover: `-write-owners`, `-owner`, `-users`, summary table, `.draft` on validation error | output loads with `owners.Load`; missing sink → `sink_tables: []` + `# TODO`; dev-stack integration test | P0 |
| P1c-1 (merged #618, 3c6f9b44) | C1 | Sonnet 5 | 0.5 | repo: `make quickstart`, `deploy/docker-compose.quickstart.yml`, README §Quickstart (≤ 25 lines) | `make quickstart` on a clean clone against the dev stack prints a URL in < 5 min | P1a-2, P1b |
| P1c-2 (merged #630, 1cb8a206) | C1 | Sonnet 5 | 0.5 | ci: quickstart job (5-minute budget, curls `/healthz` for `write_mode: "off"`) | job green on the P1c-1 merge commit | P1c-1 |
| P1d-1 (merged #617, d2238469) | C14 | Sonnet 5 | 0.5 | docs: `docs/pilot/quickstart.md` (≤ 60 lines) and the ten-variable table at the top of `setup.md` | every claim cites a file; links resolve | P1a-2, P1b |
| P1d-2 (merged #601, c06bd005) | C14 | Sonnet 5 | 0.5 | docs: F7 row → fixed by `connections:`; `decision.md` §4 read-only default, shadow as a toggle | citations to `internal/owners/tenancy.go` and the write-mode code | P1a-1 |
| P1e-1 (merged #589) | A6 | Sonnet 5.5 | 1 | web: read-only banner; "Approve (will not run)"; "Approved · waiting for write mode" | Vitest for the three states; banner is a labelled `region`; mobile one-liner | P0 (fake snapshot), P1a-1 to verify |
| P1e-2 (merged #623, 51081d09) | A6 | Sonnet 5.5 | 0.5 | web: first-run panel on the empty room list with the `curl` for `/api/alerts/webhook` | pipeline id comes from `GET /api/pipelines`; copy button; Vitest | P1e-1 |
| P1f (merged #619, 94220ca4) | A1 | Sonnet 5.5 | 1 | testenv: `Quickstart(t)` integration test; `dev/README.md` quickstart first | passes with and without `ANTHROPIC_API_KEY` against the dev stack; asserts no `action.executed` | P1a-2, P1b |

### P2

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| P2a (merged #585) | A3 | Sonnet 5.5 | 1 | owners: `auto_approve`, `IsOwned`, `OwnsAll`, `LoadWithActions` | table tests: unknown action, duplicate, unowned layer, group membership | P0 |
| P2b-1 (merged #590) | A5 | Opus 5.5 | 1.5 | room: `POST /api/proposals/{pid}/approve-all` | one call → one `approval.given` per required layer, then execution; partial owner → `403 not_layer_owner`, nothing written; solo confirm `pipeline/*` | P2a |
| P2b-2 (merged #624, 4ee89852) | A5 | Opus 5.5 | 1 | room: sole-member driver (`control.granted{reason: "sole_member"}` on first join of a driverless alert room) | second joiner changes nothing; viewer never; shadow rooms excepted; metrics | P0 |
| P2b-3 (merged #609, 544d331c) | A5 | Opus 5.5 | 1 | room: unowned-layer fallback (`fallback_layers` on the proposal; driver approves with `grant: "fallback"`) | non-driver owner of another layer cannot approve the fallback layer; re-checked at append. Design (review 2026-10-18): one proposal-aware `authorizeProposalLayer(st, p, user, layer, confirm, needConfirm)` that both `approve` and approve-all call (`authorizeLayer` first, then layer ∈ `fallback_layers` && driver == user → `"fallback"`); `approvalGrant` gains the fallback case; `createProposal` runs each step's `Validate` under `s.pipelineCtx(ctx, room pipeline)`, the same scope as the executor (P3-0, #599), with a test | P2a |
| P2b-4 (merged #629, 0e95628c) | A5 | Opus 5.5 | 2 | room: policy auto-approve (`proposal.scheduled`, timer, `cancel` route, snapshot `scheduled`, `mode.solo`/`mode.auto_approve`) | fake-clock tests: runs after delay with `grant: "policy"`; cancel stops it; failed preview, write mode off, shadow, driver-origin → never scheduled; a `schema_*` entry in `auto_approve` is refused at start (P2 out of scope). Design (review 2026-10-18, revised in leads follow-up 4): the countdown lives in the event log (`proposal.scheduled`), with one loop per replica that fires the rooms whose lease it holds; the countdown stops in the snapshot fold (a person's `approval.given`, a `proposal.cancelled` or a rejection after `proposal.scheduled`), not in `appendApprovalLocked`; the fire path re-checks it under the room lock (open, still scheduled in the fold) before the policy approvals; `appendApprovalLocked` takes the actor and optional identity so policy approvals reuse it; `mode.solo` = `authorizeAllLayers(pid, user, pipeline.Layers())`; `auto_approve` is validated against the FULL proposal registry, never an executor registry emptied by write-mode-off; a `schema_*` entry is refused | P2b-1, P2b-3, P1a-1 |
| P2b-4b (merged #652, 04f420b3) | A5 | Opus 5.5 | 0.5 | room: drop `scheduled` from the snapshot once `run_at` is a short grace in the past (no 'Runs in 0:00' forever) | a fake-clock test where `scheduled` is gone after the grace and the proposal is still approvable by hand; lands before P2c-3 | P2b-4 |
| P2c-1 (merged #616, abe07780) | A6 | Opus 5.5 | 1 | web: **Approve and run** (single button when the caller owns every required layer or `mode.solo`), confirm dialog | Vitest; the 2026-10-18 e2e approve path passes with one browser context | P2b-1 |
| P2c-2 (merged #643, 173ef021) | A6 | Sonnet 5.5 | 1 | web: fallback row ("no owner for `consumer` · driver approves"), hide request-control when sole member, hand-off only when `people > 1` | Vitest for each; a11y unchanged | P2b-2, P2b-3 |
| P2c-2b (merged #653, 07a138a3) | A6 | Sonnet 5.5 | 0.5 | web: a fallback driver gets approval attention and catch-up | the driver of a proposal with `fallback_layers` sees "approval needed" and the catch-up line (Vitest); test follow-ups in P2c-2c | P2c-2, P2b-3 |
| P2c-3 (merged #663, 797cc49f) | A6 | Sonnet 5.5 | 1 | web: countdown card ("Runs in 0:42 · policy · Cancel"), feed lines | `aria-live` only in the last 10 s; cancel calls the route; Vitest with fake timers; `cancel_until` is rendered as "the policy may run from <time>", never as a hard deadline (a cancel after it still wins if it lands first, `docs/api.md` P-wave events; leads follow-up 5) | P2b-4, P2b-4b |
| P2d | A4 | Sonnet 5.5 | 1 | agent: mode-aware wording (write mode off once per room; policy countdown note) | prompt snapshot tests; `make evals` pass rate unchanged | P1a-1, P2b-4 |
| P2e | C12 | Sonnet 5 | 0.5 | slackbridge: `proposal.scheduled` / `cancelled` / policy approval lines with the cancel deep link | unit tests with the recorded event fixtures | P2b-4 |

## Step 2 — P3 (fixes that end incidents, driver-authored proposals)

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| P3a-1 (merged #614, a93dc38e) | A2 | Opus 5.5 | 2 | connect: `PauseConnector`, `UpdateConnectorConfigKeys`; actions `connect_pause_connector`, N-key `connect_update_connector_config` (old one-key shape still parses) | `Validate` refuses out-of-pipeline, unknown key, `looksSecret`; `Preview` sends the merged config to validate; integration test with `it_` connector | P1a-1, P3-0 |
| P3a-2 (merged #592) | A2 | Opus 5.5 | 1.5 | kafka: `KafkaWriter` (`ResetGroupOffsets`, `Produce`) with `kadm`/`kgo`; `ErrGroupActive` | `go doc` for every `kadm` call in the PR; integration: reset an `it_` group to earliest and to a timestamp | P0 |
| P3a-3 (merged #608, c1e4ed56) | A2 | Opus 5.5 | 1 | actions: `kafka_reset_consumer_offsets` with per-partition preview and rollback offsets | refuses active group, future timestamp, group outside `consumer_groups`, and (shared group) a planned move on a topic outside `CheckScope`'s topics, since the client resets every committed topic of the group; executes against the dev stack; the Kafka client's `OffsetMove` (#592, `internal/clients/kafka/writer.go`) becomes `= clients.OffsetMove` and the client asserts `var _ clients.GroupResetPlanner = (*Writer)(nil)`, so the action's interface assertion holds; Validate and Preview work with write mode off through the read-only `kafka.Planner` (a `GroupResetPlanner` that is not a `KafkaWriter`; #603 ruling, done in #608) | P3a-2, P1a-1, P3-0 |
| P3-W (merged #655, 738e7410) | A4 | Opus 5.5 | 0.5 | wiring: register the P3 actions — `actions.RegisterKafka` with `Planner: kafka.NewPlanner(kc)` in both modes and `Writer: kw` only in the write-on branch; `pgactions.NewDropReplicationSlot` and `pgactions.NewAddPublicationTable` with `Writer: <SourceWriter>` write-on and `Writer: nil` (a nil interface, never a typed nil) write-off, so `GET /api/actions` lists them in both modes | `TestWriteModeOffNoWriterReachableFromActions` extended to the new actions; with write mode off every P3 action is listed, previews, and refuses at Execute; with it on the writers are the ones Execute calls | P1a-3, P3a-3, P3b-2, P3b-3 |
| P3-W2 (merged #659, 33eea2f4) | A4 | Opus 5.5 | 0.5 | agent + wiring: the agent proposes a slot drop only for a retired connector; wiring test hardening | `pg_drop_replication_slot` only with the retired-connector context (#613 ruling), `connect_resume_connector` without it; the write-mode wiring test hardened for the P3 actions | P3-W |
| P2b-3b (merged #626, 2e97b9fd) | A5 | Opus 5.5 | 0.5 | room: the driver may reject a fallback layer (`RejectProposal` uses `authorizeProposalLayer`; #609 ruling) | the driver rejects a `fallback_layers` entry with the same re-check at append; a non-driver is refused; memstore test | P2b-3 |
| P3a-4 (merged #622, 6cf24f60) | A2 | Opus 5.5 | 1 | actions: `dlq_replay` (ordered, ≤ 1000, drops `__connect.errors.*` headers, redacted key preview) | end offsets move as expected on the dev stack; stops at first produce error and reports `acked`; the Validate error says the target must be a pipeline CDC topic (`<topic_prefix>.<table>`), which is all the client's `CheckTopic` allows; Preview works with write mode off through a `RecordReader` that is not a `KafkaWriter` (a read-only view in `internal/clients/kafka`, like #608's `Planner`; #603 ruling); a DLQ topic with more than one partition is refused, or the step takes `partition` and the preview names it | P3a-2, P1a-1, P3-0 |
| P3a-5 (merged #620, d333639e) | A2 | Opus 5.5 | 1.5 | actions: `connect_snapshot_table` (`via: signal_topic`; `signal_table` delegates to `SourceWriter`) | refuses the `via` the connector is not configured for and tables outside `source_tables`; fake `SourceWriter` until P3b-4 | P3a-2, P1a-1, P3-0 |
| P3a-5b (merged #642, 4083a853) | A2 | Opus 5.5 | 0.5 | actions: `connect_snapshot_table` drops the `sendTopic` `Produce` fallback (fail closed), since it skips the every-source-table CDC-topic check | without a `clients.SnapshotSignaler` the `signal_topic` path refuses before any I/O; a test proves `Produce` is never called | #620 |
| P3b-1 (merged #591) | A3 | Opus 5.5 | 1.5 | postgres: `SourceWriter` on a separate pool (`SOURCE_DB_WRITE_URL`); identifiers via `Sanitize()` | no writer when the URL is empty; unit tests with a fake pool; integration: `it_` publication and slot | P0 |
| P3b-2 (merged #610, 837bd7f4) | A3 | Opus 5.5 | 1 | pgactions: `pg_add_publication_table` | refuses wrong publication, table outside scope, no PK/replica identity, already published; preview text; the postgres client's `ErrAlreadyPublished` (#591, `internal/clients/postgres/writer.go`) becomes `= clients.ErrAlreadyPublished` | P3b-1, P1a-1, P3-0 |
| P3b-3 (merged #606, 453ed935) | A3 | Opus 5.5 | 1 | pgactions: `pg_drop_replication_slot` (inactive, pipeline's slot only, `lost` needs `force`) | re-reads before drop; integration against an `it_` slot; `ErrSlotActive` path | P3b-1, P1a-1, P3-0 |
| P3b-4 (merged #633, 92d3745b) | A3 | Opus 5.5 | 1 | postgres + pgactions: `InsertSnapshotSignal` and the postgres-layer action `pg_snapshot_signal` (the signal-table variant of a Debezium incremental snapshot; #620 ruling: one action, one layer) | signal row written with the expected JSON; `reltuples` size warning in preview; when it merges, A2 deletes `connect_snapshot_table`'s dead `signal_table` branch | P3b-1, P3a-5, P3-0 |
| P3c | A5 | Opus 5.5 | 2 | room: `POST /api/rooms/{id}/proposals` (driver only, `Origin: "driver"`, same validation/preview/hash), `GET /api/actions`, `HumanInput` `proposal_created`/`proposal_executed` | non-driver 403; sixth open → 429; auto-approve never applies; memstore tests | P2b-4 |
| P3d-1 | A6 | Sonnet 5.5 | 1.5 | web: form generator from `ParamsSchema` (string, integer, boolean, enum, string[]; else "edit as JSON") | one Vitest per type + fallback; keyboard-complete | P3c |
| P3d-2 | A6 | Sonnet 5.5 | 1.5 | web: "Propose a fix yourself" in the decision bar; "authored by <driver>" badge on the card | driver only; preview fills in; Vitest + one e2e run | P3d-1 |
| P3e | A4 | Opus 5.5 | 1.5 | agent: react to `proposal_created` (one message, no tool run unless idle) and verify after `proposal_executed` | `agent.verification` emitted for a driver fix; token charge recorded; evals unchanged | P3c |
| P3f | C14 | Sonnet 5 | 1 | docs: write roles (Postgres publication/slot/signal, Connect pause/patch, Kafka ACLs); data-handling for driver proposals | each call listed with method/SQL and the minimal role | P3a-5, P3b-4 |
| P3g-1 (merged #647, synthetic; P3 fix labels and resources corrected by C7 in #656, cc6991b3; live re-record in P3g-1r; Kafka signal check in P3g-1s) | C7 | Sonnet 5 | 1 | scenarios: `missed-table`, `dead-slot-wal`, `consumer-bad-deploy`, `dlq-backlog` | expected action name in each label; recorded from the dev stack; `dead-slot-wal` carries the context that the connector is retired or will be re-created with a snapshot (#613 ruling): the label is `pg_drop_replication_slot` only with that context, `connect_resume_connector` without it; `missed-table` expects `pg_add_publication_table` then `pg_snapshot_signal` (#656 ruling: `devctl break missed-table` leaves Kafka signalling off, so `connect_snapshot_table` refuses); the live check of #620's Kafka signal format moved to P3g-1s | P3h |
| P3g-2 (merged #654, b83e8b01) | C6 | Sonnet 5 | 1 | evals: replay clients accept the write methods as recording no-ops; "right action proposed" score | the four scenarios score; CDC pass rate unchanged | P3g-1 |
| P3-W2e | C6 | Sonnet 5 | 0.5 | evals: run `evals-live` on `main` for `dead-slot-wal`, `dead-slot-wal-no-context`, `slot-inactive-wal-growth` and the CDC set (the owner approves the paid `evals` environment run); re-baseline `evals/baseline/default.json` | CDC pass rate unchanged; `dead-slot-wal` proposes `pg_drop_replication_slot`; `dead-slot-wal-no-context` proposes `connect_resume_connector` and no drop; `slot-inactive-wal-growth` resumes; `evals/baseline/default.json` re-baselined. **P3 is not done until this row is green** | #659, #656 |
| P3-W2f (merged #677, 5c578e8b) | A4 | Opus 5.5 | 0.5 | agent: three CDC prompt gaps from evals-live run 39212981319: the first failing layer is the most upstream broken one; a publication add is followed by a `pg_snapshot_signal` backfill; a retired connector means an orphaned slot | the CDC prompt stays under its 1335-word ceiling; CDC hash and snapshot re-pinned; batch prompt unchanged | P3-W2 |
| P3-W2g (merged #680, 48a974f4) | A4 | Opus 5.5 | 0.25 | wiring: `RegisterP3Actions` turns typed-nil deps into nil interfaces, so a typed-nil writer makes the action refuse instead of panic at Execute | `TestRegisterP3Actions` passes typed nils, a Planner and a Records fake and asserts what reaches the actions | P3g-2a |
| P3g-1r | local run (the owner's machine, like R1) + C7 | Opus 5.5 | 1.5 | scenarios: re-record `missed-table`, `dead-slot-wal`, `dead-slot-wal-no-context`, `consumer-bad-deploy`, `dlq-backlog` from the dev stack (P3g-1, #647, shipped them synthetic). A cloud agent cannot run the local stack (`AGENTS.md` §2), so the recording is done locally on the owner's machine with Docker; C7 keeps ownership of `evals/scenarios/*.yaml` and applies the recorded output in its PR | each recording comes from a live `devctl break`; the `missed-table` recording shows `pg_add_publication_table` then `pg_snapshot_signal` bringing the table's rows to the topic (#656 ruling; the Kafka signal-format check is P3g-1s, not this row); until this lands the five scenarios count as synthetic and their pass rate is not quoted as evidence that P3 is done | #633, P3g-2 |
| P3g-1s-a | A1 | Sonnet 5.5 | 0.5 | devctl: a `missed-table` break variant that turns Kafka signalling on in the dev connector (`devctl break missed-table-kafka-signal` or a flag on `missed-table`), plus its `fix` | the break leaves the table out of the publication with Kafka signalling on; the fix restores; `dev/scenarios.md` row | P3h |
| P3g-1s | local run (the owner's machine, like R1) + C7 | Opus 5.5 | 1 | scenarios: record P3g-1s-a's break locally with Docker (a cloud agent cannot run the stack); C7 adds the scenario (e.g. `missed-table-kafka-signal.yaml`) from the recording | the variant's label expects `connect_snapshot_table` (`via: signal_topic`); the recording shows the dev connector accepted the signal and the table's rows reached the topic: the first live check of #620's Kafka signal format (moved here from P3g-1 and P3g-1r by the #656 ruling) | P3g-1s-a, P3a-5c, P3g-1r |
| P3a-5c (merged #657, fc445ef2) | A2 | Opus 5.5 | 0.5 | kafka + actions: `kafka.SnapshotSignal = clients.SnapshotSignal` (alias), so `*kafka.Writer` satisfies `clients.SnapshotSignaler`; `connect_snapshot_table` swaps its local `snapshotSignaler` (`internal/actions/snapshot_table.go`) for `clients.SnapshotSignaler` | `var _ clients.SnapshotSignaler = (*Writer)(nil)` in `internal/clients/kafka`; a blocking-fake signaler test proves `Execute` is bounded on the signal path (returns by its deadline); `tasks/P0-contracts.md` item 7 note (follow-up 5) | #627, #642 |
| P3a-2b (merged #657, fc445ef2) | A2 | Opus 5.5 | 0.5 | kafka: harden `TestProbeSeam` (`internal/clients/kafka/probe_test.go:286-319`, 1200 ms deadline) the way #636 hardened `within` | no fixed wall-clock deadline the test can miss under CI load; `go test -race -count=20 ./internal/clients/kafka` green with `-p 2` | #636 |
| P3h (merged #613, afb4818a) | A1 | Sonnet 5.5 | 1.5 | devctl: `break`/`fix` for `missed-table`, `dead-slot`, `consumer-bad-deploy`, `dlq-backlog` | each break trips the watcher or an alert; each fix restores; `dev/scenarios.md` rows | P0 |

## Step 3 — P4 (pipeline kinds: batch)

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| P4a-1 (merged #588) | A3 | Sonnet 5.5 | 1 | owners: `kind`, batch fields, cross-kind field refusal, `Pipeline.Layers()` | table tests for both kinds and the refusals | P0 |
| P4a-4 (merged #607, 23995a42) | A5 | Sonnet 5.5 | 1 | server: `GET /api/pipelines` returns `kind`; the room fills `Alert.PipelineKind` from owners; `/api/me` `layers` scoped to the kinds in owners.yaml | docs/api.md rows satisfied; tests for cdc and batch | P4a-1 |
| P4a-2 (merged #611, fbb4f751) | A3 | Opus 5.5 | 1.5 | topology + scope: DAG → task → model → sink graph; batch scope (`dags`, `models`, `sink_tables`); `partial` on nil readers | builder tests with nil readers and `ErrNotSupported`; scope tests; a pipeline `connections:` block requires only the keys of its kind (batch: `warehouse_db_url`, plus Airflow/dbt keys once P4b-1 maps them); a pipeline `owners:` layer outside `p.Layers()` stays refused (landed in #588 round 1; keep the test); coordinate the equal-lists wiring test with A4 | P4a-1 |
| P4a-3 (merged #598, bb0d5118) | C5 | Sonnet 5 | 0.5 | alertin: `dag_id` label/field → the pipeline whose `dags` lists it | parse tests for Alertmanager and generic payloads | P4a-1 |
| P4b-1 (merged #625, ab324e74) | A4 | Opus 5.5 | 1.5 | wiring: register tool packages by kinds present; per-room tool list filtered by `Alert.PipelineKind`; `cmd/investigate -kind` | a batch room offers no `kafka_*`/`pg_*`/`connect_*`; a CDC room no `dag_*`/`dbt_*`. Connection keys (#611 ruling): this row (or a small A4 row before it) adds the `connFields` entries for the Airflow and dbt keys first, with `TestConnFieldsMatchOwnersKeys` checking owners keys ⊆ `connFields`; A3 then adds the keys to `owners.connKeys` and the test goes back to equality | P4a-1, P4a-4 |
| P4b-2 (merged #644, 1081ca28) | A4 | Opus 5.5 | 1.5 | agent: per-kind prompt section, batch verification (freshness + last run) | prompt snapshots per kind; evals unchanged for CDC | P4b-1 |
| P4b-3 (merged #651, 186f3ee9) | A4 | Opus 5.5 | 1 | wiring + agent: register `batchtools` and `dbttools` for batch pipelines and add both to `fullRegistry`; stop offering `propose_fix` in batch rooms | a batch room's tool list holds the `dag_*` and `dbt_*` tools and no `propose_fix`; a CDC room unchanged; the equal-lists wiring test covers both packages | #635, #644 |
| P4a-6 (merged #658, 2f87125e) | A5 | Opus 5.5 | 0.5 | room: `room.PipelineForDAG` delegates to `alertin.MatchDAG` (#631) | one DAG-to-pipeline rule in the tree: `PipelineForDAG` is kept as a one-line wrapper over `alertin.MatchDAG` because `internal/httpapi` cannot reach `internal/owners`; a batch-only match test through the room | #631 |
| P4a-7 | A3 | Opus 5.5 | 0.5 | dbtools: `warehouse_row` compares `pk_column` case-insensitively for Databricks, as for Snowflake | table test: Databricks `ID` vs `id` matches; Postgres stays exact | #632 |
| P4c-1 (merged #587, #593) | C15 | **Opus 5.5** | 2 | airflow: REST client (`OrchestratorReader`): runs, task instances, log tail from the end; 10 s, 4 MB, `netsec` | `httptest` tests: 404, timeout, oversized, bad JSON; token and basic auth | P0 |
| P4c-2 (wired by #651) | C15 | Sonnet 5 | 2 | batchtools: `dag_runs`, `task_instances`, `task_log_tail` (wrapped, redacted, scanned), `dag_schedule_gap` | read-only registry check; scope refusals; N5/N6 tests with a log fixture; guard a zero `logCeiling` on a Client not built by New (nit from #593) | P4c-1, P4a-2 |
| P4d-1 (merged #604, 3eefa558) | C16 | Sonnet 5 | 2 | dbt: artifacts reader (`run_results.json`, `manifest.json`, 16 MB, schema version) and dbt Cloud reader (`TransformReader`) | fixture tests incl. version mismatch and truncated file | P0 |
| P4d-2 (wired by #651) | C16 | Sonnet 5 | 1.5 | dbttools: `dbt_run_results`, `dbt_model_lineage`, `dbt_model_sql` (≤ 64 KiB, untrusted) | scope: model in `models` or adjacent; read-only check | P4d-1, P4a-2 |
| P4a-5 | A3 | Opus 5.5 | 0.5 | scope: when owners.yaml lists a dbt `unique_id`, model scope also matches the package (no same-name cross-package widening) | table test: `model.pkg_a.orders` in `models` admits `model.pkg_a.orders` and refuses `model.pkg_b.orders`; a bare name keeps today's match | P4d-2 (#637) |
| P4d-3 (merged #646, 0c64a120) | C16 | Sonnet 5 | 0.5 | dbttools: switch to `clients.TransformSQLReader` and `tools.CallFile`; drop the local copies | no local `ModelSQL`/SQL-reader type or `"file"` literal left in `internal/tools/dbttools` or `internal/clients/dbt`; the artifacts reader asserts `var _ clients.TransformSQLReader`; tests unchanged | leads follow-up 4, P4d-2 (#637) |
| P4e-1 (merged #612, 0822997f) | C3 | Sonnet 5 | 2.5 | bigquery: `WarehouseReader` + `LoadErrorReader` (`INFORMATION_SCHEMA.JOBS`) behind `WAREHOUSE_KIND=bigquery` | A3's warehouse contract tests pass with fakes; integration behind env creds; module listed | P0 |
| P4e-2 (merged #632, 2241cdd5) | C3 | Sonnet 5 | 2.5 | databricks: `WarehouseReader` (+ `CountReader`) behind `WAREHOUSE_KIND=databricks` | same bar as P4e-1 | P0 |
| P4e-3 | C3 | Opus 5.5 | 1.5 | databricks: OAuth M2M (service principal) with a bounded token fetch: a custom authenticator through `dbsql.WithAuthenticator` over one 10 s capped `*http.Client` (4 MB body cap), or a newer driver whose M2M fetch is bounded | `httptest` test: a hung token endpoint fails within the cap and the query never starts; bad status and oversized body tested; `go doc` for every driver call; module change listed | #632 |
| P4f-1 | A6 | Sonnet 5.5 | 1.5 | web: lane renders `dag`/`task`/`model` nodes with states | Vitest with a batch topology fixture; CDC lane unchanged | P4a-2 |
| P4f-2 | A6 | Sonnet 5.5 | 1 | web: `orchestrator`/`transform` layer badges, login card, new-room form shows kind | Vitest; a11y sweep | P4a-1, P4a-4 |
| P4f-3 | A6 | Opus 5.5 | 1 | web: DAG and model changes in the F8 "what changed" lane (P4 item 6; no P4f row covered it) | batch fixture renders DAG and dbt model changes next to the pipeline changes; CDC lane unchanged; Vitest | P4f-1 |
| P4f-4 | A6 | Opus 5.5 | 0.5 | web: `schema_subject` in the node-kind union (`topology.KindSchemaSubject`) | type-check covers every `NodeKind` in `internal/topology/graph.go`; Vitest renders a `schema_subject` node | P4f-1 |
| P4g-1 (merged #628, 95615d28) | A1 | Sonnet 5.5 | 2 | dev: compose profile `batch` (Airflow standalone with one DAG, dbt-postgres with two models, artifacts volume) | `docker compose --profile batch up` healthy in < 3 min; DAG writes `analytics.fct_revenue`; the Airflow API runs without login on loopback (`AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.default`) because the client refuses credentials over `http://` (#587) | P0 |
| P4g-2 | A1 | Sonnet 5.5 | 1.5 | devctl + testenv: `break`/`fix` `dag-failed`, `dbt-model-broken`, `late-batch`; `daily-revenue` in `dev/owners.yaml`; testenv starts the profile | each break raises the alert; fixes restore | P4g-1, P4a-3 |
| P4h-1 | C7 | Sonnet 5 | 1.5 | scenarios: `dag-failed`, `dbt-model-broken`, `late-batch`, `backfill-overlap` recorded from the profile | labels name layer and model | P4g-2, P4c-2, P4d-2 |
| P4h-2 | C6 | Sonnet 5 | 1.5 | evals: replay readers for Airflow and dbt, `kind` header, per-kind pass rate, README | four batch scenarios score; CDC rate unchanged | P4h-1 |
| P4i-1 | C14 | Sonnet 5 | 1 | docs: Airflow/dbt/BigQuery/Databricks permissions, setup reference rows, data-handling (logs and SQL reach the model) | citations per claim | P4c-2, P4d-2, P4e-2 |
| P4i-2 | C1 | Sonnet 5 | 0.5 | repo: README quickstart `--kind batch`; CI unit tests for the new packages | CI green | P4i-1 |
| P4i-3 (merged #645) | C1 | Sonnet 5 | 0.5 | Dockerfiles: replace the bare `go mod download` with a build that fetches only linked modules (the databricks kernel-bindings module is ~133 MB) | image build no longer downloads unlinked modules; build time and context size noted in the PR | #632 |
| P4i-4 | C14 | Opus 5.5 | 0.5 | docs: `docs/pilot/permissions.md` Databricks row: "a service principal's personal access token; OAuth M2M not supported yet; workspaces that disable PATs cannot connect yet"; `connect_update_connector_config` needs Kafka Connect 3.5+ (#614) | each claim cites a file; updated again when P4e-3 lands | P4i-1 |
| P4i-5 | C1 | Opus 5.5 | 0.5 | Dockerfiles + ci: pin or drop the `# syntax=docker/dockerfile:1` line (`Dockerfile`, `cmd/devconsumer/Dockerfile`); add `cmd/devconsumer/Dockerfile` to `release-image.yml`'s `paths:` filter. Ownership: C1 edits the root `Dockerfile` and `cmd/devconsumer/Dockerfile` (A1's folder) under the R6 and P4i-3 (#645) precedent, for this change only | no unpinned frontend image; a PR touching only `cmd/devconsumer/Dockerfile` runs the image build | #645 |

## Follow-up rows (leads follow-up 6, after #649–#660)

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| P4b-4 (merged #671, 9df2135e) | A4 | Opus 5.5 | 0.5 | wiring + agent: follow-ups from #651 and #659 | `scheduleReader` (`internal/wiring/kinds.go:158`) no longer duplicates `batchtools`' fallback (one copy); a runner-level test: `propose_fix` called in a batch room yields an error `tool_result` and no `Proposals.Create` call and no `proposal.*` event; `TestBatchToolsWithoutReaders` covers all 7 batch tools; a pointer comment on `checkedSlotDropperMethods` names the interface it mirrors (`internal/actions/pgactions/drop_replication_slot.go`) | #651, #659 |
| P2c-2c (merged #664, 82b6aa51) | A6 | Sonnet 5.5 | 0.5 | web: fix #653's attention and catch-up for rooms that are not open | attention and catch-up gate the whole `canDecideLayer` call on the room being open, so a layer owner in a room that is not open is not flagged (matching the decision panel); Vitest: a viewer gets no "approval needed"; a layer owner in a room that is not open gets no flag in attention or catch-up | #653 |
| P3g-2a (merged #672, c6a21c44) | A4 | Opus 5.5 | 0.25 | wiring: export the shared P3 registration helper in `internal/wiring` (the one place that registers the P3 actions with their readers and writers) | `internal/wiring` calls it itself; `TestWriteModeOffNoWriterReachableFromActions` unchanged and green | #655 |
| P3g-2b (merged #674, dbc6adf9) | C6 | Sonnet 5 | 0.5 | evals: follow-ups from #654 | `LoadScenario` no longer discards the `owners.Load` error; the replay write log is shown in reports; `evals/README.md` notes that the group-reset plan covers only the 5 recorded partitions; now that P3-W landed (#655): one shared P3 registration helper, the exported func in `internal/wiring` that P3g-2a adds (A4 owns it), which `internal/wiring` and `evals.LiveActions` (C6) both call; no copy left in `evals` | #654, #655, P3g-2a |
| P3g-1c (merged #666, 3687e953) | C7 | Sonnet 5 | 0.5 | scenarios: follow-ups from #656 | `consumer-bad-deploy`'s reset target is ≤ 08:52Z or `earliest` (checked; if the right-action score cannot check a timestamp bound today, C6 adds it to the scorer in a linked PR); `missed-table.yaml:19-20` overlong line wrapped; the `-validate` notes resolved: empty `fix_targets` in `recurring-connector-paused` and `slot-inactive-wal-growth`, empty `root_cause_forbidden` in `replication-slot-lost-failover` and `source-column-dropped` | #656 |
| P1a-3b (merged #665, 0c3457d0) | A3 | Opus 5.5 | 0.25 | owners: `source_db_write_url` key for a pipeline with its own `connections:` block (P1a-3 remainder; secret: `env:`/`file:` only) | table tests: accepted as `env:`/`file:`, refused as a literal | P1a-3 |
| P1a-3c (merged #665 and #673, fa808ade) | A4 | Opus 5.5 | 0.25 | wiring: map `source_db_write_url` to that pipeline's `SourceWriter` (write mode on only) | `TestConnFieldsMatchOwnersKeys` holds; refused equal to the pipeline's `source_db_url`; with write mode off no writer is built for the key | P1a-3b |
| U0-IL1 (merged #669, 75f02965) | A5 | Opus 5.5 | 0.25 | room: `room.labelled.actual_layer` and `shadow.labelled.actual_layer` validate against `events.IncidentLayers()` (the `IncidentLayers` switch-over, `docs/api.md` layer `environment`) | `environment` refused as an actual layer (test) | U0 (#649) |
| U0-IL2 (merged #667, 7e374b3a) | C6 | Sonnet 5 | 0.25 | evals: `scenario.go`, `learned.go`, `agreement.go`, `shadow.go` validate layers against `events.IncidentLayers()` | `environment` refused in each (table test) | U0 (#649) |
| U0-IL3 (merged #670, d2e18689) | N4 | Sonnet 5.5 | 0.25 | similarincidents: `similar_incidents` validates layers against `events.IncidentLayers()` | `environment` refused (test) | U0 (#649) |

**First batch action (#644 ruling; `tasks/P0-contracts.md` item 8 note).** No row adds a batch action yet. The row
that first does carries a contract item that (1) moves `PipelineKinds() []string` into `internal/actions/action.go` as
an optional interface (`agent.KindedAction` becomes an alias or goes) **and** (2) makes the batch prompt depend on
whether the room has actions, replacing the `promptkind_test` `nActions=0` guard with a test over the real wired
action registry.

**Follow-up, later wave (#608):** a `clients.OffsetTarget` "restore these offsets" mode, so `kafka_reset_consumer_offsets` can be reversible; a contract change, not a gap of P3a-3.

## Load per agent

| Agent | PRs | Days | Opus days | Critical for |
|---|---|---|---|---|
| A5 | 7 | 10 | 9 | the whole wave: P1a-1 unblocks every write gate; P2b-* unblock the web and P3c |
| A6 | 10 | 11 | 1 | user-visible result of every step |
| A3 | 8 | 9 | 6.5 | P2a (step 1), pgactions (step 2), kinds (step 3) |
| A2 | 5 | 7 | 7 | step 2 |
| A4 | 4 | 5.5 | 4.5 | steps 2 and 3 |
| A1 | 4 | 6 | 0 | step 2 and 3 scenarios |
| C3 | 2 | 5 | 0 | step 3 |
| C15 | 2 | 4 | 2 | step 3 |
| C16 | 2 | 3.5 | 0 | step 3 |
| C14 | 4 | 3 | 0 | docs each step |
| C6 | 2 | 2.5 | 0 | — |
| C7 | 2 | 2.5 | 0 | — |
| C1 | 3 | 1.5 | 0 | — |
| C10 | 1 | 1.5 | 0 | step 1 |
| C12 | 1 | 0.5 | 0 | — |
| C5 | 1 | 0.5 | 0 | step 3 |
| leads | 1 | 1 | 1 | everything |

Total about 75 agent-days over 59 PRs. The critical path is A5: P0 → P1a-1 → P2b-1 → P2b-3 → P2b-4 → P3c
(about 9 working days), then A6's P3d-* behind it. Everything in P4 except P4b-* and P4f-* can start the day
P0 merges and run beside steps 1 and 2.

## Suggested weekly dispatch (all agents in parallel)

| Week | Lands |
|---|---|
| 1 | P0; P1a-1, P1b, P1e-1, P1f, P2a, P2d (drafted), P3a-2, P3b-1, P3h, P4a-1, P4c-1, P4d-1, P4e-1, P4g-1 |
| 2 | P1a-2, P1c-*, P1d-*, P1e-2, P2b-1, P2b-2, P2b-3, P2c-1, P2c-2, P3a-1, P3b-2, P3b-3, P4a-2, P4a-3, P4e-2 |
| 3 | P2b-4, P2c-3, P2d, P2e, P3a-3, P3a-4, P3a-5, P3b-4, P3f, P3g-1, P4b-1, P4c-2, P4d-2, P4g-2 |
| 4 | P3c, P3d-1, P3e, P3g-2, P4b-2, P4f-1, P4f-2, P4h-1 |
| 5 | P3d-2, P4h-2, P4i-1, P4i-2; integration run of every step's "Done when" by Lead 1 |

Review load for the leads: about 12 PRs a week at the peak (weeks 2–3); the code review of C* PRs goes to
`[LEAD-CLOUD]` as usual.
