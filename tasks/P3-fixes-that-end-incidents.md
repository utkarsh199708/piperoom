# P3 — The seven fixes that end CDC incidents, and driver-authored proposals

Owners: P3a A2 · P3b A3 · P3c A5 · P3d A6 · P3e A4 · P3f C14 · P3g C7 + C6.
Model: **Opus 5.5** for P3a and P3b (writes to production systems; every `Validate`, `CheckScope` and `Preview`
is a safety boundary) and P3c (a new way for a proposal to enter the room); **Sonnet 5.5** for P3d–P3g.
Plan: `docs/product/fit-2026-10.md` §1.2, §2 row 3. Uses P0 items 5, 6, 7.

## Goal
The agent can propose, and an approved room can run, the fixes that actually close a CDC incident:

| # | Action | Layer | Write | Ends which incident |
|---|---|---|---|---|
| 1 | `connect_update_connector_config` with **N keys** (replaces the one-key limit) | kafka-connect | `PATCH /connectors/{name}/config` | one fix, one approval, not three |
| 2 | `connect_pause_connector` | kafka-connect | `PUT /connectors/{name}/pause` | stop the bleeding while people decide |
| 3 | `connect_snapshot_table` (Debezium incremental snapshot) | postgres + kafka-connect | signal table insert (P3b) or Kafka signal topic (P3a) | a missed or corrupted table |
| 4 | `pg_add_publication_table` | postgres | `ALTER PUBLICATION … ADD TABLE` | "new table is not replicating" |
| 5 | `pg_drop_replication_slot` (inactive slots only) | postgres | `SELECT pg_drop_replication_slot(..)` | WAL bloat from a dead slot |
| 6 | `kafka_reset_consumer_offsets` (to earliest or a timestamp; group must be inactive) | consumer | `kadm.Client.CommitOffsets` | reprocess after a bad deploy |
| 7 | `dlq_replay` (copy N records from the DLQ back to the source topic, in order) | kafka | produce | drain a DLQ |

And a person's own fix goes through the same door: the driver picks a registered action, fills its params,
and the proposal gets the same scope check, preview, per-layer approval, `params_hash`, execution record and
agent verification as one the agent wrote. There is no free-form command.

## Evidence
- `internal/actions/kafka_actions.go`, `connector_config.go`: six actions; `connect_update_connector_config`
  takes exactly one key (`docs/pilot/permissions.md` line 212).
- `clients.ConnectWriter` (`internal/clients/clients.go:45`) has two methods; there is no Kafka or Postgres
  writer interface at all; `SourceDB` is `SourceReader` only (`:333`).
- Proposals enter only through the agent (`service_proposals.go:72`, called from the agent sink); `docs/api.md`
  has no `POST …/proposals`.
- `go doc github.com/twmb/franz-go/pkg/kadm.Client.CommitOffsets`, `.FetchOffsets`, `.ListEndOffsets`,
  `.ListOffsetsAfterMilli`, `.DescribeGroups` exist (rule 9: confirm before use). `kgo.Client.ProduceSync` exists.
- Debezium incremental snapshots: signal table row `(id, type='execute-snapshot', data='{"data-collections":[..],"type":"incremental"}')`
  or the Kafka signal topic (`signal.kafka.topic`) with the same JSON; the connector config says which exists.

## Scope — what to build

1. **P3a (A2): Connect, Kafka and DLQ actions** in `internal/actions/**` (not `action.go`), client write methods
   in `internal/clients/connect` and `internal/clients/kafka` implementing P0 item 7.
   - Every action implements `Action`, `Scoped` and `Preview`; `Validate` reads the target (≤ 10 s) and refuses:
     a connector outside the pipeline, an unknown key, a `looksSecret` key or value (`connector_config.go:55`;
     secrets never travel through a proposal), a group with active members (`ErrGroupActive`), a timestamp in
     the future, `dlq_replay` with `count > 1000` or a target topic outside the pipeline's prefix, a snapshot
     table outside `source_tables`.
   - `connect_snapshot_table`: params `{connector, tables: [..], via: "signal_topic"}` in P3a; `via: "signal_table"`
     delegates to P3b's `SourceWriter.InsertSnapshotSignal` (A2 calls the interface; A3 implements it). `Validate`
     reads `connect_config` for `signal.kafka.topic` / `signal.data.collection` and refuses the `via` that is not
     configured. Layer: `kafka-connect` for the topic variant, `postgres` for the table variant (the action
     reports its `Layer()` from the chosen `via`; a step whose layer does not match is refused as today,
     `service_proposals.go:94`).
   - `kafka_reset_consumer_offsets`: `Preview` lists per partition current → target offset and the lag it
     creates; `Execute` commits with `kadm`; the result text names moved partitions. Never on a group outside
     `consumer_groups`.
   - `dlq_replay`: params `{dlq_topic, to_topic, count, from_offset?}`; `Preview` shows the first 3 keys
     (redacted through `redact.Text`), the span of offsets, and "headers are kept; the `__connect.errors.*`
     headers are dropped"; `Execute` reads with the existing `PeekDLQ` path and produces in order; stops at the
     first produce error and reports `acked`.
   - `connect_update_connector_config` keeps its name; `params.changes` (object) replaces `key`/`value`; the old
     shape is accepted for one wave (mapped to a one-entry `changes`) so evals and recorded rooms still parse.
     `Preview` sends the whole merged config to `ValidateConfig` (F4b) unless a secret key is present.
   - `connect_pause_connector`: `Preview` says what consumers will see (lag grows, no data loss) and that
     `connect_resume_connector` is the rollback.
   - Rollback text on every `PreviewResult` where one exists (pause ↔ resume; config → previous values; offsets
     → the previous committed offsets, which `Preview` records).
2. **P3b (A3): source Postgres writes**, in `internal/clients/postgres` (`SourceWriter`, P0 item 7) and a new
   `internal/actions/pgactions/**` folder (A3's from this wave) holding `pg_add_publication_table`,
   `pg_drop_replication_slot` and the `signal_table` half of `connect_snapshot_table`.
   - The writer is a **separate pool** from `pgconv.ReadOnlyPool`: `SOURCE_DB_WRITE_URL` (P0 item 7's config
     line; empty → no writer, actions report "no write credentials" at `Validate`) with its own role, documented
     in P3f. Identifiers go through `pgx.Identifier{}.Sanitize()` after the `^[a-zA-Z_][a-zA-Z0-9_]*$` check
     (rule 6); no statement is built from untrusted text.
   - `pg_drop_replication_slot` `Validate` re-reads `pg_replication_slots` and refuses `active`, a slot whose name
     is not the pipeline's connector slot (`slot.name` from `connect_config`) and a slot with `wal_status = lost`
     only with `force: true`; `Execute` re-checks inside the same transaction (`SELECT … FOR UPDATE` is not
     available on the view: re-read, then drop, and report the state it saw).
   - `pg_add_publication_table` `Validate` checks the publication is the connector's (`publication.name`), the
     table is in `source_tables`, has a primary key or `REPLICA IDENTITY FULL` (else refuse with the reason),
     and is not already published; `Preview` says "Debezium picks the table up on the next change; use
     `connect_snapshot_table` for history".
   - `InsertSnapshotSignal` inserts `(id uuid, type, data)` into `signal.data.collection` and returns the id;
     `Preview` reads the table's row count estimate (`pg_class.reltuples`) to warn about size.
3. **P3c (A5): driver-authored proposals.** `POST /api/rooms/{id}/proposals` (P0 item 5): driver only (`403
   not_driver`), open rooms only, not shadow; the body is validated exactly like an agent proposal
   (`createProposal`: known action, layer matches `a.Layer()`, `Validate`, `CheckScope`, `params_hash`, preview
   goroutine), `Origin: "driver"`, actor = the user. The agent gets `HumanInput{Kind: "proposal_created"}` and,
   after execution, `"proposal_executed"` (P0 item 6) so it verifies; the executor path is shared. Auto-approve
   (P2) never applies to `Origin == "driver"`. Rate: at most 5 open driver proposals per room (`429
   too_many_proposals`).
4. **P3d (A6): "Propose a fix yourself".** In the room's decision bar (F1): a secondary action for the driver that
   opens a dialog listing the registered actions (`GET /api/actions` — ask A5 in thread if the route is missing;
   `docs/api.md` must carry it, so it is a P0 follow-up if absent), their `Describe()` and a form generated from
   `ParamsSchema` (string, integer, boolean, enum, string array; nothing else in this wave; an unsupported
   schema shows "edit as JSON"). Submit → the proposal card appears with an "authored by <driver>" badge; the
   preview fills in as for agent proposals. Non-drivers do not see the action.
5. **P3e (A4): verify a driver's fix.** On `proposal_executed` for `Origin == "driver"` the runner runs the same
   verification pass as after its own fix (`verify.go`) and emits `agent.verification`; on `proposal_created`
   it adds one `agent.message` with its view of the fix (risk it sees, what it would check after), without a
   tool run unless the room is idle. Token cost is charged to the room as usual.
6. **P3f (C14): permissions.** `docs/pilot/permissions.md`: the Postgres write role (`ALTER PUBLICATION` needs
   ownership of the publication or superuser: document the `GRANT`/ownership choice and the minimal
   alternative of a role that owns only the publication; `pg_drop_replication_slot` needs `REPLICATION` or
   ownership; `INSERT` on the signal table only), the Connect calls 1–3, the Kafka ACLs for offset commit on the
   pipeline's groups and produce on its topics; `data-handling.md`: what a driver-authored proposal stores
   (params, never secrets; the `looksSecret` refusal).
7. **P3g (C7, C6): scenarios and evals.** C7: `evals/scenarios/missed-table.yaml` (fix: snapshot),
   `dead-slot-wal.yaml` (fix: drop slot), `consumer-bad-deploy.yaml` (fix: reset offsets), `dlq-backlog.yaml`
   (fix: replay), each with the expected action name in the label. C6: the replay clients accept the new write
   methods as no-ops that record the call; the scorer counts "right action proposed" per scenario.

## Out of scope
- A free-form command runner. Re-snapshot of a whole connector (`snapshot.mode` change + restart) — next wave.
- Any write to the warehouse. Any write without a preview.

## Ownership and files
| Sub-task | Agent | Files |
|---|---|---|
| P3a | A2 | `internal/actions/**` (not `action.go`, not `pgactions/`), `internal/clients/connect/**`, `internal/clients/kafka/**` |
| P3b | A3 | `internal/clients/postgres/**`, `internal/clients/pgconv/**`, `internal/actions/pgactions/**` (new, A3's) |
| P3c | A5 | `internal/room/**`, `internal/httpapi/**`, `migrations/**` (only if `origin` needs a column: it should live in the event payload) |
| P3d | A6 | `web/**` |
| P3e | A4 | `internal/agent/**` (not `contract.go`), `cmd/investigate/**` |
| P3f | C14 | `docs/pilot/**` |
| P3g | C7, C6 | `evals/scenarios/*.yaml` (C7); `evals/**` else (C6) |
| registration | A4 | one `Register` line per new package in `internal/wiring/wiring.go`: A2 and A3 ask in thread |

## Dependencies and merge order
P0 → P1a (write mode gate: these actions must not run with it off) → **P3a and P3b in parallel** (P3a's
`signal_table` variant calls P3b's interface through a fake until P3b lands) → P3c → P3d, P3e → P3f, P3g.
P2b is not a dependency; when both are in, auto-approve simply never lists these actions by default.

## Done when
- [ ] Each of the seven actions: `Validate` refuses every case listed above with a wrapped error naming it;
      `CheckScope` refuses an out-of-pipeline target without I/O; `Preview` never calls a writer (the
      `readonly_test.go` style check extended to `pgactions`); `Execute` against the dev stack does the write
      and reports it; `connect_update_connector_config` with two keys is one approval.
- [ ] `devctl break missed-table` (A1, ask in thread) → the agent proposes `connect_snapshot_table` → approved
      → the table's rows reach the sink; `devctl break dead-slot` → `pg_drop_replication_slot` → WAL retained
      drops; `consumer-bad-deploy` → reset → lag replays; `dlq-backlog` → replay → DLQ end offset unchanged,
      source topic end offset + N.
- [ ] Driver proposal: created with `Origin: "driver"`, previewed, approved per layer, executed, verified by the
      agent (`agent.verification` present); a non-driver gets `403`; the sixth open one gets `429`.
- [ ] `permissions.md` lists every new call with its exact HTTP method/SQL and the role that allows only it.

## Tests
- A2, A3: `httptest` / fake-pool unit tests for every refusal; `integration`-tagged tests against the dev stack
  with `it_<random>` connectors, slots, publications, groups and topics, cleaned up; no write in any test that
  is not tagged.
- A5: memstore tests for the route (auth, validation, rate cap, origin on the event); the executor path is
  shared and already tested.
- A6: Vitest for the form generator (one test per schema type, one for the unsupported fallback) and the badge.

## Effort
P3a 5 days · P3b 4 days · P3c 2 days · P3d 3 days · P3e 1.5 days · P3f 1 day · P3g 2 days.
