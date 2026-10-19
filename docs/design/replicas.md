# Running more than one server replica — design (main @ b81248b, 2026-10-13)

Owner of the design: LEAD-ARCH. Implementation: S1 (`s1/replicas`). Contract: this file and
`internal/config/config.go` (`PIPEROOM_MAX_AGENTS_GLOBAL`, `PIPEROOM_REPLICA_ID`).

**Rule: Postgres is the only coordination service.** No Redis, no etcd, no Kubernetes lease API,
no replica-to-replica traffic. Every guarantee below is a row, a conditional `UPDATE`, an advisory
lock or a `NOTIFY` in the room database the server already uses.

## 1. Problem

Before this change every server process ran, with no lock:

| What | Where | What a second replica did |
|---|---|---|
| agent supervisor per room | `room.startAgentLocked` | a steer or `@agent` on replica B started a second Opus run for a room that already had one on A |
| watcher | `cmd/server/watch.go` | two watchers posted the same evidence twice and could open two rooms |
| retention purge, auth prune | `cmd/server/retention.go`, `authprune.go` | the same deletes twice (harmless, wasted) |
| action executor | `room.executeProposal` | a duplicate executor found step 0 "already claimed", marked the step failed and the proposal failed **while the first run was still executing it** |
| restart sweeps | `room.Load` | a replica that started marked every in-flight proposal and shadow run "interrupted" and wrote "server restarted" into rooms whose agent was running elsewhere |
| room cache | `room.Service.rooms` | each replica folded only its own appends: a driver who took control on A was "not the driver" on B |
| alert dedup | `room.Service.alertRooms` | a repeat of the same Alertmanager alert that the load balancer sent to B opened a second room |
| Slack mirror | `cmd/server/slack.go` | every replica posted every event to Slack |
| migrations | `cmd/server` `migrateRoomDB` | two replicas starting together ran goose at the same time |

WebSocket fan-out already crossed processes (`pg_notify` in `Append`, one `LISTEN` per process),
but every socket ran its own `Since` query on every notify and a 1 s poll, always
(`reviews/load-test-2026-10.md`, follow-up 1).

## 2. Replica identity

`PIPEROOM_REPLICA_ID` (default: the host name; the chart sets the pod name) plus `-` and 6 random
hex characters chosen at process start. The suffix is not optional: a container restarted inside
the same pod must not take the old process's leases or claims for its own. The id appears in
`room_agent_leases.owner`, `proposals.claimed_by` and every leader log line. The in-memory store
(`--demo`, unit tests) is one process: it is always the leader, every lease is its own, and the
code paths below are skipped.

## 3. Singleton loops: leader election

Loops that must run on exactly one replica: **watcher, retention purge, auth prune, Slack mirror**.

- One Postgres **session advisory lock per loop**, taken with `pg_try_advisory_lock(k1, k2)` where
  `k1 = hashtext(current_schema())` and `k2 = hashtext(<loop name>)`. Scoping by schema keeps two
  installs (and the integration tests' per-test schemas) in one database apart.
- The lock lives on a **dedicated connection** opened from the store's pool configuration
  (`application_name = piperoom-leader-<loop>`), held for the whole leadership term, not borrowed
  from the request pool. Server-side TCP keepalives are set on it (`tcp_keepalives_idle=10`,
  `_interval=5`, `_count=3`) so Postgres frees the lock within ~25 s of a vanished client, and the
  client side sets `tcp_user_timeout` (4 s) so a write to a dead peer fails within the check's
  deadline instead of hanging in the kernel.
- A replica that does not get the lock **retries every 15 s**.
- The leader **checks its connection every 2 s** (`SELECT 1`, 2 s deadline). Any error ends the
  term: the loop's context is cancelled, the connection is closed, and the replica goes back to
  retrying.
- **Fencing by settle delay.** A new leader waits **5 s** after taking the lock before it starts the
  loop. The old leader's check notices a dead connection within 2 s + 2 s = 4 s of Postgres freeing
  the lock, so the old loop's context is always cancelled before the new loop starts. (A
  partitioned old leader cancels itself long before Postgres's keepalive frees the lock.) Takeover
  after the leader dies: at most 15 s (retry) + 5 s (settle) = **20 s**.
- Every change is logged at info: `leader: acquired`, `leader: lost` (with the error), `leader:
  released` (shutdown), with `loop` and `replica`.
- A leader's loop gets a fresh state each term (the watcher is rebuilt, so it never acts on
  episodes it observed before it lost the lock).
- **Requirement on every leader loop:** it must stop its side effects within about 1 s of its
  context being cancelled — every external call takes the loop's context, and nothing is written
  after a cancelled call. The settle delay (5 s) only fences loops that obey this. The elector waits
  for the loop to return (no stop timeout) before it releases the term, so a loop that ignores
  cancellation holds the term rather than overlapping the next leader.

The Slack mirror's leader also rescans open rooms every 15 s, because `OnRoomOpened` fires only on
the replica that opened the room. Each mirror resumes from `slack_threads.last_seq`, so a new leader
continues where the old one stopped. The `last_seq` write is a compare-and-set
(`… WHERE last_seq = $previous`): a mirror whose write finds another writer moved on stops, so two
mirrors never keep posting the same room. An event whose post succeeded but whose `last_seq` write
did not can still be posted twice at a leader change (at most one event per room).

## 4. Room agents: one owner per room

### 4.1 The lease

```sql
room_agent_leases (room_id PK → rooms ON DELETE CASCADE, owner text, pipeline_id text,
                   slot boolean, acquired_at timestamptz, expires_at timestamptz)
```

- **TTL 30 s, renewed every 10 s** by one `UPDATE … WHERE owner = $me AND room_id = ANY($ids)` per
  replica. Times are Postgres `now()`; the replica measures its own deadline from *before* the renew
  call, so clock offset between hosts does not matter.
- **Only the lease owner runs the room's agent** (live or shadow). Acquire is
  `INSERT … ON CONFLICT (room_id) DO UPDATE … WHERE expires_at < now() OR owner = $me`: free,
  expired or already ours. A replica that cannot acquire does not start the agent.
- **Self-fencing.** If a renew does not confirm a lease within TTL − one renew interval (20 s after
  the last confirmed renew), the owner cancels that room's agent before the lease can expire; a renew
  that reports the lease gone (taken over) cancels at once.
- **Per-run fence.** Each run has a generation number. The agent's Sink and its `Proposals.Create`
  refuse a write unless this replica holds the lease, the run is the room's current generation, and
  the local clock is before the lease's local deadline. A run that lost its lease (or was replaced)
  can write nothing more, even before its context cancellation reaches it.
- The lease is held for as long as the room "has an agent" in the old single-process sense
  (`roomState.agentCancel != nil`): while the run works *and* while it waits for input. It is deleted
  when the room closes (a shadow room: when its run has finished, grace included), when the
  supervisor gives up after its retries, and set to expire **now** on graceful shutdown so another
  replica takes over at its next scan instead of after 30 s.
- **Policy auto-approve (P2b-4) in a rolling deploy.** The lease owner's loop fires a room's
  `proposal.scheduled`; while an older replica without that loop still holds the room's lease, the fire
  waits until the lease moves to a new replica. That is safe: the proposal stays open, approvable and
  cancellable by hand, and only runs late.

### 4.2 Input reaches the owner through the log

The inbox stays what it was: in memory, fed from events as they are folded. What changes is who
folds what:

- A steer, `@agent` comment, approval or action result written on replica B is an event in
  `room_events` (it always was). The owner folds it from the log (§6) and feeds its inbox. **No
  inbox table**: the event log is the store-backed inbox.
- A replica that is not the owner does not feed its inbox. It keeps, per room, the human inputs
  logged after the agent's last own event (`step_start`, `tool_call`, `tool_result`, `message`,
  `hypothesis`; at most 50). When it becomes the owner it primes the inbox with exactly those, so a
  steer that started the run, or one the dead owner never answered, is not lost.
- `ensureAgentLocked` on a non-owner (steer, `@agent`) tries the lease; if a live owner holds it,
  nothing starts — the owner already sees the event.

### 4.3 Takeover

Every replica scans every 10 s for leases with `expires_at < now()`, and takes one with
`UPDATE … SET owner = $me … WHERE room_id = $1 AND expires_at < now() RETURNING` (exactly one
replica wins). The winner rebuilds the room from the log, then:

| Room | Action |
|---|---|
| closed | delete the lease |
| shadow | mark the shadow run `interrupted` (as the Load sweep does), reveal if closed, delete the lease. Shadow runs are one-shot and never restart |
| `PIPEROOM_RESUME_ON_TAKEOVER=1`, agent status `working` or `queued` (or human input logged after the agent's last event), and fewer than 3 resumes of this room in the last hour | **resume**: a system message ("the server running this room's agent stopped; the agent resumes on another server"), then a fresh run with the inbox primed (§4.2) plus one input listing the room's open proposals (id and title), so the run does not propose them again. It queues for a slot like any run |
| otherwise (flag off — **the default** —, cap reached, idle, done, error) | the restart notice the single-process server writes today ("agent paused; steer or @agent to resume"), delete the lease |

The resume counter lives on the lease row (`resumes`, `resume_window`) and is bumped by the same
conditional `UPDATE` that allows the resume, so the cap holds across replicas. A resumed run starts
from the alert, the pipeline map and the primed input; the runner keeps no memory between runs
(`agent.Run`). This is the same model as a hand-off.

`Load` no longer writes "server restarted" into every open room. With leases, only a room whose
agent status is `working`/`queued` but which has **no lease row at all** (a run from before this
change, or a failed lease write) gets the notice at Load; everything else is handled by the
takeover. With the flag off (default) the result matches main: every agent that was running pauses
after its server stops; the notice arrives once the lease expires (≤ 40 s) instead of at Load.

### 4.4 Slots

- `PIPEROOM_MAX_AGENTS` is now **per replica** (default 3). The local queue is **round-robin across
  pipelines, FIFO within a pipeline**: when a slot frees, the next pipeline after the one served last
  that has a waiting room gets it. One noisy pipeline with 40 rooms cannot starve a quiet one with 1.
  The "N ahead" in the queued status is the room's exact position under that order.
- `PIPEROOM_MAX_AGENTS_GLOBAL` (0 = off) caps working runs across all replicas. A room that got a
  local slot then takes a global one: in one transaction, under
  `pg_advisory_xact_lock(hashtext(current_schema()), hashtext('agent-slots'))`, count the leases with
  `slot = true AND expires_at > now()`; below the cap, set `slot = true` on its own lease. Above it,
  it shows `queued` ("waiting for a global agent slot") and retries every second. Giving the slot back
  sets `slot = false`. A dead replica's slots stop counting when its leases expire. Across replicas
  the order is first-come at the retry, not round-robin; within a replica it is round-robin. A
  database error while taking a global slot is retried with backoff (1 s doubling to 10 s); only a
  lost lease or the end of the run stops the wait, and then the room's agent is released so the next
  steer can start one again (a room is never left "having an agent" that cannot run).

## 5. Action executor: exactly once per approved step

```sql
proposals.claimed_by text, proposals.claimed_at timestamptz
```

- The proposal is claimed **when it is queued** on the room's executor (the replica that recorded the
  last approval), not when its turn comes:
  ```sql
  UPDATE proposals p SET claimed_by = $me, claimed_at = now()
   WHERE p.proposal_id = $1 AND p.claimed_by IS NULL AND p.status = 'open'
     AND jsonb_array_length(p.required_layers) > 0
     AND NOT EXISTS (
       SELECT 1 FROM jsonb_array_elements_text(p.required_layers) AS l(layer)
        WHERE NOT EXISTS (SELECT 1 FROM approvals a
                           WHERE a.proposal_id = p.proposal_id AND a.layer = l.layer
                             AND a.decision = 'approved'))
  RETURNING …
  ```
  No row: another executor has it (or had it) and nothing is queued or written. Each room runs one
  proposal at a time (review item 10), so a second approved proposal can wait in the queue behind a
  long first one; the replica refreshes the claims of **queued** proposals on every lease renew
  (10 s), so a live replica's queue is never mistaken for a dead one.
- **Only a proposal approved in full is claimed** (#638; the rule the code enforces once #638 merges):
  - "approved in full" means every entry of `required_layers` has an `approvals` row with
    `decision = 'approved'`, whatever the grant (owner, backup, solo, fallback, sole-member driver, policy);
  - a proposal with no required layers (`required_layers = []`) is never claimed, so it never runs;
  - a rejection or a cancel blocks the claim for good: neither is undone, so the proposal never qualifies again;
  - the claim `UPDATE` enforces this itself, not only its caller, so a caller bug or a race cannot queue a
    proposal that is not approved in full;
  - the room re-checks the same rule before the claim and before every step (`recheckStep`); a step that
    fails it is not run and records `action.executed {ok: false}` with output `not approved by every required layer`;
  - hash freshness (`params_hash`) is not part of the claim, because `recheckStep` checks it before every step
    (output `params hash mismatch`; `docs/design/auth.md` §3.2).
- Every write of the executor is conditional on the claim (`claimed_by = $me AND status = 'open'`):
  - the claim refresh before each step (no row: the claim was declared dead, the step does not run);
  - the per-step start (`proposal_steps` insert happens only while the claim holds; the existing
    `ON CONFLICT DO NOTHING` still makes a step at-most-once);
  - the step finish, which also refreshes `claimed_at`;
  - the final `executed`/`failed` status.
  A write that finds the claim gone skips the result event: the sweep has already recorded the
  outcome, and the room must not get two.
- **Crash after the claim, before the result.** The step is never run again. It is recorded as
  `action.executed {ok: false, outcome_unknown: true}` with output saying the target must be checked
  before a new proposal retries; the agent gets `HumanInput.OK = nil` ("unknown"), so it never claims
  the pipeline recovered or failed. The same applies when the per-step start finds the step already
  started by an earlier run. *When*: not at the next `Load` of any replica (that would hit runs that
  are alive elsewhere) but once the claim is stale — `claimed_at` older than **step timeout (30 s) +
  2 min** — or, for an approved proposal nobody claimed, 2 min after its last approval. Every replica
  sweeps every 30 s; the interruption itself is a conditional `UPDATE … SET status = 'failed' …
  WHERE status = 'open' AND (claimed_by IS NULL OR claimed_at < $stale)`, so exactly one replica
  writes the event.

## 6. Events across replicas

### 6.1 WebSocket fan-out

- `Append` keeps `pg_notify('room_events', room_id || ':' || seq)` in the insert transaction. The
  payload carries no content.
- Each replica keeps **one LISTEN connection**. Per room with local subscribers there is **one
  feed**: on a notify it reads `Since(tail)` once, keeps the last 64 events, and wakes the room's
  subscribers, which take what they need from that buffer. A subscriber that is further behind (it
  just connected, or it was slow) reads its gap from the store itself, then joins the buffer. One
  query per event per room per replica instead of ~3 per socket.
- While the LISTEN connection is down or reconnecting, **every local feed polls every 2 s**. After a
  reconnect every feed and every room cache catches up once. While LISTEN is up a **safety poll
  every 15 s** does the same, so a notification lost without the connection noticing (a proxy that
  drops idle traffic, a NOTIFY queue problem) costs at most 15 s, never a stuck room.
- Replay after `seq N` is unchanged (`ws.Gateway` reads the backlog, then subscribes after the last
  seq it sent). No change in `internal/ws`.

### 6.2 Room cache

Each replica caches every room's snapshot (as before) and now keeps it current:

- **Before every command** (not before reads: `GET`s answer from the cache the follower keeps), one
  `GetRoom` read, made outside the service lock; if `rooms.last_seq` is ahead, the missing events are
  read (also outside the lock) and folded under it. A room deleted by another replica's retention is
  evicted. The driver lease persisted on the rooms row is taken over too (renewals are not all in the
  log). So a command is checked against the log as of a moment before the check: read-your-writes
  across replicas.
- **Commands whose check must hold at the write** (§9.1) append with `AppendIfLast(expectSeq)`.
- **On every notify** for a cached room, a follower goroutine folds the new events (fetched outside
  the service lock, applied under it). That is how the owner's inbox gets steers within
  milliseconds, how a close on B stops the agent on A (its executor too, and the shadow grace rules
  run there), and how the leader's watcher hears `OnRoomClosed` for rooms closed elsewhere (every
  replica that folds a close runs its room-closed hook, asynchronously). A notify for a room the
  replica does not have loads it, so the alert index and `OpenRoomForPipeline` know rooms other
  replicas opened. While LISTEN is down, the follower re-reads every room every poll interval.
- `emitLocked` folds any events another replica appended between the last fold and its own append
  before it applies its own, so the snapshot never skips a seq.
- `ListRooms` and `OpenRoomForPipeline` refresh from one `ListRooms` query, read outside the lock;
  new rooms are rebuilt and rooms that are behind are read outside the lock too, and only the folds
  and evictions take the lock.
- Cost: each remote event costs one `Since` read per replica (the follower's), plus one per room feed
  with local WebSocket subscribers; a command costs one `GetRoom`.

### 6.3 Alert dedup

A webhook or signed alert with a dedup key takes a session advisory lock on the key for the length
of the open (`pg_advisory_lock(hashtext(current_schema()), hashtext('alert:' || key))`, on a pool
connection, before the service lock). Under it the service looks in its index *and* in the store
(`LastRoomByDedupKey`); an open room found there is loaded and joined. Two replicas getting the same
alert at the same instant therefore open one room.

## 7. Start-up

Migrations run under a session advisory lock (`migrations`), so replicas that start together apply
them once; the others wait for the lock, then find nothing to do.

## 8. Helm

- `replicaCount` may be > 1. With 1 the strategy stays `Recreate`; with more it is `RollingUpdate`
  (`maxUnavailable: 0`, `maxSurge: 1` by default, values `rollingUpdate.*`).
- A `PodDisruptionBudget` is rendered when `replicaCount > 1` (`podDisruptionBudget.minAvailable`,
  default 1; `podDisruptionBudget.enabled` to turn it off).
- `PIPEROOM_REPLICA_ID` comes from the pod name (downward API).
- The "do not scale" warning is replaced by this model and its limits.
- **The first upgrade to this version runs at `replicaCount: 1`.** Pods of the previous version
  have no leases, claims or leader locks; a rolling update with more than one replica would run old
  and new pods side by side. Scale up only after every pod runs this version.

## 8a. Delivery and migration order

Delivered as #120 (this contract), then three stacked PRs: (a) store (hub feeds, elector,
migration lock, Coordinator SQL, migration `00014`), (b) room (leases, takeover, slots, executor
claims, cache sync), (c) `cmd/server` leader wiring, Helm and README. `replicaCount > 1` stays
unsafe until (c) is merged. Migrations merge in number order (`00010` → `00014`); S1's `00014`
lands last, after the parallel `00010`–`00013`.

## 9. Limits and known gaps

1. **Check and append are one step for the commands that need it (conditional append).** Each
   replica serializes its own commands under its service lock, but two replicas do not share it.
   The commands whose check must still hold when the event lands — `GrantControl`, the driver path
   of `Steer`, `SendSuggestion`, `CreateHandoff`, `AcceptHandoff`, `CloseRoom` (and T4's
   `ExtendRoom`, `DeleteRoom` when they land) — append with `AppendIfLast(room, expectSeq)`:
   `UPDATE rooms SET last_seq = last_seq + 1 WHERE id = $1 AND last_seq = $expect`. If another
   replica appended in between (`ErrSeqConflict`), the command folds the new events, runs its check
   again and retries, at most 3 times, then answers `409 conflict`. So two people taking control at
   once get one driver, and a driver replaced on another replica cannot steer as driver afterwards.
   Commands that cannot conflict (comments, `steer.suggested`, notes, agent events) append
   unconditionally (`emitLocked`, `internal/room/service.go:849`; `AddComment`,
   `internal/room/service_steer.go:29`; the suggestion path of `Steer`, `service_steer.go:117`).
   The driver's `steer.sent` is not one of them: it is a checked append
   (`emitCheckedLocked`, `service_steer.go:88`), as listed above. A comment or steer that carries a `client_id` is not in
   that group: `docs/api.md` (Idempotent posts, #459) makes `(room, user, client_id)` unique in the
   store, so exactly one event lands even for two requests racing on different replicas. The store
   does this with a unique index or the same conditional append, not the replica's service lock.
   Not implemented on `main` yet: `AddComment` and `Steer` take no `client_id`, and no migration
   holds the key (`events.go:447-466` only reserves the payload fields). Postgres
   also enforces one decision per approval layer, one start per step, one executor per proposal and
   one room per alert key.
2. **Presence is per replica.** `ws` presence is in memory; people on different replicas do not see
   each other in the member list. Events, approvals and everything else are shared.
3. **The spend budget is per replica until E4 lands.** `PIPEROOM_ROOM_TOKEN_BUDGET` is counted in
   the endpoint's memory; a room whose agent moves to another replica starts a new count there, so
   a room can spend up to (replicas it ran on) × the budget. E4 (the shared spend ledger) closes this.
4. **Rate limits are per replica** (login, alert routes).
5. **Shadow reveal.** If a shadow run finishes on A while B closes the room, both may append
   `shadow.revealed` in a narrow window; the reveal row (`revealed_at`) is set once.
6. Leader loops that were mid-tick when leadership moved are cut by context cancellation (the purge
   transaction of the room in flight rolls back; nothing is half-deleted).
7. **Connections.** Each replica holds, besides its request pool, one LISTEN connection, one lock
   connection per leader loop it holds (up to four) and short-lived alert-key lock connections from
   the pool. Size Postgres `max_connections` for replicas × (pool + 5).
8. **Retention on other replicas.** The leader evicts purged rooms from its cache at once; other
   replicas evict them at their next access or refresh (a purged room reads as not found).

## 10. Constants

| Name | Value |
|---|---|
| lease TTL / renew / takeover scan | 30 s / 10 s / 10 s |
| leader retry / health check / settle | 15 s / 2 s (2 s deadline) / 5 s |
| feed poll while LISTEN is down | 2 s |
| safety poll while LISTEN is up | 15 s |
| resumes per room (flag on) | 3 per hour |
| conditional append retries | 3, then `409 conflict` |
| global slot error backoff | 1 s doubling to 10 s |
| feed buffer | 64 events per room |
| executor claim stale after | 30 s step timeout + 2 min |
| unclaimed approved proposal stale after | 2 min after the last approval |
| global slot retry | 1 s |

## 11. Tests (S1)

PG integration (`-tags integration`, throwaway `postgres:16`), 2–3 services in one process, each
with its own pool: one watcher/retention loop, takeover within 30 s after the leader's connection is
killed; one agent per room; one execution with 50 concurrent claims across replicas; stale claim →
one "outcome unknown" event, no re-run; WebSocket on B sees A's events with NOTIFY and with NOTIFY
off (poll); lease takeover after the owner stops renewing. Unit: round-robin across pipelines, global
cap. `helm template` with `replicaCount=3`.
