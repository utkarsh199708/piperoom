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
  `_interval=5`, `_count=3`) so Postgres frees the lock within ~25 s of a vanished client.
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

The Slack mirror's leader also rescans open rooms every 15 s, because `OnRoomOpened` fires only on
the replica that opened the room. Each mirror resumes from `slack_threads.last_seq`, so a new leader
continues where the old one stopped; an event whose post succeeded but whose `last_seq` write did not
can be posted twice at a leader change (at most one event per room).

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
  that reports the lease gone (taken over) cancels at once. The agent Sink refuses events from a room
  whose lease this replica no longer holds.
- The lease is held for as long as the room "has an agent" in the old single-process sense
  (`roomState.agentCancel != nil`): while the run works *and* while it waits for input. It is deleted
  when the room closes (a shadow room: when its run has finished, grace included), when the
  supervisor gives up after its retries, and set to expire **now** on graceful shutdown so another
  replica takes over at its next scan instead of after 30 s.

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
| agent status `working` or `queued`, or human input logged after the agent's last event | **resume**: a system message ("the server running this room's agent stopped; the agent resumes on another server"), then a fresh run with the inbox primed (§4.2). It queues for a slot like any run |
| otherwise (idle, done, error) | the restart notice the single-process server writes today ("agent paused; steer or @agent to resume"), delete the lease |

A resumed run starts from the alert, the pipeline map and the primed input; the runner keeps no
memory between runs (`agent.Run`). This is the same model as a hand-off.

`Load` no longer writes "server restarted" into every open room. With leases, only a room whose
agent status is `working`/`queued` but which has **no lease row at all** (a run from before this
change, or a failed lease write) gets the notice at Load; everything else is handled by the
takeover. On a single replica this means: an investigation that was mid-run when the server stopped
resumes after the restart (once its lease expires, ≤ 40 s), an idle one stays paused. That replaces
"every room pauses on restart"; it is deliberate and called out in the PR.

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
  the order is first-come at the retry, not round-robin; within a replica it is round-robin.

## 5. Action executor: exactly once per approved step

```sql
proposals.claimed_by text, proposals.claimed_at timestamptz
```

- Before the first step the executor claims the proposal:
  `UPDATE proposals SET claimed_by = $me, claimed_at = now() WHERE proposal_id = $1 AND claimed_by IS NULL AND status = 'open' RETURNING`.
  No row: another executor has it (or had it) and this one returns **without writing anything**.
- The existing per-step claim (`proposal_steps`, `INSERT … ON CONFLICT DO NOTHING`, "started" before
  any side effect) stays: it is what makes a single step at-most-once even if a claim is wrongly
  taken over.
- Before each step the claimer refreshes `claimed_at` (`… WHERE claimed_by = $me`). If that update
  finds no row the claim was declared dead: the step does not run.
- **Crash after the claim, before the result.** Existing semantics, kept: such a step is never run
  again; it is recorded as failed (`action.executed` with `ok=false`), with output saying the outcome
  is unknown and the target must be checked before a new proposal retries. (`ok` is a bool in the
  contract; there is no third value. The agent already treats a failed step as "do not claim
  recovery".) What changes is *when*: not at the next `Load` of any replica (that would hit runs that
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
  reconnect every feed and every room cache catches up once. While LISTEN is up there is no polling.
- Replay after `seq N` is unchanged (`ws.Gateway` reads the backlog, then subscribes after the last
  seq it sent). No change in `internal/ws`.

### 6.2 Room cache

Each replica caches every room's snapshot (as before) and now keeps it current:

- **On every access** (`getStateLocked`: every request and command), one `GetRoom` read; if
  `rooms.last_seq` is ahead, the missing events are folded first. A room deleted by another
  replica's retention is evicted. The driver lease persisted on the rooms row is taken over too
  (renewals are not all in the log). So a command is always checked against the log as of a moment
  before the check: read-your-writes across replicas.
- **On every notify** for a cached room, a follower goroutine folds the new events (fetched outside
  the service lock, applied under it). That is how the owner's inbox gets steers within
  milliseconds, how a close on B stops the agent on A (its executor too, and the shadow grace rules
  run there), and how the leader's watcher hears `OnRoomClosed` for rooms closed elsewhere (every
  replica that folds a close runs its room-closed hook, asynchronously). A notify for a room the
  replica does not have loads it, so the alert index and `OpenRoomForPipeline` know rooms other
  replicas opened. While LISTEN is down, the follower re-reads every room every poll interval.
- `emitLocked` folds any events another replica appended between the last fold and its own append
  before it applies its own, so the snapshot never skips a seq.
- `ListRooms` and `OpenRoomForPipeline` refresh from one `ListRooms` query (load new rooms, fold
  rooms that are behind, evict deleted ones).

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

## 9. Limits and known gaps

1. **Commands are checked, then appended; across replicas that is not one critical section.** Each
   replica serializes its own commands under its service lock and syncs from the log before the
   check, but two conflicting commands on two replicas within the same few milliseconds (two people
   taking control at once, say) can both pass. Postgres still enforces what must never double: one
   decision per approval layer (primary key), one start per step, one executor per proposal, one room
   per alert key. The rest is last-writer-wins in the log, which every replica folds the same way.
2. **Presence is per replica.** `ws` presence is in memory; people on different replicas do not see
   each other in the member list. Events, approvals and everything else are shared.
3. **Token budget is per replica.** `PIPEROOM_ROOM_TOKEN_BUDGET` is counted in the endpoint's memory;
   a room whose agent moves to another replica starts a new count there.
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
