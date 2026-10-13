# Retention, room limits and deletion

Status: contract (branch `lead/retention-contract`), implementation in `t4/retention`.
Closes security finding F6 (`docs/security/findings.md`) and the storage half of F3.

## 1. Problem

Before this change:

- `PIPEROOM_RETENTION_DAYS` deleted **closed** rooms only. An open room lived forever.
- A room's event log had no count or size cap. `MaxTurns` resets on every human input, so a watched
  room could grow without bound, and every first open replays the whole log (`after=0`).
- Tool `Data` had no size cap (only `Summary` is capped, by `tools.MaxSummaryBytes`).
- There was no way to delete one room (a customer asks for erasure, a secret was pasted).

## 2. Decisions (LEAD-ARCH)

| # | Decision |
|---|---|
| 1 | Open-room limit `PIPEROOM_MAX_OPEN_DAYS`, default 14, `0` = off. Warning 24 h before, then auto-close with resolution `retention: open longer than N days`. The driver may extend **once** by N days (`POST /api/rooms/{id}/extend`, event `room.extended`). |
| 2 | Per-pipeline retention: `owners.yaml` pipeline `retention_days:`. The shorter of it and `PIPEROOM_RETENTION_DAYS` wins when both are set. Validated at start-up. |
| 3 | Per-room caps: `PIPEROOM_ROOM_MAX_EVENTS` (20000), `PIPEROOM_ROOM_MAX_BYTES` (256 MiB, stored payload size), `PIPEROOM_EVENT_MAX_DATA_BYTES` (64 KiB). Warning at 90%; at 100% the agent stops (`log_full`) and agent events are refused; humans keep a 2% reserve. Enforced in the store, in the same transaction as the insert. |
| 4 | Admin-only `DELETE /api/rooms/{id}?reason=`; audit row in `room_deletions`; `devctl rooms delete`; open WebSockets closed with `room deleted`; idempotent; 404 for an unknown id, 403 for a non-admin. A delete is an erase: no copy unless `?archive=true` (`devctl … --archive`). Retention purges write an audit row too (`deleted_by` `system`, `reason` `retention: closed N days`). |
| 5 | Optional `PIPEROOM_ARCHIVE_DIR`: retention writes the room's export there before each purge, an admin delete only with `?archive=true` (`<dir>/<pipeline>/<room>.json`, `0600`); a failed write skips the delete. |
| 6 | No `web/**` change in this work (UI v2 track); the PR lists the UI follow-up (section 9). |

## 3. Closed-room retention per pipeline

Effective days for a closed room of pipeline `p`:

| `PIPEROOM_RETENTION_DAYS` (G) | `retention_days` of `p` (R) | Effective |
|---|---|---|
| 0 / empty | not set | keep forever |
| 0 / empty | R | R |
| G | not set | G |
| G | R | min(G, R) |

Rooms of a pipeline that is not in `owners.yaml` (an alert for an unknown pipeline) use G.
`retention_days` must be a whole number from 1 to 36500; `0`, negatives, decimals and strings stop the start
(owners.yaml is decoded with unknown keys refused, so a typo of the key stops the start too).

One run (every 6 h, after Load) purges, for each pipeline with an effective R, the closed rooms of that pipeline
older than R, then (when G > 0) the closed rooms of every pipeline older than G. Because R <= G whenever both
exist, the second pass never needs to skip a pipeline.

## 4. Open-room limit

- Deadline `D` = `opened_at + N days`, or `room.extended.close_at` after the extension. N is read from the
  environment at each check, so changing it changes the deadline of rooms that were not extended.
- A background loop (every 10 minutes) looks at every cached open room:
  - no `room.retention_warning` with `close_at >= D` yet and `now >= D - 24h`: append one (system actor) with
    `close_at = max(D, now + 24h)`. So no room is closed less than 24 h after its warning, even one that is
    already past `D` when the limit is turned on or lowered.
  - warned and `now >= close_at`: close the room as the system actor with `retention: open longer than K days`,
    K = whole days from `opened_at` to `close_at`. The close runs the normal close path (alert key released,
    agent and executor stopped, shadow grace and reveal, the room-closed hook), so nothing differs from a
    driver's close except the actor. A full room log never refuses `room.closed`.
- Extend: driver only, open rooms only, once (`409 already_extended`), `400 validation` when N = 0. New deadline
  = the close time the room shows (`auto_close_at`) + N days. It renews the driver lease like other driver
  actions.
- The snapshot carries `auto_close_at` (the warned `close_at`, else `D`; null when off or closed) and
  `extended`, so a list view can show the deadline without folding the log.

## 5. Per-room caps

### 5.1 Two classes of event

| Class | Who | Limit |
|---|---|---|
| agent | events whose actor kind is `agent` (status, steps, tool calls and results, messages, hypotheses, agent proposals) | `events < MAX_EVENTS` and `bytes + size <= MAX_BYTES` |
| reserve | human and system actors (comments, steers, suggestions, control, approvals and rejections, action results, hand-offs, traces, system notes, the cap and retention events, `agent.status log_full`) | the same with each cap raised by 2% (at least 1) |
| always | `room.closed` | no cap: one per room |

`size` is the length of the payload JSON the store writes (`json.Marshal(payload)`), not the jsonb on-disk size.

### 5.2 Store enforcement

- Postgres: migration `00012` adds `rooms.event_count bigint` and `rooms.byte_size bigint`, backfilled from
  `last_seq` and `octet_length(payload::text)`. The backfill is an estimate of what was written: jsonb text has
  one space more after each `:` and `,`, and one byte instead of Go's six-byte escape for each `<`, `>` and
  `&`, so `written - 5 x (escaped <>&) <= byte_size <= written + (':' and ',')` (numbers in exponent form and
  repeated keys in a raw alert body can differ further). A test seeds rooms at `00009` and pins this bound.
  `Append` does
  `UPDATE rooms SET last_seq = last_seq+1, event_count = event_count+1, byte_size = byte_size+$size WHERE id = $1 AND <class limit> RETURNING ...`
  in the insert's transaction. The row lock serialises concurrent appends of one room, so no interleaving can
  pass a limit: the agent class never exceeds the cap, the reserve class never exceeds cap + 2%.
  No row back: the room is missing (`ErrNotFound`) or over its limit (`ErrLogFull`), told apart by a second read
  in the same transaction.
- Memstore: the same check under its mutex.
- Limits are set on the store once at start (`SetLimits`); zero limits mean no cap (unit tests, tools).
- The room service follows the usage without a query per append: the event count is the last seq (the log has
  no gaps) and the bytes the store counts for an event are `len(Event.Payload)` as `Append` returns it. It reads
  the store's counters once (`RoomUsage`) when a room enters its cache.

### 5.3 Room service

- After each append: usage >= 90% of either cap and the snapshot shows no warning yet -> append
  `room.log_cap_warning` (system). Usage at 100% of either cap and the snapshot is not `log_full` -> cancel the
  room's agent and append `agent.status {state: "log_full"}` (system). Both are folded into the snapshot
  (`log_cap`), so a restart neither repeats them nor restarts the agent.
- Any event refused with `ErrLogFull` triggers the same stop (a byte cap can be passed by one large event while
  the count is still low).
- `ensureAgentLocked` never starts an agent in a `log_full` room.
- A human route refused with `ErrLogFull` answers `409 log_full`. A repeat alert joining a full room still
  answers with the room, and a watcher note into a full room is dropped (both logged), so a sender does not
  retry forever.
- A shadow room's own log has the same caps on its own counts (`shadow_runs.last_seq` and
  `shadow_runs.byte_size`, same classes, checked in `AppendShadow`'s transaction). When the shadow agent's
  event is refused, the service cancels the shadow run, which ends with status `budget` (like a run that spent
  its tokens). Nothing is written to the room log for it, and there is no 90% warning in a shadow log.

### 5.4 Data size

`PIPEROOM_EVENT_MAX_DATA_BYTES` is applied by the room service before the append, on the typed payload:
`agent.tool_result.data` over the limit becomes `events.TruncatedData`
(`{"truncated": true, "original_bytes": N, "preview": "..."}`, preview cut at a UTF-8 boundary so the object fits)
and the payload's `truncated` is set; `action.executed.output` over the limit is cut and ends with
`… [truncated: N bytes]`. Shadow events get the same treatment (same code path).

### 5.5 Bounds

`PIPEROOM_ROOM_MAX_EVENTS` 100–490000 (cap + reserve stays under the export's 500000 events, so every room can
still be exported and archived); `PIPEROOM_ROOM_MAX_BYTES` 1 MiB–16 GiB; `PIPEROOM_EVENT_MAX_DATA_BYTES` 1 KiB–16
MiB and not above the room cap; `PIPEROOM_MAX_OPEN_DAYS` 0–3650. Sizes take `KiB`, `MiB`, `GiB`. A bad value stops
the start and names the variable (`config.ParseLimits`).

## 6. Deletion

- Role: there is no admin role in `docs/design/auth.md`, so per decision 4 it is users.yaml `admin: true`. It
  grants deletion only. `GET /api/me` shows `admin: true` so the UI can show the button.
- `DELETE /api/rooms/{id}?reason=&archive=` (reason 1–500 characters, storable text). A delete is an erase:
  nothing is kept unless `archive=true`; `archive=true` without `PIPEROOM_ARCHIVE_DIR` is `400 validation`.
  1. not an admin -> `403 not_allowed` (viewers already get `403 viewer`), before any lookup, so a non-admin
     learns nothing about which ids exist;
  2. room missing: an audit row for it (an admin delete or a retention purge) -> `200` with that row
     (idempotent); none -> `404 not_found`;
  3. under the service lock: mark the room deleting (later appends through the service are refused), stop its
     agent and executor, release its alert key;
  4. with `archive=true`: write the export (section 7); failure -> `500 archive_failed`, the room is
     un-marked and kept;
  5. one store transaction: lock the room row, check `last_seq` still equals what was archived, delete events
     (batches of 5000), approvals, steps, proposals, suggestions, hand-offs, Slack thread row, shadow events and
     run, the room row, and insert the `room_deletions` row
     `(room_id, pipeline_id, deleted_by, deleted_at, reason, event_count)` - ids and counts only;
  6. drop the room from the service cache, run the room-closed hook if the room was open (token count and
     watcher cooldown), and close the room's WebSockets: one last `room.deleted` frame (seq = last seq + 1,
     never stored) and close status 1000 with reason `room deleted`.
- `devctl rooms delete <id> --reason <text> --as <admin> [--archive]` calls the route on `PIPEROOM_HTTP_ADDR` (or
  `--server`), so the running server's cache and sockets are updated too. `--as` sends `X-User` (dev mode); in
  oidc mode pass the session cookie in `PIPEROOM_COOKIE`.
- Retention purges write a `room_deletions` row too, in each room's purge transaction: `deleted_by`
  `system`, `reason` `retention: closed N days` (N = the days that applied to the room's pipeline), the
  room's event count.

## 7. Archive

- Path `<dir>/<pipeline_id>/<room_id>.json`. Pipeline ids match `^[a-zA-Z0-9_.-]+$`; `.` and `..` are written
  as `_`, and every file operation goes through `os.Root`, so nothing can land outside `<dir>`.
- Directory `0700`, file `0600`, written to a temporary name, synced and renamed, so a crash never leaves a
  half file under the final name.
- Content: exactly `GET /api/rooms/{id}/export` (with the `integrity` hash). The export's own check
  (`ErrExportChanged`) fails the archive if the log moved while it was written.
- The purge transaction deletes only when `last_seq` is still the archived seq (a shadow room can get a label
  or grade after close); otherwise the room is left for the next run.
- At start the server refuses a `PIPEROOM_ARCHIVE_DIR` that is not an existing, writable directory.
- The archive holds the room's content: a purge with an archive configured, or a delete with `archive=true`,
  does **not** erase the data. Its retention and access are the operator's job.

## 8. Tests that must fail without each guard

Fake clock: warning at `D - 24h` and not before; one warning per deadline; auto-close at `D`; a room past `D` when first seen is warned and closed 24 h later; extend once
(second `409`), driver only, closed room `409`; extension moves the deadline and re-warns. Caps on memstore and
Postgres (throwaway `postgres:16`): 90% warning once; 100% agent refused, `log_full` written once, agent stopped
and not restarted by a steer; human events up to the reserve then `ErrLogFull`; `room.closed` always accepted;
concurrent agent and human appends never pass cap / cap + reserve. Data truncation of tool data and action
output. Delete: admin `200`, member `403`, viewer `403`, unknown `404`, repeat `200` with the same row, audit row
written, events gone, WebSocket gets `room.deleted` and closes; no archive file without `archive=true`, `400`
for `archive=true` without a directory. Archive written before purge (mode `0600`, content verifies), archive
failure skips the purge (room kept, retried next run); each purge writes its audit row. Shadow log caps on
memstore and Postgres; a shadow run stopped by them ends as `budget`. Backfill within the stated bound. Bad env
and owners values stop the start.

## 9. UI follow-up (not in this work)

- Room header: when `auto_close_at` is within 24 h (or a `room.retention_warning` arrived), a banner
  "This room closes automatically at <time>"; for the driver an **Extend by N days** button (`POST …/extend`,
  hidden when `extended` is true or the limit is off; show `409 already_extended` inline).
- Log cap banner: `log_cap: "warning"` -> amber "This room's log is at 90% of its limit"; `"full"` -> red "The
  room log is full: the agent has stopped. Comments, approvals, hand-off and close still work." Render
  `agent.status log_full` as "Agent stopped · log full" instead of the generic pill.
- Admin only (`/api/me.admin`): a **Delete room** item in the room menu with a required reason field, a "keep a copy in the archive" checkbox (sends `archive=true`; off by default, a delete is an erase) and a
  typed confirm of the room title; on success navigate to the room list. Handle the WebSocket `room.deleted`
  frame / close reason `room deleted` by showing "This room was deleted by <name>: <reason>" and not
  reconnecting.
- Tool step detail: when `data.truncated` is true, show "Data truncated (N bytes)" and the preview as text.
