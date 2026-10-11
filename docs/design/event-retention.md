# Design: what the room log stores, and for how long

Task C9, doc 3 of 3 (`auth.md`, `webhook-auth.md`). Design only, no code changed. Written against `main` @ `8e326a3`+;
A5 facts are cited "(a5 @ 935f17d)" from the lead's task file.
**Findings:** F3 (record values in a permanent, broadcast log), F6 (no limit on log growth). Audit needs for F1/F2/F10.
GDPR is cited as a reason for limits (Art. 5(1)(c) data minimisation, 5(1)(e) storage limitation, 17 erasure); this is not legal advice and the customer's own obligations win.

## 1. Problem

- `room_events` is append-only with `PK(room_id, seq)`, and `ON DELETE CASCADE` from `rooms` (a5 @ 935f17d, `migrations/00001_init.sql`). `Seq` has no gaps (`events.go:66-67`);
  `Subscribe` never skips or repeats (`events.go:84-86`); WS `after=N` replays everything after `N` (`docs/api.md:128-143`).
- `rebuild` folds the whole log from seq 0 for every room snapshot (`internal/room/service.go:199`), and the hand-off reads all events (`service_handoff.go:149`). So rows cannot simply be deleted.
- Everything is broadcast live and kept forever. F3 is fixed at the source for the row tools (`a1ceb36`, `bd6e5e9`: `Data` keeps key and timestamp only), but other payloads still hold sensitive text (inventory below).
- Who approved is stored only as the event's `Actor` (the `approvals` table has no approver column, a5 @ 935f17d). If events are pruned, so is the audit trail.
- Nothing caps event size, so one tool result or alert can add megabytes (F6).

## 2. Inventory: where PII or secrets can sit

| Event type (`events.go`) | Fields that can hold sensitive data | Why |
|---|---|---|
| `room.created` (:101) | `Alert` (raw webhook body, up to 1 MB), `Title` | sender text; may contain hostnames, user names, tokens in labels |
| `agent.tool_call` (:126) | `Input` | key values, e.g. `pg_row` key, table names |
| `agent.tool_result` (:134) | `Data`, `Summary` | `pg_row`/`warehouse_row` now reduced; `Query` and `Username` in `pg_long_transactions` / `pg_recent_ddl` (`clients.Transaction`, `clients.DDLChange`) can hold SQL with literals and role names; connector config (A2 already redacts secret-like keys) |
| `agent.message`, `agent.hypothesis` (:148, :155) | `Text` | model text may repeat any of the above |
| `comment.added`, `steer.suggested`, `steer.sent` (:162-178) | `Text` | free text from people; pasted logs, tokens, customer ids |
| `proposal.created` (:199) | `Params`, `Description`, `Risk`, `Rollback` | params are action-validated (connector, subject), descriptions are model text |
| `approval.given` / `approval.rejected` (:209, :215) | `Reason` (free text); `Subject` (proposed in `auth.md`) | `Subject` is a personal identifier |
| `action.executed` (:222) | `Output` | text returned by Connect or the Registry |
| `handoff.created` (:240) | `Summary.*` | model text from the whole room |
| `trace.pinned` (:261) | `Key`, `Stages[].Facts` | record key and per-stage facts |
| every event | `Actor.Name` | a person's display name |

## 3. Options

### 3a. Where to redact at write time

| | A. In each tool (A3/A2) | B. In the agent `Sink` (A4) | C. In `Store.Append` (A5) |
|---|---|---|---|
| Security | best at the source, but every new tool must remember | covers agent events only; human events (comments, steers) bypass it | one choke point for **all** event types, including human text and webhook bodies |
| Effort | already done for row tools | small | medium; one policy table |
| Gap | no cover for human text | no cover for human text | cannot know what a tool meant to hide, so it also needs caps and key-based rules |

### 3b. Retention without breaking replay

| | A. Keep forever, write-time redaction only | B. Compact in place: keep `room_id, seq, type, actor, at`, remove content from `payload` (recommended) | C. Archive closed rooms after N days, then delete |
|---|---|---|---|
| Replay / `after=N` | unchanged | unchanged: every seq still exists, so no gaps and no skipped events | a client below the first kept seq gets nothing |
| Storage limit (GDPR 5(1)(e)) | not met | met for content; audit rows stay for their own period | met |
| Audit | full | preserved: audit types keep non-text fields | lost unless copied elsewhere |
| Snapshot rebuild | unchanged | must tolerate removed fields (§4 rule 6) | rooms vanish |
| Effort | none | medium (compactor + client rendering) | medium + a 410 path in API and WS |
| Dev impact | none | none (off when env empty) | none |

If option C were chosen, `GET /api/rooms/{id}/events?after=N` on a deleted room answers `410 gone` with `{"error":{"code":"gone"}}`, and the WebSocket sends an `error` message with
`code:"gone"` then closes. A kept room with `after` below its first kept seq cannot occur, because C deletes whole rooms only.

## 4. Recommendation

1. **Redact in `Store.Append` (3a-C), with sources still minimising (3a-A).** The append path applies a per-type field policy and caps before writing, so the broadcast
   (which comes from the stored event) is redacted too.
   - `agent.tool_result.Data` > `MaxToolDataBytes` (16 KiB) → `{"truncated":true,"bytes":N}`. `Alert` > `MaxAlertBytes` (64 KiB) → same shape. Any `Text`/`Output`/`Reason` > `MaxTextBytes` (4 KiB) is cut with an ellipsis.
   - JSON values under keys matching `password|secret|token|authorization|api[_-]?key|credential` (case-insensitive, any depth) → `"***"`, for `Alert`, `Data`, `Input`, `Params`. A2's `sensitiveKey` in `kafkatools` is the model.
   - URL user-info `scheme://user:pass@host` → `scheme://***@host` in all text fields.
   - A3 strips literals from SQL in `Query` (strings and numbers → `?`) before it enters `Data`.
2. **Compact in place (3b-B).** `events.Compactor.Compact(ctx, closedBefore)` runs from a ctx-bound loop in `cmd/server`. For rooms closed before `now - PIPEROOM_RETENTION_CONTENT_DAYS`:
   - **Content types** (`agent.*`, `comment.added`, `steer.*`, `handoff.created`, `trace.pinned`): payload becomes `{"_redacted":true}` plus the non-sensitive structural keys (`step_id`, `tool`, `tool_use_id`, `is_error`, `comment_id`, `suggestion_id`, `handoff_id`, `table`).
   - **Audit types** (`room.created`, `room.closed`, `proposal.created`, `approval.*`, `action.executed`, `control.*`, `handoff.accepted`): keep all non-free-text fields (`pipeline_id`, `severity`, `proposal_id`, `layer`, `step_index`, `ok`, `Subject`, `Params`, `required_layers`);
     remove `Alert`, `Description`, `Risk`, `Rollback`, `Reason`, `Output`; keep `Title` and `Params` (they are action-validated, so they hold names not data), and set `_redacted:true`.
   - Idempotent; touches `payload` only. `Seq`, `Type`, `Actor`, `At` never change.
3. **Audit rows outlive content.** A second, shorter list is kept for `PIPEROOM_RETENTION_AUDIT_DAYS` after close: in the same transaction as each audit-type append, A5 also inserts into an append-only `audit_log` table
   (`room_id, seq, type, actor, at, retained fields, prev_hash, hash`) where `hash = SHA-256(prev_hash || canonical row)`, one chain per room. The application role gets `INSERT, SELECT` only (no `UPDATE`/`DELETE`)
   (OWASP Logging cheat sheet, "Protection"). After the audit period the room and its audit rows are deleted together.
4. **Defaults.** Both settings empty = nothing is compacted or deleted (development). The pilot proposal: content 30 days, audit 400 days (Open question 1).
5. **Erasure (Art. 17).** `cmd/server erase --user <id>`: replaces `Actor.Name` with `(removed)` in that user's events and clears `Subject`; the internal `Actor.ID` stays as a pseudonym, and removing the user's entry in `users.yaml` severs it from a person.
6. **Rebuild must tolerate compaction.** `rebuild` (`service.go:199`) uses only fields that survive for closed rooms (`title`, `severity`, `pipeline_id`, `status`, `opened_at`, `closed_at`, `resolution`, driver, approvals). Hypothesis text and open suggestions are empty after compaction. A5 tests this.
7. **Clients** treat `payload._redacted == true` as "content removed by retention policy" and render the event row without its text.

## 5. Contract changes

`internal/events/events.go` (caps, marker, `Compactor`; additive, `Store` is unchanged):
```diff
--- a/internal/events/events.go
+++ b/internal/events/events.go
@@ -86,6 +86,24 @@
 	Subscribe(ctx context.Context, roomID string, afterSeq int64) (<-chan Event, error)
 }
 
+// Size caps applied by the room service when it appends an event (docs/design/event-retention.md).
+const (
+	MaxToolDataBytes = 16 << 10 // AgentToolResultPayload.Data; larger → {"truncated":true,"bytes":N}
+	MaxAlertBytes    = 64 << 10 // RoomCreatedPayload.Alert
+	MaxTextBytes     = 4 << 10  // comment, steer, hypothesis, agent message, action output, rejection reason
+)
+
+// PayloadRedacted is the payload key set to true on an event whose content was removed after retention.
+const PayloadRedacted = "_redacted"
+
+// Compactor removes content from the events of closed rooms. It never changes Seq, Type, Actor or At, so replay
+// by Seq still works. It is separate from Store so existing Store implementations keep compiling.
+type Compactor interface {
+	// Compact redacts the payloads of rooms closed before the given time and returns how many events it changed.
+	// It is idempotent. Event types that are audit records keep their non-text fields.
+	Compact(ctx context.Context, closedBefore time.Time) (int, error)
+}
+
 // Decode unmarshals an event payload into T.
 func Decode[T any](e Event) (T, error) {
 	var v T
```
`docs/api.md` (replay rule):
```diff
--- a/docs/api.md
+++ b/docs/api.md
@@ -140,4 +140,4 @@
 { "type": "presence", "state": "viewing" }    // send every 20 s; "away" when tab hidden
 ```
 The server marks a member `away` after 45 s without any client message (2× the heartbeat plus slack).
-Rules: the server first replays every event with `seq > after`, then streams live events. Events are never skipped or repeated. **On first open of a room the client connects with `after=0`** (full replay); on reconnect it passes its highest seen seq. Presence is not stored in the event log.
+Rules: the server first replays every event with `seq > after`, then streams live events. After the retention period (`docs/design/event-retention.md`) the events of a closed room stay in the log with the same `seq`, `type`, `actor` and `at`, but free-text and data fields in `payload` are removed and `payload._redacted` is `true`; clients must render such events without those fields. Events are never skipped or repeated. **On first open of a room the client connects with `after=0`** (full replay); on reconnect it passes its highest seen seq. Presence is not stored in the event log.
```
`internal/config/config.go` (empty = off):
```diff
--- a/internal/config/config.go
+++ b/internal/config/config.go
@@ -18,6 +18,8 @@
 	UsersFile         string // USERS_FILE
 	Model             string // PIPEROOM_MODEL
 	Effort            string // PIPEROOM_EFFORT
+	RetentionContent  string // PIPEROOM_RETENTION_CONTENT_DAYS (days after room.closed; "" = never redact content)
+	RetentionAudit    string // PIPEROOM_RETENTION_AUDIT_DAYS (days after room.closed; "" = never delete)
 	// The Anthropic SDK reads ANTHROPIC_API_KEY itself; it is never stored here.
 }
 
@@ -35,6 +37,8 @@
 		UsersFile:         get("USERS_FILE", "dev/users.yaml"),
 		Model:             get("PIPEROOM_MODEL", "claude-opus-5-5"),
 		Effort:            get("PIPEROOM_EFFORT", "high"),
+		RetentionContent:  get("PIPEROOM_RETENTION_CONTENT_DAYS", ""),
+		RetentionAudit:    get("PIPEROOM_RETENTION_AUDIT_DAYS", ""),
 	}
 }
 
```

## 6. Migration from today's dev mode

1. Caps and key redaction apply from the first deploy; existing rows are not rewritten except by `Compact`. Dev stays on empty retention settings, so nothing is compacted.
2. `audit_log` is filled for new events only. For rooms created before the migration, the chain starts at the first audit event after it (a documented gap).
3. `MaxAlertBytes` (64 KiB) is smaller than `alertin.MaxBodyBytes` (1 MiB): alerts between the two are stored as `{"truncated":true,"bytes":N}`. The webhook still accepts them.
4. Run `Compact` once on a copy of a real dataset before the first production use; it must be a no-op on rows already compacted.

## 7. Tasks per owner

- **A5 (1)** Redaction in `Store.Append`: caps, key-based and URL redaction, per-type policy table; unit test that **every** `events.Type` has a policy (new types fail the test until classified).
- **A5 (2)** `Compactor` implementation, loop in `cmd/server` (stops with ctx), idempotence test, replay test (`after=0` returns contiguous seqs after compaction), rebuild test.
- **A5 (3)** `audit_log` migration with hash chain and an `INSERT, SELECT` role grant; `verify-audit` subcommand that walks a chain.
- **A5 (4)** `erase --user` subcommand; stop storing a fresh `Actor.Name` copy if the lead agrees (Open question 3).
- **A3** Strip literals from `Query` and drop `Username` in `pg_long_transactions` / `pg_recent_ddl` results before they reach `Data`.
- **A4** Make `Sink` and hand-off input use only `Summary`, never `Data` (already true for hand-off, `renderRoomEvents`); add a test.
- **A6** Render `_redacted` events as a one-line notice; Vitest cases for `agent.tool_result` and `comment.added`; the reducer must not crash on missing text.
- **A1** Seed/test data for a closed old room; dev env leaves retention empty.
- **C5** none.

## 8. Open questions

1. Durations: content 30 days and audit 400 days are proposals. What does the pilot customer's policy say?
2. Keep `Title` and `Params` in audit events after compaction? Proposed yes (action-validated); `Title` comes from alert text cleaned by alertin.
3. Stop persisting `Actor.Name` in `room_events.actor` and resolve names from `users.yaml` at read time? That changes the `Event` JSON shape for clients, so it is a contract decision.
4. Is a per-room legal hold needed in the first release? Not designed here.
5. Backups: compaction does not touch backups. State the backup retention in the customer contract.
6. Anthropic-side retention of requests is a contract matter and was not verified here.

## 9. References

OWASP Logging cheat sheet ("Data to exclude", "Protection") and the GDPR articles are cited from the lead's task file; they could not be fetched from this sandbox and need a reviewer check.
- OWASP Logging: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
- GDPR Art. 5: https://gdpr-info.eu/art-5-gdpr/  Art. 17: https://gdpr-info.eu/art-17-gdpr/
