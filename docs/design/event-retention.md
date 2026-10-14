# Design: what the room log stores — deltas on `retention.md`

Task C9, doc 3 of 3. This is a design only; no code is changed. **Findings:** F3 (sensitive text in a permanent,
broadcast log) and the audit needs of F1/F2/F10. GDPR is cited as a reason for limits: Art. 5(1)(c) data
minimisation, 5(1)(e) storage limitation, Art. 17 erasure. This is not legal advice; the customer's own obligations
take precedence.

**Baseline: `docs/design/retention.md`** (the lead's contract, merged in #113; implemented by `t4/retention`). It
already decides:
- the open-room limit (`PIPEROOM_MAX_OPEN_DAYS`);
- per-pipeline closed-room retention (`PIPEROOM_RETENTION_DAYS`, `owners.yaml` `retention_days:`);
- per-room caps (`PIPEROOM_ROOM_MAX_EVENTS`, `PIPEROOM_ROOM_MAX_BYTES`), enforced in the store;
- the 64 KiB tool-data cap (`PIPEROOM_EVENT_MAX_DATA_BYTES`);
- admin room delete as an erase, with `room_deletions` audit rows;
- the optional archive (`PIPEROOM_ARCHIVE_DIR`), which holds the export with its `integrity` hash.

This doc does not change any of that and adds no retention period or size cap of its own. It proposes four deltas.

## 1. What main does today, beyond `retention.md`

| What | Where |
|---|---|
| Read-tool summaries, data and errors are masked for secrets before they are emitted, stored or wrapped for the model | `internal/agent/redact.go` (`scrubToolResult`, `scrubToolError`), rules in `internal/redact` |
| Past room text is redacted when the similar-incidents index is built | `internal/tools/similarincidents/room.go` |
| Human text (comments, steers, suggestions, rejection reasons), the raw alert body in `room.created.alert`, and `action.executed.output` are stored as sent | `internal/store/store.go:395` (`PG.Append` marshals the payload unchanged) |
| A snapshot is rebuilt by folding the whole log from seq 0 in pages of 1000 (`Load` for every room at start, and on a cache miss) | `internal/room/service.go:460-480`, callers `:295`, `:446` |
| Who approved is the event's `Actor` (`kind`, `id`, `name`, `verified`); no other record of it survives a purge or delete except `room_deletions` (ids and counts only) | `internal/events/events.go:73`, `retention.md` §6 |
| Person erasure: none. `retention.md` erases rooms; `auth.md` unbinds an IdP identity, but the person's display name stays in every event they wrote | `docs/design/auth.md` (Unbind), `retention.md` §6 |

## 2. Delta 1 — write-time redaction of human and system text

**Problem.** Secret masking covers only what the agent's read tools return. Engineers paste logs, connection strings
and tokens into comments and steers. Alert senders put tokens and URLs with user-info into labels. Connect returns
config in action output. All of it is stored for the room's lifetime and broadcast to every viewer, Slack and the
export.

| | A. At each route (comments, steers, webhook, executor) | B. One wrapper around `events.Store.Append` (recommended) | C. A Postgres trigger |
|---|---|---|---|
| Coverage | only the routes someone remembered | every event type, every writer (room service, watcher, executor, shadow) | every row, including scripts |
| Fits the size caps | yes | yes, if it runs before the store counts bytes | no: the count is taken in Go before the insert |
| Effort | several small changes | one wrapper type, one policy table | PL/pgSQL rules duplicating `internal/redact` |

**Proposal (B).**
- `store.Redacting(events.Store, *redact.Redactor) events.Store` wraps `Append` (and `AppendShadow`). It applies
  `redact.JSON` to the payload before it is passed on. The rules are the existing ones in `internal/redact`, and so
  are the stats, which are logged as counts only, never values.
- `cmd/server` wires the wrapper once around the real store, so the byte count of `retention.md` §5.2 sees the
  redacted payload.
- **Policy per event type.** Free text and raw bodies are redacted. Ids, seqs, layers, step indexes and the action
  `params` are not, because changing `params` would break `params_hash` (`auth.md`). A unit test fails when a new
  `events.Type` has no policy entry.
- **Dedup is unaffected.** The byte-identical dedup key is hashed from the incoming body before anything is stored
  (`internal/alertin/parse.go:335-340`). The policy must leave `room.created.dedup_key` unredacted, because
  `LastRoomByDedupKey` reads it. A test sends the same body containing a token twice and expects one room.
- The agent-side scrub stays: it masks the text before the model sees it, which this wrapper cannot do. On agent
  events the wrapper is a no-op in practice, because masking is idempotent.

No contract file changes: `events.Store` keeps its signature. The doc comment for `Append` gains one sentence ("may
mask secrets in the payload before storing"), which is for the lead to add.

## 3. Delta 2 — audit hash chain with an external anchor

**Problem.** After a retention purge or an admin delete, nothing shows who approved which action, unless the operator
set an archive. The archive holds all of the room's content, which defeats the erase. Inside the database, anyone with
`UPDATE` on `room_events` can rewrite an approval. The export `integrity` hash only covers one export at the moment it
is taken.

| | A. Keep the archive only | B. `audit_log` table with a per-room hash chain (recommended) | C. B plus a daily anchor of chain heads outside the database (recommended with B) |
|---|---|---|---|
| Survives purge without keeping content | no | yes: ids, types, actor ids, hashes; no free text | yes |
| Detects edits by a DBA or a restored backup | no | no: the whole chain can be recomputed | yes, back to the last anchor |
| Effort | none | migration + insert in the append transaction | + one daily loop and a file sink |

**Proposal (B + C).**
- New table `audit_log(room_id, seq, type, actor_kind, actor_id, at, fields jsonb, prev_hash bytea, hash bytea, primary key (room_id, seq))`.
  - `hash = SHA-256(prev_hash || JCS(row without hash))`, one chain per room.
  - `fields` holds only non-text fields: `pipeline_id`, `proposal_id`, `layer`, `step_index`, `ok`, `params_hash`.
  - It holds no `actor.name`. That way erasing a person (Delta 4) never breaks a chain.
- Rows are written in the same transaction as each audit-class append: `room.created`, `room.closed`,
  `proposal.created`, `approval.given`, `approval.rejected`, `action.executed`, `control.*`, `handoff.accepted`.
- The application role gets `INSERT, SELECT` on `audit_log`, not `UPDATE` or `DELETE`. Audit rows are kept after a
  purge or delete, like `room_deletions`; their own retention is Open question 1.
- **Anchor.** With `PIPEROOM_AUDIT_ANCHOR_DIR` set:
  - Once a day, the leader (`replicas.md` §3) writes one file `<dir>/<yyyy-mm-dd>.jsonl` with `(room_id, seq, hash)`
    for every chain that changed.
  - The operator points the directory at append-only storage, such as an object-store bucket with a retention lock.
  - `devctl audit verify <room>` recomputes the chain and compares it with the last anchored head.
- Honest limit: edits made after the last anchor and before the next one are not detected.

Contract change in `internal/config/config.go`. `git apply --check` passes on main:

````diff
--- a/internal/config/config.go
+++ b/internal/config/config.go
@@ -143,6 +143,9 @@ type Config struct {
 	RoomMaxBytes      string // PIPEROOM_ROOM_MAX_BYTES: stored payload bytes of one room log; default 256MiB
 	EventMaxDataBytes string // PIPEROOM_EVENT_MAX_DATA_BYTES: tool_result.data (and action.executed.output) bytes of one event; default 64KiB
 	ArchiveDir        string // PIPEROOM_ARCHIVE_DIR: absolute directory; when set, retention writes the room's export there before a purge, and an admin delete does when asked (?archive=true); empty = off
+	// AuditAnchorDir receives one file per day with every room's audit chain head
+	// (docs/design/event-retention.md); point it at append-only storage. Empty = off.
+	AuditAnchorDir string // PIPEROOM_AUDIT_ANCHOR_DIR
 
 	// Login (docs/design/auth.md §6). Raw strings; the auth package parses and
 	// checks them at start-up. Not wired until auth PR 2.
@@ -256,6 +259,7 @@ func Load() Config {
 		RoomMaxBytes:      get("PIPEROOM_ROOM_MAX_BYTES", DefaultRoomMaxBytes),
 		EventMaxDataBytes: get("PIPEROOM_EVENT_MAX_DATA_BYTES", DefaultEventMaxDataBytes),
 		ArchiveDir:        get("PIPEROOM_ARCHIVE_DIR", ""),
+		AuditAnchorDir:    get("PIPEROOM_AUDIT_ANCHOR_DIR", ""),
 
 		AuthMode:            get("PIPEROOM_AUTH_MODE", DefaultAuthMode),
 		DevAllowNonLoopback: get("PIPEROOM_DEV_ALLOW_NONLOOPBACK", ""),
````

## 4. Delta 3 — snapshot checkpoints (measure first)

**Problem.** `rebuild` folds every event of a room (`internal/room/service.go:460`). It runs for every room at start
(`Load`) and on every cache miss, and on followers it also runs for rooms another replica opened (`replicas.md` §6).
With the `retention.md` cap of 20 000 events (and up to 490 000 when configured) this cost is bounded but can still be
large.

**Proposal.**
- **Step 1 (A5, small).** Add a benchmark of `rebuild` on rooms of 20 000 and 200 000 events from the fake store, and
  measure start time on a seeded Postgres. Go to step 2 only if one rebuild takes over 50 ms or start takes over 10 s
  at the pilot's room count.
- **Step 2.** Add a table `room_checkpoints(room_id, seq, state jsonb, at)`.
  - Write a checkpoint after every 500 appends and at `room.closed`.
  - `rebuild` loads the latest checkpoint and folds only the events after it.
  - Checkpoints are derived data. Deleting them is always safe, and the purge and delete paths of `retention.md` §6
    delete them with the room.
  - A snapshot-version field makes the code ignore checkpoints written by an older `Apply`.
- The client contract does not change: `after=0` still replays the whole log.

No contract file changes.

## 5. Delta 4 — erase a person's name from the log

**Problem.** A person who leaves, or who asks for erasure (Art. 17), stays named in every event they wrote:
`Actor.Name`, and approval `identity.email` once auth PR 4 lands. The only tool today is to delete whole rooms.

**Proposal.**
- An admin-only `devctl users erase <id> --reason <text>` that calls a new route.
- In one transaction per room, the route sets `actor.name` to `(removed)` and clears `identity.email` in that user's
  events. It also writes a `user_erasures(user_id, erased_by, at, reason, event_count)` row.
- `Actor.ID` stays as a pseudonym. Deleting the `identities` row (`auth.md` Unbind) and removing the person from
  `users.yaml` cut the link to the person.
- Open rooms in the cache are re-folded. Connected WebSockets keep the old name until they reload; that is accepted
  and documented.
- **Interactions.**
  - The audit chain (Delta 2) holds no names, so it stays valid.
  - Exports and archives written earlier keep the name, because they are files the operator controls. Their
    `integrity` hash is unaffected.
  - Slack messages already posted keep the name.
- The route is a `docs/api.md` change. Its exact row waits for the lead's decision on the route shape (Open question
  3), so no diff is given here.

## 6. Tasks per owner

- **A5 (1)** Delta 1: `store.Redacting`, the policy table plus a test that every `events.Type` is classified, the webhook dedup test, wiring in `cmd/server`.
- **A5 (2)** Delta 2: the `audit_log` migration (next free number) and grant, the insert in the append transaction, chain tests (tamper one row → verify fails).
- **A5 (3)** Delta 2: the anchor loop under the leader lock, `PIPEROOM_AUDIT_ANCHOR_DIR` (start fails on an unwritable directory, as for `PIPEROOM_ARCHIVE_DIR`).
- **A5 (4)** Delta 3 step 1 (the benchmark); step 2 only if the threshold is hit.
- **A5 (5)** Delta 4: the erase route, the `user_erasures` table, the re-fold of cached rooms.
- **A1** `devctl audit verify`, `devctl users erase`; the dev stack leaves the anchor directory empty.
- **A6** none for Deltas 1–3. Delta 4: render `(removed)` as plain text (no avatar lookup).
- **A3** none. Tool data is already capped and masked.

## 7. Open questions

1. How long are `audit_log` rows kept after their room is gone: forever like `room_deletions`, or a separate period
   chosen by the pilot customer?
2. Should the redaction policy also mask e-mail addresses in human text? `redact.DefaultOptions` masks e-mails in tool
   text, but people mention colleagues on purpose.
3. Delta 4 route shape: `DELETE /api/users/{id}/names` or `POST /api/admin/erase-user`? Is a per-room scope needed?
4. Should Anthropic-side retention of requests be covered by the pilot contract? It was not verified here.

## 8. References

- GDPR Art. 5 and 17: https://eur-lex.europa.eu/eli/reg/2016/679/oj
- OWASP Logging Cheat Sheet (what not to log; protecting logs): https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
- RFC 8785 (JCS), already used by the export hash: https://www.rfc-editor.org/rfc/rfc8785

The design sandbox could not fetch these pages; the reviewer should confirm them.
