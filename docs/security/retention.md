# Design: how long room logs are kept

Task C9, part 3 of 3 (see also `auth.md`, `webhook.md`). Design only — no code changed.
Closes the design gap behind **F6** (unbounded log growth, no retention policy) and the "forever" side of
**F3** (full row values in a permanent, broadcast log). Written against `main` @ `270377c`; the store
(`internal/store`, `migrations/`) is A5's and unmerged.

All durations below are **proposed defaults**; section 9 lists what the leads and the pilot customer must
confirm. Nothing here is legal advice — the customer's own retention and privacy obligations win.

## 1. The tension to resolve

The contract says `room_events` is **append-only** and `Seq` has **no gaps** within a room
(`internal/events/events.go`). Customers also need old incident data to go away: tool results can contain
customer record values, and comments can contain anything an engineer typed. Plain row deletion would break
the no-gaps rule and the audit trail. So:

> **We never delete event rows. We redact their payloads.** A redacted event keeps `room_id, seq, type,
> actor, at` and replaces `payload` with a tombstone. Whole rooms are deleted only by the final purge (§5),
> and only for events that are no longer needed for audit.

That keeps replay (`after=0`), seq continuity, and "who did what, when" intact, while removing content.

## 2. What data exists, and how sensitive it is

| Data | Where | Sensitivity | Class |
|---|---|---|---|
| `agent.tool_result.data` (full rows, record keys, config) | `room_events.payload` | **High** — customer data (F3) | **C: content** |
| `room.created.alert` (raw webhook JSON) | `room_events.payload` | Medium — arbitrary sender text | C |
| `comment.added.text`, `steer.*.text` | `room_events.payload` | Medium — free text, may paste secrets/PII | C |
| `agent.message`, `agent.hypothesis`, `agent.tool_call.input`, tool_result `summary` | `room_events.payload` | Low–medium — derived; summaries show keys, not values | C |
| `proposal.created` (params), `approval.*`, `action.executed`, `control.*`, `handoff.*`, `room.*` | `room_events.payload` | Low content, **high audit value** — who approved what change to production | **A: audit** |
| Hand-off summaries | event + any derived table | Medium — written from events without `Data` | A (but see §4) |
| Sessions, integrations, replay cache | auth tables (`auth.md`, `webhook.md`) | Credential-adjacent | own TTLs (§6) |
| Application logs | stdout / log pipeline | Must hold no payloads (AGENTS.md §7) | own TTL (§6) |
| Presence | memory only (contract: "not stored in the event log") | Low | not kept |
| Model-provider side | requests sent to the Anthropic API | Medium | contractual (§7) |

Minimising at the source comes first and is not optional: apply the F3 fix (store only key + timestamp
column values in `Data`) and the F6 per-event size cap, so less content exists to be retained at all.

## 3. Policy

Defaults, per tenant, adjustable only within the bounds shown.

| Stage | Clock starts | Default | Bounds | What happens |
|---|---|---|---|---|
| **Live** | room opened | until closed | — | full log; no redaction while a room is open. If a room stays open > 30 days, an admin is notified; it is not auto-closed. |
| **Content redaction** | `room.closed` | **30 days** | 7–180 | class **C** event payloads replaced by tombstone. |
| **Audit retention** | `room.closed` | **13 months** | 12–84 | class **A** events kept in full (they hold no customer record data by design, §4). |
| **Purge** | end of audit retention | — | — | all remaining events and the room row deleted in one transaction; a one-row receipt kept (§5). |
| **Legal hold** | set by admin | until released | — | pauses redaction and purge for that room; every set/release is audited. |

Why 30 days for content: post-incident review normally happens within days to a couple of weeks; beyond
that, the raw rows are risk without value. Why 13 months for audit: covers a yearly review cycle plus a
month of slack; the customer may need more for SOX/ISO-style evidence.

Tombstone payload (fixed shape so the UI and reducers can rely on it):
```json
{ "redacted": true, "reason": "retention", "redacted_at": "2026-11-10T02:00:00Z", "policy": "content-30d" }
```

## 4. Edge cases that need explicit handling

1. **Proposal params are class A but may hold values.** Params name connectors, subjects, levels — not
   record data. Action registry validators (A2) must keep them that way; add a test that no action's
   params schema accepts a free-form blob. If one ever does, that field goes to class C.
2. **Hand-off summary is derived from content.** It is generated without `Data` (threat model §5), but
   free text can still echo a value. Policy: summaries are class **C** (redacted at 30 days), except the
   structured fields `resolution`, `open_items`, `owner` which are class A. A5 needs the summary payload
   split to match — **contract change request** to the lead if `HandoffSummary` does not already separate them.
3. **Event replay in the UI.** A6's reducer must render tombstoned events as "content removed by
   retention policy", not as an error. Closed rooms older than 30 days will legitimately show this.
4. **Agent resuming a closed room.** Not supported. A redacted room cannot be reopened; a new room links to it.
5. **Counts and derived state.** `RoomSnapshot` is folded from events; fields that come only from redacted
   content (hypothesis text) read empty after redaction. That is intended.
6. **The log must stay verifiable.** Redaction updates only `payload`; the append-only guarantee is
   enforced by DB permissions (§8), not by a promise in code.

## 5. Mechanism (proposal for A5)

- **Retention worker:** a goroutine in `cmd/server` (ctx-bound, no leaks) wakes hourly; for each tenant
  selects closed rooms past their stage with `FOR UPDATE SKIP LOCKED`, processes in batches of ≤ 500 events,
  commits per batch. Idempotent: running twice changes nothing. Config stays out of `internal/config`'s
  contract file until the leads approve the fields (`PIPEROOM_RETENTION_*`), so the first cut can read a
  `retention.yaml` next to `owners.yaml`.
- **Redact:** `UPDATE room_events SET payload = $tombstone WHERE room_id = $1 AND type = ANY($2) AND NOT (payload ? 'redacted')`.
  The `type` list is code-defined (class C types), not config, so a typo cannot redact audit events.
- **Purge:** `DELETE` of the room's events and row, then insert into `retention_receipts(room_id_hash,
  tenant_id, closed_at, purged_at, event_count)`. The receipt proves a purge happened without keeping content.
  Hash the room id so the receipt does not identify the incident.
- **Not in the event log:** the worker records its own actions in `retention_log`, **not** by appending to
  the room (appending to a closed room would conflict with `room_closed`, and a purged room has no log).
- **On-demand erasure** (customer request or data-subject request): same redaction function, scoped to a
  room or to events mentioning a user key; run by an admin command, audited the same way. Purging rooms
  earlier than the audit period requires a recorded reason.
- **Legal hold:** `rooms.legal_hold` boolean; both worker queries add `AND NOT legal_hold`.

## 6. Everything else

| Data | Default | Note |
|---|---|---|
| Application logs | **30 days** | holds ids, outcomes, sizes only; no payloads, secrets, row values |
| Sessions | until expiry (12 h) + 7 d for audit of login events | `auth.md` §5; store hashes only |
| Webhook replay cache | 10 min TTL | `webhook.md` §5 |
| Webhook integrations | until deleted; secrets destroyed on deletion | |
| **Database backups** | **35 days** | redacted/purged data remains in backups until they expire. State this plainly to customers; erasure SLA = redaction time + 35 days. |
| Event export (if added) | none stored | exports are generated on demand, not kept |
| Dev stack data | n/a | local only; not covered by this policy |

## 7. Model provider

Tool output and human text sent to the Anthropic API are subject to the provider's retention terms for the
customer's agreement, which are not in this repo. Before the pilot: confirm those terms from the current
Anthropic commercial agreement / privacy documentation (do not rely on this file), decide whether the
pilot needs a zero-data-retention arrangement, and copy the confirmed terms into this section. Piperoom
should send the model summaries rather than raw rows wherever it can (it already does for row tools).

## 8. Guardrails

- DB roles: the application role has `INSERT, SELECT` on `room_events` and `UPDATE (payload)` only for the
  retention worker's role; no role used by request handlers can `UPDATE` or `DELETE` events. This enforces
  append-only below the application.
- Tombstones are one-way: no endpoint un-redacts.
- Size and growth caps (F6) are separate from retention: per-event payload cap (proposed 64 KB, tool `Data`
  16 KB), per-room event cap (proposed 20,000) after which the agent pauses for a human, and an
  `after=0` replay that streams in pages (`limit` ≤ 1000, already in the API).
- Retention settings are admin-only and every change is audited; shortening applies on the next run,
  lengthening never resurrects redacted content.

## 9. Tests (A5 unless noted)

| Case | Expect |
|---|---|
| room closed 31 days ago | class C payloads tombstoned; class A intact; `seq` contiguous; `GET .../events?after=0` returns all seqs |
| room still open at 100 days | nothing redacted |
| run worker twice | second run changes 0 rows |
| `legal_hold = true` | no redaction, no purge |
| room closed 14 months ago | events and room row gone; a receipt exists; no content in receipt |
| redaction list | unit test enumerates every `events.Type` and asserts it is classified A or C (new event types fail the test until classified) |
| handler DB role | `UPDATE room_events` / `DELETE` as request role → permission denied |
| web reducer (A6, Vitest) | tombstoned `agent.tool_result` and `comment.added` render the removal notice, no crash |
| logs | capture shows no payload text from any redaction run |

## 10. Decisions needed

1. Defaults: content 30 d, audit 13 months, backups 35 d, logs 30 d — confirm or replace with the pilot customer's numbers.
2. Is the hand-off summary class C (proposed) or A?
3. Who may set legal hold and approve early purge: tenant admin only, or also us?
4. Anthropic-side retention terms for the pilot (§7).
5. Who owns the work: A5 (worker, migrations, DB roles), A6 (tombstone rendering), leads (`HandoffSummary`
   split, config fields, `docs/api.md` note that closed rooms may show tombstones).
