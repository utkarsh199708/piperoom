# Piperoom security findings

Reviewed against `main` @ `c5ee2e9` (2026-10-11). Every finding below was checked against the
actual code at the given file:line, not inferred from the task files. Where the relevant code
has not merged yet (A2 kafka tools/actions, A5 room/httpapi/ws/store), that is stated explicitly
and the item is marked **needs confirmation** instead of asserted as fact.

Severity scale: **critical** (defeats the human-approval safety model or leaks broad customer
data with no auth), **high** (real exposure or bypass with a plausible, low-effort scenario),
**medium** (real but needs a specific condition, or caught by another control), **low** (hygiene,
hardening, or dev-only exposure).

---

## F1 — Any room participant can become any user, including every layer owner, with zero credentials

- **Severity:** critical
- **Where:** `docs/api.md` §Auth (contract); `web/src/components/UserSwitcher.tsx:1-18`;
  `web/src/context/UserContext.tsx:9-49`
- **Scenario:** The entire approval model rests on "only the owner of a layer may approve a
  write for it." Identity is the `X-User` HTTP header (or `?user=` on the WebSocket), checked
  only against a *name*, not a credential, in `dev/users.yaml`. The web app's own
  `UserSwitcher` is a `<select>` listing every known user — one click turns Kabir (consumer
  owner) into Arjun (kafka-connect/kafka/schema-registry owner) or Meera (warehouse owner), no
  password, no session, no server-side check beyond "is this a known id." Anyone who can reach
  the API directly can do the same with `curl -H "X-User: arjun" ...`. This means any room
  member — or anyone on the network path to the API — can approve (or reject) any proposal for
  any layer, request/grant control as any driver, and accept any hand-off. It collapses "an AI
  agent can only act after every required human owner approves" into "an AI agent can act after
  anyone clicks Approve as themselves claiming to be that owner."
- **Fix:** Before any production use: real SSO (OIDC/SAML) issuing a signed session; the server
  derives identity and layer ownership from that session, never from a client-supplied header.
  Until then, treat every approval as unauthenticated and do not run this against real
  production systems. Gate `UserSwitcher` behind a `VITE_FIXTURES`/dev-only flag so it cannot
  ship to a production build by accident.
- **Owner:** A5 (`internal/httpapi` — auth middleware, not yet merged) for the server fix;
  A6 (`web/src/components/UserSwitcher.tsx`) to remove/guard the dev switcher in production
  builds. (Flagged in the contract today at `docs/api.md`, owned by the leads.)

---

## F2 — Layer owners approve a free-text description, never the actual action parameters

- **Severity:** high
- **Where:** `web/src/components/ProposalsPanel.tsx:90-98` (`Proposal` renders `s.action`,
  `s.layer`, `s.description` only); confirmed `params` is present on the wire type
  (`web/src/api/types.ts:128`, `ProposalStep.params: unknown`) but is never read anywhere in
  `ProposalsPanel.tsx`.
- **Scenario:** A proposal step carries a machine-executable `params` object (e.g. which
  connector/task, which schema-registry subject and compatibility level) plus a human-readable
  `description` the model wrote. The approval UI shows only the description. A layer owner
  approving `schema_set_compatibility` sees text like "relax compatibility so the new schema
  registers" and has no way to see — in the UI they actually use to decide — whether `subject`
  is really `pg.public.orders-value` or something else, or whether `level` is really the one
  described. Combined with F1 (anyone can approve as anyone), this is how a plausible-sounding
  but wrong or manipulated proposal (whether from a confused model, a bad prompt injection, or a
  deliberately misleading description) gets rubber-stamped instead of actually reviewed.
- **Fix:** Render each step's `params` (pretty-printed JSON, monospace) next to its description
  before Approve/Reject is clickable. Server-side (A5, once built): bind an approval to an
  immutable snapshot of the proposal's params so they cannot change between creation and the
  last approval without resetting approvals.
- **Owner:** A6 (`web/src/components/ProposalsPanel.tsx`) for the UI; A5 (`internal/room`,
  proposal storage) for the immutability guarantee — needs confirmation once that code exists.

---

## F3 — Full row values, not just the model-visible summary, are pushed into the permanent, broadcast room log

- **Severity:** high
- **Where:** `internal/tools/dbtools/pg.go:171-179` (`pgRowTool`: `Data: row` — the full
  `map[string]any` from `SELECT *`); `internal/tools/dbtools/wh.go:91-99` (`warehouseRowTool`,
  same pattern); consumed by `internal/agent/run.go:430-437` and `:461-470`
  (`AgentToolResultPayload{..., Data: data, ...}`), which per `docs/api.md` is exactly the event
  broadcast live over the room's WebSocket to every connected member and stored forever in the
  append-only `room_events` log (A5, store not yet merged, but the event shape and broadcast
  behavior are fixed by the contract `docs/api.md` and `internal/events/events.go`).
- **Scenario:** `rowSummary`/`whRowSummary` (`internal/tools/dbtools/pg.go:186-202`, `wh.go:
  104-114`) deliberately show only the primary key and timestamp columns in `Summary` — exactly
  the restraint AGENTS.md §7 asks for ("no full record values in logs"). But `Data` carries the
  *entire* row (every column, every value) and nothing re-applies that same redaction before the
  event is emitted. A `pg_row`/`warehouse_row` call on `public.orders` during a live incident
  broadcasts the customer's full row — name, amount, any PII columns — to every one of the
  (potentially dozens of) engineers watching that room, forever, in a log nobody can redact
  after the fact. The model itself never even asked for that data (it only reads `Summary`), so
  this is strictly more exposure than the investigation needed.
- **Fix:** Apply the same column-redaction policy to `Data` that `Summary` already uses (key +
  timestamp values; other column names only, no values) before the tool result reaches
  `AgentToolResultPayload`. If engineers need the full row for debugging, make that an explicit,
  audited, on-demand action (e.g. a separate endpoint a human calls directly) rather than
  something pushed to everyone automatically.
- **Owner:** A3 (`internal/tools/dbtools/pg.go`, `wh.go` — decide what belongs in `Data`) and
  A4 (`internal/agent/run.go` — or A5's room/event layer once merged, whichever ends up owning
  "what goes out over the wire").

---

## F4 — `pg_row` / `warehouse_row` / `trace_record` are not scoped to the incident's own pipeline tables

- **Severity:** high
- **Where:** `internal/tools/dbtools/pg.go:141-182` (`pgRowTool`), `wh.go:61-102`
  (`warehouseRowTool`); underlying clients `internal/clients/postgres/postgres.go:196-230,
  261-299` (`Row`, `validateIdent`/`splitTable`) and `internal/clients/warehouse/warehouse.go:
  79-137` (`Row`, `validateTableColumn`) validate `table`/`pk_column` only against an
  **identifier-syntax regex**, never against `owners.yaml`'s `Pipeline.SourceTables` /
  `SinkTables` (`internal/owners/owners.go`).
- **Scenario:** The DB connection the tools use is one shared, process-wide, read-only pool
  (`internal/config/config.go` `SourceDBURL`/`WarehouseDBURL`) — it can read every table that
  role can see, not just the one pipeline's tables. Any syntactically-valid `schema.table` is
  accepted. A room investigating `orders` can therefore call `pg_row` against an unrelated
  table in the same database (e.g. a billing or employee table) and get a real answer. The
  realistic trigger is the agent being steered there — by a legitimate but overly broad human
  steer ("also check the billing table while you're at it"), or by a prompt-injected record
  value/comment that suggests doing so, since the tool itself enforces no pipeline boundary.
  Once called, F3 means the result is broadcast to the whole room regardless of whether anyone
  there has a need to know.
- **Fix:** In `dbtools`, resolve the pipeline's `owners.Pipeline` for the room's `pipeline_id`
  and reject `table` values that are not in `SourceTables ∪ SinkTables` (plus explicitly
  allow-listed lookup tables if needed), rather than only checking identifier syntax.
- **Owner:** A3 (`internal/tools/dbtools`, `internal/owners`).

---

## F5 — Integration tests print the full database DSN, including the embedded password, on connection failure

- **Severity:** medium
- **Where:** `internal/clients/warehouse/integration_test.go:22`
  (`t.Fatalf("warehouse unreachable (%s): %v", cfg.WarehouseDBURL, err)`); identical pattern at
  `internal/clients/postgres/integration_test.go:23`
  (`t.Fatalf("source postgres unreachable (%s): %v", cfg.SourceDBURL, err)`).
- **Scenario:** `cfg.SourceDBURL`/`WarehouseDBURL` are full connection strings with the password
  embedded (`postgres://piperoom:piperoom-dev@...` by default, per
  `internal/config/config.go:28-30`; a real deployment would set these to real credentials via
  `SOURCE_DB_URL`/`WAREHOUSE_DB_URL`). If these tests ever fail — a flaky stack, a misconfigured
  CI runner, someone pointing `-tags integration` at a staging DB with real credentials to debug
  something — the full DSN, password included, lands in test output, which typically ends up in
  CI logs (retained, often searchable/exported) or pasted into Slack per `docs/collab.md`'s own
  "paste short output" convention. AGENTS.md §3/§7 explicitly rules this out ("Never put real
  secrets in ... logs", "No secrets ... in logs").
- **Fix:** Log only the host:port (or nothing) on failure, e.g. parse with
  `url.Parse`/`pgxpool.ParseConfig` and print `cfg.Host`+`cfg.Port`, never the raw URL string.
- **Owner:** A3 (`internal/clients/postgres`, `internal/clients/warehouse`).

---

## F6 — No table/row/time cap on an investigation's permanent event log; a long-lived room grows without bound

- **Severity:** medium
- **Where:** design-level; `internal/agent/run.go` loop (`r.run`, no turn/step/event ceiling
  beyond the per-episode `MaxTurns` which **resets** on any human input, `internal/agent/
  options.go` default 40); event storage/limits are owned by A5 (`internal/store`,
  `migrations/**`), **not merged**, so there is currently no code-level cap to point to —
  **needs confirmation** once A5 lands.
- **Scenario:** `docs/api.md` describes rooms staying open "at shift change," potentially for
  days on a sev1. `MaxTurns` is a per-episode runaway guard but is explicitly reset by any human
  steer/comment, so a room that stays actively watched can run the agent (and accumulate tool
  call/result events, each with a full `Data` payload per F3) indefinitely. Nothing in the
  contracts (`internal/events/events.go`, `docs/api.md`) specifies a retention policy, a
  per-room event count/size cap, or archival for closed rooms. This is primarily a storage-
  growth and WebSocket-replay-cost concern (`after=0` replays the *entire* log on every first
  open, per `docs/api.md` §WebSocket), not an acute vulnerability, but it is an unbounded
  resource the task explicitly asks to check ("event log growth").
- **Fix:** When A5's store lands: cap events per room (and/or `Data` bytes per event, beyond the
  existing `tools.MaxSummaryBytes` which only bounds `Summary`), and define a retention/
  archival story for closed rooms.
- **Owner:** A5 (`internal/store`, `internal/room`) — needs confirmation once merged.

---

## F7 — No tenant/pipeline isolation in client wiring; one process's credentials can see everything

- **Severity:** medium
- **Where:** `internal/config/config.go:9-39` — one `SourceDBURL`, `WarehouseDBURL`,
  `KafkaBrokers`, `ConnectURL`, `SchemaRegistryURL` for the whole process, shared across every
  pipeline/room `owners.yaml` defines.
- **Scenario:** Today (single customer, one `orders` pipeline) this is fine. The task asks the
  review to consider "other tenants" as an actor; if Piperoom is ever deployed to serve more
  than one customer or more than one independently-owned pipeline from a single server process,
  there is currently no mechanism to scope a room's tools to only that pipeline's systems/
  credentials — every room shares the same global client set (confirmed: `cmd/investigate`
  wires one `connect`/`schemareg`/`kafka` client set process-wide in
  `cmd/investigate/wire_realtools.go`, and `dbtools`/`topology` do the same for Postgres/
  warehouse). Combined with F4, a multi-tenant deployment would let one tenant's incident room
  read another tenant's data.
- **Fix:** Either deploy one process per tenant (simplest), or add per-pipeline credential/
  client scoping before any multi-tenant deployment. Document the chosen model.
- **Owner:** architecture-level — flag for the leads / a future task file; not a single agent's
  folder today.

---

## F8 — Terminal sink prints raw tool/record JSON to stdout with no size bound beyond `tools.MaxSummaryBytes`

- **Severity:** low
- **Where:** `cmd/investigate/sink.go:42-51` (`terminalSink.Emit` — `json.Marshal(payload)`
  then `fmt.Fprintf` to stdout, unconditionally).
- **Scenario:** `cmd/investigate` is a CLI demo/dev tool, not the production surface, so impact
  is low. `Summary` is capped by `tools.MaxSummaryBytes` (8000 bytes) but `Data` is not (see
  F3) — a large tool result (e.g. `pg_recent_ddl` with many rows, or a wide `pg_row`) is printed
  to the terminal in full. Checked for a terminal-escape-sequence injection concern specifically:
  `encoding/json` escapes all control bytes below `0x20` (including ESC, `0x1b`) as `\u00XX`
  when marshalling, so this does **not** allow raw ANSI/terminal escape injection via record
  values — confirmed by reading the standard library's `json.Marshal` escaping behavior, not
  assumed. The remaining issue is purely an unbounded-output/readability one.
- **Fix:** Low priority given this is a dev CLI; if kept, truncate the printed JSON similarly to
  `tools.Truncate`.
- **Owner:** A4 (`cmd/investigate/sink.go`).

---

## F9 — The alert webhook's authentication model is undefined

- **Severity:** needs confirmation (potential high if unauthenticated)
- **Where:** `docs/api.md:92` — `POST /api/alerts/webhook | any JSON | RoomSnapshot | generic
  alert; ... driver = none until someone takes control`. The same document's Auth section says
  "Every request sends `X-User: <user id>`" but does not say whether the webhook — which by
  nature is called by an external alerting system (Prometheus Alertmanager, PagerDuty, etc.),
  not a logged-in engineer — is exempt from that, or what it sends instead. `internal/httpapi`
  does not exist on `main` yet, so this cannot be checked in code.
- **Scenario (if unauthenticated or using a guessable/shared value):** anyone who can reach the
  endpoint can create unlimited rooms with attacker-chosen `title`/`severity`/`pipeline_id`,
  starting a real agent investigation each time (cost/DoS), or socially engineer the on-call
  team with a fake sev1 alert about a real pipeline. If the raw webhook body were ever fed into
  the agent's prompt this would also be a prompt-injection vector — checked and ruled out for
  the current contract: `agent.Alert.Raw` (`internal/agent/contract.go:49-56`) is **not**
  referenced by `firstUserMessage` (`internal/agent/prompt.go:55-78`), so today's design does not
  put raw webhook JSON in front of the model even if the endpoint itself is open.
- **Fix:** Decide and document (in `docs/api.md`) whether the webhook needs its own shared
  secret / signature (e.g. HMAC header, as Alertmanager/PagerDuty support), separate from
  `X-User`, and implement it when `internal/httpapi` is built.
- **Owner:** A5 (`internal/httpapi`) to implement; leads to decide and record the contract in
  `docs/api.md`.

---

## Summary of critical/high findings

| ID | Severity | One-line |
|---|---|---|
| F1 | critical | Any `X-User` value impersonates any user, including every layer owner — no password, UI even offers a dropdown to do it |
| F2 | high | Approval UI shows the model's free-text description, never the real action `params` being approved |
| F3 | high | Full row values (not the redacted summary) are broadcast to every room watcher and stored forever |
| F4 | high | `pg_row`/`warehouse_row`/`trace_record` can read any syntactically-valid table, not just the pipeline's own tables |
