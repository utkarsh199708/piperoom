# Piperoom threat model

C4 security review, against `main` @ `c5ee2e9` (2026-10-11). Scope: the code that exists on
`main` today. A2 (Kafka/Connect/Schema Registry clients, tools, actions) and A5 (room service,
HTTP API, WebSocket, executor) have **not merged yet** — sections below say so explicitly and
are marked "needs confirmation" rather than stated as fact. Re-review once they land.

## 1. What Piperoom is, for threat-modeling purposes

An AI agent (Claude) gets **read-only** access to a customer's production data infrastructure
(source Postgres, Kafka, Kafka Connect, Schema Registry, warehouse) and can **propose** write
actions. A proposal only runs after every affected layer's human owner approves it. Many
engineers watch and steer the same room live. This is a powerful read scope (effectively a
production DB/Kafka credential) wrapped in a human-approval gate for writes — so the two things
most worth attacking are (a) the read scope (can it be made to leak more than intended) and
(b) the approval gate (can it be bypassed, confused, or rubber-stamped).

## 2. Assets

| Asset | Where it lives | Why it matters |
|---|---|---|
| Customer record data (PII, payment amounts, etc.) in `public.orders` and other tables | Source Postgres, warehouse | `pg_row`/`warehouse_row`/`trace_record` read it; it can end up in the room's **permanent, broadcast** event log (finding F3/F4) |
| Approval authority — the fact that only a named layer owner may approve a write | `dev/users.yaml`, `X-User` header, `proposals/approvals` tables (A5, not merged) | This is the only thing standing between "AI suggests" and "AI changes production" |
| Database/Kafka/API credentials (DSNs, `ANTHROPIC_API_KEY`) | env vars, `internal/config/config.go` | A leak gives an attacker the same access as the whole platform |
| The room event log itself | `room_events` (A5, not merged); today, agent events printed by `cmd/investigate` | Append-only audit trail; also the channel through which every finding above can leak to all watchers |
| Model behavior / the investigation itself | Claude's context window | If steerable by someone other than the driver, it can be made to propose harmful actions or leak data under the guise of "investigating" |

## 3. Actors

- **Room members** (the whole engineering team per `dev/users.yaml`) — mostly trusted, but not
  uniformly: a consumer-layer owner has no inherent need to see warehouse PII, yet today nothing
  scopes what they see to what they own (see F4).
- **Someone who can set an HTTP header** — under the current dev auth (`X-User`, no password,
  §4 below), this actor can become *any* room member, including every layer owner, with zero
  credentials. This is the most important actor in this model.
- **Other tenants** — not applicable to the single-tenant dev stack, but `internal/config`
  wires one set of DB/Kafka/Connect/Registry credentials for the whole process with no
  per-pipeline or per-tenant isolation. A SaaS deployment serving multiple customers from one
  process would need this solved before launch (F7).
- **A malicious or merely messy record value, log line, error trace, or comment** — the agent
  reads these as tool output / human input. This is the classic prompt-injection surface for an
  agent with real system access.
- **A compromised or misconfigured alert source / webhook** — `POST /api/alerts/webhook` takes
  "any JSON" per `docs/api.md`; its authentication story is not written down (F9, needs
  confirmation once A5 lands).
- **The model itself** — can be jailbroken, confused, or simply wrong. The design correctly
  treats this as untrusted-but-contained: the model can only *propose*, never *execute* (see §5).

## 4. Trust boundaries

```
 [record value / error trace / comment] --(untrusted)--> [tool result / human input] --(wrapped)--> [model]
 [webhook / alert source] --(auth unclear, not yet built)--> [room created]
 [browser, X-User header] --(no password)--> [HTTP API / WebSocket] --(assumed identity)--> [room, approvals]
 [model's propose_fix tool_use] --(validated against actions.Registry)--> [proposal, pending]
 [every required layer owner's approval] --(not yet built: A5 executor)--> [actions.Action.Execute on real systems]
 [tool Result.Data] --(not re-redacted)--> [room_events log] --(broadcast)--> [every connected browser]
```

Boundary-by-boundary:

1. **Tool output → model.** Well defended. Every tool result is wrapped
   `<tool_output tool="..." untrusted="true">...</tool_output>` with `&`, `<`, `>` escaped so a
   record value cannot break out of the wrapper (`internal/agent/run.go` `wrapToolOutput`,
   `internal/agent/prompt.go` `escapeToolText`). Non-driver human input (comments, approval/
   action results) is similarly wrapped in `<untrusted>` (`formatHumanInput`). The system prompt
   explicitly tells the model this text is data, never instructions, and that it should ignore
   embedded directions. This is good, verified, defense-in-depth — but it is a probabilistic
   control on the model, not a hard boundary, so it must never be the *only* thing standing
   between an attacker and a write (see F2).

2. **Model → write.** Hard boundary, well designed in the code that exists: the model can only
   call `propose_fix`, which validates every step against `actions.Registry` and *never executes*
   (`internal/agent/tools_room.go`). Execution is explicitly someone else's job
   (`internal/actions`, the not-yet-merged A5 executor). Good — but the gate standing behind it
   (human approval) is only as strong as identity (§4.3) and what the approver actually sees
   (F2), both of which are weak today.

3. **Browser/API → identity.** Weak by design for the MVP (`docs/api.md` §Auth: any `X-User`
   value matching a line in `dev/users.yaml` is accepted, no password). The web app even ships a
   dropdown that lets anyone become any user, including every layer owner
   (`web/src/components/UserSwitcher.tsx`). This collapses the approval boundary (F1).

4. **Webhook/alert → room.** Not yet built (A5). `docs/api.md` doesn't say whether this endpoint
   is exempt from `X-User` or needs a different secret. Needs a decision before it ships (F9).

5. **Tool Data → room log → every browser.** Not defended today. `Result.Summary` is
   deliberately redacted (dbtools shows only key/timestamp columns, never full values) but
   `Result.Data` is not, and `Data` is exactly what gets stored in the permanent event log and
   broadcast live to everyone watching the room (F3/F4).

## 5. Data flows worth tracking

- **Investigation read path:** alert → `agent.Run` → read tool (dbtools/kafkatools) → target
  system → `tools.Result{Summary, Data}` → `AgentToolResultPayload` event → room log → every
  connected browser. `Summary` is what the model sees and is redaction-aware; `Data` is not and
  is strictly more exposed (goes to every browser) than the model's own view (F4).
- **Propose path:** model `propose_fix` tool_use → `actions.Registry.Get/Validate` (read-only
  checks) → `Proposals.Create` → `ProposalCreatedPayload` event (full `Params` included) → UI
  renders the proposal but **not the params**, only a free-text `description` the model wrote
  (F2) → layer owner clicks Approve based on that description.
- **Approve/execute path (A5/A2, not merged):** per the task files, approval requires the
  caller to "own" the layer (compared against `dev/users.yaml`, itself keyed only by the
  unauthenticated `X-User`), and execution is supposed to be idempotent per
  `proposal id + step index`. None of this exists in code yet — re-review when it lands,
  specifically for: double-approval races, replay of an old approval after rejection, and
  whether a step can run twice.
- **Hand-off path:** room events → `renderRoomEvents` (field-selected, untrusted-wrapped,
  excludes `Data`) → one Claude call with structured output → `HandoffSummary` → new event.
  This path is well built: it is the one place that deliberately avoids re-exposing raw `Data`,
  which makes the contrast with the live broadcast path (F4) more notable, not less.

## 6. What's already well-mitigated (so reviewers don't re-flag it)

- SQL injection: every identifier (schema/table/column) is checked against
  `^[a-zA-Z_][a-zA-Z0-9_]*$` and then passed through `pgx.Identifier{...}.Sanitize()`; every
  value is a bind parameter (`internal/clients/postgres`, `internal/clients/warehouse`).
  Verified by reading `rowQuery`, `validateTableColumn`.
- Read-only guarantee at the connection level: both Postgres clients issue
  `SET default_transaction_read_only = on` in `AfterConnect`, not just "the code never calls
  write methods" — a defense-in-depth layer against a bug in a future tool.
- Prompt injection wrapping (§4.1) is thorough and was verified against the actual escaping
  code, not just the comments describing it.
- `additionalProperties: false` + `strict: true` on every tool schema
  (`internal/agent/toolparams.go`) stops the model from smuggling extra fields into tool calls.
- History is genuinely append-only in the loop (`messages = append(messages, resp.ToParam())`,
  never mutated) — matches the Anthropic API's append-only requirement and the audit-trail goal.

## 7. Open items to re-review once A2 / A5 land

- SSRF in the Connect / Schema Registry / Kafka HTTP clients (base URLs come from config, but
  confirm no tool takes a user/model-supplied URL or host).
- WebSocket origin checks and CORS configuration (`internal/ws`, `internal/httpapi` — task says
  CORS should allow only `http://localhost:5173`; verify in code once merged).
- Approval bypass / idempotency / replay in the executor (`internal/room`), and whether layer
  ownership checks are enforced server-side against the same unauthenticated `X-User`.
- Whether `connect_config` actually redacts `password`/`secret`/`token`/`key`-named config
  values as its task file requires (`internal/tools/kafkatools`, not merged).
- The webhook endpoint's authentication model (F9).
