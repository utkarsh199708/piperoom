# Cost per investigation and durable spend limits (E4)

Status: contract (PR `[LEAD] contract: agent.usage event, durable budgets`). Implementation: branch `e4/cost`.

## 1. Goals

1. Know what each investigation costs: one `agent.usage` event per model request, with tokens, latency and an
   estimated cost.
2. Spend limits that survive a restart: the per-room token budget (today in memory, reset on restart) and new
   per-pipeline daily limits, all kept in the room database.
3. Operators can read the numbers: two GET routes.
4. The eval report shows latency, tokens, cost and model routing, and two reports can be compared run by run.

Non-goals: billing reconciliation (the cost is an estimate from a price table the operator maintains), web UI (the
UI v2 track is active; see section 8), per-user limits.

## 2. `agent.usage` (contract: `internal/events/events.go`)

```go
AgentUsage Type = "agent.usage"

type AgentUsagePayload struct {
	Model                    string   `json:"model"`
	Role                     string   `json:"role"` // triage | main | handoff | compaction
	InputTokens              int64    `json:"input_tokens"`
	CacheCreationInputTokens int64    `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int64    `json:"cache_read_input_tokens"`
	OutputTokens             int64    `json:"output_tokens"`
	LatencyMS                int64    `json:"latency_ms"`
	CostUSD                  *float64 `json:"cost_usd,omitempty"`
}
```

- **When.** After every model request the agent sends: each investigation turn (`triage` or `main`, by the
  model the triage rules picked for that request; a triage turn that asks for `propose_fix` and is re-run on the
  main model gives one `triage` and one `main` event), and the hand-off summary (`handoff`). Failed requests count
  too (a failed stream may still have used tokens). A request refused by a budget is never sent and gets no event.
- **Compaction.** The API reports a server-side compaction pass in `usage.iterations` (type `compaction`), not in the
  request's own usage fields. Such a request gets a second event, `role: compaction`, with the summed compaction
  iterations and the request's model; its `latency_ms` is 0 because its time is part of the request's.
- **Judge.** The eval judge is rule based (`evals/score.go`) and makes no model call; there is no judge role.
- **No text.** Counts and the model id only. Never prompt text, tool input or output, model text or error text.
- **Model** is the model id the request named (`PIPEROOM_MODEL`, `PIPEROOM_TRIAGE_MODEL`, a Bedrock id), the same key
  the price table uses.
- **Where it goes.** Loop events go through the run's `Sink` (room log; the shadow log in a shadow room). The
  hand-off has no sink in its contract (`HandoffWriter`), so the `Endpoint` hands its usage to a hook that
  `cmd/server` points at the room's agent sink. The plain `agent.WriteHandoff` (no endpoint) emits nothing.
- **Readers.** The room snapshot, the Slack mirror and the hand-off renderer ignore the type; the web UI must too
  (section 8).

### 2.1 Cost

`PIPEROOM_MODEL_PRICES` is the path of a JSON price table, read once at start-up:

```json
{
  "claude-opus-5-5":  {"input": 0, "output": 0, "cache_write": 0, "cache_read": 0},
  "claude-haiku-5-5": {"input": 0, "output": 0, "cache_write": 0, "cache_read": 0}
}
```

Each key is a model id exactly as sent; each value has exactly the four fields, in USD per million tokens (fill in
the prices of your own contract; the zeros above are placeholders). `cache_write` is the 5-minute cache write price
(the agent's cache breakpoints use the default TTL). Unknown or missing fields, negative or non-finite numbers, an
empty table and a file over 1 MB are start-up errors.

`cost_usd = (input × input + cache_creation × cache_write + cache_read × cache_read + output × output) / 10⁶`,
rounded to whole millionths of a USD (the unit the spend tables keep). With no table, or no entry for the model,
`cost_usd` is omitted: never guessed, never 0.

## 3. Durable spend limits

### 3.1 Settings (contract: `internal/config/config.go`, `ParseCost`)

| Variable | Meaning |
|---|---|
| `PIPEROOM_ROOM_TOKEN_BUDGET` | unchanged name and semantics (empty = 2 000 000, `0` = no cap, model tokens per room), now kept in the room database |
| `PIPEROOM_PIPELINE_DAILY_TOKEN_BUDGET` | model tokens per pipeline per UTC day; empty or `0` = off |
| `PIPEROOM_PIPELINE_DAILY_COST_USD` | estimated USD per pipeline per UTC day, plain decimal (`25`, `12.50`); empty or `0` = off; needs `PIPEROOM_MODEL_PRICES` |
| `PIPEROOM_MODEL_PRICES` | price table path (2.1); empty = no costs |

A bad value refuses start, naming the variable. With a cost cap set, `PIPEROOM_MODEL` and (when set)
`PIPEROOM_TRIAGE_MODEL` must be in the price table, or start is refused: an unpriced model would bypass the cap.
`Options.TokenBudget` (the shadow cap) still replaces the room cap for its run and is checked against the same
persisted room total.

Tokens are counted as today: input + cache creation + cache reads + output, compaction passes included, each at
face value. The pipeline is the alert's `pipeline_id`; the day is the UTC date when the request is admitted.

### 3.2 Tables (migration `00013_spend.sql`)

| Table | Key | Holds |
|---|---|---|
| `room_spend` | `room_id` (FK `rooms`, cascade) | `tokens`, `cost_micros` charged: what the room cap checks |
| `room_spend_models` | `room_id, model` (FK `rooms`, cascade) | finished calls by model: `calls`, `unpriced_calls`, the four token counts, `cost_micros` |
| `pipeline_spend_daily` | `pipeline_id, day` | `tokens`, `cost_micros` charged: what the daily caps check |
| `pipeline_spend_daily_models` | `pipeline_id, day, model` | finished calls by model, as above |

Cost is in millionths of a USD (`bigint`), so sums are exact. Retention deletes a purged room's two room tables with
the room (foreign-key cascade); the pipeline tables are kept.

### 3.3 Admit and settle

With a spend store (the server), each investigation request:

1. **Estimate.** `est` = input tokens of the room's previous loop request (0 for the first; the history is
   append-only, so the next request reads at least as much). The cost estimate is `est` × the model's
   `cache_read` price, a lower bound (0 when the model has no price).
2. **Charge (admit).** One transaction adds `est` and its cost to the room row, then to the pipeline-day row, each
   with `INSERT … ON CONFLICT DO UPDATE SET tokens = tokens + $n … RETURNING`, and returns both totals after it.
3. **Check.** Refused when the room total reaches the room cap, the pipeline-day tokens reach the daily token cap,
   or the pipeline-day cost reaches the daily cost cap (`total ≥ cap`, the same "used + estimate ≥ limit" rule as
   today), checked in that order. A refusal takes the estimate back (`-est`) and stops the run with a
   `*BudgetError`.
4. **Send**, then **settle**: one transaction adds `actual − est` (tokens and cost, which may be negative) to the
   same two rows and the finished call to both `*_models` tables, for the day the request was charged.

The hand-off is never refused: it skips steps 1 to 3 and settles its full usage. Without a spend store (the
`cmd/investigate` CLI, tests) the endpoint keeps today's in-memory count and has no daily caps.

**Concurrency (soft cap).** Charges serialize on the row locks, and a request's estimate is charged before it is
sent, so two requests running at the same time are never both admitted on the same headroom: the admitted
estimates never add up past a cap. The overshoot is what admitted requests use beyond their estimates (their output
and new input). One room's loop sends one request at a time, so a room overshoots by at most one request (plus a
hand-off, which is never refused). A first request has no estimate, so first requests of different rooms that
arrive together at a pipeline's daily cap can each overshoot by one request. A crash between charge and settle
leaves the estimate charged: conservative, and the request was probably billed anyway.

**Store errors.**

| When | A cap applies to the request (room cap > 0, or a daily cap set) | No cap applies |
|---|---|---|
| charge fails | the run stops (a `*BudgetError`, status idle, text below); the server logs the error | the request is sent uncounted; Warn log |
| refund fails | Warn log; the estimate stays charged (conservative) | same |
| settle fails | Warn log with counts only; the estimate stays charged and the call is missing from the breakdown; the next charge fails closed if the store is still down | Warn log |

**Budget texts.** The room cap keeps today's `BudgetError` behaviour and text (`token budget reached (N of M); a
person must raise PIPEROOM_ROOM_TOKEN_BUDGET or continue manually`, or the run-cap text). New texts:

- `pipeline daily token budget reached for "orders" on 2026-10-13 UTC (N of M); a person must raise PIPEROOM_PIPELINE_DAILY_TOKEN_BUDGET or continue manually`
- `pipeline daily cost budget reached for "orders" on 2026-10-13 UTC ($X of $Y); a person must raise PIPEROOM_PIPELINE_DAILY_COST_USD or continue manually`
- `spend store unavailable, so the spend limits cannot be checked; the agent stopped. A person can continue manually or steer the agent to try again`

All are `*BudgetError`: the room service treats them as a stop (no retry, no error status); a shadow run ends
`budget`. `BudgetError.Used`/`Limit` are tokens, or millionths of a USD for the cost cap.

`Deps.Usage` keeps its contract: the tokens this process counted for the room during the run.

## 4. API (contract: `docs/api.md`)

- `GET /api/rooms/{id}/usage`: the room's `charged` total, its cap, the totals and a per-model breakdown of finished
  calls. Access: whoever may read the room (its `agent.usage` events carry the same numbers), viewers included. A
  shadow room answers `409 room_open` while open, like the shadow routes, so its spend is not visible before the
  reveal.
- `GET /api/usage?pipeline=&day=`: the same for one pipeline and UTC day, shadow runs included. Access: the export
  rule applied to the pipeline (approver or backup of any of its layers, dev-mode `users.yaml` layers, the solo user)
  or an `auditors:` principal (the read-only admin role). Others `403 not_allowed`.

Both answer zeros for a scope with no spend yet. Field shapes are in `docs/api.md` (`Usage`).

## 5. Evals (implementation: `evals/stats.go`, `evals/compare.go`, `evals/report.go`)

- **Per run and per scenario:** p50, p90 and p99 of the run duration and of the per-turn latency (the
  `latency_ms` of `triage` and `main` events); tokens per turn (mean, p90; a compaction pass counts toward the turn
  it followed); cost per run (mean, p90) when a price table is given (`-prices`, default `PIPEROOM_MODEL_PRICES`;
  costs are computed from the token counts, so runs on the plain runner are priced too); model routing share
  (triage turns over loop turns). Percentiles use linear interpolation between closest ranks; with no data they
  are -1. The overall row pools every run.
- **`evalrun -compare a.json b.json`** (the `a.json,b.json` form stays): besides the existing per-scenario table, a
  paired comparison on matching (scenario, run index): the pass-rate difference (b − a) with a 95% bootstrap CI
  (10 000 resamples of the pairs, fixed seed, `math/rand/v2` only), the McNemar discordant counts (pass→fail,
  fail→pass), and `sample too small to call` when fewer than 20 pairs match.
- Scoring and calibration are not touched (E2 owns `score.go` and `calibration.go`).

## 6. Ownership

Contract (this PR, leads): `internal/events/events.go`, `internal/config/config.go`, `docs/api.md`, this file.

Implementation (`e4/cost`): `internal/agent/{budget,run,triage}.go` plus new files in `internal/agent`
(`usage.go`, `spend.go`), `internal/store/**`, `migrations/00013_spend.sql`, `internal/httpapi/**` (the two
routes), `cmd/server/**`, `evals/{report,stats,compare}.go`, `evals/cmd/evalrun/**`. Expected small exceptions,
listed in that PR: `internal/agent/endpoint.go` (spend wiring), `internal/agent/prompt.go` (the hand-off
renderer skips `agent.usage`), `internal/room` (`agent.usage` on the agent sink's allow-list, a sink for the
hand-off's usage, the pipeline access check).

## 7. Tests (each must fail without its guard)

One usage event per request with the right role across a triage→main switch; no prompt text in the event (marker
grep); cost omitted with no price table; the budget persists across a simulated restart (memstore, and Postgres 16
in an integration test); concurrent admits overshoot by at most one request; a store error fails closed with a cap
and open without; the daily rollover at UTC midnight with a fake clock; percentiles and the bootstrap CI on fixed
inputs; McNemar counts; the n < 20 note.

## 8. UI follow-up (not in this work)

The web reducer ignores unknown types, but `web/src/state/describe.ts` renders an unknown type as its name, so
an activity feed would show `agent.usage` lines. The UI v2 track should drop `agent.usage` from the feed and may
show a cost chip on the room header from `GET /api/rooms/{id}/usage`.
