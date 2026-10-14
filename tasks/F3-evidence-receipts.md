# F3 — Evidence receipts: every claim links to a step, every step shows the read it ran

Owners: F3a lead (contract, in F0) · F3b A2 · F3c A3 · F3d A4 · F3e + F3g A6 · F3f C3.
Model: **Sonnet 5.5** for F3b, F3c, F3f, F3g (mechanical, well-specified); **Opus 5.5** for F3d (grounding
rules and the hand-off retry touch prompt safety) and F3e (interaction design with a11y and motion).
Rank 3 in `docs/product/vision-2026-10.md` §4; principle 2 ("every sentence has a receipt"); defect D8.

## Goal
A watcher clicks "based on steps 3, 5" on the hypothesis, lands on those step rows (they flash), and on each
row reads "Ran: `SELECT … FROM pg_replication_slots WHERE slot_name = ANY($1)`" with a Copy button, so the claim
can be re-checked against the real system in ten seconds. A term the agent uses that no cited step shows is
marked "unbacked"; a hypothesis citing a step that does not exist or errored is refused before it reaches the
room; the hand-off summary is checked against the log before it is stored.

## Evidence
- Platform review §1 ("Evidence receipts"), §4 gaps 1, 3, 5: `tools.Result` has no provenance; citations are
  decorative (`HypothesisCard.tsx:9-15` renders numbers, unknown ids render as `s_01J…`); `tools_room.go:94-118`
  accepts `based_on_steps: []` at `high` and never checks the ids; `CheckHandoff` (`handoff_check.go:92`) has no
  caller outside tests.
- On-call walkthrough step 5 (`06b-step-expanded-dark.png`): the expanded step shows only the summary and
  "Raw input and result" (the model's input, not the SQL).
- UX critique §7 (evidence as a first-class object), §4.A (the Evidence Trail), type punch list: evidence is
  12 px mono muted in a muted box; `data-cited`, `scroll-margin-top: 96px`, `--cited: var(--agent)`.

## Scope — what to build
1. **F3a (lead, F0): contract.** `tools.Call`, `tools.Result.Provenance`, a context recorder, caps;
   `events.ToolCall` on `agent.tool_result`; `events.AgentHypothesisPayload.UnbackedTerms`;
   `events.HandoffSummary.Violations`; `docs/api.md` notes. Exact text in **Contract** below.
2. **F3b (A2): clients fill provenance.** `internal/clients/connect/connect.go` `doBody` (`connect.go:158`),
   `internal/clients/schemareg/schemareg.go` `do` (`schemareg.go:156`), every `kadm`/`kgo` call in
   `internal/clients/kafka/kafka.go` (`Topics:86`, `TopicOffsets:103`, `ConsumerGroups:237`, `GroupLag:248`,
   `FindByKey:294`) and `dlq.go` (`DLQOffsets`, `PeekDLQ`) call `tools.Record(ctx, tools.Call{...})` after the
   call returns (success or error), with `TookMS` measured around the call. Shapes:
   | Client | Kind | Target | Text | ParamsRedacted |
   |---|---|---|---|---|
   | connect | `http` | `c.baseURL` | `"GET /connectors/pg-orders-connector/status"` (method + path incl. query; the path is already built from `url.PathEscape`) | none |
   | schemareg | `registry` | `c.baseURL` | `"GET /subjects/pg.public.orders-value/versions/latest"` | none |
   | kafka | `kafka` | `"kafka " + strings.Join(c.brokers, ",")` | `"kadm.ListTopics()"`, `"kadm.ListEndOffsets(topic)"`, `"kadm.Lag(group)"`, `"kadm.ListGroups()"`, `"kgo.PollFetches(topic, since, maxScan)"` — the literal method name, arguments as names | `["topic=pg.public.orders"]`, `["group=order-sync"]`, `["topic=…","since=2026-10-14T03:00:00Z","max_scan=5000","key=<redacted>"]` — the record key is never recorded (it is a record value) |
   Request bodies are never recorded (`PATCH /connectors/x/config` carries a config value; `POST /subjects/…/versions`
   carries a schema): `Text` is method + path only. No change to any tool: the agent installs the recorder (F3d).
   `connect_config`'s redaction is unaffected (provenance holds no response data).
3. **F3c (A3): Postgres and warehouse provenance.** `internal/clients/postgres/{postgres,scoped,publication}.go`:
   add an unexported `d.query(ctx, sql string, args ...any) (pgx.Rows, error)` and `d.queryRow(...)` that wrap
   `d.pool.Query/QueryRow` and record `{Kind: tools.CallSQL, Target: "postgres <db>@<host>:<port>" (from
   `d.pool.Config().ConnConfig`; never the user or password), Text: sqlText(sql), ParamsRedacted: redactArgs(args)}`.
   `sqlText` collapses whitespace runs to one space and keeps the `$1` placeholders exactly as written; it never
   substitutes values. `redactArgs` renders `$n=<value>` with: identifiers and names as-is (`$1=debezium_orders`),
   `[]string` as `$1=[a,b]`, `time.Time` as RFC 3339, `time.Duration` as its `String()`, numbers as-is; a string
   longer than 64 bytes is cut with `…`; args of `Row(table, pkColumn, pk)` and `warehouse.Row` keep the pk (same
   policy as the tool summary today). Move every call site (`postgres.go:63,116,134,136,167,215,253`,
   `scoped.go:37,57,157,187,196,234,256`, `publication.go:49,84,156`) to the helpers; `rowQuery`'s sanitized
   identifiers (`postgres.go:290`) are part of the SQL text and are fine to show. `internal/clients/warehouse/
   warehouse.go:68-93` the same, `Target: "warehouse <db>@<host>:<port>"`. `pgconv` untouched.
4. **F3f (C3): Snowflake provenance.** `internal/clients/snowflake/sql.go:17,31` build the statements; record
   `{Kind: sql, Target: "snowflake <account>/<database>", Text: the statement with `?` placeholders,
   ParamsRedacted: ["?1=<pk>"]}` at the two execution sites. Same `redactArgs` rules as F3c (copy, not share:
   C3 owns only `internal/clients/snowflake/**`).
5. **F3d (A4): recorder, provenance on the event, grounding, hand-off check.**
   a. `run.go runReadTool` (`run.go:704-779`) and `verify.go runStep` (`verify.go:897`): before `Run`,
      `rec := tools.NewRecorder(); tctx = tools.WithRecorder(tctx, rec)`; after `Run` (error or not),
      `calls := tools.CapProvenance(append(res.Provenance, rec.Calls()...))`; run `redact.Text` on every `Text`
      and every `ParamsRedacted` item (`internal/redact`, as `scrubToolResult` does, `redact.go:21`); convert to
      `[]events.ToolCall` and set `AgentToolResultPayload.Provenance` on **both** the success and the error
      emission (an errored read still ran a query; the receipt explains the error). `failReadTool` emits none.
      The model never sees provenance (`wrapReadToolOutput` unchanged; `TestSystemPromptIsStable` unchanged).
   b. **Step ledger.** New `internal/agent/ledger.go`: `type stepLedger struct{ mu sync.Mutex; steps map[string]stepFact }`
      with `stepFact{tool string; summary string; isError bool}`; `runReadTool`, `failReadTool` and `runStep`
      record every step they open. `newRoomTools` (`tools_room.go:31`) passes it to `updateHypothesisTool`
      (new field `steps *stepLedger`; nil = no checks, so `NewProposeFixTool`-style harnesses keep working).
   c. **`update_hypothesis` rules** (`tools_room.go:94`), in this order, each a `tools.ErrBadInput` the model sees:
      1. every `based_on_steps` entry must match `^s_[0-9A-HJKMNP-TV-Z]{26}$` (`roomIDRef`, `handoff_check.go:296`);
         else `"based_on_steps: %q is not a step id; cite the step= attribute of a tool_output"`;
      2. an id not in the ledger: `"based_on_steps: %s is not a step of this run (known: s_…, s_…)"` (list at most
         the 8 most recent known ids); after a restart the ledger is empty, so the model must re-read: correct;
      3. an id whose step `isError`: `"based_on_steps: %s errored; it supports nothing"`;
      4. `confidence` medium or high with no steps: `"medium or high confidence needs at least one supporting step"`;
         low with none is accepted (a guess is allowed to say it is a guess).
      Then grounding, which **marks, never refuses**: `unbacked := groundTerms(in.Text, citedSummaries)` reusing
      `candidateNames` and `candidateNumbers` (`handoff_check.go:426,468`) against the lower-cased concatenation
      of the cited steps' summaries only (not the whole log: that is the point of a citation); numbers follow the
      same exemptions (dates, durations, %, ~, sizes within 10 %). De-duplicate, keep text order, cap 20, each
      ≤ 80 bytes. Emit `AgentHypothesisPayload{…, UnbackedTerms: unbacked}` and return
      `Summary: "hypothesis recorded"` or `"hypothesis recorded; 2 terms are not in the cited steps: `order-sync`,
      `48`. Cite the step that shows them or drop them."` so the model can fix itself on the next call.
   d. **Hand-off check wired.** `handoff.go writeHandoff` (`handoff.go:25`): after the summary parses, run
      `CheckHandoff(roomEvents, summary)`. If violations exist and this was the first attempt, append the
      assistant JSON and one user turn `"The summary failed these checks; fix them and return the corrected
      JSON:\n- " + strings.Join(lines, "\n- ")` where each line is `v.Rule + " " + v.Field + ": " +
      wrapUntrusted(v.Detail)` (Detail quotes the model's own words, which may echo tool text), call `m.Turn`
      once more (same params; the second call reports usage like the first through `reportingModel`,
      `spend.go:184`), re-check. Whatever remains is stored: `summary.Violations = toEventViolations(vs)` (cap 20,
      `Detail` ≤ 200 bytes). Shape violations (`RuleEmptyField`, `RuleStringTooLong`, `RuleListTooLong`) are
      fixed by the retry or left for `internal/room sanitizeHandoff` (`service_handoff.go:24`), which truncates;
      the summary is never refused for violations alone. A4 does not touch `internal/room`.
6. **F3e (A6): receipts, chips, unbacked marks, hand-off violations.**
   a. `api/types.ts`: `ToolCall {kind, target, text, params_redacted?, at, took_ms}`,
      `AgentToolResultPayload.provenance?: ToolCall[]`, `AgentHypothesisPayload.unbacked_terms?: string[]`,
      `HandoffSummary.violations?: HandoffViolation[]` with `HandoffViolation {rule, field, detail}`.
      `state/types.ts`: `ToolCallState.provenance?: ToolCall[]`, `HypothesisState.unbacked_terms: string[]`;
      `reducer.ts` `agent.tool_result` (`reducer.ts:147`) copies `provenance`; `agent.hypothesis` (`:199`)
      copies `unbacked_terms ?? []`; `fromSnapshot` sets `[]`.
   b. **"Ran:" line** in `Timeline.tsx Evidence` (`Timeline.tsx:46`), one per call, above the summary:
      `<div class="flex items-start gap-2 font-mono text-xs">` with a muted kicker `Ran` (12/16/500 subtle), the
      kind as a neutral badge (`sql` · `http` · `kafka` · `registry`), `<code>` holding `text` (12/16/400, wraps,
      `max-h-24 overflow-auto`), `params_redacted` as `ArgTag`s (`Timeline.tsx:25`), `took_ms` right-aligned
      `tabular-nums`, and a Copy ghost button (`h-7`, 14 px `Copy`/`Check` icons, `aria-label="Copy query"`,
      flips to "Copied" for 1.5 s; same pattern as `ProposalsPanel.tsx:167-192`). Copy writes `text` only.
      `target` is the `title` of the kind badge. No provenance → no line (replay tools, old events).
   c. **Citation chips.** `HypothesisCard.tsx`: replace `basedOn()` text with `based on` + one
      `<button type="button" class="chip">step 3</button>` per cited id (`font-mono text-[10px]`, `h-5`,
      `border-agent/40 text-agent-foreground`, hover `bg-agent-subtle`), `aria-label="Go to step 3: <title>"`.
      An id not in `steps` renders a disabled chip `step ?` with `title="This step is not in the room log"`
      (never the raw id). Click → `onCite(stepId)` prop. `RoomScreen.tsx` owns `citedStep: {id, nonce} | null`
      and passes `highlightStepId` to `Timeline`; `DoneStep` (`Timeline.tsx:234`) gets `id={`step-${step.id}`}`,
      `scroll-mt-24` (96 px, under the sticky header), opens itself when `highlightStepId === step.id`, calls
      `scrollIntoView({block: 'center', behavior: reduced-motion ? 'auto' : 'smooth'})` and adds
      `data-cited` + `motion-safe:animate-cite` for 600 ms (tailwind keyframe `cite`: `background-color` from
      `--agent-subtle` to transparent; `keyframes` at `tailwind.config.ts:99`). Focus moves to the row button
      (`tabIndex=-1` is not needed: it is a `<button>`). Hovering a chip sets `data-cited` on the row without
      scrolling (the Evidence Trail hover from UX §4.A); `E` toggles nothing yet (out of scope).
   d. **Unbacked terms.** `HypothesisCard`: when `unbacked_terms.length > 0` show `Badge variant="warning"`
      `"2 unbacked"` with `title="Terms the cited steps do not show"`; in the text, wrap the first
      case-insensitive occurrence of each term in `<mark class="unbacked">` (dotted underline
      `decoration-warning`, transparent background, `title="Not found in the cited steps"`). Matching is on
      the plain string with React text nodes: never `dangerouslySetInnerHTML`, never a regex built from the
      term without `escapeRegExp`. New `lib/unbacked.ts: markTerms(text, terms) → Array<{text, unbacked}>`.
   e. **Hand-off violations.** `HandoffDialog.tsx HandoffSummary` (`:126`) and the accept banner (F2's
      `HandoffCard` reuses the same `HandoffChecks` component): under "Risks", a block `Checks` with one row per
      violation: amber `TriangleAlert` 14 px, `rule` as a neutral mono badge (`unknown_identifier`), `detail`
      as plain text. Zero violations and a summary that has the field (`violations: []`) → green row
      "Checked against the room log · no issues". A summary without the field (older event) → nothing.
7. **F3g (A6): evidence renderers.** `components/room/evidence/{ConnectStatus,SchemaDiff,ConsumerLag,RecentDdl,
   Freshness,ReplicationSlots,Raw}.tsx` and `lib/evidence.ts` with `evidenceFor(tool): Renderer | null` keyed by
   exact tool name and a type guard per shape. Each takes `ToolCallState` and renders from `data`; a guard that
   fails (truncated `data` object `{truncated: true, …}`, old shape, missing keys) falls back to `Raw` (today's
   `<pre>` summary, kept as-is). Text only: every cell is a React text node; no links from values.
   | Tool | `data` shape (Go source) | Render |
   |---|---|---|
   | `connect_status` | `{name, state, worker, tasks: [{id, state, error?}]}` (`connect_tools.go:116-141`) | table `task · state · error`; state as `StatusDot` (`RUNNING` ok, `PAUSED` warn, `FAILED` fail, else unknown); `error` in a `<pre>` cell `max-h-32 overflow-auto`, first line bold |
   | `schema_registry_diff` | `{subject, from_version, to_version, changes: [{field, kind, from?, to?}]}` (`registry_tools.go:194-205`) | header `v2 → v3`; rows `field` mono, `kind` badge (`added` success, `removed` destructive, `changed` warning), `from → to` |
   | `kafka_consumer_lag` | `{group, state, members, total_lag, worst_partitions: [{topic, partition, committed, end, lag}]}` (`kafka_tools.go:217-220`) | header `state · N members · total lag`; table `topic/partition · committed · end · lag` `tabular-nums`, lag > 0 in warning tone |
   | `pg_recent_ddl` | `[{at, command_tag, object_identity, query, username}]` (`clients.DDLChange`, `pg.go:170`) | list: time (relative, UTC on hover via `lib/time.ts`), `command_tag` badge, `object_identity` mono, `username`, `query` in `<pre>` |
   | `pg_replication_slots` | `[{name, plugin, active, restart_lsn, confirmed_flush_lsn, retained_wal_bytes, wal_status, safe_wal_bytes?}]` (`pg.go:82`) | table `slot · active · wal_status · retained`; `active=false` or `wal_status` ∉ {reserved} in fail tone |
   | `warehouse_freshness` | `{table, ts_column, newest: string\|null, stale_for?}` (`wh.go:51-58`) | one line `newest row 09:58:14 · 48m 12s ago` or `no rows (empty table)` |
   `Evidence` in `Timeline.tsx` renders `evidenceFor(t.tool)?.(t) ?? <pre>{summary}</pre>`; the summary `<pre>`
   stays available under "Raw input and result" so nothing is lost. Numbers use `tabular-nums`; widths fit the
   column at 320 px (tables scroll horizontally inside the row, `overflow-x-auto`, never the page).

## Out of scope
`E` evidence mode (dims non-cited rows); proposal-step `evidence` citations; `Confirmed`/verdict route (F9);
escaping driver steers (`prompt.go:243`, F10); the LLM hand-off judge (`handoff_judge_claude.go`); refusing a
hypothesis for unbacked terms (marking only, platform review §1 risk); provenance for replayed eval tools
(C6: recordings have none; the "Ran:" line is simply absent); `pipeline_map`, `trace_record`, `runbook_lookup`,
`similar_incidents` provenance (no external call, or the room store); the post-mortem (F2).

## Contract (F3a — lead, in F0)
`internal/tools/tool.go` (after `Result`):
```go
// Call kinds for Call.Kind.
const (
	CallSQL      = "sql"      // a SQL statement with $1/? placeholders
	CallHTTP     = "http"     // a Kafka Connect REST request
	CallKafka    = "kafka"    // a kadm/kgo admin or fetch request
	CallRegistry = "registry" // a Schema Registry REST request
)

// Call is one read a tool made against a customer system: the receipt behind
// the tool's summary. Text is what a person would re-run: SQL with its
// placeholders (never values), "GET /connectors/x/status", "kadm.Lag(group)".
// ParamsRedacted holds the arguments as "$1=value" / "name=value" after the
// client's redaction; a record key or value is never in it. Target names the
// system (host, database, brokers), never credentials.
type Call struct {
	Kind           string    `json:"kind"`
	Target         string    `json:"target"`
	Text           string    `json:"text"`
	ParamsRedacted []string  `json:"params_redacted,omitempty"`
	At             time.Time `json:"at"`
	TookMS         int64     `json:"took_ms"`
}

// Provenance caps, applied by the agent before a call reaches the room log.
const (
	MaxProvenanceCalls = 16   // calls per Result; the first 16 are kept
	MaxCallTextBytes   = 2048 // Call.Text; cut with Truncate's marker on a rune boundary
	MaxCallParamBytes  = 200  // each ParamsRedacted item
)

// Recorder collects the Calls made under a context. A client records with
// Record(ctx, call); a caller that wants receipts installs one with
// WithRecorder and reads Calls after the tool ran. Safe for concurrent use.
type Recorder struct {
	mu    sync.Mutex
	calls []Call
}

func NewRecorder() *Recorder { return &Recorder{} }
func (r *Recorder) Record(c Call)
func (r *Recorder) Calls() []Call // a copy, in call order

type recorderKey struct{}

// WithRecorder returns ctx carrying r. RecorderFrom returns it.
func WithRecorder(ctx context.Context, r *Recorder) context.Context
func RecorderFrom(ctx context.Context) (*Recorder, bool)

// Record adds c to the recorder in ctx; without one it does nothing. Clients
// call it after every request, on success and on error. It never blocks.
func Record(ctx context.Context, c Call)

// CapProvenance applies the caps above and reports whether anything was cut.
func CapProvenance(calls []Call) ([]Call, bool)
```
`Result` gains, after `Truncated`:
```go
	// Provenance lists the reads behind this result (see Call). A tool may
	// set it; the agent also merges the calls recorded under its context
	// (WithRecorder), tool-set calls first, and applies the caps. It is
	// stored on agent.tool_result.provenance, outside Data and its cap.
	Provenance []Call `json:"provenance,omitempty"`
```
Import direction: `internal/tools` imports `owners` → `dlq`, `events`; none of them imports a client package, so
every client sub-package may import `piperoom/internal/tools` to call `Record` (checked: no cycle).

`internal/events/events.go`:
```go
// ToolCall mirrors tools.Call (same fields and JSON tags) so this package stays
// a leaf. The agent converts and applies tools' caps before emitting.
type ToolCall struct {
	Kind           string    `json:"kind"`
	Target         string    `json:"target"`
	Text           string    `json:"text"`
	ParamsRedacted []string  `json:"params_redacted,omitempty"`
	At             time.Time `json:"at"`
	TookMS         int64     `json:"took_ms"`
}
```
`AgentToolResultPayload` gains, after `TookMS`:
```go
	// Provenance is the list of reads behind Summary (the receipt), redacted
	// and capped by the agent (tools.MaxProvenanceCalls). It is outside Data:
	// a cut Data keeps its receipt. Empty for room tools and replayed tools.
	Provenance []ToolCall `json:"provenance,omitempty"`
```
`AgentHypothesisPayload` gains, after `Confirmed`:
```go
	// UnbackedTerms are identifier-like names and numbers in Text that none of
	// the cited steps' summaries contain (agent grounding, F3). The UI marks
	// them; they never block the hypothesis. At most 20, each ≤ 80 bytes.
	UnbackedTerms []string `json:"unbacked_terms,omitempty"`
```
`HandoffSummary` gains, after `Risks`, plus a type:
```go
	// Violations are the checks the summary still fails after the agent's
	// one self-correction pass (agent.CheckHandoff rules: unknown_reference,
	// unknown_identifier, unknown_number, open_proposal_missing, …). Shown to
	// the recipient before Accept; they never block a hand-off. nil on
	// summaries written before F3; [] when checked and clean.
	Violations []HandoffViolation `json:"violations,omitempty"`
}

// HandoffViolation is one failed hand-off check (agent.HandoffViolation's
// wire form). Detail may quote the summary; it is agent text, ≤ 200 bytes.
type HandoffViolation struct {
	Rule   string `json:"rule"`
	Field  string `json:"field"`
	Detail string `json:"detail"`
}
```
`docs/api.md` → Objects/Event: "`agent.tool_result` may carry `provenance: [{kind, target, text, params_redacted?,
at, took_ms}]`, the reads behind the summary (≤ 16); `agent.hypothesis` may carry `unbacked_terms: [string]`;
`handoff.created.summary` may carry `violations: [{rule, field, detail}]` (F3)". Snapshot `hypothesis` gains
`unbacked_terms`.

## Ownership and files
| File | Owner | Change |
|---|---|---|
| `internal/tools/tool.go`, `internal/events/events.go`, `docs/api.md` | lead (F3a) | modified |
| `internal/clients/connect/connect.go`, `…/schemareg/schemareg.go`, `…/kafka/{kafka,dlq}.go` + `_test.go` | A2 (F3b) | modified |
| `internal/clients/postgres/{postgres,scoped,publication,provenance}.go`, `…/warehouse/warehouse.go` + tests | A3 (F3c) | modified / new |
| `internal/clients/snowflake/{sql,snowflake}.go` + tests | C3 (F3f) | modified |
| `internal/agent/{run,verify,tools_room,handoff}.go`, new `ledger.go`, `grounding.go` + tests | A4 (F3d) | modified / new |
| `web/src/api/types.ts`, `state/{types,reducer}.ts`, `components/{Timeline,HandoffDialog,RoomScreen}.tsx`, `components/room/HypothesisCard.tsx`, new `lib/unbacked.ts`, `tailwind.config.ts` | A6 (F3e) | modified / new |
| `web/src/components/room/evidence/*.tsx`, new `lib/evidence.ts`, `components/Timeline.tsx` (one line) | A6 (F3g) | new / modified |
| `internal/room/**` | A5 | **untouched** (snapshot copies `AgentHypothesisPayload` field by field at `snapshot.go:269`: add `UnbackedTerms` there in F0's A5 line, 2 lines) |

## Dependencies and merge order
F0 (F3a) → F3b, F3c, F3f in parallel (each tested with `httptest` / a fake pool; `tools.RecorderFrom` in tests)
→ F3d (needs F3a only; its tests use fake tools that call `tools.Record`) → F3e (needs F3a types; fixtures
in `web/src/fixtures/schema-change.json` gain `provenance` and `unbacked_terms` on two events) → F3g (needs F3e's
`Evidence` hook point). F3d must rebase on `a4/cap-tool-data` (#136: `toolDataJSON` returns `(data, cut)`) and
`a4/r5b-alert-mask-readonly` (#140). F3e must rebase on `a6/first-five-minutes` (#146: `HandoffDialog.tsx`,
`Timeline.tsx`, `reducer.ts` change there) and `a6/verified-badge` (#135: `Timeline.tsx:3`, `reducer.ts`). F3
does not touch `prompt.go`'s system prompt, the room service, or `internal/actions`.

## Design details
- **Data flow.** client `tools.Record(ctx, call)` → `Recorder` installed by the runner around `Tool.Run` →
  `CapProvenance` → `redact.Text` → `events.ToolCall` on `agent.tool_result.provenance` → reducer
  `ToolCallState.provenance` → `Evidence` "Ran:" line. Provenance is outside `Data`, so #136's 64 KiB `Data`
  cap and `TruncatedData` are untouched; its own bound is 16 × (2 KiB + 16 × 200 B) ≈ 83 KiB worst case, and in
  practice one call per tool (`connect_status` makes 1–2; `kafka_find_by_key` ≤ 3).
- **Redaction.** Clients record text they built themselves (SQL constants, paths from `url.PathEscape`, kadm
  method names) and arguments the tool already shows in its summary. The agent still runs `redact.Text` over
  every string (connection strings in a driver error can reach `Target`; a secret can be a param), and logs
  redaction stats as `scrubToolResult` does. Nothing in provenance is ever sent to the model.
- **Untrusted text.** `Call.Text` and params render as text nodes only; the Copy button copies the string. A
  `Detail` of a violation and an unbacked term are agent text: text nodes only, `escapeRegExp` before matching.
- **Grounding on cited steps only** (not the whole log, unlike `CheckHandoff`): a hypothesis is a claim about
  specific evidence; a term that lives in an uncited step is "unbacked" until the agent cites that step. The
  summary text used for grounding is the capped summary the model saw (the ledger stores ≤ 8 000 bytes/step).
- **Failure modes.** No recorder in ctx → `Record` is a no-op (tools run in `cmd/investigate`, watcher, evals
  unchanged). A client that forgets to record → no "Ran:" line, nothing breaks. `update_hypothesis` refusals are
  `ErrBadInput`: the model gets the message and retries; they are not counted as tool errors of the run. The
  hand-off retry adds ≤ 1 model call (`handoffSummaryTimeout` is 3 min: the retry gets what is left of it;
  on timeout the first summary is stored with its violations). Chips for ids missing from `steps` (a room
  rendered from a snapshot before replay, or a restarted agent citing a stale id) are disabled, never broken.
- **Light/dark, motion, a11y.** Chips and marks use `--agent`/`--warning` tokens only; `animate-cite` is 600 ms
  `cubic-bezier(.2,.8,.2,1)` under `motion-safe:`; `scrollIntoView` uses `behavior: 'auto'` under
  `prefers-reduced-motion`. Chips are buttons with labels; the copy button announces "Copied" via its label;
  tables have `<th scope="col">`; the unbacked `<mark>` has a `title` and keeps the text in reading order.
- **Mobile.** "Ran:" wraps (`break-all` on the code), the copy button stays 44 px tall on coarse pointers;
  evidence tables scroll inside the row.

## Done when
- [ ] F3b/F3c/F3f: every method of each client records exactly one `tools.Call` per request (two for a retry),
      on success and on error, with the shapes in the table; `go test ./internal/clients/...` passes without
      Docker; `-tags integration` passes against the dev stack and a `pg_replication_slots` call shows
      `Text` starting with `SELECT slot_name, plugin, active` and `ParamsRedacted: ["$1=[debezium_orders]"]`.
- [ ] F3d: `agent.tool_result` events carry `provenance` (≤ 16 calls, text ≤ 2 KiB, redacted); an
      `update_hypothesis` with an unknown, malformed or errored step id, or medium/high with none, is refused
      with the exact messages above; `unbacked_terms` is set when a name or number is missing from the cited
      summaries; `writeHandoff` retries once on violations and stores the rest in `summary.violations`;
      `go test ./internal/agent/...` passes; default eval set pass rate unchanged (`evals/README.md`).
- [ ] F3e: clicking "step 3" scrolls to and opens step row 3, which flashes once; a stale id shows `step ?`;
      "Ran:" shows the query with a working Copy; unbacked terms are marked and counted; hand-off violations
      show before Accept; `npm run build`, `npm run lint`, `npx vitest run` pass; fixtures mode
      (`VITE_FIXTURES`) shows all of it from `schema-change.json`.
- [ ] F3g: the six tools render as tables/lists from `data`; a truncated or malformed `data` falls back to the
      summary `<pre>`; no `dangerouslySetInnerHTML`, no `<a>` built from values (`grep` in the PR).
- [ ] Only owned files changed; contract files untouched; PRs < 600 lines each.

## Tests
- Go, `internal/tools`: `TestRecorderCollectsInOrder`, `TestRecordWithoutRecorderIsNoop`,
  `TestCapProvenanceCutsCallsTextAndParams` (17 calls, 3 KiB text, 300-byte param; marker on a rune boundary).
- Go, F3b: `TestConnectStatusRecordsCall` (httptest; `Kind=http`, `Text="GET /connectors/c1/status"`,
  `TookMS>=0`), `TestConnectErrorStillRecords` (500 → call recorded, error returned),
  `TestRegistrySchemaRecordsPath`, `TestKafkaGroupLagRecordsKadmLag` (fake `kadm` seam or the existing
  `offsetLister` seam), `TestFindByKeyNeverRecordsKey`.
- Go, F3c: `TestSQLTextCollapsesWhitespaceKeepsPlaceholders`, `TestRedactArgsTable` (string, []string, time,
  duration, long string), `TestReplicationSlotsNamedRecordsCall` (pgxmock-free: wrap the pool behind the
  unexported `querier` interface used by the helpers), `TestWarehouseFreshnessRecordsTarget`.
- Go, F3d: `TestReadToolProvenanceEmitted`, `TestErroredReadToolKeepsProvenance`, `TestProvenanceRedacted`
  (a param `password=hunter2` → masked), `TestHypothesisRefusesUnknownStep`, `TestHypothesisRefusesErroredStep`,
  `TestHypothesisRefusesMalformedID`, `TestHypothesisNeedsStepsAtMediumHigh`, `TestHypothesisLowWithoutSteps`,
  `TestHypothesisUnbackedTermsFromCitedOnly` (term in an uncited step is unbacked), `TestHypothesisNumbersFollowHandoffRules`,
  `TestWriteHandoffRetriesOnceOnViolations` (scripted model: first answer cites `p_01…` unknown, second clean →
  `Violations == []`), `TestWriteHandoffStoresRemainingViolations`, `TestWriteHandoffRetryWrapsDetail`
  (the second user turn wraps Detail as untrusted). Eval: `evals/scenarios/schema-change.yaml` unchanged; run
  the default set once and attach the pass rate to the PR (C6's harness; no new scenario).
- Vitest, F3e: `components/Timeline.ran.test.tsx` (Ran line, copy writes `text`, no line without provenance),
  `components/room/HypothesisCard.test.tsx` (chips, `step ?`, click calls `onCite`, unbacked badge + marks,
  regex-special term `a.b(c)` does not throw), `components/RoomScreen.cite.test.tsx` (click → row opens,
  `scrollIntoView` called with `block: 'center'`, `data-cited` present then removed after 600 ms with fake
  timers), `components/HandoffDialog.checks.test.tsx` (violations list, clean row, absent field),
  `state/reducer.provenance.test.ts`, `lib/unbacked.test.ts`.
- Vitest, F3g: `components/room/evidence/evidence.test.tsx`: one happy case per renderer from the Go shapes
  above, one fallback case each (`{truncated: true}` object, wrong type), and `evidenceFor('unknown_tool') === null`.

## Effort
F3a S (lead) · F3b M (A2, ~350 lines with tests) · F3c M (A3, ~450) · F3f S (C3, ~120) · F3d L (A4: split into
PR 1 recorder + provenance + ledger ~400, PR 2 grounding + hand-off retry ~450) · F3e M (A6, ~500) ·
F3g M (A6, ~550). Total ≈ 2.8 k lines over 8 PRs, none over 600.
