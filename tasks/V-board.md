# V wave dispatch board: one row = one PR

Owner decision of 2026-10-19: Piperoom runs on every major model provider, not only Claude (OpenAI, Azure OpenAI,
Google Gemini, DeepSeek, Qwen, Mistral, Groq, Together, OpenRouter, xAI, local Ollama and vLLM, and non-Claude models
on AWS Bedrock). Claude (Anthropic API, Bedrock, Vertex) stays fully supported and is the reference model. Design
`docs/design/model-providers.md`: four adapters (Anthropic, OpenAI-compatible, Gemini, Bedrock Converse) behind one
model interface, `internal/agent/llm`. Each row is a PR an agent finishes in one or two sessions (target < 600
changed lines, §6 of `AGENTS.md`).

- **ID** is the PR tag (`[V3a] llm/openai: chat completions adapter`). The design PR is `[V0-design]`.
- **Model**: every row runs on Opus 5.5, cloud rows included (the owner's standing instruction for the Q and U
  waves, kept here). The ship workflow builds every row on Opus 5.5 anyway (`AGENTS.md` §6a).
- **Days**: one agent's working days including tests and review fixes.
- **After**: rows that must be merged first. P rows by their `tasks/P-board.md` ID; U rows by `tasks/U-board.md`.

**Timing.** Code rows start after P3 closes: **V0 is After P3-W2e** (the live eval run that re-baselines
`evals/baseline/default.json` and closes P3), because that Claude baseline is the reference every other model is
gated against (design §5). Nothing on this board is dispatched before P3-W2e merges.

**Order.** V0, then the A4 refactor (V1a, V1b) that changes nothing for Claude, then the neutral guards (V2a, V2b),
then the adapters (V3a, V3b, V4, V5) and per-role selection (V6); the evals rows (V9 to V12) run beside the
adapters; each model is admitted by its own gate run (V10) and an A4 line (V13). A4 carries most days and is the
critical path; an agent with two rows ready takes the one earlier in this order first.

**Claude unchanged.** V1a and V1b are refactors with no behaviour change: the golden request-bytes test, the
unchanged prompt hashes and today's tests are their acceptance (design §13). No later row may change a Claude
request except V16's template-token rule (open decision 6) and V6's pricing rule (open decision 4), each only as the
owner decides.

**Ownership.** No new folders: every row stays inside its agent's folders (`AGENTS.md` §2). The new contract file
`internal/agent/llm/llm.go` is the leads' (in `docs/process/ownership.json`, excluded from A4); everything else under
`internal/agent/llm/**` is A4's. **V5 adds one module** (`github.com/aws/aws-sdk-go-v2/service/bedrockruntime`);
`go.mod` and `go.sum` are in no agent's folders in `ownership.json`, so V5 is built outside the ship workflow and
merged after the §7 two-lead review (`AGENTS.md` §6a: PRs made outside the workflow keep the two-lead review). Every
other row leaves `go.mod` unchanged (a Done-when item where it matters). `internal/injectscan/**` is N6's
(`AGENTS.md` §2, earlier waves) and V16 is N6's row.

## V0 items: the contract additions each row needs

Design §4 and §9, written by the leads in V0: the new contract file `internal/agent/llm/llm.go` (`Model`, `Request`,
`ToolDef`, `Message`, `Part`, `Native`, `ToolCall`, `ToolResult`, `StopReason`, `Response`, `Usage`, `Capabilities`,
`ErrorKind`, `APIError`, `Unretryable`, the `Provider*` constants); `events.AgentUsagePayload.Provider` and
`.Estimated`; the config variables of design §9 (`PIPEROOM_MODEL_PROVIDER` values `anthropic | bedrock | vertex |
openai | gemini | bedrock-converse`, `PIPEROOM_MODEL_FLAVOR`, `PIPEROOM_MODEL_BASE_URL`, `PIPEROOM_MODEL_API_KEY`
and `_FILE` (read by adapters, never stored in `Config`), `PIPEROOM_MODEL_CONTEXT_TOKENS`, `PIPEROOM_MODEL_CAPS`,
`PIPEROOM_MODEL_UNVERIFIED`, `PIPEROOM_GEMINI_BACKEND`, the role pairs `PIPEROOM_TRIAGE_PASS_PROVIDER`,
`PIPEROOM_SUMMARY_PROVIDER`/`_MODEL`, `PIPEROOM_JUDGE_PROVIDER`/`_MODEL`, `PIPEROOM_<ROLE>_BASE_URL`) with
`ParseModelRoles` and `ParseBaseURL`; `docs/api.md` (`agent.usage` `provider` and `estimated`; `GET /api/models`;
`PUT /api/models` for standalone and quickstart only). The old unexported `model` seam is changed by V1a with its
fakes, so V0 itself breaks no fake.

## Step 0: contracts

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V0-design | leads | Opus 5.5 | 1 | docs: `docs/design/model-providers.md`, this board, the AGENTS.md wave section, `ownership.json` (the contract file to leads) | merged; the open owner decisions of design §15 posted to the owner | — |
| V0 | leads | Opus 5.5 | 1.5 | contracts: `internal/agent/llm/llm.go`, `AgentUsagePayload.Provider`/`.Estimated`, the config variables and parsers, `docs/api.md` | the V0 items in code with doc comments; `ParseBaseURL` table tests (https; http only for `localhost`, `127.0.0.0/8`, `::1`; user info, query and fragment refused); `ParseModelRoles` defaults (each role falls back to the main provider and model; the first-look model defaults to `claude-haiku-5-5` only on a Claude provider); the decisions of design §15 that the owner has ruled are applied; `go build`, `go vet`, `go test ./...` green; nothing calls the new code | P3-W2e, V0-design |

## Step 1: Claude onto the interface, no behaviour change

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V1a | A4 | Opus 5.5 | 3 | agent + llm/anthropic: the investigation loop (`run.go`, `triage.go`, `toolparams.go`, `usage.go`, `budget.go`, `spend.go`, `model.go`, `endpoint.go`) on `llm.Model`; the Anthropic adapter for `anthropic`, `bedrock`, `vertex` (streaming, workspace header, beta compaction, cache breakpoints, strict caps as its `Capabilities`); `llm.APIError` with the kinds; the adapter conformance suite `internal/agent/llm/llmtest` | a golden test captured on `main` before the change: the marshalled request of a recorded room's main turn, triage turn and a turn after a compaction block is byte-identical after it; thinking and compaction blocks round-trip through `Native`; every `perRunMarkers` / `modelRejectionMarkers` case maps to the kind that gives today's decision (table test); `agent.PromptHash()` and the evals tap hash unchanged; every existing test in `internal/agent`, `internal/wiring`, `evals` passes and none is loosened or deleted (fakes that built `anthropic.BetaMessage` go through the adapter's decoder); `go.mod` unchanged | V0 |
| V1b | A4 | Opus 5.5 | 1.5 | agent: the single-shot callers on `llm.Model`: first look (`triage_pass.go`), hand-off and post-mortem (`handoff.go`, `postmortem.go`), the hand-off judge (`handoff_judge_claude.go` becomes provider-neutral, still behind the live-judge build tag), `verify.go`, `examples.go`; no file outside `internal/agent/llm/anthropic` imports `anthropic-sdk-go` | the golden byte test extended to the first look (both requests), the hand-off, the post-mortem and the judge; `OutputSchema` maps to `output_config.format`; `grep -l anthropic-sdk-go internal/agent/*.go` (non-test) is empty; every existing test green | V1a |
| V1c | A5 | Opus 5.5 | 0.5 | room: `isUnretryable` uses `llm.Unretryable`; `modelRefused` deleted | the existing `unretryable_internal_test.go` cases pass unchanged in meaning (a 401, a 404 model, a 400 shape error stop; 408, 409, 413, 429, 5xx and "too long" retry), plus one case per new kind; no `anthropic` import left in `internal/room` (tests may keep one) | V1a |

## Step 2: the neutral guards

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V2a | A4 | Opus 5.5 | 1.5 | agent: every tool input (room tools too) validated against its schema in code before `Run`; a tool call with invalid JSON arguments becomes an error `tool_result` and stays paired; the structured-output fallback (forced tool `emit_<what>` with `ToolChoiceRequired`, else text with the first JSON object parsed) under the existing one retry | table tests per room tool for a missing required field, a wrong type, an extra field; a fake model without `StructuredOutput` gets a valid hand-off, first-look verdict and judge verdict through each fallback, and a second bad answer takes the existing failure path (`postmortem.failed`, triage `reason: model_error`); Claude golden bytes unchanged | V1b |
| V2b | A4 | Opus 5.5 | 2 | agent: client-side compaction for a model without `Capabilities.Compaction` (design §6.1): the 70% trigger, the summary call with `compactionInstructions()`, the `<compacted_history untrusted="true">` wrapper, role `compaction` usage | a recorded long room on a fake model: step ids, the hypothesis, open proposals and the driver's last instruction survive; the last 6 messages and everything after the latest human input stay verbatim; untrusted text inside the summary stays wrapped; a `context_exceeded` stop after compaction still pauses for a human; never runs on a Claude provider (test) | V1a |

## Step 3: the adapters

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V3a | A4 | Opus 5.5 | 2.5 | llm/openai: chat completions over `net/http` (no SDK); flavors `openai`, `azure`, `ollama`, `vllm`, `generic`; message, tool and stop-reason translation (design §7.3, §3); usage normalisation (cached tokens out of input, reasoning in output) and the estimate when usage is missing; `reasoning_effort` mapping; retries on 408/429/5xx with `Retry-After` ≤ 60 s; the base URL rules and the dialer of design §10.3; the key as `netsec.Secret` | the `llmtest` conformance suite passes against recorded `httptest` fixtures per flavor (tool calls, parallel calls, a refusal, `content_filter`, Azure's 400 `content_filter` as a refusal, a 429 `insufficient_quota` as `billing`, a context-length 400 as `too_long`, missing usage as `Estimated`); every `ToolResult.Content` byte-equal on the wire; a redirect is not followed; a name resolving to `169.254.169.254` is refused at dial; the key appears in no error, log line or event (capturing handler test); the full registry passes the name and count limits; `go.mod` unchanged | V1b, V2a |
| V3b | A4 | Opus 5.5 | 2 | llm/openai: hosted flavors `deepseek`, `qwen`, `mistral`, `groq`, `together`, `openrouter`, `xai`: default base URLs, key variables, reasoning rules (DeepSeek `reasoning_content`, Qwen `enable_thinking`), cache usage fields, OpenRouter `data_collection: deny` | one recorded fixture set per flavor in the conformance suite; DeepSeek's reasoning content handled by the rule its fixture proves; OpenRouter requests always carry the `provider` object (test); the flavor table in code matches design §3 or the PR says why and updates the design in a leads follow-up | V3a |
| V4 | A4 | Opus 5.5 | 2.5 | llm/gemini: `generateContent` over `net/http`; Gemini API key (`x-goog-api-key`) and Vertex (`PIPEROOM_GEMINI_BACKEND=vertex`, Google ADC through the existing `golang.org/x/oauth2/google`); `functionCall` / `functionResponse` with made ids mapped back by position; thought signatures kept as `Native` and returned; `thinkingConfig` from effort; `responseJsonSchema` for `OutputSchema`; schema conversion (`parametersJsonSchema`, else the OpenAPI subset) | conformance suite on recorded fixtures (parallel calls, a `SAFETY` stop and a `promptFeedback.blockReason` as refusals, `MALFORMED_FUNCTION_CALL` as an error tool result, thoughts counted as output, cached tokens out of input); a schema the converter cannot express fails start with the tool named; the full registry passes the name and count limits; base URL rules as V3a; `go.mod` unchanged | V1b, V2a |
| V5 | A4 | Opus 5.5 | 2 | llm/converse: Bedrock Converse for non-Claude models through `bedrockruntime` (AWS chain or `AWS_BEARER_TOKEN_BEDROCK`, the shared `wiring.LoadAWS` config); `toolUse` / `toolResult` (status `error`), `cachePoint` where the model supports it, `guardrail_intervened` and `content_filtered` as refusals; structured output through the forced tool | **built outside the ship workflow** (adds `github.com/aws/aws-sdk-go-v2/service/bedrockruntime`; `go doc` output for every SDK symbol used in the PR, rule 9); conformance suite on fixtures through the SDK's endpoint override against `httptest`; a Claude model id under `bedrock-converse` is refused at start with "use PIPEROOM_MODEL_PROVIDER=bedrock" | V1b, V2a |

## Step 4: per-role selection and the start checks

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V6 | A4 | Opus 5.5 | 1.5 | wiring + agent + cmd/investigate: one `llm.Model` per role (main and in-loop triage on the main provider; first look, summary and judge on their own provider); `internal/agent/llm/supported.json` with the Claude reference models; the start checks of design §9; provider-qualified price keys and the fail-safe rule of design §8.2; the startup log (provider, model, region, cache yes or no, dropped parameters); `cmd/investigate -provider` | table test per start failure (unknown provider or flavor, missing key per role, bad base URL, model not in `supported.json` without `PIPEROOM_MODEL_UNVERIFIED=on`, unpriced model under a cost cap, triage model on another provider); `groq:<model>` wins over `<model>` in the price lookup; with nothing new set, the endpoint, its log line and every request are as before (Claude golden bytes); the AST read-only test of `internal/wiring` still passes | V1b, V3a |

## Step 5: the API and the picker

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V7 | A5 | Opus 5.5 | 1.5 | httpapi + quickstart: `GET /api/models` (roles with provider, model, region, gate status, `prompt_cache`, price; the supported list with pass rates); `PUT /api/models` on standalone and quickstart only (loopback, the solo user), validating through V0's parsers, writing `.piperoom/model.env` 0600 through `os.Root` and rebuilding the endpoint; the *no model* start state | `GET` never returns a key or a full base URL (host only; test); a team server answers `PUT` with `404`; a non-loopback request is refused; the key is excluded from request logging and in no event (capturing logger test); an invalid base URL or an unsupported model without `UNVERIFIED` is `400 validation`; a server with no usable model in standalone starts and serves the picker data; `docs/api.md` rows match | V6, U1-5c |
| V8 | A6 | Opus 5.5 | 1.5 | web: the read-only Model panel (settings and the room header popover: provider, model, region per role, gate status, cache, price, "unverified model" badge) and the standalone first-run picker (supported list with pass rate and cost per run, experimental behind a warning, the key in a secret field) | Vitest per state (team server read-only, standalone no model, picker submit, `400` shown inline, unverified badge); the key field is never echoed and posts only to `PUT /api/models`; mobile layout; a11y unchanged; `npm run lint`, `build`, `vitest` green | V7, U1-12a |

## Step 6: evals and CI

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V9 | C6 | Opus 5.5 | 1.5 | evals: the provider-neutral judge (`NewLLMJudge(llm.Model, model)`; `-judge llm`, `-judge claude` kept as its alias); judge cache entries record `provider:model` (key unchanged); the live-judge build tag `livejudge` with `claude` kept as an alias; `APIRefusal` from `llm.APIError` kinds | every existing judge test passes; a fake model without `StructuredOutput` judges through the fallback; recorded verdicts still load; `APIRefusal` returns the same answers for today's 401, 403 and "credit balance" cases | V1b, V2a |
| V10 | C6 | Opus 5.5 | 2 | evals: `-provider`, `-flavor`, `-judge-provider` flags; per-model baselines `evals/baseline/<provider>-<model>.json` (reading `default.json` as the reference until V12 lands); the report records provider, flavor, model, effort and the prompt hash kind; `-gate REF.json` with the hard gates and the quality floor of design §5 (exit 4 and 5); `-models-table DIR` prints the published table; `evals/README.md` section | gate math table-tested on fixture reports (98% valid calls, zero forbidden actions or params, zero injection failures, the 0.8 / 0.7 ratios); `-models-table` over the committed baselines prints the reference first; an Ollama run documented with its exact command; the CDC replay set is unchanged | V9, V6 |
| V11 | C6 | Opus 5.5 | 1.5 | evals: evals-live provider inputs: an in-process model tap for non-Anthropic providers (records the models called and the `neutral-sha256:` hash), the existing HTTP tap unchanged for `anthropic`; `evalrun` checks the role keys of the chosen provider before any work, as it checks `ANTHROPIC_API_KEY` today | a report from each adapter's fake records the right models and hash kind; the Anthropic tap hash is unchanged on a replay; a missing provider key fails before the first scenario with the variable named | V10, V3a |
| V12 | C1 | Opus 5.5 | 1 | ci: `evals-live.yml` `workflow_dispatch` inputs `provider`, `flavor`, `model`; one secret per provider in the `evals` environment; compare against the model's own baseline; artifact `baseline-<provider>-<model>`; a matrix over `supported.json` for the nightly (left off, as today); a `ci.yml` check that every `supported.json` entry has a baseline that passes `-gate`; README and `docs/runbook.md` link `docs/pilot/models.md` | a dispatch with each provider value runs or skips cleanly when its secret is missing; no PR run gets a secret (the existing fork and environment rules unchanged); the check fails on a fixture entry without a baseline | V11 |
| V15 | C7 | Opus 5.5 | 1 | scenarios: `injection-template-tokens.yaml` (tool output holding chat-template control tokens and a fake provider tool-call syntax), `long-room-compaction.yaml` (a room long enough to trigger client-side compaction), `parallel-reads.yaml` (a cause that needs several reads in one turn) | each validates (`evalrun -validate`); the injection scenario is in the hard-gate set of V10; labels name the expected cause and the forbidden actions | V0 |

## Step 7: security and local runs

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V16 | N6 | Opus 5.5 | 0.5 | injectscan: a rule that flags chat-template control tokens (`<\|im_start\|>`, `<\|im_end\|>`, `<\|eot_id\|>`, `<start_of_turn>`, `[INST]`, `<\|tool_call\|>` and the like) in untrusted text, with the normalisation the package already applies | recall and false-positive tests in the package's style (`cases_test.go`); no existing case changes verdict; the agent-side neutralisation is A4's, in V3a or a follow-up, by open decision 6 | V0 |
| V17 | A1 | Opus 5.5 | 0.5 | dev: an `llm` profile in `dev/docker-compose.yml` with Ollama and a documented tool-capable model pull; `devctl llm-up`; `dev/README` line | `docker compose --profile llm up -d` serves `http://localhost:11434/v1`; `evalrun -provider openai -flavor ollama -model <id> -only schema-change` runs end to end on the dev machine (output in the PR) | V3a |
| V18 | C4 | Opus 5.5 | 0.5 | docs/security: model providers section (data to each provider, keys, base URL and SSRF, template tokens, no cross-provider failover) | every claim cites a file; findings become V fix rows | V6, V7 |

## Step 8: admission and published docs

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V13 | A4 | Opus 5.5 | 0.25 per model | llm: add one model to `supported.json` (`provider`, `model`, `status`, `baseline`, `gated_at`) from its committed baseline. One PR per model, tag `[V13]` with the model in the title (`[V13] llm: admit openai gpt-…`) | the baseline file is on `main` and passes `-gate` (V12's check green); `status` is `supported` or `experimental` as the gate says | V10, that model's baseline PR |
| V14 | C14 | Opus 5.5 | 1 | docs/pilot: `models.md` (the `-models-table` output, the env block per provider, the residency line) and the per-provider table in `data-handling.md` (design §10.4) | citations per claim; ≤ 140 lines for `models.md`; the table matches `supported.json` on `main` | the first V13 |

## Load per agent

| Agent | PRs | Days | Critical for |
|---|---|---|---|
| A4 | 10 (+1 per admitted model) | 18.75 | the interface refactor, the guards, all four adapters, per-role selection |
| C6 | 3 | 5 | judge, baselines and gate, evals-live inputs |
| A5 | 2 | 2 | room errors, the models API |
| leads | 2 | 2.5 | the design, contracts |
| A6 | 1 | 1.5 | the model panel and picker |
| C1 | 1 | 1 | the evals-live matrix |
| C7 | 1 | 1 | scenarios |
| C14 | 1 | 1 | the published table |
| N6 | 1 | 0.5 | template tokens |
| A1 | 1 | 0.5 | Ollama profile |
| C4 | 1 | 0.5 | threat model |

Total 24 rows and about 34.25 agent-days before per-model admissions. The critical path is A4's: V0 → V1a → V1b →
V2a → V3a → V6 → (C6) V10 → V11 → (C1) V12, about 14 working days after P3-W2e; V2b, V3b, V4 and V5 sit beside it
on A4's queue, so A4's 18.75 days decide the end of the wave. The first non-Claude model can be gated after V10 and
admitted with V13 about three weeks after V0.

## Suggested weekly dispatch

| Week | Lands |
|---|---|
| 1 | V0; V1a; V15, V16 |
| 2 | V1b, V1c; V2a, V2b; V9 |
| 3 | V3a, V3b; V6; V17 |
| 4 | V4, V5; V7, V10; V18 |
| 5 | V8, V11, V12; first gate runs (OpenAI, Gemini; open decision 9); V13 per model; V14 |
