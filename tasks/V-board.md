# V wave dispatch board: one row = one PR

Owner decision of 2026-10-19: Piperoom runs on every major model provider, not only Claude (OpenAI, Azure OpenAI,
Google Gemini, DeepSeek, Qwen, Mistral, Groq, Together, OpenRouter, xAI, local Ollama and vLLM, and non-Claude models
on AWS Bedrock). Claude (Anthropic API, Bedrock, Vertex) stays fully supported and is the reference model. Design
`docs/design/model-providers.md` (rev 2 after the #681 reviews): four adapters (Anthropic, OpenAI-compatible,
Gemini, Bedrock Converse) behind one model interface, `internal/agent/llm`. Each row is a PR an agent finishes in
one or two sessions (target < 600 changed lines, §6 of `AGENTS.md`).

- **ID** is the PR tag (`[V3a] llm/openai: chat completions adapter`). The design PR is `[V0-design]`.
- **Model**: every row runs on Opus 5.5, cloud rows included (the owner's standing instruction for the Q and U
  waves, kept here). The ship workflow builds every row on Opus 5.5 anyway (`AGENTS.md` §6a).
- **Days**: one agent's working days including tests and review fixes.
- **After**: rows that must be merged first. P rows by their `tasks/P-board.md` ID; U rows by `tasks/U-board.md`.

**Timing: code rows start after P3 closes.** V0 is After **every P3 row still open** on `tasks/P-board.md` on
2026-10-19: P3c, P3d-1, P3d-2, P3e, P3f, P3g-1r, P3g-1s-a, P3g-1s, P3g-1c and P3-W2e. Every other V row is After V0,
directly or through another row, so nothing on this board is dispatched before P3 closes. V1a also names **P3e**
itself: P3e edits `internal/agent` and may change the prompt hash the golden test pins. The reference every model is
gated against is the 3-run Claude baseline of **V9r**, recorded after P3 closes (design §5.2). If a new P3 row is
added before P3 closes, the leads add it to V0's After column in the same PR.

**Order.** V0, then the golden capture (V1a-0) and the A4 refactor (V1a, V1b) that changes nothing for Claude, with
V1t and V9r giving the gate its metric and its reference; then the neutral guards (V2a, V2b), the adapters (V3a,
V3b, V4, V5) and per-role selection (V6); the evals rows (V9 to V12) run beside the adapters; each model is admitted
by its own gate run (V10) and an A4 line (V13). A4 carries most days and is the critical path; an agent with two rows
ready takes the one earlier in this order first.

**Claude unchanged.** V1a and V1b are refactors with no behaviour change: the golden request test (body, path,
query and headers, captured on `main` by V1a-0 before the refactor), the unchanged prompt hashes and today's tests
are their acceptance (design §13). The supported-model list, the fail-safe pricing rule and the start probe apply
only to the new providers; Claude on `anthropic`, `bedrock` and `vertex` accepts any model id, inference-profile id
or ARN as today (design §5, V6 test). No later row may change a Claude request except the control-token
neutralisation of V2a, which the owner ruled on for every provider by default (design §15, decision 6;
`PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS=non-claude` exempts Claude).

**Owner rulings (2026-10-19).** The owner ruled "configure all of them": the leads' recommendations are the defaults
and each is a setting where that is safe (design §15). The knobs are in the V0 items below and in the rows that
implement them; the safety floors (the metadata deny list, the untrusted-data wrapper, key handling, the hard
safety gates) are never configurable (design §15.1).

**Ownership and review tier.** No new folders: every row stays inside its agent's folders (`AGENTS.md` §2). The new
contract file `internal/agent/llm/llm.go` is the leads' (in `docs/process/ownership.json`, excluded from A4, and in
the ship workflow's `CONTRACT` regex); everything else under `internal/agent/llm/**` is A4's and is in the workflow's
`HIGH` tier (both rules landed with V0-design, #681, before any code row). **V5 adds one module**
(`github.com/aws/aws-sdk-go-v2/service/bedrockruntime`); `go.mod` and `go.sum` are in no agent's folders in
`ownership.json`, so V5 is built outside the ship workflow and merged after the §7 two-lead review (`AGENTS.md` §6a:
PRs made outside the workflow keep the two-lead review). Every other row leaves `go.mod` unchanged.
`internal/injectscan/**` is N6's (`AGENTS.md` §2, earlier waves). No N6 agent runs today: the ship workflow builds
V16 acting as N6 (its builder acts as the row's owner agent, §6a), or the leads dispatch it to whichever agent they
name in the thread; the board's Agent cell stays N6 because ship-check reads ownership from it.

## V0 items: the contract additions each row needs

Design §4 and §9, written by the leads in V0: the new contract file `internal/agent/llm/llm.go` (`Model`, `Request`,
`ToolDef`, `Message`, `Part`, `Native`, `ToolCall`, `ToolResult`, `StopReason`, `Response`, `Usage`, `Capabilities`,
`ErrorKind` with `truncated`, `APIError`, `Unretryable`, the `Provider*` constants); `events.AgentToolResultPayload.
Rejected` (`unknown_tool`, `invalid_json`, `invalid_input`); `events.AgentUsagePayload.Provider` and `.Estimated`;
the config variables of design §9 (`PIPEROOM_MODEL_PROVIDER` values `anthropic`, `bedrock`, `vertex`, `openai`,
`gemini`, `bedrock-converse`; `PIPEROOM_MODEL_FLAVOR`, `PIPEROOM_MODEL_BASE_URL`, `PIPEROOM_MODEL_API_KEY` and `_FILE`
(read by adapters, never stored in `Config`), `PIPEROOM_MODEL_CONTEXT_TOKENS`, `PIPEROOM_MODEL_CAPS`,
`PIPEROOM_MODEL_UNVERIFIED`, `PIPEROOM_MODEL_MAX_CONCURRENCY`, `PIPEROOM_MODEL_TPM`, `PIPEROOM_MODEL_PROXY`,
`PIPEROOM_GEMINI_BACKEND`, and the owner-ruled knobs of design §15: `PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP` (RFC 1918,
ULA and loopback only), `PIPEROOM_MODEL_ACK_CN_ENDPOINT`, `PIPEROOM_MODEL_PRICING_STRICT`
(`all`/`non-claude`/`off`), `PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS` (`all`/`non-claude`),
`PIPEROOM_POSTMORTEM_MODE` with `PIPEROOM_POSTMORTEM_MODEL` as a deprecated alias, and the reserved, unimplemented
`PIPEROOM_MODEL_PROMPT_VARIANT`; the role pairs
`PIPEROOM_TRIAGE_PASS_PROVIDER`, `PIPEROOM_SUMMARY_PROVIDER`/`_MODEL`, `PIPEROOM_JUDGE_PROVIDER`/`_MODEL`,
`PIPEROOM_<ROLE>_BASE_URL`) with `ParseModelRoles` and `ParseBaseURL`; the first-look model default moved from
`config.Load` into `ParseModelRoles`; `docs/api.md` (`agent.tool_result` `rejected`; `agent.usage` `provider` and
`estimated`; `GET /api/models`; `PUT /api/models` for standalone and quickstart only, with the key-binding and
`Origin` rules of design §11). The old unexported `model` seam is changed by V1a with its fakes, so V0 itself breaks
no fake.

## Step 0: contracts and the reference

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V0-design | leads | Opus 5.5 | 1 | docs: `docs/design/model-providers.md`, this board, the AGENTS.md wave section, `ownership.json` (the contract file to leads), the ship workflow's `CONTRACT` regex and `HIGH` entry for `internal/agent/llm` | merged; `node docs/process/ship-workflow.dryrun.mjs` passes; the open owner decisions of design §15 posted to the owner with the leads' recommendations | — |
| V0 | leads | Opus 5.5 | 1.5 | contracts: `internal/agent/llm/llm.go`, `AgentToolResultPayload.Rejected`, `AgentUsagePayload.Provider`/`.Estimated`, the config variables and parsers, `docs/api.md` | the V0 items in code with doc comments; the `CONTRACT` regex and the `HIGH` entry for `internal/agent/llm/` are on `main` (from V0-design) before V0 merges, so V1a is reviewed at high tier; `ParseBaseURL` table tests (https; http only for `localhost`, `127.0.0.0/8`, `::1`, or a private range with `PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP=on`; user info, query and fragment refused; metadata addresses refused always); `ParseModelRoles` defaults (each role falls back to the main provider and model; the first-look model defaults to `claude-haiku-5-5` only on a Claude provider, and a Claude deployment with nothing new set parses to today's values); the owner rulings of design §15 applied in config: `ParseModelKnobs` table tests (`PIPEROOM_MODEL_PRICING_STRICT=off` with `PIPEROOM_PIPELINE_DAILY_COST_USD` set refuses start; `PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS` accepts only `all` or `non-claude`; a non-empty `PIPEROOM_MODEL_PROMPT_VARIANT` refuses start with "reserved, not implemented"; a China-operated endpoint needs `PIPEROOM_MODEL_ACK_CN_ENDPOINT=yes`; `PIPEROOM_POSTMORTEM_MODE` read first, `PIPEROOM_POSTMORTEM_MODEL` accepted with a deprecation warning, the two set to different values refuse start; no knob reaches the metadata deny list); no new name collides with an existing variable; `go build`, `go vet`, `go test ./...` green; nothing calls the new code | P3c, P3d-1, P3d-2, P3e, P3f, P3g-1r, P3g-1s-a, P3g-1s, P3g-1c, P3-W2e, V0-design |
| V1t | A4 | Opus 5.5 | 0.25 | agent: set `AgentToolResultPayload.Rejected` on every tool call refused before it runs (`run.go` `failReadTool`: unknown tool, arguments not valid JSON, schema validation; the room tools' input checks) | one test per reason asserts the field on the emitted `agent.tool_result`; a tool that ran and failed has no `rejected`; Claude request bytes unchanged (the field is on the event, not in the prompt) | V0 |
| V9r | C6 | Opus 5.5 | 1 | evals: the `invalid_tool_calls` counter (per run and overall, from `rejected` on `agent.tool_result`; printed and compared like `misses`), then the 3-run Claude reference: `evals-live` on `main`, full CDC set, `runs: 3`, `make_baseline: true` (the owner approves the paid `evals` environment run); commit the report as `evals/baseline/default.json` | counter table-tested over fixture transcripts (each reason counted once, a tool error that ran not counted); the committed reference has `runs_per_scenario: 3`, the current prompt hash, and its `invalid_tool_calls` rate; `evals/README.md` explains the counter and that it is not `rejected_attempts` | P3-W2e, V1t |

## Step 1: Claude onto the interface, no behaviour change

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V1a-0 | A4 | Opus 5.5 | 0.5 | agent: golden capture of what `main` sends, alone, before any refactor: an `httptest` server records request body, method, path and query (`?beta=true`), and the `anthropic-beta` and `anthropic-workspace-id` headers, for a recorded room's main turn, triage turn, a turn after a compaction block, both first-look requests, the hand-off, the post-mortem and the judge; goldens in `internal/agent/testdata` | the goldens are produced by today's code and committed; the test passes on `main`; no non-test file changes | V0 |
| V1a | A4 | Opus 5.5 | 3 | agent + llm/anthropic: the investigation loop (`run.go`, `triage.go`, `toolparams.go`, `usage.go`, `budget.go`, `spend.go`, `model.go`, `endpoint.go`) on `llm.Model`; the Anthropic adapter for `anthropic`, `bedrock`, `vertex` (streaming, workspace header, beta compaction, cache breakpoints, strict caps as its `Capabilities`, the SDK's retries); `llm.APIError` with the kinds; the adapter conformance suite `internal/agent/llm/llmtest` | the V1a-0 goldens for the loop requests pass unchanged (body, path, query and headers); thinking and compaction blocks round-trip through `Native`; every `perRunMarkers` / `modelRejectionMarkers` case maps to the kind that gives today's decision (table test); `agent.PromptHash()` and the evals tap hash unchanged; every existing test in `internal/agent`, `internal/wiring`, `evals` passes and none is loosened or deleted (fakes that built `anthropic.BetaMessage` go through the adapter's decoder); a 3-run `evals-live` run is within noise of V9r's reference (design §5.2: no scenario regressed at `-tolerance 5`); `go.mod` unchanged | V1a-0, P3e, V9r |
| V1b | A4 | Opus 5.5 | 1.5 | agent: the single-shot callers on `llm.Model`: first look (`triage_pass.go`), hand-off and post-mortem (`handoff.go`, `postmortem.go`), the hand-off judge (`handoff_judge_claude.go` becomes provider-neutral, still behind the live-judge build tag), `verify.go`, `examples.go`; no file outside `internal/agent/llm/anthropic` imports `anthropic-sdk-go` | the V1a-0 goldens for the first look, hand-off, post-mortem and judge pass unchanged; `OutputSchema` maps to `output_config.format`; `grep -l anthropic-sdk-go internal/agent/*.go` (non-test) is empty; every existing test green | V1a |
| V1c | A5 | Opus 5.5 | 0.5 | room: `isUnretryable` uses `llm.Unretryable`; `modelRefused` deleted | the existing `unretryable_internal_test.go` cases pass unchanged in meaning (a 401, a 404 model, a 400 shape error stop; 408, 409, 413, 429, 5xx and "too long" retry), plus one case per new kind (`truncated` stops); no `anthropic` import left in `internal/room` (tests may keep one); the `PIPEROOM_POSTMORTEM_MODEL` mentions in `internal/room` and `cmd/server` log and comment text name `PIPEROOM_POSTMORTEM_MODE` (decision 5) | V1a |

## Step 2: the neutral guards

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V2a | A4 | Opus 5.5 | 1.75 | agent: control-token neutralisation in untrusted text by `PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS` (default `all`, design §10.1, decision 6); every tool input (room tools too) validated against its schema in code before `Run`, marked `rejected: invalid_input`; a tool call with invalid JSON arguments becomes an error `tool_result` (`rejected: invalid_json`) and stays paired; the structured-output fallback (forced tool `emit_<what>` with `ToolChoiceRequired`, else text with the first JSON object parsed) under the existing one retry | table tests per room tool for a missing required field, a wrong type, an extra field; a fake model without `StructuredOutput` gets a valid hand-off, first-look verdict and judge verdict through each fallback, and a second bad answer takes the existing failure path (`postmortem.failed`, triage `reason: model_error`); control tokens in a tool output, alert text and past incident are neutralised on every provider with `all` and kept on the Claude providers with `non-claude`, and the wrapper is present either way (tests); V1a-0 goldens unchanged (they hold no control tokens) | V1b |
| V2b | A4 | Opus 5.5 | 2 | agent: client-side compaction for a model without `Capabilities.Compaction` (design §6.1): the 70% trigger, the summary call with `compactionInstructions()`, the `<compacted_history untrusted="true">` wrapper, role `compaction` usage | a recorded long room on a fake model: step ids, the hypothesis, open proposals and the driver's last instruction survive; the last 6 messages and everything after the latest human input stay verbatim; untrusted text inside the summary stays wrapped; a `context_exceeded` stop after compaction still pauses for a human; never runs on a Claude provider (test) | V1a |

## Step 3: the adapters

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V3a | A4 | Opus 5.5 | 2.5 | llm/openai: chat completions over `net/http` (no SDK); flavors `openai`, `azure`, `ollama`, `vllm`, `generic`; message, tool and stop-reason translation (design §7.3, §3); usage normalisation (cached tokens out of input, reasoning in output) and the estimate when usage is missing; the per-turn truncation check (design §6.2); `reasoning_effort` mapping; the shared retry policy `internal/agent/llm/retry` and the per-endpoint concurrency and token budget, with `x-ratelimit-*` pacing (design §7.4); the base URL rules (plain `http` to RFC 1918, ULA and loopback only with `PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP=on`), deny list, dialer and proxy handling of design §10.3; the key as `netsec.Secret`, bound to its host | the `llmtest` conformance suite passes against recorded `httptest` fixtures per flavor (tool calls, parallel calls, a refusal, `content_filter`, Azure's 400 `content_filter` as a refusal, a 429 `insufficient_quota` as `billing` and not retried, a 503 retried twice then `overloaded`, `Retry-After` honoured and capped, a context-length 400 as `too_long`, missing usage as `Estimated`, a reported prompt far below the estimate as `truncated`); every `ToolResult.Content` byte-equal on the wire; a redirect is not followed; names resolving to `169.254.169.254`, `fd00:ec2::254`, `100.100.100.200` and `::ffff:169.254.169.254` are refused at dial; with `HTTPS_PROXY` set and `PIPEROOM_MODEL_PROXY` off the proxy is not used, and with it on a metadata target is refused before the CONNECT (test with a fake proxy); `APIError.Body` only at debug level; the key appears in no error, log line or event (capturing handler test); registry name and count tests in an external test package; `go.mod` unchanged | V1b, V2a |
| V3b | A4 | Opus 5.5 | 2 | llm/openai: hosted flavors `deepseek`, `qwen`, `mistral`, `groq`, `together`, `openrouter`, `xai`: default base URLs, key variables, reasoning rules (DeepSeek `reasoning_content`, Qwen `enable_thinking`), cache usage fields, OpenRouter `data_collection: deny`, the China-operated endpoints flagged for the start acknowledgement | one recorded fixture set per flavor in the conformance suite; DeepSeek's reasoning content handled by the rule its fixture proves; OpenRouter requests always carry the `provider` object (test); the flavor table in code matches design §3 or the PR says why and updates the design in a leads follow-up | V3a |
| V4 | A4 | Opus 5.5 | 2.5 | llm/gemini: `generateContent` over `net/http`; Gemini API key (`x-goog-api-key`) and Vertex (`PIPEROOM_GEMINI_BACKEND=vertex`, Google ADC through the existing `golang.org/x/oauth2/google`) with **two transports** (the deny-listed model transport, and the credential client that may reach the metadata server; design §10.3); `functionCall` / `functionResponse` with made ids mapped back by position; thought signatures kept as `Native` and returned; `thinkingConfig` from effort; `responseJsonSchema` for `OutputSchema`; schema conversion; the shared retry policy and rate limits; the truncation check | conformance suite on recorded fixtures (parallel calls, a `SAFETY` stop and a `promptFeedback.blockReason` as refusals, `MALFORMED_FUNCTION_CALL` as an error tool result, thoughts counted as output, cached tokens out of input, 429 and 503 retried, a quota 429 not retried); the `functionResponse` content compared JSON-decoded with `ToolResult.Content`; a test that the model transport refuses the metadata address while the token source still gets a token from a fake metadata server, and that no token request goes to the model host; a schema the converter cannot express fails start with the tool named; base URL rules as V3a; `go.mod` unchanged | V1b, V2a |
| V5 | A4 | Opus 5.5 | 2 | llm/converse: Bedrock Converse for non-Claude models through `bedrockruntime`, taking an `*aws.Config` (as `EndpointConfig.AWSConfig` does; the adapter never imports `internal/wiring`) or `AWS_BEARER_TOKEN_BEDROCK`; `toolUse` / `toolResult` (status `error`), `cachePoint` where the model supports it, `guardrail_intervened` and `content_filtered` as refusals; structured output through the forced tool; the SDK retryer set to design §7.4's bounds; the truncation check | **built outside the ship workflow** (adds `github.com/aws/aws-sdk-go-v2/service/bedrockruntime`; `go doc` output for every SDK symbol used in the PR, rule 9); conformance suite on fixtures through the SDK's endpoint override against `httptest`, throttling retried within the bounds; a Claude model id under `bedrock-converse` is refused at start with "use PIPEROOM_MODEL_PROVIDER=bedrock" | V1b, V2a |

## Step 4: per-role selection and the start checks

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V6 | A4 | Opus 5.5 | 2 | wiring + agent + cmd/investigate: one `llm.Model` per role (main and in-loop triage on the main provider; first look, summary and judge on their own provider); `internal/agent/llm/supported.json` (pinned ids, `alias`, `sunset`; design §5.4), checked **only on the new providers**; the start checks of design §9; the context-fit check and the one-request tool-call probe (design §5, §6.2); provider-qualified price keys and the fail-safe pricing rule by `PIPEROOM_MODEL_PRICING_STRICT` (design §8.2, decision 4); the China acknowledgement `PIPEROOM_MODEL_ACK_CN_ENDPOINT` (decision 7); the startup log (provider, model, region, cache yes or no, dropped parameters); `cmd/investigate -provider` | table test per start failure (unknown provider or flavor, missing key per role, bad base URL, a new-provider model not in `supported.json` without `PIPEROOM_MODEL_UNVERIFIED=on`, an unverified model that fails the probe or the context fit, an Ollama server that truncates in the probe, an unpriced new-provider model under a cost cap, a China-operated endpoint without `PIPEROOM_MODEL_ACK_CN_ENDPOINT=yes`, triage model on another provider, a sunset model); **Claude unchanged**: on `anthropic`, `bedrock` and `vertex` an unlisted id, a Bedrock inference-profile id, an application ARN and a versioned Vertex id all start as today with today's warnings, and an unpriced Claude model under a cost cap keeps today's warning with the default `non-claude` and refuses start with `all` (tests); `groq:<model>` wins over `<model>` in the price lookup; with nothing new set, the endpoint, its log line and every request are as before (V1a-0 goldens); the AST read-only test of `internal/wiring` still passes | V1b, V3a |

## Step 5: the API and the picker

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V7 | A5 | Opus 5.5 | 1.5 | httpapi + quickstart: `GET /api/models` (roles with provider, model, region, gate status, `prompt_cache`, price, sunset; the supported list with pass rates); `PUT /api/models` on standalone and quickstart only (loopback, the solo user), validating through V0's parsers, writing `.piperoom/model.env` 0600 through `os.Root` and rebuilding the endpoint; the key bound to provider, flavor and base URL host; the `Origin` check; the *no model* start state | `GET` never returns a key or a full base URL (host only; test); a team server answers `PUT` with `404`; a non-loopback request is refused; a `PUT` that changes provider, flavor or base URL without a key is `400 key_required` and no request leaves for the new host (test); a missing or foreign `Origin`, a cross-site `Sec-Fetch-Site`, or a non-JSON body is `403 origin` (tests); the key is excluded from request logging and in no event (capturing logger test); an invalid base URL or an unsupported model without `UNVERIFIED` is `400 validation`; a server with no usable model in standalone starts and serves the picker data; `docs/api.md` rows match | V6, U1-5c |
| V8 | A6 | Opus 5.5 | 1.5 | web: the read-only Model panel (settings and the room header popover: provider, model, region per role, gate status, cache, price, sunset, "unverified model" badge) and the standalone first-run picker (supported list with pass rate and cost per run, experimental behind a warning, the key in a secret field, re-asked when provider, flavor or base URL changes) | Vitest per state (team server read-only, standalone no model, picker submit, `400 key_required` and `400 validation` shown inline, unverified badge, sunset warning); the key field is never echoed and posts only to `PUT /api/models` from the same origin; mobile layout; a11y unchanged; `npm run lint`, `build`, `vitest` green | V7, U1-12a |

## Step 6: evals and CI

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V9 | C6 | Opus 5.5 | 1.5 | evals: the provider-neutral judge (`NewLLMJudge(llm.Model, model)`; `-judge llm`, `-judge claude` kept as its alias); judge cache entries record `provider:model` (key unchanged); the live-judge build tag `livejudge` with `claude` kept as an alias; `APIRefusal` from `llm.APIError` kinds | every existing judge test passes; a fake model without `StructuredOutput` judges through the fallback; recorded verdicts still load; `APIRefusal` returns the same answers for today's 401, 403 and "credit balance" cases | V1b, V2a |
| V10 | C6 | Opus 5.5 | 2 | evals: `-provider`, `-flavor`, `-judge-provider` flags; per-model baselines `evals/baseline/<provider>-<model>.json`; the report records provider, flavor, model, effort and the prompt hash kind; `-gate REF.json` with the hard gates and the noise-aware floors of design §5.1 and §5.2 (exit 4 and 5), printing each margin and `SE_diff`, its thresholds read from the checked-in `evals/gate.yaml` (decision 1: the owner-ruled defaults, changed only by a reviewed PR); `-models-table DIR` prints the published table; `evals/README.md` section | gate math table-tested on fixture reports: `invalid_tool_calls / tool_calls ≤ 2%` (not `rejected_attempts`), zero forbidden actions or params, zero injection failures, root-cause and layer floors at reference − max(0.10, 2·SE_diff) with the absolute minimums, pass rate reported but not gated while the reference is below 0.50, the 2-of-3 / 3-of-3 collapse rule; `-gate` refuses a 1-run reference, and a `gate.yaml` that sets a safety gate (forbidden actions or params, injection failures) above zero; `-models-table` over the committed baselines prints the reference first; an Ollama run documented with its exact command; the CDC replay set is unchanged | V9, V9r, V6, V2b |
| V11 | C6 | Opus 5.5 | 1.5 | evals: evals-live provider inputs: an in-process model tap for non-Anthropic providers (records the models called and the `neutral-sha256:` hash), the existing HTTP tap unchanged for `anthropic`; `evalrun` checks the role keys of the chosen provider before any work, as it checks `ANTHROPIC_API_KEY` today | a report from each adapter's fake records the right models and hash kind; the Anthropic tap hash is unchanged on a replay; a missing provider key fails before the first scenario with the variable named | V10, V3a |
| V12 | C1 | Opus 5.5 | 1 | ci: `evals-live.yml` `workflow_dispatch` inputs `provider`, `flavor`, `model`, and `gate_order` (the matrix order of gate runs, default `ollama,openai,gemini,deepseek,qwen`, decision 9); one secret per provider in the `evals` environment; compare against the model's own baseline; artifact `baseline-<provider>-<model>`; a matrix over `supported.json` for the nightly (left off, as today); a `ci.yml` check that every `supported.json` entry has a 3-run baseline that passes `-gate` against the current reference and that no entry's `sunset` has passed; README and `docs/runbook.md` link `docs/pilot/models.md` | a dispatch with each provider value runs or skips cleanly when its secret is missing; no PR run gets a secret (the existing fork and environment rules unchanged); the check fails on a fixture entry without a baseline and on a past sunset | V11 |
| V15 | C7 | Opus 5.5 | 1 | scenarios: `injection-template-tokens.yaml` (tool output holding chat-template control tokens and a fake provider tool-call syntax), `long-room-compaction.yaml` (a room long enough to trigger client-side compaction), `parallel-reads.yaml` (a cause that needs several reads in one turn) | each validates (`evalrun -validate`); the injection scenario is in the hard-gate set of V10; labels name the expected cause and the forbidden actions | V0 |

## Step 7: security and local runs

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V16 | N6 | Opus 5.5 | 0.5 | injectscan: a rule that flags chat-template control tokens (`<\|im_start\|>`, `<\|im_end\|>`, `<\|eot_id\|>`, `<start_of_turn>`, `[INST]`, `<\|tool_call\|>` and the like) in untrusted text, with the normalisation the package already applies. Executed by the ship workflow acting as N6, or the agent the leads name in the thread | recall and false-positive tests in the package's style (`cases_test.go`); no existing case changes verdict; the agent-side neutralisation is A4's, in V2a (decision 6) | V0 |
| V17 | A1 | Opus 5.5 | 0.5 | dev: an `llm` profile in `dev/docker-compose.yml` with Ollama started with `OLLAMA_CONTEXT_LENGTH=131072` (or the largest the documented model supports, at least 65536) and a documented tool-capable model pull; `devctl llm-up`; `dev/README` line | `docker compose --profile llm up -d` serves `http://localhost:11434/v1`; the V6 start probe passes against it (no truncation); `evalrun -provider openai -flavor ollama -model <id> -only schema-change` runs end to end on the dev machine (output in the PR) | V3a, V6 |
| V18 | C4 | Opus 5.5 | 0.5 | docs/security: model providers section (data to each provider, keys and their host binding, base URL, metadata deny list, proxies and the Vertex credential transport, template tokens, silent truncation, no cross-provider failover) | every claim cites a file; findings become V fix rows | V6, V7 |

## Step 8: admission and published docs

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| V13 | A4 | Opus 5.5 | 0.25 per model | llm: add one model to `supported.json` (`provider`, pinned `model`, `status`, `baseline`, `gated_at`, and `alias`/`sunset` when they apply) from its committed baseline; or set a `sunset` on an existing entry when the provider announces one. One PR per model, tag `[V13]` with the model in the title (`[V13] llm: admit openai gpt-…`) | the baseline file is on `main` and passes `-gate` (V12's check green); `status` is `supported` or `experimental` as the gate says; an alias is `experimental` | V10, V12, that model's baseline PR |
| V14 | C14 | Opus 5.5 | 1 | docs/pilot: `models.md` (the `-models-table` output, the env block per provider, the residency line, the minimum rate-limit tier, the Ollama and vLLM context settings) and the per-provider table in `data-handling.md` (design §10.4) | citations per claim; ≤ 140 lines for `models.md`; the table matches `supported.json` on `main` | the first V13 |

## Load per agent

| Agent | PRs | Days | Critical for |
|---|---|---|---|
| A4 | 12 (+1 per admitted model) | 20.25 | the golden capture, the refactor, the guards, all four adapters, per-role selection and the start checks |
| C6 | 4 | 6 | the invalid-call counter and the 3-run reference, judge, baselines and gate, evals-live inputs |
| leads | 2 | 2.5 | the design, contracts |
| A5 | 2 | 2 | room errors, the models API |
| A6 | 1 | 1.5 | the model panel and picker |
| C1 | 1 | 1 | the evals-live matrix |
| C7 | 1 | 1 | scenarios |
| C14 | 1 | 1 | the published table |
| N6 | 1 | 0.5 | template tokens |
| A1 | 1 | 0.5 | Ollama profile |
| C4 | 1 | 0.5 | threat model |

Total 27 rows and about 36.75 agent-days before per-model admissions. The critical path: the open P3 rows → V0 →
V1t → V9r (a paid 3-run run, about a day with the owner's approval) → V1a → V1b → V2a → V3a → V6 → V10 → V11 → V12 →
the first V13, about 17 working days after P3 closes; V1a-0 runs beside V1t and V9r, and V2b, V3b, V4 and V5 sit
beside the path on A4's queue, so A4's 20.25 days decide the end of the wave.

## Suggested weekly dispatch (after P3 closes)

| Week | Lands |
|---|---|
| 1 | V0; V1t, V1a-0; V9r; V15, V16 |
| 2 | V1a, V1b, V1c; V9 |
| 3 | V2a, V2b; V3a; V6 |
| 4 | V3b, V4, V5; V7, V10; V17, V18 |
| 5 | V8, V11, V12; first gate runs in the ruled order (Ollama, OpenAI, Gemini, then DeepSeek and Qwen; decision 9); V13 per model; V14 |
