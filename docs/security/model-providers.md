# Model providers: what leaves the process, and what guards it

Task V18 (KAN-293), C4, docs only. Against branch `wt/v18` (V0 to V7 on `main` @ `48a974f4` + `73b13a5c`).
Design: `docs/design/model-providers.md` (§7, §10, §11, §15.1). Where this file and the design differ, the
code wins; every claim below was read in the code and cites it as `path:line` or a function name. A claim the code
does not support is under **Findings**, not above it. Style as `docs/security/findings.md`.

Not covered: the Claude SDK transports beyond what is stated, secret redaction of tool output (N5,
`internal/redact`), the eval gate (V10), `docs/pilot/data-handling.md` (C14; it still lists only `anthropic`,
`bedrock` and `vertex` at lines 19-21, so V14 owns the per-provider table).

## 1. What data goes to each provider

Every provider named in `PIPEROOM_MODEL_PROVIDER` (and the role variables, `internal/config/config.go`
`ParseModelRoles`) receives the same request the agent builds, whichever adapter sends it:

- the system prompt, the tool definitions, the alert text with secrets masked (`internal/agent/alerttext.go`
  `maskAlertText`), steers, comments, compacted history;
- every tool result, as a string the agent has already wrapped: `<tool_output ... untrusted="true">` with `&`, `<`, `>`
  escaped (`internal/agent/run.go` `wrapToolOutput`, `prompt.go` `escapeToolText`), after the injection scan
  (`echoguard.go` `scanRead`). Tool results hold topic names, connector configs, schema fields and, from
  `trace_record`, record values. Choosing a provider makes it a processor of that data.
- Adapters send those strings unaltered (design §10.1). The conformance tests per adapter
  (`internal/agent/llm/*/conformance_test.go`) are what hold them to it; this review did not re-run them.

By provider, from the code:

| Provider | Where the request goes | Notes |
|---|---|---|
| `anthropic`, `bedrock`, `vertex` (Claude) | the SDK's endpoints (`internal/agent/llm/anthropic/client.go`) | reference; SDK transport, section 4 |
| `openai` flavors | the flavor's default base URL or `PIPEROOM_MODEL_BASE_URL` (`config.FlavorDefaultBaseURL`, `ParseBaseURL`) | hosted flavors need their key; `ollama` takes none |
| `openai` flavor `openrouter` | OpenRouter, which forwards to third parties | the adapter always sends `provider.data_collection = "deny"` (`openai/encode.go:205`) |
| `gemini` | `generativelanguage.googleapis.com`, or Vertex `<region>-aiplatform.googleapis.com` (`gemini.go` `vertexBase`) | Vertex keeps data in the chosen region |
| `bedrock-converse` | the regional Bedrock endpoint | **not wired**: `EndpointConfig` (`internal/wiring/model.go:22`) never sets `Converse`, so start refuses it (`internal/agent/roles.go:149`) |

China-operated endpoints (DeepSeek, DashScope, other listed PRC hosts, any `.cn` host) start only with
`PIPEROOM_MODEL_ACK_CN_ENDPOINT=yes`, checked twice: `config.ParseModelKnobs` (`config.go:2258`) and
`clientBuilder.checkChina` (`roles.go:244`, on the adapter's own answer). The host list is best effort
(`ChinaOperatedEndpoint`, `config.go:2374`): an IP literal or an unlisted provider passes, so the acknowledgement is a
reminder, not a compliance control. Nothing switches provider on its own (section 9).

## 2. Keys

| Provider | Variable | Read by |
|---|---|---|
| Claude (`anthropic`) | `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN` | the Anthropic SDK itself |
| `openai`, main role | `PIPEROOM_MODEL_API_KEY`, else the file `PIPEROOM_MODEL_API_KEY_FILE` (64 KiB cap), else the flavor's variable | `openai.LoadKey` (`openai/key.go:20`) |
| `openai`, other role | the flavor's variable only (`KeyEnv`, `openai/flavors.go:108`) | same, `main=false`: never the main provider's key |
| `gemini` (API) | `PIPEROOM_MODEL_API_KEY` / `_FILE` for the main role, else `GEMINI_API_KEY` | `roles.go` `loadKey` |
| `gemini` (Vertex), `vertex`, `bedrock`, `bedrock-converse` | no key: Google ADC, AWS chain, or `AWS_BEARER_TOKEN_BEDROCK` | SDKs |

How a key stays with its host:

- **Request path.** The key is a `netsec.Secret`, whose `String`, `GoString`, `LogValue` and `MarshalText` redact
  (`internal/clients/netsec/netsec.go:37`). OpenAI's `keyTransport.RoundTrip` (`openai/transport.go:201`) and Gemini's
  `hostGuard` (`gemini/transport.go:141`) refuse any request whose scheme or host differs from the configured base
  URL, and the key header is added only inside them. Redirects are never followed (`openai/transport.go:185`,
  `gemini/transport.go:33`).
- **Errors and logs.** `APIError.Body` is redacted (pattern, plus the key literally) and capped at 2 KiB: OpenAI
  `safeBody` (`openai/errors.go:210`), Gemini `scrub` (`gemini/gemini.go:333`), Converse `scrub`
  (`converse/errors.go:126`). OpenAI and Gemini write the body at debug level only (`openai/errors.go:135`); the room
  sees the kind and the status. `openai.Model` prints `openai(<flavor> <origin>)` under `%v`, `%+v`, `%#v` and slog
  (`openai/openai.go:332-338`), and `openai/security_test.go:340` greps the error, the log and the formatted
  transports. Gemini has the same test for the model (`gemini/transport_test.go:174`) but not for its transport: see
  MP5.
- **Picker binding.** `PUT /api/models` stores the key against provider, flavor and the whole normalised base URL
  (scheme, host, port, path: `quickstart/models.go:245` `binding`). A change to any of them without `key` is
  `400 key_required` before anything is built (`models.go:136-139`). The binding covers the main role only: MP6.
- **At rest.** `.piperoom/model.env` is written through `os.Root`, temp file then rename, mode 0600, directory 0700,
  links refused (`quickstart/modelenv.go:181`, `openStateDir`). The reader checks the file is regular and under
  64 KiB but not its mode (`ReadModelEnv`, `modelenv.go:114`). The route is kept out of request logging
  (`httpapi/server.go:399`, `models.go:256`), and a rejected PUT scrubs the key and every base URL from its message
  (`quickstart/models.go` `scrub`). The panel shows `key_set` and the base URL host only (`httpapi/models.go`
  `BuildModelPanel`).
- **To the adapters.** The picker key reaches them as a process environment variable: MP1.

## 3. Base URL rules

`config.ParseBaseURL` (`config.go:1968`) is the one parser for `PIPEROOM_MODEL_BASE_URL` and the role variables:
`https` only; `http` only to `localhost`, `127.0.0.0/8`, `::1`, or, with `PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP=on`, RFC 1918
and ULA (`ModelPrivateHTTPAddr`, `config.go:1924`); no user info, query, fragment or IPv6 zone; non-canonical numeric
hosts (`2852039166`, `0xA9FEA9FE`) refused; the names `metadata.google.internal` and `metadata` refused; IP
literals checked against the deny list below. Errors name the variable, never the value. The Gemini and OpenAI
adapters run the same parser again on their default URL or the one given (`gemini.go:177`, `openai.go:166`), and
`checkBaseEndpoint` (`converse.go:198`) does for Converse. The Claude providers and the Anthropic API endpoint do not
go through it: MP9.

## 4. The metadata deny list

`config.ModelAddrDenied` (`config.go:1897`), no setting widens it (design §15.1; `ParseModelKnobs` has none):

- link-local `169.254.0.0/16` and `fe80::/10` (AWS, GCP, Azure, Oracle IMDS and ECS task metadata), multicast,
  unspecified, `0.0.0.0/8`, `255.255.255.255`;
- AWS IMDS over IPv6 `fd00:ec2::254` and Alibaba `100.100.100.200`;
- IPv4-mapped, IPv4-compatible (`::/96`), NAT64 `64:ff9b::/96` and `64:ff9b:1::/48`, and 6to4 `2002::/16` are
  judged by the IPv4 address they carry.

`ModelPrivateHTTPAddr` never admits an address the deny list refuses.

**Where it is enforced, per adapter:**

| Adapter | Enforcement | Cite |
|---|---|---|
| OpenAI-compatible | the dialer resolves the name, refuses the whole name if one address is denied, dials the checked literal addresses, and `ControlContext` checks the address the kernel connects to once more | `openai/transport.go` `guard.resolve`, `dialContext`, `control` (`:149`) |
| Gemini | `net.Dialer.Control` runs `dialAllowed` on the address actually dialed, so a name that resolves to metadata fails at connect | `gemini/transport.go:42`, `:73` |
| Converse | base endpoint checked once at start by `ParseBaseURL`; no dial hook; the endpoint is the SDK's regional host and `BaseEndpoint` is never set from configuration | `converse.go:164`, `:198` |
| Claude (`anthropic`, `bedrock`, `vertex`) | none; SDK default transports | MP9 |

## 5. Proxies

Both new-provider transports set `Proxy: nil` unless `PIPEROOM_MODEL_PROXY=on` (`openai/transport.go:151`,
`gemini/transport.go:87`), because through a proxy the dial hook sees only the proxy's address and the proxy resolves
the target. With the knob on, the transport's `Proxy` function resolves the target first and refuses it when the name
or any address is denied, then returns the environment's proxy (`openai/transport.go:168`; Gemini
`checkedProxy`, `gemini/transport.go:107`). OpenAI skips the proxy for a plain-http endpoint. The proxy then resolves
the name again: DNS rebinding between the two lookups is a residual risk (code comments say so, `openai/transport.go:170-173`).
Operators must deny metadata addresses at the proxy too. The SDK transports of the Claude providers and of Converse
honour `HTTPS_PROXY` whatever the knob says (the AWS SDK default client sets `Proxy: http.ProxyFromEnvironment`; the
Anthropic SDK uses `http.DefaultClient`).

## 6. Vertex credential transport (Gemini on Vertex)

Two transports. The model client is the deny-listed one, wrapped by `oauth2.Transport` which adds the token
(`gemini.go:199`, `Transport: &oauth2.Transport{Source: ts, Base: rt}`) and is itself behind `hostGuard`, so a
request to any host but the configured one is refused. The token source is built by `defaultTokenSource`
(`gemini/transport.go:166`) with a separate `*http.Client` placed under the `oauth2.HTTPClient` context key, which on
GCE must be able to reach the metadata server. If the caller supplies none, that client is a clone of
`http.DefaultTransport` with a 30 s timeout: no deny list and the environment's proxy, by design, since it only
talks to Google's token endpoints. `TestVertexTwoTransports` (`gemini/transport_test.go:250`) covers both sides. The
`vertex` provider (Claude) uses the SDK's own credential handling (`anthropic/client.go` `VertexOption`).

## 7. Template and control tokens

Open-weight models run behind chat templates whose special tokens are plain strings. `neutralisingModel`
(`internal/agent/controltokens.go:84` `withNeutraliser`) rewrites them as `(ctl:NAME)` in every user text part and
tool result before the adapter sees the request, in the raw form and in the `&lt;...&gt;` form `escapeToolText`
leaves. It is applied to the main model and to every role built separately (`endpoint.go:369`, `:428`). Default `all`
(every provider); `non-claude` exempts the Claude providers; there is no off
(`ParseModelKnobs`, `parseNeutraliseMode`). It does not touch the system prompt, the tool definitions or assistant
turns. The match list is fixed (pipe tokens, a named-tag list, `<<SYS>>`, Mistral brackets): MP8.

## 8. Silent truncation

Some servers (Ollama at its default `num_ctx`) cut an over-long prompt without an error, and the system prompt with
the untrusted-data rules is what would go. Two checks, both from the design:

- **Start** (`internal/agent/startcheck.go`): for every role on a new provider, `checkFit` (`:261`) requires system
  prompt + marshalled tool definitions (3 bytes per token) + max output + a 16k headroom to fit the context window,
  else start is refused naming the variable. For `ollama`, `vllm` and `generic` (`localFlavor`) `probe` (`:305`) also
  sends the real system prompt and tools and fails start when the reported prompt tokens are under half of
  bytes/4. Other hosted models are probed only when they are not in `supported.json`.
- **Every turn**: OpenAI `truncated` (`openai/openai.go:271`, called at `:318`), Gemini (`gemini/decode.go:216`,
  used at `gemini.go:293`) and Converse (`converse/transport.go:120`) return `APIError{Kind: ErrTruncated}` when the
  reported prompt is under half of request bytes/4. `ErrTruncated` is unretryable (`llm.Unretryable`, `llm.go:283`),
  so the run stops. An estimated usage skips the check. The ratio is coarse: MP7.

## 9. No cross-provider failover

Each role is built once, from one `config.ModelRole`, into one `llm.Model` (`endpoint.go`, `clientBuilder.build`);
a role on the main provider shares the main client (`sameEndpoint`, `roles.go`). The retry policy (`internal/agent/llm/retry`) repeats a request on the same
endpoint only; `ErrAuth`, `ErrBilling`, `ErrModelNotFound`, `ErrBadRequest` and `ErrTruncated` stop the run. No code
path constructs a second provider for a failed one (the word "failover" in `agent/run.go:294` is the room's agent
failover, unrelated). Moving incident data to a different processor is always an operator decision.

## 10. The V7 models API (`GET`, `PUT /api/models`)

`httpapi/models.go`, `quickstart/models.go`, `cmd/server/models.go`:

- `GET` returns the panel on every server: provider, flavor, model, base URL **host**, region, gate status, price,
  `key_set`; never a key (`BuildModelPanel`). A team server's `PUT` is `404` (`teamModels.Editable`,
  `modelsEditable`, checked before login).
- `PUT` checks run in this order (`mountModelsPut`, `models.go:143`): editable, loopback peer by the socket address
  only (`modelsLoopback`, a forwarded-for header is ignored), `Origin` (`modelsOrigin`), login, viewer guard, then the
  handler, which requires `userFrom == SoloUser()` (`handlePutModels:163`; the solo user is the only user of the
  server, `cmd/server/models.go:174`).
- **Origin** (`sameOriginJSON`, `models.go:236`): exactly one `Origin` header equal to `scheme://Host`; a
  `Sec-Fetch-Site` other than `same-origin` refused when sent; `Content-Type: application/json` required. Each is
  `403 origin`, before the body is read. Because the Origin is compared with the `Host` header, a rebound DNS name
  would pass it, which is what the global `hostGuard` (`httpapi/auth.go:73`, `server.go:145`, 421 `bad_host`) stops;
  the PUT route has no second Host check of its own.
- **`key_required`**: `Apply` refuses a change of the main role's provider, flavor or base URL without `key` before it
  builds anything (`models.go:136`); a new key is validated for control characters and 8 KiB, written to the
  environment, the endpoint is built (start checks, possibly one probe), then `model.env` is written and the endpoint
  swapped; any failure restores the old environment (`setKey`).
- Unsupported models on a new provider are refused unless `PIPEROOM_MODEL_UNVERIFIED=on` (`gate`).

## Findings

Severity as in `findings.md`. Each is a proposed V fix row; none is fixed by this change.

### MP1 — The picker key is handed to the adapters through `os.Setenv`
- **Severity:** medium. **Where:** `cmd/server/models.go:123-141` (`osEnv`, `setStoredKey`),
  `quickstart/models.go:271` (`setKey`).
- **Scenario:** the key lives in the process environment from then on: global mutable state (AGENTS.md §4 rule 5),
  readable via `/proc/<pid>/environ`, inherited by any child started with the default environment, and racing with
  any goroutine that reads the variable. Switching from Anthropic to another provider leaves the old
  `ANTHROPIC_API_KEY` set for the life of the process (`setKey` unsets only `PIPEROOM_MODEL_API_KEY(_FILE)`). At rest,
  `ReadModelEnv` accepts a `model.env` that is not 0600.
- **Fix:** give the factories the key as a `netsec.Secret` in the endpoint config (per role), not via the
  environment; unset the previous key on every switch; refuse a group- or world-readable `model.env` on read.
- **Row:** `[V-fix] model keys: pass the picker key to the adapters as a value, not os.Setenv`

### MP2 — Proxy mode checks the target, then the proxy resolves it again
- **Severity:** medium (needs `PIPEROOM_MODEL_PROXY=on` and an attacker-influenced DNS name).
  **Where:** `openai/transport.go:168-178`, `gemini/transport.go:107-130`.
- **Scenario:** the pre-check and the proxy's own lookup are two lookups; a name that answers a public address to
  Piperoom and `169.254.169.254` to the proxy sends the request (and the key) to metadata. Documented as residual.
- **Fix:** in proxy mode accept only the flavor's built-in hosts or an operator allow-list
  (`PIPEROOM_MODEL_PROXY_HOSTS`), so no free DNS name is used; keep the doc line telling operators to deny metadata at
  the proxy.
- **Row:** `[V-fix] llm proxy mode: restrict to known provider hosts instead of re-resolving`

### MP3 — The guarded dialer exists twice (openai and gemini)
- **Severity:** low (drift risk). **Where:** `openai/transport.go` (`guard`, `keyTransport`) and
  `gemini/transport.go` (`dialAllowed`, `checkedProxy`, `hostGuard`), plus a third `metadataHostNames` in
  `config.go:1862`.
- **Scenario:** the two already differ: OpenAI resolves and dials literals, Gemini relies on `Control` only; OpenAI
  uses `http.ErrUseLastResponse`, Gemini an `errRedirect`; the proxy path forces port 443 only in Gemini. A fix to one
  does not reach the other.
- **Fix:** one `internal/agent/llm/netguard` (A4): deny-listed dialer, checked proxy, host-bound key transport,
  used by both, with one test table.
- **Row:** `[V-fix] llm: one shared netguard for the openai and gemini adapters`

### MP4 — A transient AWS credential failure in Converse is unretryable auth
- **Severity:** medium (availability; Converse is not wired yet, so it applies when V6 wires it).
  **Where:** `converse/transport.go:144` (`markedCredentials.Retrieve`), `converse/errors.go:88-93`.
- **Scenario:** every `Retrieve` error becomes `ErrAuth`, which `llm.Unretryable` stops on. An IMDS or ECS-credentials
  timeout or 5xx during a turn fails the run for good; the test (`review_test.go:21`) covers only "no role found".
- **Fix:** classify by cause: deadline, `net.Error` timeout, and 429/5xx from the credential endpoint as `ErrNetwork`
  (still not sent, retried); no provider, 4xx and expired token stay `ErrAuth`.
- **Row:** `[V-fix] converse: a transient credential retrieval failure is network, not auth`

### MP5 — Gemini's `apiKeyTransport` prints the key under `fmt`
- **Severity:** low. **Where:** `gemini/transport.go:152`.
- **Scenario:** verified with a throwaway test: `%v`, `%+v` and `%#v` of `*apiKeyTransport` print the key, because
  fmt cannot call `Secret.String` on an unexported field. Not reachable today (the model holds it behind a pointer),
  but OpenAI's `keyTransport` has `String`/`GoString` for this reason.
- **Fix:** add `String`, `GoString` and `LogValue` to `apiKeyTransport` and a formatted-transport case to
  `TestKeyNeverLeaks`.
- **Row:** `[V-fix] gemini: apiKeyTransport redacts itself under fmt`

### MP6 — `key_required` binds the main role only
- **Severity:** low (the solo user, same origin). **Where:** `quickstart/models.go:135-139`, `binding`.
- **Scenario:** a role on another provider (`roles.summary`, `judge`, `first_look`) reads its key from the standard
  variable (for example `OPENAI_API_KEY`); `PUT` may set that role's `base_url` to a new host and the next start
  check or turn sends that key there, with no `key_required`.
- **Fix:** bind every role (provider, flavor, base URL) and refuse a role endpoint change when its standard key is set,
  unless the role carries its own key in the request.
- **Row:** `[V-fix] PUT /api/models: bind every role's key to its endpoint`

### MP7 — The truncation test only catches a cut past one half
- **Severity:** low. **Where:** `openai/openai.go:271`, `gemini/decode.go:216`, `converse/transport.go:120`,
  `startcheck.go:341`.
- **Scenario:** a server that keeps 55% of the prompt reports 55% of bytes/4 and passes; the lost part can be the
  system prompt's untrusted-data rules. Hosted, listed models get no start probe.
- **Fix:** calibrate tokens-per-byte from the start probe's real prompt and use that, with a margin, as the per-turn
  floor; for local flavors keep the explicit `PIPEROOM_MODEL_CONTEXT_TOKENS` requirement.
- **Row:** `[V-fix] llm truncation: per-turn floor calibrated from the start probe`

### MP8 — The control-token list is fixed
- **Severity:** low. **Where:** `controltokens.go:34-37`.
- **Scenario:** sentinel tokens outside the three regex families (T5 `<extra_id_0>`, `<unused0>`, Alpaca-style
  `### Instruction:` headers) pass through on a server that does not escape them.
- **Fix:** extend the list with the V16 scan, and let the start probe add the served tokenizer's special tokens.
- **Row:** `[V-fix] controltokens: cover sentinel tokens and the served model's special tokens`

### MP9 — Claude providers and Converse have no dial-time deny list
- **Severity:** low (operator configuration). **Where:** `anthropic/client.go` (SDK client; the SDK reads
  `ANTHROPIC_BASE_URL` in its `client.go`), `converse/converse.go:164`.
- **Scenario:** `ANTHROPIC_BASE_URL` is not run through `ParseBaseURL` and the key goes to whatever it names;
  Converse's `BaseEndpoint` check is by name only, so a hostname that resolves to metadata passes (unused in
  production wiring today).
- **Fix:** validate `ANTHROPIC_BASE_URL` with `ParseBaseURL` at start; give Converse the shared guarded `HTTPClient`
  (MP3) before any endpoint option is wired.
- **Row:** `[V-fix] anthropic and converse: validate the endpoint and use the guarded transport`
