# C12 — Mirror each room into a Slack thread, outbound only (Claude cloud agent, Sonnet)

> **Status (lead, 2026-10-12 00:30 IST):** launched as a claude.ai/code session. The contract diff below is
> **stale** (config.go changed in 6283c9c): take settings through an `Options` struct; the lead adds
> `SLACK_BOT_TOKEN`, `SLACK_CHANNEL_ID` (empty = off) to `internal/config` at merge. Corrections: `Token` is a
> `netsec.Secret` (`Reveal()` only for the Authorization header); an untrusted text that is empty after cleaning
> becomes `(empty)` (Slack text objects need ≥ 1 character, ≤ 3000); pacing gate: reserve the slot under the mutex,
> unlock, then sleep — never sleep or retry while holding the lock; retry only 429 and 5xx (chat.postMessage is not
> idempotent, so no retry after a transport timeout); cite the docs.slack.dev pages for every method, field and limit.


Read `AGENTS.md`, `docs/collab.md`, `docs/api.md` and `internal/events/events.go` first. You own a **new** package:
`internal/slackbridge/**`. Edit nothing else. Do not wire it into `cmd/server`; A5 does that later. Do not edit
`internal/config`. The lead adds `PublicURL`, `SlackBotToken` and `SlackChannelID` before you start (see Work 6).

The cloud has no Slack workspace, no Docker and no `ANTHROPIC_API_KEY`. Write unit tests only: use `httptest` as the
fake Slack API and put a fake `events.Store` in a `_test.go` file. Add **no new dependency**. You call only one Slack
method, so plain `net/http` is smaller and easier to review than `slack-go`. Open one GitHub PR titled
`[C12] slackbridge: mirror rooms into a Slack thread` into `main`.

## Goal
On-call engineers work in Slack. For each incident room, post one parent message in a configured channel. Then reply in
that message's thread for the events that matter: what broke, the proposed fix and its params, who approved or
rejected it, what ran and how the room ended. Do not post tool-call noise. The bridge is outbound only. Approvals still
happen only in Piperoom. Slack has no buttons here, and nothing from Slack comes back in.

## Work
1. **Slack client** (`slack.go`, unexported). It sends `POST {APIBaseURL}/chat.postMessage` (default base
   `https://slack.com/api`) with a JSON body and these headers: `Authorization: Bearer <token>` and
   `Content-Type: application/json; charset=utf-8`. Slack requires JSON calls to send the token as a bearer header,
   never in the body or the query.
   - Body fields: `channel`, `text` (a short notification fallback, ≤ 300 bytes), `blocks`, `thread_ts` (replies
     only, and always the **parent** ts) and `reply_broadcast` (true only for `room.closed`). Always send
     `unfurl_links: false`, `unfurl_media: false` and `parse: "none"`, with no `omitempty`.
   - Use one `*http.Client` with `Timeout: 10 * time.Second`, and a `CheckRedirect` that returns
     `http.ErrUseLastResponse` so the bearer header is never sent to a redirect target. Use a 10 s `ctx` deadline per
     attempt. Build requests with `http.NewRequestWithContext`. Close every body. Decode through
     `io.LimitReader(resp.Body, 4<<20)`, as `internal/clients/connect/connect.go:121` does.
   - Slack replies with `{"ok":bool,"error":"<code>","channel":"C…","ts":"…"}`. If `ok` is false, return
     `*APIError{Code}`. Callers use `errors.As`, never string matching. If `ok` is true but `ts` is empty, return an
     error. Keep `ts` as a string and never parse it as a number.
   - **Retries.** On HTTP 429, wait for the `Retry-After` value in seconds (parse it with `strconv.Atoi`). If the header
     is missing or bad, back off 1 s, 2 s, 4 s and so on, up to 30 s per wait. Use the same backoff for HTTP 5xx,
     transport errors, and `ok:false` with `ratelimited` or `rate_limited`. Stop after 5 attempts. If `Retry-After` is
     more than 120 s, give up at once with a wrapped `ErrRateLimited`. Every wait must stop when `ctx` ends.
   - **Pacing.** Slack allows about 1 message per second per channel. Keep at least 1 s between posts across all
     rooms, using one gate in the client guarded by a mutex.
   - Inject time for tests with unexported fields `now func() time.Time` and
     `sleep func(context.Context, time.Duration) error`. Tests record the waits instead of sleeping.
   - **Never log or return the token.** Error strings name only the method, the HTTP status and the Slack code.

2. **Escaping and caps** (`escape.go`). Treat every string from an event as untrusted. That includes titles, actor
   names, reasons, hypothesis text, step params, action output and hand-off text.
   - Run `strings.ToValidUTF8`, drop control characters except `\n` and `\t`, then use one `strings.NewReplacer` to
     turn `&` into `&amp;`, `<` into `&lt;` and `>` into `&gt;`. Slack docs name only these three as control
     characters. Escaping them blocks `<!channel>`, `<!here>`, `<@U…>` and `<https://x|fake label>`.
   - Give every `mrkdwn` text object that holds untrusted text `"verbatim": true`, so Slack does not auto-link URLs,
     channel names or mentions in it.
   - Inside code blocks, replace each backtick in untrusted text with `'` so the text cannot close the block.
     (Slack has no escape for `*`, `_` or `~`. Stray bold or italic is cosmetic only, so accept it.)
   - Write `clip(s string, maxBytes int) string`: escape first, then cut on a rune boundary so the **escaped** text
     fits, and add `…`. Measure in bytes, which is a safe upper bound because Slack counts characters. Escape each
     untrusted piece, then put it into trusted markup. Never escape the whole message, because that breaks the room link.
   - Block Kit limits: a text object holds at most 3000 characters, a `context` block holds at most 10 elements, and
     a message holds at most 50 blocks. Use only `section` and `context` blocks with `mrkdwn` text objects.

3. **Rendering** (`render.go`). Decode payloads with `events.Decode[T]`. For the actor, use `e.Actor.Name`, or
   `e.Actor.ID` when the name is empty. The room link is `{PublicURL}/rooms/{url.PathEscape(roomID)}`, which matches
   the web route `/rooms/:roomId` in `web/src/App.tsx:12`. Show it as `<link|Open room>` only when `PublicURL` is set.
   | Event | Message |
   |---|---|
   | `room.created` | The **parent** message, with no `thread_ts`: `*[SEV2] <title>*` (title ≤ 200 bytes, severity upper-cased and clipped). A context block shows the pipeline, the room ID and the link. **Never** include `Alert` (the raw webhook body). |
   | `agent.hypothesis` | `Hypothesis (confidence: <c>[, confirmed])` plus the text (≤ 1500 bytes). Skip it if it is the same as the last hypothesis posted in this `Run`. |
   | `proposal.created` | Title and `proposal_id`, then `Needs approval from:` and each item of `required_layers`, then risk and rollback (≤ 500 bytes each). Add one section per step: `Step N` (1-based), action, layer, description, and `params` compacted with `json.Compact` in a code block (≤ 600 bytes). Show at most 10 steps, then `+K more steps — open the room`. End with a context block: `Approve or reject in Piperoom` plus the link. |
   | `approval.given` / `approval.rejected` | Who, which layer and which proposal. Use the title if this `Run` saw the proposal, else the ID. Add the rejection reason (≤ 500 bytes). |
   | `action.executed` | `Step {StepIndex+1} ran OK` or `FAILED` (`step_index` is 0-based on the wire, see `web/src/components/ProposalsPanel.tsx:159`), the proposal, and `Output` in a code block (≤ 500 bytes). |
   | `handoff.created` | From the actor to `To.Name`, then `WhatBroke` and `Why` (≤ 500 bytes each), then `Open` and `Risks` (at most 5 items each, ≤ 200 bytes per item). |
   | `handoff.accepted` | `<actor> accepted the hand-off and is now the driver.` |
   | `room.closed` | `Room closed by <actor>: <resolution ≤ 1000 bytes>`, with `reply_broadcast: true`. |

   Export `func Important(t events.Type) bool`. It returns true for exactly these 9 types. The bridge never posts any
   other type: `agent.status`, `agent.step.started`, `agent.tool_call`, `agent.tool_result`, `agent.message`,
   `comment.added`, `steer.*`, `control.*` and `trace.pinned`. Tool `Data` never leaves Piperoom.

4. **Thread state** (`threads.go`). Keep this interface small so A5 can store it in Postgres later:
   ```go
   type Thread struct{ Channel, TS string; LastSeq int64 } // LastSeq = highest seq handled
   type Threads interface {
       Get(ctx context.Context, roomID string) (Thread, bool, error)
       Put(ctx context.Context, roomID string, t Thread) error
   }
   func NewMemThreads() *MemThreads // mutex + map; the default
   ```
   Set `Channel` from the `channel` field of the parent post's response. Replies always go to the stored channel, so
   a room's thread stays where it started even if the config changes later.

5. **Bridge** (`bridge.go`). Expose `New(store events.Store, o Options) (*Bridge, error)` and
   `(*Bridge).Run(ctx, roomID string) error`. `Options` has these fields: `Token`, `ChannelID`, `PublicURL`,
   `APIBaseURL` (for tests only, and never read from config), `HTTPClient`, `Threads` (default `NewMemThreads()`) and
   `Logger` (default `slog.New(slog.DiscardHandler)`). Also add `FromConfig(cfg config.Config) Options`.
   - `New` returns `ErrDisabled` when both `Token` and `ChannelID` are empty. It returns a different error when only
     one of them is set, when `ChannelID` starts with `#` or holds spaces (it must be an ID, because channel names
     change), or when `PublicURL` is not an absolute `http` or `https` URL (check it with `url.Parse`). Trim a
     trailing `/` from `PublicURL`.
   - `Run` starts with `ctx, cancel := context.WithCancel(ctx)` and `defer cancel()`, so returning ends the
     subscription. The contract says the channel closes when `ctx` ends. Then it calls `Threads.Get` and
     `store.Subscribe(ctx, roomID, thread.LastSeq)`. A second `Run` for a room that is already running returns
     `ErrAlreadyRunning`, so the room never gets two parents.
   - For each event, skip it if `!Important(e.Type)`. Skip `room.created` if the room already has a thread. If there is
     no thread yet and the first important event is not `room.created`, first post a minimal parent:
     `Piperoom room <id>` plus the link. After each post, call `Put` with `LastSeq = e.Seq`. Return nil after posting
     `room.closed` or when the channel closes. Return `ctx.Err()` when `ctx` ends.
   - **Fatal Slack codes** stop this room's mirror. `Run` logs one `Error` line and returns the wrapped error. The
     codes are `invalid_auth`, `not_authed`, `token_revoked`, `token_expired`, `account_inactive`, `missing_scope`,
     `channel_not_found`, `not_in_channel` and `is_archived`. Any other failure, such as another code or spent
     retries, logs one `Warn` line, still advances `LastSeq` and goes on. Log lines hold only `room_id`, `seq`,
     `type` and the Slack code, never message text or the token.
   - The package doc comment explains three points. The bridge is outbound only. The channel should be **private**,
     because Slack reaches more people than the room. A crash between a post and `Put` can repeat one message on
     restart.

6. **Config.** Do not edit `internal/config/config.go`. The lead adds `PublicURL` (`PIPEROOM_PUBLIC_URL`),
   `SlackBotToken` (`SLACK_BOT_TOKEN`) and `SlackChannelID` (`SLACK_CHANNEL_ID`) before launch, all defaulting to
   `""`. If they are missing on your branch's `main`, write a local `Options` adapter and a `## Contract change request`.
   In the PR, describe the Slack app setup: a bot token with the `chat:write` scope, and the bot invited to the private
   channel (otherwise Slack answers `not_in_channel`).

## Tests
In each test, decode the request JSON before you assert on it. `encoding/json` escapes `&`, `<` and `>` as `&`
and so on, so raw byte checks give wrong results.
1. **Parent, then thread.** Feed the fake store one event of each of the 20 `events.Type` values, in seq order. Expect
   exactly 9 requests. The first has no `thread_ts` and uses the configured channel. The other 8 carry the parent `ts`
   that the fake returned (such as `"1700000000.000100"`) and come in seq order. `room.closed` is last and has
   `reply_broadcast: true`. `MemThreads` ends with that ts and `LastSeq` equal to the `room.closed` seq.
2. **`Important` table.** Check all 20 type constants. Exactly the 9 types in Work 3 return true.
3. **Escaping.** Use the title `<!channel> & <https://evil.example|click>`. The decoded text holds
   `&lt;!channel&gt; &amp; &lt;https://evil.example|click&gt;` and no `<!`. Every mrkdwn object with untrusted text has
   `verbatim: true`. A backtick in params cannot close the code block. Invalid UTF-8 and control bytes are cleaned out.
4. **Caps.** Build a proposal with 30 steps and 10 KB of params each. The message has ≤ 50 blocks, every text is
   ≤ 3000 bytes, and the message shows `+20 more steps`. Check `clip` on text full of `&` and on multi-byte runes. The
   escaped result always fits and never splits a rune.
5. **429 retry.** First reply is 429 with `Retry-After: 3`, then 200 → one recorded wait of 3 s and 2 requests.
   With no header → a 1 s wait. With `Retry-After: 600` → `errors.Is(err, ErrRateLimited)` after 1 request and no
   wait. With 5xx five times → an error after 5 attempts, with waits of 1, 2, 4 and 8 s. With `ctx` cancelled during a
   wait → `errors.Is(err, context.Canceled)`.
6. **Pacing.** Two posts in a row with the fake clock. The second one waits about 1 s.
7. **Token hygiene.** The fake answers `{"ok":false,"error":"invalid_auth"}`. `Run` returns an error, and `errors.As`
   gives the code `invalid_auth`. The request carried `Authorization: Bearer xoxb-test-…`, and the body has no `token`
   key. Neither the error string nor a `slog.NewTextHandler` buffer contains the token.
8. **Resume.** Set up `MemThreads` with a stored thread and `LastSeq: 5`. `Subscribe` is called with `afterSeq = 5`,
   no new parent is posted, and replies use the stored ts.
9. **`New` checks.** Test `ErrDisabled`, the half-configured case, a `#name` channel, a bad `PublicURL`, and
   `ErrAlreadyRunning`. Test that a redirect from the fake API is not followed.

## Done when
These all pass: `gofmt -l .` (empty), `go vet ./...`, `go build ./...` and `go test ./internal/slackbridge/...`.
`go test -race` passes too, if the toolchain supports it. Only `internal/slackbridge/**` changed. There are no new
modules in `go.mod`. The diff is about 900 changed lines or fewer, including tests. The PR lists the Slack app setup,
the limits used (with links to the Slack docs pages), and the A5 follow-ups: wiring `go bridge.Run(ctx, roomID)`
per open room in `cmd/server`, and a Postgres `Threads`.
