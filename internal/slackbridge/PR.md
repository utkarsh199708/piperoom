# [C12] slackbridge: mirror rooms into a Slack thread

[C12] READY FOR REVIEW

## What
New package `internal/slackbridge` (only folder touched; plus a merge of `main` for `netsec`). Outbound only: one parent message per room, then thread replies for the 9 important event types (`Important()`): room.created (parent), agent.hypothesis, proposal.created, approval.given/rejected, action.executed, handoff.created/accepted, room.closed (`reply_broadcast`). Tool calls/results, chatter, steering, control and traces never go to Slack; the raw alert is never rendered.
- `slack.go` — `chat.postMessage` over `net/http`; `Authorization: Bearer` (token is `netsec.Secret`, `Reveal()` only for that header); no redirects; 10 s per attempt; 4 MB response cap; `*APIError{Code}`.
- `escape.go` — `clean` + `&<>` escaping, `clip` (escaped result fits, rune-safe, empty → `(empty)`), backtick-safe code blocks.
- `render.go` — Block Kit (`section`/`context` only), `verbatim:true` on all untrusted text; caps from the task file.
- `threads.go` — `Threads` interface + `MemThreads`. `bridge.go` — `New(store, Options)`, `Run`, `ErrDisabled`, `ErrAlreadyRunning`, fatal Slack codes stop a room.

## Why
On-call engineers work in Slack; approvals stay in Piperoom.

## How I tested
```
gofmt -l internal/slackbridge          # (empty)
go vet ./internal/slackbridge          # clean
go build ./...                         # ok
go test -race -count=5 ./internal/slackbridge   # ok, no network/Docker
```
Tests (httptest fake Slack + fake store): all 20 event types → exactly 9 posts, parent then thread in seq order; `Important` table; escaping/verbatim/code-fence/UTF-8; caps (30 steps × 10 KB → ≤50 blocks, `+20 more steps`, ≤3000 bytes); `clip`; 429 with/without `Retry-After`, 600 s → `ErrRateLimited`, 5xx ×5 (waits 1,2,4,8 s), cancel during wait, **no retry after transport error**; 1 s pacing; token hygiene (header only, absent from body, errors and logs); fatal vs non-fatal codes; resume from stored `LastSeq`/channel; hypothesis de-dup; `New` validation; `ErrAlreadyRunning`; redirect not followed.

## Not done / known gaps
- Diff is ~1400 lines (≈550 non-test) against the ~900 target; most is tests.
- `FromConfig(cfg config.Config)` is **not** written: `config.Config` has no Slack/PublicURL fields on `main` and I may not edit it. A5 maps config → `Options` once the lead adds them.
- `ok:false` `ratelimited`/`rate_limited` is retried like 429 (Slack did not accept the message); transport errors/timeouts are not (non-idempotent).
- Pacing is one gate for all rooms (reserve slot under mutex, sleep outside it).
- `MemThreads` forgets on restart (new parent). A crash between post and `Put` can repeat one message.
- Earlier commits on this branch had an inbound Slack→comment design; it contradicted the task file and is removed here.

## New dependencies
None.

## Contract change request
`internal/config/config.go`: add `PublicURL` (`PIPEROOM_PUBLIC_URL`), `SlackBotToken` (`SLACK_BOT_TOKEN`, ideally `netsec.Secret`), `SlackChannelID` (`SLACK_CHANNEL_ID`), all default `""` (empty = bridge off).

## Slack app setup and A5 follow-ups
Create a Slack app with a bot token and the `chat:write` scope; use a **private** channel and invite the bot (else `not_in_channel`); set `SLACK_CHANNEL_ID` to the channel ID (not `#name`).
Docs: [chat.postMessage](https://docs.slack.dev/reference/methods/chat.postMessage) (arguments, errors), [rate limits](https://docs.slack.dev/apis/web-api/rate-limits), [text object](https://docs.slack.dev/reference/block-kit/composition-objects/text-object) (1–3000 chars), [formatting/escaping](https://docs.slack.dev/messaging/formatting-message-text).
A5: `go bridge.Run(ctx, roomID)` per open room in `cmd/server`; Postgres-backed `Threads`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01MGYNND1isLEukvHPBKS42Q
