# [C12] slackbridge: each incident room shows up as a Slack thread

## What
New package `internal/slackbridge` (only folder touched):
- `Bridge.Run(ctx, roomID)` subscribes to the room's event log and posts one root message (`room.created`) plus thread replies for hypotheses, agent messages, steps, proposals, approvals/rejections, executed actions, hand-offs, control changes, comments and close. Tool calls/results are not mirrored.
- `Client` — minimal Slack `chat.postMessage` client (10 s timeout, 4 MB response cap, 429 `Retry-After` retries, token never logged).
- `Handler` — Slack Events API endpoint: thread replies from mapped users become **comments only** (via a `CommentPoster` interface).
- `ThreadStore` + in-memory `MemThreads` (room ↔ thread ts, last mirrored seq).

## Why
Engineers live in Slack; the room should be visible and discussable there without a second control plane.

## How I tested
```
gofmt -l internal/slackbridge        # no output
go vet ./internal/slackbridge        # clean
go test -race ./internal/slackbridge # ok, no network or Docker
```
Tests cover: escaping/truncation/filtering, threading, echo suppression, Slack post failure not blocking the room, 429 retry and `ok:false`, signature (valid / wrong secret / stale timestamp), unmapped user, other channel, unknown thread, bot and edited messages, Slack retries, URL verification.

## Not done / known gaps
- `tasks/C12-slack-bridge.md` was not on `origin/main` when I worked, so I built from the lead brief and AGENTS.md; please check "Done when" against it.
- Not wired into `cmd/server` (A5 owns it); needs a `CommentPoster` and `UserMapper` there, a config entry (contract file) for token/signing secret/channel, and mounting the handler.
- `MemThreads` loses state on restart (room is re-mirrored into a new thread). A durable store is a follow-up.
- Mirroring is best effort: a failed Slack post is logged and skipped.
- Slack text only enters the room as a comment via the room service; the agent's untrusted-data wrapper (A4) applies when it reads comments. Slack cannot steer, take control or approve. Proposals say "approve in the room".
- Outbound text escapes `& < >` so room content cannot ping or build links.

## New dependencies
None (standard library only).

## Contract change request
Config (contract file) will need `SLACK_BOT_TOKEN`, `SLACK_SIGNING_SECRET`, `SLACK_CHANNEL` when the bridge is wired in; not edited here.
