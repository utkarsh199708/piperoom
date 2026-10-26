# Slack bridge

The bridge mirrors each incident room into one thread in one Slack channel. It is
**outbound only**: nothing typed in Slack reaches a room, and approvals happen only in
Piperoom.

- **Mirrored:** room opened (severity, title, pipeline id, link to the room when a public
  URL is set), agent hypotheses, proposals, approvals and rejections, executed actions,
  hand-offs created and accepted, room closed.
- **Never mirrored:** tool calls and tool `Data`, the raw alert payload, agent status and
  messages, comments, steering, control events, pinned traces.

## Turning it on

The server starts the bridge when `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID` are both set
(`cmd/server/slack.go`, `internal/config/config.go`); with neither set it logs
`slack bridge disabled` and runs without it. Setting only one of the two is a start-up
error, not "off": the server exits with
`level=ERROR msg="server: failed" err="server: slack bridge: slackbridge: token and channel id must be set together"`.
`PIPEROOM_PUBLIC_URL`, when set, adds a link to the room in each thread's first message.

## Slack app setup

1. Create a Slack app with a bot token that has only the **`chat:write`** scope.
2. Use a **private** channel — Slack reaches more people than the room does, and the thread
   repeats proposals and hand-offs. Invite the bot to it.
3. Note the channel **ID** (like `C0123ABC`, from the channel details), not its `#name`;
   the bridge rejects names.

Give the server the token and channel ID (and the public web URL for room links) through
the env vars above. Operations (token rotation, turning it off, error codes) are in
[`docs/runbook.md`](../runbook.md).
