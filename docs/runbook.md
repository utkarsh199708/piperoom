# Dev stack runbook

Day-to-day operation of the local CDC pipeline (`dev/docker-compose.yml`) used to develop
and test Piperoom. See `README.md` for the one-line versions and `dev/scenarios.md` for
what each failure scenario looks like and how to fix it.

## Start / stop

```sh
go run ./cmd/devctl lock --owner <tag>     # take the shared stack lock first (see below)
go run ./cmd/devctl up --owner <tag>       # start the stack, register the connector, start devconsumer
go run ./cmd/devctl reset --owner <tag>    # remove all data and start clean (use this most of the time)
go run ./cmd/devctl status                 # one-screen health: connector, replication slot, consumer group, warehouse
go run ./cmd/devctl down --owner <tag>     # stop the stack
go run ./cmd/devctl unlock --owner <tag>   # release the lock when you're done
```

`devctl status` is read-only, requires no lock, and is safe to run at any time, including
while someone else holds the stack (see below). Generate traffic with
`go run ./cmd/devctl load --rate 20 --owner <tag>` in a second terminal — some scenarios
(e.g. WAL/slot growth) only show up under load.

## Failure scenarios

```sh
go run ./cmd/devctl break <scenario> --owner <tag>
go run ./cmd/devctl fix <scenario> --owner <tag>
```

The 4 scenarios — `schema-change` (the demo scenario), `connector-down`, `consumer-stuck`,
`long-transaction` — are documented in full in `dev/scenarios.md`: what breaks it, the exact
symptoms in `devctl status`, the real error traces, and the real-world fix. Read that file
before writing a tool or an investigation step that needs to detect one of these.

## The stack is shared — lock it, then release it

Everyone working locally shares one dev stack. `up`, `down`, `reset`, `load`, `break`, and
`fix` all mutate shared state (they restart connectors, alter schemas, kill backends, remove
all data), so `devctl` refuses to run any of them without the shared stack lock:

```sh
go run ./cmd/devctl lock --owner [A2]          # take the lock
go run ./cmd/devctl lock status                # who holds it, and since when
go run ./cmd/devctl reset --owner [A2]          # now allowed — owner matches the lock
go run ./cmd/devctl unlock --owner [A2]         # release it when you're done
```

Pass `--owner <tag>` on every mutating command, or set `PIPEROOM_STACK_OWNER=<tag>` once in
your shell so you can drop the flag. The lock lives in a shared file (`PIPEROOM_STACK_LOCK`,
default `C:\Users\Utkarsh\Desktop\interview\piperoom-bus\stack.lock` — change it if your repo
layout differs) so it's visible to every agent's worktree, not just yours.

- A command whose `--owner` doesn't match the current holder refuses to run and names who
  holds it: `devctl: stack is locked by "[A3]" (you are "[A2]"); wait for them to run
  \`devctl unlock --owner [A3]\``.
- `devctl lock --owner <tag> --steal` removes a lock only once it's older than 15 minutes
  (and then re-acquires it for `<tag>` in the same command) — use it when someone's session
  died without releasing. It refuses on a fresh lock.
- Still post to the team channel around a `reset`/`break`/`fix` (`docs/collab.md` §5) — the
  lock stops two agents from mutating the stack at the same time, but it doesn't replace
  telling people what you're about to do to data they're relying on:

```
[<your tag>] STACK: reset
...
[<your tag>] STACK: free
```

Integration tests skip (never fail) when the lock is held by someone else — set
`PIPEROOM_STACK_OWNER` to your tag before running them if you also hold the lock, otherwise
they assume the stack is in use and skip.

## Common errors

- **`docker: Cannot connect to the Docker daemon`** — Docker Desktop (or the Docker daemon) isn't
  running. Start it, then retry `devctl up`/`reset`.
- **`port is already allocated`** (5432, 29092, 8081, or 8083) — something else on the host is
  already using one of the stack's ports. Stop it, or stop a stale `piperoom-dev` compose project
  with `docker compose -f dev/docker-compose.yml down` before retrying.
- **Connector registration or `devctl status` hangs or times out** — the stack is still starting;
  `devctl up`/`reset` waits on each service's health check, but a cold pull of the Kafka/Connect/
  Schema Registry images can take a few minutes the first time. Re-run `devctl status` once it
  returns.
- **`goose: command not found`** (once `migrations/` and the room-service migrations land) —
  install goose (`go install github.com/pressly/goose/v3/cmd/goose@latest`) and make sure
  `$(go env GOPATH)/bin` is on `PATH`.
- **Integration tests fail instead of skipping** — they should detect an unreachable stack and
  skip; if one fails hard, that's a bug in that test, not a reason to leave the stack running
  unnecessarily to work around it.

## Re-running `discover` safely

`go run ./cmd/discover -out owners.draft.yaml` (or `make discover-draft`) is read-only
against the cluster and only ever writes the `-out` file (`README.md` → *Pilot onboarding*).

- Without `-force` it refuses to touch an existing `-out` file, checking before it contacts
  the cluster: `discover: owners.draft.yaml already exists (use -force to overwrite)`.
- `-force` overwrites the `-out` file. The draft is fully rendered before the file is
  opened, so a failed run never truncates an existing draft.
- Point `-out` at a scratch file, never at the file `OWNERS_FILE` names: a re-run with
  `-force` would replace the reviewed owners file with a fresh draft full of `TODO`s.
  `discover` itself never reads or writes `OWNERS_FILE`; diff a new draft against it and
  copy changes across by hand.
- `-out -` prints the draft to stdout and writes no file at all.
- Errors and warnings name env vars (`CONNECT_URL`, `KAFKA_BROKERS`, …) instead of quoting
  addresses or secrets. `discover: discovery failed: … (raise -timeout?)` means the run hit
  `-timeout` (default `2m`).

## Slack bridge operations

The bridge library (`internal/slackbridge`) is merged, but the server does not start it on
`main` yet — wiring and the `SLACK_BOT_TOKEN` / `SLACK_CHANNEL_ID` / `PIPEROOM_PUBLIC_URL`
env vars are pending in PR #24. These notes describe the library's behaviour, for when that
lands.

- **Rotating the token.** The token is read once, when the bridge is built at server start;
  issue the new token in Slack, update the server's environment and restart the server. The
  bridge never logs the token or message text — its log lines carry only room id, seq,
  event type and the Slack error code.
- **Turning it off.** Unset both the token and the channel ID; the bridge then reports
  itself disabled and the server runs without it. Setting only one of the two is a
  configuration error, not "off".
- **`slack mirror stopped … code=not_in_channel`** (also `channel_not_found`, `is_archived`)
  — the bot is not in the channel or the channel is gone. Invite the bot / fix the channel
  ID.
- **`slack mirror stopped … code=invalid_auth`** (also `not_authed`, `token_revoked`,
  `token_expired`, `account_inactive`, `missing_scope`) — the token is wrong, revoked or
  lacks `chat:write`. Rotate it as above.
- In every such case **the room keeps working**: investigation, comments, approvals and
  hand-offs are unaffected; only that room's Slack mirror stops. Other codes are logged at
  Warn as `slack post failed` and that one message is skipped. How a stopped mirror is
  restarted after the fix is part of the PR #24 wiring.
