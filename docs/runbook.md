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
your shell so you can drop the flag. The lock lives in a shared file so it's visible to every
agent's worktree, not just yours. Its path is `PIPEROOM_STACK_LOCK`; when that is unset the
default (`internal/stacklock`, used by `cmd/devctl/lock.go`) is `piperoom-bus/stack.lock` next to
the main checkout: `<parent of the repo root>/piperoom-bus/stack.lock`, where the repo root is
the one that holds the shared `.git` directory (`git rev-parse --git-common-dir`), so every
worktree of one clone finds the same file. Change it (set `PIPEROOM_STACK_LOCK`) when your
checkout lives elsewhere. Without git and without the variable, lock commands refuse to run.

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
- **Room database migrations** — there is no `goose` CLI to install: the migrations in
  `migrations/` are embedded in the server, which applies them itself at start (the
  `pressly/goose` library, not the command). A start that fails with
  `server: ping for migrations` means `PIPEROOM_DB_URL` does not answer.
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

The server starts the bridge (`cmd/server/slack.go`) when `SLACK_BOT_TOKEN` and
`SLACK_CHANNEL_ID` are both set and logs `slack bridge enabled` with the channel ID;
`PIPEROOM_PUBLIC_URL` adds the room link to each thread. With several replicas only the
elected leader mirrors rooms.

- **Rotating the token.** The token is read once, when the bridge is built at server start;
  issue the new token in Slack, update the server's environment and restart the server. The
  bridge never logs the token or message text — its log lines carry only room id, seq,
  event type and the Slack error code.
- **Turning it off.** Unset both the token and the channel ID; the bridge then reports
  itself disabled and the server runs without it. Setting only one of the two is a
  configuration error, not "off": the server refuses to start with
  `level=ERROR msg="server: failed" err="server: slack bridge: slackbridge: token and channel id must be set together"`.
- **`slack mirror stopped … code=not_in_channel`** (also `channel_not_found`, `is_archived`)
  — the bot is not in the channel or the channel is gone. Invite the bot / fix the channel
  ID.
- **`slack mirror stopped … code=invalid_auth`** (also `not_authed`, `token_revoked`,
  `token_expired`, `account_inactive`, `missing_scope`) — the token is wrong, revoked or
  lacks `chat:write`. Rotate it as above.
- In every such case **the room keeps working**: investigation, comments, approvals and
  hand-offs are unaffected; only that room's Slack mirror stops. Other codes are logged at
  Warn as `slack post failed` and that one message is skipped. After the fix, restart the
  server: at start (and on a leader change) it mirrors every open room again.

## Upgrading to room limits (migration 00012)

Migration `00012` (per-room log caps, `docs/design/retention.md`) adds two nullable columns and the
`room_deletions` table. It rewrites no row, so it takes an instant whatever the size of the room database.
Rooms that existed before it get their byte count in the background after the server starts:

- Look for `byte_size backfill done rooms=N shadow_runs=M took=...` in the server log (or
  `another replica holds the lock` on the other replicas). A shutdown during the backfill is fine: the next
  start continues where it stopped.
- Until then an old room's byte cap (`PIPEROOM_ROOM_MAX_BYTES`) is not enforced; its event cap is.
- Expected duration: about 45 s per GB of stored event payloads (7 to 8 minutes for 10 GB), measured at
  24 MB/s on postgres:16; see `docs/design/retention.md` section 5.2. Appends wait only for the batch of
  500 rooms being counted.
- Rooms not yet counted: `SELECT count(*) FROM rooms WHERE byte_size IS NULL;`

## CI: full-stack integration, smoke, release image

| Job | When | What it needs |
|---|---|---|
| `Go integration (full dev stack)` (`ci.yml`) | every PR and push to `main` | nothing; starts `dev/docker-compose.yml`, polls Connect (`.github/scripts/wait-for-stack.sh`), runs `go test -tags integration ./internal/... ./cmd/...` (the room-database tests use a separate Postgres service on host port 5433, so their WAL stays out of the Debezium slot), then the dev-stack smoke on the same stack (`devctl status`, `health`, `break` + `fix schema-change`, `health` again). **Any skipped test fails the job** (`fail-on-skips.sh`), except reviewed non-stack skips in `.github/integration-skip-allow.txt` (owner and review-by date; an expired entry fails) |
| `Release image` (`release-image.yml`) | tags `v*` push to `ghcr.io/<owner>/piperoom`; PRs touching the image inputs only build | the digest is in the job summary, the `image-digest` artifact and the job output `digest`. Deploy by digest: `ghcr.io/<owner>/piperoom@sha256:...`. The PR build also checks both binaries (U1-22): the entrypoint is still `/piperoom` (`--version` runs), and `/piperoom-setup --help` prints its flags |
| `Setup matrix` (`setup-matrix.yml`) | every PR and push to `main` | the setup capability on ubuntu, macOS and Windows: `go test ./internal/setup/... ./cmd/setup/...`, the §2.3 path table and the Docker endpoint detection (fakes). Ubuntu (and macOS when that runner has Colima) also runs `go test -tags integration ./internal/setup/...`. The Windows unit run skips three upstream tests that cannot pass there: `TestPropertiesWriteFixesMode` and `TestStackOpsRunValidatedSnapshot` (both assert 0600; NTFS reports 0666 and `os.Chmod` only toggles the read-only bit) and `TestStackRefusesSymlinkBind` (no symlink privilege) — C19 follow-up. In the path table the two real-symlink rows may skip where the user cannot create symlinks, while the fake-based rows and the Windows junction test must pass |
| `Setup nightly (Windows)` (`setup-nightly.yml`) | `schedule` and `workflow_dispatch` only — **not** a required check | the Compose integration on the owner's self-hosted Windows runner (`self-hosted, windows, x64, piperoom-r1`, Docker Desktop in Linux-container mode). It never runs on a pull request, so a checkout without that runner never shows it as pending, and it runs only on `main` of this repository. It runs `TestComposeStackToHealthyConnect` only. The repository is public and a runner is chosen by label, so the runner needs "Require approval for all external contributors" and a dedicated unprivileged Windows account. The `go test -json` export is the `setup-compose-integration-windows-<run>` artifact, kept 30 days |

Live evals are `evals-live.yml`, documented in `evals/README.md` ("CI (evals-live)").

Allowlist upkeep: an entry past its review-by date fails the job for every PR, so the owner named on the line renews it
or fixes the test before that date.

Reproduce a full-stack failure locally: `go run ./cmd/devctl lock --owner me && go run ./cmd/devctl up --owner me`,
then `PIPEROOM_STACK_OWNER=me go test -count=1 -json -tags integration ./internal/... ./cmd/... > test.json` and
`.github/scripts/fail-on-skips.sh test.json`.
