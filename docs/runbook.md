# Dev stack runbook

Day-to-day operation of the local CDC pipeline (`dev/docker-compose.yml`) used to develop
and test Piperoom. See `README.md` for the one-line versions and `dev/scenarios.md` for
what each failure scenario looks like and how to fix it.

## Start / stop

```sh
go run ./cmd/devctl up      # start the stack, register the connector, start devconsumer
go run ./cmd/devctl reset   # remove all data and start clean (use this most of the time)
go run ./cmd/devctl status  # one-screen health: connector, replication slot, consumer group, warehouse
go run ./cmd/devctl down    # stop the stack
```

`devctl status` is read-only and safe to run at any time, including while someone else holds
the stack (see below). Generate traffic with `go run ./cmd/devctl load --rate 20` in a second
terminal — some scenarios (e.g. WAL/slot growth) only show up under load.

## Failure scenarios

```sh
go run ./cmd/devctl break <scenario>
go run ./cmd/devctl fix <scenario>
```

The 4 scenarios — `schema-change` (the demo scenario), `connector-down`, `consumer-stuck`,
`long-transaction` — are documented in full in `dev/scenarios.md`: what breaks it, the exact
symptoms in `devctl status`, the real error traces, and the real-world fix. Read that file
before writing a tool or an investigation step that needs to detect one of these.

## The stack is shared — hold it, then free it

Everyone working locally shares one dev stack. `devctl reset`, `break`, and `fix` all mutate
shared state (they restart connectors, alter schemas, kill backends), so before running any of
them, post to the team channel and wait ~30 s for anyone currently using the stack to speak up,
then post again once you're done (`docs/collab.md` §5):

```
[<your tag>] STACK: reset
...
[<your tag>] STACK: free
```

Never run `reset`/`break`/`fix` while someone else holds the stack. `devctl status` is always
safe — it never mutates anything. Integration tests must skip (not fail) when the stack is
unreachable, so they don't fight over it either.

There is no `devctl` command that enforces this lock yet — it's a convention enforced by
posting to the channel, not by the tool. If that changes, update this section.

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
