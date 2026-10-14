# Shadow mode on the dev stack

Runs the Piperoom server in **shadow mode** against the local CDC stack: the agent investigates every
room silently (read tools only, no proposals), a human works and closes the room, then labels it
blind, reads the agent's answer and grades it. Design: `docs/design/shadow.md`. API: `docs/api.md`
→ *Shadow mode*. UI: the closed-room panel and `#/shadow` (Shadow results) in `web/`.

This is **not tested against a live stack by CI**: the unit tests cover the Go logic and the compose
file's shape only (no Docker). Do the checklist at the end the first time.

## What you need

- The dev stack: `go run ./cmd/devctl up --owner <tag>` (take the lock first, `docs/collab.md`).
- `ANTHROPIC_API_KEY` in your environment. Shadow mode runs the **real** agent, which costs tokens
  (default cap 500 000 per room, 20 turns). `devctl` checks the variable is set; it never prints it or
  writes it to a file.
- Docker. The first `shadow up` builds the repo-root `Dockerfile` (server + web UI), a few minutes.

## Start

```
go run ./cmd/devctl shadow up --owner <tag>
```

This starts the compose service `piperoom-shadow` (profile `shadow`, so a plain `devctl up` never
starts it) on **http://127.0.0.1:8090** with:

| Setting | Value |
|---|---|
| `OWNERS_FILE` | `dev/owners.shadow.yaml` = `dev/owners.yaml` + `shadow: {enabled: true}` on `orders` (a test fails if the two drift) |
| `PIPEROOM_WATCH_MODE` | `rooms`: the watcher opens the rooms (shadow needs it or an external alert source) |
| `PIPEROOM_WATCH_INTERVAL` / `_COOLDOWN` | `15s` / `5m` (server defaults are 60s / 60m) |
| `PIPEROOM_SHADOW_GRACE` / `_TOKEN_BUDGET` / `_MAX_TURNS` | `0` / `500000` / `20` |
| Room database | `piperoom` on the dev Postgres (migrations run at start); data survives `shadow down` |
| Auth | dev mode (pick a user from `dev/users.yaml`), published on loopback only |

Override the numbers with flags; each is checked with the server's own parser before anything is built,
and the error names the env var:

```
devctl shadow up --owner <tag> --grace 10m --token-budget 200000 --max-turns 12 --watch-interval 20s --watch-cooldown 2m
```

`--grace 10m` lets a run that is still working when the room closes finish (it holds an agent slot).

## Run a shadow incident

1. Open http://127.0.0.1:8090, choose a user (e.g. Arjun).
2. Break something: `go run ./cmd/devctl break connector-down --owner <tag>` (scenarios: `dev/scenarios.md`).
   Within about a minute the watcher opens a room. It is a shadow room: the banner says so, the steer
   box is hidden, and the room log shows no agent output.
3. Take control (a watcher room has no driver), investigate by hand, then
   `go run ./cmd/devctl fix connector-down --owner <tag>` and **close the room with a resolution**
   (the server refuses an empty one in a shadow room).
4. In the closed room: record your **blind label** (layer + root-cause terms). Only then does the server
   release the agent's conclusion. Read it next to your resolution and label, then **grade** it.
5. `#/shadow` (sidebar: Shadow results) shows "N of M matched, median X vs Y, tokens" over every closed
   shadow room.

Labelling and grading need a member of the room's pipeline (for the dev stack: a user with a layer in
`dev/users.yaml`, e.g. Arjun, Priya, Meera or Kabir). Viewers cannot write.

## Other commands

```
devctl shadow status                    # is the server ready; which pipelines are shadow
devctl shadow down --owner <tag>        # stop the server (rooms and grades stay in the database)
devctl shadow env [--shell sh|powershell] [flags]
```

`shadow env` prints the settings to run the server on the host instead of in a container, handy while
changing server code (key already exported; the dev stack's addresses are the config defaults):

```
go run ./cmd/devctl shadow env --shell powershell | Invoke-Expression
go run ./cmd/server
```
```
eval "$(go run ./cmd/devctl shadow env --shell sh | grep '^export')"
go run ./cmd/server
```

## Without the real agent

`go run ./cmd/server --demo` with `OWNERS_FILE` set to a copy of `dev/owners.shadow.yaml` runs the
scripted demo agent on an in-memory store: no stack, no key, no tokens (`tokens` shows as not counted).
Use it for UI work, not for judging the agent.

## First-run checklist (not automated)

- [ ] `devctl shadow up` ends with "shadow server is up"; `devctl shadow status` says ready.
- [ ] After `break connector-down`, a room opens with the shadow banner and no agent steps.
- [ ] Closing without a resolution is refused; with one it works.
- [ ] The closed room asks for the blind label first; the conclusion appears only after it.
- [ ] A grade saves; `#/shadow` counts the room.
- [ ] `devctl shadow down` stops the container; `devctl up` alone never starts it.
