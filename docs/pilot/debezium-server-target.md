# Debezium Server target: the server as a Compose service or a systemd user unit

For the engineer who wants `piperoom-setup` to run Debezium Server (Debezium without Kafka Connect, streaming
straight to a sink such as HTTP, Kinesis, Pub/Sub or Redis). It says what the target writes and where, which sinks and
sources it accepts, how credentials reach the server, what each step needs, how to undo it, and what it does not do.

Checked against branch `claude/serene-maxwell-1dgs4o` at `cd3e1b8`. Every claim names the file and line it comes from
(`path:line`), or a section of a design document. Where the code and `docs/design/setup-agent.md` §5 differ, the code
is described and the difference is in section 9. Nothing here was run on a real machine except what the integration
test in section 7 does, and this page did not run it.

## Status on this release: written, not wired

| Piece | State | Where |
|---|---|---|
| The driver: `server_properties_write`, `server_compose_write` (T1), `server_unit_write` (T2), `server_status` (T0) | code and unit tests exist | `internal/setup/targets/server/server.go:9-13`, `internal/setup/targets/server/server.go:224-230` |
| Registering the driver in the runner | **not done**. No package outside `internal/setup/targets/server` imports it; the shipped runner registers only the Compose or the host driver | `internal/setup/setupcmd/targets.go:39-61` |
| `sink_probe` (T0) | registered in every runner's generic catalogue, but it needs a `host` target in the runner's target list, which the shipped runner leaves empty | `internal/setup/ops/op.go:237-241`; `internal/setup/ops/sink_probe.go:166-183`; `internal/setup/setupcmd/setupcmd.go:105` |
| The server's action catalogue for the agent | **no `server_*` entry**: the environment specs list the Compose and host ops only | `internal/setup/actions/specs.go:27-187` |
| Read tools for the agent | **no `server_status` or `sink_probe` tool** | `internal/setup/tools/read.go:229-345` |
| A setup room with target `dbzserver` | accepted by `POST /api/setup` | `internal/room/setup.go:49-52` |
| Verification ladder for `dbzserver` | **refused**: the ladder covers `compose`, `host` and `strimzi` only, so the room notes "nothing was verified" and `pipeline_register` stays blocked | `internal/setup/plan/ladder.go:31-40`, `:79-85`; `internal/room/setup_ladder.go:238-244` |
| `setup_config_help` (the sink lookup tool) | written, registered by no server | `internal/setup/tools/confighelp.go:56` (no caller) |

So today the agent cannot propose a Debezium Server step from a room. The rest of this page is the contract of the
driver once it is registered; the names are the driver's own.

## 1. Modes and supported systems

| Mode | What runs | Where it works | Where |
|---|---|---|---|
| `compose` (default) | the pinned server image as one Compose service; started and stopped by the Compose driver's `compose_up`, `compose_ps`, `compose_down` | anywhere the Compose target works | `internal/setup/targets/server/server.go:41-44`; `internal/setup/targets/server/server.go:117-124` |
| `service` | `run.sh` of a distribution under the project, as a systemd **user** unit | Linux only; other OSes answer "not supported" | `internal/setup/targets/server/unit.go:196-206`; `internal/setup/targets/server/status.go:408-411` |

The mode is fixed when the driver registers (`Options.Mode`); nothing in an op's params changes it
(`internal/setup/targets/server/server.go:155-157`, `:198-204`). The image is `quay.io/debezium/server:3.2.0.Final`
pinned by digest (`internal/setup/knowledge/images.go:36-45`).

## 2. The ops

| Op | Floor | What it does | Source |
|---|---|---|---|
| `server_properties_write` | T1 | writes `debezium/config/application.properties` for one sink and one source connector | `internal/setup/targets/server/properties_ops.go:16-95` |
| `server_compose_write` | T1 | renders `compose.yaml` (the server, and optionally a Postgres source) and writes it | `internal/setup/targets/server/compose.go:287-344` |
| `server_unit_write` | T2 | renders `systemd/piperoom-dbzserver.service` and writes it; does **not** enable or start it | `internal/setup/targets/server/unit.go:143-195` |
| `server_status` | T0 | compose mode: `docker compose ... ps --format json`; service mode: `systemctl --user show` of nine properties | `internal/setup/targets/server/status.go:380-428`; `internal/setup/targets/server/status.go:30-35`, `:210-223` |
| `sink_probe` | T0 | one empty `HEAD` to the configured HTTP sink (section 7) | `internal/setup/ops/sink_probe.go:42-165` |

No op takes a command string. `server_status` runs only `docker` or `systemctl` from a fixed argv
(`internal/setup/targets/server/status.go:327-328`; `internal/setup/targets/server/compose.go:263-283`). The
`server_` prefix exists because a compose-mode room also holds the Compose driver, whose names are taken
(`internal/setup/targets/server/server.go:9-13`).

## 3. What it writes and where

| What | Path | Mode | Source |
|---|---|---|---|
| server config | `<project>/debezium/config/application.properties` | 0644 in compose mode (the container reads it across a bind), 0600 in service mode | `internal/setup/targets/server/server.go:89`; `internal/setup/targets/server/properties_ops.go:68-75` |
| Compose file | `<project>/compose.yaml`, with a header saying it is generated | 0644 | `internal/setup/targets/server/server.go:103`; `internal/setup/targets/server/compose.go:126-128` |
| unit file | `<project>/systemd/piperoom-dbzserver.service` | 0644 | `internal/setup/targets/server/server.go:100`; `internal/setup/targets/server/unit.go:177` |
| offsets | `data/offsets.dat` under the working directory: the named volume `dbz-server-data` in compose mode, `<project>/debezium/data` in service mode | | `internal/setup/targets/server/properties.go:50`; `internal/setup/targets/server/compose.go:25-30` |

- Every write goes through the project's path rule and an atomic `O_EXCL` temporary plus rename; an unchanged file is
  left alone but its mode is still fixed (`internal/setup/targets/server/files.go:23-97`).
- `debezium/` and `systemd/` are registered as mount directories, so `file_write` and `file_delete` refuse them; the
  driver's own two files are the only exception (`internal/setup/targets/server/server.go:232-236`;
  `internal/setup/targets/server/files.go:17-21`).
- **Compose render.** The server service has the pinned image, `./debezium/config` bound read-only at
  `/debezium/config` and the named volume at `/debezium/data`. With `source: postgres` (the default) the stack adds a
  Postgres with `wal_level=logical`, a `pg_isready` health check the server waits for, and the server gets
  `DEBEZIUM_SOURCE_DATABASE_PASSWORD=${POSTGRES_PASSWORD}` from `.env`. `source: external` adds no database. Nothing
  is published unless `source_port` is set, and then only on `127.0.0.1` for a local class
  (`internal/setup/targets/server/compose.go:142-209`, `:237-243`). The render must pass the Compose driver's validator
  before it is written (`internal/setup/targets/server/compose.go:129-133`). The project name must start `piperoom-`
  (`internal/setup/targets/server/compose.go:53-57`).
- **Unit render.** `WorkingDirectory` is `<project>/debezium`, `ExecStart` is `<project>/debezium/run.sh`, and
  `JAVA_HOME` is the JDK the `java` on `PATH` resolves to; the template is the host driver's `RenderUnit`
  (`internal/setup/targets/server/unit.go:59-66`, `:169-173`). It needs an executable `debezium/run.sh` and the
  properties file first, and refuses a JDK inside the project directory (`unit.go:30-53`, `:104-128`). Its summary
  tells you to run `systemctl --user enable <path>` yourself (`unit.go:186`).

You extract the Debezium Server distribution into `<project>/debezium/` yourself for service mode; no op downloads
it (`internal/setup/targets/server/unit.go:38-40`).

## 4. The properties file

- **Sinks** come from the knowledge index, not from this package: `http`, `kinesis`, `pubsub`, `pubsublite`,
  `redis`, `kafka`, `pulsar`, `rabbitmq`, `rabbitmqstream`, `eventhubs`, `nats-jetstream`, `infinispan`, each verified
  for `>=3.2.0.Final` only (`internal/setup/knowledge/index/sink-server.yaml:1-7`, `:13`, `:40`, `:62`, `:89`, `:109`,
  `:138`, `:157`, `:178`, `:206`, `:229`, `:249`, `:272`). A key the index does not list for that sink, a missing
  required key, a value outside the key's allowed values, or a key the pinned release does not have is refused
  (`internal/setup/targets/server/properties.go:411-456`).
- **Sources**: `postgres`, `mysql`, `sqlserver`, `mongodb`, `oracle`, each to its documented connector class
  (`properties.go:61-72`). Source keys come from a fixed allow-list per connector; hosts are checked and metadata
  names and addresses refused; a file path may name only `data/` (`properties.go:208-268`, `:491-560`, `:614-640`).
  The driver writes the connector class, the formats and the offset store itself and refuses a second value for them
  (`properties.go:76-81`, `:382-388`).
- **Transforms and predicates** must be index entries; scripting SMTs (a `condition`, `expression` or `language` key)
  are refused (`properties.go:688-727`).
- **Formats**: value `json` (default), `jsonbytearray`, `avro`, `protobuf`, `cloudevents`, `simplestring`, `binary`;
  key the same without `cloudevents` (`properties.go:53-58`).
- **Bounds**: 64 keys, 32 KiB, 1024 bytes per value; no control character, backslash, leading or trailing space,
  leading `#`, or `${...}` other than `${secret:name}` (`properties.go:21-26`, `:850-883`).

## 5. Credentials

- A key the index marks secret (for example `debezium.sink.http.authentication.jwt.password`) takes only a
  `${secret:name}` reference; a plain credential anywhere is refused, and a reference on a key that is not secret is
  refused too (`internal/setup/targets/server/properties.go:458-470`, `:529-545`;
  `internal/setup/knowledge/index/sink-server.yaml:27-28`).
- **The runner does not turn the reference into a value yet**: the file keeps `${secret:name}`
  (`internal/setup/targets/server/properties.go:926-929`). In compose mode the co-located Postgres password reaches
  the server through `.env` instead (`internal/setup/targets/server/compose.go:142-165`); the integration test writes
  that `.env` by hand for this reason (`internal/setup/targets/server/integration_test.go:93-97`). A sink credential
  has no working path on this release.
- Kinesis credentials come from the AWS default provider chain or a named profile, and Pub/Sub uses the Google
  credentials in use, per the index (`internal/setup/knowledge/index/sink-server.yaml:48`, `:54`, `:68`).

## 6. Tiers and approvals

| Step | Floor | What it needs |
|---|---|---|
| `server_status`, `sink_probe` | T0 | a read, never a proposal (`internal/room/setup_route.go:207-211`) |
| `server_properties_write`, `server_compose_write` | T1 | T1 only on a `local` target; the server never derives `local` today, so in practice T2, an approval (`internal/setup/plan/tier.go:42-66`; `internal/room/setup_targets.go:313`) |
| `server_unit_write` | T2 | the approver of a T2 step: an overlapping pipeline's owners, else a tenant admin, or you alone on a standalone server (`internal/room/service_fallback.go:93-138`) |

All four ops are in the owners layer `environment` (`internal/setup/targets/server/server.go:113-114`). A
`production` target makes each write T3, which the runner refuses on this release
(`internal/setup/runner/ops.go:241-246`).

## 7. `sink_probe` and the real run

- `sink_probe` takes one param, `target`, which must equal the address of a registered `host` target. It reads
  `debezium/config/application.properties` through the project's path rule, requires `debezium.sink.type=http`,
  requires `debezium.sink.http.url` to match that target's host and port, rebuilds the URL from the target, and sends
  one empty `HEAD` with redirects refused and a 10 s cap. A 2xx answer is "configured"; anything else fails the probe.
  Property values never appear in the result (`internal/setup/ops/sink_probe.go:166-275`). Metadata addresses are
  refused (`internal/setup/ops/sink_probe_test.go:282`).
- Only the HTTP sink has a probe (`internal/setup/ops/sink_probe.go:107-110`).
- `TestServerHTTPSinkIntegration` (build tag `integration`) takes an empty directory to the pinned server and Postgres
  images and checks that a canary row reaches an HTTP receiver on the test machine as one event
  (`internal/setup/targets/server/integration_test.go:30-41`).

## 8. How to undo

There is no rollback op. `server_properties_write` and `server_compose_write` rewrite their file atomically and keep
no backup (`internal/setup/targets/server/files.go:72-95`). `compose_down` stops the stack and keeps volumes, so the
offsets in `dbz-server-data` survive (`internal/setup/targets/compose/stack.go:15-29`). A unit this driver wrote was
never enabled by it; delete `systemd/piperoom-dbzserver.service` if you did not enable it, or disable it with
`systemctl --user disable --now` first (a standard command, not from this repository).

## 9. Known limits and differences from the design

- **Not wired** (the status table). Treat this page as the driver's contract.
- **Design §5 versus code.** The design lists `properties_write`, `compose_write`/`service_install` and a verify rung of
  "process up plus the sink's canary read". The code has its own `server_*` ops, `server_unit_write` writes but does
  not install the unit, and no ladder exists for `dbzserver` (`docs/design/setup-agent.md` §5;
  `internal/setup/targets/server/server.go:9-13`; `internal/setup/plan/ladder.go:79-85`).
- **A sink URL by name is not resolved** before the server uses it; only an IP literal is checked, and Debezium
  Server's own dialer has no denylist (`internal/setup/targets/server/properties.go:887-897`).
- **The writer checks the path, then reopens the project root** and does not re-walk after the rename, unlike
  `file_write` (`internal/setup/targets/server/files.go:29-41`; threat model Finding 14).
- **Source keys are a fixed table** until the source drivers supply connector configs
  (`internal/setup/targets/server/server.go:20-28`).
