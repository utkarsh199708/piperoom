# Setup agent: `piperoom-setup` and the setup room

For the engineer who wants Piperoom to set a Debezium pipeline up from a room instead of by hand. Checked against
branch `claude/serene-maxwell-1dgs4o` at `cd3e1b8`; every claim names its file. The security view is
[../security/setup-runner-threat-model.md](../security/setup-runner-threat-model.md); the host driver is
[host-target.md](host-target.md); the Debezium Server driver is [debezium-server-target.md](debezium-server-target.md).

## Read this first: what works on this commit

The pieces exist, but a real setup does not reach the end yet. Each line is a finding of the threat model.

- A paired runner refuses every write step: the server does not send it the frozen proposal it checks the step
  against (`internal/setup/actions/adapter.go:420-430`; Finding 1).
- The runner has no Kafka Connect, source database, Kafka or Registry ops, so the agent's reads and the Q actions
  answer `unknown_op` (`internal/setup/remoteclients/doc.go:102-106`; Finding 3).
- The verification ladder cannot pass: no canary action is registered (`internal/room/setup_ladder.go:364-371`;
  Finding 4).
- `${secret:name}` references are never turned into values (`internal/setup/runner/secrets.go:579`; Finding 7).

Use it on a throwaway project directory and a test database until these land.

## 1. Install

- **Server.** `PIPEROOM_SETUP_ENABLED=on` needs `PIPEROOM_WRITE_MODE=on` and an absolute `PIPEROOM_SECRETS_DIR`, or
  the server refuses to start (`internal/config/config.go:1836-1890`). Only then is `/api/runners/ws` mounted
  (`cmd/server/main.go:723-725`; `internal/httpapi/runners.go:20-25`). Optional: `PIPEROOM_SETUP_PROD_PATTERNS`,
  `PIPEROOM_SETUP_LOCAL_DELAY` (default `20s`), `PIPEROOM_SETUP_PLUGIN_HOSTS` (default `repo1.maven.org`),
  `PIPEROOM_SETUP_TOKEN_BUDGET` (default 2 000 000), `OWNERS_DIR` (`internal/config/config.go:342-348`).
- **Runner.** `go build ./cmd/setup`, or take `/piperoom-setup` from the server image (`Dockerfile:50`,
  `Dockerfile:66`). `piperoom setup` runs the same code (`internal/setup/setupcmd/setupcmd.go:8-9`).
- **One machine, no server.** `piperoom-setup --standalone --project DIR` runs the server in-process on loopback
  with write mode and setup on, and `--watch` restarts it when `owners.yaml` changes
  (`internal/setup/setupcmd/setupcmd.go:50-80`; `internal/setup/standalone/standalone.go:84-156`). It starts no
  runner itself: pair one from a second terminal, where `--yes` is allowed because the server is loopback
  (`internal/setup/runner/runner.go:263-266`).

## 2. Pair and confirm

1. In the web app open **Set up** (`/setup`) and describe the pipeline: id, title, source kind, target, topic
   prefix, tables, and the hosts you already have (`web/src/App.tsx:31`; `web/src/components/setup/SetupPage.tsx`).
   You become the room's driver (`internal/room/setup.go:196-203`).
2. The answer carries a code and the command to paste (`internal/httpapi/setup.go:38-78`). The code is 8
   characters, valid 10 minutes, single use, 5 tries (`internal/setup/protocol/protocol.go:154-166`;
   `internal/room/runners.go:27-29`). It is a credential: prefer `PIPEROOM_SETUP_CODE` to `--code`, which shell
   history and process lists keep (`internal/config/config.go:355`; Finding 5).
3. The runner refuses `http://` off loopback and redirects (`internal/setup/runner/loopback.go:83-98`;
   `internal/setup/runner/runner.go:420-435`). It prints the server origin, the room, its pipeline and tenant, its
   creator, every target the setup may reach (kind, address, which for a managed target is the AWS account and region,
   the Confluent environment and cluster or the Aiven project and service, and class) and the pairing fingerprint, and
   waits for `y` (`internal/setup/runner/confirm.go`, `confirm` and `printWelcome`). Compare the fingerprint with the
   one the room was opened with (`POST /api/setup` returns it as `fingerprint`); answer `N` if they differ or you did
   not just ask for this room. Nothing of the
   room's targets runs before your `y`: the target drivers register, and a managed driver loads this machine's AWS
   credential chain, only after it (`internal/setup/runner/runner.go`, `pair`).

   **The fingerprint needs `PIPEROOM_PUBLIC_URL`.** The server hashes its public origin into the fingerprint
   (`internal/room/runners.go`, `SetupFingerprint`; `internal/httpapi/runners.go`, `publicOriginString`). Without
   `PIPEROOM_PUBLIC_URL` the origin is empty: the server's and the runner's fingerprints still match each other,
   but they no longer bind the server's address, so compare the origin the runner prints with the address you opened
   instead (the runner says so). The in-cluster runner, which has no terminal, refuses to pair at all unless the
   Welcome's public origin is the `--server` it dialed (`ErrOriginMismatch`), so a Strimzi setup needs
   `PIPEROOM_PUBLIC_URL` set to exactly that origin. The web app does not show the fingerprint on the card yet (known
   gap A6); the `POST /api/setup` answer carries it.
4. After `y` it stays connected and reconnects with a rotating token for up to 30 minutes per connection; after that,
   or if the code expires unused, nothing issues a new code (`internal/room/runners.go:406-430`;
   `internal/httpapi/server.go:255-270`; Finding 8).

The runner appends every write it runs or refuses to `<project>/.piperoom/ops.jsonl`, your own record
(`internal/setup/runner/oplog.go:15-20`).

## 3. Targets and classes

- A target is a host the setup may reach: the ones in your request, plus each one you add with the targets control,
  driver only (`internal/room/setup.go:353-398`). The server refuses user info in a URL, link-local and cloud
  metadata addresses, multicast, unspecified, loopback on a team server and its own addresses
  (`internal/room/setup_targets.go:161-247`).
- Each target has a class: `shared` by default, `production` when you say so or it matches
  `PIPEROOM_SETUP_PROD_PATTERNS`. You can raise a class, never lower it (`internal/room/setup_targets.go:258-335`;
  `internal/room/setup.go:403-437`). `local` exists in the code but is never derived on this commit
  (`internal/setup/plan/class.go:114` has no caller; Finding 9).
- A step's tier is the higher of the op's floor and its target's class: `local` T1, `shared` T2, `production` T3;
  reads stay T0 (`internal/setup/plan/tier.go:42-66`).
- The runner picks its driver from the targets the server sends: `host` for a host target, Compose otherwise
  (`internal/setup/setupcmd/targets.go:77-101`). The server sends none today, so it is always Compose
  (`internal/httpapi/runners.go:97-105`; Finding 2).

## 4. What auto-runs

- **Reads (T0)** run as the agent's tools, with no approval: files under the project, `docker info`, `compose ps`,
  container logs, service status (`internal/setup/tools/read.go:229-345`). A T0 op is never a proposal
  (`internal/room/setup_route.go:207-211`).
- **T1 writes** would run after a cancellable `PIPEROOM_SETUP_LOCAL_DELAY` countdown (`internal/room/autoapprove.go:345-361`;
  `internal/config/config.go:345`). Only a `local` target makes a write T1, so on this commit **nothing auto-runs**:
  every write waits for an approval.

## 5. What asks, and who approves

| Tier | Who approves | Source |
|---|---|---|
| T2 | owners of an existing pipeline the step overlaps, else a tenant admin; on a standalone server, you alone (solo) | `internal/room/service_fallback.go:93-138` |
| T2, your own driver-origin step | not you, on a team server | `internal/room/service_fallback.go:118-125` |
| `pipeline_register` | not the room's creator, on a team server | `internal/room/service_fallback.go:115-117` |
| T3 | refused by the runner before any prompt on this release | `internal/setup/runner/ops.go:241-246` |

A T3 `ALTER SYSTEM` or database restart also needs a maintenance window the driver records: at most 4 hours, at
most 24 hours ahead (`internal/room/setup_route.go:217-221`; `internal/room/setup.go:515-544`). Approve only what
the card shows: the runner's terminal prints op, target, proposal and step, not the params
(`internal/setup/runner/ops.go:650-661`; Finding 10).

## 6. The production guard

On a `production` target these are refused whatever anyone approves: dropping a table, truncating one, dropping a
replication slot or a publication, `ALTER SYSTEM` outside `wal_level`, `max_replication_slots`, `max_wal_senders`,
`max_slot_wal_keep_size` and `wal_sender_timeout`, deleting a Connect or MSK connector, a Kafka topic or a
namespace, `compose_down` that removes volumes, and `file_write`/`file_delete` (`internal/setup/plan/guard.go:111-125`,
`:207-226`). The server checks at create and before each step (`internal/room/setup_route.go:212-216`;
`internal/room/service_executor.go:420-424`); the runner checks the file ops itself (`internal/setup/ops/op.go:409-412`).
No op deletes data: `compose_down` has no `-v` form (`internal/setup/targets/compose/stack.go:15-29`).

## 7. The secret store

- Secrets live on the runner, in `<project>/.piperoom/secrets.env` (or `PIPEROOM_SETUP_SECRETS_FILE`), mode 0600, in
  `.gitignore` (`internal/setup/runner/secrets.go:111-198`). No file op can read `.piperoom` (`internal/setup/ops/paths.go:161-163`).
- Set one on the runner's terminal (`secret_set`) or in the room's secret field. The field's value goes to the runner
  in one frame and is stored nowhere on the server; the room records the name only
  (`internal/setup/runner/secretops.go:87-114`; `internal/room/setup_secrets.go:90-133`). Values: 6 bytes to 4 KiB
  from the field (`internal/room/setup_secrets.go:39-45`).
- Configs carry `${secret:name}`, never a value (`internal/setup/targets/compose/properties.go:182-188`). Everything
  the runner sends back has each stored value replaced by `${secret:name}` (`internal/setup/runner/secrets.go:878-908`).
- **Register read credentials** asks the runner; it prints the names and sends them only after your `y`, and the server
  writes each to `PIPEROOM_SECRETS_DIR/<pipeline>/<name>`, 0600 (`internal/setup/runner/secretops.go:198-263`;
  `internal/room/setup_secrets.go:386-498`). It sends **every** name you set, not a generated read-only role
  (`internal/room/setup_secrets.go:147`; Finding 6). Only names that are connection keys (`source_db_url`,
  `connect_url`, `kafka_brokers`, ...) become `connections:` entries (`internal/room/setup_register.go:759-788`;
  `internal/owners/tenancy.go:197`), so store a read-only login under those names.

## 8. How the pipeline is watched

- `pipeline_register` needs a green ladder for the current plan with no step after it
  (`internal/room/setup_register.go:176-199`), your read credentials registered
  (`internal/room/setup_register.go:420-438`), and `OWNERS_DIR` (or `repos:` for a pull request)
  (`internal/room/setup_register.go:408-418`).
- It writes `OWNERS_DIR/<pipeline>.yaml`, never over an existing file, reloads the owners set and tells the watcher,
  which lists the pipeline on its next tick (`internal/room/setup_register.go:634-692`, `:799-830`). The room closes
  "pipeline live" (`internal/room/setup_register.go:55`).
- Every registration on this commit is `watched_by: server`, because no target is `local`
  (`internal/room/setup_register.go:730-740`). The standalone server sets no `OWNERS_DIR`, so set one there too
  (`internal/setup/standalone/standalone.go:166-230`; Finding 15).
