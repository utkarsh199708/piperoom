# U wave dispatch board: one row = one PR

Plan `docs/product/setup-2026-10.md`: 572 of the 722 Stack Overflow Debezium problems (79%) are first-time setup,
and the fix for most of them lives in the user's own image, compose file, manifests, worker properties or network.
This wave gives Piperoom a setup capability (the owner's decision: a functionality of the one product, not a
separate one): the same agent, in the same room, with the same approval model, takes a user from nothing to a
watched pipeline through a runner that executes typed operations where the user's Docker, `kubectl` and cloud
credentials are. Design `docs/design/setup-agent.md`. The wave takes agent-fixable setup problems from 0 to 473 of
572 (82.7%); the per-step curve is plan §4. Each row is a PR an agent finishes in one or two sessions (target < 600
changed lines, §6 of `AGENTS.md`).

- **ID** is the PR tag (`[U1-3] setup/compose: driver and T1 ops`). A design row is `U<N>-0`.
- **Model**: every row runs on Opus 5.5, cloud rows included (the owner's standing instruction: the strongest model
  for every coding row).
- **Days**: one agent's working days including tests and review fixes.
- **After**: rows that must be merged first. U0 always; Q rows by their `tasks/Q-board.md` ID.

**Order.** U0, then the design row U1-0 and the U1 rows (the core: runner, catalogue, Compose, Postgres,
verification); then steps 2 to 13 in coverage-per-day order: MySQL, transforms and sinks, SQL Server, logs,
embedded, MongoDB, plain hosts, Oracle, tuning, Debezium Server, Strimzi, managed. The design rows U12-0 and U13-0
start as soon as U0 merges (decided, plan §7 item 1), so pulling Strimzi or the managed services forward costs
no rework. An agent with two rows ready takes the lower step first.

**Before the wave.** U0 merges after Q0. U1 codes against Q1a (known issues), Q2a to Q2d (Postgres settings and
admin), Q4a (validate), Q5a and Q5c (lifecycle), Q6a (`net_probe`); each row's After column names them. Two Q rows
gain one line each for this wave, inside their own PRs: Q6a's probe takes a location interface so the runner can
run it from its own network position; Q4a accepts `${secret:...}` as a non-secret value. Both are noted in the Q
threads by the leads with U0.

**Real runs.** Every target row's "Done when" includes a real run: a person starts `piperoom-setup` on the named
OS against the named target from an empty directory and the room closes `pipeline live`; the room export (JSON) is
attached to the PR. Linux runs happen in CI; macOS on a lead's machine; Windows on the R1 dev machine.

## U0 items: the contract additions each row needs

Items 1 to 6 of `docs/design/setup-agent.md` §4, written by the leads in the U0 PR: events (`setup.*`,
`pipeline.registered`, the `environment` layer, `room.created.mode`, `ActionExecutedPayload.Receipt`,
`PolicySetupLocal`); `agent.Alert.Mode`, `Options.Setup`, `ErrNotInPlan`; `actions.Tiered`;
`clients.RunnerExecutor`; `actions.LooksSecret` (the exported `looksSecret` rule); the config variables; the
`docs/api.md` routes and snapshot fields. Item 7 (protocol
versioning, Windows service API; Strimzi CRDs and the in-cluster runner; the managed clients' auth) is settled by
the design rows U1-0, U12-0 and U13-0, each landing its own contract change. New methods go on **new** interfaces
and new events, so every existing fake and the eval replay compile unchanged.

## Step 0: contracts and the core design

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U0 | leads | Opus 5.5 | 1.5 | contracts: setup events, `Alert.Mode`, `Options.Setup`, `actions.Tiered`, `clients.RunnerExecutor`, config, API | items 1 to 6 in code and `docs/api.md`; `LayersOf("cdc")` gains `environment` at the end and every existing owners file still loads; all stubs and fakes compile; `go build`, `go vet`, `go test ./...` green; the two Q-thread notes posted | Q0 |
| U1-0 | leads | Opus 5.5 | 1.5 | design: protocol field list and versioning, the Windows service spike, re-estimate of U1 | `internal/setup/protocol` types final with a `Version` and a compatibility rule (a runner one minor behind still pairs); a 30-line spike on the dev machine proves `golang.org/x/sys/windows/svc/mgr` can install, start and query a service (`go doc` output in the PR); the U1 and U8 day estimates revised on this board | U0 |

## Step 1: the core: runner, catalogue, Compose target, Postgres prerequisites, verification

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U1-1 | A9 | Opus 5.5 | 2 | setup/protocol + setup/runner: pairing, `Hello`, ping, reconnect with the token, `OpRequest`/`OpResult` dispatch | `httptest` WebSocket fake server: pairs with a code, refuses a used or expired code, reconnects with the token after a drop, marks in-flight ops unknown after 90 s silence; frames over 1 MiB cut with the marker; TLS required unless loopback (`netsec`); the token is never logged | U1-0 |
| U1-2 | A9 | Opus 5.5 | 2 | setup/ops: `Op`, `Registry`, the rule tests; T0 ops `env_detect`, `file_read`, `dir_list`, `http_get`, `jar_list`, `java_version`; `net_probe` over Q6a from the runner | table tests: a path outside the project dir, `..`, a symlink out, `.git/`, a device file are refused; `http_get` refuses a host not in `Env.Targets`; a T1+ request without `proposal_id` is refused; a request with a tier below the op floor is refused; `jar_list` flags flat jars, two Debezium versions and missing converter jars; every exec op builds an argv slice, no shell (an AST test as `wiring/readonly_ast_test.go`) | U1-1, Q6a |
| U1-3 | C19 | Opus 5.5 | 2.5 | setup/targets/compose: driver, templates (KRaft Kafka, Connect with the plugin dir, optional Registry), T0 `docker_info`, `compose_ps`, `compose_config`, `container_inspect`; T1 `compose_up`, `compose_restart`, `compose_down` (keeps volumes), `plugin_install` | `compose_config` is the preview of every compose write; `plugin_install` downloads only from `PIPEROOM_SETUP_PLUGIN_HOSTS`, verifies the published SHA-256, unpacks one plugin per directory; Docker Desktop on macOS and Windows: the socket path and host networking differences handled and tested with fakes; integration on Linux: an empty dir to a healthy Connect with the Postgres plugin listed by `GET /connector-plugins` | U1-2 |
| U1-4 | A9 | Opus 5.5 | 1.5 | setup/runner: the secret store, `secret_set`, materialisation (`.env`, Connect `${file:}` references, a properties file next to the compose file), redaction before transport | the store is 0600 and in the generated `.gitignore`; a secret value never appears in an `OpResult`, a receipt or a log (test greps every frame); `secret_set` prompts on the terminal or takes the UI frame; `redact.Text` runs on every `Summary` and `Data` | U1-1 |
| U1-5 | A5 | Opus 5.5 | 2.5 | room + httpapi + ws: runner registry, `GET /api/runners/ws`, `POST /api/setup`, pairing codes, `setup.runner_attached`/`lost`, `RunnerExecutor`, snapshot `setup` | a code pairs once and expires in 10 min; one runner per room; `RunnerExecutor.Execute` times out at the request deadline and returns `ErrNoRunner` when none is paired; replicas: the runner's socket lives on one replica and `Execute` from another replica is routed through the room lease holder (`docs/design/replicas.md` §3) or refused with a clear error, decided and tested; `PIPEROOM_SETUP_ENABLED=off` hides every route | U0 |
| U1-6 | A5 | Opus 5.5 | 1.5 | room: tiered routing (`actions.Tiered`): T0 refused as a proposal, T1 scheduled by policy `setup_local` with `PIPEROOM_SETUP_LOCAL_DELAY`, T2 waits for the owner, T3 owner plus typed confirm; the guard table; `ErrNotInPlan`; `setup.target_classed` and the raise-only route | fake-clock tests per tier; the guard table refuses each listed op on a production target with a reason; a class can be raised by the driver and never lowered; a proposal whose step is not in the latest `setup.plan` is refused | U1-5, P2b-4 |
| U1-7 | A9 | Opus 5.5 | 1.5 | setup/tools + setup/actions: the KindRead forwarding tools, the KindRoom `setup_plan` tool, the action adapters (`Preview`, `Scoped`, `Tiered`) | every forwarding tool is `KindRead` and registers in a read-only registry; `setup_plan` refuses an op not in the catalogue; the adapters' `Preview` calls the runner's T0 preview op only; `CheckScope` refuses a target outside the room's list | U1-2, U1-5 |
| U1-8 | A9 | Opus 5.5 | 1.5 | setup/plan: environment classes, `TierOf`, the verification ladder, the bounds | `TierOf` table-tested over every (floor, class); the classifier never derives `production` from a host name alone and never lowers a person's answer; `Ladder(postgres, compose, none)` is the eight rungs of the design §3.4 with their tool and predicate; the bounds stop at 3 attempts per step, 12 steps, 45 min, the token budget | U1-2 |
| U1-9 | A3 | Opus 5.5 | 2 | setup/sources/postgres: the prerequisite driver over Q2 (`wal_level`, slots, senders, publication, `REPLICA IDENTITY`, grants, RDS parameter group), the canary table, the connector config with `${secret:}` references | each check names its fix op and tier floor; the connector config passes Q4a's `connect_validate_config` on the dev stack; integration with `it_` objects; a table test per check with the fix the plan proposes | U1-2, Q2a, Q2d, Q4a |
| U1-10 | A4 | Opus 5.5 | 2 | agent + wiring: setup mode (prompt frame, bounds, plan-before-propose, re-plan after failure, the ladder as verification), `wiring.SetupStack` | prompt snapshot for setup mode; a recorded-model test takes a fake runner from an empty dir to `setup.verified` rungs 1 to 7; after a failed step the next proposal without a new `setup_plan` is refused and the model recovers; CDC evals pass rate unchanged; the setup registries are built only for a room with a paired runner | U1-7, U1-8, U1-9, Q1d |
| U1-11 | A9 | Opus 5.5 | 1.5 | cmd/setup + setup/standalone: the runner binary, `--standalone` (embed the quickstart server, attach, open the browser), `setup-log.jsonl` | `piperoom-setup --standalone --project ./orders` prints a URL in < 10 s on the three OSes (CI matrix); refuses a non-loopback address as `--quickstart` does; every room event is appended to the log file; the image ships the binary | U1-1, U1-4, P1a-2 |
| U1-12 | A6 | Opus 5.5 | 2.5 | web: New pipeline → Set up (the form, the pairing command with copy), the setup stepper above the feed, runner status, class raise, the secret field, the canary prompt, the verified card | Vitest per state (no runner, paired, planning, running step, awaiting approval, blocked, verified, registered); the decision bar shows the first step that needs a person; the secret field posts to the setup secret route and never to a comment; mobile: the stepper is a bottom sheet | U0 (fake snapshot), U1-5 to verify |
| U1-13 | A3 | Opus 5.5 | 1 | owners: `OWNERS_DIR` (per-pipeline files loaded after `OWNERS_FILE`), `Reload`, duplicate id refused | table tests: a file that fails validation is skipped with a logged reason and the rest load; a reload swaps the set atomically; a pipeline id in both the file and the dir is refused at start | U0 |
| U1-14 | A5 | Opus 5.5 | 1 | room + cmd/server: `pipeline_register` action (writes `OWNERS_DIR/<id>.yaml`, reloads, `pipeline.registered`), the watcher picks the pipeline up, the room closes `pipeline live` | the written file loads with `owners.Load` and is byte-equal to what `discover -write-owners` writes for the same pipeline; the watcher's next tick lists the pipeline; `GET /api/pipelines` shows `setup`; on the standalone server the file is written next to the project | U1-13, U1-6 |
| U1-15 | A1 | Opus 5.5 | 1.5 | dev + devctl: `setup-break` scenarios (flat plugin dir, missing converter jars, `wal_level=replica`, missing grant, DB unreachable from Connect, plugin added after the worker scan) and the Linux integration test | each break makes a fresh `piperoom-setup` run against the dev stack fail at a named check and the recorded agent fixes it; `dev/scenarios.md` rows; `testenv.Setup(t)` | U1-11 |
| U1-16 | C1 | Opus 5.5 | 1.5 | ci: `setup-matrix.yml` (ubuntu, macos, windows: unit and detection; ubuntu: Compose integration; macos: Colima when present else a documented skip), the self-hosted Windows nightly job | matrix green on the U1-11 merge commit; the Windows nightly runs the Compose integration on the dev machine and posts its export as an artifact | U1-11, U1-15 |
| U1-17 | C4 | Opus 5.5 | 1 | docs/security: threat-model section for the runner (the Docker socket, pairing code, secret frame, tier locks) | every claim cites a file; findings become U1 fix rows before U1-R | U1-5, U1-6 |
| U1-18 | C14 | Opus 5.5 | 1 | docs/pilot/setup-agent.md: the user's page (install, pair, classes, what auto-runs, what asks, the production guard, the secret store) | citations per claim; ≤ 120 lines; README's quickstart starts with `piperoom-setup` | U1-11 |
| U1-19 | C7 | Opus 5.5 | 1 | scenarios: `setup/compose-postgres-clean`, `setup/flat-plugin-dir`, `setup/wal-level-replica`, `setup/missing-grant`, `setup/db-unreachable-from-connect` | each label names the failing check and the fixing op | U1-15 |
| U1-20 | C6 | Opus 5.5 | 1.5 | evals: runner recordings replay (`evals/setup`), per-target and per-source pass rate in the report | the five U1-19 scenarios score; the CDC pass rate is unchanged | U1-19 |
| U1-R | A9 (Linux, macOS), A1 (Windows) | Opus 5.5 | 1.5 | real runs: Linux, macOS Docker Desktop, Windows Docker Desktop, each from an empty directory to `pipeline live`; the write-up of the three runs in `reviews/setup-runs-2026-11.md` (leads' folder; A9 posts, the leads commit) | three exports attached; wall clock, steps, approvals and tokens per run in the write-up; every failure the agent could not fix is a row on this board or an issue | U1-10 to U1-16 |
| U1-21 | A9 | Opus 5.5 | 1 | cmd/piperoom: the umbrella binary (`piperoom server`, `discover`, `investigate`, `setup`), the image's entrypoint | the optional row of plan §7 item 2; each subcommand is the existing `main` with its flags; `piperoom setup` is `piperoom-setup`; the pilot image still works with the old entrypoint | U1-11 |

## Step 2: MySQL prerequisites

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U2a | A8 | Opus 5.5 | 2 | setup/sources/mysql: the driver over Q8 (`log_bin`, `binlog_format`, `binlog_row_image`, GTID, `server_id`, `binlog_expire_logs_seconds`, grants, the RDS parameter group), the canary table, the connector config | each check names its fix op and floor; `SetReplicaIdentity` answered `ErrNotSupported` is not a check; integration on the Q8a profile | U1-9, Q8b, Q8f |
| U2b | A1 | Opus 5.5 | 1 | devctl: `setup-break` `binlog-statement`, `no-replication-grant`, `server-id-zero` on the mysql profile | each break fails at the named check and the recorded agent fixes it | U2a, Q8a |
| U2c | C7 + C6 | Opus 5.5 | 1 | scenarios: the three U2b breaks and `setup/compose-mysql-clean`; eval replay | the four score; Postgres setup pass rate unchanged | U2b |
| U2-R | A8 | Opus 5.5 | 1 | real runs: MySQL 8.4 on Compose, Linux and Windows, from an empty directory | two exports in the write-up | U2a, U1-R |

## Step 3: transforms (SMT) and sink targets

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U3a | C22 | Opus 5.5 | 2.5 | setup/knowledge: SMT index (`ExtractNewRecordState`, `ByLogicalTableRouter`, `Filter`, `ContentBasedRouter`, `TimezoneConverter`, outbox, the Connect built-ins) and sink index (JDBC sink, Elasticsearch, S3, BigQuery, Snowflake, HTTP) as embedded YAML: keys, defaults, version ranges, the check each implies, the renamed classes from Q1b | at least 60 entries citing the Debezium or Confluent docs; a bad range fails start-up; a table test resolves each renamed class | U0, Q1b |
| U3b | A9 | Opus 5.5 | 1 | setup/tools: `setup_config_help {kind: smt \| sink, name}` over the index; the connector config builder adds SMT and sink keys through it; sink connector registration through Q5c | the tool reads no customer system; a config with an unknown SMT class gets the nearest known class; a sink config passes Q4a's validate on the dev stack | U3a, U1-9 |
| U3c | C19 | Opus 5.5 | 1 | setup/targets/compose: sink services (a JDBC sink to Postgres, the dev warehouse) and their plugin install | integration: source and sink on one Compose, the canary row in the sink (rung 7) | U3b, U1-3 |
| U3d | C7 | Opus 5.5 | 0.5 | scenarios: `setup/unwrap-missing`, `setup/jdbc-sink-no-pk` | labels name the SMT or sink key | U3b |
| U3e | C14 | Opus 5.5 | 0.5 | docs: what the agent knows about transforms and sinks, and what it does not | citations per claim | U3a |

## Step 4: SQL Server prerequisites

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U4a | A10 | Opus 5.5 | 2.5 | clients/sqlserver: read-only client (`SourceSettingsReader` subset: CDC enabled per db and table, SQL Server Agent state, the capture user's grants, version and edition) and the admin methods (`sp_cdc_enable_db`, `sp_cdc_enable_table`, grants) on a write pool | `go-mssqldb` listed with why; 10 s per call; identifiers quoted with `[]` after the rule 6 check (a line in `AGENTS.md` §4 requested in the PR); integration on the mssql profile | U0 |
| U4b | A10 | Opus 5.5 | 1.5 | setup/sources/sqlserver: the driver, canary table, connector config | each check names its fix op and floor; Agent stopped is a check whose fix is T2 (`service_restart` on a host target) or a documented ask on managed SQL | U4a, U1-9 |
| U4c | A1 | Opus 5.5 | 1 | dev: compose profile `mssql` (`mcr.microsoft.com/mssql/server`, CDC off by default), `setup-break` `agent-stopped`, `cdc-not-enabled-table` | profile healthy in < 3 min; each break fails at the named check | U4a |
| U4d | C7 + C6 | Opus 5.5 | 1 | scenarios: `setup/compose-sqlserver-clean` and the two U4c breaks; eval replay | the three score | U4c |
| U4-R | A10 | Opus 5.5 | 1 | real runs: SQL Server 2022 on Compose, Linux; Azure SQL as a manual check (the sandbox of plan §7 item 6) | exports in the write-up; the Azure run marked manual with its date | U4b, U1-R |

## Step 5: container and worker logs in the loop

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U5a | C19 | Opus 5.5 | 1 | setup/targets/compose + host: `container_logs`, `journal_tail` (T0; 200 lines, 64 KiB, redacted, newest last) | capped and redacted on the runner; a container not in the project is refused | U1-3 |
| U5b | A4 | Opus 5.5 | 1 | agent: after a failed step read `container_logs` or `connect_worker_logs` (Q3c) before re-planning; the known-issues lookup with the failing version | prompt snapshot; a recorded test shows the second plan citing the log line; CDC evals unchanged | U5a, Q3c |
| U5c | C7 | Opus 5.5 | 0.5 | scenarios: `setup/plugin-after-scan` (the log shows the worker scanned before the install) | the label names the log line class | U5b |

## Step 6: embedded engine

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U6a | C22 | Opus 5.5 | 2 | setup/knowledge: embedded reference project templates (Maven and Gradle, Postgres and MySQL), the version mismatch rules (engine, connector, Kafka client, Quarkus) | the templates build and run against the dev stack in an integration test; each rule cites a release note | U0, Q1b |
| U6b | A9 | Opus 5.5 | 1 | setup/ops: `project_inspect` (T0: build files, dependency versions), the reference project write under `<project dir>/piperoom-reference/` (T1) | the agent never writes outside that directory in embedded mode (test); `project_inspect` reads build files only, never source | U6a, U1-2 |
| U6c | C7 | Opus 5.5 | 0.5 | scenarios: `setup/embedded-version-mismatch`, `setup/embedded-offset-storage` | the label names the mismatch and says "diagnosed, reference project generated" | U6b |

## Step 7: MongoDB prerequisites

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U7a | A10 | Opus 5.5 | 2 | clients/mongodb: read-only client (replica set status, version, change streams support, the user's roles) and admin methods (create the oplog user, grant roles) | the MongoDB Go driver listed with why; 10 s per call; integration on the mongo profile | U0 |
| U7b | A10 | Opus 5.5 | 1.5 | setup/sources/mongodb: the driver, canary collection, connector config (`mongodb.connection.string` with a `${secret:}` reference) | each check names its fix op and floor; a standalone `mongod` is a check whose fix is T1 only when the setup created it | U7a, U1-9 |
| U7c | A1 | Opus 5.5 | 1 | dev: compose profile `mongo` (a one-member replica set), `setup-break` `standalone-mongod`, `no-oplog-role` | profile healthy in < 2 min; each break fails at the named check | U7a |
| U7d | C7 + C6 | Opus 5.5 | 0.5 | scenarios: `setup/compose-mongodb-clean` and the two U7c breaks; eval replay | the three score | U7c |
| U7-R | A10 | Opus 5.5 | 0.5 | real runs: MongoDB 7 on Compose, Linux; Atlas as a manual check | exports in the write-up | U7b, U1-R |

## Step 8: plain hosts: systemd, a Windows service

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U8a | C19 | Opus 5.5 | 2.5 | setup/targets/host (Linux): `service_status`, `properties_read` (T0); `properties_write` (T1 under the project dir, T2 elsewhere), `service_install` (a systemd unit from a template), `service_restart` (T2); standalone versus distributed properties, the plugin path | `systemctl` argv built from typed params only; a unit file preview is a diff; integration on an ubuntu CI runner with a user-level systemd unit: an empty dir to a RUNNING connector | U1-2, U1-0 |
| U8b | C19 | Opus 5.5 | 2.5 | setup/targets/host (Windows): the service through `svc/mgr`, the properties and plugin path with Windows paths, Java detection | `go doc` for every `mgr` call in the PR; unit tests with a fake manager; the real run on the dev machine (U8-R) is the integration | U8a, U1-0 |
| U8c | A1 | Opus 5.5 | 1 | devctl: `setup-break` `wrong-standalone-args`, `plugin-path-unset`, `java-too-old` on a host target (a container that plays a host on Linux) | each break fails at the named check | U8a |
| U8d | C7 | Opus 5.5 | 0.5 | scenarios: the three U8c breaks | labels name the properties key or the argv | U8c |
| U8e | C14 | Opus 5.5 | 0.5 | docs: the host target (systemd, Windows service), what the agent needs (sudo or not, a service account) | citations per claim | U8b |
| U8-R | A1 (Windows), C19 (Linux) | Opus 5.5 | 1 | real runs: Ubuntu 24.04 with systemd, Windows 11 as a service, from an empty directory | two exports in the write-up | U8b |

## Step 9: Oracle prerequisites

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U9a | A10 | Opus 5.5 | 2.5 | clients/oracle: read-only client (archive log mode, supplemental logging per db and table, the LogMiner user's grants, CDB and PDB names, version and edition) and admin methods (the grants of the Debezium docs, supplemental logging) | `go-ora` (pure Go, no instant client) listed with why; 10 s per call; identifiers quoted after the rule 6 check; integration on the oracle profile | U0 |
| U9b | A10 | Opus 5.5 | 2 | setup/sources/oracle: the driver, canary table, connector config (`database.pdb.name`, LogMiner) | archive log mode off is a T3 check on anything not created by the setup (a restart); 19c and 21c free editions covered, another edition says so | U9a, U1-9 |
| U9c | A1 | Opus 5.5 | 1.5 | dev: compose profile `oracle` (`gvenzl/oracle-free`), `setup-break` `no-archivelog`, `no-supplemental-logging`, `missing-logminer-grant` | profile healthy in < 5 min; each break fails at the named check | U9a |
| U9d | C7 + C6 | Opus 5.5 | 1 | scenarios: `setup/compose-oracle-clean` and the three U9c breaks; eval replay | the four score | U9c |
| U9-R | A10 | Opus 5.5 | 0.5 | real run: Oracle Free 23 on Compose, Linux | export in the write-up | U9b, U1-R |

## Step 10: tuning knowledge

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U10a | C22 | Opus 5.5 | 1.5 | setup/knowledge: tuning index (heap, `max.batch.size`, `max.queue.size`, `poll.interval.ms`, snapshot fetch size, `tasks.max` per source, producer settings) with the symptom each answers | at least 25 entries citing the docs; `setup_config_help {kind: tuning}` | U3a |
| U10b | A4 | Opus 5.5 | 0.5 | agent: the plan proposes a tuning step only after a symptom (a slow snapshot, OOM in the logs), never by default | prompt snapshot; evals unchanged | U10a, U5b |

## Step 11: Debezium Server

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U11a | C20 | Opus 5.5 | 2.5 | setup/targets/server: the properties template per sink (Kinesis, Pub/Sub, Redis, HTTP, Kafka), the server as a Compose service or a systemd unit, `server_status` (T0) | the sink config keys come from the knowledge index; integration on Linux: Postgres to the HTTP sink (a loopback receiver) with the canary event | U3a, U1-3, U8a |
| U11b | C20 | Opus 5.5 | 1.5 | setup/targets/server: `sink_probe` (T0: reachability and auth for Kinesis, Pub/Sub, Redis, HTTP; the cloud ones over the shared AWS config and a GCP token the runner holds) | no credentials in any receipt; `httptest` and a local Redis for tests; the cloud probes have recorded fakes | U11a |
| U11c | C7 | Opus 5.5 | 0.5 | scenarios: `setup/server-sink-unreachable`, `setup/server-offset-file-unwritable` | labels name the probe or the property | U11b |
| U11d | C14 | Opus 5.5 | 0.5 | docs: the Debezium Server target | citations per claim | U11b |
| U11-R | C20 | Opus 5.5 | 0.5 | real run: Debezium Server to Redis on Compose, Linux | export in the write-up | U11b |

## Step 12: Kubernetes with Strimzi

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U12-0 | leads | Opus 5.5 | 1 | design: `docs/design/setup-strimzi.md` (the CRD versions, the in-cluster runner Job and its RBAC for one namespace, how the pairing code reaches a Job, `kubectl` versus the API from the runner) | the page answers each; item 7's Strimzi part lands in code with it | U0 |
| U12a | C20 | Opus 5.5 | 2.5 | setup/targets/strimzi: `k8s_get`, `k8s_logs`, `k8s_events`, `strimzi_build_status` (T0, one namespace); `k8s_apply` (T2, `--dry-run=server` as preview), `k8s_secret_apply` (T2) | the namespace comes from `Env.Targets` only; a manifest with a kind outside `KafkaConnect`, `KafkaConnector`, `Secret`, `ConfigMap` is refused; `kubectl` argv from typed params only; unit tests with a fake `kubectl` on `PATH` | U12-0, U1-2 |
| U12b | C20 | Opus 5.5 | 2 | setup/targets/strimzi: the `KafkaConnect` manifest with `build.plugins` (the Debezium artifact with its SHA-512), the `KafkaConnector` manifest, the ladder over `KafkaConnector` status conditions | kind integration: an empty namespace to a Ready connector and the canary event in < 20 min | U12a |
| U12c | C1 | Opus 5.5 | 1.5 | deploy/helm + ci: the in-cluster runner Job (image, RBAC, the pairing code as a Secret), the nightly kind job | `helm template` with `setup.runner.enabled=true` renders the Job and the Role for one namespace; the nightly kind job runs U12b's integration and posts the export | U12b |
| U12d | C7 | Opus 5.5 | 0.5 | scenarios: `setup/strimzi-build-failed`, `setup/strimzi-connector-not-ready` | labels name the condition | U12b |
| U12e | C14 | Opus 5.5 | 0.5 | docs: the Strimzi target and the in-cluster runner | citations per claim | U12c |
| U12-R | C20 | Opus 5.5 | 1 | real run: a kind cluster on Linux and a real cluster the owner names (manual) | exports in the write-up | U12c |

## Step 13: managed: MSK Connect, Confluent Cloud, Aiven

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U13-0 | leads | Opus 5.5 | 1.5 | design: `docs/design/setup-managed.md` (the three clients' auth: SigV4 over the shared AWS config, Confluent Cloud API keys, Aiven tokens; which operations each exposes; the recorded-fakes harness; the sandbox accounts of plan §7 item 6) | the page answers each; item 7's managed part lands in code with it | U0 |
| U13a | C21 | Opus 5.5 | 1 | setup/targets/managed: the recorded-fakes harness (`httptest` cassettes, secrets scrubbed at record time, a `//go:build manual` re-record) | a cassette with a credential-looking string fails the test that loads it | U13-0 |
| U13b | C21 | Opus 5.5 | 2.5 | clients/mskconnect + ops: `msk_describe_cluster`, `msk_list_plugins`, `msk_describe_connector`, the IAM, VPC and security group reads (T0); `msk_create_plugin` (from an S3 object the person uploaded), `msk_create_worker_config`, `msk_create_connector`, `msk_delete_connector` (T2; T3 on production) | the AWS SDK calls confirmed with `go doc`; the cassettes cover 400, 403, 404, 409, throttling; the manual check on the sandbox account dated in the PR | U13a |
| U13c | C21 | Opus 5.5 | 2 | clients/confluentcloud + ops: plugins, connector status, cluster (T0); `ccloud_create_connector` (validate then create), `ccloud_acl_create` (T2) | basic auth over https only (`netsec`); cassettes; manual check dated | U13a |
| U13d | C21 | Opus 5.5 | 2 | clients/aiven + ops: service, connectors, plugins (T0); `aiven_create_connector`, `aiven_integration_create` (T2) | token in a header, never a URL; cassettes; manual check dated | U13a |
| U13e | C7 + C6 | Opus 5.5 | 1 | scenarios: `setup/msk-plugin-s3-denied`, `setup/msk-vpc-no-route`, `setup/ccloud-acl-missing`, `setup/aiven-plugin-unavailable`; eval replay over the cassettes | the four score | U13b, U13c, U13d |
| U13f | C14 | Opus 5.5 | 1 | docs: the three managed targets, the IAM policy, the API key scopes, the token scope | citations per claim | U13d |
| U13-R | leads | Opus 5.5 | 1 | manual runs on the three sandbox accounts, RDS Postgres as the source | three exports in the write-up, dated | U13e |

## Step 14: the measurement

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U14 | leads | Opus 5.5 | 1 | `reviews/setup-runs-2026-11.md`: the first ten real setups (wall clock, steps, approvals, tokens, what blocked), the coverage re-count against the label files, the next wave's candidates | the numbers are in the plan's §8 caveats paragraph as a follow-up; plan §7 items 1, 4 and 8 revisited with data | U13-R |

## New folders and owners

| Agent | Owns (new in the U wave) |
|---|---|
| A9 (Qoder, new) | `cmd/setup/**`, `cmd/piperoom/**`, `internal/setup/**` except the `targets/`, `sources/` and `knowledge/` subfolders below |
| A10 (Qoder, new) | `internal/clients/sqlserver/**`, `internal/clients/mongodb/**`, `internal/clients/oracle/**`, `internal/setup/sources/sqlserver/**`, `internal/setup/sources/mongodb/**`, `internal/setup/sources/oracle/**` |
| A3 | `internal/setup/sources/postgres/**` |
| A8 | `internal/setup/sources/mysql/**` |
| C19 (cloud, new) | `internal/setup/targets/compose/**`, `internal/setup/targets/host/**` |
| C20 (cloud, new) | `internal/setup/targets/strimzi/**`, `internal/setup/targets/server/**` |
| C21 (cloud, new) | `internal/setup/targets/managed/**`, `internal/clients/mskconnect/**`, `internal/clients/confluentcloud/**`, `internal/clients/aiven/**` |
| C22 (cloud, new) | `internal/setup/knowledge/**` |
| C7 | `evals/scenarios/setup/**` (C6 keeps the rest of `evals/**`, `evals/setup/**` included) |

A5's, A4's, A6's, A1's, C1's, C4's and C14's rows stay inside their existing folders. If no ninth or tenth local
agent runs, A2 takes the A9 rows and A3 the A10 rows, in that order of priority; the After column does not change.

## Load per agent

| Agent | PRs | Days | Critical for |
|---|---|---|---|
| A9 | 10 | 14.5 | the core: protocol, runner, catalogue, tools and actions, standalone; the longest chain |
| C19 | 6 | 10.5 | Compose and host targets |
| A10 | 9 | 14 | SQL Server, MongoDB, Oracle |
| C21 | 4 | 7.5 | managed |
| C20 | 7 | 9.5 | Strimzi and Debezium Server |
| C22 | 3 | 6 | knowledge |
| A5 | 3 | 5 | the runner endpoint, tiering, registration |
| A1 | 8 | 10 | dev profiles and breaks, Windows runs |
| A3 | 2 | 3 | Postgres driver, owners reload |
| A4 | 3 | 3.5 | setup mode |
| A6 | 1 | 2.5 | the setup UI |
| A8 | 2 | 3 | MySQL |
| C7 (+C6 on shared rows) | 12 | 8 | scenarios |
| C6 | 1 (+ shared) | 1.5 | eval replay |
| C14 | 6 | 4 | docs each step |
| C1 | 2 | 3 | CI matrix, Helm Job, kind |
| C4 | 1 | 1 | threat model |
| leads | 6 | 8 | contracts, three designs, manual runs, the measurement |

Total about 115 agent-days over 86 PRs; 94 of those days are the per-step work the plan's curve counts. The critical
path is A9: U1-0, U1-1, U1-2, U1-7, U1-11, U1-R, about 10 working days, then everything else fans out. The
second-longest chain is A10's three databases (about 14 days, sequential). If A9 slips, C19's Compose driver and A3's
Postgres driver can be reviewed against the protocol fakes and merge first.

## Suggested weekly dispatch (all agents in parallel)

| Week | Lands |
|---|---|
| 1 | U0; U1-0, U12-0, U13-0; U1-1, U1-5, U1-13, U3a, U4a, U13a |
| 2 | U1-2, U1-3, U1-4, U1-6, U1-8, U1-9, U1-12, U1-17, U4b, U4c, U6a, U7a, U9a, U12a, U13b |
| 3 | U1-7, U1-10, U1-11, U1-14, U1-15, U2a, U2b, U3b, U3c, U5a, U7b, U7c, U8a, U9b, U9c, U12b, U13c, U13d |
| 4 | U1-16, U1-18, U1-19, U1-20, U1-R, U2c, U3d, U3e, U4d, U5b, U5c, U6b, U6c, U7d, U8b, U8c, U9d, U10a, U11a, U12c, U13e, U13f |
| 5 | U1-21, U2-R, U4-R, U7-R, U8d, U8e, U8-R, U9-R, U10b, U11b, U11c, U11d, U11-R, U12d, U12e, U12-R, U13-R; U14 |

Review load for the leads peaks at about 20 PRs in weeks 3 and 4; the code review of C* PRs goes to `[LEAD-CLOUD]`
as usual, and every target row's real run is checked by Lead 1 from the attached export before the merge.
