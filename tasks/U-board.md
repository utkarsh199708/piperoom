# U wave dispatch board: one row = one PR

Plan `docs/product/setup-2026-10.md`: 572 of the 722 Stack Overflow Debezium problems (79%) are first-time setup,
and the fix for most of them lives in the user's own image, compose file, manifests, worker properties or network.
This wave gives Piperoom a setup capability (the owner's decision: a functionality of the one product, not a
separate one): the same agent, in the same room, with the same approval model, takes a user from nothing to a
watched pipeline through a runner that executes typed operations where the user's Docker, `kubectl` and cloud
credentials are. Design `docs/design/setup-agent.md` (rev 2 after the #641 reviews). The wave takes agent-fixable
setup problems from 0 to 473 of 572 (82.7%); the per-step curve is plan §4. Each row is a PR an agent finishes in
one or two sessions (target < 600 changed lines, §6 of `AGENTS.md`).

- **ID** is the PR tag (`[U1-3a] setup/compose: driver, templates and validator`). A design row is `U<N>-0`.
- **Model**: every row runs on Opus 5.5, cloud rows included (the owner's standing instruction: the strongest model
  for every coding row).
- **Days**: one agent's working days including tests and review fixes. The plan's §4 Days column is the sum of
  every row of a step on this board, design, docs, scenario, eval and real-run rows included; U0 and U14 are
  outside the steps.
- **After**: rows that must be merged first. U0 always; Q rows by their `tasks/Q-board.md` ID; P rows by
  `tasks/P-board.md`.

**Order.** U0, then the design row U1-0 and the U1 rows (the core: runner, catalogue, Compose, Postgres,
verification); then steps 2 to 13 in coverage-per-day order. Step numbers are identifiers; the dispatch order is the plan's §4
table, recomputed from this board's days: U2 MySQL, U3 transforms and sinks, U5 logs, U4 SQL Server, U6 embedded,
U7 MongoDB, U8 plain hosts, U10 tuning, U9 Oracle, U11 Debezium Server, U12 Strimzi, U13 managed. The design rows
U12-0 and U13-0
start as soon as U0 merges (decided, plan §7 item 1), so pulling Strimzi or the managed services forward costs no
rework. An agent with two rows ready takes the lower step first.

**Before the wave.** U0 merges after Q0. U1 codes against Q1a (known issues), Q2a to Q2d (Postgres settings and
admin), Q4a (validate), Q5a and Q5c (lifecycle), Q6a (`net_probe`); each row's After column names them. Three Q
rows gain one line each for this wave, inside their own PRs, noted in the Q threads by the leads with U0: Q6a's
probe takes a location interface (the runner's position) and a target source (the provisional pipeline's hosts);
Q4a accepts `${secret:...}` as a non-secret value and calls `actions.LooksSecret` (U0) instead of its private copy.

**Real runs.** Every target row's "Done when" includes a real run: a person starts `piperoom-setup` on the named
OS against the named target from an empty directory and the room closes `pipeline live`; the room export (JSON) is
attached to the PR. Linux runs happen in CI; macOS on a lead's machine; Windows on the owner's dev machine.

**Grants for this wave** (recorded in `AGENTS.md` and `docs/process/ownership.json`): C1 owns `Dockerfile` and
`deploy/helm/**` for the U rows that name them; `internal/setup/protocol/**` is a contract file (leads). If no
ninth or tenth local agent runs, the leads switch A9's rows to A2 and A10's rows to A3 on this board and in
`ownership.json` in one PR (`ship-check.mjs` reads both), in that order of priority; the After column does not
change.

## U0 items: the contract additions each row needs

Items 1 to 8 of `docs/design/setup-agent.md` §4, written by the leads in the U0 PR: the new contract file
`internal/setup/protocol/protocol.go` (wire types, `RunnerExecutor`, `ErrNoRunner`, `ErrParamsHash`,
`ErrDuplicateStep`); events (`setup.*`, `pipeline.registered`, `SetupReceipt`, `SetupRequest`, the `environment`
layer, `room.created.mode`, `ActionExecutedPayload.Receipt`, `PolicySetupLocal`, `GrantAdmin`); `agent.Alert.Mode`,
`Options.Setup`, `ErrNotInPlan`; `actions.Receipted`, `actions.Tiered`, `actions.LooksSecret`;
`clients.SetupSourceWriter`; the owners schema (`tenants.<t>.admins`, `tenants.<t>.repos`, `setup:`, `OWNERS_DIR`,
`Provisional`, `Overlap`, `Reload` signatures); the config variables; the `docs/api.md` routes and snapshot
fields. Item 9 (protocol versioning and the Windows service API; Strimzi CRDs and the in-cluster runner; the
managed clients' auth) is settled by the design rows U1-0, U12-0 and U13-0, each landing its own contract change.
New methods go on **new** interfaces and new events, so every existing fake and the eval replay compile unchanged.

## Step 0: contracts and the core design

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U0 | leads | Opus 5.5 | 2 | contracts: the protocol contract file, setup events and `SetupRequest`, `Alert.Mode`, `Options.Setup`, `Receipted`, `Tiered`, `LooksSecret`, `SetupSourceWriter`, owners schema, config, API | items 1 to 8 in code and `docs/api.md`; `clients.go` imports nothing under `internal/setup`; `LayersOf("cdc")` gains `environment` at the end and every existing owners file still loads; `PIPEROOM_SETUP_ENABLED=on` with write mode off is refused at start; all stubs and fakes compile; `go build`, `go vet`, `go test ./...` green; the three Q-thread notes posted | Q0 |
| U1-0 | leads | Opus 5.5 | 1.5 | design: protocol field list and versioning, the Windows service spike, re-estimate of U1 and U8 | the protocol types final with a `Version` and a compatibility rule (a runner one minor behind still pairs); a 30-line spike on the dev machine proves `golang.org/x/sys/windows/svc/mgr` can install, start and query a service (`go doc` output in the PR) and `os.Root` refuses the Windows forms of design §2.3 (`go doc os.Root` in the PR); the U1 and U8 day estimates revised on this board | U0 |

## Step 1: the core: runner, catalogue, Compose target, Postgres prerequisites, verification

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U1-1 | A9 | Opus 5.5 | 2.5 | setup/runner: pairing with local confirmation, `Hello`, ping, token idle expiry and rotation, `Resume` reconciliation, `params_hash` recomputation, duplicate-step refusal, the local op log | `httptest` fake server: the runner prints origin, room title and creator and waits for `y` (refuses `--yes` off loopback); a used or expired code is refused; a reconnect after a drop sends `Resume` and a `done` op is never re-run; an idle token expires at 30 min and rotates on reconnect; a `params_hash` mismatch and a duplicate `(proposal_id, step_index)` are refused and reported as `setup.runner_refused`; the op log survives a restart; frames over 1 MiB cut; TLS unless loopback; the token never in a log line; the runner keeps its own target registry with classes from the pairing confirmation, raised by every `setup.target_classed`, never lowered, and raises `OpRequest.Class` to that floor before the guard (a server that sends `shared` for a production target is refused, test); every T3 op waits for `y` on the terminal | U1-0 |
| U1-2 | A9 | Opus 5.5 | 2 | setup/ops: `Op`, `Registry`, `os.Root` confinement, the rule tests; T0 ops `env_detect`, `file_read`, `dir_list`, `jar_list`, `java_version`, `http_get`; `net_probe` over Q6a from the runner; `file_write` and `file_delete` (non-executed files) | every file op opens through `os.Root` and refuses any symlink, junction or reparse point in the path; table tests for `..`, absolute, `.git` variants, `.piperoom/**`, and on Windows drive-relative, UNC, `\\?\`, ADS, reserved names, junctions (run on all three CI legs); `file_write` refuses the executed file kinds listed below; `http_get` refuses a host not in `Env.Targets`; a T1+ request without `proposal_id` is refused; an exec op builds an argv slice, no shell (an AST test as `wiring/readonly_ast_test.go`); `jar_list` flags flat jars, two Debezium versions, missing converter jars; content keys checked with `actions.LooksSecret`; `file_write` refuses a path under any directory a template mounts or builds from (plugin dir, worker config, log4j, every `volumes:` source and `build:` context the driver registered), the names `.env*`, `Dockerfile*`, `*.jar`, `*.sh`, `*.ps1`, `*.bat`, `*.service`, the compose names `compose.yaml`, `compose.yml`, `compose.override.yaml`, `compose.override.yml`, `docker-compose*.y*ml`, non-UTF-8 or over-256-KiB content, and a destination that is a device file, FIFO or socket; `file_read` and `dir_list` skip non-regular files; one `net.Dialer` with a `Control` hook refuses the denylisted ranges against the address being dialed, and a test with a resolver answering `169.254.169.254` for a listed name asserts the refusal names that address | U1-0, Q6a |
| U1-2b | A9 | Opus 5.5 | 1.5 | setup/remoteclients: `clients.ConnectClient`, `ConfigValidator`, `ConnectLifecycleWriter`, `SourceSettingsReader`, `SourceAdminWriter`, `SetupSourceWriter`, the Kafka and Registry readers over one op per method | the Q tools and Q actions (`connect_validate_config`, `connect_create_connector`, `pg_set_replica_identity`, `source_grant_select`) construct over the remote clients and pass their own unit tests against a fake runner; every writer method is a T2 op that carries the proposal fields; a method with no runner answers `ErrNoRunner` | U1-2, Q2b, Q2c, Q2d, Q4a, Q5a, Q5c |
| U1-3a | C19 | Opus 5.5 | 1.5 | setup/targets/compose: templates (KRaft Kafka, Connect with the plugin dir, optional Registry, a local Postgres), `compose_write`, `properties_write` with the key allow-list, the compose validator | the validator refuses, with a table test per rule: an image not in the knowledge allow-list or without its pinned digest, `privileged`, `cap_add`, `devices`, `security_opt`, `pid`, `ipc`, `network_mode: host`, `userns_mode`, a bind mount outside the project dir, the Docker socket, `/`, `extends`, a `build:` outside the templates, a port not bound to `127.0.0.1` on a local target; a validator failure makes the step T2 with the reason in the preview; `properties_write` refuses an unknown key and a `plugin.path` outside the project dir; the `.env` allow-list excludes `COMPOSE_*` and `DOCKER_*`; every compose op's argv is `docker compose --project-directory <dir> -f <rendered file> --env-file <rendered .env> <subcommand> [services]` and nothing else (argv test per op); the driver registers every mounted and build directory with the ops package when it renders | U1-2, U1-24 |
| U1-3b | C19 | Opus 5.5 | 1.5 | setup/targets/compose: T0 `docker_info`, `compose_ps`, `compose_config` (masked render), `container_inspect` (env values dropped); T1 `compose_up`, `compose_restart`, `compose_down` (keeps volumes), `plugin_install`; Docker Desktop differences | `plugin_install` downloads only from `PIPEROOM_SETUP_PLUGIN_HOSTS` and verifies the digest pinned in the knowledge index, one plugin per directory; `container_inspect` output has env names and no values (test); macOS and Windows socket paths and host networking handled with fakes; integration on Linux: an empty dir to a healthy Connect with the Postgres plugin listed by `GET /connector-plugins` | U1-3a |
| U1-4 | A9 | Opus 5.5 | 1.5 | setup/runner: the secret store, `secret_set`, materialisation, literal redaction, denied paths, HMAC receipts | the store is 0600 and in the generated `.gitignore`; `file_read`, `dir_list`, `file_write`, `file_delete` refuse `.piperoom/**` and every materialised path; every stored value is replaced literally in every `Summary`, `Data` and receipt (a test greps every frame, including a `file_read` of a materialised properties file and a `container_inspect`); values under 6 bytes refused; `OutMAC` is HMAC-SHA256 under a per-runner key; `secret_set` prompts on the terminal or takes the UI frame; the runner generates the read role's password (`CreateReadRole` returns only a secret name); "Register read credentials" prints the server origin and the names, waits for `y`, and sends one `Credentials` frame to the server; the value is in no op result and no event (test) | U1-1, U1-2 |
| U1-5a | A5 | Opus 5.5 | 2 | room + ws: runner registry keyed by (tenant, room, creator), `GET /api/runners/ws`, pairing codes (8 chars, single use, 10 min, 5 attempts per code, 20 per source and 100 in all per 10 min, `subtle.ConstantTimeCompare`), token expiry and rotation, `setup.pairing`, `setup.runner_attached`/`lost`/`refused`, `RunnerExecutor`, `Resume` reconciliation | fake-clock tests per rule; a code pairs once; the sixth attempt is `rate_limited` and audited with the source; a replacement after loss is audited; `Execute` times out at the deadline and answers `ErrNoRunner` without a runner; on `Resume` a `running` or `done` op is never re-sent; replicas: the socket lives on one replica and `Execute` from another is routed through the room lease holder (`docs/design/replicas.md` §3) or refused with a clear error, decided and tested; `PIPEROOM_SETUP_ENABLED=off` hides the endpoint; the 101st attempt closes pairing for 10 min and is audited | U0 |
| U1-5b | A5 | Opus 5.5 | 1.5 | httpapi + room: `POST /api/setup` (tenant membership, the provisional pipeline), the targets route with the address denylist, class raise (floors, `max`), pipeline extend, maintenance window, snapshot `setup` | `POST /api/setup` from a non-member of the tenant is `403`; a `pipeline_id` that exists is `409`; the targets route refuses link-local, metadata, multicast, unspecified, other hosts' loopback and the server's own addresses, and never registers a class below `shared`; a class is raised and never lowered; extend emits `setup.pipeline_extended` and the room's scope changes; a window over 4 h or more than 24 h ahead is refused; snapshot fields per `docs/api.md` | U1-5a, U1-13 |
| U1-5c | A5 | Opus 5.5 | 1.5 | quickstart: move the `--quickstart` server from `cmd/server/quickstart.go` into `internal/quickstart` with options `WriteMode` and `SetupEnabled`; `cmd/server --quickstart` unchanged | `cmd/server --quickstart` still refuses write mode on and a non-loopback address (the existing tests pass unchanged); `quickstart.Run` with `WriteMode: on, SetupEnabled: true` serves the setup routes on loopback with the in-memory store and builds no write client for any pipeline but the provisional one; `testenv.Quickstart(t)` unchanged | P1a-2, U1-5a |
| U1-5d | A5 | Opus 5.5 | 1 | httpapi: `POST /api/rooms/{id}/setup/secret` (driver only, value in the body, excluded from request logging), `POST /setup/credentials` (driver only, no body: asks the runner), the runner's `Credentials` frame into `PIPEROOM_SECRETS_DIR`, `setup.secret_set`, `setup.credentials_registered` | a non-driver gets `403`; the value appears in no log line and no event (a test with a capturing logger); the secret frame reaches the fake runner once and the server keeps nothing; credentials arrive only in the runner's frame, land in `PIPEROOM_SECRETS_DIR/<pipeline>/` 0600, appear in no event (names only) and the path is what `connections:` will reference; a server with setup enabled that cannot write the directory refuses to start; the Helm values and the pilot compose mount one shared volume for it | U1-5a |
| U1-6 | A5 | Opus 5.5 | 2 | room: tiered routing (`actions.Tiered` with the class of the resolved endpoint): T0 refused as a proposal, T1 scheduled by policy `setup_local` (`PIPEROOM_SETUP_LOCAL_DELAY`), T2 to the approver of design §3.6 (overlapping pipeline's owners, else a tenant admin with grant `admin`; fallback only in solo mode and standalone), T3 plus typed confirm and the maintenance window; the guard at `Create` and `Execute`; `ErrNotInPlan` | fake-clock tests per tier; the guard refuses each listed op on a production target at `Create` and again at `Execute` with a reason; a T3 window step outside the recorded window is refused at both points; on a team server a non-admin driver cannot approve a T2 step on a shared target; a proposal whose step is not in the latest `setup.plan` is refused; `Class` and the proposal fields are in every `OpRequest` | U1-5b, U1-8, U1-13, P2b-4 |
| U1-7 | A9 | Opus 5.5 | 1.5 | setup/tools + setup/actions: the setup-only KindRead tools, the KindRoom `setup_plan` and `setup_request_verification`, the setup-only action adapters (`Preview`, `Scoped`, `Tiered`, `Receipted`) | every forwarding tool is `KindRead` and registers in a read-only registry; `setup_plan` refuses an op not in the catalogue; `setup_request_verification` emits nothing but a request (the agent's `Sink` refuses `setup.verified`, test); the adapters' `Preview` calls the runner's T0 preview op only; `ExecuteReceipt` returns the runner's receipt; `CheckScope` refuses a target outside the provisional pipeline | U1-2b, U1-5a |
| U1-8 | A9 | Opus 5.5 | 1.5 | setup/plan: environment classes (floors, receipts-only "created by this room"), `TierOf`, the guard table, the bounds, the ladder definition (rungs and predicates on structured `Data`) | `TierOf` table-tested over every (floor, class); the classifier reads only structured `Hello` and `env_detect` fields, never derives `production` from a host name alone, never lowers a floor, and counts "created by this room" only from `action.executed` receipts; the guard table has a test per row; `Ladder(postgres, compose, none)` is the eight rungs of design §3.4 with a predicate that ignores `Summary` | U1-2 |
| U1-8b | A5 | Opus 5.5 | 1.5 | room: the ladder runner (server nonce, the canary insert as a system-origin proposal, reads issued by the room, `setup.verified`, `setup.awaiting_change` for rung 8, `POST /api/rooms/{id}/setup/verify`) | only the ladder runner can append `setup.verified` (an agent `Sink` attempt is refused, test); the nonce is 16 random bytes and the predicate matches only an event with that key; every rung records a step id and receipt id that exist in the room log after the last executed step; a stale event from a previous run does not match; rung 8 waits up to 10 min and times out as a note, not a failure | U1-8, U1-7, U1-6 |
| U1-9 | A3 | Opus 5.5 | 2 | setup/sources/postgres: the prerequisite driver over Q2 (`wal_level`, slots, senders, publication, `REPLICA IDENTITY`, grants, RDS parameter group), `SetupSourceWriter` for Postgres (publication, canary table and insert, read role with the `permissions.md` grants, `AlterSystem` on the allow-list only), the connector config with `${secret:}` references | each check names its fix op and tier floor; `AlterSystem` refuses a name outside the allow-list before any I/O; `CreateReadRole` grants SELECT on the listed tables and `pg_read_all_stats` only and returns the secret name the runner stored the password under, never the password; identifiers through `Sanitize()`; the connector config passes Q4a's validate on the dev stack; integration with `it_` objects | U1-2b, Q2a, Q2d, Q4a |
| U1-10 | A4 | Opus 5.5 | 2 | agent + wiring: setup mode (prompt frame, bounds, plan-before-propose, re-plan after failure, request-not-report verification), `wiring.SetupStack` over the remote clients, the write-mode rule | prompt snapshot for setup mode; a recorded-model test takes a fake runner from an empty dir to a ladder request and the fake ladder runner's green `setup.verified`; after a failed step the next proposal without a new `setup_plan` is refused and the model recovers; with write mode off no setup action and no remote writer is built (the P1a-3 test extended); CDC evals pass rate unchanged | U1-6, U1-7, U1-8b, U1-9, Q1d |
| U1-11 | A9 | Opus 5.5 | 1.5 | cmd/setup + setup/standalone: the runner binary, `--standalone` over `internal/quickstart` (write mode and setup on, loopback, `--yes` allowed), `setup-log.jsonl`, `--watch` | `piperoom-setup --standalone --project ./orders` prints a URL in < 10 s on the three OSes (CI matrix); refuses a non-loopback address; every room event is appended to the log file; `--watch` restarts the standalone server on a written `owners.yaml` and the watcher ticks | U1-1, U1-4, U1-5c |
| U1-12a | A6 | Opus 5.5 | 1.5 | web: New pipeline → Set up (the form with tenant, source, target, tables, targets and classes), the pairing command with copy, the setup stepper above the feed, runner status | Vitest per state (no runner, paired, planning, running step, awaiting approval, blocked); the decision bar shows the first step that needs a person; mobile: the stepper is a bottom sheet | U0 (fake snapshot), U1-5b to verify |
| U1-12b | A6 | Opus 5.5 | 1.5 | web: the targets, class-raise and pipeline-extend controls, the secret and credential fields, the maintenance window, the canary prompt (rung 8), the verified card, the registration card with "watched by" | the secret and credential fields post to their routes and never to a comment (test); a class control only offers higher classes; the registration card says who approves and where the pipeline will be watched, and its "Register read credentials" button only asks the runner and shows "confirm on the runner's terminal"; a11y unchanged | U1-12a, U1-5d, U1-8b |
| U1-13 | A3 | Opus 5.5 | 1.5 | owners: `OWNERS_DIR` and `Reload`, `Provisional(req)`, `Overlap(p)`, `tenants.<t>.admins` and `repos`, the `setup:` block | table tests: a dir file that fails validation is skipped with a logged reason and the rest load; a reload swaps the set atomically; an id in both the file and the dir is refused at start; `Provisional` yields a pipeline every Q `CheckScope` accepts for the request's names and refuses an empty topic prefix; `Overlap` finds a shared topic prefix, connector, table per host and host across tenants; an overlap refusal names only the requester's tenant's pipelines and says "a pipeline in another tenant" otherwise (test) | U0 |
| U1-14 | A5 | Opus 5.5 | 1.5 | room + cmd/server: `pipeline_register` (gated on a green `setup.verified` for the current plan, non-self approver on a team server, overlap refusal, `connections:` with `file:` references, `watched_by`, write to `OWNERS_DIR`, reload, `pipeline.registered`), the PR path when `repos:` is set, the watcher pickup, the room closes `pipeline live` | `Proposals.Create` refuses the action without a green ladder or after a later executed step; on a team server the requester's own approval is refused unless solo; an overlap is refused unless the overlapping pipeline's owners approved this proposal; the written file loads with `owners.Load`, carries `tenant:` and `setup:`, and its non-secret part is byte-equal to `discover -write-owners` for the same pipeline; a `local` target registers `watched_by: local` and the server's watcher skips it; the watcher's next tick lists a `server` pipeline; `GET /api/pipelines` shows `setup`; with `repos:` the PR goes through Q7c and the room waits for `pr_merged` | U1-13, U1-6, U1-8b, U1-5d, Q7c, Q7e |
| U1-14b | A5 | Opus 5.5 | 1 | room: "roll back this setup" (one proposal from the recorded rollbacks in reverse order, each step through the guard again), the room closes `rolled back` | a rollback step the guard refuses on a production target stops the proposal at `Create` with the reason; the rollback of a `compose_up` is `compose_down` without volumes; fake-runner test over a three-step setup | U1-6 |
| U1-15 | A1 | Opus 5.5 | 1.5 | dev + devctl: `setup-break` scenarios (flat plugin dir, missing converter jars, `wal_level=replica`, missing grant, DB unreachable from Connect, plugin added after the worker scan) and the Linux integration test | each break makes a fresh `piperoom-setup` run against the dev stack fail at a named check and the recorded agent fixes it; a compose file with each forbidden key is refused before `compose_up`; `dev/scenarios.md` rows; `testenv.Setup(t)` | U1-11, U1-3b |
| U1-16 | C1 | Opus 5.5 | 1.5 | ci: `setup-matrix.yml` (ubuntu, macos, windows: unit, the §2.3 path tests and detection; ubuntu: Compose integration; macos: Colima when present else a documented skip), the self-hosted Windows nightly job | matrix green on the U1-11 merge commit; the Windows leg runs the path table tests; the nightly runs the Compose integration on the owner's dev machine and posts its export as an artifact | U1-11, U1-15 |
| U1-17 | C4 | Opus 5.5 | 1 | docs/security: threat-model section for the runner, covering S1 to S7 of the #641 review by name (artifact execution, pairing and replay, tenant binding, secrets, confinement, the ladder, target reach) | every claim cites a file; findings become U1 fix rows before U1-R | U1-3a, U1-4, U1-5a, U1-5b, U1-6, U1-8b |
| U1-18 | C14 | Opus 5.5 | 1 | docs/pilot/setup-agent.md: the user's page (install, pair and confirm, targets and classes, what auto-runs, what asks and who approves, the production guard, the secret store, how the pipeline is watched) | citations per claim; ≤ 140 lines | U1-11, U1-14 |
| U1-19 | C7 | Opus 5.5 | 1 | scenarios: `setup/compose-postgres-clean`, `setup/flat-plugin-dir`, `setup/wal-level-replica`, `setup/missing-grant`, `setup/db-unreachable-from-connect`, `setup/compose-privileged-refused` | each label names the failing check and the fixing op, or the validator rule | U1-15 |
| U1-20 | C6 | Opus 5.5 | 1.5 | evals: runner recordings replay (`evals/setup`), a fake ladder runner, per-target and per-source pass rate in the report | the six U1-19 scenarios score; the CDC pass rate is unchanged | U1-19 |
| U1-21 | A9 | Opus 5.5 | 1 | cmd/piperoom: the umbrella binary (`piperoom server`, `discover`, `investigate`, `setup`) | the optional row of plan §7 item 2; each subcommand is the existing `main` with its flags; `piperoom setup` is `piperoom-setup`; the entrypoint change is U1-22's | U1-11 |
| U1-22 | C1 | Opus 5.5 | 0.5 | Dockerfile + README: the image ships `piperoom-setup` (and `piperoom` when U1-21 lands); the README quickstart starts with `piperoom-setup` | the pilot image builds with both binaries and the old entrypoint still works; `make quickstart` unchanged | U1-11 |
| U1-23 | A2 | Opus 5.5 | 0.25 | actions: switch `connector_config.go` and Q4a to `actions.LooksSecret` | the private `looksSecret` is gone; tests unchanged | U0 |
| U1-24 | C22 | Opus 5.5 | 1 | setup/knowledge: the image allow-list with pinned digests (Debezium Connect and Server, Kafka, Schema Registry, the local database images) and the plugin artifact digests per Debezium release | each entry cites the registry or Maven Central; a bad digest fails start-up; a table test resolves each Debezium release to its plugin digest | U0 |
| U1-R | A9 (Linux, macOS), A1 (Windows) | Opus 5.5 | 1.5 | real runs: Linux, macOS Docker Desktop, Windows Docker Desktop, each from an empty directory to `pipeline live`; the write-up of the three runs in `reviews/setup-runs-2026-11.md` (leads' folder; A9 posts, the leads commit) | three exports attached; wall clock, steps, approvals and tokens per run in the write-up; every failure the agent could not fix is a row on this board or an issue | U1-10, U1-11, U1-12b, U1-14, U1-15, U1-16 |

## Step 2: MySQL prerequisites

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U2a | A8 | Opus 5.5 | 2 | setup/sources/mysql: the driver over Q8 (`log_bin`, `binlog_format`, `binlog_row_image`, GTID, `server_id`, `binlog_expire_logs_seconds`, grants, the RDS parameter group), `SetupSourceWriter` for MySQL (canary table and insert, read role; `CreatePublication` and `AlterSystem` answer `ErrNotSupported`), the connector config | each check names its fix op and floor; a `SET PERSIST` is not offered (no `AlterSystem` on MySQL in this wave); integration on the Q8a profile | U1-9, Q8b, Q8f |
| U2b | A1 | Opus 5.5 | 1 | devctl: `setup-break` `binlog-statement`, `no-replication-grant`, `server-id-zero` on the mysql profile | each break fails at the named check and the recorded agent fixes it | U2a, Q8a |
| U2c | C7 + C6 | Opus 5.5 | 1 | scenarios: the three U2b breaks and `setup/compose-mysql-clean`; eval replay | the four score; Postgres setup pass rate unchanged | U2b |
| U2-R | A8 | Opus 5.5 | 1 | real runs: MySQL 8.4 on Compose, Linux and Windows, from an empty directory | two exports in the write-up | U2a, U1-R |

## Step 3: transforms (SMT) and sink targets

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U3a | C22 | Opus 5.5 | 2.5 | setup/knowledge: SMT index (`ExtractNewRecordState`, `ByLogicalTableRouter`, `Filter`, `ContentBasedRouter`, `TimezoneConverter`, outbox, the Connect built-ins) and sink index (JDBC sink, Elasticsearch, S3, BigQuery, Snowflake, HTTP) as embedded YAML: keys, defaults, version ranges, the check each implies, the renamed classes from Q1b, the sink images and plugin digests | at least 60 entries citing the Debezium or Confluent docs; a bad range or digest fails start-up; a table test resolves each renamed class | U1-24, Q1b |
| U3b | A9 | Opus 5.5 | 1 | setup/tools: `setup_config_help {kind: smt \| sink, name}` over the index; the connector config builder adds SMT and sink keys through it; sink connector registration through Q5c over the remote clients | the tool reads no customer system; a config with an unknown SMT class gets the nearest known class; a sink config passes Q4a's validate on the dev stack | U3a, U1-9 |
| U3c | C19 | Opus 5.5 | 1 | setup/targets/compose: sink services (a JDBC sink to Postgres, the dev warehouse) in the templates and their plugin install | integration: source and sink on one Compose, the canary row in the sink (rung 7); the validator accepts the new templates and nothing else | U3b, U1-3b |
| U3d | C7 | Opus 5.5 | 0.5 | scenarios: `setup/unwrap-missing`, `setup/jdbc-sink-no-pk` | labels name the SMT or sink key | U3b |
| U3e | C14 | Opus 5.5 | 0.5 | docs: what the agent knows about transforms and sinks, and what it does not | citations per claim | U3a |

## Step 4: SQL Server prerequisites

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U4a | A10 | Opus 5.5 | 2.5 | clients/sqlserver: read-only client (`SourceSettingsReader` subset: CDC enabled per db and table, SQL Server Agent state, the capture user's grants, version and edition) and `SetupSourceWriter` (`sp_cdc_enable_db`, `sp_cdc_enable_table` as `CreatePublication`, canary, read role; `AlterSystem` answers `ErrNotSupported`) on a write pool | `go-mssqldb` listed with why; 10 s per call; identifiers quoted with `[]` after the rule 6 check (a line in `AGENTS.md` §4 requested in the PR); integration on the mssql profile | U0 |
| U4b | A10 | Opus 5.5 | 1.5 | setup/sources/sqlserver: the driver, canary table, connector config | each check names its fix op and floor; Agent stopped is a check whose fix is T2 (`service_restart` on a host target) or a documented ask on managed SQL | U4a, U1-9, U1-2b |
| U4c | A1 | Opus 5.5 | 1 | dev: compose profile `mssql` (`mcr.microsoft.com/mssql/server`, CDC off by default), `setup-break` `agent-stopped`, `cdc-not-enabled-table` | profile healthy in < 3 min; each break fails at the named check | U4a |
| U4d | C7 + C6 | Opus 5.5 | 1 | scenarios: `setup/compose-sqlserver-clean` and the two U4c breaks; eval replay | the three score | U4c |
| U4-R | A10 | Opus 5.5 | 1 | real runs: SQL Server 2022 on Compose, Linux; Azure SQL as a manual check (the sandbox of plan §7 item 6) | exports in the write-up; the Azure run marked manual with its date | U4b, U1-R |

## Step 5: container and worker logs in the loop

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U5a | C19 | Opus 5.5 | 1 | setup/targets/compose + host: `container_logs`, `journal_tail` (T0; 200 lines, 64 KiB, redacted, newest last) | capped and redacted literally and by pattern on the runner; a container not in the project is refused; injection-scanned on the server (test with a planted instruction) | U1-3b, U1-4 |
| U5b | A4 | Opus 5.5 | 1 | agent: after a failed step read `container_logs` or `connect_worker_logs` (Q3c) before re-planning; the known-issues lookup with the failing version | prompt snapshot; a recorded test shows the second plan citing the log line and a planted instruction in the log changing no tier and no target; CDC evals unchanged | U5a, Q3c |
| U5c | C7 | Opus 5.5 | 0.5 | scenarios: `setup/plugin-after-scan` (the log shows the worker scanned before the install), `setup/log-injection-ignored` | the label names the log line class | U5b |

## Step 6: embedded engine

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U6a | C22 | Opus 5.5 | 2 | setup/knowledge: embedded reference project templates (Maven and Gradle, Postgres and MySQL), the version mismatch rules (engine, connector, Kafka client, Quarkus) | the templates build and run against the dev stack in an integration test that the CI runner executes (never the runner binary); each rule cites a release note | U1-24, Q1b |
| U6b | A9 | Opus 5.5 | 1 | setup/ops: `project_inspect` (T0: build files, dependency versions), the reference project write under `<project dir>/piperoom-reference/` (T1), the ladder's embedded rung over a log file the person names | the runner never builds or runs the reference project (no such op exists; test lists the catalogue); `project_inspect` reads build files only, never source; the agent never writes outside `piperoom-reference/` in embedded mode | U6a, U1-2, U1-8b |
| U6c | C7 | Opus 5.5 | 0.5 | scenarios: `setup/embedded-version-mismatch`, `setup/embedded-offset-storage` | the label names the mismatch and says "diagnosed, reference project generated" | U6b |

## Step 7: MongoDB prerequisites

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U7a | A10 | Opus 5.5 | 2 | clients/mongodb: read-only client (replica set status, version, change streams support, the user's roles) and `SetupSourceWriter` (the oplog user as the read role, canary collection and insert; the rest `ErrNotSupported`) | the MongoDB Go driver listed with why; 10 s per call; integration on the mongo profile | U0 |
| U7b | A10 | Opus 5.5 | 1.5 | setup/sources/mongodb: the driver, canary collection, connector config (`mongodb.connection.string` with a `${secret:}` reference) | each check names its fix op and floor; a standalone `mongod` is a check whose fix is T1 only when this room created it (by receipt) | U7a, U1-9, U1-2b |
| U7c | A1 | Opus 5.5 | 1 | dev: compose profile `mongo` (a one-member replica set), `setup-break` `standalone-mongod`, `no-oplog-role` | profile healthy in < 2 min; each break fails at the named check | U7a |
| U7d | C7 + C6 | Opus 5.5 | 0.5 | scenarios: `setup/compose-mongodb-clean` and the two U7c breaks; eval replay | the three score | U7c |
| U7-R | A10 | Opus 5.5 | 0.5 | real runs: MongoDB 7 on Compose, Linux; Atlas as a manual check | exports in the write-up | U7b, U1-R |

## Step 8: plain hosts: systemd, a Windows service

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U8a | C19 | Opus 5.5 | 2.5 | setup/targets/host (Linux): `service_status`, `properties_read` (T0); `properties_write` (T1 under the project dir, T2 elsewhere), `service_install` from the unit template with typed params only, `service_restart` (T2); standalone versus distributed properties, the plugin path | `systemctl` argv built from typed params only; the unit's `ExecStart`, `User` and `Environment` come from the template and the detected paths, never from a param (test); a unit file preview is a diff; integration on an ubuntu CI runner with a user-level systemd unit: an empty dir to a RUNNING connector | U1-2, U1-0, U1-3a |
| U8b | C19 | Opus 5.5 | 2.5 | setup/targets/host (Windows): the service through `svc/mgr`, the properties and plugin path with Windows paths, Java detection | `go doc` for every `mgr` call in the PR; the service's binary path comes from the template and the project dir only; unit tests with a fake manager; the real run on the dev machine (U8-R) is the integration | U8a, U1-0 |
| U8c | A1 | Opus 5.5 | 1 | devctl: `setup-break` `wrong-standalone-args`, `plugin-path-unset`, `java-too-old` on a host target (a container that plays a host on Linux) | each break fails at the named check | U8a |
| U8d | C7 | Opus 5.5 | 0.5 | scenarios: the three U8c breaks | labels name the properties key or the argv | U8c |
| U8e | C14 | Opus 5.5 | 0.5 | docs: the host target (systemd, Windows service), what the agent needs (sudo or not, a service account) | citations per claim | U8b |
| U8-R | A1 (Windows), C19 (Linux) | Opus 5.5 | 1 | real runs: Ubuntu 24.04 with systemd, Windows 11 as a service, from an empty directory | two exports in the write-up | U8b |

## Step 9: Oracle prerequisites

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U9a | A10 | Opus 5.5 | 2.5 | clients/oracle: read-only client (archive log mode, supplemental logging per db and table, the LogMiner user's grants, CDB and PDB names, version and edition) and `SetupSourceWriter` (supplemental logging as `CreatePublication`, canary, the LogMiner read role with the grants of the Debezium docs; `AlterSystem` for archive log mode only, allow-listed) | `go-ora` (pure Go, no instant client) listed with why; 10 s per call; identifiers quoted after the rule 6 check; integration on the oracle profile | U0 |
| U9b | A10 | Opus 5.5 | 2 | setup/sources/oracle: the driver, canary table, connector config (`database.pdb.name`, LogMiner) | archive log mode off is a T3 check with a maintenance window on anything this room did not create (a restart); 19c and 21c free editions covered, another edition says so | U9a, U1-9, U1-2b |
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
| U11a | C20 | Opus 5.5 | 2.5 | setup/targets/server: the properties template per sink (Kinesis, Pub/Sub, Redis, HTTP, Kafka) under the key allow-list, the server as a Compose service (template, validated image) or a systemd unit (template), `server_status` (T0) | the sink config keys come from the knowledge index; integration on Linux: Postgres to the HTTP sink (a loopback receiver) with the nonce canary | U3a, U1-3b, U8a |
| U11b | C20 | Opus 5.5 | 1.5 | setup/targets/server: `sink_probe` (T0: reachability and auth for Kinesis, Pub/Sub, Redis, HTTP; the cloud ones over the shared AWS config and a GCP token the runner holds); the denylist of design §2.6 applies | no credentials in any receipt; `httptest` and a local Redis for tests; the cloud probes have recorded fakes | U11a |
| U11c | C7 | Opus 5.5 | 0.5 | scenarios: `setup/server-sink-unreachable`, `setup/server-offset-file-unwritable` | labels name the probe or the property | U11b |
| U11d | C14 | Opus 5.5 | 0.5 | docs: the Debezium Server target | citations per claim | U11b |
| U11-R | C20 | Opus 5.5 | 0.5 | real run: Debezium Server to Redis on Compose, Linux | export in the write-up | U11b |

## Step 12: Kubernetes with Strimzi

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U12-0 | leads | Opus 5.5 | 1 | design: `docs/design/setup-strimzi.md` (the CRD versions, the in-cluster runner Job and its RBAC for one namespace, how the pairing code reaches a Job and how the local confirmation is given for a Job, `kubectl` versus the API from the runner, the manifest field allow-list) | the page answers each; item 9's Strimzi part lands in code with it | U0 |
| U12a | C20 | Opus 5.5 | 2.5 | setup/targets/strimzi: `k8s_get`, `k8s_logs`, `k8s_events`, `strimzi_build_status` (T0, one namespace); `k8s_apply {kind, template, params}` (T2, templates only, the field allow-list, `--dry-run=server` as preview), `k8s_secret_apply` (T2) | the namespace comes from `Env.Targets` only; a kind outside `KafkaConnect`, `KafkaConnector`, `Secret`, `ConfigMap` is refused; a rendered manifest with `spec.template` pod overrides, `hostPath`, `hostNetwork`, `serviceAccountName`, `initContainers` or `volumes` fails the validator (test per field); `kubectl` argv from typed params only; unit tests with a fake `kubectl` on `PATH` | U12-0, U1-2 |
| U12b | C20 | Opus 5.5 | 2 | setup/targets/strimzi: the `KafkaConnect` template with `build.plugins` (artifacts and `sha512sum` from the knowledge index), the `KafkaConnector` template, the ladder over `KafkaConnector` status conditions | kind integration: an empty namespace to a Ready connector and the nonce canary in < 20 min | U12a, U1-24 |
| U12c | C1 | Opus 5.5 | 1.5 | deploy/helm + ci: the in-cluster runner Job (image, RBAC for one namespace, the pairing code as a Secret), the nightly kind job | `helm template` with `setup.runner.enabled=true` renders the Job and the Role for one namespace and nothing cluster-wide; the nightly kind job runs U12b's integration and posts the export | U12b |
| U12d | C7 | Opus 5.5 | 0.5 | scenarios: `setup/strimzi-build-failed`, `setup/strimzi-connector-not-ready` | labels name the condition | U12b |
| U12e | C14 | Opus 5.5 | 0.5 | docs: the Strimzi target and the in-cluster runner | citations per claim | U12c |
| U12-R | C20 | Opus 5.5 | 1 | real run: a kind cluster on Linux and a real cluster the owner names (manual) | exports in the write-up | U12c |

## Step 13: managed: MSK Connect, Confluent Cloud, Aiven

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U13-0 | leads | Opus 5.5 | 1.5 | design: `docs/design/setup-managed.md` (the three clients' auth: SigV4 over the shared AWS config, Confluent Cloud API keys, Aiven tokens; which operations each exposes; the recorded-fakes harness; the sandbox accounts of plan §7 item 6; how the server watches a managed pipeline with read-only keys) | the page answers each; item 9's managed part lands in code with it | U0 |
| U13a | C21 | Opus 5.5 | 1 | setup/targets/managed: the recorded-fakes harness (`httptest` cassettes, secrets scrubbed at record time, a `//go:build manual` re-record) | a cassette with a credential-looking string fails the test that loads it | U13-0 |
| U13b1 | C21 | Opus 5.5 | 1.5 | clients/mskconnect: the client over the AWS SDK with the shared AWS config (describe cluster, plugins, connector; IAM, VPC and security group reads; create plugin, worker config, connector; delete connector) | the SDK calls confirmed with `go doc`; the cassettes cover 400, 403, 404, 409, throttling; the manual check on the sandbox account dated in the PR | U13a |
| U13b2 | C21 | Opus 5.5 | 1.5 | setup/targets/managed: the MSK ops `msk_describe_*` (T0), `msk_create_plugin` (from an S3 object the person uploaded), `msk_create_worker_config`, `msk_create_connector`, `msk_delete_connector` (T2; T3 on production), the ladder over `describe-connector` | the worker configuration is rendered from a template under the key allow-list; the account id is matched against `PIPEROOM_SETUP_PROD_PATTERNS`; unit tests over the cassettes | U13b1, U1-8 |
| U13c | C21 | Opus 5.5 | 2 | clients/confluentcloud + ops: plugins, connector status, cluster (T0); `ccloud_create_connector` (validate then create), `ccloud_acl_create` (T2) | basic auth over https only (`netsec`); cassettes; manual check dated | U13a |
| U13d | C21 | Opus 5.5 | 2 | clients/aiven + ops: service, connectors, plugins (T0); `aiven_create_connector`, `aiven_integration_create` (T2) | token in a header, never a URL; cassettes; manual check dated | U13a |
| U13e | C7 + C6 | Opus 5.5 | 1 | scenarios: `setup/msk-plugin-s3-denied`, `setup/msk-vpc-no-route`, `setup/ccloud-acl-missing`, `setup/aiven-plugin-unavailable`; eval replay over the cassettes | the four score | U13b2, U13c, U13d |
| U13f | C14 | Opus 5.5 | 1 | docs: the three managed targets, the IAM policy, the API key scopes, the token scope, the read-only keys the server watches with | citations per claim | U13d |
| U13-R | leads | Opus 5.5 | 1 | manual runs on the three sandbox accounts, RDS Postgres as the source | three exports in the write-up, dated | U13e |

## Step 14: the measurement

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| U14 | leads | Opus 5.5 | 1 | `reviews/setup-runs-2026-11.md`: the first ten real setups (wall clock, steps, approvals, tokens, what blocked), the coverage re-count against the label files, the next wave's candidates (a runner relay for laptop stacks, `--resume`, signed catalogues) | the numbers are in the plan's §8 caveats paragraph as a follow-up; plan §7 items 1, 4 and 8 revisited with data | U13-R |

## New folders and owners

| Agent | Owns (new in the U wave) |
|---|---|
| leads | `internal/setup/protocol/**` (a contract file) |
| A9 (Qoder, new) | `cmd/setup/**`, `cmd/piperoom/**`, `internal/setup/**` except `internal/setup/protocol/**` and the `targets/`, `sources/` and `knowledge/` subfolders below |
| A10 (Qoder, new) | `internal/clients/sqlserver/**`, `internal/clients/mongodb/**`, `internal/clients/oracle/**`, `internal/setup/sources/sqlserver/**`, `internal/setup/sources/mongodb/**`, `internal/setup/sources/oracle/**` |
| A3 | `internal/setup/sources/postgres/**` |
| A5 | `internal/quickstart/**` |
| A8 | `internal/setup/sources/mysql/**` |
| C1 | `Dockerfile`, `deploy/helm/**` (a grant for this wave) |
| C19 (cloud, new) | `internal/setup/targets/compose/**`, `internal/setup/targets/host/**` |
| C20 (cloud, new) | `internal/setup/targets/strimzi/**`, `internal/setup/targets/server/**` |
| C21 (cloud, new) | `internal/setup/targets/managed/**`, `internal/clients/mskconnect/**`, `internal/clients/confluentcloud/**`, `internal/clients/aiven/**` |
| C22 (cloud, new) | `internal/setup/knowledge/**` |
| C7 | `evals/scenarios/setup/**` (C6 keeps the rest of `evals/**`, `evals/setup/**` included) |

A4's, A6's, A1's, C4's and C14's rows stay inside their existing folders; A5's rows stay in A5's folders plus
`internal/quickstart/**`.

## Load per agent

| Agent | PRs | Days | Critical for |
|---|---|---|---|
| A9 | 11 | 15.75 | the core: runner, catalogue, remote clients, tools and actions, plan, standalone |
| A10 | 9 | 14 | SQL Server, MongoDB, Oracle |
| A5 | 8 | 12 | the runner endpoint and pairing, routes, quickstart package, tiering and the guard, the ladder runner, registration, rollback |
| C19 | 7 | 10.5 | Compose and host targets, their validators |
| C20 | 6 | 10 | Strimzi and Debezium Server |
| A1 | 8 | 8.25 | dev profiles and breaks, Windows runs |
| leads | 6 | 8 | contracts, three designs, manual runs, the measurement |
| C21 | 5 | 8 | managed |
| C22 | 4 | 7 | knowledge, the image and plugin digests |
| C7 (half of each shared row) | 12 | 6.25 | scenarios |
| C14 | 6 | 4 | docs each step |
| C6 (half of each shared row) | 6 | 3.75 | eval replay |
| A3 | 2 | 3.5 | Postgres driver, owners (provisional, overlap, reload) |
| A4 | 3 | 3.5 | setup mode |
| C1 | 3 | 3.5 | CI matrix, the image, the Helm Job, kind |
| A6 | 2 | 3 | the setup UI |
| A8 | 2 | 3 | MySQL |
| C4 | 1 | 1 | threat model |
| A2 | 1 | 0.25 | `LooksSecret` switch |

Total 95 rows and about 125 agent-days; 122.25 of those days are the step rows the plan's §4 curve counts (U0 and
U14 are outside the steps). The critical path by After and days is U0 → U1-0 → U1-2 → U1-8 → U1-6 → U1-8b → U1-10 →
U1-R, about 12 working days after U0 across A9, A5 and A4; U1-1, U1-5a and U1-9 run beside it. The second-longest
chain is A10's three databases (about 14 days, sequential). If A9 slips, C19's Compose driver and A3's Postgres
driver can be reviewed against the protocol fakes and merge first.

## Suggested weekly dispatch (all agents in parallel)

| Week | Lands |
|---|---|
| 1 | U0; U1-0, U12-0, U13-0; U1-1, U1-5a, U1-5c, U1-13, U1-23, U1-24, U4a, U13a |
| 2 | U1-2, U1-2b, U1-3a, U1-4, U1-5b, U1-5d, U1-8, U1-12a, U3a, U4c, U6a, U7a, U9a, U12a, U13b1 |
| 3 | U1-3b, U1-6, U1-7, U1-8b, U1-9, U1-11, U1-14b, U1-17, U2a, U2b, U3b, U4b, U5a, U7b, U7c, U8a, U9b, U9c, U12b, U13b2, U13c, U13d |
| 4 | U1-10, U1-12b, U1-14, U1-15, U1-16, U1-18, U1-19, U1-20, U1-22, U1-R, U2c, U3c, U3d, U3e, U4d, U5b, U5c, U6b, U6c, U7d, U8b, U8c, U9d, U10a, U11a, U12c, U13e, U13f |
| 5 | U1-21, U2-R, U4-R, U7-R, U8d, U8e, U8-R, U9-R, U10b, U11b, U11c, U11d, U11-R, U12d, U12e, U12-R, U13-R; U14 |

Review load for the leads peaks at about 25 PRs in weeks 3 and 4; the code review of C* PRs goes to `[LEAD-CLOUD]`
as usual, and every target row's real run is checked by Lead 1 from the attached export before the merge.
