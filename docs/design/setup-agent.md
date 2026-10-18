# Setup agent: design (main @ 6cf24f60, 2026-10-18; rev 2 after the #641 reviews)

Owner of the design: LEAD-ARCH. Plan: `docs/product/setup-2026-10.md`. Board: `tasks/U-board.md`. Contract
changes land in U0 and the design rows (`U1-0`, `U12-0`, `U13-0`); the shapes in §4 are proposals in the style of
`tasks/P0-contracts.md` and the Q0 items: new interfaces, new events and one new contract file, so every existing
fake and the eval replay compile unchanged. Where this text and merged code differ, the code and its doc comments
win.

**Decision recorded:** setup is a functionality of Piperoom (plan §2). One agent, one room, one approval model, one
`owners.yaml`, one web UI. This page is about how the setup capability is built inside that.

**Rev 2** answers the two reviews of #641: the artifacts an op executes are validated or templated (S1, §2.5);
pairing is single-use, rate-limited, locally confirmed and replay-safe (S2, §2.1, §2.2); setup rooms, runners and
registration are tenant-bound and need a non-self approver on a team server (S3, §3.6); secrets are unreadable
by any op and redacted literally (S4, §2.4); file confinement uses `os.Root` with one symlink rule and Windows
tests (S5, §2.3); the verification ladder runs on the server with a nonce canary and the model can only request it
(S6, §3.4); registering a target is a driver click with an address denylist (S7, §2.6). A provisional pipeline
scopes every tool and action before the owners entry exists (A1, §3.2); watching after setup has one mechanism
per target class (A2, §3.7); standalone mode builds on an importable quickstart with write mode on (A3, §1, §2.7);
the U0 list is complete and `clients.go` imports nothing of A9's (A4, §4).

## 1. Components and folders

```
browser ──► server (cmd/server) ◄──── runner (cmd/setup) ── docker · kubectl · aws/confluent/aiven CLIs/APIs
   ▲            │  ▲                        │                · the source DB · the user's project dir
   │            │  └─ /api/runners/ws ──────┘  (outbound from the runner; typed ops in, results out)
   │            ▼
   │      room_events (setup.* events, proposals, approvals, action.executed with receipts)
   │            │
   └── same web room, setup stepper (web/**)
            agent.Run in setup mode: Q tools and setup tools (KindRead, over remote clients) + Q actions and setup
            actions (executed through the runner); the ladder runner (server code) verifies
```

| Folder | Owner | Holds |
|---|---|---|
| `internal/setup/protocol/**` | leads (**contract file**, see §4) | the wire types between server and runner (`Hello`, `OpRequest`, `OpResult`, `Resume`, `Cancel`), `RunnerExecutor`, the protocol version and its compatibility rule |
| `cmd/setup/**` | A9 (new) | the runner binary: `piperoom-setup [--server URL --code XXXX] [--standalone] [--project DIR]` |
| `internal/setup/runner/**` | A9 | the runner: WebSocket client, pairing with local confirmation, capability detection, op dispatch, replay checks, the local op log, the secret store, literal redaction before transport |
| `internal/setup/ops/**` | A9 | the operation catalogue: one Go type per op with its JSON Schema, `argv` builder or API call, tier floor, preview, rollback; the artifact validators; `ops.Registry` |
| `internal/setup/remoteclients/**` | A9 | `clients.*` interface implementations that forward each method as one op to the paired runner, so the Q tools and Q actions run unchanged against the runner's reach |
| `internal/setup/plan/**` | A9 | the plan model (`Plan`, `Phase`, `Step`), the environment classifier, `TierOf`, the guard table, the bounds, the ladder definition (rungs and predicates) |
| `internal/setup/tools/**` | A9 | the setup-only KindRead tools (`env_detect`, `jar_list`, ...) and the KindRoom tools `setup_plan` and `setup_request_verification` |
| `internal/setup/actions/**` | A9 | the setup-only `actions.Action`s (file, compose, service, k8s, cloud ops) that forward to the runner; `Preview`, `Scoped`, `Tiered`, `Receipted` per op |
| `internal/setup/standalone/**` | A9 | no-server mode over `internal/quickstart` (A5): starts the server in-process with write mode and setup on, attaches the runner, opens the browser, keeps watching |
| `internal/quickstart/**` | A5 | the `--quickstart` server extracted from `cmd/server` into an importable package with write mode configurable (A3 of the review) |
| `internal/setup/targets/compose/**`, `targets/host/**` | C19 (new) | the Compose and plain-host drivers: ops, templates, validators and checks |
| `internal/setup/targets/strimzi/**`, `targets/server/**` | C20 (new) | the Strimzi and Debezium Server drivers |
| `internal/setup/targets/managed/**`, `internal/clients/mskconnect/**`, `internal/clients/confluentcloud/**`, `internal/clients/aiven/**` | C21 (new) | the managed drivers and their API clients |
| `internal/setup/sources/postgres/**` | A3 | Postgres prerequisites over the Q2 interfaces and the new `SetupSourceWriter` |
| `internal/setup/sources/mysql/**` | A8 | MySQL prerequisites over the Q8 client |
| `internal/setup/sources/sqlserver/**`, `sources/mongodb/**`, `sources/oracle/**`, `internal/clients/sqlserver/**`, `internal/clients/mongodb/**`, `internal/clients/oracle/**` | A10 (new) | the three new thin clients and their prerequisite drivers |
| `internal/setup/knowledge/**` | C22 (new) | SMT, sink, Debezium Server sink and tuning knowledge as embedded YAML; the image allow-list with pinned digests; the plugin artifact digests; the embedded reference project templates |
| `internal/room/setup*.go`, `internal/room/runners*.go`, `internal/room/ladder*.go`, `internal/httpapi/setup*.go`, `internal/ws/runner*.go` | A5 | runner registry, pairing, the `/api/runners/ws` endpoint, the setup routes, tiered routing and the guard, the ladder runner's scheduling, registration with the overlap check |
| `internal/owners/reload*.go`, `internal/owners/provisional*.go` | A3 | `OWNERS_DIR` hot-reload, the provisional pipeline, tenant admins, the overlap check |
| `internal/agent/setup*.go`, the mode switch in `run.go`, `internal/wiring/setup.go` | A4 | the setup prompt frame, the bounds, plan-before-propose, wiring of setup registries over the remote clients |
| `web/src/setup/**` | A6 | the setup stepper, runner status, the target and class controls, the secret field, the canary prompt, the verified card |
| `evals/setup/**`, `evals/scenarios/setup/*.yaml` | C6, C7 | runner recordings replay, setup scenarios |
| `dev/setup/**`, `cmd/devctl` `setup-break` | A1 | broken environments for the agent to fix on the dev stack |
| `.github/workflows/setup-matrix.yml`, `Dockerfile`, `deploy/helm/**` | C1 (the two latter by a grant for this wave, `AGENTS.md`) | the three-OS matrix, kind for Strimzi, the image that ships `piperoom-setup`, the in-cluster runner Job |
| `docs/pilot/setup-agent.md` | C14 | the user's page |

`agent.Run`'s contract is unchanged; its implementation (`run.go`, A4's) gains the setup-mode switch (§3.1). A
target driver and a source driver are **data plus checks**, not code paths the agent branches on: each registers
its ops, its templates, its validators, its verification rungs and its known failure signatures with the plan
package.

## 2. The runner

### 2.1 Pairing and transport (S2)

- `POST /api/setup` (the UI's **Set up** button; driver of the new room) returns a pairing code: 8 characters from
  a 32-character alphabet (40 bits), valid 10 minutes, **single use**, bound to `(tenant, room, creator)`. The
  server allows 5 attempts per code, 20 per source address and 100 in all per 10 minutes (then pairing is closed
  for 10 minutes and audited), compares the code and every later
  token with `crypto/subtle.ConstantTimeCompare`, and emits `setup.pairing {outcome, source, runner_label?}` for
  every attempt, success or not, and for a replacement of a lost runner.
- `piperoom-setup --server https://piperoom.acme --code XXXXXXXX` opens one WebSocket to `/api/runners/ws`
  (TLS required unless loopback, the `netsec` rule) and sends the code. The server answers with the room's title,
  pipeline id, tenant and creator. **The runner prints the server origin, the room title and the creator and asks
  for local confirmation** (`y/N` on its terminal) before it sends `Hello`. A runner started with `--yes` is refused
  unless the server is loopback (standalone mode). A pairing command a person did not expect is thereby a question
  on their own screen, not a silent takeover.
- The server then issues a runner token: random, 32 bytes, in memory on both sides, bound to `(tenant, room,
  creator, runner_label)`, **idle expiry 30 minutes, rotated on every reconnect**; a reconnect with an expired token
  needs a new pairing code. The token is never logged; the audit event carries only the runner label.
- `Hello`: OS, architecture, a runner label the person may change (never the raw hostname), the tools found on
  `PATH` with versions (`docker`, `docker compose`, `kubectl`, `helm`, `aws`, `confluent`, `avn`, `java`,
  `systemctl`), the project directory, the catalogue and protocol versions. The server stores it on
  `setup.runner_attached`.
- Frames are JSON, at most 1 MiB; a larger `OpResult` is cut on the runner with the summary marker and
  `truncated: true`. Ping every 20 s; a runner silent for 90 s is `setup.runner_lost` and every in-flight step is
  marked unknown (`OK == nil`), as an interrupted executor step is today.
- One runner per room. A second pairing for the same room replaces the first only after the first is lost, and is
  audited.
- **The runner prints every T1 or higher op** (op, target, proposal id, step) on its own terminal and appends it,
  with the result and receipt, to a local append-only op log (`<project dir>/.piperoom/ops.jsonl`) in both team
  and standalone modes: a witness the person holds that does not depend on the server telling the truth.

### 2.2 Operation execution and replay safety (S2)

```go
// protocol.OpRequest is what the server sends; the runner executes exactly this.
type OpRequest struct {
	ID         string          `json:"id"`                    // unique per request; results and receipts carry it back
	Op         string          `json:"op"`                    // ops.Registry name, e.g. "compose_up"
	Params     json.RawMessage `json:"params"`                // validated against the op's schema on both sides
	Tier       string          `json:"tier"`                  // T0..T3 as the server decided; the runner refuses a tier below the op's floor
	Class      string          `json:"class"`                 // local | shared | production of the resolved target; the runner enforces the guard too
	Deadline   time.Time       `json:"deadline"`
	ProposalID string          `json:"proposal_id,omitempty"` // T1+ only
	StepIndex  int             `json:"step_index"`            // T1+ only
	ParamsHash string          `json:"params_hash,omitempty"` // T1+ only: the proposal's hash; the runner recomputes it over the frozen proposal it was shown
	Proposal   json.RawMessage `json:"proposal,omitempty"`    // T1+ only, first request of a proposal: the frozen proposal (events.ProposalCreatedPayload) so the runner can recompute ParamsHash and print it
}

type OpResult struct {
	ID        string          `json:"id"`
	OK        bool            `json:"ok"`
	Summary   string          `json:"summary"`        // redacted on the runner, ≤ tools.MaxSummaryBytes
	Data      json.RawMessage `json:"data,omitempty"` // redacted, ≤ 64 KiB
	Receipt   events.SetupReceipt `json:"receipt"`
	Truncated bool            `json:"truncated,omitempty"`
}

// Resume is the runner's first frame after a reconnect: what it was doing.
type Resume struct {
	InFlight []OpStatus `json:"in_flight"` // {id, proposal_id, step_index, state: running | done, result?}
}
```

`events.SetupReceipt` (the setup equivalent of `tools.Call`) lives in `internal/events/events.go`:

```go
type SetupReceipt struct {
	Kind     string    `json:"kind"`               // "exec" | "http" | "sql" | "file"
	Argv     []string  `json:"argv,omitempty"`     // exec: the exact argv, every secret as ${secret:name}
	Target   string    `json:"target"`             // host, URL, file path, database; never credentials
	ExitCode int       `json:"exit_code"`
	At       time.Time `json:"at"`
	TookMS   int64     `json:"took_ms"`
	OutMAC   string    `json:"out_mac"`            // HMAC-SHA256 of the raw output under a key only the runner holds
}
```

The runner, before it executes anything:

- validates `Params` against the op's schema a second time and builds `argv` or the API call itself; every exec op
  uses `os/exec` with an argv slice, there is no shell; on Windows the service ops call `golang.org/x/sys/windows/
  svc/mgr` (rule 9: `go doc` before use);
- refuses a tier below the op's floor, a T1+ request without `ProposalID`, `StepIndex` and `ParamsHash`, and a
  `Class` of `production` for any op in the guard table (§3.5);
- **recomputes `params_hash`** (RFC 8785 over the frozen proposal it received with the first request of that
  proposal, `docs/design/auth.md` §3.1) and refuses a mismatch with `ErrParamsHash`;
- **refuses a duplicate `(proposal_id, step_index)`** it has already started (`ErrDuplicateStep`), from its op log,
  which survives a runner restart;
- on reconnect sends `Resume`; the server reconciles: an op the runner reports `running` or `done` is never re-sent,
  a `done` result is taken as the step's result, and an op the server sent that the runner never saw is re-sent
  once. This keeps the exactly-once promise of `docs/design/replicas.md` §5 with the runner on the path.

Every refusal is `setup.runner_refused {op, reason, proposal_id?, step_index?}` on the server (the runner reports
it) and a line in the runner's op log.

### 2.3 File confinement (S5)

One rule, stated everywhere: **every file op opens through `os.Root` rooted at the project directory, and any path
component that is a symlink, junction or other reparse point is refused.** There is no check-then-open: `os.Root`
resolves inside the root at open time, so a swapped symlink cannot escape it, and the "no symlink at all" rule
removes the ambiguity the first draft had (the plan said refuse, the design said follow). The rule covers
`file_read`, `dir_list`, `file_write`, `file_delete`, `properties_write`, the reference project write and every
template render.

Refused paths, each a table-test row that runs on the three CI legs: `..` anywhere; an absolute path; `.git/` and
every case or trailing-dot or trailing-space variant (`.GIT`, `.git.`, `.git `); `.piperoom/` and every
materialisation path (§2.4); on Windows additionally a drive-relative path (`C:foo`), a UNC path (`\\host\share`),
a `\\?\` path, an alternate data stream (`a.yaml:x`), a reserved device name (`CON`, `NUL`, `COM1`, `LPT1`, with
or without an extension), a junction and a reparse point. The runner is built with `go doc os.Root` output in the
U1-2 PR.

### 2.4 Secrets on the runner (S4)

- The store is `PIPEROOM_SETUP_SECRETS_FILE` (default `<project dir>/.piperoom/secrets.env`, created 0600, listed
  in a generated `.gitignore`) or the environment the runner was started from.
- `secret_set {name}` prompts on the runner's terminal, or takes the value from the UI through
  `POST /api/rooms/{id}/setup/secret` (§4 item 6: driver only, the value in the body, the route excluded from
  request logging with a test, forwarded to the runner in one frame the server stores nowhere). The server emits
  `setup.secret_set {name, actor}`; the value is in no event.
- Materialisation points, all on the runner: compose `.env` files, a properties file next to the compose file
  referenced from connector configs as `${file:...:key}` (never plaintext in a config), Kubernetes Secrets
  (`k8s_secret_apply`, T2), cloud API calls. The runner keeps the list of materialised paths.
- **No op can read a secret back.** `file_read`, `dir_list`, `file_write` and `file_delete` refuse `.piperoom/**`
  and every materialised path; `container_inspect` drops `Config.Env` values and keeps only the names;
  `compose_config` renders with secrets masked (`docker compose config --no-interpolate` is the preview, and the
  rendered output is then redacted literally). Tests prove each: a frame that carries a stored value fails the
  test.
- **Literal redaction.** Before any `Summary`, `Data` or receipt leaves the runner, every value in the store (and
  every value the runner materialised) is replaced literally by `${secret:name}`, in addition to `redact.Text`'s
  patterns. A value shorter than 6 bytes is refused at `secret_set` so literal redaction cannot mangle ordinary
  text.
- `OutMAC` is an HMAC-SHA256 under a per-runner key generated at first start and kept in the store, so a short
  output is not a guessing oracle; a dispute is settled by the person re-running the receipt on the runner, which
  recomputes the MAC.

### 2.5 Catalogue rules and artifact validators (S1)

Every op is a Go value implementing:

```go
// ops.Op is one typed operation the runner can execute. There is no generic exec.
type Op interface {
	Name() string                       // snake_case, unique: "compose_up", "file_write", "k8s_apply"
	Target() string                     // "compose" | "host" | "strimzi" | "msk" | "confluent" | "aiven" | "dbzserver" | "" (any)
	Tier() string                       // the op's floor
	Layer() string                      // "environment", "postgres", "kafka-connect", ...
	Describe() string
	ParamsSchema() map[string]any
	Validate(params json.RawMessage, env Env) error    // no I/O; paths, hosts, and the artifact validator of the op
	Preview(ctx context.Context, params json.RawMessage, env Env) (events.PreviewResult, error)
	Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error)
	Rollback(params json.RawMessage, result Result) (name string, rollbackParams json.RawMessage, ok bool)
}
```

**The S1 rule: an artifact that will be executed is either rendered from a template with typed parameters, or
validated against an allow-list; anything else is never T1.** Writing a file and then applying it is code execution
on the host, and "no shell from model text" holds only if the content is typed too. Per artifact kind:

| Artifact | How it is produced | Validator (in `Validate`, no I/O) | If it fails |
|---|---|---|---|
| compose file | `compose_write {template, params}`: templates from the driver (`kafka-kraft`, `connect`, `registry`, `jdbc-sink`, `source-postgres` for a local DB), typed params (versions, ports, plugin list, project name); `file_write` is refused for the compose names (`compose.yaml`, `compose.yml`, `compose.override.yaml`, `compose.override.yml`, `docker-compose*.y*ml`) | images only from the knowledge index allow-list, pinned by digest (`debezium/connect@sha256:...`); no `privileged`, `cap_add`, `devices`, `security_opt`, `pid`, `ipc`, `network_mode: host`, `userns_mode`; bind mounts only under the project dir and never the Docker socket or `/`; no `extends`, no `build:` with a `Dockerfile` outside the templates; ports bind `127.0.0.1` unless the person raised the target class | the step is T2 with a click, the validator's reason in the preview |
| `.env` and properties files | `properties_write {file, entries}` with a key allow-list per file kind from the knowledge index; values that look like a path or an URL are checked like hosts and paths | unknown key, a `plugin.path` outside the project dir, a `${}` reference other than `${secret:}`/`${file:}` to the materialised file | T2 with a click |
| systemd unit, Windows service | `service_install {template, params}`: the driver's unit template with typed `ExecStart` built from the Connect distribution path under the project dir, the properties file and the Java path from `java_version`; no free-form `ExecStart`, `Environment`, `User` | the template is the only source; params are paths under the project dir and the detected Java | refused |
| Kubernetes manifests | `k8s_apply {kind, template, params}` for `KafkaConnect`, `KafkaConnector`, `Secret`, `ConfigMap` only, rendered from templates | field allow-list: no `spec.template` pod overrides (`securityContext`, `hostPath`, `hostNetwork`, `serviceAccountName`, `initContainers`, `volumes`), `build.plugins` artifacts only from the knowledge index with their `sha512sum`; a namespace from `Env.Targets` | refused |
| connector config | the source driver's `ConnectorConfig` plus `setup_config_help` keys | Connect's own `config/validate` (Q4a) as the preview; `${secret:}` references only | the proposal is not created |
| the embedded reference project | `file_write` under `<project dir>/piperoom-reference/` from the knowledge templates | the runner **never builds or runs it**; `reference_run` does not exist; the person runs it and the ladder reads the log file the person points at (U6) | n/a |

**`file_write` is for files nothing executes or loads, and the runner proves it** (S1 closure; every rule a table
test in U1-2):

- it refuses any path under a directory a template mounts into a container or builds from: the Connect plugin
  dir, the worker config dir, the log4j dir, every `volumes:` source of the rendered compose file and every
  `build:` context; the driver registers these directories with the ops package when it renders a template, and
  `Validate` consults the list;
- it refuses the names `.env*`, `Dockerfile*`, `*.jar`, `*.sh`, `*.ps1`, `*.bat`, the compose names above, unit
  files (`*.service`) and manifests (`*.yaml`/`*.yml` under a `k8s/` or `manifests/` directory); those have their
  own ops or no op at all;
- it refuses content that is not valid UTF-8 (so no jar or binary ever arrives through it) and content over 256 KiB;
- it refuses a destination that exists and is not a regular file (a device file, a FIFO, a socket), which
  `os.Root` alone does not; `file_read` and `dir_list` skip non-regular files the same way;
- the `.env` key allow-list of `properties_write` excludes `COMPOSE_*` and `DOCKER_*` (they change what compose
  does, not what the stack is);
- the compose argv is pinned: `docker compose --project-directory <project dir> -f <rendered file> --env-file
  <rendered .env> <subcommand> [service names]`, every path the one the template rendered, no other flag; a test
  asserts the argv shape for each compose op.

Other rules, each with a test in `internal/setup/ops`:

- An op that reaches a host reaches only a host in `Env.Targets` (§2.6).
- No op deletes data: there is no `drop_table`, no `delete_topic`, no `compose_down_volumes`. `compose_down` keeps
  volumes. Rollback ops are ordinary ops under the same rules and are checked by the guard again (§3.5).
- Output goes through literal redaction and `redact.Text` on the runner before it enters `OpResult`; `Data` is
  capped at 64 KiB.
- `plugin_install` downloads only from `PIPEROOM_SETUP_PLUGIN_HOSTS` and verifies the artifact against the
  **digest pinned in the knowledge index** (C22 records the Maven Central SHA-512 per Debezium release), not a
  digest fetched from the same host.
- **Container logs are an injection vector.** `container_logs`, `journal_tail` and `k8s_logs` are capped (200
  lines, 64 KiB), redacted, injection-scanned (N6) and wrapped as untrusted. What makes T1 safe after a log read is
  not the model's judgement but the validators: a T1 step still executes only a validated or templated artifact.

The catalogue at the end of U1 (Compose, Postgres), by tier floor:

| Tier | Ops |
|---|---|
| T0 | `env_detect`, `file_read`, `dir_list`, `docker_info`, `compose_ps`, `compose_config` (masked render), `container_logs`, `container_inspect` (env values dropped), `jar_list`, `java_version`, `http_get` (Connect and Registry REST to hosts in `Env.Targets`), `net_probe` (the Q6a probe from the runner's position, targets from the provisional pipeline), and the client-method ops the remote clients use (`connect.status`, `connect.plugins`, `connect.validate_config`, `source.settings`, `source.table_replication`, `source.role_info`, `kafka.topic_offsets`, `kafka.find_by_key`, `registry.versions`, ...) |
| T1 | `compose_write`, `properties_write`, `file_write` (non-executed files only), `file_delete`, `compose_up`, `compose_restart`, `compose_down`, `plugin_install`, `secret_set` |
| T2 | the Q actions through the remote clients: `connect_create_connector`, `connect_delete_connector` (Q5c), `pg_set_replica_identity` (Q2c, its own name), `source_grant_select`, `source_create_heartbeat_table` (Q2c, Q2d); the setup-only source ops over `SetupSourceWriter` (§4 item 4): `source_create_publication`, `source_create_canary_table`, `source_insert_canary` (the nonce comes from the ladder runner, §3.4), `source_create_read_role` (§3.7) |
| T3 | `source_alter_system` (allow-list: `wal_level`, `max_replication_slots`, `max_wal_senders`, `max_slot_wal_keep_size`, `wal_sender_timeout`; `pending_restart` in the preview), `source_restart` (only a database this room created, by receipt) |

### 2.6 Targets and reach (S7)

`Env.Targets` starts as the hosts the person named in the `SetupRequest` (the source database, an existing Kafka,
Connect or Registry, a Kubernetes context and namespace, a cloud account). It grows only through
`POST /api/rooms/{id}/setup/targets {kind, address, class?}`, **a driver click** (`setup.target_registered`), never
an auto-run op; the model may ask for a target in a message. The server resolves the address and refuses, always:
loopback addresses of another host, link-local (`169.254.0.0/16`, `fe80::/10`), cloud metadata endpoints
(`169.254.169.254`, `fd00:ec2::254`, `metadata.google.internal`), multicast, unspecified (`0.0.0.0`, `::`), and
the server's own addresses. A new target's class is never below `shared`. `http_get` and `net_probe` reach only
listed targets; the tier of a step comes from the class of the **endpoint the op resolves from its params**
(host and port after name resolution on the runner, matched to a target entry), never from a label the model
supplies.

**The denylist is enforced at dial time on the runner, against the IP that is dialed.** The runner re-resolves
names on its own network, so split DNS or DNS rebinding could turn a listed name into `169.254.169.254` after the
server checked it. Every runner connection (`http_get`, `net_probe`, the remote clients' Connect, Registry and
database dials) goes through one `net.Dialer` whose `Control` hook receives the resolved address the kernel is
about to connect to and refuses the denylisted ranges there, before `connect(2)`; there is no earlier resolve
whose answer could differ from the dialed one. A unit test uses a resolver that answers a listed name with
`169.254.169.254` and asserts the dial is refused with the address in the error (U1-2). The server keeps its own
check at registration as a first filter, not as the guarantee.

### 2.7 Standalone mode (A3)

`piperoom-setup --standalone --project ./orders` starts `internal/quickstart.Run` (A5: the `--quickstart` server of
`cmd/server/quickstart.go` moved into an importable package) in-process with `WriteMode: on` and `SetupEnabled:
true`, the in-memory store, dev login on loopback, and the runner attached over loopback with `--yes` allowed.
The pairing code is generated and consumed in-process. The person sees the same room in the browser; approvals
are one click (solo: one person owns every layer of the provisional pipeline). When the pipeline is live the
process keeps running as the pipeline's watcher (§3.7). `--quickstart` on `cmd/server` keeps its read-only
contract; only the package gains the two options and only standalone sets them.

## 3. The agent loop in setup mode

### 3.1 Same loop, three differences

`agent.Run`'s contract is unchanged; its implementation gains a mode switch on `Alert.Mode == ModeSetup` (A4) that
changes:

1. **The prompt frame** (`internal/agent/setup_prompt.go`): the alert is "no pipeline yet"; the goal is a green
   ladder; the rules say plan before propose, re-plan after failure, request verification rather than claim it,
   never ask the person to run a command, never ask for a secret value, stop and ask after the bounds. The
   untrusted-data wrapper, the receipts and the hypothesis grounding apply unchanged.
2. **The registries**: `Deps.Tools` holds the Q read tools and the setup tools over the remote clients, scoped to
   the provisional pipeline (§3.2); `Deps.Actions` holds the Q actions over the remote clients plus the setup-only
   actions. Both are built by `wiring.SetupStack` only for a room with a paired runner and only when
   `PIPEROOM_SETUP_ENABLED=on` **and** write mode is on (§3.8).
3. **The bounds** (`Options.Setup`): 3 attempts per step, 12 executed steps per plan, 45 min wall clock, a
   per-setup token budget (`PIPEROOM_SETUP_TOKEN_BUDGET`, default 2 M); on any bound the run emits `setup.blocked`
   and goes idle like a budget stop today (`*BudgetError`).

### 3.2 The provisional pipeline (A1)

A setup room has no `owners.yaml` entry until its last step, but every Q tool checks `tools.ScopeFor` and every
action implements `Scoped.CheckScope(p owners.Pipeline, ...)` and reads the pipeline in `Validate` (Q5c: "the name
is in the pipeline's `connectors`"; Q2c: "table in `source_tables`"). So U0 adds `owners.Provisional`:

```go
// Provisional builds the pipeline a setup room is scoped to before it is registered: kind cdc, the request's
// id, tenant, topic prefix, tables, connector names and hosts (as connections: entries with no credentials),
// every layer unowned. The room passes it to tools.WithPipeline and to every action's CheckScope and Validate.
func Provisional(req events.SetupRequest) (Pipeline, error)
```

The provisional pipeline **grows only through a driver click**: `POST /api/rooms/{id}/setup/pipeline
{add_connectors?, add_tables?, add_consumer_groups?, add_sink_tables?}` emits `setup.pipeline_extended` and the
room re-derives the scope; the model may ask for an extension in a message. Hosts are targets (§2.6). The final
`pipeline_register` writes exactly the provisional pipeline plus the owners and the `connections:` block of §3.7;
nothing the model said is in it.

**Q actions are reused, not re-implemented.** `internal/setup/remoteclients` implements `clients.ConnectClient`,
`clients.ConfigValidator`, `clients.ConnectLifecycleWriter`, `clients.SourceSettingsReader`,
`clients.SourceAdminWriter`, `clients.SetupSourceWriter`, the Kafka and Registry readers and the new three source
clients' interfaces, each method as one T0 or T2 op to the runner. The Q tools and actions are constructed over
these clients by `wiring.SetupStack` exactly as `wiring.OpenStacks` constructs them over the real ones, so Q5c,
Q2c, Q2d, Q4a, Q6a work in a setup room with their own names, validations and previews.

### 3.3 Plan, act, observe, re-plan

- `setup_plan` (KindRoom): the model records `{phases: [{name, steps: [{op, params_hint, verify}]}], supersedes?}`.
  The server fills the real tier per step from the resolved endpoint's class and stores `setup.plan`. A proposal
  whose step is not in the current plan is refused by `Proposals.Create` (`ErrNotInPlan`).
- `propose_fix` with Q and setup actions: the existing path, including `params_hash`, preview and approvals. The
  tier decides the route: T0 ops are tools, never proposals; T1 proposals are scheduled (`proposal.scheduled`,
  policy `setup_local`, `PIPEROOM_SETUP_LOCAL_DELAY`, cancellable) by the P2 mechanism; T2 and T3 wait for the
  approver of §3.6.
- Observe: each step's `Verify` sentence is checked by the existing verify-after-fix path with read tools; the
  `agent.verification` event is the outcome. A failed verification is a failed step.
- Re-plan: the runner's `OpResult`, the logs (§2.5) and `runbook_lookup` with `versions` (Q1) go back to the model;
  it must emit a new `setup_plan` before the next proposal.

### 3.4 The verification ladder runs on the server (S6)

The model **requests** verification (`setup_request_verification`, KindRoom, no parameters) and never reports it.
The ladder runner (`internal/room/ladder.go`, A5, over `plan.Ladder` from A9) then:

1. generates a canary nonce (16 random bytes, hex) and runs `source_insert_canary {nonce}` as a T2 step of a
   system-origin proposal (approved like any other; auto on a local source);
2. issues each rung's read itself through the room's clients (the remote clients, and the server's own clients
   when it has them, §3.7) and evaluates the predicate on the structured `Data`, never on a summary;
3. records, per rung, the step id and receipt id it produced, which exist in this room's log after the last
   executed step;
4. emits `setup.verified` with every rung's outcome. Only the ladder runner emits it; the agent's `Sink` refuses
   the type (a test).

Rungs for `(source, target, sink)`: connector exists and RUNNING (or the target's equivalent status); every task
RUNNING; the canary topic exists; the snapshot finished (`debezium_metrics` `SnapshotCompleted`, else the canary row
count on the topic); the canary event with the nonce as key seen by `kafka_find_by_key` within 60 s; the canary
subject in the registry when one is configured; the canary row in the sink when a sink was set up; a real change
the person makes (`setup.awaiting_change {table, example_sql}`, the UI shows the SQL) seen within 10 min. Rungs 1 to
7 are the ladder's; rung 8 is the person's and the only thing the agent waits on a human for that is not an
approval. `Proposals.Create` refuses `pipeline_register` unless a `setup.verified` with rungs 1 to 7 green exists for
the current plan and no executed step came after it.

### 3.5 Classes, tiers and the guard

The server derives one class per target from structured fields of `Hello` and `env_detect` only, and from the
person's answers:

| Class | When |
|---|---|
| `local` | the resolved endpoint is loopback or a local Docker engine socket, or a compose service under the project dir, **or** a database or Kafka this room created (by an `action.executed` receipt in this room's log, never by model text) that is also on loopback or the local engine |
| `shared` | anything else, by default |
| `production` | the person said so for that target, or the target matches `PIPEROOM_SETUP_PROD_PATTERNS` (host globs, `kubectl` context names, cloud account ids) |

`POST /api/setup` `classes` and the raise route are **floors** combined with `max`: a person can raise a class,
never lower it below what the server derived. Tier of a step = max(op floor, class of the resolved endpoint);
`plan.TierOf` is pure and table-tested.

The guard table (plan §3.6) is checked **at `Create`, again before `Execute`, on every step of the "roll back this
setup" proposal, and on the runner** (the `Class` field of `OpRequest`). A T3 step whose guard row says "maintenance
window" (changing `wal_level`, `binlog_format`, or restarting a database) is created only inside a window the driver
recorded with `POST /api/rooms/{id}/setup/maintenance-window {start, end}` (`setup.maintenance_window`, at most
4 hours, at most 24 hours ahead), and the window is re-checked at `Execute`.

### 3.6 Tenant binding and who approves (S3)

- `SetupRequest` carries `tenant`. With `tenants:` configured the creator must be a member; without tenancy the
  tenant is `""`. Setup rooms, pairing codes, tokens and the runner registry are keyed by `(tenant, room,
  creator)`; a runner can execute only for its room.
- Before registration every layer of the provisional pipeline is unowned, so P2b-3 would let the driver approve
  everything with grant `fallback`. On a **team server** that is not enough. The approver of a T2 or T3 step is:
  the owners of an existing pipeline whose host, topic prefix, connector or table the step's resolved endpoint
  matches (the proposal's `required_layers` are that pipeline's layers); otherwise a **tenant admin**
  (`tenants.<t>.admins:`, a new owners field, U0; principals as in `members:`) with grant `admin`. The driver's
  `fallback` grant applies only in solo mode (`mode.solo`: one person is the whole tenant, or no tenancy and one
  user) and on the standalone server.
- `pipeline_register` on a team server needs an approver who is not the requester (a tenant admin or, with an
  overlap, the overlapping pipeline's owners), unless solo mode applies. It writes `tenant:` and the creator as
  owner of every layer; a tenant admin may edit the owners in the UI before approving.
- **Overlap check**: `owners.Overlap(p Pipeline) []Overlap` compares topic prefix, connector names, source tables
  (per source host) and hosts against every loaded pipeline across tenants; `pipeline_register` refuses with the
  list unless the overlapping pipeline's owners are among the approvers of that very proposal. The refusal names
  the overlap kind and the requester's own tenant's pipelines only; a pipeline of another tenant appears as "a
  pipeline in another tenant" with its tenant admin as the contact, so the check leaks no other tenant's names.

### 3.7 Watching after setup (A2)

"Done means watched" needs the server to read the new pipeline. The server never receives the runner's secrets,
so the mechanism depends on the target class, decided per pipeline at registration and shown on the registration
card:

| Target class | Who watches | How the credentials get there | What the person sees |
|---|---|---|---|
| `shared` or `production` (a database the person brought, a Kafka or Connect on the network, Strimzi, managed) | the team server, as for any pipeline | the setup creates a **read-only role** (`source_create_read_role`, T2, the grants of `docs/pilot/permissions.md`): the **runner generates the password**, stores it in its own secret store under a name, and `CreateReadRole` returns only that name; read-only API keys where the target supports them are stored the same way. The person then clicks **Register read credentials with this server** in the UI, which only asks the runner; the runner prints the server origin and the secret names and waits for `y/N` on its terminal, then sends the values in one `Credentials` frame over the paired socket straight into `PIPEROOM_SECRETS_DIR/<pipeline>/` (0600, server side), never through the model, an action result or the event log (the server emits `setup.credentials_registered {names}`); `pipeline_register` writes a `connections:` block whose values are `file:` references to those paths (tenancy §3: references only). Nothing is registered without the click and the terminal confirmation. With more than one replica `PIPEROOM_SECRETS_DIR` must be one volume every replica mounts (the Helm chart mounts one `ReadWriteMany` volume or a Secret-backed store for it; the pilot compose uses a named volume); a replica that cannot write to it refuses to start with setup enabled | "Watched by this server. Read role `piperoom_ro` on `db.acme`; Connect at `connect.acme:8083`." |
| `local` (a Compose stack on a laptop, Docker Desktop) | the **standalone server on the same machine** (`piperoom-setup --standalone` keeps running; a later `piperoom-setup --watch` restarts it against the written `owners.yaml`) | the standalone server reads the same `file:` references from `<project dir>/.piperoom/` | "Watched locally while `piperoom-setup` runs. A team server cannot reach 127.0.0.1." A team-server room that set up a local target registers the pipeline with `watched_by: local` and the server does not watch it |

Why not a runner relay for local targets: a long-lived daemon that proxies the watcher's reads is a different
product (an agent on every laptop), nobody pages on a laptop stack, and the standalone server already has the
watcher. The relay is listed as a candidate for a later wave in the plan's §8.

The standalone server therefore does hold read credentials, on the same machine as the runner and in the same
directory the runner wrote; the claim is that **credentials never cross the boundary to a team server except by
the person's click, and the model never sees them**, not that no server process ever holds any.

### 3.8 Write mode and `PIPEROOM_SETUP_ENABLED`

`PIPEROOM_SETUP_ENABLED=on` requires `PIPEROOM_WRITE_MODE=on`; the server refuses to start otherwise with an error
that names both. With write mode off nothing changes: no setup action, no remote writer, no runner endpoint, so
the P1a-3 test ("write mode off: no `*Writer` reachable") stays true with setup in the tree. The standalone
server sets both on through the quickstart package's options (§2.7).

### 3.9 Finishing

On `setup.verified` with rungs 1 to 7 green the agent proposes `pipeline_register` (T2, layer `environment`, the
approver of §3.6): the owners entry (the provisional pipeline, the owners, `connections:` per §3.7, `auto_approve:
[]`, `setup: {target, runner_label, room_id, watched_by}`) and, when `repos:` is set, the pull request instead
(§3.10). On the standalone server it writes the file next to the project; on a team server it writes
`OWNERS_DIR/<id>.yaml` and the server reloads (`owners.Reload`, `pipeline.registered`); the watcher picks the
pipeline up on its next tick. The room closes with resolution `pipeline live`; the hand-off writer produces the
summary; `room.labelled` gets `cause: setup`.

### 3.10 `repos:` before registration

Q7's PR path needs the pipeline's `repos:`. In setup it comes from the `SetupRequest` (`repos:` typed by the
person in the form) or from the tenant's default (`tenants.<t>.repos:`, a new owners field, U0). When present, the
compose file, manifests and the owners entry go out as one PR through `repo_open_pull_request` (Q7c) and the room
waits for `pr_merged` as Q7e does; the local writes of the same artifacts are then T1 only for a local target.

## 4. Contract additions (U0 lands items 1 to 8; the design rows the rest)

### 1. A new contract file: `internal/setup/protocol/protocol.go` (leads)

The wire types of §2.2 (`Hello`, `OpRequest`, `OpResult`, `OpStatus`, `Resume`, `Cancel`, `Version`, the
compatibility rule) and:

```go
// RunnerExecutor is how a setup action or remote client reaches the paired runner. The room service implements
// it over the runner registry; wiring hands it to the setup stack of a room. ErrNoRunner when none is paired.
type RunnerExecutor interface {
	Execute(ctx context.Context, roomID string, req OpRequest) (OpResult, error)
}
var ErrNoRunner = errors.New("setup: no runner paired to this room")
var ErrParamsHash = errors.New("setup: params_hash mismatch")
var ErrDuplicateStep = errors.New("setup: step already executed on this runner")
```
`internal/clients/clients.go` imports nothing of it. The file joins the contract list in `AGENTS.md`.

### 2. Events, `internal/events/events.go`

```go
SetupPairing            Type = "setup.pairing"            // {outcome: ok | bad_code | expired | rate_limited | replaced, source, runner_label?}
SetupRunnerAttached     Type = "setup.runner_attached"    // {runner_label, os, arch, tools: {name: version}, project_dir, catalogue_version, protocol_version}
SetupRunnerLost         Type = "setup.runner_lost"        // {runner_label, last_seen}
SetupRunnerRefused      Type = "setup.runner_refused"     // {op, reason, proposal_id?, step_index?}
SetupPlan               Type = "setup.plan"               // {plan_id, supersedes?, phases: [{name, steps: [{index, op, target, tier, layer, verify}]}]}
SetupTargetRegistered   Type = "setup.target_registered"  // {kind, address, class, by}
SetupTargetClassed      Type = "setup.target_classed"     // {target, class, reason, by: "server" | "person"}
SetupPipelineExtended   Type = "setup.pipeline_extended"  // {add_connectors?, add_tables?, add_consumer_groups?, add_sink_tables?, by}
SetupSecretSet          Type = "setup.secret_set"         // {name, actor}
SetupCredentialsRegistered Type = "setup.credentials_registered" // {names, runner_label}; values nowhere
SetupMaintenanceWindow  Type = "setup.maintenance_window" // {start, end, by}
SetupVerified           Type = "setup.verified"           // {plan_id, nonce_hash, rungs: [{name, ok, step_id?, receipt_id?, note}], first_event_latency_ms?}; emitted by the ladder runner only
SetupAwaitingChange     Type = "setup.awaiting_change"    // {table, example_sql}
SetupBlocked            Type = "setup.blocked"            // {reason: "attempts" | "steps" | "wall_clock" | "budget" | "runner_lost" | "refused", step_index?, question}
PipelineRegistered      Type = "pipeline.registered"      // {pipeline_id, tenant, path | pull_request_url, watched_by: "server" | "local", by, approved_by}
```
`SetupReceipt` (§2.2); `ActionExecutedPayload.Receipt *SetupReceipt json:"receipt,omitempty"`;
`ProposalScheduledPayload.Policy` gains `PolicySetupLocal` (`"setup_local"`); `ApprovalGivenPayload.Grant` gains
`GrantAdmin` (`"admin"`); `LayersOf("cdc")` gains `"environment"` **at the end**; `room.created` gains
`mode: "setup"` next to `shadow`. `SetupRequest`:

```go
type SetupRequest struct {
	PipelineID  string            `json:"pipeline_id"`
	Tenant      string            `json:"tenant,omitempty"`
	Title       string            `json:"title"`
	SourceKind  string            `json:"source_kind"` // postgres | mysql | sqlserver | mongodb | oracle
	Target      string            `json:"target"`      // compose | host | strimzi | msk | confluent | aiven | dbzserver | embedded
	TopicPrefix string            `json:"topic_prefix"`
	Tables      []string          `json:"tables"`
	Connectors  []string          `json:"connectors,omitempty"`
	Targets     []SetupTarget     `json:"targets"`     // {kind, address, class?}; classes are floors
	Repos       []string          `json:"repos,omitempty"` // repo ids of owners.yaml repos: or the tenant default
}
```

### 3. Agent, `internal/agent/contract.go`

```go
Mode string `json:"mode,omitempty"` // Alert.Mode: "" (incident) or ModeSetup; Raw holds the SetupRequest
const ModeSetup = "setup"
Setup *SetupBounds // Options.Setup; nil outside setup mode
type SetupBounds struct { AttemptsPerStep int; StepsPerPlan int; WallClock time.Duration; TokenBudget int64 }
var ErrNotInPlan = errors.New("agent: proposal step is not in the current setup plan")
```

### 4. Actions and clients, `internal/actions/action.go`, `internal/clients/clients.go`

```go
// Receipted is implemented by an action whose Execute can hand back a receipt (setup actions). The executor
// prefers it over Execute and stores the receipt on action.executed. (U)
type Receipted interface {
	ExecuteReceipt(ctx context.Context, params json.RawMessage) (string, events.SetupReceipt, error)
}
// Tiered: see rev 1; the class is of the endpoint the room resolved from params, passed in. (U)
type Tiered interface {
	Tier(params json.RawMessage, class string) (string, error)
}
// LooksSecret reports whether a config key names a credential (the rule of internal/actions/connector_config.go,
// moved here so the runner's ops, Q4a and the setup validators share it). (U; A2 switches its private copy in U1-23)
func LooksSecret(key string) bool

// SetupSourceWriter holds the writes only a setup makes; built only with write mode on, on the write pool.
// Postgres in U1-9 (A3), MySQL in U2a (A8), the others in A10's clients; ErrNotSupported where a source has no
// equivalent. The fail-closed scope rule of SourceWriter applies to every method. (U)
type SetupSourceWriter interface {
	CreatePublication(ctx context.Context, name string, tables []string) error
	CreateCanaryTable(ctx context.Context, table string) error
	InsertCanary(ctx context.Context, table, nonce string) error
	CreateReadRole(ctx context.Context, role string, tables []string) (secretName string, err error) // the permissions.md grants; the runner generates and stores the password and returns only its name
	AlterSystem(ctx context.Context, name, value string) (pendingRestart bool, err error)       // name in the allow-list of §2.5 only
}
```

### 5. Owners, `internal/owners` (A3 implements; the schema is fixed here)

`tenants.<t>.admins: [principals]`, `tenants.<t>.repos: [..]`; per pipeline `setup: {target, runner_label, room_id,
watched_by}`; `OWNERS_DIR`; `Provisional(req)`, `Overlap(p)`, `Reload()`.

### 6. Config and API, `internal/config/config.go`, `docs/api.md`

`PIPEROOM_SETUP_ENABLED` (`off` | `on`; `on` requires write mode on), `PIPEROOM_SETUP_PROD_PATTERNS`,
`PIPEROOM_SETUP_TOKEN_BUDGET`, `PIPEROOM_SETUP_LOCAL_DELAY` (default `20s`; `0` = every T1 step waits for a click),
`PIPEROOM_SETUP_PLUGIN_HOSTS`, `PIPEROOM_SECRETS_DIR` (server side, §3.7), `OWNERS_DIR`; runner side
`PIPEROOM_SETUP_SECRETS_FILE`, `PIPEROOM_SETUP_SERVER_URL`, `PIPEROOM_SETUP_CODE`.

| Method | Path | Body | Returns | Notes |
|---|---|---|---|---|
| POST | `/api/setup` | `SetupRequest` | `{room, code, expires_at, command}` | opens a setup room (creator is driver), single-use code; tenant membership checked; `pipeline_id` must not exist; `PIPEROOM_SETUP_ENABLED=on` |
| GET | `/api/runners/ws` | first frame `{code}` or `{token}` | WebSocket | rate-limited, constant-time compare; one runner per room; audited |
| POST | `/api/rooms/{id}/setup/targets` | `{kind, address, class?}` | `Event` | driver only; the denylist of §2.6; class ≥ shared |
| POST | `/api/rooms/{id}/setup/targets/{t}/class` | `{class}` | `Event` | driver only; raise only |
| POST | `/api/rooms/{id}/setup/pipeline` | `{add_connectors?, add_tables?, add_consumer_groups?, add_sink_tables?}` | `Event` | driver only; extends the provisional pipeline |
| POST | `/api/rooms/{id}/setup/secret` | `{name, value}` | `204` | driver only; same CSRF rule as every non-GET; excluded from request logging (test); forwarded to the runner in one frame, stored nowhere; emits `setup.secret_set` |
| POST | `/api/rooms/{id}/setup/credentials` | – | `202` | driver only; asks the paired runner to register the read credentials of §3.7; the values travel in the runner's `Credentials` frame after its terminal confirmation, never in this request; `setup.credentials_registered` on success |
| POST | `/api/rooms/{id}/setup/maintenance-window` | `{start, end}` | `Event` | driver only; ≤ 4 h, ≤ 24 h ahead |
| POST | `/api/rooms/{id}/setup/verify` | – | `202` | driver only: the person can also request the ladder |
| GET | `/api/pipelines` | | rows gain `setup: {room_id, target, registered_at, watched_by}` | |

Snapshot gains `setup: {mode: true, runner: {label, connected, tools}, plan_id, phase, step_index, bounds, targets:
[{kind, address, class}], provisional: {topic_prefix, tables, connectors}, verified: [...], watched_by}`.

### 7. Settled by the design rows

`U1-0`: the protocol's final field list and the compatibility rule; the Windows service API. `U12-0`: the Strimzi
CRD versions, the in-cluster runner Job and its RBAC, how the pairing code reaches a Job. `U13-0`: the three managed
clients' auth and operations.

## 5. Targets: ops, verify, what is new per target

| Target | Ops added (tier floor) | Verify rung 1 source | New code |
|---|---|---|---|
| Compose (U1) | §2.5 | `connect.status` through the remote client to the project's Connect | the driver, templates (KRaft Kafka, Connect with the plugin dir mounted, optional Registry), the compose validator, the plugin layout check |
| Host (U8) | `service_status`, `journal_tail` (T0); `properties_write` (T1 under the project dir, T2 elsewhere), `service_install` (template only), `service_restart` (T2; `systemctl` on Linux, `mgr` on Windows) | the same | the unit and service templates; standalone versus distributed args; the properties key allow-list |
| Strimzi (U12) | `k8s_get`, `k8s_logs`, `k8s_events`, `strimzi_build_status` (T0, one namespace); `k8s_apply` (T2, templates and the field allow-list, `--dry-run=server` as preview), `k8s_secret_apply` (T2) | `KafkaConnector` status conditions | the in-cluster runner Job (Helm), the `KafkaConnect` template with `build.plugins` from the knowledge index, RBAC for one namespace |
| MSK Connect (U13) | `msk_describe_*` (T0); `msk_create_plugin` (from an S3 object the person uploaded; T2), `msk_create_worker_config`, `msk_create_connector`, `msk_delete_connector` (T2; T3 on production) | `describe-connector` state | the client over the AWS SDK with the shared AWS config, VPC and IAM checks |
| Confluent Cloud (U13) | `ccloud_list_plugins`, `ccloud_connector_status` (T0); `ccloud_create_connector` (validate first), `ccloud_acl_create` (T2) | connector status | the client over the Connect v1 API |
| Aiven (U13) | `aiven_service_get`, `aiven_connector_status` (T0); `aiven_create_connector`, `aiven_integration_create` (T2) | connector status | the client over the Aiven API |
| Debezium Server (U11) | `properties_write` (T1), `compose_write`/`service_install` for the server process; `sink_probe` (T0) | process up plus the sink's canary read | the properties template per sink; the sink probes |
| Embedded (U6) | `project_inspect` (T0: build files only); `file_write` for the reference project (T1, never run by the runner) | the log file the person points at shows the nonce | the templates (Maven and Gradle), the version mismatch rules |

## 6. Sources: prerequisite drivers

Each driver implements `plan.SourceDriver`: `Checks()` (ordered reads with the expected value and the fix op),
`FixFor(check)`, `CanaryTable()`, `ReadRole()`, `ConnectorConfig(target, tables, secrets)` (with `${secret:}`
references). Postgres (A3, U1-9) uses Q2's `SourceSettingsReader` and `SourceAdminWriter` plus
`SetupSourceWriter`; MySQL (A8, U2a) Q8's plus the same. SQL Server, MongoDB and Oracle (A10) get thin read-only
clients plus the admin methods each needs behind the same interfaces, with `ErrNotSupported` where a method has no
meaning. Drivers (`go-mssqldb`, the MongoDB Go driver, `go-ora`) are new modules, each listed in its PR with why.

## 7. Security model

- **Credentials.** The runner holds the Docker socket, `kubectl` context, cloud credentials and database
  passwords; the server holds the model key and the room database. Read credentials reach a team server only by
  the person's click (§3.7), by reference, into a server-side directory. The pairing token is random, in memory,
  idle-expiring and rotated.
- **The model never writes a command, and never writes an artifact that executes.** Only typed ops; executed
  artifacts are templated or validated (§2.5); the guard table refuses the destructive subset on production
  whatever is asked, at `Create`, `Execute`, rollback and on the runner.
- **Two locks on every write, and their limit.** The server routes by tier and approval; the runner refuses a T1+
  op without a proposal, recomputes `params_hash`, refuses duplicates and the guard, and prints every write. **This
  second lock does not hold against a malicious server:** a server the person paired with can send any op in the
  catalogue with a made-up proposal. What holds against that is the local confirmation at pairing (the runner shows
  the origin, the room and the creator), the catalogue and validators (no artifact executes with host privileges,
  nothing deletes data, no data leaves except redacted op output), the runner's own op log, and the `--server`
  origin being what the person typed, plus two runner-local floors: the runner keeps **its own target registry
  with classes** (the targets and classes the person confirmed at pairing, raised by every `setup.target_classed`
  it is told of, never lowered) and raises `OpRequest.Class` to its own floor before the guard runs, so a server
  that sends `Class: shared` for a production target still meets the guard; and **every T3 op needs a `y` on the
  runner's terminal**. What a malicious server can still do: start validated containers, read non-secret project
  files, and send T2 ops against the targets the person registered (create a connector, a publication, a read
  role); §9 keeps that as the residual risk.
- **Untrusted output.** Everything the runner returns is redacted literally and by pattern on the runner,
  injection-scanned and wrapped on the server, and treated as data by the prompt. Logs are the largest new surface
  and are capped; and because tiers, classes, targets and the pipeline grow only by driver clicks or server
  derivation, an injected instruction can at most make the model propose a validated T1 step or ask for a click.
- **Tenancy.** Rooms, codes, tokens and runners are keyed by `(tenant, room, creator)`; registration needs a
  non-self approver on a team server and refuses overlaps (§3.6).
- **Audit.** Pairing attempts, secret names, runner refusals, target registrations, class changes, pipeline
  extensions, maintenance windows, every op with its receipt, every rung of the ladder: all in the room log and in
  the export's integrity hash; `room.labelled` covers blocked and rolled-back setups.
- **Blast radius.** One runner, one room, one tenant, one project directory, the target list the person named
  and extended by click; the address denylist of §2.6; plugins from listed hosts with digests pinned in the
  knowledge index.
- **Threat model update.** U1-17 (C4) covers S1 to S7 of the #641 review by name before U1 ships; its findings are
  U1 rows.

## 8. Test strategy

- **Unit, no Docker:** every op's `Validate` and `argv` builder (table tests: path escapes on three OSes, symlinks
  and reparse points, Windows forms of §2.3, secrets in content, each compose and manifest validator rule, the
  properties key allow-list), `plan.TierOf`, the classifier (floors, receipts-only "created by this room"), the
  ladder's predicates on fixture `Data`, the guard at the four check points, the protocol (`httptest` WebSocket
  with a fake runner and a fake server: pairing single-use and rate limit, constant-time compare, token expiry and
  rotation, `params_hash` mismatch, duplicate step, `Resume` reconciliation), the prompt snapshot in setup mode, the
  room routes with a fake runner (driver-only, logging exclusion, overlap refusal, non-self approver). `go test
  ./...` stays Docker-free.
- **Integration (`-tags integration`), Linux:** the Compose driver against a real Docker on the CI runner: the
  agent (recorded model turns) takes an empty project dir to a RUNNING Postgres connector and a green ladder with a
  real nonce; the `devctl setup-break` scenarios each end green; a compose file with each forbidden key is refused
  before `compose_up`; `file_read` of the secret store is refused and no frame carries a stored value.
- **CI matrix (`.github/workflows/setup-matrix.yml`):** `ubuntu-latest`, `macos-latest`, `windows-latest` run the
  unit tests, the §2.3 path tests and the runner's capability detection and service ops with fakes;
  `ubuntu-latest` also runs the Compose integration; `macos-latest` runs Compose through Colima when it is
  available on the runner image and is otherwise a documented skip; Windows Linux-container runs happen on the
  owner's dev machine as a self-hosted nightly (plan §7 item 7).
- **Strimzi:** `kind` on `ubuntu-latest` with the Strimzi operator, a one-broker KRaft Kafka and the in-cluster
  runner Job; the ladder to a canary event; 20-minute budget, nightly.
- **Managed clouds:** recorded fakes (`httptest` cassettes, secrets scrubbed at record time, a test that fails on a
  credential-looking string) in CI; credentialed manual checks under `//go:build manual` with the sandbox accounts
  (plan §7 item 6), run by a lead before each U13 row merges and dated in the PR.
- **Databases:** Postgres and MySQL on the dev stack; SQL Server, MongoDB (replica set of one) and Oracle as compose
  profiles for integration on Linux; RDS, Azure SQL and Atlas as manual checks.
- **Evals:** `evals/setup/**` replays runner recordings; each U row ships a scenario under `evals/scenarios/setup/`;
  the pass rate per target and per source is in the report; the CDC incident pass rate must not move.
- **Real runs** are in every target row's "Done when" with the room export attached to the PR.

## 9. Known gaps and follow-ups

- A malicious server the person paired with can start validated containers, read non-secret project files and
  run T2 ops against the registered targets through the runner (§7); T3 ops and a lowered class are stopped by the
  runner's own floor and terminal confirm. Signed catalogues, a server allow-list in the runner's config and a
  terminal confirm for T2 on `production`-class targets are candidates for a later wave.
- The standalone mode keeps rooms in memory; a laptop that sleeps loses the setup room. `setup-log.jsonl` and the
  op log keep the record; `--resume` is not in this wave.
- A runner relay so a team server can watch a laptop stack is not in this wave (§3.7).
- Oracle's prerequisites vary by edition and by `CDB`/`PDB`; the U9 driver covers the free editions and says so
  when it meets another.
- Windows service ops are estimated, not measured; U1-0 re-estimates after the spike.
