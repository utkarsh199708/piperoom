# Setup runner threat model (S1 to S7)

Row U1-17 (KAN-297), C4, docs only. Checked against branch `claude/serene-maxwell-1dgs4o` at `cd3e1b8`. The design
is `docs/design/setup-agent.md` (rev 2); S1 to S7 are the names the #641 review gave its seven concerns, mapped to
the design's sections in the table below. Every claim cites the code as `path:line` or as `path` plus a symbol.
Where the code and the design differ, the code is what is described and the difference is said. A gap is under
**Findings**, numbered for the leads to turn into U1 fix rows before U1-R. Style as `docs/security/findings.md`
and `docs/security/model-providers.md`. Nothing here was run against a real runner or server: the tests named
below exist, and this review did not re-run them.

| Name | Concern | Design |
|---|---|---|
| S1 | artifact execution | §2.5 |
| S2 | pairing and replay | §2.1, §2.2 |
| S3 | tenant binding | §3.6 |
| S4 | secrets | §2.4 |
| S5 | confinement | §2.3 |
| S6 | the ladder | §3.4 |
| S7 | target reach | §2.6 |

## 0. What is wired on this commit

Several controls below exist as package code with unit tests but are not reached by the shipped binaries. The
threat model is only as strong as the wired part, so the status comes first.

| Piece | State | Where |
|---|---|---|
| Runner binary `piperoom-setup` | builds the real catalogue `ops.New` with only `ProjectDir` set, and registers the Compose or host driver when the first Welcome arrives | `internal/setup/setupcmd/setupcmd.go:105-120`; `internal/setup/setupcmd/targets.go:39-61` |
| Welcome's target list | **never filled by the server**: both Welcome literals set room id, title, pipeline, tenant, creator and version only | `internal/httpapi/runners.go:97-105`, `internal/httpapi/runners.go:210-218` |
| Driver choice | with no targets, `chooseTargetDriver` answers `compose`; the host driver is chosen only for a Welcome that names a `host` target, which no server sends | `internal/setup/setupcmd/targets.go:77-101` |
| Ops reaching a host (`http_get`, `net_probe`, `sink_probe`) | allow-list is `Env.Targets`, set once from `Config.Targets` at `ops.New`; the shipped runner passes none and the registry has no setter | `internal/setup/ops/op.go:234`; the `Registry` methods (`op.go:251-550`) include no target setter; `setupcmd.go:105` |
| Remote-client ops (`connect_*`, `source_*`, `kafka_*`, `registry_*`) | sent by the server, **not in the runner catalogue** except `source_publication_tables`, which answers `not_supported` without a source; `RegisterSQLServerSource` has no non-test caller | `internal/setup/remoteclients/ops.go:21-84`; `internal/setup/remoteclients/doc.go:99-106`; `internal/setup/ops/op.go:237-241`; `internal/setup/ops/sqlserver_ops.go:85` |
| Frozen proposal in `OpRequest.Proposal` | **never set by the server**: `remoteclients.WithProposal` has no non-test caller, so the first T1+ request of every proposal is refused `params_hash` | `internal/setup/actions/adapter.go:420-430`; `internal/setup/remoteclients/client.go:225`; `internal/room/service_executor.go:445-449` |
| T3 on the runner | refused as `tier_disabled` before any prompt (`Options.AllowT3`, never set by `cmd/setup`) | `internal/setup/runner/ops.go:241-246`; `internal/setup/runner/runner.go:92-97` |
| Secret materialisation (`${secret:name}` to a value) | **not wired**: `Store.Materialise`, `MaterialiseEnv` and `RegisterMaterialised` have no caller outside the store | `internal/setup/runner/secrets.go:579`, `:616`, `:636`; the comment "until U1-4" in `internal/setup/targets/compose/templates.go:303` |
| Class `local` | never derived: targets are floored at `shared`, and `plan.Classify` has no non-test caller | `internal/room/setup_targets.go:313`; `internal/setup/plan/class.go:114` |
| Debezium Server driver | imported by nothing outside its package | `internal/setup/targets/server/server.go:189` (no importer) |

Consequence: on this commit a paired runner refuses every T1+ op (no frozen proposal) and every client op
(unknown op), and the ladder cannot pass rung 5. The system fails closed. The sections below describe the controls
as they are written, so that the fix rows (Findings 1 to 4) do not ship them unchecked.

## 1. Assets, actors, boundaries

- **Assets on the runner's machine:** the Docker engine, `systemctl --user`, the source database credentials the
  person stores, the project directory, `.git/`, the runner's secret store and receipt key
  (`internal/setup/runner/secrets.go:26-42`).
- **Assets on the server:** the room log, the approvals, `PIPEROOM_SECRETS_DIR` (registered credentials,
  `internal/room/setup_secrets.go:386-425`) and `OWNERS_DIR` (`internal/room/setup_register.go:799-830`).
- **Actors:** the person who runs the runner; the room's driver and approvers; the model (writes plans and
  proposals, reads runner output); a holder of a leaked pairing code; a malicious or compromised server the person
  paired with; text from logs and endpoints the runner reads (untrusted).
- **Boundaries:** server to runner is one WebSocket the runner dials (`internal/setup/runner/runner.go:386-411`);
  runner to the person's systems is the op catalogue (`internal/setup/ops/op.go:1-25`); runner output back to the
  model goes through redaction on the runner and the untrusted-data wrapper on the server
  (`internal/setup/runner/ops.go:376-379`; `internal/room/setup_ladder.go:691-718`).

## S1. Artifact execution

**Threat.** The model writes a file that something on the host then executes or loads (a compose file, a unit, a
properties file with `plugin.path`, a script), so "no shell from model text" is bypassed by content.

**Mitigations as implemented.**

- No op takes a shell string; every program runs from an argv slice. An AST test fails the build on a shell
  (`internal/setup/ops/shell_ast_test.go:30`; `internal/setup/ops/exec.go`).
- Compose files are rendered by `compose_write` from four templates with typed params, then validated before the
  write and again from a private snapshot of the on-disk bytes at `compose_up`, `compose_restart` and
  `compose_down`, so a file swapped after the write is not what docker reads
  (`internal/setup/targets/compose/compose.go:13-19`; `internal/setup/targets/compose/docker.go:146-158`).
- The validator is an allow-list of service fields (`image`, `command`, `environment`, `ports`, `volumes`,
  `depends_on`, `healthcheck`, `network_mode` restricted); every other field is refused, with named reasons for
  `privileged`, `cap_add`, `devices`, `pid`, `build`, `extends`, `entrypoint`, `env_file` and others
  (`internal/setup/targets/compose/validator.go:38-73`). Every `$` must be an exact `${KEY}` from the `.env`
  allow-list (`validator.go:85-107`). Images must be pinned by digest and in the knowledge allow-list
  (`validator.go:400-427`). Ports bind `127.0.0.1` on a local class (`validator.go:514-595`). A bind of the
  Docker socket or pipe is refused (`validator.go:688`, `validator.go:735`).
- The compose argv is pinned: `docker compose --project-directory <dir> -f <file> --env-file <env> <sub>` with one
  fixed flag sequence per subcommand; `down` has no `-v` form (`internal/setup/targets/compose/ops.go:304-345`;
  `internal/setup/targets/compose/stack.go:15-51`).
- `file_write` refuses names that execute or load: `.env*`, Dockerfiles, build files, hook configs, scripts and
  units by suffix, `*.properties`, the compose names, `.husky`/`.githooks`, YAML under `k8s/` or `manifests/`,
  every registered mount directory and the secret names; content must be UTF-8, at most 256 KiB, and may not set a
  key that `actions.LooksSecret` names (`internal/setup/ops/files.go:584-681`).
- Mount directories: the Compose driver registers `connect-plugins`, every render registers what it mounts, the host
  driver registers `kafka`, `connect-plugins`, `systemd` (Windows: `services`), the Debezium Server driver
  `debezium` and `systemd` (`internal/setup/targets/compose/compose.go:175`; `internal/setup/targets/compose/ops.go:96`;
  `internal/setup/targets/host/host.go:241-252`; `internal/setup/targets/server/server.go:231-236`).
- Units: `service_install` takes a name and a mode only; the unit text, `ExecStart` and the Windows service command
  line come from templates and detection (`internal/setup/targets/host/host.go:15-26`); the unit prefix
  `piperoom-` keeps service ops off other services (`internal/setup/targets/host/host.go:97`).
- `plugin_install` downloads only from the plugin-host allow-list and compares the SHA-512 with the digest the
  knowledge index pins, in constant time (`internal/setup/targets/compose/plugin.go:58`, `:276-306`, `:437-454`).
  The runner refuses to start when the shipped knowledge index fails its own check (`internal/setup/runner/runner.go:256-261`;
  `internal/setup/knowledge/knowledge.go:216`).
- Debezium Server's `application.properties` takes only keys the knowledge index lists for the sink, an allow-list
  of source keys, and refuses scripting SMTs (`internal/setup/targets/server/properties.go:411-470`,
  `:491-560`, `:716-727`).

**Tests that pin it.** `TestNoShellExecution`; `TestValidateRules`, `TestValidateImages`
(`internal/setup/targets/compose/validator_test.go:28`, `:124`); `TestStackWriteRefusesTamperedFile`,
`TestStackArgvHasNoVolumeRemoval` (`internal/setup/targets/compose/stack_test.go:210`, `:142`);
`TestPluginDigestMismatch`, `TestPluginHostNotAllowed` (`internal/setup/targets/compose/plugin_test.go:233`, `:216`);
`TestFileWriteRefusals`, `TestFileWriteDenyListAdditions` (`internal/setup/ops/files_test.go:202`;
`internal/setup/ops/security_pass_test.go:109`); `TestDigestCheckFailureRefusesToStart`
(`internal/setup/runner/check_test.go:23`); `TestPropertiesRefusesUnknownKey`, `TestPropertiesSecretRule`
(`internal/setup/targets/server/properties_test.go:193`, `:214`).

**Residual risks.** `file_write` is a deny-list by name. Files the person runs later (`package.json`, `pom.xml`,
`build.gradle`, `.vscode/tasks.json`, an XML log4j2 config outside a mount directory) are writable; the design
accepts this for the reference project, which the runner never runs (design §2.5 table). The `.env` values are not
re-validated at `compose_up`; only the variable names are (`validator.go:85-107`). Debezium Server's file writer
opens a second root after the path check and does not re-walk after the rename (Finding 14).

## S2. Pairing and replay

**Threat.** Someone other than the person attaches a runner to the room, or a recorded or altered request runs a
step twice or with other params.

**Mitigations as implemented.**

- Code: 8 characters of a 32-symbol alphabet, 10-minute TTL (`internal/setup/protocol/protocol.go:154-166`),
  bound to `(tenant, room, creator)` at issue (`internal/room/runners.go:184-201`). Rate limits: 5 attempts per code,
  20 per source per 10 minutes, 100 in all, after which pairing closes for 10 minutes
  (`internal/room/runners.go:27-29`, `:235-290`). Codes and tokens are compared in constant time by a linear scan
  (`internal/room/runners.go:292-303`, `:504-530`). Every attempt is audited as `setup.pairing`
  (`internal/room/runners.go:532-541`). The source address trusts `X-Forwarded-For` only from configured proxies
  (`internal/httpapi/auth_oidc.go:401-431`).
- The runner refuses `--yes` off loopback, refuses plain `http` off loopback, refuses redirects during the
  handshake and pins a loopback dial to loopback addresses (`internal/setup/runner/runner.go:263-266`,
  `:420-445`; `internal/setup/runner/loopback.go:31-98`). It prints the origin, room and creator, scrubbed of
  control and format characters, and waits for `y` (`internal/setup/runner/confirm.go:28-66`).
- Token: 32 random bytes, rotated on every reconnect, never logged (`internal/httpapi/runners.go:325-331`;
  `internal/room/runners.go:406-430`). The runner caps a server-sent expiry at its own clock plus 30 minutes
  (`internal/setup/runner/ops.go:125-137`).
- Replay: a T1+ request must carry proposal id, step index and params hash (`internal/setup/runner/ops.go:252-256`);
  the runner recomputes the RFC 8785 hash over the frozen proposal it cached and binds the request's op and params
  to the frozen step (`ops.go:285-297`, `:414-523`); a cached proposal cannot be swapped (`ops.go:431-443`). The
  duplicate ledger `(proposal_id, step_index)` is fsynced to `.piperoom/ops.jsonl` before the op runs and reloaded
  on restart; a write failure refuses the op (`ops.go:546-578`; `internal/setup/runner/oplog.go:63-131`). A resend of
  an op still held is dropped (`internal/setup/runner/runner.go:678-685`). On reconnect `Resume` reports running
  and done ops; the server never re-sends those and re-sends an unseen op at most once
  (`internal/room/runner_exec.go:65-80`).
- Frames are capped at 1 MiB both ways and unknown frame types are dropped unread
  (`internal/setup/runner/runner.go:393`, `:664-670`; `internal/httpapi/runners.go:44`, `:293-309`).

**Tests that pin it.** `TestPairingCodeShapeAndSingleUse`, `TestPairingCodeMaxAttempts`, `TestPairingSourceLimit`,
`TestPairingGlobalLimitClosesPairing` (`internal/room/runners_test.go:64`, `:107`, `:152`, `:176`);
`TestRunnerWSPairingAndAttach`, `TestRunnerWSTokenReconnect`, `TestRunnerWSUnknownTokenRefused`
(`internal/httpapi/runners_test.go:75`, `:256`, `:399`); `TestParamsHashRecomputed`, `TestDuplicateStepRefused`,
`TestStepBindingRefused`, `TestFrozenProposalNotSwapped`, `TestOpRefusedWhenStartRecordNotPersisted`,
`TestResentT0OpRunsOnce` (`internal/setup/runner/ops_test.go:75`, `:123`, `:375`, `:525`, `:486`, `:1130`);
`TestOpLogSurvivesRestart` (`internal/setup/runner/oplog_test.go:100`); `TestTokenRotatesOnReconnect`,
`TestTokenNeverLogged` (`internal/setup/runner/reconnect_test.go:351`, `:533`); `TestParamsHashMatchesRoom`
(`internal/setup/runner/params_hash_test.go:15`).

**Code versus design.** The token's expiry is set at issue and not moved by traffic, on both sides
(`internal/room/runners.go:372-384`, `:427`; `internal/setup/runner/ops.go:125-137`): it is an absolute 30 minutes
per connection, not an idle expiry. `Attach` replaces a runner that is still connected (design §2.1: only a lost
one) (`internal/room/runners.go:309-321`). No route issues a second pairing code (`internal/httpapi/server.go:255-270`).

**Residual risks.** The params hash proves only that the request matches the proposal the server sent; it does not
prove a person approved it. Against a malicious server the runner's protection is the pairing confirmation, the
catalogue and validators, the op log and (by design) its own class floors, which are not seeded today (Finding 2).
The code is reusable inside its welcome-to-hello window (Finding 5).

## S3. Tenant binding

**Threat.** A runner, a code or a registration crosses tenants, or the setup room's driver approves their own
writes on a team server.

**Mitigations as implemented.**

- `POST /api/setup` requires a tenant on a server with `tenants:` and the creator's membership, and refuses one
  without (`internal/room/setup.go:76-89`). Codes, runners and tokens are keyed by `(tenant, room, creator)`
  (`internal/room/runners.go:69-74`). A setup action refuses a call for another room than the one it is bound to
  (`internal/setup/actions/adapter.go:466-480`).
- Every setup route is driver-only and tenant-visible (`internal/room/setup.go:321-347`).
- Approver of a T2 or T3 step on a team server: an overlapping pipeline's owners, else a tenant admin (grant
  `admin`); the driver's fallback only in solo mode on a standalone server; a driver-origin proposal cannot be
  approved by its driver; `pipeline_register` cannot be approved by the room's creator outside solo mode
  (`internal/room/service_fallback.go:93-138`).
- `pipeline_register` refuses an overlap unless the overlapping pipeline's owners approved, and names another
  tenant's pipeline only as "a pipeline in another tenant" (`internal/room/setup_register.go:219-315`,
  `:323-368`).
- A credentials frame is accepted only from the room's current attachment, only for the pending request id, with
  exactly the requested names, and the pipeline id is the room's, never the frame's
  (`internal/room/setup_secrets.go:201-275`, `:298-327`).

**Tests that pin it.** `TestSetupCredentialsRefusals`, `TestSetupCredentialsRunnerSwapForgetsPending`
(`internal/room/setup_secrets_test.go:302`, `:629`); `internal/room/setup_register_test.go`;
`internal/room/setup_tiers_internal_test.go`.

**Residual risks.** The registered read credentials are whatever the driver stored under a connection-key name;
nothing ties them to a read-only role (Finding 6). A tenant admin who is the driver can approve the agent's T2
steps in their own room; the design allows it (§3.6).

## S4. Secrets

**Threat.** A secret value reaches the server, the model, a log, an event or a file op's output.

**Mitigations as implemented.**

- Store: `<project>/.piperoom/secrets.env`, file 0600 in a 0700 directory, a symlinked or non-regular store or
  directory refused, a custom path inside the project but outside `.piperoom/` refused, `.piperoom/` added to
  `.gitignore`, atomic writes with `O_EXCL` temporaries (`internal/setup/runner/secrets.go:125-263`, `:411-446`).
  Values shorter than 6 bytes are refused (`secrets.go:60`, `:565-573`).
- Literal redaction: before a result leaves the runner every stored value and the receipt key are replaced by
  `${secret:NAME}` in Summary, every string of Data (keys included), the receipt argv and target; Data that is not
  JSON is withheld (`secrets.go:748-908`). The catalogue also runs `redact.Text` and `redact.JSONStats` on every
  result (`internal/setup/ops/op.go:510-532`). `OutMAC` is HMAC-SHA256 under a per-runner key over the redacted
  bytes (`secrets.go:915-943`; `internal/setup/runner/ops.go:376-379`).
- No op reads the store: file ops refuse `.piperoom` and `.git` (and `.ssh`) by name and by identity, secret-named
  files (`.env*`, `.netrc`, `.pgpass`, key and keystore suffixes) and the persisted materialised-path list
  (`internal/setup/ops/paths.go:161-163`; `internal/setup/ops/files.go:487-557`).
- UI path: `POST /api/rooms/{id}/setup/secret` is driver-only, excluded from request logging, forwarded in one frame
  and stored nowhere; the event carries the name only (`internal/room/setup_secrets.go:96-133`;
  `internal/httpapi/server.go:399-406`). The runner prints the name only (`internal/setup/runner/secretops.go:174-191`).
- Credentials to the server: the runner prints the origin, pipeline and names and waits for `y`
  (`secretops.go:198-263`); the server writes each value through `os.Root` at `<dir>/<pipeline>/<name>`, 0600,
  temp then rename, refusing a symlink on the way (`internal/room/setup_secrets.go:386-498`).

**Tests that pin it.** `TestStorePermissionsAndGitignore`, `TestStoreRedactWrappedValue`, `TestStoreSanitizeResult`,
`TestStoreHMAC`, `TestStoreRefusesSymlinks` (`internal/setup/runner/secrets_test.go:53`, `:281`, `:316`, `:368`,
`:461`); `TestOpResultLiterallyRedactedAndMACed`, `TestRegisterCredentialsConfirms`, `TestRegisterCredentialsDeclined`
(`internal/setup/runner/secret_frames_test.go:92`, `:342`, `:414`); `TestFileOpsRefuseMaterialisedPaths`,
`TestFileReadRefusesSymlinkedSecretStore` (`internal/setup/ops/secret_paths_test.go:13`, `:99`);
`TestSetupSecretForwardsOnceAndKeepsNothing`, `TestSetupCredentialsSymlinkRefused`
(`internal/room/setup_secrets_test.go:94`, `:499`); `internal/httpapi/setup_secrets_test.go:126` (logging exclusion).

**Residual risks.** Materialisation is not wired (Finding 7), so the "materialised path" refusal protects nothing yet.
Literal redaction matches exact bytes only (Finding 12). A secret frame silently overwrites a stored value
(Finding 11). The standalone server keeps registered credentials in `<project>/.piperoom/secrets`
(`internal/setup/standalone/standalone.go:207-221`), which no file op can read.

## S5. Confinement

**Threat.** A file op leaves the project directory, writes into `.git` or `.piperoom`, or follows a link.

**Mitigations as implemented.**

- Every file op opens through an `os.Root` at the project directory (`internal/setup/ops/paths.go:55-65`).
- A lexical check on every OS refuses control and format characters, wildcards, the slash, colon and dot
  look-alikes, non-NFKC names, a leading separator, any `:`, `..`, a trailing dot or space, reserved device names
  with any extension, 8.3 aliases, and `.git`, `.piperoom` and `.ssh` by simple fold and by upper case
  (`paths.go:97-191`).
- A walk `Lstat`s every existing prefix and refuses a symlink or reparse point and any component that is the same
  file as `.git` or `.piperoom`; the opened handle is checked again (`paths.go:198-279`).
- `file_write` creates an `O_EXCL` temporary, renames it into place (so a planted hard link is replaced) and
  re-walks after the rename (`internal/setup/ops/files.go:356-399`). A non-regular destination is refused
  (`files.go:332-340`). `dir_list` skips links and non-regular entries (`files.go:220-235`).

**Tests that pin it.** `TestLexicalRefusal`, `TestWalkRefusesNestedAlias`, `TestConfinerRefusesSymlinkEscape`,
`TestConfinerRefusesRelativeSymlink`, `TestVerifyHandleRefusesDeniedIdentity`, `TestPiperoomResolvedAfterStart`
(`internal/setup/ops/paths_test.go:13`, `:125`, `:196`, `:217`, `:241`, `:270`); `TestConfinerRefusesJunction`
(`internal/setup/ops/paths_windows_test.go:18`); `TestFileWriteReplacesHardLink`,
`TestFileWriteSymlinkDestinationRefused` (`internal/setup/ops/files_test.go:270`, `:308`).

**Residual risks.** The post-rename walk narrows a race with a concurrent local process, it does not close it
(`files.go:389-394`). The op log is opened with plain `os.MkdirAll` and `os.OpenFile`, outside this rule
(Finding 13). `file_read` follows a hard link that someone with write access to the project planted; that person
already holds the project.

## S6. The ladder

**Threat.** The model, or anything it controls, claims a pipeline is verified, and `pipeline_register` writes an
owners entry for a pipeline that does not work.

**Mitigations as implemented.**

- The model only requests verification; the ladder runner in the room service draws a 16-byte nonce, inserts the
  canary through an ordinary system-origin proposal, issues each rung's read itself and judges the predicate on the
  structured Data, never the summary (`internal/room/setup_ladder.go:23-42`, `:364-405`, `:467-545`;
  `internal/setup/plan/ladder.go:55-59`). `setup.verified` carries the nonce's hash only
  (`setup_ladder.go:841-850`).
- `setup.verified` and `setup.awaiting_change` are refused by the agent's sink and by the generic setup emitter
  (`internal/room/service.go:1241`; `internal/room/service_runners.go:25-28`).
- `pipeline_register` needs a green `setup.verified` for the current plan with no executed step after it, at create
  and again before execute (`internal/room/setup_register.go:176-199`).

**Tests that pin it.** `TestAgentSinkSetupTypes` (`internal/room/agentsink_internal_test.go:47`);
`internal/room/setup_ladder_internal_test.go`; `internal/room/setup_register_internal_test.go`;
`internal/setup/plan/ladder_test.go`.

**Residual risks.** Every rung reads through the room's tools over the paired runner; the server's own clients are
not consulted (`setup_ladder.go:600-689`), so a replaced runner (Finding 5) can answer green. The ladder always runs
with no sink (`setup_ladder.go:238-240`), and refuses `dbzserver`, `embedded` and the managed targets
(`internal/setup/plan/ladder.go:31-40`, `:79-85`). No server action named `source_insert_canary` is registered, so
rung 5 cannot pass today (Finding 4).

## S7. Target reach

**Threat.** An op reaches a host the person did not name, cloud metadata, or the server itself, through a model
param or DNS.

**Mitigations as implemented.**

- Targets grow only by a driver click (`internal/room/setup.go:353-398`). At registration the server refuses user
  info in a URL, link-local, metadata names and addresses, multicast, unspecified, loopback on a team server, and
  the server's own addresses; a new target is never below `shared`
  (`internal/room/setup_targets.go:161-247`, `:302-335`; `internal/connprobe/guard.go:42-57`).
- On the runner every dial goes through one `net.Dialer` whose `ControlContext` checks the address being dialed,
  refusing link-local, metadata, multicast and unspecified; loopback and private ranges are allowed
  (`internal/setup/ops/dial.go:20-56`). The HTTP client has no proxy and refuses redirects (`dial.go:59-78`).
- `http_get` matches host and port to a target entry (`internal/setup/ops/http_get.go:212-235`); `sink_probe`
  needs an exact registered `host` target and rebuilds the URL from it (`internal/setup/ops/sink_probe.go:166-183`,
  `:240-275`). The runner refuses a request outside the targets before the guard (`internal/setup/ops/op.go:428-447`;
  `internal/setup/runner/ops.go:258-266`).

**Tests that pin it.** `TestDialGuardRefusesMetadataFromResolver`, `TestNetProbeMetadataFromResolver`
(`internal/setup/ops/net_probe_test.go:84`, `:212`); `TestHTTPGetRefusesHostOutsideTargets`,
`TestHTTPGetRefusesRedirect` (`internal/setup/ops/http_get_test.go:80`, `:136`); `TestSinkProbeRefusesMetadataAddress`,
`TestSinkProbeRejectsUnregisteredHost` (`internal/setup/ops/sink_probe_test.go:282`, `:258`).

**Residual risks.** A name the server cannot resolve is accepted at registration (`setup_targets.go:224-233`); on
the runner it may resolve to a loopback or private address, which the dial guard allows. The runner's target list is
empty in the shipped binary (Finding 2), so today every reaching op is refused.

## Findings

Severity as in `docs/security/findings.md`. Availability findings that fail closed are marked so. None is fixed by
this change.

### 1. No setup write can run: the frozen proposal never reaches the runner
- **Severity:** high (availability; fails closed). **Where:** `internal/setup/actions/adapter.go:420-430`,
  `internal/room/service_executor.go:445-449`.
- The runner caches the frozen proposal only from `OpRequest.Proposal` and refuses `params_hash` without it
  (`internal/setup/runner/ops.go:414-429`). The executor adds `WithSetupStep` but not `remoteclients.WithProposal`.
- **Fix row:** U1-fix-1: the executor puts the frozen `proposal.created` payload into every setup step's context;
  a test drives a T1 step through the real runner and catalogue.

### 2. The runner's own class floors and target list are never filled
- **Severity:** high (a design control is absent). **Where:** `internal/httpapi/runners.go:97-105`, `:210-218`;
  `internal/setup/setupcmd/setupcmd.go:105`; `internal/setup/ops/op.go:234`.
- Welcome carries no targets, `raiseFloor` has no caller (`internal/setup/runner/ops.go:100`), and the catalogue's
  `Env.Targets` cannot change after `ops.New`. So the second lock of design §7 against a server that lowers
  `Class` does not exist, the host driver is never chosen, and `http_get`, `net_probe` and `sink_probe` refuse every
  host.
- **Fix row:** U1-fix-2: fill Welcome targets from the room snapshot on pair and reconnect; send target and class
  changes to the runner; add a target setter on `ops.Registry` fed by `RegisterTargets`; test that a production
  floor survives a server sending `shared`.

### 3. The runner catalogue lacks the remote-client ops
- **Severity:** high (availability; fails closed). **Where:** `internal/setup/ops/op.go:237-241`;
  `internal/setup/remoteclients/doc.go:99-106`; `internal/setup/ops/sqlserver_ops.go:85`.
- Every `connect_*`, `source_*`, `kafka_*` and `registry_*` op the server sends is `unknown_op` on a real runner;
  the runner's guard table covers only `file_write` and `file_delete` (`op.go:409-412`), and
  `TestGuardTableAgreesWithPlan` checks only ops in its test registry (`internal/setup/ops/guard_test.go:134-152`).
- **Fix row:** U1-fix-3: register the client ops in `cmd/setup` behind runner-held connections; add every guard row
  of `plan` for them to the runner's table; extend the agreement test to the shipped catalogue.

### 4. The setup-only source writes have no server action
- **Severity:** high (availability; fails closed). **Where:** `internal/room/setup_ladder.go:364-371`;
  `internal/setup/actions/specs.go:27-187`; `internal/setup/remoteclients/doc.go:96-101`.
- No action is registered for `source_insert_canary`, `source_create_canary_table`, `source_create_publication` or
  `source_create_read_role`, so the canary never runs and registration can never be gated green.
- **Fix row:** U1-fix-4: register the four setup-only source actions over `SetupSourceWriter` with their scope,
  tier and preview.

### 5. A pairing code works twice inside its welcome-to-hello window
- **Severity:** medium (needs a leaked code). **Where:** `internal/room/runners.go:281-286`, `:309-321`;
  `internal/httpapi/runners.go:79-158`; `internal/httpapi/setup.go:73-78`.
- `PairCode` keeps the code until `Attach`, and `Attach` neither checks that the code is still outstanding nor
  refuses to replace a connected runner. Two sockets presenting one code both get Welcome; the last to send Hello
  owns the room (audited `replaced`). That runner then receives the UI's secret frames, answers the ladder's reads
  and can answer a credentials request. The pasted command carries the code in argv, visible in process lists and
  shell history.
- **Fix row:** U1-fix-5: reserve the code at the first successful `PairCode` for that socket only; `Attach` requires
  the reservation; refuse replacing a connected runner; make the command read the code from
  `PIPEROOM_SETUP_CODE` or a prompt.

### 6. Registered "read" credentials are whatever the driver stored
- **Severity:** medium. **Where:** `internal/room/setup_secrets.go:141-192`; `internal/room/setup_snapshot.go:521-527`;
  `internal/setup/runner/secretops.go:198-263`; `internal/room/setup_register.go:759-788`.
- The request names every `setup.secret_set` name, and the runner sends any stored name on `y`. Nothing creates a
  read-only role on a real runner (Finding 3), so the server receives the setup's own credentials, usually an admin
  login, and the watcher uses them. The design's runner-generated read role (§3.7) is not what runs.
- **Fix row:** U1-fix-6: the runner marks names `source_create_read_role` generated and refuses to send any other;
  the server asks only for names a read-role receipt returned.

### 7. Secrets are never materialised
- **Severity:** medium (functional; makes S4's materialised-path rule untested in practice). **Where:**
  `internal/setup/runner/secrets.go:579-658`; `internal/setup/targets/compose/templates.go:300-305`;
  `internal/setup/targets/server/properties.go:926-929`.
- No caller resolves `${secret:name}` into the `.env`, a properties file or a child environment, and no path is ever
  registered as materialised. The Debezium Server integration test writes `.env` by hand for this reason
  (`internal/setup/targets/server/integration_test.go:93-97`).
- **Fix row:** U1-fix-7 (U1-4): materialise on the runner only, register each path, and test that no frame carries
  the value afterwards.

### 8. A runner cannot come back after 30 minutes, and no second code exists
- **Severity:** medium (availability). **Where:** `internal/room/runners.go:372-384`, `:406-430`;
  `internal/setup/runner/ops.go:125-137`; `internal/httpapi/server.go:255-270`.
- The token expiry is fixed at issue; `Touch` does not move it. After any drop past 30 minutes, or a code that
  expired unused, the room needs a new code, and no route issues one; a second room for the pipeline is refused while
  the first is open (`internal/room/setup.go:98-104`).
- **Fix row:** U1-fix-8: move the expiry on every frame from the attached runner; add a driver-only, audited
  re-pair route that works only while no runner is connected.

### 9. The `local` class is never derived
- **Severity:** low (fails safe). **Where:** `internal/room/setup_targets.go:313`; `internal/setup/plan/class.go:114`;
  `internal/room/setup_snapshot.go:170-183`.
- Every step is at least T2 on every server, so the `setup_local` countdown never runs and a registration is never
  `watched_by: local` (`internal/room/setup_register.go:730-740`).
- **Fix row:** U1-fix-9: wire `plan.Classify` from `Hello` and `env_detect` facts and this room's receipts, emitted as
  `setup.target_classed` by the server.

### 10. The runner's terminal witness does not show what a step does
- **Severity:** low. **Where:** `internal/setup/runner/ops.go:650-661`; `internal/setup/runner/runner.go:515`.
- A T1+ op prints op, target, proposal id and step index; not the title or params the design says the runner prints.
  `Hello.Tools` is always empty.
- **Fix row:** U1-fix-10: print the frozen proposal's title and the step's canonical params (scrubbed, capped)
  before the op runs.

### 11. A secret frame replaces a stored value without asking
- **Severity:** low. **Where:** `internal/setup/runner/secretops.go:174-191`; `internal/setup/runner/secrets.go:492-512`.
- `FrameSecret` upserts any name; a server can replace a stored credential silently.
- **Fix row:** U1-fix-11: refuse a frame for a name already stored unless the person confirms on the terminal.

### 12. Literal redaction misses encoded forms
- **Severity:** low. **Where:** `internal/setup/runner/secrets.go:789-822`.
- A stored value that appears URL-encoded, base64-encoded or JSON-escaped inside Summary text passes unless
  `redact.Text`'s patterns catch it.
- **Fix row:** U1-fix-12: also replace the common encodings of each stored value; table test per encoding.

### 13. The op log is opened outside the confinement rule
- **Severity:** low. **Where:** `internal/setup/runner/oplog.go:63-75`; `internal/setup/runner/ops.go:598-645`.
- A refusal can open `.piperoom/ops.jsonl` before the store's symlink check runs; a symlinked `.piperoom` (or a
  store outside the project) sends the witness log wherever the link points.
- **Fix row:** U1-fix-13: open the op log through the same symlink-refusing path as the store, before any op.

### 14. The Debezium Server writer checks then reopens
- **Severity:** low (driver not wired). **Where:** `internal/setup/targets/server/files.go:29-97`;
  `internal/setup/targets/server/status.go:68-105`.
- `writeFile` checks with the Confiner, then opens a second `os.Root` and does not re-walk after the rename;
  `server_status` validates the compose file and then hands docker the on-disk path.
- **Fix row:** U1-fix-14: write through the ops package's helper and read through the Compose driver's snapshot.

### 15. Standalone registration has no owners directory
- **Severity:** low (availability). **Where:** `internal/room/setup_register.go:413-418`;
  `internal/setup/standalone/standalone.go:166-230`.
- Without `repos:`, `pipeline_register` needs `OWNERS_DIR`; standalone does not default it, though the error text
  says it writes next to the project.
- **Fix row:** U1-fix-15: standalone sets `OWNERS_DIR` under the project and `--watch` polls it.

### 16. The ladder trusts the paired runner alone
- **Severity:** low. **Where:** `internal/room/setup_ladder.go:600-689`, `:238-240`.
- Design §3.4 reads through the server's own clients when it has them; the code reads only the room's tools over
  the runner, and never checks a sink.
- **Fix row:** U1-fix-16: on a team server, read rungs 1 to 6 through the server's clients for shared and
  production targets; pass the sink kind when one was set up.
