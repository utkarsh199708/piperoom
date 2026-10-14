# Vulnerability report 2026-10: govulncheck and npm audit

Agent C3 (cloud), 2026-10-13. Branch `c3/vuln-report`. Scanned at `1b31e7a` (base `2fb0055`). At review,
`main` was `09473b2`; it is now `f902c5a` after #87 and #90. Those merges do not change the Go call paths or web
lockfile discussed here. The reviewer's rescan at `09473b2` gave identical govulncheck results.

## Summary

- **Go, called findings: 18 before, 4 after on Go 1.27.2 (17 after on Go 1.27.0).** Module-level (not called): 1, unchanged.
- **Fixed in this PR:** GO-2026-5764 (aws-sdk-go-v2 `service/s3` and `eventstream`), by upgrading s3 to v1.97.3.
- **Cleared by the Go 1.27.2 toolchain:** 13 standard library findings. The fix is binding only once `go 1.27.2` is in
  `go.mod` (setup-go reads only that line) and `FROM golang:1.27.2-alpine` is at Dockerfile:21. Both are recommended
  here and not applied in this PR (see "Go toolchain").
- **Still open, no fix in the current majors:**
  - GO-2026-6665 (gosnowflake v1, called). The fix is in gosnowflake **v2.2.0**, a major version. Mitigation in the
    section below.
  - GO-2026-5046, 5047, 5048 (hamba/avro v2, called). hamba/avro is archived with no fix. The maintained drop-in fork
    `github.com/iskorotkov/avro/v2` v2.33.0 fixes all three (5048 only with `Config.MaxMapAllocSize` set).
  - **Correction from the previous revision:** the avro findings are reachable from production, not only from
    `cmd/devctl`. The Kafka client decodes untrusted record keys (see the table and "Priority" below).
- **npm, production dependencies (`--omit=dev`): 0 vulnerabilities.** On this PR's lockfile, full `npm audit`
  reports 3 dev-tooling advisories. On current `main`, the full audit reports 10 advisories (3 moderate, 5 high,
  2 critical), all dev-only; the added UI dependencies include `braces` (high) and
  `postcss-selector-parser` (moderate). Both are pulled in through Tailwind CSS. The npm fix for those two requires
  Tailwind CSS 4.3.3, a breaking major upgrade, so there is no safe patch in the current major. The web lockfile is
  unchanged in this PR.
- **Upgrades in this PR:** `go.mod`/`go.sum` only, aws-sdk-go-v2 and smithy-go minor/patch bumps (listed below).
  No major upgrades.

## Scan method

`https://vuln.go.dev` is blocked by the session's egress proxy (CONNECT answered 403 by policy). The scan used the
Go vulnerability DB at `https://storage.googleapis.com/go-vulndb` through `-db`, which serves the same records. The
proxy was not bypassed. Advisory text was read from `https://storage.googleapis.com/go-vulndb/ID/<GO-ID>.json`.

Symbol-level call paths from govulncheck over-approximate. Several listed paths go through generic symbols such as
`bufio.Writer.Flush`, `json.Decoder.Decode`, `fmt.Fprintln` or `chi.Mux.ServeHTTP` (for example
`internal/httpapi/server.go:203`, `evals/cmd/evalrun/main.go:69`, `internal/room/service_export.go:233`). A path
through one of those is noise on its own. The findings below cite only paths that call the vulnerable code directly
(or that the reviewer confirmed).

## Tool versions

| Tool | Version |
|---|---|
| Go toolchain before (`go.mod` `go 1.27.0`) | go1.27.0 linux/amd64 |
| Go toolchain after (verified, not yet pinned) | go1.27.2 linux/amd64 |
| govulncheck | v1.9.0, built with `GOTOOLCHAIN=go1.27.0` |
| Node / npm (original report scan) | v22.22.0 / 10.9.4 |
| Node (reviewer re-run on current main) | v24.19.0 (npm version not recorded) |

`go install golang.org/x/vuln/cmd/govulncheck@latest` resolves to v1.9.0, which built with go1.26 and fails to load
go1.27 packages. Building it with `GOTOOLCHAIN=go1.27.0` fixed that.

## Commands and results

```
# Before (go1.27.0, original go.mod)
govulncheck ./...                                           -> vuln.go.dev Forbidden (default DB blocked)
govulncheck -db https://storage.googleapis.com/go-vulndb ./...
     -> 18 called (13 stdlib, gosnowflake, aws s3/eventstream, 3 avro); 1 module-level

# After the s3 upgrade (go1.27.0)
govulncheck -db https://storage.googleapis.com/go-vulndb ./...   -> 17 called (13 stdlib, gosnowflake, 3 avro)

# After the s3 upgrade, Go 1.27.2 (GOTOOLCHAIN=go1.27.2)
govulncheck -db https://storage.googleapis.com/go-vulndb ./...                   -> 4 called
govulncheck -show verbose -db https://storage.googleapis.com/go-vulndb ./...     -> 4 called + 1 module-level
govulncheck -tags integration -db https://storage.googleapis.com/go-vulndb ./... -> 4 called

npm audit --omit=dev (web)   -> found 0 vulnerabilities
npm audit (web, full)        -> 3 vulnerabilities (1 moderate, 2 critical), dev-only (see below)
```

Build and test checks after the upgrade:

```
gofmt -l . (excluding web/node_modules)  -> no output
go mod tidy                              -> rc=0
go build ./... / go vet ./...            -> rc=0 on go1.27.0 and go1.27.2
go test -count=1 ./...                   -> rc=0, 39 packages ok on go1.27.0 and go1.27.2
```

## Findings table (after the upgrade; go1.27.2 where it applies)

Reachable = govulncheck found a call path from our code to the vulnerable symbol.

| ID | Package | Version | Fixed in | Reachable | Example call path (govulncheck) | Advice |
|---|---|---|---|---|---|---|
| GO-2026-6665 | github.com/snowflakedb/gosnowflake | v1.19.1 (latest v1) | **v2.2.0** (major; none in v1) | **yes** via `internal/clients/snowflake/snowflake.go:39:21` `snowflake.New` -> `sql.Open` -> `Connector.Connect` | see "gosnowflake" below | Migrate to gosnowflake/v2 (track separately). Until then apply the mitigation below. |
| GO-2026-5046 | github.com/hamba/avro/v2 | v2.31.0 (latest hamba release) | none in hamba; fork `github.com/iskorotkov/avro/v2` **v2.33.0** | **yes, production**: `internal/clients/kafka/kafka.go:436:22` `Client.decodeKey` calls `avro.Decoder.Decode` | `decodeKey` is reached from `FindByKey` (kafka.go:347), which is called by the read-only tool `internal/tools/kafkatools/kafka_tools.go:254` and by `internal/topology/trace.go:126`. Both link into `cmd/server` and `cmd/investigate`. | Switch to the fork and decode with a bounded config (see "avro"). |
| GO-2026-5047 | github.com/hamba/avro/v2 | v2.31.0 | fork v2.33.0 | **yes, production**: `kafka.go:434:33` `decodeKey` calls `avro.NewDecoderForSchema`; `internal/actions/kafka_actions.go:564:25` `registerSchema.Validate` calls `avro.Parse` (schema parsing only, a package-level call; the advisories are decoder-only, so this path is low risk) | same as above | same as GO-2026-5046 |
| GO-2026-5048 | github.com/hamba/avro/v2 | v2.31.0 | fork v2.33.0 (needs `Config.MaxMapAllocSize`) | **yes, production** (same `decodeKey` path) | same as above | same as GO-2026-5046, plus set `MaxMapAllocSize` |
| 13 stdlib IDs (below) | Go standard library | go1.27.0 | **go1.27.2** | yes on go1.27.0, **cleared** on go1.27.2 | e.g. `cmd/server/main.go:364:35` `server.run` -> `http.Server.ListenAndServe` (GO-2026-6617) | Binding only with `go 1.27.2` in `go.mod` and `FROM golang:1.27.2-alpine` at Dockerfile:21 (see "Go toolchain"). Neither is applied in this PR. |
| GO-2026-5932 | golang.org/x/crypto | v0.58.0 (required) | none | **no** (module-level only) | `openpgp` package not called | No action. |

Standard library IDs (all called on go1.27.0, all cleared by go1.27.2): GO-2026-6617, 6613, 6612, 6611, 6610, 6609,
6608, 6607, 6605, 6604 (Windows only), 6603, 6600, 6599.

GO-2026-5764 (aws-sdk-go-v2 `service/s3` v1.53.1 and `eventstream` v1.6.3, panic in the EventStream decoder) is
**fixed** by this PR. It is absent from the "after" results.

### avro: why it matters (priority)

The Kafka client decodes record keys from topics that pipeline producers write, and the key carries the schema id
in its first bytes. `decodeKey` (kafka.go:434-436) parses a schema from the registry, then calls `Decoder.Decode`
on the key bytes. The `Decode` call takes no context, so the 15 s deadline on `FindByKey` does not stop it. The
read-tool goroutine in `internal/agent/run.go` starts at line 631 and recovers a panic at line 634. `Run` waits
for the tool goroutines at line 657, then re-panics with the first captured panic. In `cmd/server`,
`internal/room/service.go:904-912` (`runGuarded`) catches that panic and turns it into a failed room run; it does not
bring down the server. `cmd/investigate` has no such outer guard.

Per the advisories:
- GO-2026-5046: a block count can be attacker-chosen up to MaxInt64 followed by EOF, so one payload can spin a core
  indefinitely. `Decode` has no context cancellation; the room run remains blocked waiting at `run.go:657`.
- GO-2026-5047: three sub-issues panic or bypass caps on any platform (cumulative size overflow, MinInt negation,
  negative block size). The panic is recovered in the tool goroutine and re-panicked by `Run` after its sibling calls
  finish. The server's `runGuarded` turns it into a failed room run; `cmd/investigate` still sees the panic.
- GO-2026-5048: unbounded map growth, OOM.

Exploitability (reviewer analysis): anyone who can produce to a pipeline topic controls the key bytes, including the
schema id. Rate production exposure medium to high.

Recommended follow-ups (not in this PR; owners to assign):
- **A2 (owns `internal/clients/kafka/**`):** import `github.com/iskorotkov/avro/v2` at v2.33.0 or later and decode
  with a frozen bounded config:

  ```go
  api := avro.Config{MaxByteSliceSize: 64 << 10, MaxSliceAllocSize: 1 << 20, MaxMapAllocSize: 1 << 16}.Freeze()
  dec := api.NewDecoder(schema, bytes.NewReader(key[5:]))
  ```

  `API` (config.go:124) has `NewDecoder`, not `NewDecoderForSchema`. Do not use the package-level
  `NewDecoderForSchema` or `Unmarshal` for untrusted input: they use `DefaultConfig` (decoder.go:24-25), which sets no
  limits, so GO-2026-5048 stays open (`MaxMapAllocSize` 0 means unbounded). Refuse keys larger than 64 KiB, refuse key
  schemas containing array or map types, and add a `defer recover()` in `decodeKey`.
- The fork's config fields (`MaxByteSliceSize`, `MaxSliceAllocSize`, `MaxMapAllocSize`) and `Config.Freeze` were
  checked in a scratch module against v2.33.0. Switching the import path also needs the `go.mod` change, which is
  not in this PR.

### gosnowflake (GO-2026-6665)

The advisory's fix is gosnowflake **v2.2.0**; v1.19.1 is the latest v1 and has no fix. What v1.19.1 does, checked in
its module source:
- Logs whole HTTP response headers (session tokens, storage credentials) at Info: `restful.go:305,359,426,515`,
  `auth.go:320`, `authokta.go:302,343`. At Debug: `heartbeat.go:125`, `chunk_downloader.go:377,401` (the latter also
  logs response bodies).
- The driver's default level is ERROR: `driver.go:123` calls `logger.SetLogLevel("error")` in `init`. So nothing
  leaks by default.
- Level changes come from two places only: `Tracing` (DSN `tracing=`), applied at `driver.go:60-61`, and easy
  logging. Easy logging runs on each new connection (`connection.go:890`) and reads a client config file found via
  `SF_CLIENT_CONFIG_FILE`, the DSN `clientConfigFile` parameter, or `sf_client_config.json` in the executable's
  directory or the home directory (`client_configuration.go:24-25, 82-96`).

Our code sets no gosnowflake logger, and no `SetLogger`/`SetLogLevel` call exists in the repo. Exposure therefore
depends only on the DSN and on the config-file search above. The reachable paths are real (`snowflake.New` ->
`sql.Open`), but leaking needs a level change, so reachable is not the same as exploitable here.

Mitigation (reviewer's recommendation, verified against the API):
1. In `internal/clients/snowflake/snowflake.go` `New`, parse the DSN with `gosnowflake.ParseDSN`, refuse a non-empty
   `Tracing` or `ClientConfigFile`, then open through `sql.OpenDB(gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, *cfg))`.
   Verified: `ParseDSN`, `NewConnector(driver InternalSnowflakeDriver, config Config)`, `SnowflakeDriver` exist; the
   `Config` fields `Tracing` and `ClientConfigFile` exist (`dsn.go:135,154`).
2. Call `gosnowflake.GetLogger().SetLogLevel("error")` in `New`. `GetLogger` and the `SFLogger.SetLogLevel(string)
   error` method exist.
3. Caveat: step 2 alone is not enough. Easy logging runs on the first connection and can apply a level from a config
   file found in the search paths above, which would override the level set in `New`. Deployments must keep
   `SF_CLIENT_CONFIG_FILE` unset and must not place `sf_client_config.json` in the executable's or home directory.
   Document this in the snowflake client's package comment. (Not checked here: the contents of the deployed image.)
4. Track the gosnowflake v2 migration separately.

## Upgrades applied

All minor or patch bumps within their major, pulled by `go get github.com/aws/aws-sdk-go-v2/service/s3@v1.97.3` and
`go mod tidy`:

| Module | Old | New | Why |
|---|---|---|---|
| github.com/aws/aws-sdk-go-v2/service/s3 | v1.53.1 | v1.97.3 | fixes GO-2026-5764 (EventStream decoder panic DoS) |
| github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream | v1.6.3 | v1.7.8 | fixes GO-2026-5764; required by s3 v1.97.3 |
| github.com/aws/aws-sdk-go-v2 | v1.38.1 | v1.41.5 | required by s3 v1.97.3 |
| github.com/aws/aws-sdk-go-v2/internal/configsources | v1.3.15 | v1.4.21 | required by s3 v1.97.3 |
| github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 | v2.6.15 | v2.7.21 | required by s3 v1.97.3 |
| github.com/aws/aws-sdk-go-v2/internal/v4a | v1.3.5 | v1.4.22 | required by s3 v1.97.3 |
| github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding | v1.11.3 | v1.13.7 | required by s3 v1.97.3 |
| github.com/aws/aws-sdk-go-v2/service/internal/checksum | v1.3.7 | v1.9.13 | required by s3 v1.97.3 |
| github.com/aws/aws-sdk-go-v2/service/internal/presigned-url | v1.11.17 | v1.13.21 | required by s3 v1.97.3 |
| github.com/aws/aws-sdk-go-v2/service/internal/s3shared | v1.17.5 | v1.19.21 | required by s3 v1.97.3 |
| github.com/aws/smithy-go | v1.22.5 | v1.24.2 | required by s3 v1.97.3 |

Not upgraded (no fixed version in the same major): hamba/avro/v2 (latest v2.31.0; the fork at v2.33.0 is a different
module path, a follow-up for A2), gosnowflake v1.x (latest v1.19.1).

### Mixed AWS SDK versions (follow-up, not in this PR)

After the bump, the AWS modules are not all on the same release train. Still at older versions in `go.mod`:
`github.com/aws/aws-sdk-go-v2/config` v1.27.27, `credentials` v1.17.27, `service/sts` v1.30.3,
`service/sso` v1.22.4, `service/ssooidc` v1.26.4, `feature/ec2/imds` v1.16.11. The core module is a direct
requirement (`go.mod:7`) at v1.41.5. These credential-chain modules feed the Bedrock and MSK IAM credentials. The
build and unit tests pass, but there are no integration tests for those signing paths; a coordinated bump of all of
them is the recommended follow-up.

## Go toolchain

`go.mod` says `go 1.27.0`. The Dockerfile uses `FROM golang:1.27-alpine AS server` (line 21), a floating tag. CI
(`.github/workflows/ci.yml`) uses `actions/setup-go` with `go-version-file: go.mod`.

Verified: build, vet, unit tests and govulncheck all pass with `GOTOOLCHAIN=go1.27.2`.

CI uses `actions/setup-go` v5.6.0 (SHA `40f1582b`). Its `parseGoVersionFile` (`src/installer.ts:491-503`) matches only
`^go (\d+(\.\d+)*)` in `go.mod` and ignores any `toolchain` line, so the `go` directive is the binding setting for CI.
These lines are recommended and **not applied in this commit**:

1. `go.mod` line 3: `go 1.27.0` -> `go 1.27.2`. This is a `go.mod` change; CI and local builds then use 1.27.2.
   Re-run build, vet and tests after the change.
2. `Dockerfile` line 21: `FROM golang:1.27-alpine AS server` -> `FROM golang:1.27.2-alpine AS server`, ideally pinned by
   digest. Owner: the Dockerfile owner (lead to assign).
3. CI: no workflow edit needed once `go.mod` says 1.27.2. Owner of `.github/**`: C1.

## npm (web)

- Production dependencies (`npm audit --omit=dev`): 0 vulnerabilities.
- Full audit of this PR's lockfile (`npm audit`, dev included): 3 advisories, all dev-only, through `vitest`:
  - `tinypool` 1.1.1 (installed; audit range `<= 2.1.1`): critical, prototype pollution gadgets leading to RCE,
    GHSA-5gmw-xhrv-c9v3 and GHSA-85c8-ppgw-ccpr.
  - `@vitest/mocker` 3.2.4 (audit range `2.1.0 - 4.1.10`): moderate, path traversal via redirect mock,
    GHSA-82fw-gwwq-j7x9.
  - Root: `vitest` pinned at `3.2.4` in `web/package.json` (line 34).
- Current `main` also reports `braces` (high) and `postcss-selector-parser` (moderate), both pulled in through
  Tailwind CSS. The current-major Tailwind line has no safe fix; npm's suggested `tailwindcss@4.3.3` is a breaking
  major upgrade. These are development dependencies; `npm audit --omit=dev` remains at zero.
- Fix: `vitest` 3.2.7 (npm's suggested target). This needs a `web/package.json` and lockfile change, which is owned by
  A6. Not applied here.
- Node: the original report used v22.22.0, but the reviewer re-ran the audit on Node v24.19.0. The Dockerfile uses
  `node:24-alpine`; the Node 24 re-run is complete. The npm version for that re-run was not recorded.

## Flaky tests observed

- `internal/httpapi` `TestLogoutClosesWebSocket` failed once in a full run after the upgrade (go1.27.0). It passed
  5 of 5 isolated runs and 3 of 3 package runs.
- `internal/ws` `TestPresenceTickClosesDeadSessions` failed once in a full run on go1.27.2. It passed 3 of 3 package
  runs on both toolchains.
- Both are timing tests with 5 s and 10 s deadlines. The final full suite passed on both go1.27.0 and go1.27.2
  (39 packages ok each).

## Not done

- The gosnowflake mitigation in the code (steps 1-2 above) is not applied in this PR. `internal/clients/snowflake/**`
  is owned by C3 (AGENTS.md §2), so it is a follow-up for C3, not A2.
- The avro switch to the fork, bounded decode config, and `recover()` in `decodeKey` are follow-ups for A2. The
  read-tool goroutine already recovers panics on current `main`; `cmd/server` also contains the `runGuarded` panic
  boundary described above.
- The `go` directive and Dockerfile pin are recommended, not applied.
- The vitest bump to 3.2.7 is for A6.
- `npm audit` on the current main lockfile still reports dev-only findings; upgrading Tailwind to v4 is a separate
  breaking-change decision for A6.

## New dependencies

None added. Changed versions: the aws-sdk-go-v2 set and smithy-go above. No new modules.
