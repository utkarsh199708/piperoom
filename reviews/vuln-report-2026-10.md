# Vulnerability report 2026-10: govulncheck and npm audit

Agent C3 (cloud), 2026-10-13, against `main` @ 2fb0055. Branch `c3/vuln-report`.

## Summary

- **Go: 18 called findings before, 4 after the upgrade on Go 1.27.2 (17 on Go 1.27.0).** Module-level (not called):
  1 before, 1 after.
  - Fixed here: GO-2026-5764 (aws-sdk-go-v2 s3 / eventstream), by upgrading `service/s3` to v1.97.3.
  - Cleared by Go 1.27.2: 13 standard library findings. Upgrading the Go toolchain is the fix, not a `go.mod` change.
  - Still open, no fixed version exists: GO-2026-6665 (gosnowflake, called) and GO-2026-5046/5047/5048 (hamba/avro,
    called). Both modules are already at their latest release.
- **npm (web, production dependencies): 0 vulnerabilities** (`npm audit --omit=dev`). Lockfile not changed.
- **Changes in this PR:** `go.mod` and `go.sum` only (aws-sdk-go-v2 minor/patch bumps, listed below). No major
  upgrades. No `web/package.json` or `web/package-lock.json` change.

## How the scan ran, and why the mirror

`https://vuln.go.dev` is blocked by the session's egress proxy (CONNECT answered 403 by policy), so the default
govulncheck run fails before scanning. The scan used the Go vulnerability DB from
`https://storage.googleapis.com/go-vulndb` via `-db`, the same DB origin. The proxy was not bypassed; the mirror is a
different host, and the proxy allowed it.

## Tool versions

| Tool | Version |
|---|---|
| Go toolchain before (`go.mod` `go 1.27.0`) | go1.27.0 linux/amd64 |
| Go toolchain after (verified, not pinned) | go1.27.2 linux/amd64 |
| govulncheck | v1.9.0, built with `GOTOOLCHAIN=go1.27.0` |
| Node / npm | v22.22.0 / 10.9.4 |

govulncheck built with `go install @latest` resolves to v1.9.0, which built with go1.26 and fails to load go1.27
packages. Building it with `GOTOOLCHAIN=go1.27.0` fixed that.

## Commands and results

```
# Before (go1.27.0, unmodified go.mod)
govulncheck ./...                        -> "vuln.go.dev ... Forbidden" (default DB blocked)
govulncheck -db https://storage.googleapis.com/go-vulndb ./...
     -> "Your code is affected by 18 vulnerabilities from 3 modules and the Go standard library"
        "1 vulnerability in modules you require, but your code doesn't appear to call these"

# After upgrade (go1.27.0)
govulncheck -db https://storage.googleapis.com/go-vulndb ./...
     -> 17 called (13 stdlib, gosnowflake, avro x3)

# After upgrade, Go 1.27.2 (GOTOOLCHAIN=go1.27.2)
govulncheck -db https://storage.googleapis.com/go-vulndb ./...                  -> 4 called
govulncheck -show verbose -db https://storage.googleapis.com/go-vulndb ./...    -> 4 called + 1 module-level
govulncheck -tags integration -db https://storage.googleapis.com/go-vulndb ./... -> 4 called

npm audit --omit=dev (web)               -> found 0 vulnerabilities
```

Build and test checks (after the upgrade):

```
gofmt -l . (excluding web/node_modules)  -> no output
go mod tidy                              -> rc=0
go build ./...                           -> rc=0 (go1.27.0 and go1.27.2)
go vet ./...                             -> rc=0 (go1.27.0 and go1.27.2)
go test -count=1 ./...                   -> rc=0, 39 packages ok (go1.27.0 and go1.27.2)
```

## Findings table (after the upgrade, Go 1.27.2 where it applies)

Reachable = govulncheck found a call path from our code to the vulnerable symbol. One example path is given.

| ID | Package | Version | Fixed in | Reachable | Example call path (govulncheck) | Advice |
|---|---|---|---|---|---|---|
| GO-2026-6665 | github.com/snowflakedb/gosnowflake | v1.19.1 (latest) | none | **yes** | `internal/clients/snowflake/snowflake.go:39:21` `snowflake.New` calls `sql.Open`, which eventually calls `gosnowflake.Connector.Connect`. Logger paths too: `internal/room/service_export.go:233:20` `room.Export.Write` calls `bufio.Writer.Flush`, which eventually calls `gosnowflake.defaultLogger.Info`. | No fixed version. See "gosnowflake mitigation" below. |
| GO-2026-5046 | github.com/hamba/avro/v2 | v2.31.0 (latest) | none | **yes** (dev tool) | `cmd/devctl/scenarios.go:162:74` `devctl.amountIsDecimal` calls `avro.ArraySchema.Type` | No fixed version. Only reached from `cmd/devctl` in this scan. Keep decoding of untrusted input out of that path, or bound input size. |
| GO-2026-5047 | github.com/hamba/avro/v2 | v2.31.0 (latest) | none | **yes** (dev tool) | same entry point as above | same as GO-2026-5046 |
| GO-2026-5048 | github.com/hamba/avro/v2 | v2.31.0 (latest) | none | **yes** (dev tool) | same entry point as above | same as GO-2026-5046 |
| 13 stdlib IDs (see below) | Go standard library | go1.27.0 | go1.27.2 | **yes** on go1.27.0, **cleared** on go1.27.2 | e.g. `cmd/server/main.go:364:35` `server.run` calls `http.Server.ListenAndServe`, which eventually calls `http2.Server.ServeConn` (GO-2026-6617) | Upgrade the Go toolchain to 1.27.2 (see "Go toolchain"). Not a `go.mod` change. |
| GO-2026-5932 | golang.org/x/crypto | v0.58.0 (required) | none | **no** (module-level only) | `openpgp` package unmaintained; not called | No action. We do not import `openpgp`. |

Standard library IDs (all cleared by go1.27.2; all were called on go1.27.0): GO-2026-6617, 6613, 6612, 6611, 6610,
6609, 6608, 6607, 6605, 6604 (Windows only), 6603, 6600, 6599.

GO-2026-5764 (aws-sdk-go-v2 s3 v1.53.1 and eventstream v1.6.3, DoS panic in the EventStream decoder) is **fixed**
by this PR (see upgrades). It is absent from the "after" results.

### gosnowflake mitigation

Our code does not configure gosnowflake's logger: no `gosnowflake.SetLogger` or `SFLogger.SetLogLevel` call exists in
the repo. The driver therefore uses its default logger, which `go doc` describes as INFO level. The reachable paths
run through the driver's default logger and the DSN parser (`redact.go` already redacts DSN secrets for our own
errors). The advisory text (leak trigger and level) was not retrieved, so I cannot say which log level leaks. If the
advisory's trigger is above INFO, `gosnowflake.SFLogger.SetLogLevel("warn")` (or a stricter level) via a custom
`SetLogger` would reduce exposure. Confirm against the advisory before relying on this. No fixed version exists in
v1.x (v1.19.1 is latest), so a config mitigation is the only option in the interim.

## Upgrades applied

All minor/patch bumps within their major version, pulled by `go get github.com/aws/aws-sdk-go-v2/service/s3@v1.97.3`
and `go mod tidy`:

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

Not upgraded (no fixed version exists within the same major): hamba/avro/v2 (v2.31.0, latest), gosnowflake v1.x
(v1.19.1, latest).

## Go toolchain

`go.mod` says `go 1.27.0`. The Dockerfile uses `FROM golang:1.27-alpine AS server` (line 21), which floats to the
newest 1.27 image. CI (`.github/workflows/ci.yml`) reads `go-version-file: go.mod`.

Verified: the full build, vet, test, and govulncheck all pass with `GOTOOLCHAIN=go1.27.2` and no `go.mod` edit.

**Not applied in this PR:** adding `toolchain go1.27.2` to `go.mod`. The Dockerfile is owned by another agent, and
with a floating image tag the toolchain switch would depend on a network download during the build. Exact lines to
change to move to 1.27.2:

1. `Dockerfile` line 21: `FROM golang:1.27-alpine AS server` -> `FROM golang:1.27.2-alpine AS server` (owner: C1/A1).
2. `go.mod`, after `go 1.27.0`: add `toolchain go1.27.2` (optional; makes CI and local builds use 1.27.2 via
   `go-version-file: go.mod`). Re-run build, vet and tests if added.

## Flaky tests observed (not caused by these changes, as far as we can tell)

- `internal/httpapi` `TestLogoutClosesWebSocket` failed once in a full run after the upgrade (go1.27.0). It passed 5
  of 5 isolated runs and 3 of 3 package runs.
- `internal/ws` `TestPresenceTickClosesDeadSessions` failed once in a full run on go1.27.2. It passed 3 of 3 package
  runs on both toolchains.
- Both are timing-based tests (5 s and 10 s deadlines). The final full suite passed on both go1.27.0 and go1.27.2
  (39 packages ok each run).

## Not done

- The advisory text for GO-2026-6665 (leak trigger and log level) was not retrieved, so the gosnowflake mitigation
  is a recommendation to confirm, not a verified fix.
- The Go toolchain pin is recommended, not applied (see above).
- Dev-tool exposure to avro (`cmd/devctl`) is what the scan found; production reachability of avro was not
  separately confirmed beyond the scan output.

## New dependencies

None added. Changed versions are the aws-sdk-go-v2 set above and smithy-go v1.22.5 -> v1.24.2. No new modules.
