# Vulnerability report 2026-10: govulncheck and npm audit

Agent C3 (cloud), 2026-10-13, against `main` @ 2fb0055. Branch `c3/vuln-report`.

## Result

- **Go (govulncheck): scan did not complete.** The Go vulnerability database at `https://vuln.go.dev` is blocked
  by the session's egress proxy (CONNECT answered 403 by policy). govulncheck stops before scanning, so this report
  makes **no Go findings, reachable or unreachable, in either direction**. Absence of a table row here does not mean
  the code is clean. Re-run the scan from a host that can reach `vuln.go.dev` (see "Not done").
- **npm (web, production dependencies): 0 vulnerabilities** reported by `npm audit --omit=dev`.
- **Upgrades applied: none.** No Go finding exists, so no `go.mod` change qualifies under "upgrade only what fixes a
  finding". `go.mod`, `go.sum`, `web/package.json` and `web/package-lock.json` are unchanged.

## Tool versions

| Tool | Version |
|---|---|
| Go toolchain (project, `go.mod` `go 1.27.0`) | go1.27.0 linux/amd64 |
| govulncheck | v1.9.0 (`golang.org/x/vuln/cmd/govulncheck@v1.9.0`), built with go1.27.0 |
| Node | v22.22.0 |
| npm | 10.9.4 |

Note on the build: `go install golang.org/x/vuln/cmd/govulncheck@latest` resolved to v1.9.0, which requires Go 1.26
and built itself with go1.26.9. Run against the go1.27.0 standard library, that binary failed while loading packages
(`package requires newer Go version go1.27`). Rebuilding with `GOTOOLCHAIN=go1.27.0 go install ...@v1.9.0` fixed the
loader. The DB fetch failure below is independent of this.

## Commands and output

Go scan (run 4 times: default, `-show verbose`, `-tags integration`, `-db https://vuln.go.dev`):

```
$ govulncheck ./...
Go: go1.27.0
Scanner: govulncheck@v1.9.0
DB: https://vuln.go.dev
govulncheck: fetching vulnerabilities: Get "https://vuln.go.dev/index/modules.json.gz": Forbidden

$ govulncheck -show verbose ./...      # same DB fetch failure
$ govulncheck -tags integration ./...  # same DB fetch failure
```

Proxy evidence: `curl https://vuln.go.dev/index.json` returns `CONNECT tunnel failed, response 403`. The proxy status
endpoint lists two `connect_rejected` entries for `vuln.go.dev:443` ("gateway answered 403 to CONNECT (policy
denial or upstream failure)"). `proxy.golang.org` is reachable (HTTP 200), so module downloads work; only the vuln DB
host is blocked. No local vuln DB exists. The only `modules.json.gz` files on disk are govulncheck's own testdata
fixtures, which were not used.

npm:

```
$ cd web && npm ci && npm audit --omit=dev
found 0 vulnerabilities
```

Build and test checks (on this branch, no code changed):

```
$ gofmt -l .            # (excluding web/node_modules) -> no output
$ go build ./...        # rc=0
$ go vet ./...          # no output
$ go test -count=1 ./...  # rc=0, all packages ok or no test files
```

## Findings table

| ID | Package | Current | Fixed in | Reachable | Advice |
|---|---|---|---|---|---|
| (none determined) | - | - | - | not determined: DB unreachable | Re-run govulncheck with DB access (see below) |

Go standard library: not determined. The toolchain in use is go1.27.0. A stdlib finding would be fixed by upgrading
the Go toolchain (`go` line in `go.mod` and CI images), not by a `go.mod` dependency upgrade.

## Not done

- **Go vulnerability scan.** Not completed; the DB host is blocked by egress policy. Per the proxy docs, a 403 policy
  denial is reported, not routed around. To finish: run `govulncheck -show verbose ./...` and
  `govulncheck -tags integration ./...` from a host with access to `https://vuln.go.dev`, then add a row per finding
  with the call path for reachable ones. Apply only patch/minor fixes that govulncheck ties to a finding.
- **Unreachable (module-level) Go findings.** Could not be listed for the same reason.
- **Go upgrades.** None applied, because no finding was confirmed.
- **Integration-tagged scan.** Attempted; the same DB fetch failure stopped it, so its result is not known.

## New dependencies

None. No module or package was added or changed.
