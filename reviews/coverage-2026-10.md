# Coverage report, 2026-10 (H8)

Branch `h8/coverage` from `origin/main` (b042c24). Baseline total statement coverage: 74.6%. After: 75.2%.

## Commands

```
git fetch origin && git checkout -b h8/coverage origin/main
go test -count=1 -coverprofile=c.out ./...      # c.out is git-ignored (*.out)
go tool cover -func=c.out
go test -count=1 -cover ./...                    # per-package percentages (table below)
go test -count=1 -cover ./internal/events/ ./internal/tools/ ./internal/room/demo/
go test -count=1 -race ./internal/tools/ ./internal/room/demo/
gofmt -l . ; go build ./... ; go vet ./...
```

Per-package coverage counts only tests living in that package (`go test` without `-coverpkg`).
Code that is exercised by other packages' tests but has no test file of its own shows 0% here.

## Per-package coverage (after this PR), sorted ascending

Only `internal/events`, `internal/room/demo` and `internal/tools` changed. Every other row is unchanged from `main`.

| Package | Coverage |
|---|---|
| internal/clients | no test files (interfaces and DTOs only) |
| internal/agent/fake | 0.0% |
| migrations | 0.0% |
| internal/clients/postgres | 7.8% |
| internal/store | 8.3% |
| internal/clients/warehouse | 10.5% |
| internal/clients/kafka | 20.2% |
| cmd/investigate | 20.5% |
| cmd/devctl | 39.4% |
| cmd/devconsumer | 39.9% |
| evals/cmd/evalrun | 43.7% |
| internal/store/memstore | 44.4% |
| cmd/server | 62.0% |
| internal/wiring | 67.5% |
| internal/clients/pgconv | 68.5% |
| internal/auth | 70.9% |
| internal/room/demo | 27.9% -> 74.3% (changed) |
| internal/ws | 75.3% |
| internal/stacklock | 78.7% |
| internal/tools/kafkatools | 83.0% |
| internal/room | 85.8% |
| internal/clients/snowflake | 86.6% |
| internal/actions | 86.7% |
| cmd/discover | 87.6% |
| internal/httpapi | 87.9% |
| internal/testenv | 89.6% |
| internal/slackbridge | 90.3% |
| internal/tools/dbtools | 90.5% |
| internal/clients/schemareg | 91.0% |
| internal/agent | 91.4% |
| internal/owners | 91.4% |
| evals | 92.0% |
| internal/topology | 92.7% |
| internal/tools/runbooks | 93.2% |
| internal/jsonnum | 93.8% |
| internal/clients/connect | 94.5% |
| internal/config | 94.7% |
| internal/discover | 95.1% |
| internal/watch | 95.1% |
| internal/clients/netsec | 99.0% |
| internal/tools | 72.6% -> 99.2% (changed) |
| internal/alertin | 99.7% |
| internal/events | 0.0% -> 100.0% (changed) |

`internal/ws` moved between 75.3% and 76.0% across runs (timing-dependent tests); it is not touched by this PR.

## Choice of packages

The three lowest-covered packages that are pure logic (in-memory, no network, DB, Docker or subprocess) are:

1. **internal/events** (0.0% -> 100.0%): payload decoding (`Decode`) and the JSON wire names of `Event`. The package has no tests of its own. Its one function is pure. `events.go` is a contract file and is not modified; only `events_test.go` is added.
2. **internal/room/demo** (27.9% -> 74.3%): deterministic fixtures and in-memory fake Connect and Schema Registry clients for `--demo`. There are no network calls. The scripted `Run` is covered only on its cancel path; its full path takes about 9 s because of the 550 ms step delay and is not exercised. This is the weakest pick in terms of "production logic": it is demo code, but it is in-memory and deterministic.
3. **internal/tools** (72.6% -> 99.2%): the `Registry` (register, get, registration order, concurrency), `Truncate` (UTF-8 safe cut) and the scope predicates.

Packages skipped, with reasons:

- `cmd/*`, `evals/cmd/*`: mains (excluded by the brief).
- `internal/clients/*`, `internal/store/*`, `internal/clients/pgconv`: clients and stores (excluded by the brief). `pgconv` is pure, but it sits under `clients`.
- `migrations`: SQL embed, no Go logic.
- `internal/agent/fake`: test doubles.
- `internal/auth` (70.9%): OIDC over HTTP to the fake IdP (httptest, testenv) plus claims parsing. Network-facing.
- `internal/stacklock` (78.7%): runs `git` as a subprocess and touches the filesystem.
- `internal/ws`, `internal/httpapi`, `internal/room`, `internal/actions`, `internal/wiring`, `internal/tools/kafkatools`, `internal/tools/dbtools`, `internal/testenv`, `internal/slackbridge`: network, DB, Docker or client-backed.

## Before / after (chosen packages)

| Package | Before | After | Gain |
|---|---|---|---|
| internal/events | 0.0% | 100.0% | +100.0 |
| internal/room/demo | 27.9% | 74.3% | +46.4 |
| internal/tools | 72.6% | 99.2% | +26.6 |

All three meet the +15 target.

## Tests added

- `internal/events/events_test.go`: table tests for `Decode` (valid, unknown fields, null, empty object, truncated JSON, wrong JSON type, wrong field type); error wraps `*json.SyntaxError` and `*json.UnmarshalTypeError` and names the event type; the error does not echo payload content (payloads carry alert text and record values); `Event` JSON keys `room_id`, `seq`, `type`, `actor`, `payload`, `at` and round trip.
- `internal/tools/tool_test.go`: `Truncate` (empty, at limit, one over, far over, a 3-byte rune straddling the cut at each continuation offset, exact-cut boundary); `Registry` register/get table (empty name, duplicate, unknown, case sensitivity), refused registrations leave the registry unchanged, `All` keeps registration order and returns a copy, 50 concurrent registrations; `Scope.PipelineID`, `TopicInScope`, `SourceTableInScope` for scoped and unscoped scopes.
- `internal/room/demo/demo_test.go`: `Build` consistent with `LayerOwner` and edges point at real nodes; `Render` exact text for empty and small graphs; `Trace` table (ok, out-of-pipeline table, unknown pipeline, empty table) with `topology.ErrBadInput`; `LayerOwner` table; `WriteHandoff` complete fixture; `mustJSON` including the unmarshallable fallback; fake Connect status, config, restart and resume unknown-connector and unknown-task paths (`clients.ErrNotFound`); fake Registry subjects, versions, schema (version 0, 2, -1, unknown subject), compatibility and set (failed set does not create a subject), register; `Run` with a cancelled context returns nil after one `agent.status` event.

## Bugs found

No real bug was found in the three packages. Each test was checked against the intended behaviour; nothing was skipped.

Observations, not bugs, not fixed, not tested (both in the `tools` contract file, out of scope for this PR):

- `tools.Registry` zero value (`var r tools.Registry`) panics on `Register` (nil map). `NewRegistry` is the documented constructor, so this is a robustness note only.
- `Register(nil)` panics on `t.Name()`. Same status.

## Not done / known gaps

- `demo.Run` full scripted path (about 9 s with the 550 ms step delay) is not covered; only the cancel path is.
- `mustJSON` fallback is covered via an unmarshallable channel value, which is a deliberate error-path test.
- Coverage of `internal/room/demo` is still under 75%; the remaining uncovered code is almost entirely the scripted `Run` body.
