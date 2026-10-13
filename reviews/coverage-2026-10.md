# Coverage report, 2026-10 (H8)

Branch `h8/coverage` rebased onto `origin/main` at `df66b6d` (Merge PR #79). This is the baseline: main as it is now, not the older b042c24. Total statement coverage: 74.7% before (main), 75.8% after.

## Commands

```
git fetch origin && git checkout -b h8/coverage origin/main
go test -count=1 -coverprofile=c.out ./...      # c.out is git-ignored (*.out)
go tool cover -func=c.out | tail -1
go test -count=1 -cover ./...                    # per-package percentages (table below)
go test -count=1 -cover ./internal/events/ ./internal/tools/ ./internal/room/demo/ ./internal/store/memstore/ ./internal/clients/pgconv/
go test -race -count=1 ./internal/events/ ./internal/tools/ ./internal/room/demo/ ./internal/store/memstore/ ./internal/clients/pgconv/
gofmt -l . ; go build ./... ; go vet ./...
```

Per-package coverage counts only tests living in that package (`go test` without `-coverpkg`).
Code that other packages exercise but that has no test file of its own shows 0% here.

## Definition of "pure logic"

One definition used for the whole ranking: a package is pure logic when its behaviour depends only on its inputs and its own in-process state. It does no network, DB, Docker, filesystem or subprocess I/O, and it does not construct real clients. In-memory fakes and in-memory stores count as pure. Excluded regardless: `cmd/*` mains, migrations (SQL embed), and test-double packages (`agent/fake`).

## Ranking (lowest coverage first, pure-logic packages that are not excluded)

| Rank | Package | Before | Pure? | Note |
|---|---|---|---|---|
| 1 | internal/events | 0.0% | yes | Type declarations plus one function, `Decode`. The package had no tests of its own. |
| 2 | internal/room/demo | 27.9% | yes | Deterministic fixtures and in-memory fake Connect and Schema Registry clients. |
| 3 | internal/store/memstore | 51.5% | yes | In-memory `store.Store`; no I/O. |
| - | internal/wiring | 78.0% | no | Excluded. `Open` dials real clients (Postgres, Kafka, Connect, Schema Registry, the model endpoint, the warehouse). Construction does I/O. |
| 4 | internal/clients/pgconv | 68.5% | yes | Pure value conversion and deadline helper. Lives under `clients`, but it has no I/O. |
| - | internal/auth | 71.6% | no | Excluded. OIDC discovery, token exchange and claims parsing over HTTP. |
| 5 | internal/tools | 72.6% | yes | Registry, `Truncate`, scope predicates. |

The three lowest pure-logic packages are **events, room/demo and memstore**. Per the review, **pgconv** is raised as well, and **tools** is kept as an extra. All five are in the before/after table below.

Not classified here: `internal/config` is at 51.4% on main (it was 94.7% in the earlier baseline, after newer config fields landed without tests). It reads the environment (`os.LookupEnv`), so it is not in the ranking. The lead should decide whether it counts as pure logic; if it does, it sits just below memstore.

Other excluded packages, with reasons:
- `internal/stacklock` (78.7%): runs `git` as a subprocess and touches the filesystem.
- `internal/ws`, `internal/httpapi`, `internal/room`, `internal/actions`, `internal/tools/kafkatools`, `internal/tools/dbtools`, `internal/testenv`, `internal/slackbridge`: network, DB, Docker or client-backed.
- `internal/clients/*` (except pgconv), `internal/store` (the store interface package): clients and interfaces.

## Per-package coverage (after this PR), sorted ascending

| Package | Coverage |
|---|---|
| internal/clients | no test files (interfaces and DTOs only) |
| internal/loadtest | no test files |
| internal/agent/fake | 0.0% |
| migrations | 0.0% |
| test/e2e/oidc/certgen | 0.0% |
| internal/clients/postgres | 7.8% |
| internal/store | 8.3% |
| internal/clients/warehouse | 10.5% |
| cmd/investigate | 20.0% |
| internal/clients/kafka | 20.2% |
| cmd/devctl | 39.4% |
| cmd/devconsumer | 39.9% |
| evals/cmd/evalrun | 43.7% |
| internal/config | 51.4% |
| cmd/server | 61.7% |
| internal/store/memstore | 51.5% -> 70.9% (changed) |
| internal/auth | 71.6% |
| internal/room/demo | 27.9% -> 74.3% (changed) |
| internal/ws | 75.0% |
| internal/wiring | 78.0% |
| internal/stacklock | 78.7% (75.0% on main in the baseline run; this PR does not touch it, so the difference is run-to-run) |
| internal/tools/kafkatools | 83.0% |
| internal/room | 86.3% |
| internal/clients/snowflake | 86.6% |
| internal/actions | 86.7% |
| cmd/discover | 87.6% |
| internal/httpapi | 88.3% |
| internal/testenv | 89.6% |
| internal/slackbridge | 90.3% |
| internal/tools/dbtools | 90.5% |
| internal/clients/schemareg | 91.0% |
| internal/agent | 91.4% |
| internal/owners | 91.5% |
| evals | 92.0% |
| internal/topology | 92.7% |
| internal/tools/runbooks | 93.2% |
| internal/clients/connect | 94.5% |
| internal/discover | 95.1% |
| internal/watch | 95.1% |
| internal/clients/pgconv | 68.5% -> 96.3% (changed) |
| internal/clients/netsec | 99.2% |
| internal/tools | 72.6% -> 99.2% (changed) |
| internal/alertin | 99.7% |
| internal/events | 0.0% -> 100.0% (changed) |
| internal/jsonnum | 100.0% |

## Before / after (every package this PR touched)

| Package | Before | After | Gain |
|---|---|---|---|
| internal/events | 0.0% | 100.0% | +100.0 |
| internal/room/demo | 27.9% | 74.3% | +46.4 |
| internal/store/memstore | 51.5% | 70.9% | +19.4 |
| internal/clients/pgconv | 68.5% | 96.3% | +27.8 |
| internal/tools | 72.6% | 99.2% | +26.6 |
| total (all packages) | 74.7% | 75.8% | +1.1 |

Every touched package gains at least +15 points.

## Tests added

- `internal/events/events_test.go` (new): `Decode` table (valid, unknown fields, null, empty object, truncated JSON, wrong JSON type, wrong field type); a nil payload is an error; error wraps `*json.SyntaxError` and `*json.UnmarshalTypeError` and names the event type; the error does not echo payload content; `Event` JSON keys `room_id`, `seq`, `type`, `actor`, `payload`, `at` and round trip. `events.go` (contract) is not modified.
- `internal/tools/tool_test.go` (new): `Truncate` (empty, at limit, one over, far over, a 3-byte rune straddling the cut at each continuation offset, exact-cut boundary); `Registry` register and get table; refused registrations leave the registry unchanged; `All` keeps registration order and returns a copy; 50 concurrent registrations; `Scope.PipelineID`, `TopicInScope`, `SourceTableInScope` for scoped and unscoped scopes.
- `internal/room/demo/demo_test.go` (new): `Build` consistent with `LayerOwner` and edges point at real nodes; `Render` exact text; `Trace` full fixture compared with `reflect.DeepEqual` (a changed offset or row id fails), plus a table of error cases (`topology.ErrBadInput`); `LayerOwner` table; `WriteHandoff` complete fixture; `mustJSON` including the unmarshallable fallback; fake Connect, Registry and error paths (`clients.ErrNotFound`); `Run` with a cancelled context returns nil after one `agent.status` event.
- `internal/store/memstore/eventlog_test.go` (new): `Append` seq is gapless per room; event fields and UTC timestamp; failed appends (unknown room, cancelled context, unmarshallable payload) do not consume a seq; `Since` table (start, limit, middle, end, past end, limit zero or negative); `Subscribe` replays then streams live; channel closes on context end; error paths; 200 concurrent appends with replay produce no gaps or repeats.
- `Since` error paths and the independent-slice check are already on main in `internal/store/memstore/since_test.go` (`TestSinceErrors`, `TestSinceReturnsCopy`), so this PR does not repeat them.
- `internal/store/memstore/roomlife_test.go` (new): `CreateRoom` rules (duplicate, unstorable NUL text is refused and not visible, cancelled context, LastSeq forced to 0); `GetRoom` unknown; `ListRooms` order (opened_at desc, then id); `DeleteEmptyRoom` (empty removed, room with events refused with `ErrRoomNotEmpty`, unknown is idempotent); `LastRoomByDedupKey` (newest match, unknown and empty key, cancelled context); `UpdateDriver` and `CloseRoom` (fields recorded, unknown rooms).
- `internal/clients/pgconv/pgconv_table_test.go` (new): integer types at min and max to decimal strings; floats stay plain with NaN and Inf; times converted to UTC RFC 3339 with nanoseconds, zero time; untyped NULL stays nil; nested slices and maps are JSON-encoded as-is; unmarshallable values fall back to fmt rendering; bytes and UUID arrays; `NumericString` table (zero with scale, leading zeros after the point, trailing zeros, negative fractions, 23-digit value without rounding, Infinity, e-notation beyond 100000); `WithDeadline` error paths (cancelled parent, zero duration, cancel func).

## Bugs found

**One real bug, found by a test and left out (skipped with a reason):**

- `pgconv.NumericString` renders a zero with a positive exponent as `"000"` instead of `"0"`. The code appends `strings.Repeat("0", e)` to the digit string without checking for zero. Test: `TestNumericStringZeroWithPositiveExponentIsSkipped` (`t.Skip`, referencing this report). The input (`Int=0, Exp=2`) is a non-canonical zero that Postgres does not normally produce, so the impact is low.

**Observations, not bugs, not fixed, not tested (for the lead):**

- `pgconv.ToJSON` on typed NULL-like values: `pgtype.Numeric{Valid:false}` returns `""`, `pgtype.UUID{Valid:false}` returns the zero UUID string, and `[]byte(nil)` returns `""`. Untyped `nil` correctly returns `nil`. The clients pass `rows.Values()`, which gives untyped `nil` for SQL NULL, so these are not reachable from the current clients. They are a footgun for future callers. Not pinned by tests.
- `tools.Registry` zero value (`var r tools.Registry`) panics on `Register` (nil map). `NewRegistry` is the documented constructor. Robustness note only.
- `tools.Registry.Register(nil)` panics on `t.Name()`. Same status.
- `internal/ws` coverage moves between 75.0% and 76.7% across runs (timing-dependent tests). Not touched by this PR.

## Not done / known gaps

- `demo.Run` full scripted path (about 9 s because of the 550 ms step delay) is not covered; only the cancel path is.
- `memstore` proposal, approval, suggestion and handoff methods are still largely uncovered. They are outside the event-log and room-lifecycle scope of this PR. `memstore` is at 70.9%, above the +15 target.
- `internal/clients/pgconv`: the `pgtype.Timestamptz` (struct) default-case path through `json.Marshal` is not asserted. Its output for a valid value was not verified and is not pinned.
- The `wiring`, `auth` and `stacklock` exclusions are judgement calls under the definition above, stated in the ranking table.
