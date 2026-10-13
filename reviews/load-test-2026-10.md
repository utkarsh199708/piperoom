# WebSocket fan-out load test — 2026-10

Author: [O5]. Test: `internal/loadtest/ws_load_test.go` (build tag `load`, not part of `go test ./...`).

## Setup

- **Machine:** Linux cloud VM, 4 vCPU, 15 GiB RAM, kernel 6.18, Go 1.27.0 linux/amd64, GOMAXPROCS=4.
- **Stack under test (one process):** `memstore` → `room.Service` → `httpapi.New` (real router, auth
  middleware in dev mode with `X-User`, real `ws.Gateway`) served by `httptest.Server` on loopback.
  The load clients run in the same process and share the 4 vCPUs, so the numbers are an upper bound
  on server cost, not a pure server measurement.
- **Load:** 50 open rooms × 20 WebSocket clients (1,000 sockets, users rotate arjun/priya/meera/kabir).
  A fake agent per room (`room.Deps.Run`) emits `agent.message` at 5 events/s/room through the real
  agent Sink (~300 B padding per message) and drains its inbox; every 50th tick (~10 s) it creates a
  proposal through `agent.Proposals`. One approver approves each proposal over HTTP as `priya` with the
  `params_hash` from `GET /api/rooms/{id}` (executor then appends `approval.given`, `action.executed`,
  ...). One comment per room every 2 s over `POST /comments`. Clients send presence every 20 s.
- **Window:** 60 s of measured load after all 1,000 sockets are connected.
- **Latency:** the agent writes `time.Now()` into the message just before `Sink.Emit`; the client takes
  the receive time right after `conn.Read`. So latency = room service append + store fan-out +
  gateway marshal/queue/write + loopback + client frame read.
- **Seq check:** every client checks every event frame: seq must be exactly `last+1` (gaps and
  duplicates counted), and after the window each client's last seq must equal the room's final seq.
  The test fails on any gap, duplicate, missing tail event, or goroutine leak after shutdown. It
  also fails when clients reconnect more than `LOAD_MAX_RECONNECTS` times (default 0), because a
  server-side drop such as "send buffer full" is otherwise hidden by reconnect + replay. It fails when
  any agent emit, proposal, comment or approval request errors, and when not every created proposal
  was approved. Both checks were verified by injecting faults: an approve with a wrong layer gave
  `approveErrs=5` and `approvals=0, want 5` → FAIL; dropping each client's first connection gave
  `gaps=0 ... reconnects=20` → FAIL. Both faults were reverted.
- **Agent phase:** by default the 50 agents are spread evenly over one 200 ms period (real rooms are
  not phase-locked). `LOAD_BURST=1` makes all 50 tick at the same instant (worst-case herd:
  1,000 frames at once).

### Exact commands

```
# headline run (staggered agents)
go test -tags load ./internal/loadtest -run TestWSFanoutLoad -v -count=1 -timeout 10m

# worst case: all agents tick together
LOAD_BURST=1 go test -tags load ./internal/loadtest -run TestWSFanoutLoad -v -count=1 -timeout 10m

# profiled run (CPU, mutex, heap at the end of the window)
LOAD_HEAP_PROFILE=/tmp/heap-peak.out go test -tags load ./internal/loadtest -run TestWSFanoutLoad -v \
  -count=1 -timeout 10m -cpuprofile /tmp/cpu.out -mutexprofile /tmp/mutex.out -o /tmp/loadtest.test
```

Knobs (env): `LOAD_ROOMS` (50), `LOAD_CLIENTS` (20), `LOAD_RATE` (5), `LOAD_DURATION` (60s),
`LOAD_COMMENT_EVERY` (2s), `LOAD_PROPOSAL_EVERY` (50), `LOAD_PAD_BYTES` (300), `LOAD_BURST`,
`LOAD_HEAP_PROFILE`, `LOAD_MAX_RECONNECTS` (0). Smoke run: `LOAD_ROOMS=5 LOAD_CLIENTS=4 LOAD_DURATION=10s`.

## Results

Every run: 14,700 agent events emitted (245/s), ~17,000 events in the room logs, ~341,000 event
frames delivered (~5,700/s), 250–300 proposals created and approved, ~1,500 comments, 0 HTTP errors.
**Seq check on every run: gaps=0, dups=0, reconnects=0, missing-at-end=0.**

### Fan-out latency (emit → client receive, n ≈ 294,000 samples per run)

| Run | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|
| Before, staggered | 0.74 ms | 1.25 ms | 27.0 ms | 150 ms | 204 ms |
| Before, staggered (profiled) | 0.66 ms | 1.72 ms | 19.5 ms | 61 ms | 114 ms |
| Before, burst | 14.2 ms | 33.6 ms | 61.1 ms | 97 ms | 343 ms |
| **After, staggered** | **0.58 ms** | **1.03 ms** | **10.7 ms** | **34 ms** | **50 ms** |
| After, staggered (2nd run) | 0.48 ms | 1.02 ms | 11.7 ms | 39 ms | 68 ms |
| After, staggered (profiled) | 0.42 ms | 0.85 ms | 3.9 ms | 19 ms | 46 ms |
| After, burst | 10.5 ms | 21.3 ms | 41.6 ms | 104 ms | 120 ms |

Run-to-run spread on this shared VM is large at p99 and above (see the two "after" runs); compare
rows of the same kind. The burst p50 of ~10–14 ms is pure queueing: 1,000 frames are produced in the
same instant and each costs roughly 10 µs of CPU end to end (server write syscall + client read and
JSON decode on the same 4 cores).

### Memory and goroutines (unprofiled staggered runs)

| | Before | After |
|---|---|---|
| Heap in use, 1,000 sockets connected, idle | 96–99 MiB | 62–70 MiB |
| Heap in use, peak during load | 176–186 MiB | 105–112 MiB |
| RSS, connected idle | 118–119 MiB | 118–126 MiB |
| RSS, peak | 214–215 MiB | 173–176 MiB |
| Goroutines, connected idle | 6,057 | 6,057 |
| Goroutines, peak | ~6,350 | ~6,340 |
| Goroutines after shutdown | 2–3 (= baseline) | 2–3 (= baseline) |
| CPU samples over the 60 s profiled run | 20.97 s (34% of one core) | 9.02 s (15% of one core) |
| Mutex wait (profiled run, total) | 178 s | 105 s |

Heap and RSS include the in-process load clients (each client side holds a websocket conn with its
own bufio buffers), so roughly half of the per-socket cost is the client. Goroutines: ~6 per socket
(server: reader, writer, event loop, store subscriber; client: reader, presence ticker) — no leak;
everything returns to baseline after `Server.Close`.

## Profile findings (before the fixes)

1. **`memstore.Since` held the store-wide lock for a linear scan of the whole room log** —
   75 % (first run) / 26 % (profiled baseline) of all mutex wait. Every subscriber (one per socket) calls
   `Since(lastSeq)` on every append, so the cost was O(subscribers × room length) under one lock shared
   by all rooms and by `Append`. This is the test store, but every unit test of `room`, `httpapi` and `ws`
   runs on it, and it is the store of this load test.
2. **Per-subscription channel of 256 `events.Event` values** — `memstore.(*Store).Subscribe` was
   50 % of in-use heap at the end of the window (43 MB of 86 MB sampled). Each slot is an
   `events.Event` by value (136 B), so 256 slots are ~35 KB per open socket. The Postgres store
   (`store.(*PG).Subscribe`) allocates the same channel. The gateway drains that channel immediately
   into its own 256-slot `[]byte` send buffer, so the large store buffer buys nothing.
3. **`ws.(*Gateway).addClient` / presence broadcast under the gateway-wide `g.mu`** — 41 % of mutex
   wait in the profiled baseline, almost all during the connect phase: each join marshals the room's
   member list and pushes it to every member while holding the one gateway lock for all rooms
   (O(n²) per room on a reconnect storm). Not fixed (see below).
4. CPU: ~27 % of CPU is write syscalls (one `write` per frame per socket, plus a flush), JSON encoding
   of the same event once per client (`json.Marshal` in `eventLoop`, ~5–7 %), and per-frame
   `context.WithTimeout` / `AfterFunc` allocations inside `coder/websocket` (largest allocators by
   bytes along with `io.ReadAll` on the client side).

## Fixes made

1. `internal/store/memstore/memstore.go` — `Since` uses `sort.Search` on the seq-ordered log and copies
   at most `limit` events (O(log n + limit) under the lock instead of O(room length)). Behaviour pinned by
   `TestSinceMatchesLinearScan` (every `afterSeq`/`limit` edge compared with the original linear filter,
   including negative and past-the-end `afterSeq`), `TestSinceReturnsCopy` and `TestSinceErrors`.
2. `internal/store/store.go` + memstore — new `store.SubscribeBuffer = 16` used by both stores instead of
   256. On a full channel the subscriber goroutine blocks in its send (until the reader takes the event
   or ctx ends), and `lastSeq` only advances after a successful send. So nothing is lost or reordered.
   This is pinned by `TestSubscribeSlowReaderGetsEverything`, which appends max(10× the buffer, 100)
   events before reading and checks every seq arrives once and in order. This cut peak heap by ~40 % in
   the load test. The constant also applies to the Postgres store (~35 KB → ~2 KB per open socket),
   **but 256 → 16 was only measured on memstore.** Follow-up: a Postgres integration twin of
   `TestSubscribeSlowReaderGetsEverything` (build tag `integration`, dev stack).

No change to `internal/ws`, `internal/room` or `internal/httpapi`.

Effect: staggered p99 27 ms → ~11 ms, CPU per minute of load roughly halved, peak heap 180 → 108 MiB,
burst p50 14 → 10.5 ms. Correctness unchanged (0 gaps / 0 dups in every run; all unit tests pass).

## Not fixed — recommended follow-ups

1. **One store subscription per WebSocket (production impact, Postgres).** `ws.Gateway` calls
   `store.Subscribe` per connection; on `*PG` every subscriber runs its own
   `SELECT ... WHERE room_id=$1 AND seq>$2` on each NOTIFY. `PG.drain` (`internal/store/hub.go`) also
   has no `len(events) < limit` early return. So one wake costs about 3 queries per subscriber: the
   page with the event, an empty read that ends `drain`, and an empty read on the immediate re-drain in
   `runSub`. With 20 viewers per room that is about 60 queries per event. At this test's load (~285
   appends/s including comments and approvals) that is roughly 15,000–17,000 queries/s against Postgres,
   not the ~4,900/s an earlier draft of this report said. Adding the early return to `PG.drain`, as
   memstore has, would cut this about 3×. It was not changed here.
   Fix: one subscription per room in the gateway (or in the store hub) that fans the same event out to
   every client of that room, each client keeping its own `lastSeq` for gap-free resume. Not small; touches
   the gateway's lifecycle. **Highest-value next step.** This test cannot show it because it uses memstore;
   an integration variant against the dev-stack Postgres should be added.
2. **Marshal each event once per room, not once per client.** Comes for free with (1): the room fan-out
   can marshal `serverMsg{event}` once and hand the same `[]byte` to all 20 send buffers.
3. **Presence broadcast under the gateway-wide lock.** Make the lock per room (or marshal outside the
   lock) and coalesce presence broadcasts during join storms (e.g. at most one per room per 250 ms).
4. **memstore global lock.** Still the main source of mutex wait after the fix (all rooms share one
   `sync.Mutex`); a per-room lock would remove it. Low priority: test store only.
5. **`coder/websocket` per-call timeouts.** `client.write` and `readLoop` create a `context.WithTimeout`
   per frame, which the library turns into `AfterFunc` + timer allocations. Acceptable at this load;
   revisit if a profile at 10× shows GC pressure.

## Done-when checks (this branch)

- `gofmt -l .` → empty
- `go build ./...`, `go vet ./...`, `go vet -tags load ./...` → clean
- `go test -count=1 ./...` → all packages ok (the load test is not compiled without the tag)
- `go test -count=1 -race ./internal/store/memstore/` → ok
- `go test -tags load ./internal/loadtest -run TestWSFanoutLoad -v` → PASS (numbers above)

### Re-check after review round 1 (stricter assertions)

`LOAD_DURATION=30s go test -tags load ./internal/loadtest -run TestWSFanoutLoad -v -count=1` → PASS:
7,350 agent events, 101 proposals / 101 approvals, 750 comments, every error counter 0, 170,060 frames
(5,668/s). Latency p50 0.40 ms, p90 0.79 ms, p99 4.1 ms, p99.9 15.7 ms, max 29.7 ms. gaps=0 dups=0
reconnects=0 missing=0. Peak heap 87 MiB, peak RSS 163 MiB, goroutines 6,321 at peak and 2 after shutdown.
