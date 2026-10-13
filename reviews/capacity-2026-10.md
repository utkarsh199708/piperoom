# Capacity and isolation under load — 2026-10

Author: [S2]. Harness: `internal/loadtest` + `cmd/loadtest` (PR "[S2] loadtest: queue fairness, room replay and
WebSocket capacity harness"). Phase 1 (this document as first published): **one replica, `main` @ b81248b**.
Phase 2 (2 and 3 replicas on S1's `s1/replicas`) is a separate PR and section 9.

> **Read this first.** Every number below comes from one shared 4-vCPU cloud VM on which other agents were
> building and testing at the same time (load average 15–60 during the runs, see each table). The
> processes under test got between 0.3 and 1.8 cores in total. **These numbers are a floor, not a target.**
> The supported-scale statement in section 7 is a proposal for the pilot docs, with 2x headroom below
> what broke first here; section 8 lists what must be re-run on production-like hardware before anyone
> publishes a scale target.

## 1. Summary — what broke first

| Scenario | What broke first on this box | Where |
|---|---|---|
| 1. Queue fairness | FIFO agent queue: pipelines B and C (3 rooms each) waited **15.6–18.6 s = 15–17 run holds** behind 46–47 rooms of pipeline A. With real runs lasting minutes, that is the better part of an hour. | `internal/room/agentslots.go` (FIFO) |
| 2. Agent throughput | Not the agent cap: the **event write path**. Every append in the process runs one at a time under `room.Service.mu` (DB round trips included). With zero model latency the serialized section was 94–97% busy at **176–366 events/s (52–109 turns/s)**, whatever `PIPEROOM_MAX_AGENTS` was (3, 10 or 30). | `internal/room/service.go` `emitLocked` |
| 3. Room replay | **Join storms on big rooms.** 200 viewers opening a 10k-event room at once: **all 200** replays were cut by the gateway's 30 s replay timeout; they finished after reconnecting (35–57 s). 20k-event room: 51–113 s, up to 3 attempts each. A single new viewer replays 20k events in 0.7–1.1 s (11.3 MB). | `internal/ws/gateway.go` `replayTimeout`, per-client replay |
| 4. WebSocket fan-out | **Writes, not deliveries.** At 2,000 viewers (40 per room, 50 rooms, 1 comment/s/room) the server accepted only **30–31 of 50 comments/s**, comment POST p50 **1.5–1.7 s**. Cause: every socket polls Postgres on its own (~5,600–5,800 queries/s on a **4-connection pool**, 99% of acquires waited), and appends wait for a connection while holding `Service.mu`. No drops: 0 send-buffer-full closes, 0 gaps, 0 goroutine leaks. | `internal/store/hub.go` (one subscription per socket), pool size, `Service.mu` |
| 5. Isolation | Not isolated. One room flooding tool events (54–58 model turns, 8 tool calls each, 61–65 events/s — the most the shared write path gave it) raised quiet rooms' **comment POST p95 1.5–6.9x** and **delivery p95 0.8–2.5x** across three runs. | same shared write path and pool |

Also found (section 6): **`pool_max_conns` in `PIPEROOM_DB_URL` stops `cmd/server` from starting** and kills the
LISTEN hub of any store opened with it, so the pool size cannot be raised today; it is pgxpool's default,
`max(4, NumCPU)` — **4 on a 4-core host**.

Correctness held under every load: in two full runs and the smoke runs, every subscriber saw every event once
and in order (0 gaps, 0 duplicates, 0 missing at end, 0 parse mismatches), the fake model never saw more
concurrent runs than `PIPEROOM_MAX_AGENTS`, no room had a duplicate agent run, every approved fix (CI smoke)
executed exactly once, there were 0 append errors and server goroutines returned to their baseline after every
fan-out run.

## 2. Machine and method

**Machine.** Linux cloud VM, **4 vCPU** (Intel Xeon @ 2.10 GHz), 15.7 GiB RAM (`MemTotal: 16479424 kB`), kernel
6.18.44, Go 1.27.0 linux/amd64, GOMAXPROCS=4, Docker 29.8.2. Postgres **16.15** (`postgres:16` image) in a
throwaway container on the same VM, default settings except `max_connections=300`, data on the container's
overlay filesystem (fsync on). **Shared:** other agents' builds and test suites ran throughout; the load
average at the start and end of every run is in the raw tables. `/proc/loadavg` and per-process CPU (server
`getrusage`, driver `getrusage`, Postgres container cgroup) are recorded in every result.

**Processes.**

- **Driver** (`loadtest run`): the fake model (HTTP on loopback), the room/comment/approval HTTP clients and
  the WebSocket clients (Go, `coder/websocket`). It never shares a process with a server, so server memory
  and goroutine counts are the server's alone.
- **Server(s)** (`loadtest serve`, re-executed by the driver): the room service exactly as `cmd/server`
  assembles it on Postgres — `store` on a `pgxpool` built from the same URL (so the **same default pool
  size**), `room.New`, `agent.NewEndpoint` (provider `anthropic`, no token cap) whose `Run` is the real
  agent loop with the real Anthropic SDK streaming client, `httpapi.New` with the real WebSocket gateway, dev
  auth (`X-User`). `ANTHROPIC_BASE_URL` points at the fake model; the server refuses to start if that URL is
  not loopback, and the driver strips every `ANTHROPIC_*`, `AWS_*`, `PIPEROOM_*` variable from the child's
  environment, so no real key or endpoint can be used. Replaced: the read tools (one fake `lt_probe`) and
  the write actions (one fake scoped `lt_noop`). Left out: topology builder (no pipeline map), watcher,
  retention, Slack mirror, OIDC, static web UI, ingress/TLS. Extra: `/loadtest/stats`, `/loadtest/reset`,
  `/loadtest/gc` and `/debug/pprof/` on the same loopback listener.
- **Postgres**: one container per driver invocation, a fresh database per scenario run (migrated by the driver,
  dropped afterwards).

**Fake model.** Streams Messages API SSE (`message_start` … `message_stop`) that the SDK accumulates exactly as
in production. Per turn: waits `model_latency`, reports `model_tokens` output tokens, and either calls
`lt_probe` (until `tool_calls` per run are done) or ends the turn with ~4 bytes/token of text. Room modes by
title marker: `ltmode-quiet` (end at once), `ltmode-flood` (tool calls on every turn), `ltmode-propose` (one
`propose_fix` with `lt_noop`). The reply is a pure function of the config and the request body
(unit-tested); it counts per room the run starts (requests whose history is only the opening message) and
concurrent requests — the duplicate-run detector.

**Fake tools.** `lt_probe` waits `tool_latency` and returns a `tool_summary_bytes` summary; `lt_noop` counts its
executions by key, summed over every server — the duplicate-execution detector.

**Measurements.**

- *Time to first agent turn (TTFT)* and *queue wait*: server clock, from the room log — `room.created` to the
  first event of a completed model turn (`agent.step.started`, `tool_call`, `message`, `hypothesis`), and to
  the first `agent.status working`. *Run hold*: slot to the run's first `idle`.
- *Event→delivery*: client receive time minus the event's `at` (set in `PG.Append` just before the insert
  transaction; same host clock). *Post→delivery*: receive time minus the driver's send time of the comment
  (in the comment text), i.e. including the HTTP request and any wait for `Service.mu`.
- *Insert latency*: duration of `PG.Append` (a timing wrapper around the store), which includes waiting for a
  pool connection but **not** the wait for `Service.mu`.
- *Pool*: `pgxpool.Stat` — acquires, acquires that had to wait (`EmptyAcquireCount`), total wait; sampled every
  50 ms for "all connections busy". *Memory*: `runtime.MemStats.HeapInuse` sampled every 50 ms (10 ms in the
  replay scenario) with the peak kept; "heap per replaying client" = (peak − baseline after a forced GC) / clients.
- Percentiles are nearest-rank (unit-tested); every value reported was observed.
- WebSocket clients parse frames with a fast path; every 512th frame is fully decoded and compared (0
  mismatches in every run). Viewers send presence every 20 s and reconnect with `after=` like the web UI,
  except in the replay measurements, which count a cut replay as not completed.

## 3. Exact commands

```sh
go build -o loadtest ./cmd/loadtest
./loadtest list                                   # profiles and their parameters

# the two full runs reported below (run1 17:53Z, run2 18:00Z)
./loadtest run -profile all -out run1
./loadtest run -profile all -out run2

# server-bound throughput: no model or tool latency, 10 turns per run (17:26Z and 18:09Z)
./loadtest run -profile throughput -p model_latency=0s -p tool_latency=0s -p turns=10 -out zero

# profiles of a 1,000-viewer fan-out (17:47Z), read mid-window from the server URL the driver logs
./loadtest run -profile fanout -p subs=1000 -mutex-profile 5 -out prof
curl -o mutex.pb.gz http://127.0.0.1:<port>/debug/pprof/mutex
curl -o cpu.pb.gz "http://127.0.0.1:<port>/debug/pprof/profile?seconds=10"

# pool-size experiment (18:12Z): the harness sets the pool size in code
./loadtest run -profile fanout,isolation -pool-max-conns 20 -out pool20

# CI smoke (< 30 s; ~18 s on a quiet box, 26–27 s at load average 50)
go test -tags integration -run TestSmokeAllProfiles ./internal/loadtest/
```

Every run writes `<profile>.json` (all raw numbers, per-room timelines, per-server stats) and
`<profile>.md`, plus `machine.json` and one log per server process under `logs/`. `-db-url` uses an existing
Postgres instead of a container; `-replicas N` starts N servers on one database; `-p key=value` (or
`profile.key=value`) overrides a parameter; `-short` uses the CI sizes.

## 4. Results (raw numbers)

### 4.1 Queue fairness (`fairness`)

Pipeline A opens 50 rooms (8 concurrent creators), B and C open 3 each 500 ms later. `PIPEROOM_MAX_AGENTS=3`,
model 300 ms/turn, 2 tool calls per run (50 ms each), so a run holds a slot ~1 s.

| run (load avg start→end) | pipeline | rooms | queued | TTFT p50 ms | p95 | max | queue wait p50 ms | p95 | max | max overtaken by other pipelines |
|---|---|---|---|---|---|---|---|---|---|---|
| run1 (20.7→18.3) | A | 50 | 47 | 8,996 | 18,140 | 19,265 | 8,686 | 17,828 | 18,958 | 6 |
| | B | 3 | 3 | 15,054 | 18,088 | 18,088 | 14,748 | 17,781 | 17,781 | 46 |
| | C | 3 | 3 | 14,098 | 18,133 | 18,133 | 13,777 | 17,827 | 17,827 | 47 |
| run2 (24.4→23.4) | A | 50 | 47 | 9,065 | 19,427 | 19,918 | 8,731 | 19,118 | 19,610 | 6 |
| | B | 3 | 3 | 15,616 | 18,812 | 18,812 | 15,303 | 18,483 | 18,483 | 46 |
| | C | 3 | 3 | 14,609 | 18,875 | 18,875 | 14,297 | 18,554 | 18,554 | 47 |

Run hold p50 1,054 ms (p95 1.35 s). B/C's worst wait is **16.9x (run1) / 17.6x (run2) a run hold**; a
pipeline-fair queue would bound the first B/C room's wait to about one run hold. Fake model max in-flight 3
(= the cap). The server used 0.04 cores: nothing here is a capacity limit, it is queue order.

**What broke first:** fairness, by design of the FIFO queue. In production the unit is the run hold: an Opus
investigation takes minutes, so B's first turn would wait ~16 runs' worth of time.

### 4.2 Agent throughput (`throughput`)

60 rooms opened at once, 5 model turns per run (4 tool calls), 2 WebSocket viewers per room.

| run | max_agents | turns | makespan s | turns/s | ideal turns/s | events/s | queue wait p50 ms | p95 | insert p50 ms | p95 | p99 | pool waits (total wait) | peak heap | CPU server / driver / pg (cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| run1, model 500 ms | 3 | 300 | 57.8 | 5.19 | 5.33 | 20 | 26,246 | 51,054 | 3.3 | 15.2 | 34.7 | 6,415 (27.9 s) | 16.2 MiB | 0.05 / 0.01 / 0.03 |
| | 10 | 300 | 17.3 | 17.37 | 17.74 | 65 | 5,733 | 13,955 | 2.7 | 8.8 | 22.0 | 1,814 (13.1 s) | 17.6 MiB | 0.13 / 0.03 / 0.08 |
| | 30 | 300 | 8.9 | 33.79 | 38.94 | 122 | 258 | 3,400 | 3.4 | 13.0 | 39.8 | 1,173 (24.7 s) | 20.0 MiB | 0.19 / 0.05 / 0.12 |
| run2, model 500 ms | 3 | 300 | 60.5 | 4.96 | 5.12 | 19 | 27,712 | 53,325 | 4.0 | 32.0 | 77.9 | 5,370 (40.4 s) | 16.1 MiB | 0.05 / 0.01 / 0.03 |
| | 10 | 300 | 18.9 | 15.90 | 16.96 | 59 | 6,355 | 14,548 | 3.5 | 18.0 | 44.1 | 2,435 (21.6 s) | 17.4 MiB | 0.11 / 0.03 / 0.07 |
| | 30 | 300 | 7.8 | 38.22 | 45.04 | 138 | 293 | 3,545 | 2.9 | 11.4 | 40.5 | 1,379 (10.0 s) | 20.1 MiB | 0.20 / 0.05 / 0.13 |
| zero latency, 10 turns (17:26Z, load 17) | 3 | 600 | 7.9 | 75.99 | 92.49 | 258 | 3,750 | 6,451 | 3.0 | 8.1 | 13.5 | 1,485 (12.8 s) | 19.4 MiB | — |
| | 10 | 600 | 5.5 | 108.67 | 130.36 | 366 | 2,062 | 3,986 | 2.2 | 5.6 | 8.9 | 1,202 (6.3 s) | 19.4 MiB | — |
| | 30 | 600 | 7.6 | 78.67 | 84.33 | 260 | 261 | 3,659 | 2.6 | 8.3 | 21.2 | 2,143 (14.1 s) | 19.9 MiB | — |
| zero latency, 10 turns (18:09Z, load 27) | 3 | 600 | 11.5 | 52.01 | 64.25 | 176 | 5,557 | 9,114 | 3.7 | 13.0 | 32.0 | 2,291 (14.1 s) | 18.9 MiB | 0.26 / 0.06 / 0.16 |
| | 10 | 600 | 11.3 | 53.13 | 61.79 | 179 | 4,086 | 8,144 | 3.5 | 13.4 | 37.2 | 1,743 (12.6 s) | 19.2 MiB | 0.26 / 0.06 / 0.17 |
| | 30 | 600 | 10.0 | 59.80 | 69.17 | 197 | 218 | 4,614 | 3.8 | 11.7 | 24.5 | 1,973 (10.6 s) | 20.2 MiB | 0.27 / 0.06 / 0.17 |

"Ideal" = slots × turns per run / median run hold. Pool: the 4-connection pool was fully busy in 0–12% of the
50 ms samples, but thousands of acquires had to wait (2 viewers per room poll it).

**What broke first:** with a 500 ms model the cap is the limit and throughput scales with it (87–98% of
ideal). With no model latency, raising the cap from 10 to 30 bought nothing: events/s × mean append time =
**0.94–0.97** in every zero-latency run, i.e. the appends ran back to back, one at a time. That is
`Service.mu`, held across the whole `store.Append` (section 6.1). The process's write ceiling on this box is
**~1 / mean append ≈ 180–370 events/s ≈ 50–110 model turns/s**. Real model turns take 5–30 s, so at real
latencies even 30 agents produce a few turns/s; the server is not the limit for the agent cap — cost is.

### 4.3 Room replay (`replay`)

Rooms seeded through `store.Append` with 1k, 10k and 20k events (step, tool call, 600-byte tool result,
600-byte message; ~563 B per event on the wire), then the server started.

New subscriber, replay from seq 0 (3 repeats):

| room | run1 p50 / max ms | run2 p50 / max ms | bytes |
|---|---|---|---|
| 1,002 events | 31 / 52 | 39 / 137 | 0.56 MB |
| 10,002 events | 352 / 367 | 353 / 526 | 5.64 MB |
| 20,002 events | 734 / 772 | 1,110 / 1,152 | 11.29 MB |

20 subscribers replaying the same room at once (server heap sampled every 10 ms):

| room | run | p50 ms | p95 | wall ms | heap before → peak | heap per replaying client | CPU server / driver / pg |
|---|---|---|---|---|---|---|---|
| 1k | run1 | 468 | 482 | 487 | 3.2 → 10.9 MiB | 0.4 MiB | 0.57 / 0.38 / 0.22 |
| | run2 | 617 | 643 | 644 | 3.3 → 10.1 MiB | 0.3 MiB | 0.36 / 0.20 / 0.12 |
| 10k | run1 | 4,117 | 4,194 | 4,195 | 4.4 → 16.2 MiB | 0.6 MiB | 0.65 / 0.43 / 0.14 |
| | run2 | 5,024 | 5,057 | 5,072 | 3.8 → 15.9 MiB | 0.6 MiB | 0.57 / 0.38 / 0.12 |
| 20k | run1 | 8,082 | 8,127 | 8,128 | 4.5 → 16.7 MiB | 0.6 MiB | 0.70 / 0.47 / 0.15 |
| | run2 | 9,790 | 9,823 | 9,829 | 4.8 → 15.4 MiB | 0.5 MiB | 0.59 / 0.39 / 0.13 |

Join storm: 200 subscribers dial the same room at the same moment and reconnect with `after=` when cut:

| room | run | completed | per-client p50 ms | p95 | max | replays cut at 30 s ("1008 replay timeout") | reconnects | peak heap | peak goroutines |
|---|---|---|---|---|---|---|---|---|---|
| 1k | run1 | 200 | 4,379 | 4,588 | 4,640 | 0 | 0 | 29.8 MiB | 811 |
| | run2 | 200 | 5,096 | 5,501 | 5,540 | 0 | 0 | 32.2 MiB | 811 |
| 10k | run1 | 200 | 34,837 | 35,654 | 35,695 | 200 | 200 | 35.5 MiB | 811 |
| | run2 | 200 | 57,004 | 57,277 | 57,295 | 199 (+1 "error") | 200 | 34.4 MiB | 810 |
| 20k | run1 | 200 | 50,671 | 51,031 | 51,105 | 198 (+2 "error") | 200 | 36.0 MiB | 810 |
| | run2 | 200 | 112,285 | 112,812 | 112,830 | 598 (+2 "error") | 600 | 33.5 MiB | 809 |

An exploratory run at 17:27Z without reconnects: 0 of 200 completed on the 10k and 20k rooms.

**What broke first:** the replay timeout under a join storm. Memory is not the problem (~0.5 MiB per replaying
client, ~0.1 MiB per client in a 200-client storm); aggregate replay throughput is (~30–50k events/s with 20
clients, ~35–80k with 200 on this box). Concurrent replays share it fairly, so once `clients × room size`
exceeds what the server sends in 30 s (~1–2M events here), **every** client is cut at once — a cliff, not a
slope. Each client re-reads the same pages (`Since`, 500 at a time) and re-marshals every event (section 6.4).
Clients that reconnect with `after=` keep their progress, so the storm does finish.

### 4.4 WebSocket fan-out (`fanout`)

50 quiet rooms (their agent ends at once), N viewers spread round-robin, 1 comment/s per room for 30 s (3 s
unrecorded warm-up). The commenters are closed-loop (one request at a time per room), so a slow server shows
up as fewer comments accepted.

| run | viewers | comments/s accepted / offered | deliveries/s | event→delivery p50 ms | p95 | p99 | max | post→delivery p50 | p95 | comment POST p50 | p99 | insert p50 | pool acquires/s (waits) | send-buffer-full closes | server goroutines before / peak / after close | peak heap | CPU server / driver / pg |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| run1 (18.8→22.4) | 500 | 48.4 / 50 | 484 | 4.1 | 13.2 | 33.4 | 76.8 | 4.5 | 16.8 | 2.7 | 62.0 | 2.0 | 2,017 (49,374) | 0 | 65 / 2,079 / 65 | 33.2 MiB | 0.24 / 0.06 / 0.16 |
| | 1,000 | 48.4 / 50 | 965 | 4.9 | 45.0 | 109.5 | 222.5 | 5.4 | 158.5 | 3.0 | 221.0 | 2.3 | 3,961 (113,820) | 0 | 63 / 4,088 / 63 | 59.1 MiB | 0.45 / 0.08 / 0.29 |
| | 2,000 | **31.3** / 50 | 1,247 | 49.0 | 294.5 | 454.8 | 721.3 | 1,524 | 2,231 | **1,493** | 2,701 | 19.9 | 5,792 (183,070) | 0 | 63 / 8,164 / 63 | 112.8 MiB | 0.45 / 0.08 / 0.27 |
| run2 (39.7→33.5) | 500 | 48.4 / 50 | 483 | 10.7 | 80.9 | 180.9 | 514.1 | 31.6 | 260.2 | 19.4 | 375.2 | 5.0 | 2,014 (57,482) | 0 | 63 / 2,118 / 63 | 34.0 MiB | 0.20 / 0.05 / 0.13 |
| | 1,000 | 44.9 / 50 | 896 | 20.5 | 103.8 | 210.5 | 1,051 | 197.5 | 1,694 | 184.6 | 2,141 | 10.8 | 3,751 (114,584) | 0 | 63 / 4,163 / 63 | 56.8 MiB | 0.33 / 0.07 / 0.20 |
| | 2,000 | **29.6** / 50 | 1,180 | 51.0 | 241.9 | 349.7 | 529.3 | 1,702 | 2,320 | **1,641** | 2,615 | 21.6 | 5,594 (176,814) | 0 | 63 / 8,162 / 63 | 112.3 MiB | 0.45 / 0.08 / 0.26 |
| pool = 20 (experiment, 24.6→32.0) | 500 | 48.4 / 50 | 484 | 7.1 | 40.6 | 80.2 | 526.6 | 9.9 | 140.3 | 7.3 | 401.3 | 4.7 | 2,017 (8,843) | 0 | 63 / 2,118 / 63 | 37.1 MiB | 0.17 / 0.05 / 0.16 |
| | 1,000 | 48.4 / 50 | 967 | 12.1 | 57.3 | 106.0 | 317.3 | 27.4 | 208.5 | 22.9 | 375.8 | 8.2 | 3,951 (60,548) | 0 | 63 / 4,104 / 63 | 61.9 MiB | 0.25 / 0.06 / 0.27 |
| | 2,000 | 47.9 / 50 | 1,906 | 20.8 | 84.4 | 177.6 | 548.1 | 410.8 | 1,214 | 400.7 | 1,474 | 12.7 | 7,791 (221,860) | 0 | 63 / 8,162 / 65 | 115.8 MiB | 0.42 / 0.08 / 0.49 |

Per viewer: ~4 server goroutines, ~57 KiB heap, ~1 query/s idle (the 1 s safety tick) plus ~3 per event in
its room (2,000 viewers: 50 rooms × 30 accepted events/s × 40 viewers × 3 + 2,000 ≈ 5,600, as measured). Share of
pool acquires that had to wait: 79–92% at 500 viewers, 93–97% at 1,000, **99.3–99.5% at 2,000** (9,400–10,300 s
of total wait in ~31 s: about 300 goroutines queued for a connection at any moment).

Profiles of a 1,000-viewer run (17:47Z, `-mutex-profile 5`, captured 20 s into the run): **149 s of mutex wait**
in the server, all of it on `room.Service.mu` — `Service.AddComment` 55 s, `ws.Gateway.ServeHTTP` →
`Service.GetRoom` 50 s, `Service.CreateRoom` 24 s, agent queue status (`emitQueued`) 15 s. CPU profile over 10
s: the server used 0.29 cores, 54% of it in `store.(*PG).runSub` → `Since` (the per-socket polling).

**What broke first:** the write path, at 1,000–2,000 viewers. Delivery itself held (0 drops, p99 ≤ 455 ms),
but comments slowed to 1.5–1.7 s and the server took only 60% of the offered rate. The 4-connection pool is
saturated by per-socket polling, each append waits for a connection, and it waits **holding `Service.mu`**, so
every write in the process queues behind it. A 20-connection pool (not possible with `cmd/server` today,
section 6.3) moved the 2,000-viewer case to 47.9/50 accepted and POST p50 401 ms — better, still saturated.

### 4.5 Isolation (`isolation`)

10 quiet rooms × 20 viewers, 2 comments/s per room. Phase 1 (20 s): quiet rooms only. Phase 2 (20 s): one more
room whose agent calls 8 tools per turn with no model or tool latency (2,000-byte summaries, 20 viewers), steered
every second so the 40-turn guard never parks it.

| run | phase | flood room events/s | quiet event→delivery p50 ms | p95 | p99 | post→delivery p95 | comment POST p50 | p95 | insert p50 | pool waits |
|---|---|---|---|---|---|---|---|---|---|---|
| run1 (22.4→25.2) | baseline | — | 8.0 | 41.4 | 103.6 | 53.2 | 3.8 | 40.3 | 2.7 | 26,354 |
| | flood | 61 | 17.2 | 104.2 | 295.7 | 315.7 | 36.0 | 278.0 | 7.7 | 82,304 |
| run2 (33.5→27.3) | baseline | — | 9.9 | 61.0 | 128.4 | 82.7 | 6.2 | 76.3 | 3.9 | 26,070 |
| | flood | 65 | 16.3 | 51.0 | 74.7 | 120.3 | 30.5 | 112.4 | 8.5 | 90,355 |
| exploratory (17:32Z, 33.3→33.7) | baseline | — | 12.1 | 66.7 | 105.9 | 94.6 | 6.8 | 68.7 | 5.5 | — |
| | flood | 35 | 24.1 | 80.1 | 191.3 | 510.5 | 51.5 | 407.8 | 12.6 | — |
| pool = 20 (experiment) | baseline | — | 13.3 | 50.0 | 88.7 | 56.5 | 9.1 | 50.4 | 8.1 | 3,275 |
| | flood | 40 | 18.8 | 63.0 | 108.2 | 173.8 | 58.7 | 167.9 | 10.7 | 43,639 |

Ratios flood / baseline for quiet rooms: event→delivery p95 **2.52x, 0.84x, 1.20x, 1.26x**; post→delivery p95
**5.93x, 1.45x, 5.40x, 3.08x**; comment POST p95 **6.90x, 1.47x, 5.93x, 3.33x**. The flood room itself managed only
35–65 events/s (54–58 model turns, 432–464 tool calls in 20 s): it is throttled by the same shared path.

**What broke first:** isolation of writes. Every room's appends share one lock and one small pool, so a busy
room makes everyone else's comments, steers and approvals slower. Delivery to quiet rooms is affected less
(their subscribers poll their own rooms), but the measured effect is large and noisy; no isolation number is
claimed here.

## 5. Proposed answers to the inputs

- `docs/pilot/setup.md` (`PIPEROOM_MAX_AGENTS`, ~line 106): the cap is a cost and fairness knob, not a server
  capacity knob — the server handled 30 concurrent agents; what limits it is the event write path. Suggested
  addition for the docs owner (C14): "The queue is first-in first-out across all pipelines: a burst of rooms
  from one pipeline delays every other pipeline's first agent turn by about (rooms ahead ÷ cap) × the length of
  one investigation." (Remove after S1's round-robin queue lands.)
- `deploy/helm/README.md` (one replica, ~line 114): unchanged by phase 1; see section 9 for 2–3 replicas.
- Both docs should say that the database pool is pgxpool's default, `max(4, NumCPU)` per process, and that it
  cannot be set through `PIPEROOM_DB_URL` until section 6.3 is fixed.

## 6. Bugs and issues for the owners (A5: `internal/room`, `internal/store`, `internal/ws`, `cmd/server`)

No code outside `internal/loadtest/**` and `cmd/loadtest/**` was changed.

### 6.1 `Service.mu` is held across every event append (all rooms serialize on one DB round trip)

`room.Service.emitLocked` calls `store.Append` (BEGIN, `UPDATE rooms … RETURNING`, `INSERT`, `pg_notify`,
COMMIT, plus waiting for a pool connection) while the caller holds the process-wide `Service.mu`
(`agentEmit`, `emitAs`, `AddComment`, `CreateRoom`, `emitQueued`, …). Consequences measured above: the write
ceiling is 1 / append latency for the whole process (0.94–0.97 utilization at 176–366 events/s); one slow
append (pool wait, a slow disk, a remote database) delays every room; WebSocket connects (`GetRoom` under the
same lock) wait behind writes (50 s of the 149 s mutex wait).
Reproduce: `./loadtest run -profile throughput -p model_latency=0s -p tool_latency=0s -p turns=10` (events/s ×
`event_insert_latency.mean_ms` ≈ 1) and `./loadtest run -profile fanout -p subs=1000 -mutex-profile 5`, then
`go tool pprof -top loadtest mutex.pb.gz`. Direction: a per-room lock for the room's snapshot and seq, with the
DB call outside the service-wide lock.

### 6.2 One store subscription per WebSocket (per-socket polling)

Known as follow-up 1 of `reviews/load-test-2026-10.md`; now measured on Postgres. Each socket runs `runSub`:
one `Since` on every NOTIFY for its room, a re-drain, and a 1 s safety tick — ~1 query/s idle plus ~3 per
event. 2,000 viewers made 5,600–5,800 queries/s on 4 connections; 99% of acquires waited. Reproduce:
`./loadtest run -profile fanout` (`pool_acquires_per_sec` in the JSON). S1's per-room feed
(`s1/replicas`, `internal/store/hub.go`) replaces this; section 9 measures it.

### 6.3 `pool_max_conns` in `PIPEROOM_DB_URL` breaks the server; the pool size cannot be raised

`pgx.ParseConfig` keeps `pool_*` keys as Postgres runtime parameters (only `pgxpool.ParseConfig` removes them).
`cmd/server`'s `migrateRoomDB` and the store's LISTEN hub (`hub.connect`) both call `pgx.ParseConfig` on the
pool URL, so the server sends `pool_max_conns` to Postgres at startup:

```
$ PIPEROOM_DB_URL='postgres://lt:…@127.0.0.1:55432/lt?sslmode=disable&pool_max_conns=20' ./server
level=ERROR msg="server: failed" err="server: ping for migrations: failed to connect to `user=lt database=lt`: …
  server error: FATAL: unrecognized configuration parameter \"pool_max_conns\" (SQLSTATE 42704)"
```

A store opened with such a URL (`store.New`) runs without LISTEN — `store: listen connect failed … unrecognized
configuration parameter "pool_max_conns"` every 2 s — and live delivery falls back to the 1 s safety tick:
event→delivery p50 430 ms, p95 917 ms in `./loadtest run -short -profile fanout -server-db-params
pool_max_conns=20` (vs ~5 ms). So today the pool is always `max(4, NumCPU)`. Direction: build the LISTEN and
migration connections from `pool.Config().ConnConfig` (or strip `pool_*` before `pgx.ParseConfig`), add a test
with a `pool_max_conns` URL, and document the knob.

### 6.4 Join storms on large rooms hit the 30 s replay timeout all at once

`ws.Gateway` replays each connection on its own (`Since` pages of 500, `json.Marshal` per event per client,
blocking writes), bounded by `replayTimeout` = 30 s per connection. Concurrent replays share throughput
fairly, so they all reach the timeout together (200/200 cut on a 10k-event room). Reconnecting with `after=`
finishes the job, but the room takes 35–113 s to open for everyone and the cut replays waste the work done.
Reproduce: `./loadtest run -profile replay -p sizes=10000 -p join_sizes=10000`. Directions: share replay pages
and marshalled bytes per room, cap concurrent replays per room (queue the rest), or make the timeout depend on
progress rather than total time.

### 6.5 FIFO agent queue starves small pipelines

As expected by the task: B/C waited 15–17 run holds behind A's burst (4.1). S1's round-robin queue addresses
it; section 9 measures it.

### 6.6 Minor: no `IdleTimeout` on the HTTP server

`cmd/server`'s `http.Server` sets only `ReadHeaderTimeout`, so an idle keep-alive connection holds a server
goroutine until the client closes it (seen as +20–40 goroutines after the first fan-out runs, before the
harness closed its idle connections). Ingresses usually close idle connections; low priority.

## 7. Proposed supported-scale statement (single replica, `main` today)

For the pilot docs, **to be confirmed on production-like hardware (section 8)**:

> One Piperoom replica on 4 vCPUs with the default database pool supports: up to **50 open rooms**; up to **500
> connected viewers in total** (about 20 per room); up to **25 room events per second** across all rooms;
> `PIPEROOM_MAX_AGENTS` up to 10 (the agent cap is limited by model cost, not by the server); rooms up to
> **10,000 events**, with up to **20 people opening the same 10,000-event room at the same moment**. Beyond
> these, comments and agent events slow down for every room before anything is dropped.

Headroom: each figure is about half of where something broke first on this box — the write path at
1,000–2,000 viewers and ~30–50 accepted comments/s with them, replay join storms beyond ~200 × 1k or ~20 × 20k
events within 30 s, and FIFO queue order with any burst larger than the cap. **These are floor numbers from a
shared 4-vCPU VM, not a target.** The fairness problem (4.1) is a queue-order issue, not a scale issue, and no
statement should be made about it until S1's round-robin queue lands.

## 8. Must be re-run on production-like hardware

1. A **dedicated** host (no neighbors), 4 and 8 vCPU, so CPU and run-queue delay are the server's own.
2. **Managed Postgres over the network** (RDS / Cloud SQL, 0.5–2 ms RTT). The serialized append (6.1) is
   ~5 round trips; network latency lowers the write ceiling directly. This is the most important re-run.
3. The production node's real `NumCPU` (it sets the pool size, 6.3) and real memory limits.
4. Through the real ingress (nginx, TLS, WebSocket proxying) with browser-like clients.
5. Real model latencies (seconds per turn, streaming) and the real read tools against a real stack.
6. A soak of several hours: heap growth, GC, connection churn, retention running.
7. Multi-replica (section 9) once `s1/replicas` lands, and again after 6.1–6.4 are fixed.

Use the same commands (section 3) with `-db-url` pointing at the managed database.

## 9. Multi-replica runs

Pending: second PR "[S2] loadtest: multi-replica runs" on `s2/load-test-replicas`, stacked on `s1/replicas`.
