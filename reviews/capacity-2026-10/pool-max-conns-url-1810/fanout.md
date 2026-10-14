# loadtest fanout (short)

Started 2026-10-13T18:10:51Z, wall 5.3 s, load average "26.98 29.51 27.79" at start and "26.90 29.45 27.78" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `pad=50 rate=4 rooms=4 subs=16 timeout=60s warmup=500ms window=1500ms`

## WebSocket fan-out

| subs | rooms | comments/s accepted / offered | deliveries/s | event->delivery p50 ms | p95 | p99 | max | post->delivery p50 | p99 | cross-replica delivery p50 | p99 | POST p50 | POST p99 | insert p50 | send-buffer-full closes | reconnects | server goroutines before/peak/after | pool | peak heap | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 16 | 4 | 14.2 / 16.0 | 46 | 429.7 | 916.5 | 1002.0 | 1002.0 | 512.1 | 1002.5 | - | - | 9.3 | 336.9 | 8.3 | 0 | 0 | 17/90/17 | 16/20 0%, 104 q/s, 1 waits | 6.0 MiB | server 0.04, driver 0.01, postgres 0.03 cores |

## Findings

- 16 subscribers over 4 rooms: the server accepted 14.2 of 16.0 comments/s offered (POST p50 9 ms, p99 337 ms); event->delivery p50 429.7 ms, p95 916.5 ms, p99 1002.0 ms; 0 send-buffer-full closes, 0 reconnects; pool 16/20 busy in 0% of samples, 104 acquires/s, 1 waits; server goroutines 17 before, 17 after close; CPU server 0.04, driver 0.01, postgres 0.03 cores.

## Checks

| check | result | detail |
|---|---|---|
| subs=16: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=16: comments accepted | PASS | posted 24, errors 0 |
| subs=16: no server goroutine leak after close | PASS | 17 before subscribers, 17 after they closed |
| subs=16: no append errors | PASS | append errors 0 |
