# loadtest fanout

Started 2026-10-13T18:46:20Z, wall 133.5 s, load average "37.66 35.86 39.51" at start and "45.13 39.79 40.48" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `pad=300 rate=1 rooms=50 subs=500,1000,2000 timeout=5m warmup=3s window=30s`

## WebSocket fan-out

| subs | rooms | comments/s accepted / offered | deliveries/s | event->delivery p50 ms | p95 | p99 | max | post->delivery p50 | p99 | cross-replica delivery p50 | p99 | POST p50 | POST p99 | insert p50 | send-buffer-full closes | reconnects | server goroutines before/peak/after | pool | peak heap | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 500 | 50 | 48.4 / 50.0 | 483 | 5.3 | 38.8 | 107.6 | 212.4 | 8.5 | 246.8 | - | - | 7.0 | 243.2 | 3.4 | 0 | 0 | 69/2156/69 | 3/4; 0%; 7 waits 48 ms; 148 q/s | 30.5 MiB | server 0.08, driver 0.04, postgres 0.04 cores |
| 1000 | 50 | 48.4 / 50.0 | 967 | 6.9 | 48.5 | 119.9 | 509.6 | 15.0 | 403.8 | - | - | 12.6 | 402.3 | 4.7 | 0 | 0 | 69/4175/69 | 4/4; 0%; 10 waits 315 ms; 149 q/s | 55.2 MiB | server 0.09, driver 0.05, postgres 0.04 cores |
| 2000 | 50 | 48.4 / 50.0 | 1935 | 8.0 | 58.2 | 141.3 | 446.7 | 23.0 | 1366.3 | - | - | 18.4 | 1353.2 | 4.9 | 0 | 0 | 67/8217/67 | 4/4; 0%; 23 waits 367 ms; 149 q/s | 98.4 MiB | server 0.13, driver 0.07, postgres 0.04 cores |

## Findings

- 500 subscribers over 50 rooms: the server accepted 48.4 of 50.0 comments/s offered (POST p50 7 ms, p99 243 ms); event->delivery p50 5.3 ms, p95 38.8 ms, p99 107.6 ms; 0 send-buffer-full closes, 0 reconnects; pool 3/4 busy in 0% of samples, 148 acquires/s, 7 waits; server goroutines 69 before, 69 after close; CPU server 0.08, driver 0.04, postgres 0.04 cores.
- 1000 subscribers over 50 rooms: the server accepted 48.4 of 50.0 comments/s offered (POST p50 13 ms, p99 402 ms); event->delivery p50 6.9 ms, p95 48.5 ms, p99 119.9 ms; 0 send-buffer-full closes, 0 reconnects; pool 4/4 busy in 0% of samples, 149 acquires/s, 10 waits; server goroutines 69 before, 69 after close; CPU server 0.09, driver 0.05, postgres 0.04 cores.
- 2000 subscribers over 50 rooms: the server accepted 48.4 of 50.0 comments/s offered (POST p50 18 ms, p99 1353 ms); event->delivery p50 8.0 ms, p95 58.2 ms, p99 141.3 ms; 0 send-buffer-full closes, 0 reconnects; pool 4/4 busy in 0% of samples, 149 acquires/s, 23 waits; server goroutines 67 before, 67 after close; CPU server 0.13, driver 0.07, postgres 0.04 cores.

## Checks

| check | result | detail |
|---|---|---|
| subs=500: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=500: comments accepted | PASS | posted 1500, errors 0 |
| subs=500: no server goroutine leak after close | PASS | 69 before subscribers, 69 after they closed |
| subs=500: no append errors | PASS | append errors 0 |
| subs=1000: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=1000: comments accepted | PASS | posted 1500, errors 0 |
| subs=1000: no server goroutine leak after close | PASS | 69 before subscribers, 69 after they closed |
| subs=1000: no append errors | PASS | append errors 0 |
| subs=2000: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=2000: comments accepted | PASS | posted 1500, errors 0 |
| subs=2000: no server goroutine leak after close | PASS | 67 before subscribers, 67 after they closed |
| subs=2000: no append errors | PASS | append errors 0 |
