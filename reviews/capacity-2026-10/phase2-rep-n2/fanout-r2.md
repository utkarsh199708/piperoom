# loadtest fanout (2 replicas)

Started 2026-10-13T18:50:09Z, wall 125.7 s, load average "42.39 39.50 40.26" at start and "28.83 35.20 38.58" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `pad=300 rate=1 rooms=50 subs=500,1000,2000 timeout=5m warmup=3s window=30s`

## WebSocket fan-out

| subs | rooms | comments/s accepted / offered | deliveries/s | event->delivery p50 ms | p95 | p99 | max | post->delivery p50 | p99 | cross-replica delivery p50 | p99 | POST p50 | POST p99 | insert p50 | send-buffer-full closes | reconnects | server goroutines before/peak/after | pool | peak heap | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 500 | 50 | 48.4 / 50.0 | 484 | 4.9 | 33.4 | 106.1 | 316.4 | 6.7 | 191.6 | 5.1 | 98.2 | 5.1 | 178.4 | 3.3 | 0 | 0 | 84/2165/84 | 4/4; 0%; 0 waits 0 ms + 3/4; 0%; 0 waits 0 ms; 197 q/s | 18.4 MiB | server 0.10, driver 0.04, postgres 0.05 cores |
| 1000 | 50 | 48.4 / 50.0 | 966 | 5.3 | 29.1 | 82.0 | 164.8 | 6.7 | 150.0 | 6.1 | 81.9 | 4.8 | 145.2 | 3.1 | 0 | 0 | 86/4155/86 | 3/4; 0%; 0 waits 0 ms + 3/4; 0%; 0 waits 0 ms; 197 q/s | 29.8 MiB | server 0.13, driver 0.05, postgres 0.05 cores |
| 2000 | 50 | 48.4 / 50.0 | 1934 | 7.7 | 49.3 | 109.9 | 229.3 | 9.6 | 246.2 | 7.6 | 136.5 | 7.1 | 246.3 | 4.4 | 0 | 0 | 84/8172/84 | 3/4; 0%; 0 waits 0 ms + 4/4; 0%; 1 waits 1 ms; 197 q/s | 54.4 MiB | server 0.16, driver 0.07, postgres 0.06 cores |

## Findings

- 500 subscribers over 50 rooms: the server accepted 48.4 of 50.0 comments/s offered (POST p50 5 ms, p99 178 ms); event->delivery p50 4.9 ms, p95 33.4 ms, p99 106.1 ms; 0 send-buffer-full closes, 0 reconnects; pool 4/4 busy in 0% of samples, 197 acquires/s, 0 waits; server goroutines 84 before, 84 after close; CPU server 0.10, driver 0.04, postgres 0.05 cores.
- 1000 subscribers over 50 rooms: the server accepted 48.4 of 50.0 comments/s offered (POST p50 5 ms, p99 145 ms); event->delivery p50 5.3 ms, p95 29.1 ms, p99 82.0 ms; 0 send-buffer-full closes, 0 reconnects; pool 3/4 busy in 0% of samples, 197 acquires/s, 0 waits; server goroutines 86 before, 86 after close; CPU server 0.13, driver 0.05, postgres 0.05 cores.
- 2000 subscribers over 50 rooms: the server accepted 48.4 of 50.0 comments/s offered (POST p50 7 ms, p99 246 ms); event->delivery p50 7.7 ms, p95 49.3 ms, p99 109.9 ms; 0 send-buffer-full closes, 0 reconnects; pool 3/4 busy in 0% of samples, 197 acquires/s, 0 waits; server goroutines 84 before, 84 after close; CPU server 0.16, driver 0.07, postgres 0.06 cores.

## Checks

| check | result | detail |
|---|---|---|
| subs=500: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=500: comments accepted | PASS | posted 1500, errors 0 |
| subs=500: no server goroutine leak after close | PASS | 84 before subscribers, 84 after they closed |
| subs=500: no append errors | PASS | append errors 0 |
| subs=1000: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=1000: comments accepted | PASS | posted 1500, errors 0 |
| subs=1000: no server goroutine leak after close | PASS | 86 before subscribers, 86 after they closed |
| subs=1000: no append errors | PASS | append errors 0 |
| subs=2000: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=2000: comments accepted | PASS | posted 1500, errors 0 |
| subs=2000: no server goroutine leak after close | PASS | 84 before subscribers, 84 after they closed |
| subs=2000: no append errors | PASS | append errors 0 |
