# loadtest fanout

Started 2026-10-13T18:12:49Z, wall 124.2 s, load average "24.56 27.99 27.45" at start and "32.02 29.14 27.91" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `pad=300 rate=1 rooms=50 subs=500,1000,2000 timeout=5m warmup=3s window=30s`

## WebSocket fan-out

| subs | rooms | comments/s accepted / offered | deliveries/s | event->delivery p50 ms | p95 | p99 | max | post->delivery p50 | p99 | cross-replica delivery p50 | p99 | POST p50 | POST p99 | insert p50 | send-buffer-full closes | reconnects | server goroutines before/peak/after | pool | peak heap | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 500 | 50 | 48.4 / 50.0 | 484 | 7.1 | 40.6 | 80.2 | 526.6 | 9.9 | 403.3 | - | - | 7.3 | 401.3 | 4.7 | 0 | 0 | 63/2118/63 | 20/20 4%, 2017 q/s, 8843 waits | 37.1 MiB | server 0.17, driver 0.05, postgres 0.16 cores |
| 1000 | 50 | 48.4 / 50.0 | 967 | 12.1 | 57.3 | 106.0 | 317.3 | 27.4 | 379.8 | - | - | 22.9 | 375.8 | 8.2 | 0 | 0 | 63/4104/63 | 20/20 30%, 3951 q/s, 60548 waits | 61.9 MiB | server 0.25, driver 0.06, postgres 0.27 cores |
| 2000 | 50 | 47.9 / 50.0 | 1906 | 20.8 | 84.4 | 177.6 | 548.1 | 410.8 | 1478.1 | - | - | 400.7 | 1473.5 | 12.7 | 0 | 0 | 63/8162/65 | 20/20 61%, 7791 q/s, 221860 waits | 115.8 MiB | server 0.42, driver 0.08, postgres 0.49 cores |

## Findings

- 500 subscribers over 50 rooms: the server accepted 48.4 of 50.0 comments/s offered (POST p50 7 ms, p99 401 ms); event->delivery p50 7.1 ms, p95 40.6 ms, p99 80.2 ms; 0 send-buffer-full closes, 0 reconnects; pool 20/20 busy in 4% of samples, 2017 acquires/s, 8843 waits; server goroutines 63 before, 63 after close; CPU server 0.17, driver 0.05, postgres 0.16 cores.
- 1000 subscribers over 50 rooms: the server accepted 48.4 of 50.0 comments/s offered (POST p50 23 ms, p99 376 ms); event->delivery p50 12.1 ms, p95 57.3 ms, p99 106.0 ms; 0 send-buffer-full closes, 0 reconnects; pool 20/20 busy in 30% of samples, 3951 acquires/s, 60548 waits; server goroutines 63 before, 63 after close; CPU server 0.25, driver 0.06, postgres 0.27 cores.
- 2000 subscribers over 50 rooms: the server accepted 47.9 of 50.0 comments/s offered (POST p50 401 ms, p99 1473 ms); event->delivery p50 20.8 ms, p95 84.4 ms, p99 177.6 ms; 0 send-buffer-full closes, 0 reconnects; pool 20/20 busy in 61% of samples, 7791 acquires/s, 221860 waits; server goroutines 63 before, 65 after close; CPU server 0.42, driver 0.08, postgres 0.49 cores.

## Checks

| check | result | detail |
|---|---|---|
| subs=500: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=500: comments accepted | PASS | posted 1500, errors 0 |
| subs=500: no server goroutine leak after close | PASS | 63 before subscribers, 63 after they closed |
| subs=500: no append errors | PASS | append errors 0 |
| subs=1000: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=1000: comments accepted | PASS | posted 1500, errors 0 |
| subs=1000: no server goroutine leak after close | PASS | 63 before subscribers, 63 after they closed |
| subs=1000: no append errors | PASS | append errors 0 |
| subs=2000: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=2000: comments accepted | PASS | posted 1475, errors 0 |
| subs=2000: no server goroutine leak after close | PASS | 63 before subscribers, 65 after they closed |
| subs=2000: no append errors | PASS | append errors 0 |
