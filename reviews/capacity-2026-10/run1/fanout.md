# loadtest fanout

Started 2026-10-13T17:57:58Z, wall 120.7 s, load average "18.83 21.45 25.37" at start and "22.36 20.51 24.46" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `pad=300 rate=1 rooms=50 subs=500,1000,2000 timeout=5m warmup=3s window=30s`

## WebSocket fan-out

| subs | rooms | comments/s accepted / offered | deliveries/s | event->delivery p50 ms | p95 | p99 | max | post->delivery p50 | p99 | POST p50 | POST p99 | insert p50 | send-buffer-full closes | reconnects | server goroutines before/peak/after | pool | peak heap | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 500 | 50 | 48.4 / 50.0 | 484 | 4.1 | 13.2 | 33.4 | 76.8 | 4.5 | 67.9 | 2.7 | 62.0 | 2.0 | 0 | 0 | 65/2079/65 | 4/4 17%, 2017 q/s, 49374 waits | 33.2 MiB | server 0.24, driver 0.06, postgres 0.16 cores |
| 1000 | 50 | 48.4 / 50.0 | 965 | 4.9 | 45.0 | 109.5 | 222.5 | 5.4 | 229.4 | 3.0 | 221.0 | 2.3 | 0 | 0 | 63/4088/63 | 4/4 31%, 3961 q/s, 113820 waits | 59.1 MiB | server 0.45, driver 0.08, postgres 0.29 cores |
| 2000 | 50 | 31.3 / 50.0 | 1247 | 49.0 | 294.5 | 454.8 | 721.3 | 1523.5 | 2713.9 | 1493.3 | 2701.0 | 19.9 | 0 | 0 | 63/8164/63 | 4/4 73%, 5792 q/s, 183070 waits | 112.8 MiB | server 0.45, driver 0.08, postgres 0.27 cores |

## Findings

- 500 subscribers over 50 rooms: the server accepted 48.4 of 50.0 comments/s offered (POST p50 3 ms, p99 62 ms); event->delivery p50 4.1 ms, p95 13.2 ms, p99 33.4 ms; 0 send-buffer-full closes, 0 reconnects; pool 4/4 busy in 17% of samples, 2017 acquires/s, 49374 waits; server goroutines 65 before, 65 after close; CPU server 0.24, driver 0.06, postgres 0.16 cores.
- 1000 subscribers over 50 rooms: the server accepted 48.4 of 50.0 comments/s offered (POST p50 3 ms, p99 221 ms); event->delivery p50 4.9 ms, p95 45.0 ms, p99 109.5 ms; 0 send-buffer-full closes, 0 reconnects; pool 4/4 busy in 31% of samples, 3961 acquires/s, 113820 waits; server goroutines 63 before, 63 after close; CPU server 0.45, driver 0.08, postgres 0.29 cores.
- 2000 subscribers over 50 rooms: the server accepted 31.3 of 50.0 comments/s offered (POST p50 1493 ms, p99 2701 ms); event->delivery p50 49.0 ms, p95 294.5 ms, p99 454.8 ms; 0 send-buffer-full closes, 0 reconnects; pool 4/4 busy in 73% of samples, 5792 acquires/s, 183070 waits; server goroutines 63 before, 63 after close; CPU server 0.45, driver 0.08, postgres 0.27 cores.

## Checks

| check | result | detail |
|---|---|---|
| subs=500: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=500: comments accepted | PASS | posted 1500, errors 0 |
| subs=500: no server goroutine leak after close | PASS | 65 before subscribers, 65 after they closed |
| subs=500: no append errors | PASS | append errors 0 |
| subs=1000: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=1000: comments accepted | PASS | posted 1500, errors 0 |
| subs=1000: no server goroutine leak after close | PASS | 63 before subscribers, 63 after they closed |
| subs=1000: no append errors | PASS | append errors 0 |
| subs=2000: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=2000: comments accepted | PASS | posted 992, errors 0 |
| subs=2000: no server goroutine leak after close | PASS | 63 before subscribers, 63 after they closed |
| subs=2000: no append errors | PASS | append errors 0 |
