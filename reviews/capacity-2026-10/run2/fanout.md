# loadtest fanout

Started 2026-10-13T18:06:38Z, wall 127.4 s, load average "39.65 31.16 27.48" at start and "33.53 31.89 28.23" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `pad=300 rate=1 rooms=50 subs=500,1000,2000 timeout=5m warmup=3s window=30s`

## WebSocket fan-out

| subs | rooms | comments/s accepted / offered | deliveries/s | event->delivery p50 ms | p95 | p99 | max | post->delivery p50 | p99 | POST p50 | POST p99 | insert p50 | send-buffer-full closes | reconnects | server goroutines before/peak/after | pool | peak heap | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 500 | 50 | 48.4 / 50.0 | 483 | 10.7 | 80.9 | 180.9 | 514.1 | 31.6 | 378.2 | 19.4 | 375.2 | 5.0 | 0 | 0 | 63/2118/63 | 4/4 39%, 2014 q/s, 57482 waits | 34.0 MiB | server 0.20, driver 0.05, postgres 0.13 cores |
| 1000 | 50 | 44.9 / 50.0 | 896 | 20.5 | 103.8 | 210.5 | 1051.0 | 197.5 | 2167.0 | 184.6 | 2140.9 | 10.8 | 0 | 0 | 63/4163/63 | 4/4 71%, 3751 q/s, 114584 waits | 56.8 MiB | server 0.33, driver 0.07, postgres 0.20 cores |
| 2000 | 50 | 29.6 / 50.0 | 1180 | 51.0 | 241.9 | 349.7 | 529.3 | 1701.5 | 2652.7 | 1640.6 | 2614.7 | 21.6 | 0 | 0 | 63/8162/63 | 4/4 75%, 5594 q/s, 176814 waits | 112.3 MiB | server 0.45, driver 0.08, postgres 0.26 cores |

## Findings

- 500 subscribers over 50 rooms: the server accepted 48.4 of 50.0 comments/s offered (POST p50 19 ms, p99 375 ms); event->delivery p50 10.7 ms, p95 80.9 ms, p99 180.9 ms; 0 send-buffer-full closes, 0 reconnects; pool 4/4 busy in 39% of samples, 2014 acquires/s, 57482 waits; server goroutines 63 before, 63 after close; CPU server 0.20, driver 0.05, postgres 0.13 cores.
- 1000 subscribers over 50 rooms: the server accepted 44.9 of 50.0 comments/s offered (POST p50 185 ms, p99 2141 ms); event->delivery p50 20.5 ms, p95 103.8 ms, p99 210.5 ms; 0 send-buffer-full closes, 0 reconnects; pool 4/4 busy in 71% of samples, 3751 acquires/s, 114584 waits; server goroutines 63 before, 63 after close; CPU server 0.33, driver 0.07, postgres 0.20 cores.
- 2000 subscribers over 50 rooms: the server accepted 29.6 of 50.0 comments/s offered (POST p50 1641 ms, p99 2615 ms); event->delivery p50 51.0 ms, p95 241.9 ms, p99 349.7 ms; 0 send-buffer-full closes, 0 reconnects; pool 4/4 busy in 75% of samples, 5594 acquires/s, 176814 waits; server goroutines 63 before, 63 after close; CPU server 0.45, driver 0.08, postgres 0.26 cores.

## Checks

| check | result | detail |
|---|---|---|
| subs=500: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=500: comments accepted | PASS | posted 1500, errors 0 |
| subs=500: no server goroutine leak after close | PASS | 63 before subscribers, 63 after they closed |
| subs=500: no append errors | PASS | append errors 0 |
| subs=1000: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=1000: comments accepted | PASS | posted 1407, errors 0 |
| subs=1000: no server goroutine leak after close | PASS | 63 before subscribers, 63 after they closed |
| subs=1000: no append errors | PASS | append errors 0 |
| subs=2000: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=2000: comments accepted | PASS | posted 937, errors 0 |
| subs=2000: no server goroutine leak after close | PASS | 63 before subscribers, 63 after they closed |
| subs=2000: no append errors | PASS | append errors 0 |
