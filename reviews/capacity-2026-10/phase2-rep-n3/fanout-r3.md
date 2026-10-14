# loadtest fanout (3 replicas)

Started 2026-10-13T18:53:18Z, wall 126.7 s, load average "23.49 32.20 37.30" at start and "34.53 33.07 36.95" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `pad=300 rate=1 rooms=50 subs=500,1000,2000 timeout=5m warmup=3s window=30s`

## WebSocket fan-out

| subs | rooms | comments/s accepted / offered | deliveries/s | event->delivery p50 ms | p95 | p99 | max | post->delivery p50 | p99 | cross-replica delivery p50 | p99 | POST p50 | POST p99 | insert p50 | send-buffer-full closes | reconnects | server goroutines before/peak/after | pool | peak heap | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 500 | 50 | 48.4 / 50.0 | 484 | 6.0 | 36.0 | 96.2 | 202.6 | 7.5 | 123.6 | 6.1 | 93.4 | 5.6 | 117.5 | 4.2 | 0 | 0 | 101/2268/101 | 4/4; 0%; 4 waits 6 ms + 4/4; 0%; 3 waits 5 ms + 4/4; 0%; 7 waits 9 ms; 342 q/s | 14.3 MiB | server 0.14, driver 0.05, postgres 0.07 cores |
| 1000 | 50 | 48.4 / 50.0 | 966 | 6.9 | 43.8 | 92.4 | 266.9 | 8.4 | 143.0 | 6.9 | 92.4 | 6.1 | 138.7 | 4.2 | 0 | 0 | 101/4270/101 | 4/4; 0%; 5 waits 22 ms + 3/4; 0%; 2 waits 2 ms + 4/4; 0%; 15 waits 76 ms; 342 q/s | 22.6 MiB | server 0.16, driver 0.06, postgres 0.07 cores |
| 2000 | 50 | 48.4 / 50.0 | 1934 | 10.4 | 67.1 | 131.8 | 389.5 | 14.8 | 393.4 | 10.5 | 129.6 | 11.7 | 387.7 | 7.4 | 0 | 0 | 103/8316/103 | 4/4; 0%; 3 waits 25 ms + 4/4; 0%; 6 waits 15 ms + 4/4; 0%; 10 waits 13 ms; 342 q/s | 38.6 MiB | server 0.20, driver 0.08, postgres 0.08 cores |

## Findings

- 500 subscribers over 50 rooms: the server accepted 48.4 of 50.0 comments/s offered (POST p50 6 ms, p99 117 ms); event->delivery p50 6.0 ms, p95 36.0 ms, p99 96.2 ms; 0 send-buffer-full closes, 0 reconnects; pool 4/4 busy in 0% of samples, 342 acquires/s, 4 waits; server goroutines 101 before, 101 after close; CPU server 0.14, driver 0.05, postgres 0.07 cores.
- 1000 subscribers over 50 rooms: the server accepted 48.4 of 50.0 comments/s offered (POST p50 6 ms, p99 139 ms); event->delivery p50 6.9 ms, p95 43.8 ms, p99 92.4 ms; 0 send-buffer-full closes, 0 reconnects; pool 4/4 busy in 0% of samples, 342 acquires/s, 5 waits; server goroutines 101 before, 101 after close; CPU server 0.16, driver 0.06, postgres 0.07 cores.
- 2000 subscribers over 50 rooms: the server accepted 48.4 of 50.0 comments/s offered (POST p50 12 ms, p99 388 ms); event->delivery p50 10.4 ms, p95 67.1 ms, p99 131.8 ms; 0 send-buffer-full closes, 0 reconnects; pool 4/4 busy in 0% of samples, 342 acquires/s, 3 waits; server goroutines 103 before, 103 after close; CPU server 0.20, driver 0.08, postgres 0.08 cores.

## Checks

| check | result | detail |
|---|---|---|
| subs=500: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=500: comments accepted | PASS | posted 1500, errors 0 |
| subs=500: no server goroutine leak after close | PASS | 101 before subscribers, 101 after they closed |
| subs=500: no append errors | PASS | append errors 0 |
| subs=1000: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=1000: comments accepted | PASS | posted 1500, errors 0 |
| subs=1000: no server goroutine leak after close | PASS | 101 before subscribers, 101 after they closed |
| subs=1000: no append errors | PASS | append errors 0 |
| subs=2000: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=2000: comments accepted | PASS | posted 1500, errors 0 |
| subs=2000: no server goroutine leak after close | PASS | 103 before subscribers, 103 after they closed |
| subs=2000: no append errors | PASS | append errors 0 |
