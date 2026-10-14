# loadtest fanout

Started 2026-10-13T17:47:41Z, wall 42.9 s, load average "34.05 41.52 31.49" at start and "26.61 38.68 30.96" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `pad=300 rate=1 rooms=50 subs=1000 timeout=5m warmup=3s window=30s`

## WebSocket fan-out

| subs | rooms | comments/s accepted / offered | deliveries/s | event->delivery p50 ms | p95 | p99 | max | post->delivery p50 | p99 | POST p50 | POST p99 | insert p50 | send-buffer-full closes | reconnects | server goroutines before/peak/after | pool | peak heap | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 50 | 48.3 / 50.0 | 962 | 13.8 | 74.4 | 152.9 | 303.4 | 23.3 | 1191.1 | 14.0 | 1177.5 | 6.4 | 0 | 0 | 63/4162/63 | 4/4 49%, 3933 q/s, 117323 waits | 57.7 MiB | server 0.36, driver 0.08, postgres 0.21 cores |

## Findings

- 1000 subscribers over 50 rooms: the server accepted 48.3 of 50.0 comments/s offered (POST p50 14 ms, p99 1178 ms); event->delivery p50 13.8 ms, p95 74.4 ms, p99 152.9 ms; 0 send-buffer-full closes, 0 reconnects; pool 4/4 busy in 49% of samples, 3933 acquires/s, 117323 waits; server goroutines 63 before, 63 after close; CPU server 0.36, driver 0.08, postgres 0.21 cores.

## Checks

| check | result | detail |
|---|---|---|
| subs=1000: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=1000: comments accepted | PASS | posted 1497, errors 0 |
| subs=1000: no server goroutine leak after close | PASS | 63 before subscribers, 63 after they closed |
| subs=1000: no append errors | PASS | append errors 0 |
