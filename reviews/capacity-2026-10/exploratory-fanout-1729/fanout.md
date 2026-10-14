# loadtest fanout

Started 2026-10-13T17:29:28Z, wall 146.0 s, load average "25.81 18.17 11.17" at start and "31.68 23.01 13.98" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `pad=300 rate=1 rooms=50 subs=500,1000,2000 timeout=5m warmup=3s window=30s`

## WebSocket fan-out

| subs | rooms | deliveries/s | event->delivery p50 ms | p95 | p99 | max | post->delivery p50 | p99 | POST p50 | POST p99 | send-buffer-full closes | reconnects | goroutines before/peak/after | pool | peak heap |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 500 | 50 | 484 | 14.2 | 69.1 | 165.6 | 293.6 | 30.7 | 572.5 | 20.3 | 560.4 | 0 | 0 | 71/2121/93 | 4/4 51% (56880 waits) | 34.3 MiB |
| 1000 | 50 | 789 | 31.6 | 115.4 | 245.8 | 600.1 | 1072.7 | 2842.8 | 1057.8 | 2837.5 | 0 | 0 | 73/4162/113 | 4/4 74% (109360 waits) | 60.4 MiB |
| 2000 | 50 | 812 | 63.5 | 228.1 | 464.3 | 870.4 | 2230.8 | 3681.4 | 2194.6 | 3654.0 | 0 | 0 | 73/8165/115 | 4/4 78% (146088 waits) | 113.3 MiB |

## Findings

- 500 subscribers over 50 rooms: event->delivery p50 14.2 ms, p95 69.1 ms, p99 165.6 ms; 0 send-buffer-full closes, 0 reconnects; pool 4/4 busy in 51% of samples with 56880 waits; server goroutines 71 before, 93 after close.
- 1000 subscribers over 50 rooms: event->delivery p50 31.6 ms, p95 115.4 ms, p99 245.8 ms; 0 send-buffer-full closes, 0 reconnects; pool 4/4 busy in 74% of samples with 109360 waits; server goroutines 73 before, 113 after close.
- 2000 subscribers over 50 rooms: event->delivery p50 63.5 ms, p95 228.1 ms, p99 464.3 ms; 0 send-buffer-full closes, 0 reconnects; pool 4/4 busy in 78% of samples with 146088 waits; server goroutines 73 before, 115 after close.

## Checks

| check | result | detail |
|---|---|---|
| subs=500: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=500: comments accepted | PASS | posted 1500, errors 0 |
| subs=500: no server goroutine leak after close | FAIL | 71 before subscribers, 93 after they closed |
| subs=500: no append errors | PASS | append errors 0 |
| subs=1000: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=1000: comments accepted | PASS | posted 1277, errors 0 |
| subs=1000: no server goroutine leak after close | FAIL | 73 before subscribers, 113 after they closed |
| subs=1000: no append errors | PASS | append errors 0 |
| subs=2000: every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| subs=2000: comments accepted | PASS | posted 663, errors 0 |
| subs=2000: no server goroutine leak after close | FAIL | 73 before subscribers, 115 after they closed |
| subs=2000: no append errors | PASS | append errors 0 |
