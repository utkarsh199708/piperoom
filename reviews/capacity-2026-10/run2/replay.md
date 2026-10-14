# loadtest replay

Started 2026-10-13T18:02:48Z, wall 230.0 s, load average "22.40 21.40 24.15" at start and "39.65 31.16 27.48" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `concurrent=20 event_bytes=600 join=200 join_sizes=1000,10000,20000 repeats=3 sizes=1000,10000,20000 timeout=5m`

## Replay from seq 0, one new subscriber

| room size | events | replay p50 ms | max ms | bytes | bytes/event | failed |
|---|---|---|---|---|---|---|
| 1000 | 1002 | 38.6 | 137.1 | 0.56 MB | 561 | 0 |
| 10000 | 10002 | 353.1 | 526.0 | 5.64 MB | 563 | 0 |
| 20000 | 20002 | 1109.5 | 1151.9 | 11.29 MB | 564 | 0 |

## Concurrent replays (server memory per replaying client)

| room size | clients | completed | p50 ms | p95 | max | wall ms | heap before | peak heap | heap/client | peak goroutines | pool | server closes | reconnects | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 20 | 20 | 617.0 | 642.8 | 642.9 | 644 | 3.3 MiB | 10.1 MiB | 0.3 MiB | 91 | 4/4 32% | map[] | 0 | server 0.36, driver 0.20, postgres 0.12 cores |
| 10000 | 20 | 20 | 5023.5 | 5056.8 | 5070.9 | 5072 | 3.8 MiB | 15.9 MiB | 0.6 MiB | 91 | 4/4 72% | map[] | 0 | server 0.57, driver 0.38, postgres 0.12 cores |
| 20000 | 20 | 20 | 9790.3 | 9823.2 | 9828.6 | 9829 | 4.8 MiB | 15.4 MiB | 0.5 MiB | 91 | 4/4 76% | map[] | 0 | server 0.59, driver 0.39, postgres 0.13 cores |

## Join storm (all subscribers dial at once; they reconnect with after= like the web UI)

| room size | clients | completed | p50 ms | p95 | max | wall ms | heap before | peak heap | heap/client | peak goroutines | pool | server closes | reconnects | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 200 | 200 | 5095.8 | 5501.1 | 5539.5 | 5542 | 4.8 MiB | 32.2 MiB | 0.1 MiB | 811 | 4/4 61% | map[] | 0 | server 0.51, driver 0.32, postgres 0.12 cores |
| 10000 | 200 | 200 | 57003.5 | 57276.9 | 57295.3 | 57301 | 11.6 MiB | 34.4 MiB | 0.1 MiB | 810 | 4/4 76% | map[1008 replay timeout:199 error:1] | 200 | server 0.48, driver 0.30, postgres 0.12 cores |
| 20000 | 200 | 200 | 112284.7 | 112811.8 | 112829.7 | 112837 | 6.3 MiB | 33.5 MiB | 0.1 MiB | 809 | 4/4 78% | map[1008 replay timeout:598 error:2] | 600 | server 0.50, driver 0.31, postgres 0.12 cores |

## Findings

- Join storm on a 10002-event room: 199 replays were cut by the gateway's 30 s replay timeout (internal/ws replayTimeout) and resumed with after=; 200 reconnects in total.
- Join storm on a 20002-event room: 598 replays were cut by the gateway's 30 s replay timeout (internal/ws replayTimeout) and resumed with after=; 600 reconnects in total.
- New subscriber, 1002-event room: replay p50 39 ms, 0.6 MB on the wire (561 B/event).
- New subscriber, 10002-event room: replay p50 353 ms, 5.6 MB on the wire (563 B/event).
- New subscriber, 20002-event room: replay p50 1110 ms, 11.3 MB on the wire (564 B/event).
- 20 clients replaying a 1002-event room at once: about 0.3 MiB of server heap per replaying client (peak 10.1 MiB over a 3.3 MiB baseline), p95 643 ms each.
- 20 clients replaying a 10002-event room at once: about 0.6 MiB of server heap per replaying client (peak 15.9 MiB over a 3.8 MiB baseline), p95 5057 ms each.
- 20 clients replaying a 20002-event room at once: about 0.5 MiB of server heap per replaying client (peak 15.4 MiB over a 4.8 MiB baseline), p95 9823 ms each.
- Join storm of 200 on a 1002-event room: p50 5096 ms, p95 5501 ms, max 5539 ms per client, wall 5542 ms, peak heap 32.2 MiB, pool 4/4 busy in 61% of samples.
- Join storm of 200 on a 10002-event room: p50 57004 ms, p95 57277 ms, max 57295 ms per client, wall 57301 ms, peak heap 34.4 MiB, pool 4/4 busy in 76% of samples.
- Join storm of 200 on a 20002-event room: p50 112285 ms, p95 112812 ms, max 112830 ms per client, wall 112837 ms, peak heap 33.5 MiB, pool 4/4 busy in 78% of samples.

## Checks

| check | result | detail |
|---|---|---|
| size 1000: single replays complete | PASS | 0 of 3 failed |
| size 10000: single replays complete | PASS | 0 of 3 failed |
| size 20000: single replays complete | PASS | 0 of 3 failed |
| size 1000: 20 concurrent replays complete and gap-free | PASS | completed 20, gaps+dups+mismatches 0, closes map[] |
| size 10000: 20 concurrent replays complete and gap-free | PASS | completed 20, gaps+dups+mismatches 0, closes map[] |
| size 20000: 20 concurrent replays complete and gap-free | PASS | completed 20, gaps+dups+mismatches 0, closes map[] |
| size 1000: join storm of 200 is gap-free | PASS | gaps+dups+mismatches 0 (completed 200 of 200) |
| size 10000: join storm of 200 is gap-free | PASS | gaps+dups+mismatches 0 (completed 200 of 200) |
| size 20000: join storm of 200 is gap-free | PASS | gaps+dups+mismatches 0 (completed 200 of 200) |
