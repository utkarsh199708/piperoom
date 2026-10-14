# loadtest replay

Started 2026-10-13T17:55:45Z, wall 133.1 s, load average "15.20 23.01 26.43" at start and "18.83 21.45 25.37" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `concurrent=20 event_bytes=600 join=200 join_sizes=1000,10000,20000 repeats=3 sizes=1000,10000,20000 timeout=5m`

## Replay from seq 0, one new subscriber

| room size | events | replay p50 ms | max ms | bytes | bytes/event | failed |
|---|---|---|---|---|---|---|
| 1000 | 1002 | 31.0 | 52.3 | 0.56 MB | 560 | 0 |
| 10000 | 10002 | 351.6 | 366.9 | 5.64 MB | 563 | 0 |
| 20000 | 20002 | 733.5 | 772.0 | 11.29 MB | 564 | 0 |

## Concurrent replays (server memory per replaying client)

| room size | clients | completed | p50 ms | p95 | max | wall ms | heap before | peak heap | heap/client | peak goroutines | pool | server closes | reconnects | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 20 | 20 | 467.7 | 482.3 | 486.2 | 487 | 3.2 MiB | 10.9 MiB | 0.4 MiB | 91 | 4/4 52% | map[] | 0 | server 0.57, driver 0.38, postgres 0.22 cores |
| 10000 | 20 | 20 | 4117.2 | 4194.1 | 4194.6 | 4195 | 4.4 MiB | 16.2 MiB | 0.6 MiB | 92 | 4/4 65% | map[] | 0 | server 0.65, driver 0.43, postgres 0.14 cores |
| 20000 | 20 | 20 | 8082.1 | 8127.3 | 8127.4 | 8128 | 4.5 MiB | 16.7 MiB | 0.6 MiB | 92 | 4/4 80% | map[] | 0 | server 0.70, driver 0.47, postgres 0.15 cores |

## Join storm (all subscribers dial at once; they reconnect with after= like the web UI)

| room size | clients | completed | p50 ms | p95 | max | wall ms | heap before | peak heap | heap/client | peak goroutines | pool | server closes | reconnects | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 200 | 200 | 4378.8 | 4588.1 | 4639.6 | 4653 | 4.1 MiB | 29.8 MiB | 0.1 MiB | 811 | 4/4 60% | map[] | 0 | server 0.63, driver 0.40, postgres 0.14 cores |
| 10000 | 200 | 200 | 34836.7 | 35653.9 | 35695.4 | 35733 | 9.7 MiB | 35.5 MiB | 0.1 MiB | 811 | 4/4 51% | map[1008 replay timeout:200] | 200 | server 0.76, driver 0.48, postgres 0.18 cores |
| 20000 | 200 | 200 | 50670.6 | 51030.6 | 51105.1 | 51114 | 12.4 MiB | 36.0 MiB | 0.1 MiB | 810 | 4/4 44% | map[1008 replay timeout:198 error:2] | 200 | server 1.01, driver 0.61, postgres 0.24 cores |

## Findings

- Join storm on a 10002-event room: 200 replays were cut by the gateway's 30 s replay timeout (internal/ws replayTimeout) and resumed with after=; 200 reconnects in total.
- Join storm on a 20002-event room: 198 replays were cut by the gateway's 30 s replay timeout (internal/ws replayTimeout) and resumed with after=; 200 reconnects in total.
- New subscriber, 1002-event room: replay p50 31 ms, 0.6 MB on the wire (560 B/event).
- New subscriber, 10002-event room: replay p50 352 ms, 5.6 MB on the wire (563 B/event).
- New subscriber, 20002-event room: replay p50 733 ms, 11.3 MB on the wire (564 B/event).
- 20 clients replaying a 1002-event room at once: about 0.4 MiB of server heap per replaying client (peak 10.9 MiB over a 3.2 MiB baseline), p95 482 ms each.
- 20 clients replaying a 10002-event room at once: about 0.6 MiB of server heap per replaying client (peak 16.2 MiB over a 4.4 MiB baseline), p95 4194 ms each.
- 20 clients replaying a 20002-event room at once: about 0.6 MiB of server heap per replaying client (peak 16.7 MiB over a 4.5 MiB baseline), p95 8127 ms each.
- Join storm of 200 on a 1002-event room: p50 4379 ms, p95 4588 ms, max 4640 ms per client, wall 4653 ms, peak heap 29.8 MiB, pool 4/4 busy in 60% of samples.
- Join storm of 200 on a 10002-event room: p50 34837 ms, p95 35654 ms, max 35695 ms per client, wall 35733 ms, peak heap 35.5 MiB, pool 4/4 busy in 51% of samples.
- Join storm of 200 on a 20002-event room: p50 50671 ms, p95 51031 ms, max 51105 ms per client, wall 51114 ms, peak heap 36.0 MiB, pool 4/4 busy in 44% of samples.

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
