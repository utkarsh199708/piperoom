# loadtest replay

Started 2026-10-13T17:27:12Z, wall 113.0 s, load average "13.53 12.78 8.61" at start and "28.25 18.11 11.00" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `concurrent=20 event_bytes=600 join=200 join_sizes=1000,10000,20000 repeats=3 sizes=1000,10000,20000 timeout=5m`

## Replay from seq 0, one new subscriber

| room size | events | replay p50 ms | max ms | bytes | bytes/event | failed |
|---|---|---|---|---|---|---|
| 1000 | 1002 | 38.5 | 39.9 | 0.56 MB | 560 | 0 |
| 10000 | 10002 | 581.8 | 675.3 | 5.64 MB | 563 | 0 |
| 20000 | 20002 | 1019.2 | 1050.3 | 11.29 MB | 564 | 0 |

## Concurrent replays (server memory per replaying client)

| room size | clients | completed | p50 ms | p95 | max | wall ms | heap before | peak heap | heap/client | peak goroutines | pool | server closes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 20 | 20 | 506.7 | 542.9 | 577.0 | 579 | 3.5 MiB | 9.0 MiB | 0.3 MiB | 88 | 4/4 78% | map[] |
| 10000 | 20 | 20 | 5649.7 | 5699.2 | 5710.2 | 5714 | 4.4 MiB | 16.8 MiB | 0.6 MiB | 88 | 4/4 77% | map[] |
| 20000 | 20 | 20 | 12361.4 | 12664.1 | 12680.1 | 12681 | 4.6 MiB | 13.8 MiB | 0.5 MiB | 84 | 4/4 89% | map[] |

## Join storm (all subscribers dial at once)

| room size | clients | completed | p50 ms | p95 | max | wall ms | heap before | peak heap | heap/client | peak goroutines | pool | server closes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 200 | 200 | 5375.5 | 5602.1 | 5646.8 | 5652 | 4.5 MiB | 30.0 MiB | 0.1 MiB | 421 | 4/4 64% | map[] |
| 10000 | 200 | 0 | 0.0 | 0.0 | 0.0 | 30107 | 7.5 MiB | 30.7 MiB | 0.1 MiB | 264 | 4/4 67% | map[1008 replay timeout:199 error:1] |
| 20000 | 200 | 0 | 0.0 | 0.0 | 0.0 | 30051 | 5.8 MiB | 34.5 MiB | 0.1 MiB | 217 | 4/4 72% | map[1008 replay timeout:200] |

## Findings

- Join storm on a 10002-event room: only 0 of 200 subscribers finished the replay; server closes map[1008 replay timeout:199 error:1] (replay timeout is 30 s in internal/ws).
- Join storm on a 20002-event room: only 0 of 200 subscribers finished the replay; server closes map[1008 replay timeout:200] (replay timeout is 30 s in internal/ws).
- New subscriber, 1002-event room: replay p50 38 ms, 0.6 MB on the wire (560 B/event).
- New subscriber, 10002-event room: replay p50 582 ms, 5.6 MB on the wire (563 B/event).
- New subscriber, 20002-event room: replay p50 1019 ms, 11.3 MB on the wire (564 B/event).
- 20 clients replaying a 1002-event room at once: about 0.3 MiB of server heap per replaying client (peak 9.0 MiB over a 3.5 MiB baseline), p95 543 ms each.
- 20 clients replaying a 10002-event room at once: about 0.6 MiB of server heap per replaying client (peak 16.8 MiB over a 4.4 MiB baseline), p95 5699 ms each.
- 20 clients replaying a 20002-event room at once: about 0.5 MiB of server heap per replaying client (peak 13.8 MiB over a 4.6 MiB baseline), p95 12664 ms each.
- Join storm of 200 on a 1002-event room: p50 5375 ms, p95 5602 ms, max 5647 ms per client, wall 5652 ms, peak heap 30.0 MiB, pool 4/4 busy in 64% of samples.
- Join storm of 200 on a 10002-event room: p50 0 ms, p95 0 ms, max 0 ms per client, wall 30107 ms, peak heap 30.7 MiB, pool 4/4 busy in 67% of samples.
- Join storm of 200 on a 20002-event room: p50 0 ms, p95 0 ms, max 0 ms per client, wall 30051 ms, peak heap 34.5 MiB, pool 4/4 busy in 72% of samples.

## Checks

| check | result | detail |
|---|---|---|
| size 1000: single replays complete | PASS | 0 of 3 failed |
| size 10000: single replays complete | PASS | 0 of 3 failed |
| size 20000: single replays complete | PASS | 0 of 3 failed |
| size 1000: 20 concurrent replays complete and gap-free | PASS | completed 20, gaps+dups+mismatches 0, closes map[] |
| size 10000: 20 concurrent replays complete and gap-free | PASS | completed 20, gaps+dups+mismatches 0, closes map[] |
| size 20000: 20 concurrent replays complete and gap-free | PASS | completed 20, gaps+dups+mismatches 0, closes map[] |
| size 1000: join storm of 200 completes | PASS | completed 200 of 200, closes map[], dial errors 0 |
| size 10000: join storm of 200 completes | FAIL | completed 0 of 200, closes map[1008 replay timeout:199 error:1], dial errors 0 |
| size 20000: join storm of 200 completes | FAIL | completed 0 of 200, closes map[1008 replay timeout:200], dial errors 0 |
