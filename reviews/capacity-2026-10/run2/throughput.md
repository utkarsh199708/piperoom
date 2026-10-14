# loadtest throughput

Started 2026-10-13T18:01:16Z, wall 91.8 s, load average "23.37 21.28 24.41" at start and "22.40 21.40 24.15" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `max_agents=3,10,30 model_latency=500ms model_tokens=300 rooms=60 subs_per_room=2 timeout=15m tool_latency=50ms tool_summary_bytes=1024 turns=5`

## Agent throughput

| max_agents | rooms | turns | makespan s | turns/s | ideal turns/s | events/s | queue wait p50 ms | p95 | max | insert p50 ms | p95 | p99 | pool (max acquired/max; saturated; waits) | peak heap | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 3 | 60 | 300 | 60.5 | 4.96 | 5.12 | 19 | 27712.2 | 53325.1 | 56233.6 | 4.0 | 32.0 | 77.9 | 4/4; 2%; 5370 waits 40411 ms | 16.1 MiB | server 0.05, driver 0.01, postgres 0.03 cores |
| 10 | 60 | 300 | 18.9 | 15.90 | 16.96 | 59 | 6354.7 | 14547.8 | 14649.9 | 3.5 | 18.0 | 44.1 | 4/4; 5%; 2435 waits 21562 ms | 17.4 MiB | server 0.11, driver 0.03, postgres 0.07 cores |
| 30 | 60 | 300 | 7.8 | 38.22 | 45.04 | 138 | 293.2 | 3544.8 | 3574.1 | 2.9 | 11.4 | 40.5 | 4/4; 5%; 1379 waits 10045 ms | 20.1 MiB | server 0.20, driver 0.05, postgres 0.13 cores |

## Findings

- max_agents=3: 4.96 turns/s (97% of the 5.12 an ideal 3-slot pool gives at the measured turn time), insert p99 77.9 ms, queue wait p95 53325 ms, pool 4/4; 2%; 5370 waits 40411 ms.
- max_agents=10: 15.90 turns/s (94% of the 16.96 an ideal 10-slot pool gives at the measured turn time), insert p99 44.1 ms, queue wait p95 14548 ms, pool 4/4; 5%; 2435 waits 21562 ms.
- max_agents=30: 38.22 turns/s (85% of the 45.04 an ideal 30-slot pool gives at the measured turn time), insert p99 40.5 ms, queue wait p95 3545 ms, pool 4/4; 5%; 1379 waits 10045 ms.

## Checks

| check | result | detail |
|---|---|---|
| max_agents=3: agent cap respected | PASS | max in-flight 3, cap 3 x 1 replicas |
| max_agents=3: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=3: every run made all its turns | PASS | model turns 300, want 300 |
| max_agents=3: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=3: no append errors | PASS | append errors 0 |
| max_agents=10: agent cap respected | PASS | max in-flight 10, cap 10 x 1 replicas |
| max_agents=10: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=10: every run made all its turns | PASS | model turns 300, want 300 |
| max_agents=10: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=10: no append errors | PASS | append errors 0 |
| max_agents=30: agent cap respected | PASS | max in-flight 30, cap 30 x 1 replicas |
| max_agents=30: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=30: every run made all its turns | PASS | model turns 300, want 300 |
| max_agents=30: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=30: no append errors | PASS | append errors 0 |
