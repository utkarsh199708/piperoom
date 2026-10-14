# loadtest throughput

Started 2026-10-13T17:54:18Z, wall 86.6 s, load average "18.34 25.87 27.58" at start and "15.20 23.01 26.43" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `max_agents=3,10,30 model_latency=500ms model_tokens=300 rooms=60 subs_per_room=2 timeout=15m tool_latency=50ms tool_summary_bytes=1024 turns=5`

## Agent throughput

| max_agents | rooms | turns | makespan s | turns/s | ideal turns/s | events/s | queue wait p50 ms | p95 | max | insert p50 ms | p95 | p99 | pool (max acquired/max; saturated; waits) | peak heap | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 3 | 60 | 300 | 57.8 | 5.19 | 5.33 | 20 | 26246.1 | 51054.2 | 53778.1 | 3.3 | 15.2 | 34.7 | 4/4; 0%; 6415 waits 27922 ms | 16.2 MiB | server 0.05, driver 0.01, postgres 0.03 cores |
| 10 | 60 | 300 | 17.3 | 17.37 | 17.74 | 65 | 5733.2 | 13955.3 | 14012.2 | 2.7 | 8.8 | 22.0 | 4/4; 2%; 1814 waits 13073 ms | 17.6 MiB | server 0.13, driver 0.03, postgres 0.08 cores |
| 30 | 60 | 300 | 8.9 | 33.79 | 38.94 | 122 | 258.1 | 3400.2 | 3581.6 | 3.4 | 13.0 | 39.8 | 4/4; 6%; 1173 waits 24734 ms | 20.0 MiB | server 0.19, driver 0.05, postgres 0.12 cores |

## Findings

- max_agents=3: 5.19 turns/s (97% of the 5.33 an ideal 3-slot pool gives at the measured turn time), insert p99 34.7 ms, queue wait p95 51054 ms, pool 4/4; 0%; 6415 waits 27922 ms.
- max_agents=10: 17.37 turns/s (98% of the 17.74 an ideal 10-slot pool gives at the measured turn time), insert p99 22.0 ms, queue wait p95 13955 ms, pool 4/4; 2%; 1814 waits 13073 ms.
- max_agents=30: 33.79 turns/s (87% of the 38.94 an ideal 30-slot pool gives at the measured turn time), insert p99 39.8 ms, queue wait p95 3400 ms, pool 4/4; 6%; 1173 waits 24734 ms.

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
