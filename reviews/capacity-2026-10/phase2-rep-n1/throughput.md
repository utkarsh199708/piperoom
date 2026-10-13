# loadtest throughput

Started 2026-10-13T18:44:19Z, wall 121.5 s, load average "24.27 35.37 39.93" at start and "37.66 35.86 39.51" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `max_agents=3,10,30 max_agents_global=0 model_latency=500ms model_tokens=300 propose=true rooms=60 subs_per_room=2 timeout=15m tool_latency=50ms tool_summary_bytes=1024 turns=5`

## Agent throughput

| max_agents | rooms | turns | makespan s | turns/s | ideal turns/s | events/s | queue wait p50 ms | p95 | max | insert p50 ms | p95 | p99 | pool (max acquired/max; saturated; waits) | peak heap | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 3 | 60 | 360 | 70.4 | 5.12 | 5.24 | 21 | 31983.9 | 62312.0 | 65699.4 | 4.6 | 31.7 | 77.8 | 4/4; 0%; 384 waits 360 ms | 17.8 MiB | server 0.03, driver 0.01, postgres 0.02 cores |
| 10 | 60 | 360 | 22.9 | 15.70 | 16.84 | 70 | 7564.0 | 17400.9 | 17444.3 | 3.9 | 19.4 | 55.8 | 4/4; 0%; 407 waits 326 ms | 19.3 MiB | server 0.08, driver 0.02, postgres 0.05 cores |
| 30 | 60 | 360 | 9.8 | 36.58 | 41.34 | 164 | 391.2 | 4289.8 | 4304.4 | 4.3 | 14.5 | 33.0 | 4/4; 2%; 388 waits 533 ms | 21.1 MiB | server 0.16, driver 0.05, postgres 0.10 cores |

max_agents=3 approvals: 60 proposals, 60 approved (0 errors); 60 executions (0 duplicate, 0 missing); 60 action.executed events (0 duplicate). Agent runs: 0 rooms with more than one run start, 0 with concurrent model calls.

max_agents=10 approvals: 60 proposals, 60 approved (0 errors); 60 executions (0 duplicate, 0 missing); 60 action.executed events (0 duplicate). Agent runs: 0 rooms with more than one run start, 0 with concurrent model calls.

max_agents=30 approvals: 60 proposals, 60 approved (0 errors); 60 executions (0 duplicate, 0 missing); 60 action.executed events (0 duplicate). Agent runs: 0 rooms with more than one run start, 0 with concurrent model calls.

## Findings

- max_agents=3: 5.12 turns/s (98% of the 5.24 an ideal 3-slot pool gives at the measured turn time), insert p99 77.8 ms, queue wait p95 62312 ms, pool 4/4; 0%; 384 waits 360 ms.
- max_agents=10: 15.70 turns/s (93% of the 16.84 an ideal 10-slot pool gives at the measured turn time), insert p99 55.8 ms, queue wait p95 17401 ms, pool 4/4; 0%; 407 waits 326 ms.
- max_agents=30: 36.58 turns/s (88% of the 41.34 an ideal 30-slot pool gives at the measured turn time), insert p99 33.0 ms, queue wait p95 4290 ms, pool 4/4; 2%; 388 waits 533 ms.

## Checks

| check | result | detail |
|---|---|---|
| max_agents=3: agent cap respected | PASS | max in-flight 3, limit 3 (3 per replica x 1 replicas, global 0) |
| max_agents=3: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=3: every run made all its turns | PASS | model turns 360, want 360 |
| max_agents=3: every approved fix executed exactly once | PASS | proposals 60 approved 60 approve errors 0; executions 60 (duplicate 0, missing 0); action.executed events 60 (duplicate 0) |
| max_agents=3: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=3: no append errors | PASS | append errors 0 |
| max_agents=10: agent cap respected | PASS | max in-flight 10, limit 10 (10 per replica x 1 replicas, global 0) |
| max_agents=10: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=10: every run made all its turns | PASS | model turns 360, want 360 |
| max_agents=10: every approved fix executed exactly once | PASS | proposals 60 approved 60 approve errors 0; executions 60 (duplicate 0, missing 0); action.executed events 60 (duplicate 0) |
| max_agents=10: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=10: no append errors | PASS | append errors 0 |
| max_agents=30: agent cap respected | PASS | max in-flight 30, limit 30 (30 per replica x 1 replicas, global 0) |
| max_agents=30: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=30: every run made all its turns | PASS | model turns 360, want 360 |
| max_agents=30: every approved fix executed exactly once | PASS | proposals 60 approved 60 approve errors 0; executions 60 (duplicate 0, missing 0); action.executed events 60 (duplicate 0) |
| max_agents=30: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=30: no append errors | PASS | append errors 0 |
