# loadtest throughput (3 replicas)

Started 2026-10-13T18:52:27Z, wall 51.1 s, load average "26.80 34.56 38.34" at start and "23.49 32.20 37.30" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `max_agents=3,10,30 max_agents_global=0 model_latency=500ms model_tokens=300 propose=true rooms=60 subs_per_room=2 timeout=15m tool_latency=50ms tool_summary_bytes=1024 turns=5`

## Agent throughput

| max_agents | rooms | turns | makespan s | turns/s | ideal turns/s | events/s | queue wait p50 ms | p95 | max | insert p50 ms | p95 | p99 | pool (max acquired/max; saturated; waits) | peak heap | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 3 | 60 | 360 | 24.3 | 14.84 | 16.03 | 63 | 10150.7 | 20314.9 | 20376.2 | 4.1 | 22.5 | 55.3 | 4/4; 1%; 149 waits 458 ms + 4/4; 0%; 155 waits 604 ms + 4/4; 1%; 101 waits 285 ms | 10.6 MiB | server 0.13, driver 0.03, postgres 0.07 cores |
| 10 | 60 | 360 | 7.6 | 47.44 | 51.44 | 198 | 118.3 | 3523.4 | 3537.4 | 4.2 | 19.1 | 54.7 | 4/4; 1%; 175 waits 602 ms + 4/4; 1%; 244 waits 262 ms + 4/4; 1%; 164 waits 378 ms | 11.7 MiB | server 0.28, driver 0.06, postgres 0.18 cores |
| 30 | 60 | 360 | 6.6 | 54.81 | 75.58 | 213 | 239.4 | 979.6 | 1013.0 | 5.6 | 41.3 | 124.0 | 4/4; 2%; 130 waits 1111 ms + 4/4; 4%; 120 waits 465 ms + 4/4; 4%; 171 waits 1158 ms | 12.5 MiB | server 0.30, driver 0.06, postgres 0.18 cores |

max_agents=3 approvals: 60 proposals, 60 approved (0 errors); 60 executions (0 duplicate, 0 missing); 60 action.executed events (0 duplicate). Agent runs: 0 rooms with more than one run start, 0 with concurrent model calls.

max_agents=10 approvals: 60 proposals, 60 approved (0 errors); 60 executions (0 duplicate, 0 missing); 60 action.executed events (0 duplicate). Agent runs: 0 rooms with more than one run start, 0 with concurrent model calls.

max_agents=30 approvals: 60 proposals, 60 approved (0 errors); 60 executions (0 duplicate, 0 missing); 60 action.executed events (0 duplicate). Agent runs: 0 rooms with more than one run start, 0 with concurrent model calls.

## Findings

- max_agents=3: 14.84 turns/s (93% of the 16.03 an ideal 9-slot pool gives at the measured turn time), insert p99 55.3 ms, queue wait p95 20315 ms, pool 4/4; 1%; 149 waits 458 ms + 4/4; 0%; 155 waits 604 ms + 4/4; 1%; 101 waits 285 ms.
- max_agents=10: 47.44 turns/s (92% of the 51.44 an ideal 30-slot pool gives at the measured turn time), insert p99 54.7 ms, queue wait p95 3523 ms, pool 4/4; 1%; 175 waits 602 ms + 4/4; 1%; 244 waits 262 ms + 4/4; 1%; 164 waits 378 ms.
- max_agents=30: 54.81 turns/s (73% of the 75.58 an ideal 60-slot pool gives at the measured turn time), insert p99 124.0 ms, queue wait p95 980 ms, pool 4/4; 2%; 130 waits 1111 ms + 4/4; 4%; 120 waits 465 ms + 4/4; 4%; 171 waits 1158 ms.
- Raising max_agents 10 -> 30 gained less than 20% turns/s: the server, not the cap, limits throughput here.

## Checks

| check | result | detail |
|---|---|---|
| max_agents=3: agent cap respected | PASS | max in-flight 9, limit 9 (3 per replica x 3 replicas, global 0) |
| max_agents=3: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=3: every run made all its turns | PASS | model turns 360, want 360 |
| max_agents=3: every approved fix executed exactly once | PASS | proposals 60 approved 60 approve errors 0; executions 60 (duplicate 0, missing 0); action.executed events 60 (duplicate 0) |
| max_agents=3: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=3: no append errors | PASS | append errors 0 |
| max_agents=10: agent cap respected | PASS | max in-flight 30, limit 30 (10 per replica x 3 replicas, global 0) |
| max_agents=10: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=10: every run made all its turns | PASS | model turns 360, want 360 |
| max_agents=10: every approved fix executed exactly once | PASS | proposals 60 approved 60 approve errors 0; executions 60 (duplicate 0, missing 0); action.executed events 60 (duplicate 0) |
| max_agents=10: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=10: no append errors | PASS | append errors 0 |
| max_agents=30: agent cap respected | PASS | max in-flight 60, limit 90 (30 per replica x 3 replicas, global 0) |
| max_agents=30: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=30: every run made all its turns | PASS | model turns 360, want 360 |
| max_agents=30: every approved fix executed exactly once | PASS | proposals 60 approved 60 approve errors 0; executions 60 (duplicate 0, missing 0); action.executed events 60 (duplicate 0) |
| max_agents=30: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=30: no append errors | PASS | append errors 0 |
