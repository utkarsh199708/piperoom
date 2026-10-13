# loadtest throughput (2 replicas)

Started 2026-10-13T18:48:56Z, wall 72.6 s, load average "42.93 39.63 40.41" at start and "42.39 39.50 40.26" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `max_agents=3,10,30 max_agents_global=0 model_latency=500ms model_tokens=300 propose=true rooms=60 subs_per_room=2 timeout=15m tool_latency=50ms tool_summary_bytes=1024 turns=5`

## Agent throughput

| max_agents | rooms | turns | makespan s | turns/s | ideal turns/s | events/s | queue wait p50 ms | p95 | max | insert p50 ms | p95 | p99 | pool (max acquired/max; saturated; waits) | peak heap | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 3 | 60 | 360 | 37.6 | 9.57 | 10.11 | 39 | 14972.3 | 31890.0 | 32814.8 | 4.7 | 63.7 | 136.0 | 4/4; 1%; 163 waits 324 ms + 4/4; 0%; 158 waits 210 ms | 12.8 MiB | server 0.07, driver 0.02, postgres 0.04 cores |
| 10 | 60 | 360 | 12.6 | 28.66 | 32.45 | 124 | 3958.4 | 7553.9 | 7590.2 | 4.0 | 24.1 | 76.0 | 4/4; 2%; 379 waits 1205 ms + 4/4; 0%; 204 waits 378 ms | 14.6 MiB | server 0.18, driver 0.04, postgres 0.10 cores |
| 30 | 60 | 360 | 6.1 | 59.29 | 78.31 | 232 | 183.7 | 455.0 | 602.6 | 4.0 | 20.9 | 92.3 | 4/4; 2%; 280 waits 521 ms + 4/4; 1%; 201 waits 308 ms | 14.7 MiB | server 0.28, driver 0.07, postgres 0.17 cores |

max_agents=3 approvals: 60 proposals, 60 approved (0 errors); 60 executions (0 duplicate, 0 missing); 60 action.executed events (0 duplicate). Agent runs: 0 rooms with more than one run start, 0 with concurrent model calls.

max_agents=10 approvals: 60 proposals, 60 approved (0 errors); 60 executions (0 duplicate, 0 missing); 60 action.executed events (0 duplicate). Agent runs: 0 rooms with more than one run start, 0 with concurrent model calls.

max_agents=30 approvals: 60 proposals, 60 approved (0 errors); 60 executions (0 duplicate, 0 missing); 60 action.executed events (0 duplicate). Agent runs: 0 rooms with more than one run start, 0 with concurrent model calls.

## Findings

- max_agents=3: 9.57 turns/s (95% of the 10.11 an ideal 6-slot pool gives at the measured turn time), insert p99 136.0 ms, queue wait p95 31890 ms, pool 4/4; 1%; 163 waits 324 ms + 4/4; 0%; 158 waits 210 ms.
- max_agents=10: 28.66 turns/s (88% of the 32.45 an ideal 20-slot pool gives at the measured turn time), insert p99 76.0 ms, queue wait p95 7554 ms, pool 4/4; 2%; 379 waits 1205 ms + 4/4; 0%; 204 waits 378 ms.
- max_agents=30: 59.29 turns/s (76% of the 78.31 an ideal 60-slot pool gives at the measured turn time), insert p99 92.3 ms, queue wait p95 455 ms, pool 4/4; 2%; 280 waits 521 ms + 4/4; 1%; 201 waits 308 ms.

## Checks

| check | result | detail |
|---|---|---|
| max_agents=3: agent cap respected | PASS | max in-flight 6, limit 6 (3 per replica x 2 replicas, global 0) |
| max_agents=3: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=3: every run made all its turns | PASS | model turns 360, want 360 |
| max_agents=3: every approved fix executed exactly once | PASS | proposals 60 approved 60 approve errors 0; executions 60 (duplicate 0, missing 0); action.executed events 60 (duplicate 0) |
| max_agents=3: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=3: no append errors | PASS | append errors 0 |
| max_agents=10: agent cap respected | PASS | max in-flight 20, limit 20 (10 per replica x 2 replicas, global 0) |
| max_agents=10: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=10: every run made all its turns | PASS | model turns 360, want 360 |
| max_agents=10: every approved fix executed exactly once | PASS | proposals 60 approved 60 approve errors 0; executions 60 (duplicate 0, missing 0); action.executed events 60 (duplicate 0) |
| max_agents=10: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=10: no append errors | PASS | append errors 0 |
| max_agents=30: agent cap respected | PASS | max in-flight 60, limit 60 (30 per replica x 2 replicas, global 0) |
| max_agents=30: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=30: every run made all its turns | PASS | model turns 360, want 360 |
| max_agents=30: every approved fix executed exactly once | PASS | proposals 60 approved 60 approve errors 0; executions 60 (duplicate 0, missing 0); action.executed events 60 (duplicate 0) |
| max_agents=30: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=30: no append errors | PASS | append errors 0 |
