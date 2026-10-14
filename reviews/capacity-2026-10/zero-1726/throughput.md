# loadtest throughput

Started 2026-10-13T17:26:36Z, wall 22.2 s, load average "17.22 13.21 8.58" at start and "16.34 13.25 8.69" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `max_agents=3,10,30 model_latency=0s model_tokens=300 rooms=60 subs_per_room=2 timeout=15m tool_latency=0s tool_summary_bytes=1024 turns=10`

## Agent throughput

| max_agents | rooms | turns | makespan s | turns/s | ideal turns/s | events/s | queue wait p50 ms | p95 | max | insert p50 ms | p95 | p99 | pool (max acquired/max; saturated; waits) | peak heap |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 3 | 60 | 600 | 7.9 | 75.99 | 92.49 | 258 | 3749.8 | 6451.0 | 6664.3 | 3.0 | 8.1 | 13.5 | 4/4; 3%; 1485 waits 12844 ms | 19.4 MiB |
| 10 | 60 | 600 | 5.5 | 108.67 | 130.36 | 366 | 2061.7 | 3986.4 | 3998.8 | 2.2 | 5.6 | 8.9 | 4/4; 12%; 1202 waits 6283 ms | 19.4 MiB |
| 30 | 60 | 600 | 7.6 | 78.67 | 84.33 | 260 | 260.9 | 3659.4 | 3697.3 | 2.6 | 8.3 | 21.2 | 4/4; 9%; 2143 waits 14054 ms | 19.9 MiB |

## Findings

- max_agents=3: 75.99 turns/s (82% of the 92.49 an ideal 3-slot pool gives at the measured turn time), insert p99 13.5 ms, queue wait p95 6451 ms, pool 4/4; 3%; 1485 waits 12844 ms.
- max_agents=10: 108.67 turns/s (83% of the 130.36 an ideal 10-slot pool gives at the measured turn time), insert p99 8.9 ms, queue wait p95 3986 ms, pool 4/4; 12%; 1202 waits 6283 ms.
- max_agents=30: 78.67 turns/s (93% of the 84.33 an ideal 30-slot pool gives at the measured turn time), insert p99 21.2 ms, queue wait p95 3659 ms, pool 4/4; 9%; 2143 waits 14054 ms.
- Raising max_agents 10 -> 30 gained less than 20% turns/s: the server, not the cap, limits throughput here.

## Checks

| check | result | detail |
|---|---|---|
| max_agents=3: agent cap respected | PASS | max in-flight 1, cap 3 x 1 replicas |
| max_agents=3: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=3: every run made all its turns | PASS | model turns 600, want 600 |
| max_agents=3: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=3: no append errors | PASS | append errors 0 |
| max_agents=10: agent cap respected | PASS | max in-flight 2, cap 10 x 1 replicas |
| max_agents=10: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=10: every run made all its turns | PASS | model turns 600, want 600 |
| max_agents=10: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=10: no append errors | PASS | append errors 0 |
| max_agents=30: agent cap respected | PASS | max in-flight 2, cap 30 x 1 replicas |
| max_agents=30: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=30: every run made all its turns | PASS | model turns 600, want 600 |
| max_agents=30: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=30: no append errors | PASS | append errors 0 |
