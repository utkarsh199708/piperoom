# loadtest throughput

Started 2026-10-13T18:09:43Z, wall 36.8 s, load average "27.17 30.28 27.90" at start and "26.63 29.76 27.81" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `max_agents=3,10,30 model_latency=0s model_tokens=300 rooms=60 subs_per_room=2 timeout=15m tool_latency=0s tool_summary_bytes=1024 turns=10`

## Agent throughput

| max_agents | rooms | turns | makespan s | turns/s | ideal turns/s | events/s | queue wait p50 ms | p95 | max | insert p50 ms | p95 | p99 | pool (max acquired/max; saturated; waits) | peak heap | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 3 | 60 | 600 | 11.5 | 52.01 | 64.25 | 176 | 5557.2 | 9113.8 | 9503.2 | 3.7 | 13.0 | 32.0 | 4/4; 10%; 2291 waits 14075 ms | 18.9 MiB | server 0.26, driver 0.06, postgres 0.16 cores |
| 10 | 60 | 600 | 11.3 | 53.13 | 61.79 | 179 | 4086.2 | 8144.2 | 8167.7 | 3.5 | 13.4 | 37.2 | 4/4; 6%; 1743 waits 12624 ms | 19.2 MiB | server 0.26, driver 0.06, postgres 0.17 cores |
| 30 | 60 | 600 | 10.0 | 59.80 | 69.17 | 197 | 217.5 | 4614.1 | 4657.7 | 3.8 | 11.7 | 24.5 | 4/4; 9%; 1973 waits 10571 ms | 20.2 MiB | server 0.27, driver 0.06, postgres 0.17 cores |

## Findings

- max_agents=3: 52.01 turns/s (81% of the 64.25 an ideal 3-slot pool gives at the measured turn time), insert p99 32.0 ms, queue wait p95 9114 ms, pool 4/4; 10%; 2291 waits 14075 ms.
- max_agents=10: 53.13 turns/s (86% of the 61.79 an ideal 10-slot pool gives at the measured turn time), insert p99 37.2 ms, queue wait p95 8144 ms, pool 4/4; 6%; 1743 waits 12624 ms.
- Raising max_agents 3 -> 10 gained less than 20% turns/s: the server, not the cap, limits throughput here.
- max_agents=30: 59.80 turns/s (86% of the 69.17 an ideal 30-slot pool gives at the measured turn time), insert p99 24.5 ms, queue wait p95 4614 ms, pool 4/4; 9%; 1973 waits 10571 ms.
- Raising max_agents 10 -> 30 gained less than 20% turns/s: the server, not the cap, limits throughput here.

## Checks

| check | result | detail |
|---|---|---|
| max_agents=3: agent cap respected | PASS | max in-flight 2, cap 3 x 1 replicas |
| max_agents=3: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=3: every run made all its turns | PASS | model turns 600, want 600 |
| max_agents=3: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=3: no append errors | PASS | append errors 0 |
| max_agents=10: agent cap respected | PASS | max in-flight 3, cap 10 x 1 replicas |
| max_agents=10: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=10: every run made all its turns | PASS | model turns 600, want 600 |
| max_agents=10: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=10: no append errors | PASS | append errors 0 |
| max_agents=30: agent cap respected | PASS | max in-flight 1, cap 30 x 1 replicas |
| max_agents=30: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=30: every run made all its turns | PASS | model turns 600, want 600 |
| max_agents=30: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=30: no append errors | PASS | append errors 0 |
