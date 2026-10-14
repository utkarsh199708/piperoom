# loadtest throughput

Started 2026-10-13T18:56:28Z, wall 15.3 s, load average "38.86 34.61 37.24" at start and "42.62 35.69 37.56" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `max_agents=10 max_agents_global=0 model_latency=0s model_tokens=300 propose=false rooms=60 subs_per_room=2 timeout=15m tool_latency=0s tool_summary_bytes=1024 turns=10`

## Agent throughput

| max_agents | rooms | turns | makespan s | turns/s | ideal turns/s | events/s | queue wait p50 ms | p95 | max | insert p50 ms | p95 | p99 | pool (max acquired/max; saturated; waits) | peak heap | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 60 | 600 | 13.4 | 44.85 | 56.93 | 151 | 5335.9 | 9239.6 | 9293.4 | 3.4 | 16.0 | 49.7 | 4/4; 2%; 386 waits 1480 ms | 21.2 MiB | server 0.17, driver 0.05, postgres 0.10 cores |

## Findings

- max_agents=10: 44.85 turns/s (79% of the 56.93 an ideal 10-slot pool gives at the measured turn time), insert p99 49.7 ms, queue wait p95 9240 ms, pool 4/4; 2%; 386 waits 1480 ms.

## Checks

| check | result | detail |
|---|---|---|
| max_agents=10: agent cap respected | PASS | max in-flight 1, limit 10 (10 per replica x 1 replicas, global 0) |
| max_agents=10: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=10: every run made all its turns | PASS | model turns 600, want 600 |
| max_agents=10: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=10: no append errors | PASS | append errors 0 |
