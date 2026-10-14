# loadtest throughput (3 replicas)

Started 2026-10-13T18:56:08Z, wall 13.7 s, load average "37.25 34.09 37.14" at start and "37.46 34.27 37.15" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `max_agents=10 max_agents_global=0 model_latency=0s model_tokens=300 propose=false rooms=60 subs_per_room=2 timeout=15m tool_latency=0s tool_summary_bytes=1024 turns=10`

## Agent throughput

| max_agents | rooms | turns | makespan s | turns/s | ideal turns/s | events/s | queue wait p50 ms | p95 | max | insert p50 ms | p95 | p99 | pool (max acquired/max; saturated; waits) | peak heap | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 60 | 600 | 11.1 | 54.17 | 63.69 | 179 | 831.1 | 4486.6 | 4580.4 | 6.9 | 56.7 | 123.3 | 4/4; 4%; 187 waits 1394 ms + 4/4; 3%; 288 waits 1008 ms + 4/4; 4%; 171 waits 1476 ms | 13.1 MiB | server 0.31, driver 0.06, postgres 0.19 cores |

## Findings

- max_agents=10: 54.17 turns/s (85% of the 63.69 an ideal 30-slot pool gives at the measured turn time), insert p99 123.3 ms, queue wait p95 4487 ms, pool 4/4; 4%; 187 waits 1394 ms + 4/4; 3%; 288 waits 1008 ms + 4/4; 4%; 171 waits 1476 ms.

## Checks

| check | result | detail |
|---|---|---|
| max_agents=10: agent cap respected | PASS | max in-flight 2, limit 30 (10 per replica x 3 replicas, global 0) |
| max_agents=10: no duplicate agent runs | PASS | rooms with >1 run start 0, with concurrent model calls 0 |
| max_agents=10: every run made all its turns | PASS | model turns 600, want 600 |
| max_agents=10: viewers saw gap-free logs | PASS | gaps 0 dups 0 parse mismatches 0 |
| max_agents=10: no append errors | PASS | append errors 0 |
