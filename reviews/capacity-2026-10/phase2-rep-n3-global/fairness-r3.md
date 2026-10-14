# loadtest fairness (3 replicas)

Started 2026-10-13T18:55:34Z, wall 25.7 s, load average "36.63 33.56 37.07" at start and "37.53 34.09 37.16" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `big_rooms=50 max_agents=3 max_agents_global=3 model_latency=300ms model_tokens=300 small_delay=500ms small_rooms=3 timeout=10m tool_calls=2 tool_latency=50ms tool_summary_bytes=1024`

## Time to first agent turn per pipeline

| pipeline | rooms | queued | TTFT p50 ms | p95 | max | queue wait p50 ms | p95 | max | max overtaken |
|---|---|---|---|---|---|---|---|---|---|
| pa | 50 | 47 | 11896.3 | 22070.4 | 23039.4 | 11579.3 | 21755.2 | 22719.9 | 6 |
| pb | 3 | 3 | 2610.5 | 3585.0 | 3585.0 | 2278.6 | 3276.6 | 3276.6 | 5 |
| pc | 3 | 3 | 3526.9 | 3732.5 | 3732.5 | 3218.9 | 3411.0 | 3411.0 | 6 |

B/C max queue wait **3411 ms**, max TTFT **3733 ms**. Run hold (slot to idle) p50 1114 ms, p95 1305 ms; a pipeline-fair queue would bound the first B/C room's wait to about one run hold (1114 ms). Fake model max in-flight 3. CPU: server 0.07, driver 0.01, postgres 0.04 cores.

## Findings

- Pipeline A (50 rooms) p50 TTFT 11896 ms; B/C (3 rooms each) max queue wait 3411 ms and max TTFT 3733 ms, behind up to 6 rooms of other pipelines.
- FIFO starves B and C: their worst wait is 3.1x a run hold; a pipeline-fair queue would bound it to about one run hold (1114 ms).

## Checks

| check | result | detail |
|---|---|---|
| every room got a first agent turn | PASS | 0 of 56 rooms without slot or first turn |
| agent cap respected (fake model in-flight <= limit) | PASS | max in-flight 3, limit 3 (3 per replica x 3 replicas, global 3) |
| no duplicate agent runs | PASS | rooms with >1 run start 0, rooms with concurrent model calls 0 |
| no append errors | PASS | append errors 0 |
