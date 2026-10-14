# loadtest fairness

Started 2026-10-13T18:00:53Z, wall 22.8 s, load average "24.42 21.32 24.51" at start and "23.37 21.28 24.41" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `big_rooms=50 max_agents=3 model_latency=300ms model_tokens=300 small_delay=500ms small_rooms=3 timeout=10m tool_calls=2 tool_latency=50ms tool_summary_bytes=1024`

## Time to first agent turn per pipeline

| pipeline | rooms | queued | TTFT p50 ms | p95 | max | queue wait p50 ms | p95 | max | max overtaken |
|---|---|---|---|---|---|---|---|---|---|
| pa | 50 | 47 | 9065.1 | 19427.2 | 19918.0 | 8730.9 | 19118.0 | 19610.3 | 6 |
| pb | 3 | 3 | 15615.7 | 18811.7 | 18811.7 | 15302.9 | 18483.4 | 18483.4 | 46 |
| pc | 3 | 3 | 14609.1 | 18875.0 | 18875.0 | 14297.2 | 18554.2 | 18554.2 | 47 |

B/C max queue wait **18554 ms**, max TTFT **18875 ms**. Run hold (slot to idle) p50 1102 ms, p95 1455 ms; a pipeline-fair queue would bound the first B/C room's wait to about one run hold (1102 ms). Fake model max in-flight 3. CPU: server 0.04, driver 0.01, postgres 0.03 cores.

## Findings

- Pipeline A (50 rooms) p50 TTFT 9065 ms; B/C (3 rooms each) max queue wait 18554 ms and max TTFT 18875 ms, behind up to 47 rooms of other pipelines.
- FIFO starves B and C: their worst wait is 16.8x a run hold; a pipeline-fair queue would bound it to about one run hold (1102 ms).

## Checks

| check | result | detail |
|---|---|---|
| every room got a first agent turn | PASS | 0 of 56 rooms without slot or first turn |
| agent cap respected (fake model in-flight <= max_agents x replicas) | PASS | max in-flight 3, cap 3 x 1 replicas |
| no duplicate agent runs | PASS | rooms with >1 run start 0, rooms with concurrent model calls 0 |
| no append errors | PASS | append errors 0 |
