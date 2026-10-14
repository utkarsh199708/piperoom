# loadtest fairness

Started 2026-10-13T17:53:56Z, wall 21.9 s, load average "20.69 26.81 27.92" at start and "18.34 25.87 27.58" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `big_rooms=50 max_agents=3 model_latency=300ms model_tokens=300 small_delay=500ms small_rooms=3 timeout=10m tool_calls=2 tool_latency=50ms tool_summary_bytes=1024`

## Time to first agent turn per pipeline

| pipeline | rooms | queued | TTFT p50 ms | p95 | max | queue wait p50 ms | p95 | max | max overtaken |
|---|---|---|---|---|---|---|---|---|---|
| pa | 50 | 47 | 8996.1 | 18139.7 | 19264.8 | 8685.5 | 17828.4 | 18958.2 | 6 |
| pb | 3 | 3 | 15054.3 | 18088.1 | 18088.1 | 14747.6 | 17780.9 | 17780.9 | 46 |
| pc | 3 | 3 | 14097.9 | 18132.5 | 18132.5 | 13777.1 | 17827.0 | 17827.0 | 47 |

B/C max queue wait **17827 ms**, max TTFT **18132 ms**. Run hold (slot to idle) p50 1054 ms, p95 1352 ms; a pipeline-fair queue would bound the first B/C room's wait to about one run hold (1054 ms). Fake model max in-flight 3. CPU: server 0.04, driver 0.01, postgres 0.02 cores.

## Findings

- Pipeline A (50 rooms) p50 TTFT 8996 ms; B/C (3 rooms each) max queue wait 17827 ms and max TTFT 18132 ms, behind up to 47 rooms of other pipelines.
- FIFO starves B and C: their worst wait is 16.9x a run hold; a pipeline-fair queue would bound it to about one run hold (1054 ms).

## Checks

| check | result | detail |
|---|---|---|
| every room got a first agent turn | PASS | 0 of 56 rooms without slot or first turn |
| agent cap respected (fake model in-flight <= max_agents x replicas) | PASS | max in-flight 3, cap 3 x 1 replicas |
| no duplicate agent runs | PASS | rooms with >1 run start 0, rooms with concurrent model calls 0 |
| no append errors | PASS | append errors 0 |
