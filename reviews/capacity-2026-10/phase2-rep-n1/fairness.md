# loadtest fairness

Started 2026-10-13T18:43:56Z, wall 23.1 s, load average "25.13 36.50 40.41" at start and "24.27 35.37 39.93" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `big_rooms=50 max_agents=3 max_agents_global=0 model_latency=300ms model_tokens=300 small_delay=500ms small_rooms=3 timeout=10m tool_calls=2 tool_latency=50ms tool_summary_bytes=1024`

## Time to first agent turn per pipeline

| pipeline | rooms | queued | TTFT p50 ms | p95 | max | queue wait p50 ms | p95 | max | max overtaken |
|---|---|---|---|---|---|---|---|---|---|
| pa | 50 | 47 | 11271.5 | 18891.1 | 19796.0 | 10916.3 | 18587.1 | 19464.5 | 6 |
| pb | 3 | 3 | 2599.3 | 3516.3 | 3516.3 | 2277.1 | 3193.9 | 3193.9 | 5 |
| pc | 3 | 3 | 2612.9 | 3547.8 | 3547.8 | 2295.9 | 3196.4 | 3196.4 | 6 |

B/C max queue wait **3196 ms**, max TTFT **3548 ms**. Run hold (slot to idle) p50 1090 ms, p95 1378 ms; a pipeline-fair queue would bound the first B/C room's wait to about one run hold (1090 ms). Fake model max in-flight 3. CPU: server 0.04, driver 0.01, postgres 0.03 cores.

## Findings

- Pipeline A (50 rooms) p50 TTFT 11272 ms; B/C (3 rooms each) max queue wait 3196 ms and max TTFT 3548 ms, behind up to 6 rooms of other pipelines.

## Checks

| check | result | detail |
|---|---|---|
| every room got a first agent turn | PASS | 0 of 56 rooms without slot or first turn |
| agent cap respected (fake model in-flight <= limit) | PASS | max in-flight 3, limit 3 (3 per replica x 1 replicas, global 0) |
| no duplicate agent runs | PASS | rooms with >1 run start 0, rooms with concurrent model calls 0 |
| no append errors | PASS | append errors 0 |
