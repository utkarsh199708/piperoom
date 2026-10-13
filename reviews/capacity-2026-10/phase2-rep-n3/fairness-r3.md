# loadtest fairness (3 replicas)

Started 2026-10-13T18:52:19Z, wall 8.4 s, load average "27.57 34.84 38.45" at start and "26.80 34.56 38.34" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `big_rooms=50 max_agents=3 max_agents_global=0 model_latency=300ms model_tokens=300 small_delay=500ms small_rooms=3 timeout=10m tool_calls=2 tool_latency=50ms tool_summary_bytes=1024`

## Time to first agent turn per pipeline

| pipeline | rooms | queued | TTFT p50 ms | p95 | max | queue wait p50 ms | p95 | max | max overtaken |
|---|---|---|---|---|---|---|---|---|---|
| pa | 50 | 41 | 3367.4 | 5499.0 | 6360.9 | 3052.6 | 5188.8 | 6046.0 | 6 |
| pb | 3 | 3 | 1001.0 | 1034.7 | 1034.7 | 691.3 | 726.0 | 726.0 | 6 |
| pc | 3 | 3 | 962.1 | 995.9 | 995.9 | 652.8 | 689.2 | 689.2 | 4 |

B/C max queue wait **726 ms**, max TTFT **1035 ms**. Run hold (slot to idle) p50 1045 ms, p95 1076 ms; a pipeline-fair queue would bound the first B/C room's wait to about one run hold (1045 ms). Fake model max in-flight 9. CPU: server 0.16, driver 0.02, postgres 0.09 cores.

## Findings

- Pipeline A (50 rooms) p50 TTFT 3367 ms; B/C (3 rooms each) max queue wait 726 ms and max TTFT 1035 ms, behind up to 6 rooms of other pipelines.

## Checks

| check | result | detail |
|---|---|---|
| every room got a first agent turn | PASS | 0 of 56 rooms without slot or first turn |
| agent cap respected (fake model in-flight <= limit) | PASS | max in-flight 9, limit 9 (3 per replica x 3 replicas, global 0) |
| no duplicate agent runs | PASS | rooms with >1 run start 0, rooms with concurrent model calls 0 |
| no append errors | PASS | append errors 0 |
