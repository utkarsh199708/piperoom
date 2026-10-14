# loadtest isolation

Started 2026-10-13T17:32:28Z, wall 49.0 s, load average "33.34 24.27 14.73" at start and "33.72 25.93 15.81" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `flood_model_latency=0s flood_subs=20 flood_summary_bytes=2000 flood_tool_latency=0s flood_tools_per_turn=8 pad=300 quiet_rooms=10 rate=2 subs_per_room=20 timeout=5m warmup=3s window=20s`

## Isolation: quiet rooms alone vs. while one room floods

| phase | flood events/s | quiet event->delivery p50 ms | p95 | p99 | post->delivery p50 | p95 | POST p50 | POST p95 | insert p50 | insert p95 | pool |
|---|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 0 | 12.1 | 66.7 | 105.9 | 13.0 | 94.6 | 6.8 | 68.7 | 5.5 | 39.0 | 4/4 61% |
| flood | 35 | 24.1 | 80.1 | 191.3 | 64.6 | 510.5 | 51.5 | 407.8 | 12.6 | 45.4 | 4/4 67% |

Quiet-room delivery p95 ratio flood/baseline: **1.20x** (+13.4 ms); post->delivery p95 ratio 5.40x; POST p95 ratio 5.93x. Flood room delivery p95 68.1 ms over 764 events.

## Findings

- While one room appended 35 events/s, quiet-room event->delivery p95 went 66.7 -> 80.1 ms (1.20x, +13.4 ms); post->delivery p95 94.6 -> 510.5 ms (5.40x); comment POST p95 68.7 -> 407.8 ms (5.93x).

## Checks

| check | result | detail |
|---|---|---|
| every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| comments accepted in both phases | PASS | baseline 0/400 failed, flood 0/400 failed |
| the flood room really flooded | PASS | 35 events/s |
