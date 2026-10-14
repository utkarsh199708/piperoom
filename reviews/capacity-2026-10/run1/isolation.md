# loadtest isolation

Started 2026-10-13T17:59:59Z, wall 49.7 s, load average "22.36 20.51 24.46" at start and "25.16 21.41 24.56" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `flood_model_latency=0s flood_subs=20 flood_summary_bytes=2000 flood_tool_latency=0s flood_tools_per_turn=8 pad=300 quiet_rooms=10 rate=2 subs_per_room=20 timeout=5m warmup=3s window=20s`

## Isolation: quiet rooms alone vs. while one room floods

| phase | flood events/s | quiet comments/s accepted / offered | quiet event->delivery p50 ms | p95 | p99 | post->delivery p50 | p95 | POST p50 | POST p95 | insert p50 | insert p95 | pool | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 0 | 19.6 / 20.0 | 8.0 | 41.4 | 103.6 | 8.6 | 53.2 | 3.8 | 40.3 | 2.7 | 29.2 | 4/4 47% (26354 waits) | server 0.15, driver 0.04, postgres 0.09 cores |
| flood | 61 | 19.6 / 20.0 | 17.2 | 104.2 | 295.7 | 46.3 | 315.7 | 36.0 | 278.0 | 7.7 | 29.1 | 4/4 72% (82304 waits) | server 0.39, driver 0.11, postgres 0.25 cores |

Quiet-room delivery p95 ratio flood/baseline: **2.52x** (+62.8 ms); post->delivery p95 ratio 5.93x; POST p95 ratio 6.90x. Flood room delivery p95 136.1 ms over 1320 events.

## Findings

- While one room appended 61 events/s, quiet-room event->delivery p95 went 41.4 -> 104.2 ms (2.52x, +62.8 ms); post->delivery p95 53.2 -> 315.7 ms (5.93x); comment POST p95 40.3 -> 278.0 ms (6.90x).

## Checks

| check | result | detail |
|---|---|---|
| every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| comments accepted in both phases | PASS | baseline 0/400 failed, flood 0/400 failed |
| the flood room really flooded | PASS | 61 events/s |
