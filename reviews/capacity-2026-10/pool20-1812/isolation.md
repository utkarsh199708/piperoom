# loadtest isolation

Started 2026-10-13T18:14:53Z, wall 51.0 s, load average "32.02 29.14 27.91" at start and "55.42 35.78 30.22" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `flood_model_latency=0s flood_subs=20 flood_summary_bytes=2000 flood_tool_latency=0s flood_tools_per_turn=8 pad=300 quiet_rooms=10 rate=2 subs_per_room=20 timeout=5m warmup=3s window=20s`

## Isolation: quiet rooms alone vs. while one room floods

| phase | flood events/s | quiet comments/s accepted / offered | quiet event->delivery p50 ms | p95 | p99 | post->delivery p50 | p95 | POST p50 | POST p95 | insert p50 | insert p95 | pool | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 0 | 19.6 / 20.0 | 13.3 | 50.0 | 88.7 | 14.2 | 56.5 | 9.1 | 50.4 | 8.1 | 39.8 | 20/20 7% (3275 waits) | server 0.11, driver 0.03, postgres 0.12 cores |
| flood | 40 | 19.6 / 20.0 | 18.8 | 63.0 | 108.2 | 62.3 | 173.8 | 58.7 | 167.9 | 10.7 | 44.9 | 20/20 36% (43639 waits) | server 0.26, driver 0.06, postgres 0.26 cores |

Quiet-room delivery p95 ratio flood/baseline: **1.26x** (+12.9 ms); post->delivery p95 ratio 3.07x; POST p95 ratio 3.33x. Flood room delivery p95 54.0 ms over 878 events.

## Findings

- While one room appended 40 events/s, quiet-room event->delivery p95 went 50.0 -> 63.0 ms (1.26x, +12.9 ms); post->delivery p95 56.5 -> 173.8 ms (3.07x); comment POST p95 50.4 -> 167.9 ms (3.33x).

## Checks

| check | result | detail |
|---|---|---|
| every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| comments accepted in both phases | PASS | baseline 0/400 failed, flood 0/400 failed |
| the flood room really flooded | PASS | 40 events/s |
