# loadtest isolation

Started 2026-10-13T18:08:45Z, wall 49.4 s, load average "33.53 31.89 28.23" at start and "27.26 30.40 27.91" at end. Machine: 4 CPUs (Intel(R) Xeon(R) Processor @ 2.10GHz), 16479424 kB, kernel 6.18.44-fc-v114, go1.27.0, Postgres 16.15 (Debian 16.15-1.pgdg13+2). shared box: other agents' builds and tests ran at the same time; numbers are a floor, not a target.

Parameters: `flood_model_latency=0s flood_subs=20 flood_summary_bytes=2000 flood_tool_latency=0s flood_tools_per_turn=8 pad=300 quiet_rooms=10 rate=2 subs_per_room=20 timeout=5m warmup=3s window=20s`

## Isolation: quiet rooms alone vs. while one room floods

| phase | flood events/s | quiet comments/s accepted / offered | quiet event->delivery p50 ms | p95 | p99 | post->delivery p50 | p95 | POST p50 | POST p95 | insert p50 | insert p95 | pool | CPU (avg cores) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 0 | 19.5 / 20.0 | 9.9 | 61.0 | 128.4 | 11.2 | 82.7 | 6.2 | 76.3 | 3.9 | 45.9 | 4/4 13% (26070 waits) | server 0.14, driver 0.03, postgres 0.09 cores |
| flood | 65 | 19.6 / 20.0 | 16.3 | 51.0 | 74.7 | 39.1 | 120.3 | 30.5 | 112.4 | 8.5 | 26.9 | 4/4 72% (90355 waits) | server 0.44, driver 0.12, postgres 0.26 cores |

Quiet-room delivery p95 ratio flood/baseline: **0.84x** (+-10.0 ms); post->delivery p95 ratio 1.45x; POST p95 ratio 1.47x. Flood room delivery p95 43.5 ms over 1410 events.

## Findings

- While one room appended 65 events/s, quiet-room event->delivery p95 went 61.0 -> 51.0 ms (0.84x, +-10.0 ms); post->delivery p95 82.7 -> 120.3 ms (1.45x); comment POST p95 76.3 -> 112.4 ms (1.47x).

## Checks

| check | result | detail |
|---|---|---|
| every subscriber saw every event once, in order | PASS | gaps 0 dups 0 missing-at-end 0 parse mismatches 0 |
| comments accepted in both phases | PASS | baseline 0/400 failed, flood 0/400 failed |
| the flood room really flooded | PASS | 65 events/s |
