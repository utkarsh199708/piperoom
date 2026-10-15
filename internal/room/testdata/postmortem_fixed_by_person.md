# Post-mortem: Incident

Status: closed

| | |
|---|---|
| Severity | sev2 |
| Pipeline | orders |
| Opened (UTC) | 2026-10-11 10:00:00 |
| Closed (UTC) | 2026-10-11 10:05:30 |
| Duration | 5 min 30 s |
| Closed by | Arjun |
| Resolution | Fixed by Kabir: consumer restarted, lag back to zero |
| Room id | `ROOM` |

## Timeline

| time (UTC) | +mm:ss | what |
|---|---|---|
| 10:00:00 | +00:00 | room opened (opened by hand) |
| 10:02:30 | +02:30 | hypothesis (high): order-sync is stuck on a poison record |
| 10:03:00 | +03:00 | hand-off requested by Arjun to Priya |
| 10:03:30 | +03:30 | hand-off failed (summary\_failed) |
| 10:04:00 | +04:00 | hand-off requested by Arjun to Priya |
| 10:04:30 | +04:30 | hand-off `h_1` created: Arjun to Priya |
| 10:05:00 | +05:00 | Kabir logged a fix note: restarted the order-sync consumer by hand; lag is draining |
| 10:05:30 | +05:30 | room closed by Arjun |

## Root cause

Final hypothesis (high confidence): order-sync is stuck on a poison record

Why: the consumer is stuck on a poison record

Resolution: Fixed by Kabir: consumer restarted, lag back to zero

## What was done

### Fix notes (done outside Piperoom)

- 10:05:00 · Kabir: restarted the order-sync consumer by hand; lag is draining

## Verification

No agent verification ran: the fix was made outside Piperoom, per the fix notes and the resolution.

## Still open

none recorded after the fix

Earlier, from the hand-off at 10:04 (before the fix: fix note at 10:05), possibly resolved since:
- Restart the consumer

## Risks

none recorded after the fix

Earlier, from the hand-off at 10:04 (before the fix: fix note at 10:05), possibly resolved since:
- lag keeps growing

## Ruled out

- Kafka broker health

## Agent

- steps: 1
- tool calls: 1 (0 failed)
- first hypothesis after 150 s
- model calls: 1
- tokens: 1200 in, 300 out
- cost: $0.07

Drafted by Piperoom dev from 14 events; summary source: handoff `h_1`

Note: 3 summary lines about the hand-off process (a failed or truncated hand-off) were left out of the sections above; the timeline shows the hand-off events.
