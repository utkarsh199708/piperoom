# Restart a connector automatically after a source restart

A planned Postgres restart (a patch release, an upgrade, a failover) ends the walsender with
`FATAL: terminating connection due to administrator command`. If the database takes longer to come back
than Debezium's retries allow, task 0 of the connector stays `FAILED` until someone restarts it. The
replication slot keeps the WAL in the meantime, so a restart loses nothing; it just has to happen.

Piperoom diagnoses this case (runbook `source-restart`, eval scenario `source-restart-reconnect`) and
proposes `connect_restart_task`. By default that proposal waits for the kafka-connect owner's approval. A
pipeline can instead let it run on a timer, so the connector is back without anyone waking up.

## Turn it on

1. Write mode on: `PIPEROOM_WRITE_MODE=on` with the Connect write grant in [write-roles.md](write-roles.md).
   In read-only mode nothing runs, the timer included.
2. List the restart actions in the pipeline's `auto_approve:` in `owners.yaml`:

   ```yaml
   pipelines:
     - id: orders
       # ...
       auto_approve: [connect_restart_task, connect_restart_connector]
   ```

   The server refuses an unknown action name at start (`internal/owners/autoapprove.go`).
3. Optional: `PIPEROOM_AUTO_APPROVE_DELAY` sets the countdown (Go duration, default `60s`). `0` turns the
   policy off even when `owners.yaml` lists actions.

When the agent proposes one of the listed actions, the room shows a countdown (`proposal.scheduled`). Anyone
in the room can cancel it (`POST /api/proposals/{pid}/cancel`; the proposal then waits for a manual
approval). When it runs out, one approval per required layer is recorded with grant `policy` and the
executor restarts the task. No timer starts in a shadow room, with write mode off, or when the preview's
dry run failed (`docs/api.md`, "Scheduled proposals").

## What stays manual

Keep `connect_stop_connector`, `connect_reset_source_offsets` and `pg_drop_replication_slot` out of the
list. The agent must not drop a slot or reset offsets after a restart; those lose data or re-read it, and
need the owners' eyes. If the task fails again after the restart (the database still down, or a
different error), the room stays open for the owners as usual.
