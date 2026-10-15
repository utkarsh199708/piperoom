-- +goose Up
-- Idempotent steer and comment posts (docs/api.md, Idempotent posts; #459).
-- A request's client_id is unique per (room, caller, key) for the life of the
-- room, across comment.added, steer.suggested and steer.sent, so a retry or two
-- racing requests on any replica append exactly one event. The store tells this
-- violation apart from others by the index name (room_events_client_id_uq).
-- Only events whose payload has the key are indexed; every older event has none.
--
-- Plain CREATE INDEX in the migration's transaction, not CONCURRENTLY:
-- replicas that start together wait for the migrations advisory lock
-- (store.WithMigrationLock) holding a snapshot, and CONCURRENTLY waits for
-- every older snapshot, so it would wait for them and never finish
-- (TestMigrateReplicasTogether). The build holds a SHARE lock on room_events,
-- so appends wait while it scans the table once.
CREATE UNIQUE INDEX room_events_client_id_uq
    ON room_events (room_id, (actor->>'id'), (payload->>'client_id'))
    WHERE payload ? 'client_id';

-- +goose Down
DROP INDEX IF EXISTS room_events_client_id_uq;
