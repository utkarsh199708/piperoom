-- +goose NO TRANSACTION
-- +goose Up
-- Idempotent steer and comment posts (docs/api.md, Idempotent posts; #459).
-- A request's client_id is unique per (room, caller, key) for the life of the
-- room, across comment.added, steer.suggested and steer.sent, so a retry or two
-- racing requests on any replica append exactly one event. The store tells this
-- violation apart from others by the index name (room_events_client_id_uq).
-- Only events whose payload has the key are indexed; every older event has none.
--
-- CONCURRENTLY, so the build does not block appends on a large room_events
-- while older replicas keep serving during a rolling deploy. It cannot run in a
-- transaction (hence NO TRANSACTION), and a failed build leaves an INVALID
-- index behind: goose then has not recorded this version, so the next start
-- runs the file again, and the DROP removes such a leftover (or a valid index
-- whose version row was lost) before the build.
DROP INDEX CONCURRENTLY IF EXISTS room_events_client_id_uq;
CREATE UNIQUE INDEX CONCURRENTLY room_events_client_id_uq
    ON room_events (room_id, (actor->>'id'), (payload->>'client_id'))
    WHERE payload ? 'client_id';

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS room_events_client_id_uq;
