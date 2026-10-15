-- +goose Up
-- Idempotent steer and comment posts (docs/api.md, Idempotent posts; #459).
-- A request's client_id is unique per (room, caller, key) for the life of the
-- room, across comment.added, steer.suggested and steer.sent, so a retry or two
-- racing requests on any replica append exactly one event. The store tells this
-- violation apart from others by the index name (room_events_client_id_uq).
-- Only events whose payload has the key are indexed; every older event has none.
CREATE UNIQUE INDEX room_events_client_id_uq
    ON room_events (room_id, (actor->>'id'), (payload->>'client_id'))
    WHERE payload ? 'client_id';

-- +goose Down
DROP INDEX IF EXISTS room_events_client_id_uq;
