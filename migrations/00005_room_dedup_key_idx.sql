-- +goose Up
-- LastRoomByDedupKey (the watcher's restart cooldown) looks a room up by the
-- dedup_key stored in its room.created payload. Only room.created rows carry
-- one, so the index is partial and stays small.
CREATE INDEX room_events_created_dedup_key_idx
    ON room_events ((payload->>'dedup_key'))
    WHERE type = 'room.created';

-- +goose Down
DROP INDEX IF EXISTS room_events_created_dedup_key_idx;
