-- +goose Up
-- Per-room log caps (docs/design/retention.md section 5). Append updates both
-- columns in the insert's transaction, under the rooms row lock, so a cap can
-- never be passed by concurrent appends. event_count equals last_seq (the log
-- has no gaps); byte_size sums the payload JSON sizes the store wrote. The
-- backfill reads jsonb text, which has a few more spaces than the JSON that
-- was written, so old rooms start slightly over-counted (never under).
ALTER TABLE rooms ADD COLUMN event_count bigint NOT NULL DEFAULT 0;
ALTER TABLE rooms ADD COLUMN byte_size   bigint NOT NULL DEFAULT 0;

UPDATE rooms r SET
    event_count = r.last_seq,
    byte_size   = COALESCE((SELECT sum(octet_length(e.payload::text)) FROM room_events e WHERE e.room_id = r.id), 0);

-- One content-free row per room an admin deleted (DELETE /api/rooms/{id}):
-- ids, who, when, why and how many events, never room content. The primary
-- key makes a repeated delete find the first row instead of adding one.
CREATE TABLE room_deletions (
    room_id     text        PRIMARY KEY,
    pipeline_id text        NOT NULL,
    deleted_by  text        NOT NULL,
    deleted_at  timestamptz NOT NULL,
    reason      text        NOT NULL,
    event_count bigint      NOT NULL
);

-- +goose Down
DROP TABLE IF EXISTS room_deletions;
ALTER TABLE rooms DROP COLUMN IF EXISTS byte_size;
ALTER TABLE rooms DROP COLUMN IF EXISTS event_count;
