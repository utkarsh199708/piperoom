-- +goose Up
-- Per-room log caps (docs/design/retention.md section 5). Append updates both
-- columns in the insert's transaction, under the rooms row lock, so a cap can
-- never be passed by concurrent appends. event_count equals last_seq (the log
-- has no gaps); byte_size sums the payload JSON sizes the store wrote.
--
-- The backfill can only read the payloads back as jsonb text, so for a room
-- from before this migration byte_size is an estimate of what was written:
-- jsonb text has one space more after each ':' and ',' than the JSON Go
-- writes, and 5 bytes less for each <, > or & (Go writes each as a six-byte
-- unicode escape, jsonb prints one byte; 3 less for U+2028 and U+2029, which
-- jsonb prints as three UTF-8 bytes). For other payloads the two
-- are equal, apart from numbers in exponent form and repeated keys in a raw
-- alert body. So: written - 5 x (escaped <>&) <= byte_size <= written +
-- (number of ':' and ','), which store's backfill test pins.
ALTER TABLE rooms ADD COLUMN event_count bigint NOT NULL DEFAULT 0;
ALTER TABLE rooms ADD COLUMN byte_size   bigint NOT NULL DEFAULT 0;

UPDATE rooms r SET
    event_count = r.last_seq,
    byte_size   = COALESCE((SELECT sum(octet_length(e.payload::text)) FROM room_events e WHERE e.room_id = r.id), 0);

-- The same caps hold for a shadow room's own log (AppendShadow); its event
-- count is shadow_runs.last_seq. Same backfill estimate.
ALTER TABLE shadow_runs ADD COLUMN byte_size bigint NOT NULL DEFAULT 0;

UPDATE shadow_runs r SET
    byte_size = COALESCE((SELECT sum(octet_length(e.payload::text)) FROM shadow_events e WHERE e.room_id = r.room_id), 0);

-- One content-free row per deleted room: an admin delete (DELETE
-- /api/rooms/{id}) or a retention purge (deleted_by 'system'). Ids, who, when,
-- why and how many events, never room content. The primary key makes a
-- repeated delete find the first row instead of adding one.
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
ALTER TABLE shadow_runs DROP COLUMN IF EXISTS byte_size;
ALTER TABLE rooms DROP COLUMN IF EXISTS byte_size;
ALTER TABLE rooms DROP COLUMN IF EXISTS event_count;
