-- +goose Up
-- Per-room log caps (docs/design/retention.md section 5). The event cap is
-- checked on last_seq (the log has no gaps), so it needs no new column. The
-- byte cap needs byte_size, the sum of the payload JSON sizes the store wrote;
-- Append updates it in the insert's transaction, under the row lock.
--
-- ADD COLUMN with no default rewrites no row, so this migration holds its
-- ACCESS EXCLUSIVE lock on rooms and shadow_runs for an instant whatever their
-- size. Rooms from before it start with byte_size NULL: their byte cap is not
-- enforced (their event cap is) until the server counts them in the
-- background after start, BackfillBatch rooms per short transaction, under an
-- advisory lock (store.PG.BackfillByteSize). New rooms and runs are created
-- with 0.
--
-- The backfill can only read the payloads back as jsonb text, so for an old
-- room byte_size is an estimate of what was written: jsonb text has one space
-- more after each ':' and ',' than the JSON Go writes, and 5 bytes less for
-- each <, > or & (Go writes each as a six-byte unicode escape, jsonb prints
-- one byte; 3 less for U+2028 and U+2029, which jsonb prints as three UTF-8
-- bytes). For other payloads the two are equal, apart from numbers in exponent
-- form and repeated keys in a raw alert body. So: written - 5 x (escaped <>&)
-- <= byte_size <= written + (number of ':' and ','), which store's backfill
-- test pins.
--
-- goose runs this in one transaction, so SET LOCAL lasts exactly as long as
-- the migration: if a long open transaction holds rooms, the ALTER gives up
-- after 5s (and the start fails, to be retried) rather than queueing every
-- room read and write behind its ACCESS EXCLUSIVE request.
SET LOCAL lock_timeout = '5s';
ALTER TABLE rooms ADD COLUMN byte_size bigint;
ALTER TABLE shadow_runs ADD COLUMN byte_size bigint;

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
