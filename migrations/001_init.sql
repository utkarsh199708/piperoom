-- +goose Up
-- The event log is the single source of truth. Proposals, approvals, suggestions
-- and hand-offs are folded from it by the room service, so they get no tables.

-- rooms registers each log and owns its sequence counter. Append locks the row
-- (UPDATE ... last_seq + 1) so concurrent appends to one room are serialised and
-- seq has no gaps; appends to different rooms never contend.
CREATE TABLE rooms (
    id         text        PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_seq   bigint      NOT NULL DEFAULT 0
);

-- The primary key (room_id, seq) is also the index that serves "events after
-- seq N" replays.
CREATE TABLE room_events (
    room_id text        NOT NULL REFERENCES rooms (id),
    seq     bigint      NOT NULL,
    type    text        NOT NULL,
    actor   jsonb       NOT NULL,
    payload jsonb       NOT NULL,
    at      timestamptz NOT NULL,
    PRIMARY KEY (room_id, seq)
);

-- +goose Down
DROP TABLE room_events;
DROP TABLE rooms;
