-- +goose Up
-- Store the hand-off creator so acceptance can require them to still be driver.
ALTER TABLE handoffs ADD COLUMN from_id text NOT NULL DEFAULT '';
ALTER TABLE handoffs ADD COLUMN from_name text NOT NULL DEFAULT '';

-- Per-step execution state: a step is marked "started" BEFORE it runs so a
-- crash or a duplicate executor can never run it twice.
CREATE TABLE proposal_steps (
    proposal_id text        NOT NULL REFERENCES proposals(proposal_id) ON DELETE CASCADE,
    step_index  int         NOT NULL,
    status      text        NOT NULL DEFAULT 'started',
    output      text        NOT NULL DEFAULT '',
    started_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (proposal_id, step_index)
);

-- executed_count is superseded by proposal_steps.
ALTER TABLE proposals DROP COLUMN executed_count;

-- The composite PK (room_id, seq) already provides this index.
DROP INDEX IF EXISTS room_events_room_seq_idx;

-- +goose Down
CREATE INDEX room_events_room_seq_idx ON room_events (room_id, seq);
ALTER TABLE proposals ADD COLUMN executed_count int NOT NULL DEFAULT 0;
DROP TABLE IF EXISTS proposal_steps;
ALTER TABLE handoffs DROP COLUMN IF EXISTS from_id;
ALTER TABLE handoffs DROP COLUMN IF EXISTS from_name;
