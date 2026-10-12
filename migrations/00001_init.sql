-- +goose Up
CREATE TABLE rooms (
    id                  text        PRIMARY KEY,
    title               text        NOT NULL,
    severity            text        NOT NULL,
    pipeline_id         text        NOT NULL,
    status              text        NOT NULL DEFAULT 'open',
    opened_at           timestamptz NOT NULL,
    closed_at           timestamptz,
    last_seq            bigint      NOT NULL DEFAULT 0,
    driver_id           text,
    driver_name         text,
    driver_lease_until  timestamptz
);

CREATE TABLE room_events (
    room_id  text        NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    seq      bigint      NOT NULL,
    type     text        NOT NULL,
    actor    jsonb       NOT NULL,
    payload  jsonb       NOT NULL,
    at       timestamptz NOT NULL,
    PRIMARY KEY (room_id, seq)
);

CREATE TABLE proposals (
    proposal_id      text        PRIMARY KEY,
    room_id          text        NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    title            text        NOT NULL,
    status           text        NOT NULL DEFAULT 'open',
    required_layers  jsonb       NOT NULL DEFAULT '[]',
    steps            jsonb       NOT NULL DEFAULT '[]',
    risk             text        NOT NULL DEFAULT '',
    rollback         text        NOT NULL DEFAULT '',
    executed_count   int         NOT NULL DEFAULT 0,
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE approvals (
    proposal_id  text        NOT NULL REFERENCES proposals(proposal_id) ON DELETE CASCADE,
    layer        text        NOT NULL,
    decision     text        NOT NULL,
    reason       text        NOT NULL DEFAULT '',
    decided_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (proposal_id, layer)
);

CREATE TABLE suggestions (
    suggestion_id text        PRIMARY KEY,
    room_id       text        NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    from_id       text        NOT NULL,
    from_name     text        NOT NULL,
    text          text        NOT NULL,
    status        text        NOT NULL DEFAULT 'pending',
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE handoffs (
    handoff_id       text        PRIMARY KEY,
    room_id          text        NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    to_id            text        NOT NULL,
    to_name          text        NOT NULL,
    status           text        NOT NULL DEFAULT 'pending',
    summary          jsonb       NOT NULL DEFAULT '{}',
    stay_as_watcher  boolean     NOT NULL DEFAULT false,
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX room_events_room_seq_idx ON room_events (room_id, seq);
CREATE INDEX proposals_room_idx ON proposals (room_id);
CREATE INDEX suggestions_room_idx ON suggestions (room_id);
CREATE INDEX handoffs_room_idx ON handoffs (room_id);

-- +goose Down
DROP INDEX IF EXISTS handoffs_room_idx;
DROP INDEX IF EXISTS suggestions_room_idx;
DROP INDEX IF EXISTS proposals_room_idx;
DROP INDEX IF EXISTS room_events_room_seq_idx;
DROP TABLE IF EXISTS handoffs;
DROP TABLE IF EXISTS suggestions;
DROP TABLE IF EXISTS approvals;
DROP TABLE IF EXISTS proposals;
DROP TABLE IF EXISTS room_events;
DROP TABLE IF EXISTS rooms;
