-- +goose Up
-- Shadow mode (docs/design/shadow.md section 3.2). A shadow room's agent output
-- never enters room_events before the reveal: it goes to shadow_events, with
-- its own gap-free seq kept in shadow_runs.last_seq (the same pattern as
-- rooms.last_seq). One run row per shadow room.
CREATE TABLE shadow_runs (
    room_id              text        PRIMARY KEY REFERENCES rooms(id) ON DELETE CASCADE,
    status               text        NOT NULL,          -- queued|running|hypothesised|no_hypothesis|error|budget|interrupted
    last_seq             bigint      NOT NULL DEFAULT 0,
    model                text        NOT NULL DEFAULT '',
    pipeline_map         text        NOT NULL DEFAULT '',
    started_at           timestamptz,                   -- slot acquired; NULL while queued
    first_hypothesis_at  timestamptz,
    final_hypothesis_at  timestamptz,
    finished_at          timestamptz,
    finished_after_close boolean     NOT NULL DEFAULT false,
    final_hypothesis     jsonb,                         -- events.AgentHypothesisPayload
    tool_calls           integer     NOT NULL DEFAULT 0,
    tokens               bigint      NOT NULL DEFAULT 0,
    revealed_at          timestamptz
);

CREATE TABLE shadow_events (
    room_id text        NOT NULL REFERENCES shadow_runs(room_id) ON DELETE CASCADE,
    seq     bigint      NOT NULL,
    type    text        NOT NULL,
    actor   jsonb       NOT NULL,
    payload jsonb       NOT NULL,
    at      timestamptz NOT NULL,
    PRIMARY KEY (room_id, seq)
);

-- +goose Down
DROP TABLE IF EXISTS shadow_events;
DROP TABLE IF EXISTS shadow_runs;
