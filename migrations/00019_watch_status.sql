-- +goose Up
-- Watcher status per pipeline (X5a; clients.WatchStatusWriter /
-- WatchStatusReader): the watcher's latest view of one pipeline, replaced on
-- every tick (one row per pipeline, never a history). The fleet view
-- (GET /api/fleet) reads it instead of asking the watcher, which runs on one
-- replica only. signals is a JSON array of clients.WatchSignal (at most
-- clients.MaxWatchSignals entries): signal names, states, severities, node ids,
-- times and forecast metrics. Never a row value, a record, a config value or
-- free text.
CREATE TABLE watch_status (
    pipeline_id text        PRIMARY KEY,
    checked_at  timestamptz NOT NULL,
    signals     jsonb       NOT NULL DEFAULT '[]'::jsonb
);

-- +goose Down
DROP TABLE IF EXISTS watch_status;
