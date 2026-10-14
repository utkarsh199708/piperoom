-- +goose Up
-- Pipeline change log (tasks/F8-what-changed-checklist.md, section 1): what
-- changed on a pipeline before an alert (DDL, connector config and state,
-- schema versions). Fed by the watcher. before/after hold masked, changed keys
-- only (at most 4 KiB per row), never a record value. The store keeps the
-- newest 5000 rows per pipeline; retention deletes by age.
CREATE TABLE pipeline_changes (
    id          bigserial PRIMARY KEY,
    pipeline_id text        NOT NULL,
    kind        text        NOT NULL,   -- ddl | connector_config | connector_state | schema_version
    object      text        NOT NULL,
    at          timestamptz NOT NULL,
    actor       text        NOT NULL DEFAULT '',
    source      text        NOT NULL,
    fingerprint text        NOT NULL,
    before      jsonb,
    after       jsonb,
    summary     text        NOT NULL DEFAULT '',
    UNIQUE (pipeline_id, kind, object, fingerprint)
);
CREATE INDEX pipeline_changes_pipeline_at ON pipeline_changes (pipeline_id, at DESC);
-- Retention deletes by age across pipelines.
CREATE INDEX pipeline_changes_at ON pipeline_changes (at);

-- +goose Down
DROP TABLE IF EXISTS pipeline_changes;
