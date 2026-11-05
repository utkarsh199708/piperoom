-- +goose Up
-- Connector config versions (X5c; clients.ConfigVersionWriter / Reader): one
-- row per config of a connector the watcher saw (X3b). config holds the form
-- after actions.RedactConfig only: a credential key's value is "<redacted>"
-- and every other value has passed redact.Text, so no secret value is stored
-- (the store refuses a credential key with any other value). hash is
-- actions.ConfigHash of that form, computed by the store. config and
-- changed_keys are JSON in text columns, not jsonb: jsonb refuses a \u0000
-- escape, which a connector config value may hold. Retention
-- (PIPEROOM_CHANGES_RETENTION_DAYS) deletes by age but keeps each
-- connector's newest version; the store also keeps at most 1000 versions per
-- connector.
CREATE TABLE connector_config_versions (
    id           bigserial PRIMARY KEY,
    pipeline_id  text        NOT NULL,
    connector    text        NOT NULL,
    hash         text        NOT NULL,
    at           timestamptz NOT NULL,
    actor        text        NOT NULL DEFAULT '',   -- ConnectorConfigVersion.By
    changed_keys text        NOT NULL DEFAULT '[]', -- JSON array of key names
    config       text        NOT NULL               -- JSON object, redacted
);
-- The list (newest first) and the "latest version" check of each write.
CREATE INDEX connector_config_versions_conn_at
    ON connector_config_versions (pipeline_id, connector, at DESC, id DESC);
-- The diff route looks a version up by hash.
CREATE INDEX connector_config_versions_conn_hash
    ON connector_config_versions (pipeline_id, connector, hash);
-- Retention deletes by age across pipelines.
CREATE INDEX connector_config_versions_at ON connector_config_versions (at);

-- +goose Down
DROP TABLE IF EXISTS connector_config_versions;
