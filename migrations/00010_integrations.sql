-- +goose Up
-- Integrations saved through the Integrations page (docs/design/integrations.md
-- section 4.3, docs/design/secret-providers.md). Every row belongs to one
-- workspace: a self-hosted install has only 'default', hosted has one per
-- customer. Secrets are never stored in clear: `secrets` is AES-256-GCM
-- (nonce || ciphertext) sealed by the caller, and a secret held in a secret
-- manager is only a {"$ref": "secret://..."} pointer inside `config`.
CREATE TABLE integrations (
    workspace_id text        NOT NULL DEFAULT 'default',
    id           text        NOT NULL,                   -- "int_<random>"
    category     text        NOT NULL,
    kind         text        NOT NULL,
    scope        text        NOT NULL DEFAULT '*',       -- '*' = whole workspace; a pipeline id later (section 9)
    config       jsonb       NOT NULL DEFAULT '{}',      -- non-secret fields and secret:// references only
    secrets      bytea,                                  -- sealed typed-in secrets; NULL = none
    key_id       text        NOT NULL DEFAULT '',        -- first 8 hex of sha256(key) that sealed `secrets`
    secrets_set  text[]      NOT NULL DEFAULT '{}',      -- secret field NAMES that have a value, sorted
    version      bigint      NOT NULL DEFAULT 1,         -- optimistic lock (If-Match)
    enabled      boolean     NOT NULL DEFAULT true,
    last_test    jsonb,                                  -- []Check of the last test, no secrets; NULL = never
    last_test_at timestamptz,
    updated_by   text        NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    source       text        NOT NULL DEFAULT 'db',
    PRIMARY KEY (workspace_id, id)
);

-- One integration per category and scope in a workspace (spec.UniqueKey),
-- except alerts (one per alert source kind) and secret managers (one per
-- provider name, since secret:// references name the provider, not a scope).
CREATE UNIQUE INDEX integrations_one_per_category
    ON integrations (workspace_id, category, scope)
    WHERE category NOT IN ('alerts', 'secret_manager');
CREATE UNIQUE INDEX integrations_one_per_alert_source
    ON integrations (workspace_id, kind, scope)
    WHERE category = 'alerts';
CREATE UNIQUE INDEX integrations_secret_manager_name
    ON integrations (workspace_id,
        (CASE WHEN jsonb_typeof(config->'name') = 'string' THEN config->>'name' ELSE '' END))
    WHERE category = 'secret_manager';

-- Every write on the Integrations page. `changed` holds field NAMES only,
-- never values. Rows outlive their integration (no foreign key).
CREATE TABLE integration_audit (
    seq          bigserial   PRIMARY KEY,
    workspace_id text        NOT NULL DEFAULT 'default',
    integration  text        NOT NULL,
    action       text        NOT NULL CHECK (action IN
                     ('created', 'updated', 'tested', 'enabled', 'disabled', 'deleted', 'secret_rotated')),
    actor        text        NOT NULL,
    changed      text[]      NOT NULL DEFAULT '{}',
    at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX integration_audit_by_integration
    ON integration_audit (workspace_id, integration, seq DESC);

-- +goose Down
DROP TABLE IF EXISTS integration_audit;
DROP TABLE IF EXISTS integrations;
