-- +goose Up
-- Server-side login sessions (docs/design/auth.md §1.3). The cookie holds 32
-- random bytes; only their SHA-256 is stored, so a database read never yields a
-- usable token. last_seen_at is written at most once a minute (idle expiry);
-- expires_at is the absolute cap. revoked_at is set by logout.
CREATE TABLE sessions (
    id             text        PRIMARY KEY,
    token_hash     bytea       NOT NULL UNIQUE,
    user_id        text        NOT NULL,
    name           text        NOT NULL DEFAULT '',
    iss            text        NOT NULL DEFAULT '',
    sub            text        NOT NULL DEFAULT '',
    groups         text[]      NOT NULL DEFAULT '{}',
    groups_missing boolean     NOT NULL DEFAULT false,
    auth_time      timestamptz,
    amr            text[]      NOT NULL DEFAULT '{}',
    acr            text        NOT NULL DEFAULT '',
    method         text        NOT NULL CHECK (method IN ('oidc', 'dev')),
    created_at     timestamptz NOT NULL DEFAULT now(),
    last_seen_at   timestamptz NOT NULL DEFAULT now(),
    expires_at     timestamptz NOT NULL,
    revoked_at     timestamptz
);

-- Auth PR 3 looks up the newest live session of a person by user_id.
CREATE INDEX sessions_user_id_idx ON sessions (user_id, created_at DESC);

-- +goose Down
DROP TABLE IF EXISTS sessions;
