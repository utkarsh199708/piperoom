-- +goose Up
-- OIDC login (docs/design/auth.md sections 1.1, 1.2 and 2.1).
--
-- login_states: one row per pre-login cookie. The row id is the cookie value.
-- The callback finds the row through the cookie (never by state) and deletes it
-- on first use, so a replayed callback finds nothing. Rows live 10 minutes and
-- expired rows are pruned on insert; the expiry index serves that prune.
CREATE TABLE login_states (
    id            text        PRIMARY KEY,
    state         text        NOT NULL,
    nonce         text        NOT NULL,
    pkce_verifier text        NOT NULL,
    return_path   text        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    expires_at    timestamptz NOT NULL
);
CREATE INDEX login_states_expires_idx ON login_states (expires_at);

-- identities: the (iss, sub) an IdP account is bound to, per users.yaml user.
-- The first verified-email login of a users.yaml user binds the pair. user_id is
-- unique, so a second pair for the same user (a recycled email on a new account)
-- cannot bind; an admin runs `devctl identities unbind <user>` to move a person.
CREATE TABLE identities (
    iss        text        NOT NULL,
    sub        text        NOT NULL,
    user_id    text        NOT NULL UNIQUE,
    email      text        NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (iss, sub)
);

-- +goose Down
DROP TABLE IF EXISTS identities;
DROP TABLE IF EXISTS login_states;
