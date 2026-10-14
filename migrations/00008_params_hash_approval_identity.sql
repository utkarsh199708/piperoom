-- +goose Up
-- Approval integrity (docs/design/auth.md sections 3.1 and 3.2).
--
-- proposals.params_hash: hex SHA-256 of the RFC 8785 (JCS) form of the frozen
-- proposal (title, risk, rollback, required layers, steps). Set at creation.
-- A row written before this migration has '' here; the service computes the
-- hash on the first approve or reject of such a proposal and stores it (params
-- are frozen, so the lazy value is the same one creation would have written).
ALTER TABLE proposals ADD COLUMN params_hash text NOT NULL DEFAULT '';

-- approvals: the hash the approver saw and who they were. identity_session_id is
-- the public session id (never the cookie token). identity_method is 'oidc' or
-- 'dev'; identity_grant is how the right was derived ('user:..', 'group:..',
-- 'backup:..', 'solo'). Rows from before this migration keep the empty defaults
-- and a NULL auth time.
ALTER TABLE approvals
    ADD COLUMN params_hash          text        NOT NULL DEFAULT '',
    ADD COLUMN identity_iss         text        NOT NULL DEFAULT '',
    ADD COLUMN identity_sub         text        NOT NULL DEFAULT '',
    ADD COLUMN identity_session_id  text        NOT NULL DEFAULT '',
    ADD COLUMN identity_method      text        NOT NULL DEFAULT '',
    ADD COLUMN identity_grant       text        NOT NULL DEFAULT '',
    ADD COLUMN identity_auth_time   timestamptz;

-- +goose Down
ALTER TABLE approvals
    DROP COLUMN IF EXISTS identity_auth_time,
    DROP COLUMN IF EXISTS identity_grant,
    DROP COLUMN IF EXISTS identity_method,
    DROP COLUMN IF EXISTS identity_session_id,
    DROP COLUMN IF EXISTS identity_sub,
    DROP COLUMN IF EXISTS identity_iss,
    DROP COLUMN IF EXISTS params_hash;
ALTER TABLE proposals DROP COLUMN IF EXISTS params_hash;
