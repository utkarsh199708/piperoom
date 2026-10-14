-- +goose Up
-- The auth prune (internal/store/authprune.go, run from cmd/server every few
-- minutes) deletes sessions with
--   expires_at < cutoff OR (revoked_at IS NOT NULL AND revoked_at < cutoff)
-- and login_states with expires_at <= now. Without these indexes each run scans
-- the whole sessions table. login_states_expires_idx (migration 00007) already
-- serves the login_states delete, so only sessions needs indexes here.
--
-- The revoked index is partial: most rows are live, so it stays small and the
-- prune's "revoked_at IS NOT NULL AND revoked_at < cutoff" arm can use it.
--
-- Plain CREATE INDEX (not CONCURRENTLY): this runs once at server startup,
-- before the listener opens, and the sessions table is small. CONCURRENTLY would
-- require a goose NO TRANSACTION annotation and can leave an INVALID index behind
-- if it fails, so it is not worth the extra moving parts here.
CREATE INDEX IF NOT EXISTS sessions_expires_at_idx ON sessions (expires_at);
CREATE INDEX IF NOT EXISTS sessions_revoked_at_idx ON sessions (revoked_at)
    WHERE revoked_at IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS sessions_revoked_at_idx;
DROP INDEX IF EXISTS sessions_expires_at_idx;
