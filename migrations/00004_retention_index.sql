-- +goose Up
-- Retention (PIPEROOM_RETENTION_DAYS) finds closed rooms by closed_at. Partial,
-- so open rooms (the hot rows) add nothing to the index.
CREATE INDEX rooms_closed_at_idx ON rooms (closed_at) WHERE status = 'closed';

-- +goose Down
DROP INDEX IF EXISTS rooms_closed_at_idx;
