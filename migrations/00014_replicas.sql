-- +goose Up
-- Multi-replica coordination (docs/design/replicas.md).
--
-- One row per room whose agent runs somewhere: only the owner replica runs it.
-- The owner renews expires_at every 10 s (TTL 30 s); a row past expires_at is a
-- run whose replica stopped, and another replica takes it over. slot is true
-- while the run holds an agent slot; PIPEROOM_MAX_AGENTS_GLOBAL counts the
-- live rows with slot = true.
CREATE TABLE room_agent_leases (
    room_id     text        PRIMARY KEY REFERENCES rooms(id) ON DELETE CASCADE,
    owner       text        NOT NULL,
    pipeline_id text        NOT NULL DEFAULT '',
    slot        boolean     NOT NULL DEFAULT false,
    acquired_at timestamptz NOT NULL DEFAULT now(),
    expires_at  timestamptz NOT NULL
);

CREATE INDEX room_agent_leases_owner_idx ON room_agent_leases (owner);
CREATE INDEX room_agent_leases_expires_idx ON room_agent_leases (expires_at);

-- The executor claims a fully approved proposal once, across replicas, before
-- its first step; claimed_at is refreshed before every step, so a claim whose
-- claimed_at is old belongs to a replica that stopped.
ALTER TABLE proposals ADD COLUMN claimed_by text;
ALTER TABLE proposals ADD COLUMN claimed_at timestamptz;

-- The stale-claim sweep reads open proposals only.
CREATE INDEX proposals_open_idx ON proposals (proposal_id) WHERE status = 'open';

-- +goose Down
DROP INDEX IF EXISTS proposals_open_idx;
ALTER TABLE proposals DROP COLUMN IF EXISTS claimed_at;
ALTER TABLE proposals DROP COLUMN IF EXISTS claimed_by;
DROP INDEX IF EXISTS room_agent_leases_expires_idx;
DROP INDEX IF EXISTS room_agent_leases_owner_idx;
DROP TABLE IF EXISTS room_agent_leases;
