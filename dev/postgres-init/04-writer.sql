-- Source write role for the pg_* write actions (docs/pilot/write-roles.md): the
-- value of SOURCE_DB_WRITE_URL in dev/demo.env. It is a separate role from the
-- read-only SOURCE_DB_URL one (wiring refuses the same role). REPLICATION is
-- what pg_drop_replication_slot needs; it owns nothing, so pg_add_publication_table
-- and pg_snapshot_signal still refuse on this stack. Dev credentials only.
CREATE ROLE piperoom_writer LOGIN PASSWORD 'piperoom-dev'
  NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION NOBYPASSRLS
  CONNECTION LIMIT 2;
