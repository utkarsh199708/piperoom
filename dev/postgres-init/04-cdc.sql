-- CDC objects the Debezium connector needs (dev/connectors/pg-orders-connector.json).
-- See dev/scenarios.md, "Known issues", for the two QA findings behind them.
--
-- debezium.heartbeat: the connector's heartbeat.action.query updates its one row
-- every heartbeat. The update is a change on the slot's own publication, so the
-- connector confirms WAL even while public.orders is quiet; without it the slot's
-- restart_lsn stands still while other databases on this server write (QA G18).
--
-- dbz_publication is created here, and the connector runs with
-- publication.autocreate.mode=disabled. In "filtered" mode Debezium runs
-- ALTER PUBLICATION ... SET TABLE on its replication connection at every task
-- start; that walsender then decodes WAL written before a column type change with
-- the new row type ("compressed lz4 data is corrupt", QA G17).
--
-- Idempotent: `devctl up` runs this file again, for volumes created before it existed.
-- session_replication_role=replica keeps the DDL audit trigger (02-ddl-audit.sql)
-- quiet: this is stack setup, not a schema change of the pipeline.
BEGIN;
SET LOCAL session_replication_role = replica;

CREATE SCHEMA IF NOT EXISTS debezium;

CREATE TABLE IF NOT EXISTS debezium.heartbeat (
  id      INTEGER     PRIMARY KEY,
  beat_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO debezium.heartbeat (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'dbz_publication') THEN
    CREATE PUBLICATION dbz_publication FOR TABLE public.orders, debezium.heartbeat;
  ELSIF NOT EXISTS (SELECT FROM pg_publication_rel pr JOIN pg_publication p ON p.oid = pr.prpubid
                    WHERE p.pubname = 'dbz_publication' AND pr.prrelid = 'debezium.heartbeat'::regclass) THEN
    ALTER PUBLICATION dbz_publication ADD TABLE debezium.heartbeat;
  END IF;
END
$$;

COMMIT;
