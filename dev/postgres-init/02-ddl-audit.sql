-- Records every DDL command in the orders database, so the agent can see
-- schema changes (clients.DDLChange maps 1:1 to these columns).
CREATE SCHEMA piperoom_audit;

CREATE TABLE piperoom_audit.ddl_log (
  id              BIGSERIAL PRIMARY KEY,
  at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  command_tag     TEXT        NOT NULL,
  object_identity TEXT,
  query           TEXT,
  username        TEXT        NOT NULL
);

CREATE FUNCTION piperoom_audit.log_ddl() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  r record;
BEGIN
  FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
    IF r.schema_name IS DISTINCT FROM 'piperoom_audit' THEN
      INSERT INTO piperoom_audit.ddl_log (command_tag, object_identity, query, username)
      VALUES (r.command_tag, r.object_identity, current_query(), session_user);
    END IF;
  END LOOP;
END
$$;

CREATE EVENT TRIGGER piperoom_ddl_audit ON ddl_command_end
  EXECUTE FUNCTION piperoom_audit.log_ddl();

-- A DROP never reaches pg_event_trigger_ddl_commands(): by the time
-- ddl_command_end fires, the object is gone and the function has nothing to
-- report for it. Drops are only visible from pg_event_trigger_dropped_objects()
-- inside a trigger ON sql_drop, so a dropped source table used to leave no audit
-- row at all. The agent then saw "no recorded change" and blamed the consumer
-- for the rows that stopped arriving (KAN-401).
CREATE FUNCTION piperoom_audit.log_drop() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  r record;
BEGIN
  FOR r IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
    -- A cascaded table is a table that disappeared, so it is logged even when
    -- it is not the object the statement named. Indexes, constraints, triggers
    -- and TOAST tables follow their table and are skipped, or one DROP would
    -- fill the log with rows the agent cannot act on.
    IF r.schema_name IS DISTINCT FROM 'piperoom_audit'
       AND r.object_type IN ('table', 'schema', 'foreign table', 'materialized view') THEN
      INSERT INTO piperoom_audit.ddl_log (command_tag, object_identity, query, username)
      VALUES (CASE WHEN r.original THEN tg_tag ELSE 'DROP ' || upper(r.object_type) END,
              r.object_identity, current_query(), session_user);
    END IF;
  END LOOP;
END
$$;

CREATE EVENT TRIGGER piperoom_drop_audit ON sql_drop
  EXECUTE FUNCTION piperoom_audit.log_drop();
