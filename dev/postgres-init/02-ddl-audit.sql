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
LANGUAGE plpgsql AS $$
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
