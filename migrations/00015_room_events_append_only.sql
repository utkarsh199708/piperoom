-- +goose Up
-- room_events is an append-only log (AGENTS.md section 1). Until now only the
-- code kept it so; this makes the database refuse UPDATE, DELETE and TRUNCATE
-- on it too (QA sweep 2026-10-14). The one legitimate removal, retention purge
-- and admin room deletion (internal/store/retention.go deleteRoomTx), first sets
-- the transaction-local setting piperoom.purge to 'on'; a DELETE of rooms that
-- cascades to room_events needs the same. The setting is a guard against
-- mistakes and ad-hoc SQL, not an access control: any session may set it.
-- +goose StatementBegin
CREATE FUNCTION room_events_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'UPDATE' AND current_setting('piperoom.purge', true) = 'on' THEN
        -- A row trigger must return OLD for the DELETE to go ahead (NULL
        -- would skip the row silently); the TRUNCATE statement trigger's
        -- return value is ignored.
        IF TG_LEVEL = 'ROW' THEN
            RETURN OLD;
        END IF;
        RETURN NULL;
    END IF;
    RAISE EXCEPTION 'room_events is append-only: % refused', TG_OP
        USING ERRCODE = 'restrict_violation',
              HINT = 'room events are removed only by the retention purge';
END;
$$;
-- +goose StatementEnd

-- Row level, so a DELETE of an empty room (DeleteEmptyRoom, whose cascade
-- touches no event) is not refused; statement triggers fire even for 0 rows.
CREATE TRIGGER room_events_no_update_delete
    BEFORE UPDATE OR DELETE ON room_events
    FOR EACH ROW EXECUTE FUNCTION room_events_append_only();

CREATE TRIGGER room_events_no_truncate
    BEFORE TRUNCATE ON room_events
    FOR EACH STATEMENT EXECUTE FUNCTION room_events_append_only();

-- +goose Down
DROP TRIGGER IF EXISTS room_events_no_truncate ON room_events;
DROP TRIGGER IF EXISTS room_events_no_update_delete ON room_events;
DROP FUNCTION IF EXISTS room_events_append_only();
