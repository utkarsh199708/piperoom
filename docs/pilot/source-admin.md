# The source write role's admin grants

[write-roles.md](write-roles.md) lists the write role of the `pg_*` fix actions. This page adds the four
**source admin** actions of `internal/actions/sourceactions` — `pg_set_replica_identity`,
`source_grant_select`, `source_end_session`, `source_create_heartbeat_table` — one section each: what it
runs, the exact SQL, the minimal grant, which role holds it, what a missing grant looks like, and whether
the step can be undone. Citations name `internal/clients/postgres/source_admin.go` or a file of
`internal/actions/sourceactions` (`replica_identity.go`), and `<...>` stands for your own names.

All four use the pool and login of `SOURCE_DB_WRITE_URL`, **Piperoom's write role** below, never the
read-only `SOURCE_DB_URL` role and never the connector's role (`source_admin.go:17-46`). Without
`SOURCE_DB_WRITE_URL`, or with `PIPEROOM_WRITE_MODE` off, no writer is built and each is refused at
`Validate` with "no write credentials" (`common.go:95`), yet it still previews. `Validate` and `Preview`
use only the read-only clients; `Execute` alone holds the writer (`readonly_test.go`). *Piperoom's write
role* is the role in `SOURCE_DB_WRITE_URL`; the *connector's role* is Debezium's `database.user`.

| Action | Minimal grant | Needed by | Reversible |
|---|---|---|---|
| `pg_set_replica_identity` | ownership of the table | write role | yes, if the old mode was `full`/`default` |
| `source_grant_select` | own the table, or `SELECT` with grant option | write role | yes, by a DBA's `REVOKE` |
| `source_end_session` | `pg_signal_backend` (or superuser) | write role | no |
| `source_create_heartbeat_table` | `CREATE` on the schema | write role | yes, by a DBA's `DROP TABLE` |

## `pg_set_replica_identity`

**What it does.** Sets the table's replica identity: `FULL` for a table without a primary key (whose
UPDATEs and DELETEs otherwise have no key) or when consumers need every old column; `DEFAULT` back to the
primary key; `index` to use a unique index's columns (`replica_identity.go:58-79`). Refused: a table
outside the pipeline's `source_tables`, the mode the table already has, and `DEFAULT` on a table with no
primary key (`replica_identity.go:127-178`).

```sql
ALTER TABLE "public"."orders" REPLICA IDENTITY FULL;   -- or DEFAULT
ALTER TABLE "public"."orders" REPLICA IDENTITY USING INDEX "orders_key";
```

**Minimal grant.** `ALTER TABLE` is owner-only; no `GRANT` allows it. Piperoom's write role must own each
captured table, or be a member that inherits from the role that owns it: `ALTER TABLE public.orders OWNER
TO piperoom_writer;`, repeat per captured table. It is the same ownership `pg_add_publication_table` needs
([write-roles.md](write-roles.md#source-postgres-source_db_write_url)), and full control of those tables.

**Missing grant.** The server's `must be owner of table` (SQLSTATE `42501`) is passed through with no
hint: only `42P01` and `42704` become `ErrNotFound` (`source_admin.go:185-196`). Nothing is changed.

**Reversible.** Only when the old identity was `full` or `default`: the rollback is this action with the
old mode (`replica_identity.go:309-313`). An old `index` identity's name is not read, and `nothing` has no
registered action, so those rollbacks are DBA statements (`replica_identity.go:314-317`).

## `source_grant_select`

**What it does.** `GRANT SELECT`, nothing else, on 1 to 50 source tables of the pipeline, to the
connector's `database.user` exactly (`grant_select.go:63-67, 21-23`). Refused: a role that is not that
connector's `database.user` — a `${...}` config-provider reference cannot be compared, so a DBA grants it
(`grant_select.go:144-165`); a nonexistent role or a grant that changes nothing
(`grant_select.go:190-199`); a `pg_`-namespaced or reserved grantee such as `PUBLIC`
(`source_admin.go:253-270`).

```sql
GRANT SELECT ON TABLE "public"."orders", "public"."customers" TO "debezium";
```

One statement for all the tables, all-or-none, with no `WITH GRANT OPTION`: the connector's role cannot
re-grant it (`source_admin.go:104-112`).

**Minimal grant — two roles.** Piperoom's write role is the grantor, and a grantor must own the table or
hold the privilege with grant option: `GRANT SELECT ON public.orders TO piperoom_writer WITH GRANT
OPTION;`, repeat per captured table, or the ownership above. This is the one place the write role gets
plain `SELECT` on data (grant option includes the privilege); keep it to those tables. It is the
exception to write-roles.md's "do not grant `SELECT` to the writer". The connector's role is the grantee:
it gains `SELECT` on the tables it could not read, and Piperoom's read role gets nothing.

**Missing grant.** SQLSTATE `42501` becomes `grant: the write role must own the tables or hold GRANT OPTION
on them`; a missing table or role is `ErrNotFound` (`source_admin.go:232-239`). Nothing is granted.

**Reversible.** Yes: `REVOKE SELECT ON TABLE <the tables that lacked it> FROM <role>`
(`grant_select.go:285-287`). No revoke action is registered; a DBA runs it.

## `source_end_session`

**What it does.** Ends one source session that holds back the pipeline's slot, with `pg_terminate_backend`
(`end_session.go:75-80`). `pid` and `started_at` come from `source_blocking_sessions`; the session must
still be on that list, with the same start time, at `Validate` and at `Execute`
(`end_session.go:161-190`). Never ended: a walsender, the write pool's own backend, another session of the
write role, a session whose `application_name` starts with `piperoom`, and the connector's `database.user`
sessions (`source_admin.go:353-372`; `end_session.go:183-185`).

```sql
SELECT backend_start, COALESCE(backend_type, ''), COALESCE(application_name, ''),
       COALESCE(usename, ''), pid = pg_backend_pid(), COALESCE(usename = current_user, false)
FROM pg_stat_activity WHERE pid = $1;            -- the re-read: identity, type, user
SELECT pg_stat_clear_snapshot();                 -- discard the transaction's activity snapshot
SELECT pg_terminate_backend(a.pid) FROM pg_stat_activity a
WHERE a.pid = $1 AND a.backend_start = $2;       -- only the session that was read
```

**Minimal grant.** A superuser, or membership in `pg_signal_backend`, on Piperoom's write role:
`GRANT pg_signal_backend TO piperoom_writer;` (`source_admin.go:326-331`). The connector's role needs
nothing. `pg_signal_backend` is a broad grant, like `REPLICATION` in write-roles.md: an attribute of the
role, not a per-session right.

**Missing grant.** SQLSTATE `42501` becomes `terminate: needs a superuser or pg_signal_backend`, and no
session is ended (`source_admin.go:384-388`). The other refusals come before any terminate call:
`ErrSessionChanged` for a pid gone or reused, `ErrSessionProtected` for the sessions above
(`source_admin.go:48-60`).

**Reversible.** No (`end_session.go:258-259`). Its open transaction is rolled back and the client sees a
dropped connection (`end_session.go:75-80`); it may reconnect and repeat it. A termination is not
transactional: if the commit fails after the signal, the error says the session is already gone
(`source_admin.go:394-400`).

## `source_create_heartbeat_table`

**What it does.** Creates the table Debezium's `heartbeat.action.query` writes to, so a connector on a
quiet database still confirms a newer LSN and its slot stops holding WAL
(`heartbeat_table.go:65-72, 84-88`). The table must not be one of the pipeline's `source_tables` and must
be in a schema that holds one (`heartbeat_table.go:132-146`; `source_admin.go:422-441`).

```sql
CREATE TABLE IF NOT EXISTS "public"."debezium_heartbeat" (
  id integer PRIMARY KEY,
  ts timestamptz NOT NULL DEFAULT now()
);
```

Idempotent: an existing table is not altered or checked (`heartbeat_table.go:278-283`).

**Minimal grant.** `CREATE` on that schema, on Piperoom's write role: `GRANT CREATE ON SCHEMA public TO
piperoom_writer;` (`source_admin.go:303-305`). PostgreSQL has no grant that allows creating one named
table only, so this is `CREATE` on a schema that holds captured tables (the scope rule above). If that is
too much, a DBA creates the same table and this action stays ungranted.

**Missing grant.** SQLSTATE `42501` becomes `create table: the write role needs CREATE on schema
"<schema>"`; a missing schema (`3F000`) is `ErrNotFound` (`source_admin.go:300-306`). Nothing is created.

**Follow-up steps**, listed in the same proposal's preview (`heartbeat_table.go:284-332`):

1. `pg_add_publication_table` when the publication is filtered — Debezium sees only published tables. It
   needs ownership of the publication **and** the table, and accepts only the pipeline's `source_tables`,
   so the table goes into `owners.yaml` first ([write-roles.md](write-roles.md)).
2. `connect_update_connector_config` with `heartbeat.action.query = INSERT INTO <t> (id, ts) VALUES (1,
   now()) ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts`, plus `heartbeat.interval.ms` (10 s) when the
   connector sends no heartbeats (`heartbeat_table.go:244-246, 292-297`). Connect restarts the tasks once.

The upsert runs as the connector (`heartbeat_table.go:284`), and Piperoom's write role owns the table it
created. So the **connector's role** also needs `INSERT` and `UPDATE` on it — `GRANT INSERT, UPDATE ON
<heartbeat table> TO <connector role>` — which Piperoom neither grants nor checks.

**Reversible.** Yes: `DROP TABLE <t>` by a DBA after removing `heartbeat.action.query` from the connector
(`heartbeat_table.go:249-254`); no action is registered. When the table already existed, the preview says
the step changes nothing and there is no rollback (`heartbeat_table.go:278-283`).
