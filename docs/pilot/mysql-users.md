# MySQL source: database users

For the DBA who connects Piperoom to a pipeline whose source is **MySQL** (the Debezium MySQL connector reading the
binlog). Three accounts, each with only what it needs: the **connector's** user (Debezium), **Piperoom's read** user
(`SOURCE_DB_URL`) and, only if you turn write mode on, **Piperoom's write** user (`SOURCE_DB_WRITE_URL`). The
Postgres equivalents are in [permissions.md](permissions.md#source-postgres) and [source-admin.md](source-admin.md).

Written against the design `docs/design/mysql-source.md` (Q8-0, Jira KAN-302) on the integration branch at
`e61d90ec`. Each claim names the design section or the vendor page it comes from; `<...>` stands for your own names.
If the design or the code changes, update the claim and its citation in the same PR.

**Vendor facts are not confirmed yet.** The design took the MySQL and Debezium facts (privilege names, variable
names, `performance_schema` tables, `mysql.user` columns) from the leads' knowledge of MySQL 8.0/8.4 and Debezium
2.x/3.x, without fetching the vendor docs, and names the row that must confirm each one (design header, §6). This
page could not fetch them either (the vendor hosts were not reachable from this session). Every vendor link below
is the page to check against; the rows marked **confirm** are the ones the design flags. Test every grant on a
staging server before production.

## Status on this release: partly built, not wired

| Piece | State | Where |
|---|---|---|
| MySQL read client (Q8b) | **built, not wired** (`internal/clients/mysql`) | `tasks/Q-board.md` row Q8b |
| MySQL write client (Q8f) | **not in the repo** | Q-board row Q8f |
| `source_kind: mysql` in `owners.yaml` (Q8d) | **in the loader** (`internal/owners/source_kind.go`): it loads and is checked, but the server still opens the source with the Postgres client (Q8g) | Q-board row Q8d; design §4.2 |
| `mysql://` in `SOURCE_DB_URL` / `SOURCE_DB_WRITE_URL` | **not accepted**: the wiring opens the source with the Postgres client only (`internal/wiring/wiring.go`, `postgres.New`); the scheme dispatch is Q8g | design §6.1; Q-board row Q8g |
| `mysql_binlog_status`, `kafka_schema_history`, the `source_*` tools on MySQL | not in the repo (Q8c, Q8e, Q8n) | design §7 |

Until those rows land, create the users below on staging and keep `SOURCE_DB_URL` on your Postgres source. The
grants are what the code is designed to need; Q8b and Q8f confirm them on the dev profile (design §6.2, §6.4, §6.5,
§6.8).

## Supported versions

MySQL **8.0** and **8.4 LTS**; 9.x innovation releases best effort; Amazon RDS for MySQL 8.0 and Aurora MySQL 3
(8.0-compatible). **Not supported:** MySQL 5.7 (end of life; the start-up probe says so) and **MariaDB** (Debezium
has a separate MariaDB connector and MariaDB GTIDs have another format) (design §1, §12, §14 D9).

## Summary

| Account | Grants | Used by |
|---|---|---|
| connector (`debezium`) | `SELECT`, `RELOAD`, `SHOW DATABASES`, `REPLICATION SLAVE`, `REPLICATION CLIENT` (+ `LOCK TABLES` on RDS/Aurora) | Debezium's binlog stream and snapshot |
| Piperoom read | `SELECT` on the captured tables; `REPLICATION CLIENT`; `PROCESS`; `SELECT` on `performance_schema`; column-level `SELECT` on `mysql.user` (no password hash); `SELECT` on `mysql.db`, `mysql.tables_priv`, `mysql.role_edges`, `mysql.default_roles`; RDS: `EXECUTE` on `mysql.rds_show_configuration`. **No `REPLICATION SLAVE`** | every read tool |
| Piperoom write | `SELECT ... WITH GRANT OPTION` on the captured tables; `CONNECTION_ADMIN`; `CREATE` on the heartbeat database; the read grants of its re-reads | the three source admin actions, after approval |

Source: design §10.2.

## 1. The connector's user (Debezium)

Debezium's own setup, repeated here so all three accounts are on one page
([Debezium MySQL connector](https://debezium.io/documentation/reference/stable/connectors/mysql.html), section
"Setting up MySQL"; design §10.2):

```sql
CREATE USER 'debezium'@'<connect workers host or %>' IDENTIFIED BY '<from your secret store>' REQUIRE SSL;
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'<host>';
-- RDS for MySQL and Aurora MySQL only (no global read lock there; the snapshot locks tables instead):
GRANT LOCK TABLES ON *.* TO 'debezium'@'<host>';
```

| Privilege | Why (design §10.2, §6.4) | Scope |
|---|---|---|
| `REPLICATION SLAVE` | read the binlog stream | global only ([MySQL privileges](https://dev.mysql.com/doc/refman/8.4/en/privileges-provided.html)) |
| `REPLICATION CLIENT` | `SHOW BINARY LOG STATUS` / `SHOW MASTER STATUS`, `SHOW BINARY LOGS` | global only |
| `RELOAD` | `FLUSH TABLES WITH READ LOCK` during the snapshot | global only |
| `SHOW DATABASES` | list the databases to capture | global only |
| `SELECT` | read the tables during the snapshot | Debezium's docs grant it `ON *.*`; you may try narrowing it to the captured databases (`ON shop.*`) on staging first |
| `LOCK TABLES` | RDS/Aurora: the table-level snapshot lock | **confirm** on your engine |

- Piperoom never grants `REPLICATION SLAVE` or `REPLICATION CLIENT` to anyone. A missing replication grant is
  diagnosed (the `source_role_grants` tool shows `REPLICATION SLAVE` false) and the room drafts the `GRANT` as text
  for a DBA, because `REPLICATION SLAVE` lets an account read every change on the whole server, not only the
  pipeline's tables (design §8.4, §14 D4).
- Server settings Debezium needs, which Piperoom reads and reports (design §6.2): `log_bin = ON`,
  `binlog_format = ROW`, `binlog_row_image = FULL`; `gtid_mode = ON` and `enforce_gtid_consistency = ON`
  recommended (positions survive a failover); a `server_id` different from the connector's `database.server.id`;
  `binlog_expire_logs_seconds` (on RDS `rds.binlog_retention_hours`) longer than the longest planned connector
  downtime. **Confirm** the names on 8.0 and 8.4, and whether `binlog_format` (deprecated in 8.0.34) is still
  readable on 8.4 (design §6.2).
- If you later approve `source_create_heartbeat_table` and add a `heartbeat.action.query`, the connector's user
  also needs `INSERT` and `UPDATE` on that table, which Piperoom neither grants nor checks (as on Postgres,
  [source-admin.md](source-admin.md#source_create_heartbeat_table)). On MySQL, `heartbeat.interval.ms` alone keeps
  the stored position moving; the table is needed only for the action query (design §8.1).

## 2. Piperoom's read user (`SOURCE_DB_URL`)

Read-only **by grant and by session**: this user has no write privilege, and the client also sets
`transaction_read_only = 1` and `max_execution_time = 10000` on every connection of the read pool (design §6.1,
§11). The grant is the real boundary.

```sql
CREATE USER 'piperoom_reader'@'<piperoom host>' IDENTIFIED BY '<from your secret store>'
  REQUIRE SSL WITH MAX_USER_CONNECTIONS 5;       -- the read pool opens at most 4 (design §6.1)

-- Only the tables listed under source_tables in owners.yaml (db.table on MySQL).
GRANT SELECT ON shop.orders      TO 'piperoom_reader'@'<piperoom host>';
GRANT SELECT ON shop.order_items TO 'piperoom_reader'@'<piperoom host>';   -- repeat per captured table

GRANT REPLICATION CLIENT, PROCESS ON *.* TO 'piperoom_reader'@'<piperoom host>';
GRANT SELECT ON performance_schema.* TO 'piperoom_reader'@'<piperoom host>';

-- The connector user's grants, without the password hash: named columns only.
GRANT SELECT (User, Host, account_locked, password_expired, Repl_slave_priv, Repl_client_priv,
              Select_priv, Reload_priv, Lock_tables_priv, Show_db_priv)
  ON mysql.user TO 'piperoom_reader'@'<piperoom host>';
GRANT SELECT ON mysql.db            TO 'piperoom_reader'@'<piperoom host>';
GRANT SELECT ON mysql.tables_priv   TO 'piperoom_reader'@'<piperoom host>';
GRANT SELECT ON mysql.role_edges    TO 'piperoom_reader'@'<piperoom host>';
GRANT SELECT ON mysql.default_roles TO 'piperoom_reader'@'<piperoom host>';

-- Amazon RDS / Aurora only (best effort; without it the tool says "RDS retention not readable"):
GRANT EXECUTE ON PROCEDURE mysql.rds_show_configuration TO 'piperoom_reader'@'<piperoom host>';
```

| Grant | What reads it (design section) | Without it |
|---|---|---|
| `SELECT` on the captured tables | `source_row` (by primary key), `source_table_replication` (`information_schema` shows only tables the user can see) (§6.3, §6.6) | the table is reported "missing, or not visible to the read user" |
| `REPLICATION CLIENT` | `mysql_binlog_status`: `SHOW BINARY LOG STATUS` (8.2+) or `SHOW MASTER STATUS`, `SHOW BINARY LOGS` (§6.7) | the tool names the missing privilege (§12) |
| `PROCESS` | other users' threads and `information_schema.innodb_trx`: long transactions and blocking sessions (§6.5, §6.6) | only the user's own sessions; the tool names the privilege |
| `SELECT ON performance_schema.*` | `global_variables`, `persisted_variables` (settings, §6.2), `threads`, `metadata_locks`, `session_connect_attrs` (blockers, §6.5) | the tool names the privilege; with the `wait/lock/metadata/sql/mdl` instrument off, lock holders are empty and the tool says so (**confirm**, §6.5) |
| column-level `SELECT` on `mysql.user` | `source_role_grants`: the connector account's lock, expiry and global privileges (§6.4) | grants unreadable |
| `SELECT` on `mysql.db`, `mysql.tables_priv`, `mysql.role_edges`, `mysql.default_roles` | database and table `SELECT` grants of the connector, and one level of roles active at login (§6.4) | those grants are not counted |
| RDS: `EXECUTE` on `mysql.rds_show_configuration` | `rds.binlog_retention_hours` (§6.2, §6.7) | "RDS retention not readable" |

Notes:

- **No `REPLICATION SLAVE`.** Piperoom never opens a replication connection or reads the binlog stream; Debezium
  does. The board's first Q8l text listed it and the design corrected it (design §2, §10.2, §13).
- **Never the password hash.** The client never runs `SHOW GRANTS` and never selects `authentication_string`; the
  column-level grant makes that a property of the account, not only of the code (design §6.4, §11). Do not grant
  `SELECT ON mysql.*` or `SELECT ON mysql.user` without a column list.
- **Do not** grant `SELECT ON *.*` or on whole databases. Row reads are limited to the pipeline's `source_tables`
  in code (design §9), but the credential is the real boundary, as on Postgres
  ([permissions.md](permissions.md#source-postgres)).
- **`PROCESS` and `performance_schema` show other sessions' statement text.** Piperoom keeps only the first
  keyword for long transactions and, for blocking sessions, the query redacted and cut to 200 bytes, wrapped as
  untrusted (design §6.5, §6.6, §11); the account itself can still read more than the tools return. This is the
  MySQL counterpart of `pg_read_all_stats`, which the Postgres page asks you not to grant; on MySQL the blocker and
  long-transaction tools need it. If that is too much, leave `PROCESS` out and accept that those tools see only
  the read user's own sessions.
- Roles are modelled one level deep and partial revokes are not modelled; the tool says so when
  `partial_revokes = ON` (design §6.4). **Confirm** the `mysql.user` column names on 8.0 and 8.4 and the role
  activation rule (Q8b).
- Never use the connector's user or an application user here.

## 3. Piperoom's write user (`SOURCE_DB_WRITE_URL`, only with write mode on)

Needed only for the three source admin actions on MySQL: `source_grant_select`, `source_create_heartbeat_table`,
`source_end_session` (design §6.8, §8.1). `pg_set_replica_identity` is not offered on MySQL (the MySQL writer's
`SetReplicaIdentity` answers "not supported"), and no `pgactions` action is registered for a MySQL pipeline
(design §3, §8.1). Without `SOURCE_DB_WRITE_URL`, or with `PIPEROOM_WRITE_MODE` off, no writer is built; the actions
still preview and are refused at Execute (design §6.8; the Q2b rule in [source-admin.md](source-admin.md)). Use a
different account from the read user: the "write URL is not the read URL" rule applies to MySQL too (design §6.1).

```sql
CREATE USER 'piperoom_writer'@'<piperoom host>' IDENTIFIED BY '<from your secret store>'
  REQUIRE SSL WITH MAX_USER_CONNECTIONS 3;       -- the write pool opens at most 2 (design §6.1)

-- source_grant_select: grantor of SELECT on the captured tables only.
GRANT SELECT ON shop.orders      TO 'piperoom_writer'@'<piperoom host>' WITH GRANT OPTION;
GRANT SELECT ON shop.order_items TO 'piperoom_writer'@'<piperoom host>' WITH GRANT OPTION;

-- source_end_session: KILL CONNECTION of another user's session.
GRANT CONNECTION_ADMIN ON *.* TO 'piperoom_writer'@'<piperoom host>';

-- source_create_heartbeat_table: in the database that holds the captured tables.
GRANT CREATE ON shop.* TO 'piperoom_writer'@'<piperoom host>';

-- The re-reads before each write (account resolution, the session's identity).
GRANT PROCESS ON *.* TO 'piperoom_writer'@'<piperoom host>';
GRANT SELECT ON performance_schema.* TO 'piperoom_writer'@'<piperoom host>';
GRANT SELECT (User, Host) ON mysql.user TO 'piperoom_writer'@'<piperoom host>';
```

| Action | What it runs (design §6.8) | Minimal grant | Reversible |
|---|---|---|---|
| `source_grant_select` | ``GRANT SELECT ON `db`.`t` TO 'user'@'host'`` per table, to the one account of the connector's user; several accounts with that user name are refused ("grant by hand or remove one") | `SELECT ... WITH GRANT OPTION` on those tables ([GRANT](https://dev.mysql.com/doc/refman/8.4/en/grant.html)) | yes, by a DBA's `REVOKE` |
| `source_create_heartbeat_table` | ``CREATE TABLE IF NOT EXISTS `db`.`t` (id INT NOT NULL PRIMARY KEY, ts TIMESTAMP(6) NOT NULL) ENGINE=InnoDB``; the name contains `heartbeat`, sits in a database of the pipeline's `source_tables`, and is not a source table | `CREATE` on that database | yes, by a DBA's `DROP TABLE` |
| `source_end_session` | re-read the thread on one pinned connection, then `KILL CONNECTION <pid>` | `CONNECTION_ADMIN` (`SUPER` is deprecated) ([KILL](https://dev.mysql.com/doc/refman/8.4/en/kill.html)) | no: the open transaction rolls back, and a large one can take as long to roll back as it ran |

Notes:

- **Grant option.** In MySQL the grant option at a level lets the user pass on every privilege it holds at that
  level. Here the writer holds only `SELECT` on each captured table, so `SELECT` on those tables is all it can
  give. It also means the write user can read those tables' rows: keep it to the captured tables, or leave it
  ungranted and let a DBA run the `GRANT`. Execute then fails with the missing privilege and changes nothing
  (design §6.1 error numbers, §6.8).
- **`CREATE` on the database** allows any new table in it, not one named table, as `CREATE` on a schema does on
  Postgres ([source-admin.md](source-admin.md#source_create_heartbeat_table)). If that is too much, a DBA creates
  the table and the action stays ungranted.
- **`CONNECTION_ADMIN` is server-wide**: it lets the account kill any session on the server. Piperoom's own checks
  refuse its pinned connection, any session of the read or write user, a `program_name` starting `piperoom-`, a
  `Binlog Dump` or `Binlog Dump GTID` thread (a connector's or a replica's stream), background threads,
  `system user` and `event_scheduler`, and any session that has no open transaction or whose transaction start
  changed (design §6.8, §14 D3). Those are Piperoom's checks, not MySQL's, so a leaked write password is not bound
  by them. On a shared server, leave `CONNECTION_ADMIN` ungranted: the action fails with "the write role lacks
  CONNECTION_ADMIN" (error 1095) and a DBA ends the session (design §6.1, §6.8).
- **A session with no open transaction is never ended by Piperoom** (a `LOCK TABLES` or `FLUSH TABLES WITH READ
  LOCK` holder): it has no stable identity. The room names it and the DBA ends it (design §14 D3).
- **Re-read grants.** The design says the writer needs "the read grants needed for the re-reads" (design §10.2).
  The three re-read grants above are this page's reading of §6.8 (account lookup in `mysql.user`, the thread in
  `performance_schema.threads` and `session_connect_attrs`, its transaction in `information_schema.innodb_trx`);
  **confirm** in Q8f.

## 4. Amazon RDS and Aurora MySQL

- **Connector:** add `LOCK TABLES` (section 1): there is no global read lock for the snapshot. **Confirm** against
  the Debezium MySQL page for your version.
- **Binlog retention** on RDS is `rds.binlog_retention_hours` (read with `CALL mysql.rds_show_configuration`),
  not `binlog_expire_logs_seconds`. Set it longer than the longest planned connector downtime (design §6.2). The
  read user needs `EXECUTE` on that procedure (section 2); **confirm** that the RDS master user can grant it.
- **Ending a session:** RDS may not let you grant `CONNECTION_ADMIN`. The design's recommended path (D7): the writer
  tries `KILL`; on error 1095 on RDS (`@@basedir` under `/rdsdbbin/`) it calls `CALL mysql.rds_kill(<pid>)`, which
  binds the id. Then the write user needs `GRANT EXECUTE ON PROCEDURE mysql.rds_kill TO 'piperoom_writer'@'<host>'`
  (design §14 D7; AWS: [Ending a session or query](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/mysql-stored-proc-ending.html)).
  This is a recommendation the owner adopted, not built code: **confirm** in Q8f.

## 5. The `mysql://` URL

`SOURCE_DB_URL` and `SOURCE_DB_WRITE_URL` (or a pipeline's `connections:` keys `source_db_url` and
`source_db_write_url`) hold a URL of this form (design §1 "DSN", §6.1):

```
mysql://piperoom_reader:<url-encoded password>@db.internal:3306/shop?tls=true&tls_ca=/run/secrets/mysql-ca.pem
```

- The pipeline's `source_kind` must match the scheme: `source_kind: mysql` with a `mysql://` URL. A mismatch fails
  the start with the pipeline id and both values (design §6.1, §12).
- **Parameters are an allow-list:** `tls` (`true`, the default off loopback; `false` only on a loopback host),
  `tls_ca` (a CA file) and `tls_server_name`. **Any other parameter stops the start**, which blocks
  `allowAllFiles` (`LOAD DATA LOCAL INFILE` would read the Piperoom host's files), `multiStatements`,
  `allowCleartextPasswords`, `allowOldPasswords` and the driver's "unknown parameter becomes a `SET`" path
  (design §6.1, §11). There is no go-sql-driver DSN syntax (`user:pass@tcp(host)/db`) and no pass-through.
- Refused: `mariadb://`, `mysqlx://`, and Unix sockets (`unix(`) (design §6.1).
- Port defaults to 3306 (design §4.2, §6.1).
- The client sets the session itself, never from the URL: no local-infile, no multi-statements, server-side
  prepared statements, UTC, 5 s connect and 10 s read/write timeouts, and the connection attribute
  `program_name` `piperoom-reader` (`piperoom-writer` on the write pool), the MySQL twin of Postgres
  `application_name` (design §6.1).
- The URL holds a password: keep it in your secret store and pass it by `connections:` reference
  ([setup.md](setup.md#2a-tenancy)); it is never logged (design §11).

## 6. `owners.yaml` (proposed, Q8d)

```yaml
pipelines:
  - id: orders-mysql
    kind: cdc
    source_kind: mysql        # postgres when absent
    topic_prefix: shop
    source_tables: [shop.orders, shop.order_items]   # db.table; the topic is shop.shop.orders
    connectors: [orders-mysql]
    owners:
      mysql: { users: [dana] }
```

The source layer of a MySQL pipeline is `mysql`, so its owners approve the source admin actions; `postgres:` is
refused in its `owners:` block, and so are `watch.slot`, `watch.wal_headroom_bytes` and the slot signals (design
§4.1, §4.2).

## Review checklist

- [ ] Three separate accounts: connector, Piperoom read, Piperoom write (only with write mode on).
- [ ] Read user: no `REPLICATION SLAVE`, no write privilege, no `SELECT` on `mysql.user` without a column list, no
  `SELECT ON *.*`.
- [ ] Captured tables only, the same list as `source_tables`.
- [ ] Write user: grant option on the captured tables only; `CONNECTION_ADMIN` only if a shared server allows it.
- [ ] `REQUIRE SSL` and `tls=true` off loopback; the CA file through `tls_ca`.
- [ ] Every grant above tried on staging, on your exact MySQL version or RDS/Aurora engine.
