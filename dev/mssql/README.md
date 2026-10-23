# SQL Server source: the `mssql` profile (U4c)

SQL Server 2022 Developer (CU27 GDR1, pinned by digest in `../docker-compose.yml`) with SQL Server Agent on and
**CDC off**: enabling CDC on the database and the table is the setup run's job. The image's setup hook runs
`init/01-orders.sql` on a fresh data directory: database `orders`, table `dbo.orders` (primary key, the same shape as
the Postgres `orders`), 100 rows. The profile depends on nothing, so it starts alone.

```
docker compose -f dev/docker-compose.yml --profile mssql up -d --wait mssql
docker compose -f dev/docker-compose.yml --profile mssql rm -sf mssql      # stop and remove it (data goes with it)
```

| What | Value |
|---|---|
| Host address | `127.0.0.1:11433` (not 1433, so a SQL Server installed on the dev machine keeps its port) |
| Login | `sa` / `Piperoom-dev-1` (dev-only; the password meets SQL Server's complexity rule) |
| DSN (`PIPEROOM_MSSQL_DSN`, the client's integration tests) | `sqlserver://sa:Piperoom-dev-1@localhost:11433?database=orders&encrypt=disable` |
| sqlcmd in the container | `/opt/mssql-tools18/bin/sqlcmd -C` (tools18 encrypts by default; `-C` trusts the self-signed cert) |

The image is amd64 only (Microsoft publishes no arm64 SQL Server image); on Apple silicon Docker runs it under
emulation. Measured on Linux, 2026-10-23: healthy 39 s after `up` with the image pull, 12 s with the image cached.

## Setup breaks

| `devctl setup-break …` | Named check (client call that fails) | Break | Fix (`devctl setup-fix …`) |
|---|---|---|---|
| `agent-stopped` | `sqlserver-agent-running` (`AgentState().Running` false) | `xp_servicecontrol STOP SQLServerAGENT` | `xp_servicecontrol START`; a container restart also starts it (`MSSQL_AGENT_ENABLED=true`) |
| `cdc-not-enabled-table` | `sqlserver-table-cdc` (`TableCDC` reports `dbo.orders` with `CDCEnabled` false while `DatabaseCDC("orders")` is true) | `sp_cdc_enable_db` if needed, then `sp_cdc_disable_table` on `dbo.orders` | `sp_cdc_enable_table` on `dbo.orders` (no gating role, net changes) |

Both are idempotent: a break on a stack that already fails the check, or a fix on one that passes, only reads.
The table check is defined for a CDC-enabled database, so on the fresh profile (CDC off everywhere) it passes and
`setup-fix cdc-not-enabled-table` is a no-op; the database check is the one that fails there.

`go test -tags integration -run TestMSSQLSetupBreaksIntegration ./cmd/devctl/` runs both pairs on the running
profile and reads each check with the SQL Server client (`internal/clients/sqlserver`); it skips when the profile
is not up.
