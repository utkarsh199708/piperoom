# C3 — Snowflake warehouse client (Claude cloud agent, Sonnet)

Read `AGENTS.md`, `docs/collab.md`, `tasks/A3-db-tools-topology.md` first. You own a **new** package:
`internal/clients/snowflake/**`. Do not edit `internal/clients/warehouse/**` (A3's dev Postgres warehouse).

No Snowflake account and no Docker in the cloud: unit tests only, with a fake `database/sql` driver or
`sqlmock`-style test double (justify any new test dependency).

## Goal
Production customers use Snowflake. Implement `clients.Warehouse` for Snowflake behind the same interface,
so the topology builder and DB tools work unchanged.

## Work
1. `snowflake.New(ctx, dsn string) (*DB, error)` using the official Go driver `github.com/snowflakedb/gosnowflake`
   (verify the API with `go doc`; list the module + version in the PR).
2. `Freshness(ctx, table, tsColumn)` and `Row(ctx, table, pkColumn, pk)`:
   - identifiers: validate `^[A-Za-z_][A-Za-z0-9_$]*$` per part (`db.schema.table` allowed), then quote with Snowflake
     double-quote rules; values only as bind parameters (AGENTS.md rule 6 adapted to Snowflake).
   - read-only: use a role/session setting that prevents writes where possible; document the minimum grants
     (USAGE on warehouse/db/schema, SELECT on the tables).
   - 10 s default timeout via ctx; map "object does not exist" to `clients.ErrNotFound`.
   - convert values like `pgconv.ToJSON` does (numbers as strings, times RFC 3339).
3. Config: add nothing to the contract `internal/config`; take the DSN as a constructor argument. Describe in the PR
   how `cmd/server` should choose Postgres vs Snowflake (a `## Contract change request` if config needs a field).
4. Tests: identifier validation table, query text built correctly, error mapping, value conversion.

## Done when
`var _ clients.Warehouse = (*DB)(nil)` compiles, unit tests pass, PR lists grants and any contract change request.
