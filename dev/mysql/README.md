# MySQL profile (Q8a)

MySQL 8.4 LTS as a second CDC source of the dev stack, read by the Debezium MySQL connector `orders-mysql`
(`dev/connectors/orders-mysql.json`) on the default `connect` worker. Design: `docs/design/mysql-source.md` §10.1.
Breaks: `dev/scenarios.md` §25-§28.

```
go run ./cmd/devctl up --owner <tag>
docker compose -f dev/docker-compose.yml --profile mysql up -d --wait mysql
```

The first `devctl break` or `fix` of a MySQL scenario registers `orders-mysql` when it is missing. To register it by
hand: `curl -X PUT -H 'Content-Type: application/json' --data @dev/connectors/orders-mysql.json
http://localhost:8083/connectors/orders-mysql/config`.

- Server: `--gtid-mode=ON --enforce-gtid-consistency=ON --binlog-format=ROW --binlog-row-image=FULL
  --server-id=223344 --binlog-expire-logs-seconds=86400`.
- Port: `127.0.0.1:13306` on the host (not 3306, so a local MySQL keeps its port); `mysql:3306` inside the network.
- Data: database `shop`, tables `orders` and `order_items` (100 rows each), created by `init/01-shop.sql` on a fresh
  volume. `docker compose -f dev/docker-compose.yml --profile mysql down -v` starts it over.
- Piperoom server on it (once Q8b and Q8d land): `OWNERS_FILE=dev/owners.mysql.yaml`,
  `SOURCE_DB_URL=mysql://piperoom:piperoom-dev@localhost:13306/shop`.

Users (dev-only passwords, local stack only, AGENTS.md §3; grants of the design's §10.2):

| User | Password | For |
|---|---|---|
| `root` | `piperoom-dev` | devctl breaks (`docker exec`, password through an option file, never argv) |
| `debezium` | `dbz-dev` | the connector: `SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT` |
| `piperoom` | `piperoom-dev` | Piperoom read (`SOURCE_DB_URL`): `SELECT` on the tables, `REPLICATION CLIENT`, `PROCESS`, `performance_schema`, the listed `mysql.user` columns; no `REPLICATION SLAVE` |
| `piperoom_write` | `piperoom-dev-write` | Piperoom write (`SOURCE_DB_WRITE_URL`): `SELECT ... WITH GRANT OPTION`, `CONNECTION_ADMIN`, `CREATE` on `piperoom_heartbeat` |

Not observed yet: written on 2026-10-23 without a Docker daemon. `docker compose -f dev/docker-compose.yml --profile
mysql config -q` passes; the image, the init script and the connector have not run.
