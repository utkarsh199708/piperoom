# The `oracle` profile (U9c)

An Oracle source for the setup agent's Oracle driver (U9a client, U9b driver): Oracle Database Free in
`dev/docker-compose.yml`, service `oracle`, configured the way the Debezium Oracle connector documents it.
Nothing else in the stack depends on it, and `devctl up` does not start it.

```
docker compose -f dev/docker-compose.yml --profile oracle up -d --wait oracle   # healthy when --wait returns
docker compose -f dev/docker-compose.yml --profile oracle rm -sf oracle         # stop and drop it (data included)
```

| What | Value |
|---|---|
| Image | `gvenzl/oracle-free:23.9` (pinned by digest); `ORACLE_IMAGE` overrides it |
| Listener | `localhost:1521` (host loopback only); CDB service `FREE`, PDB service `FREEPDB1` |
| `SYS` / `SYSTEM` | password `piperoom-dev` (dev only) |
| LogMiner user | `C##DBZUSER` / `piperoom-dev`, common user, grants of the Debezium docs |
| Captured table | `PIPEROOM.ORDERS` in `FREEPDB1` (100 rows; owner `PIPEROOM` / `piperoom-dev`) |
| Go DSN (U9a client, tests) | `ORACLE_DSN`, default `oracle://system:piperoom-dev@localhost:1521/FREEPDB1` |

**First start.** The image unpacks its database, then runs `initdb/01-cdc-setup.sh`, which runs
`cdc-setup.sh` as SYSDBA: a recovery area under `oradata/FRA` (10 GB cap), ARCHIVELOG (one database restart),
minimal supplemental logging, `C##DBZUSER` with its grants in every container, and `PIPEROOM.ORDERS` with an
`ALL COLUMNS` supplemental log group and `SELECT` for `C##DBZUSER`. A failing step aborts the container start, so the
container never turns healthy half-configured. The script checks before it changes anything; to restore the healthy
state by hand, run `docker exec piperoom-dev-oracle-1 bash /opt/piperoom/oracle/cdc-setup.sh`. Measured on a 4-core
Linux VM with the image already pulled: healthy after 31 s (23.9) and 53 s (23.26.3); the image is about 1.1 GB
compressed to pull.

**Grants are not a recommendation.** The `C##DBZUSER` grant set in `cdc-setup.sh` is the dev superset copied from the
Debezium docs, not a least-privilege recommendation for customers.

**Why 23.9 and not -slim.** The regular variant ships `DBMS_LOGMNR` and `DBMS_LOGMNR_D`; a LogMiner session as
`C##DBZUSER` read this table's changes on both 23.9 and 23.26.3. The `-slim` variants strip more components and were
not tried. 23.26.x calls itself "Oracle AI Database 26ai" in `V$VERSION`, and the U9a client's `Database` read
requires a banner starting "Oracle Database", so it fails there; the default stays on 23.9 until the client reads
both. Archives accumulate in the recovery area; when it fills (10 GB), the archiver stops and so do writes:
`rm -sf` and `up` again.

**Setup breaks.** `devctl setup-break` / `setup-fix` `no-archivelog`, `no-supplemental-logging`,
`missing-logminer-grant` (dev/scenarios.md, "Oracle setup breaks"). They need the profile running and take the
shared stack lock like every other setup break.
