# The `mongo` profile: a MongoDB source (U7c)

A one-member replica set `rs0` with auth, MongoDB 7.0 (`mongo:7.0.43`, pinned by digest in
`dev/docker-compose.yml`). It is the source the MongoDB setup checks (`internal/clients/mongodb`, U7a) and the
two MongoDB setup breaks run against. Nothing in the default stack depends on it, and `devctl up` does not start it.

```
docker compose -f dev/docker-compose.yml --profile mongo up -d --wait mongo   # healthy = rs0 initiated, this member PRIMARY
docker compose -f dev/docker-compose.yml --profile mongo down -v              # a plain `down` / `devctl down` leaves it running
```

| What | Value (dev-only, never a real secret) |
|---|---|
| Host address | `127.0.0.1:27017` (loopback only) |
| Root user | `piperoom_admin` / `piperoom-mongo-admin` (auth db `admin`) |
| Capture user | `piperoom_cdc` / `piperoom-dev` (auth db `admin`): `readAnyDatabase`, `read` on `local` (the oplog), `read` on `config` |
| Seeded data | `inventory.orders`, three documents |
| Host URI | `mongodb://piperoom_cdc:piperoom-dev@127.0.0.1:27017/?authSource=admin&directConnection=true` |
| URI on the compose network | `mongodb://piperoom_cdc:piperoom-dev@mongo:27017/?authSource=admin&replicaSet=rs0` |

The member is named `mongo:27017`, which only resolves on the compose network, so a client on the host connects
with `directConnection=true` instead of discovering the set. The client's integration test runs with
`PIPEROOM_IT_MONGODB_URI=<host URI> go test -tags integration ./internal/clients/mongodb/`.

## How it starts

- **keyFile.** Auth together with `--replSet` needs a keyFile, and mongod refuses one that is not mode `0400` and
  owned by the `mongodb` user. A file bind-mounted from a Windows checkout cannot carry either, so `start.sh`
  generates the key inside the container on first start (756 random bytes, base64) into `/tmp`, sets the owner and
  mode, then hands over to the image's `docker-entrypoint.sh`. A one-member set never shares its key, so a recreated
  container getting a new one is harmless.
- **Users.** `docker-entrypoint.sh` runs `init/01-capture-user.js` once, on an empty data directory, with
  `--replSet` and `--keyFile` stripped for that first start (the image's documented behaviour); the root user comes
  from `MONGO_INITDB_ROOT_*`.
- **Replica set.** The healthcheck is idempotent: `rs.status()` throws until the set exists, and then it runs
  `rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'mongo:27017'}]})`. It reports healthy only once this member is
  the writable primary. Measured on Linux (Docker 29.9, image already pulled): about 20 s from nothing to healthy, and
  under 10 s for a restart on an existing volume.
- **Memory.** The WiredTiger cache is capped at 256 MB (`start.sh`). By default it takes half the host's memory.

## Setup breaks

`devctl setup-break` / `setup-fix` (dev/scenarios.md, "Setup breaks: MongoDB") with `standalone-mongod` and
`no-oplog-role`. Both need the profile up, and their checks are the client's own reads, connected as the capture
user.
