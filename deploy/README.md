# Run Piperoom with Docker (pilot)

One image has the server and the web UI. One compose file starts it with its own Postgres.
You need Docker with Compose v2, and network access from the Docker host to your source
database, Kafka, Kafka Connect, Schema Registry, the warehouse, and `api.anthropic.com`.

## Read this first: network exposure

Piperoom has **no real login yet**. A user is whoever the `X-User` header says. Anyone who can
reach the port can act as any user, **including approving fixes**. So:

- Run it behind the customer's **VPN** or an **authenticating reverse proxy**. Never publish
  it to the internet.
- A proxy that does the authenticating must control **both** places where the server reads
  the identity: the `X-User` header (set it itself, strip any client value) **and** the
  `?user=` query parameter on the WebSocket route `/api/rooms/{id}/ws`, because browsers
  cannot send headers on a WebSocket. Force `user` to the authenticated user there, or
  reject requests whose `user` differs. Otherwise a logged-in person can watch a room
  as someone else.
- Inside the container the server must listen on `0.0.0.0`, so the image starts it with
  `--allow-remote` (the server refuses non-loopback addresses without it).
- The compose file publishes the port on `127.0.0.1` only. To let other machines reach it, set
  `PIPEROOM_BIND` in `deploy/.env` to the host's VPN address (better) or `0.0.0.0`.

More limits: [docs/pilot/README.md](../docs/pilot/README.md#read-this-first-pilot-limits).

## The 10-minute path

Run these from the repository root.

1. **Create the read-only accounts** that Piperoom uses on your systems. Follow
   [docs/pilot/permissions.md](../docs/pilot/permissions.md) and grant exactly what it lists:
   a read-only role on the source Postgres and on the warehouse, Kafka describe/read,
   Connect and Registry `GET` (write access only if you want fixes to run; read-only is a
   valid, safe pilot mode). Use a staging or replica pipeline first.

2. **Copy the settings file and fill it in.**

   ```sh
   cp deploy/pilot.env.example deploy/.env
   ```

   In `deploy/.env` set at least:

   | Setting | What |
   |---|---|
   | `ANTHROPIC_API_KEY` | the agent's API key (`ANTHROPIC_WORKSPACE_ID` too, for a user key). Only for the default `anthropic` provider; for AWS Bedrock or Google Vertex AI see [Model provider](#model-provider) |
   | `PIPEROOM_POSTGRES_PASSWORD` | the bundled Postgres password. Generate it with `openssl rand -hex 32`. It goes into a URL unescaped, so it must not contain `/ @ : # ? % $` |
   | `SOURCE_DB_URL`, `WAREHOUSE_DB_URL` | the read-only connection URLs |
   | `KAFKA_BROKERS`, `CONNECT_URL`, `SCHEMA_REGISTRY_URL` | your hosts. `localhost` inside the container is the container itself, so use real host names |

   Add Kafka/Registry/Connect logins and TLS in sections 5 to 7 of the file if your clusters
   need them. Put certificate files in `deploy/certs/` and refer to them as `/certs/<file>`.
   `deploy/.env` holds secrets and is git-ignored. Compose reads `$` in this file as the start
   of a variable, so write a literal `$` as `$$` (for example in a password).

3. **Say who owns what.** Copy the two examples and edit them (users, layers, your tables):

   ```sh
   cp deploy/config/owners.example.yaml deploy/config/owners.yaml
   cp deploy/config/users.example.yaml  deploy/config/users.yaml
   ```

   The guide is [docs/pilot/setup.md](../docs/pilot/setup.md#2-describe-your-pipeline-ownersyaml-and-usersyaml).
   The server refuses to start if the two files disagree; the error names the user and layer.

4. **Start it.**

   ```sh
   docker compose -f deploy/docker-compose.pilot.yml up
   ```

   The first run builds the image (a few minutes) and starts Postgres, which stores Piperoom's
   own room log in the `piperoom-pgdata` volume. Database migrations run by themselves.
   Add `-d` to run in the background.

5. **Open it:** `http://<host>:8090` (`http://localhost:8090` on the same machine; with the
   default `PIPEROOM_BIND` that is the only place it works). Pick a user from `users.yaml`.

## Check that it works

```sh
docker compose -f deploy/docker-compose.pilot.yml ps              # piperoom should be "healthy"
curl http://localhost:8090/healthz                                # {"status":"ok"}
curl http://localhost:8090/readyz                                 # also checks the room database
curl -H 'X-User: alice' http://localhost:8090/api/me              # your user, from users.yaml
docker compose -f deploy/docker-compose.pilot.yml logs -f piperoom
```

Then follow [docs/pilot/setup.md](../docs/pilot/setup.md#5-verify-with-a-dry-run-no-real-alert-needed)
(dry run, read-only check, approval gate), then connect your alerts (next section).

## Alerts

Alert senders call the signed routes `POST /api/alerts/{source}`, where `{source}` is
`alertmanager`, `grafana`, `pagerduty`, `datadog` or `generic`. Each one needs its secret in
`deploy/.env` (section 10 of the file); a source with an empty secret answers 401. Sender
setup for each system is in [internal/alertin/README.md](../internal/alertin/README.md).

These routes have **no `X-User`**: the sender's token or signature is the login. That matters
behind a proxy:

- If the proxy requires a login for everything, it must let `POST /api/alerts/<source>` through
  without one. PagerDuty, Datadog and Grafana Cloud cannot log in to your proxy.
- Expose **only those paths** to the senders, over HTTPS, ideally limited to the sender's
  published IP ranges. Do not expose the rest of the API.
- `POST /api/alerts/webhook` is a different route: the dev webhook, with no sender check. Keep it
  closed at the proxy.
- With the default `PIPEROOM_BIND=127.0.0.1`, no sender can reach the port at all. Put the
  proxy on the host (or bind to the address the proxy uses) and publish a URL such as
  `https://piperoom.corp.example/api/alerts/pagerduty`.

## Model provider

`PIPEROOM_MODEL_PROVIDER` chooses where the agent's model runs. Empty means `anthropic`. If a
setting is wrong or the cloud credentials cannot be found, the server stops at start-up and the
error names the setting. `PIPEROOM_MODEL` (and the optional `PIPEROOM_TRIAGE_MODEL`) must be an
id that the chosen provider knows. `PIPEROOM_ROOM_TOKEN_BUDGET` caps the tokens one room may
use (empty is 2,000,000; `0` is no cap).

| Provider | Set in `deploy/.env` |
|---|---|
| `anthropic` (default) | `ANTHROPIC_API_KEY` (and `ANTHROPIC_WORKSPACE_ID` for a user key). Needs egress to `api.anthropic.com` |
| `bedrock` | `PIPEROOM_MODEL_PROVIDER=bedrock`, `AWS_REGION`, a Bedrock model or inference-profile id in `PIPEROOM_MODEL`, and one credential source (below) |
| `vertex` | `PIPEROOM_MODEL_PROVIDER=vertex`, `VERTEX_PROJECT_ID`, `VERTEX_REGION`, a Vertex model id in `PIPEROOM_MODEL`, and Google credentials (below) |

**Bedrock.**

- Models are named by Bedrock ids. To keep traffic in India use an India inference profile,
  whose id starts with `in.` (for example `in.anthropic.<model>`), with `AWS_REGION` set to an
  Indian region. Take the exact id from the Bedrock console.
- A **cross-region** inference profile (a prefix such as `in.`, `us.`, `eu.`, `apac.` or
  `global.`) can process a request in another region than `AWS_REGION`. If data must not leave
  a region or country, check the profile's list of regions before you use it.
- Credentials, from easiest to hardest: a Bedrock API key in
  `AWS_BEARER_TOKEN_BEDROCK`; static keys in `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
  (plus `AWS_SESSION_TOKEN` for temporary ones); an instance or task **role**, with nothing
  set (on EC2 the IMDSv2 hop limit must be 2 for containers); on Kubernetes **IRSA**, whose
  variables the cluster injects (this compose file is for a Docker host). Give the identity
  only `bedrock:InvokeModel` and `bedrock:InvokeModelWithResponseStream` on the models you use.
- `AWS_PROFILE` works only if you mount a config file. `~/.aws` is **not** mounted by default;
  the commented lines under `volumes` in the compose file show how to mount one profile file.
  The container runs as uid 65532, so give those files to that user:
  `sudo chown 65532:65532 <file> && chmod 0400 <file>`. A file that is mode 0600 and owned
  by you cannot be read inside the container.
- With an instance or task role, add `169.254.169.254` (EC2 IMDS) and `169.254.170.2` (ECS
  credentials) to `NO_PROXY` if you use a proxy (see [Outbound proxy](#outbound-proxy)).
- The server checks that credentials resolve at start-up, so a missing credential fails the
  start rather than the first alert. It does not call the model at start-up.

**Vertex AI.**

- `VERTEX_REGION` is a region such as `europe-west4`, or `global`, `us` or `eu`. **`global`,
  `us` and `eu` are multi-region endpoints**: the request can be served in any region of that
  area, so choose a single region if data must stay in one place.
- Credentials use Google application default credentials. Either run where the metadata
  server provides them (a GCE VM), or use a service-account key file: save it as
  `deploy/config/gcp-sa.json` (git-ignored; `deploy/config` is already mounted at `/config`),
  set `GOOGLE_APPLICATION_CREDENTIALS=/config/gcp-sa.json`, and give the service account only
  the Vertex AI User role. To keep the key outside `deploy/config`, use the commented extra
  mount in the compose file. Treat the key file as a secret. The container runs as uid 65532,
  and a bind-mounted file that is mode 0600 and owned by you is unreadable to it, so run
  `sudo chown 65532:65532 deploy/config/gcp-sa.json && chmod 0400 deploy/config/gcp-sa.json`.

`ANTHROPIC_BASE_URL` is deliberately not passed to the container (see the comment in the
compose file): a value in the host environment must not be able to redirect model traffic.

## Outbound proxy

If the host reaches the internet through a proxy, set `HTTPS_PROXY` (and `HTTP_PROXY` if you
need it) in `deploy/.env`, and `NO_PROXY` for the hosts that must go direct, such as your
internal Connect and Schema Registry. They apply to the HTTP(S) calls: the model APIs, Slack,
Connect, Schema Registry. Kafka and Postgres use plain TCP and do not use the proxy, so the
container must reach them directly. A proxy that re-signs TLS needs its CA in the image; that
is not covered here.

- Use the UPPER-CASE names. Lower-case `https_proxy` or `no_proxy` in `deploy/.env` are
  ignored, and a lower-case variable exported in your shell is not passed to the container.
- **Bedrock with an instance or task role:** add `169.254.169.254` (EC2 IMDS) and
  `169.254.170.2` (ECS credentials) to `NO_PROXY`. The AWS SDK fetches role credentials over
  plain HTTP and follows the proxy variables, so without this the request goes to your proxy
  and fails.
- Google's metadata client ignores the proxy variables, so Vertex on a GCE VM needs nothing.

## Run compose from a clean shell

Compose gives a variable exported in your shell priority over `deploy/.env`. A developer's own
`AWS_*`, `ANTHROPIC_*`, `HTTPS_PROXY` or `PIPEROOM_*` could therefore reach the container
without being in the file. Run compose from a clean shell (for example
`env -i PATH="$PATH" HOME="$HOME" docker compose ...`), or check first with
`docker compose -f deploy/docker-compose.pilot.yml config`, which prints the values the
container will get (it shows secrets, so do not paste the output).

## Data retention

`PIPEROOM_RETENTION_DAYS` is **live**. It deletes whole closed rooms (events, proposals,
approvals and hand-offs) that were closed more than that many days ago. The check runs every
6 hours. Open rooms are never deleted. Empty or `0` keeps everything. A value must be a whole number
from 0 to 36500; anything else stops the server at start-up.

Deletion cannot be undone. If you must keep a room, export it before it ages out:

```sh
curl -H 'X-User: alice' http://localhost:8090/api/rooms/<room id>/export > room.json
```

The export carries a SHA-256 over the JCS (RFC 8785) form of the room with the top-level
`integrity` member removed (also sent in the `X-Piperoom-Export-SHA256` header), so a plain
`sha256sum` of the file will not match it. Use it to check later that the file was not changed.
Any user in `users.yaml` may export.

## Watcher

The built-in watcher is live. The four `PIPEROOM_WATCH_*` settings are passed through to the server.
Empty means the default, which is mode `log` (shadow mode: it only writes log lines and opens no
room). Set `PIPEROOM_WATCH_MODE=rooms` to let it open rooms. A bad value stops the server at start
with an error that names the variable. `--demo` always runs with the watcher off. The source database
URL must point at the primary, PostgreSQL 13 or newer. See
[setup.md](../docs/pilot/setup.md#1-configure-the-environment).

## Try the UI with no stack (demo mode)

No database, keys or data stack needed. Scripted agent, in-memory, nothing is saved:

```sh
docker compose -f deploy/docker-compose.pilot.yml run --rm --no-deps --service-ports piperoom --demo
```

`--no-deps` keeps the bundled Postgres from starting; the image already adds `--allow-remote`.
Demo mode still needs:

- `deploy/.env` with `PIPEROOM_POSTGRES_PASSWORD` set to any value (compose refuses to read
  the file without it, even though demo mode never connects to the database);
- `deploy/config/owners.yaml` and `deploy/config/users.yaml` (step 3). They must agree. If
  `users.yaml` is missing, demo mode uses built-in demo users, which only fit the dev
  `owners.yaml`, not yours.

It needs no API key and no data stack. Do not use demo mode for a real incident.

## What is in the image

| | |
|---|---|
| Build | `Dockerfile` at the repo root: Node 24 builds `web/dist`, Go 1.27 builds `cmd/server` (`CGO_ENABLED=0`, `-trimpath`), then both go into `gcr.io/distroless/static-debian13:nonroot` |
| Size | about 100 MB on disk (the server binary is about 73 MB, with the Anthropic, AWS and Google SDKs in it); about 22 MB compressed to pull |
| Runs as | uid 65532 (`nonroot`), read-only root filesystem in compose, all capabilities dropped |
| Listens on | `0.0.0.0:8090` (`PIPEROOM_HTTP_ADDR`); the UI is served from `/web` (`PIPEROOM_WEB_DIR`) at `/`, the API at `/api` |
| Health | `HEALTHCHECK` runs `/piperoom --healthcheck`, which calls `GET /healthz` (distroless has no curl). `GET /readyz` also checks the room database, and suits a load balancer or Kubernetes readiness probe |
| Browser rules | The UI calls the API and WebSocket on its own origin, so no CORS setup is needed (CORS and the `localhost:5173` dev origins are off when the server serves the UI). The server sends a CSP, `X-Content-Type-Options`, `Referrer-Policy` and `frame-ancestors 'none'` |

Build the image alone: `docker build -t piperoom:pilot .`

Behind a reverse proxy that changes the `Host` header, set `PIPEROOM_PUBLIC_URL` (for example
`https://piperoom.corp.example`) so WebSocket connections from that address are accepted.
It must be a plain `http(s)` URL with a host name or IPv4 address (no wildcard, user name or
query); anything else stops the server at start-up with an error that names the variable.
The proxy must pass WebSocket upgrades on `/api/rooms/<id>/ws`.

## Day-2 operations

```sh
docker compose -f deploy/docker-compose.pilot.yml up -d --build   # after a git pull
docker compose -f deploy/docker-compose.pilot.yml down            # stop; data stays in the volume
docker compose -f deploy/docker-compose.pilot.yml down -v         # stop AND delete the room database
```

On a restart the server rebuilds every room from its log and starts a new agent for each open
room. A fix that was approved but not finished is marked failed, not re-run
([setup.md](../docs/pilot/setup.md#what-happens-on-a-restart)).

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `set PIPEROOM_POSTGRES_PASSWORD in deploy/.env` | step 2 not done, or `deploy/.env` is missing |
| `env file .../deploy/.env not found` | run `cp deploy/pilot.env.example deploy/.env` |
| Exits with `users/owners config` | `users.yaml` and `owners.yaml` disagree about a layer owner |
| Exits with `load users file` or `load owners file` | `deploy/config/users.yaml` or `owners.yaml` is missing (step 3) |
| Exits with `PIPEROOM_PUBLIC_URL ... invalid host` (or similar) | the URL has a wildcard, a user name, a query or a bad port (see above) |
| Exits with `PIPEROOM_WEB_DIR ... has no index.html` | the image was built without the UI; rebuild with `up --build` |
| Exits with `open the data stack` | a URL or login in `deploy/.env` is wrong or unreachable from the container |
| Page does not load from another machine | `PIPEROOM_BIND` is still `127.0.0.1` |
| UI loads but live updates fail behind a proxy | the proxy does not pass WebSocket upgrades, or `PIPEROOM_PUBLIC_URL` is not set |
| No agent answer | the model provider's credentials are missing, or egress to its API is blocked (behind a proxy, set `HTTPS_PROXY` and `NO_PROXY`) |
| Exits at start with an error about the model provider, region, project or credentials | see [Model provider](#model-provider); the error names the setting |
| A password with `$` is cut short | write `$` as `$$` in `deploy/.env` |
| An alert sender gets 401 | its secret in `deploy/.env` is empty or wrong, or the proxy is asking it to log in (see [Alerts](#alerts)) |

## Not covered here

- Images are pinned by tag, not digest (`node:24-alpine`, `golang:1.27-alpine`,
  `distroless/static-debian13:nonroot`, `postgres:17`). Pin digests for a regulated site.
- The UI loads two fonts from Google Fonts. On a network with no internet the page still works
  with fallback fonts; the CSP allows only those two hosts for this.
- TLS: put a reverse proxy in front for HTTPS. The server speaks plain HTTP.
