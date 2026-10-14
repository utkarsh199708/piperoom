# Run Piperoom with Docker (pilot)

One image has the server and the web UI. One compose file starts it with its own Postgres.
You need Docker with Compose v2, and network access from the Docker host to your source
database, Kafka, Kafka Connect, Schema Registry, the warehouse, and `api.anthropic.com`.

## Read this first: network exposure

Piperoom has two login modes (`PIPEROOM_AUTH_MODE`):

- **`dev`** (the default, and what the image starts in). You open the UI and pick a user from
  `users.yaml`. There is **no password**. Anyone who can reach the port can pick any user,
  **including approving fixes**.
- **`oidc`**: real single sign-on through your identity provider (Okta, Entra, Keycloak, Google, ...).
  Set it up with [Single sign-on (OIDC)](#single-sign-on-oidc) below. This is the mode for anything
  that is reachable by more than the people you trust on a VPN.

So:

- Pilots build from source with `deploy/docker-compose.pilot.yml`. There is no published image.
- **Dev mode:** run it on a laptop, behind the customer's **VPN**, or behind an SSH tunnel. Never
  publish it to the internet.
- **A TLS reverse proxy is supported in `oidc` mode only.** Dev mode cannot trust an identity
  header from a proxy: the browser logs in with its own cookie, which a proxy cannot force, and a
  request that carries `X-User` together with a cookie for another user (or calls `/auth/dev-login`)
  gets `403 proxy_identity`. A pilot that runs behind a proxy that sets `X-User` and forces
  `?user=` has two choices. Switch to `oidc` mode (see below): then it runs on the current code
  and no longer needs the git tag `pre-auth`. Or, if it cannot use OIDC yet, stay on the git tag
  `pre-auth` (`git checkout pre-auth`, then rebuild with `up -d --build`). Do not upgrade a
  proxy pilot to the current code and leave it in dev mode.
- Inside the container the server must listen on `0.0.0.0`, so the image sets
  `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1` (dev mode refuses a non-loopback address without it
  and logs a loud warning when it is on). In `oidc` mode the variable is ignored, with a
  warning (see [the expected warning](#expected-start-up-messages)). The old `--allow-remote`
  flag still works in dev mode for one release but is deprecated; use the variable.
- The compose file publishes the port on `127.0.0.1` only. To let other machines reach it, set
  `PIPEROOM_BIND` in `deploy/.env` to the host's VPN address (better) or `0.0.0.0`. With a
  proxy on the same host, keep `127.0.0.1` and point the proxy at it.
- **Host check.** The server answers `421 bad_host` to a request whose `Host` header is not
  a loopback name (any port), the host of `PIPEROOM_PUBLIC_URL`, or a value in
  `PIPEROOM_ALLOWED_HOSTS`. The allow-list counts in every mode. The `PIPEROOM_PUBLIC_URL` host
  counts always in `oidc` mode, and in dev mode only with `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1`, which
  the image sets. Set `PIPEROOM_ALLOWED_HOSTS`
  (comma-separated, `host` or `host:port`, compared as written) when a tunnel or proxy
  rewrites `Host`, for example an SSH tunnel to `localhost:9000`, or a name the VPN uses
  for the machine. Signed alert routes `POST /api/alerts/{source}` and `GET /healthz`,
  `GET /readyz` skip this check. The unsigned dev webhook does not.
- **Export.** Today a member may export a room, and a viewer may export only if named in
  `auditors:` (see [Identity and rights](#identity-and-rights)). From auth PR 4, members
  who are not owners, backups or auditors cannot export and must be added to `auditors:`.

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
   | `KAFKA_BROKERS`, `CONNECT_URL`, `SCHEMA_REGISTRY_URL` | your hosts (`SCHEMA_REGISTRY_URL`: `off` when the pipelines use no registry; compose requires a value). `localhost` inside the container is the container itself, so use real host names |

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
   default `PIPEROOM_BIND` that is the only place it works). This is dev mode; for real login
   see [Single sign-on (OIDC)](#single-sign-on-oidc). The login screen lists the users
   from `users.yaml`: pick one. That is the **dev login**; the page sets a session cookie
   (`piperoom_session`) and the user switcher in the UI changes user later. There are no more
   `?user=` links: a link cannot choose the user. Behind an SSH tunnel to a different port,
   or by a name other than `localhost`, set `PIPEROOM_ALLOWED_HOSTS` (see the Host check above).

## Check that it works

```sh
docker compose -f deploy/docker-compose.pilot.yml ps              # piperoom should be "healthy"
curl http://localhost:8090/healthz                                # {"status":"ok"}
curl http://localhost:8090/readyz                                 # also checks the room database
curl -H 'X-User: alice' http://localhost:8090/api/me              # your user, from users.yaml
docker compose -f deploy/docker-compose.pilot.yml logs -f piperoom
```

**Using curl in dev mode.** Every `POST` needs `-H 'Content-Type: application/json'`, except
the signed `POST /api/alerts/{source}` routes, which take the sender's own content type.
Without it the server answers `400 bad_request` (this is part of the browser-request
protection). curl may still send `X-User: <id>` when it sends no cookie; that works only in
dev mode. Send exactly one `X-User` value: more than one (several header lines, or one line
with a comma) answers `403 proxy_identity`, with or without a cookie. If you send a session
cookie too, the `X-User` must name the same user, or the answer is `403 proxy_identity`.
In `oidc` mode `X-User` is ignored.

```sh
curl -X POST -H 'X-User: alice' -H 'Content-Type: application/json' \
  -d '{"title":"test","severity":"sev2","pipeline_id":"<pipeline id>"}' \
  http://localhost:8090/api/rooms
```

Then follow [docs/pilot/setup.md](../docs/pilot/setup.md#5-verify-with-a-dry-run-no-real-alert-needed)
(dry run, read-only check, approval gate), then connect your alerts (next section).

## Single sign-on (OIDC)

In `oidc` mode people sign in at your identity provider (IdP): Okta, Microsoft Entra, Keycloak,
Google, or any OpenID Connect provider. Piperoom stores no passwords. The server runs the
authorization-code flow itself, with **PKCE (S256) and a nonce always on**. After a good login it
keeps its own session in a cookie (`__Host-piperoom_session`) and throws away every IdP token.
Logging out of Piperoom (`POST /auth/logout`) ends that session only; it does not sign the
person out of the IdP.

`oidc` mode makes three things different from dev mode: `X-User` is ignored and `/auth/dev-login` does
not exist, the server does not mind listening on `0.0.0.0`, and a TLS reverse proxy in front of it
is supported.

### 1. Register Piperoom at your IdP

Create an OpenID Connect **web application** (authorization-code grant) and set:

| At the IdP | Value |
|---|---|
| Redirect URI (sign-in redirect) | `<PIPEROOM_PUBLIC_URL>/auth/callback`, for example `https://piperoom.corp.example/auth/callback`. It must match exactly. The server builds it from the scheme and host of `PIPEROOM_PUBLIC_URL`: a default port (`:443`) is dropped, any other port is kept, and a path in the URL is ignored. This is the only redirect URI Piperoom uses |
| Scopes | `openid email profile` (the default). `openid` is required; the server refuses to start without it. `email` gives the email used to match `users.yaml`; `profile` gives the display name. If your IdP only puts the groups claim into the token when a scope asks for it, add that scope to `PIPEROOM_OIDC_SCOPES` |
| Groups claim | Needed only if you use `group:` entries in `owners.yaml` or `PIPEROOM_AUTH_VIEWER_GROUPS`. Add a claim named `groups` (or the name you set in `PIPEROOM_OIDC_GROUPS_CLAIM`) to the **ID token**. The server reads claims from the ID token only; it does not call the userinfo endpoint. The value is a list of strings, or one string. Most IdPs need a mapper or a claim rule for this. Entra sends group object ids and **drops the claim when a person has too many groups** (the overage case, see [groups_missing](#troubleshooting)). Google sends no groups: map Google users with `users.yaml` |
| PKCE | Always sent (`S256`). If your IdP can require PKCE for this app, you may switch that on |
| Client type | **Confidential** (a client secret) is the default and what Okta, Entra and Google issue for a server app. Some IdPs only offer a **public** client for this: leave `PIPEROOM_OIDC_CLIENT_SECRET` empty and set `PIPEROOM_OIDC_PUBLIC_CLIENT=1`. PKCE still protects the code. The server logs a warning at start-up for this case |
| ID token claims | `sub` and `iss` always. For matching by email the token needs `email` and `email_verified: true`. An `email_verified` of the string `"true"` also counts |

Then copy the issuer URL, the client id and (for a confidential client) the client secret.

### 2. Settings in `deploy/.env`

Compose passes all of these to the container (they are listed in
`deploy/docker-compose.pilot.yml`). The names match `internal/config/config.go`.

| Variable | Default | What |
|---|---|---|
| `PIPEROOM_AUTH_MODE` | `dev` | set to `oidc` |
| `PIPEROOM_PUBLIC_URL` | empty | **required, and it must be `https`**, for example `https://piperoom.corp.example`. This is the address people type. It sets the redirect URI, the cookie host and the `Host` the server accepts. A plain `http` URL, or none, stops the start |
| `PIPEROOM_OIDC_ISSUER` | empty | **required.** The issuer URL of the IdP, written exactly as in its discovery document (`<issuer>/.well-known/openid-configuration`) |
| `PIPEROOM_OIDC_CLIENT_ID` | empty | **required** |
| `PIPEROOM_OIDC_CLIENT_SECRET` | empty | **SECRET.** Required unless `PIPEROOM_OIDC_PUBLIC_CLIENT=1`. Never logged |
| `PIPEROOM_OIDC_PUBLIC_CLIENT` | empty | `1` allows an empty secret (PKCE only) |
| `PIPEROOM_OIDC_SCOPES` | `openid email profile` | space-separated; must include `openid` |
| `PIPEROOM_OIDC_GROUPS_CLAIM` | `groups` | the ID-token claim with the groups. An **empty** value turns group rights off. See the compose note below |
| `PIPEROOM_AUTH_VIEWER_GROUPS` | empty | comma-separated group values whose members may log in as read-only **viewers** (see [Roles](#roles)) |
| `PIPEROOM_SESSION_IDLE` | `8h` | a session with no request for this long ends. Go duration (`30m`, `8h`) |
| `PIPEROOM_SESSION_MAX` | `12h` | a session ends this long after login, however busy. Go duration. It also bounds how stale a person's groups can be: groups are read at login only |
| `PIPEROOM_TRUSTED_PROXIES` | empty | comma-separated CIDRs or addresses of your reverse proxies (`10.0.0.0/8`, `172.18.0.5`). `X-Forwarded-For` is read only from these peers. See [Behind a TLS reverse proxy](#behind-a-tls-reverse-proxy) |
| `PIPEROOM_ALLOWED_HOSTS` | empty | extra `Host` values (`host` or `host:port`) for a proxy that rewrites `Host` |
| `PIPEROOM_SOLO_USER` | empty | solo mode, see [Solo mode](#solo-mode) |

A bad value stops the start with an error that names the variable. In `deploy/.env`,
**leave `PIPEROOM_OIDC_GROUPS_CLAIM` out** unless your IdP uses another claim name. The compose
file writes it as `${PIPEROOM_OIDC_GROUPS_CLAIM-groups}` (no colon): an unset variable gives
`groups`, and an intentional empty value (`PIPEROOM_OIDC_GROUPS_CLAIM=`) turns group rights
off. Do not "tidy" it to `:-`.

Minimal example:

```sh
PIPEROOM_AUTH_MODE=oidc
PIPEROOM_PUBLIC_URL=https://piperoom.corp.example
PIPEROOM_OIDC_ISSUER=https://idp.corp.example/realms/corp
PIPEROOM_OIDC_CLIENT_ID=piperoom
PIPEROOM_OIDC_CLIENT_SECRET=<from your secret manager>
PIPEROOM_TRUSTED_PROXIES=172.18.0.0/16
```

Then restart: `docker compose -f deploy/docker-compose.pilot.yml up -d`. The first start applies
migration `00007`, which adds the `login_states` and `identities` tables.

At start-up the server does **not** contact the IdP. It looks up the provider's configuration in the
background and retries with a growing pause (1 s up to 30 s). Until that works, new logins answer
`503 idp_unavailable`. People who are already signed in keep working.

Check it: open `https://piperoom.corp.example/auth/login` in a browser. You should land at the
IdP. After you sign in you come back to Piperoom. `curl -i https://piperoom.corp.example/auth/login`
shows a `302` to the IdP when discovery has worked, and `503` when it has not.

### Behind a TLS reverse proxy

The server speaks plain HTTP, so put a reverse proxy in front for HTTPS. In `oidc` mode this is
supported. The browser talks `https` to the proxy; the proxy talks `http` to the container port.

- Set `PIPEROOM_PUBLIC_URL` to the address people use. Its host is allowed as `Host` and as the
  browser origin (including the WebSocket) with no other setting.
- Best: the proxy passes the original `Host` header on. If it rewrites `Host` to an internal name,
  add that name to `PIPEROOM_ALLOWED_HOSTS`, or every request gets `421 bad_host`. The container
  health check calls `127.0.0.1`, which is always allowed.
- The proxy must pass WebSocket upgrades on `/api/rooms/<id>/ws`.
- The proxy must let `POST /api/alerts/<source>` through without a login (see [Alerts](#alerts)).
- **Trusted proxies.** All people reach the server through the proxy, so the server sees one peer
  address. `/auth/login` is limited to **300 requests per minute per client address**, and the
  request log has a `client` field. To use the real client address, list the proxy's address in
  `PIPEROOM_TRUSTED_PROXIES`. Then the server reads `X-Forwarded-For` from that peer and takes the
  rightmost entry that is not itself a trusted proxy. IPv6 clients count as their /64.
  Without it every person shares the proxy's one bucket.
- **Send plain addresses in `X-Forwarded-For`.** An entry with a port (`203.0.113.7:51234`, or
  `[2001:db8::1]:443`) is not an address. The server then falls back to the proxy's own peer
  address, so **everyone behind that proxy shares one login rate-limit bucket**. Configure the
  proxy to send IP addresses only (nginx `$remote_addr`, not `$remote_addr:$remote_port`; check
  that your load balancer does not add a port).
- A request over the limit gets `429 rate_limited` with `Retry-After: 60`. The limit is in memory
  and per process.

### Expected start-up messages

In `oidc` mode under the image and the compose file you will see this warning on **every** start.
It is normal. The image and compose set `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1`, and `oidc` mode ignores it:

```
server: PIPEROOM_DEV_ALLOW_NONLOOPBACK and --allow-remote are ignored in oidc mode
```

Other lines you may see:

| Log line | Meaning |
|---|---|
| `auth: oidc discovery succeeded` | the IdP answered; logins work |
| `auth: oidc discovery failed, will retry; new logins answer 503 until it succeeds` | the IdP is not reachable yet (wrong issuer, no network, outbound proxy). Not fatal; it retries |
| `server: oidc public client: no client secret, PKCE only` | you set `PIPEROOM_OIDC_PUBLIC_CLIENT=1` with an empty secret |
| `server: PIPEROOM_OIDC_GROUPS_CLAIM is empty: groups give no rights` | group rights are off |
| `users: owners->users check skips layers with group principals (not checked against users.yaml); users->owners is always checked` | normal when `owners.yaml` lists a `group:` for a layer |
| `server: SOLO MODE: ...` | `PIPEROOM_SOLO_USER` is set |
| `auth: no groups claim in id token` (per login) | the token had no groups claim, see [Troubleshooting](#troubleshooting) |

`--demo` always runs in dev mode, even if `PIPEROOM_AUTH_MODE=oidc` is set.

### Alerts in `oidc` mode

The unsigned `POST /api/alerts/webhook` is **not mounted** in `oidc` mode (the answer is `404`).
Send alerts to a signed route instead. For a custom sender use
`POST /api/alerts/generic` with `GENERIC_ALERT_SECRET` (an HMAC). See [Alerts](#alerts).

## Identity and rights

Two questions, answered separately:

1. **Who can log in?** The *login gate*.
2. **What can they do?** Their *role*, and for approvals their *layer rights*.

Both are worked out again on **every request** from `users.yaml`, `owners.yaml` and the groups in
the person's session. Remove someone from the files and restart the server: their next request
answers `401 session_expired`, with no wait for the session to run out. The two files are read at
start, so a change needs a restart (people stay signed in: sessions live in the room database).
A person's IdP groups are the exception: they are read at login only.

### Who can log in (the gate)

A login works only if the person is **one** of these:

- a user in `users.yaml`;
- named in `owners.yaml`: as a `user:` or `group:` in `layers:`, in a pipeline `owners:` block (as an
  approver or a backup), or in `auditors:`;
- in a group listed in `PIPEROOM_AUTH_VIEWER_GROUPS`.

Everyone else gets `403 not_provisioned`, even with a valid IdP login. "Anyone at our company can
sign in" is **not** the default.

### Which Piperoom user is this?

The server maps the IdP account (the pair `iss` + `sub`, never the email alone) to a Piperoom user
id. In this order:

1. **`sub:` in `users.yaml`** wins. The user whose `sub:` equals the token's `sub`.
2. **A saved binding.** After a first email match (next step) the pair `iss` + `sub` is saved in the
   `identities` table. Later logins of that account use it.
3. **A verified `email:` in `users.yaml`.** It matches only when the IdP says `email_verified` is
   true, and the comparison ignores upper and lower case. The first match saves the binding.
   - The email must be plain ASCII. The server refuses a non-ASCII email at start-up in
     `users.yaml`, and a non-ASCII email from the IdP never matches (so look-alike letters, such
     as the Kelvin sign for `k`, cannot pose as someone).
   - A user is bound to **one** account. A second account with the same email is refused
     (`403 not_provisioned`): this is the recycled-email case. See
     [Moving a person to a new IdP account](#moving-a-person-to-a-new-idp-account).
   - A `users.yaml` user who has a `sub:` is **not** matched by email. An account whose email
     matches such a user but whose `sub` is not the pinned one is refused, and so is an account
     that an old binding still points at that user.
4. **Everyone else** gets the id `u_` + the first 12 hex characters of SHA-256 of `<iss>|<sub>`, for
   example `u_3f9a1c07b2de`. Their display name comes from the token's `name` claim (else the
   email, else the id). The same IdP account always gives the same id. Ids that start with `u_` are
   reserved for this, so **`users.yaml` refuses a user id that starts with `u_`**.

To name a person who is not in `users.yaml` (for example in `auditors:` or as a backup), you
need their `u_` id. The easiest way: ask them to try to sign in once. If the gate refuses them, the
server logs `auth: login refused by the gate (not provisioned)` with `user=u_...`. If they get in,
`GET /api/me` shows their `id`. Copy that id into `owners.yaml` as `user:u_...`.

### `users.yaml`

```yaml
users:
  - id: priya                     # no ':' or spaces; must not start with "u_"
    name: Priya
    email: priya@corp.example     # ASCII only; matched only when the IdP says email_verified
    layers: [postgres, schema-registry]
  - id: arjun
    name: Arjun
    sub: "00u1abcd2efGh3IJk4l5"   # the IdP's stable id for him; wins over email
    layers: [kafka-connect, kafka]
```

`email:` and `sub:` are optional, and each must be unique across the file. Use `sub:` when you
cannot trust an email (shared mailbox, an IdP that does not verify email), or to pin a user to one
account. Everyone in `users.yaml` is a **member**. For each layer a user lists in `users.yaml`,
`owners.yaml` must include that user as `user:<id>`, even when the layer also lists groups; the
server refuses to start otherwise. The reverse owners-to-users check skips a layer containing any
group principal because group membership is not available at startup; the server logs skipped layers.

### `owners.yaml`: who may approve and who may export

A layer value is a user id (the old form), or a list of principals. `user:<id>` is a `users.yaml` id
(or a `u_` id). `group:<value>` is one value of the token's groups claim, compared as an exact
string.

```yaml
pipelines:
  - id: orders
    # ...topic_prefix, source_tables, connectors as before...
    owners:                       # replaces layers: for this pipeline and these layers only
      kafka-connect:
        approvers: [group:dataeng-oncall]
        backup: [user:priya]      # may approve at once; the room gets a system message
layers:                           # the default owners of each layer
  postgres: priya
  kafka: [user:arjun, group:dataeng-oncall]
auditors: [group:security, user:meera]   # may export; see Roles
```

- A pipeline `owners:` block **replaces** `layers:` for that layer in that pipeline. The
  `users.yaml` `layers:` list is then **not** used for that layer either.
- A pipeline that has no `owners:` entry for a layer uses `layers:`.
- A fix needs the approval of **every** layer it touches. Each layer needs one approver (or backup).
- A backup is a full approver and may approve at once. The server log records the grant as
  `backup:...`, and an approval by a backup adds a system message to the room naming the backup.
  (Storing the grant on the approval event itself comes with auth PR 4.)
- In `layers:`, a layer whose principals are all `user:` must name `users.yaml` users. To give a
  layer to a person with no `users.yaml` entry, use a `group:`, or put the `user:u_...` in a
  pipeline `owners:` block (these are not checked against `users.yaml`).
- `auditors:` may name `user:` or `group:` principals, same rules.
- Unknown keys inside an `owners:` entry, a bad prefix such as `grp:` and an empty name are errors at
  start-up.

**Group values.** Names can be forged in an IdP where ordinary users may create groups (Entra and
Okta allow this in some tenants): anyone could make a group called `dataeng-oncall`. So write the
IdP's group **id** in `owners.yaml` where the token carries ids (Entra object ids), and **turn
self-service group creation off** in IdPs that send names. The server compares strings and cannot
tell the two apart.

### Roles

| Role | Who | May |
|---|---|---|
| **member** | in `users.yaml`, or an approver or backup (`user:` or `group:`) in `layers:` or a pipeline `owners:` block | everything a room allows: create rooms, comment, steer, suggest, request and take control, hand off, close, trace. Approve only the layers they own |
| **viewer** | passes the gate **only** through `PIPEROOM_AUTH_VIEWER_GROUPS` or `auditors:` | read and watch: every `GET` except export and `GET /api/people` (`403 viewer`), and the live WebSocket. **Every non-GET under `/api` answers `403 viewer`**, including accepting a hand-off. A viewer can still log out |
| **auditor** | named in `auditors:`, any role | export a room. `auditors:` **never makes a member**: a person who is only in `auditors:` is a viewer who can also export |

Today a **member can still export** too. From auth PR 4 a member who is not an approver, a backup
or an auditor will get `403 not_allowed` on export. Add them to `auditors:` before you rely on it. A viewer
who is not in `auditors:` gets `403 not_allowed` on export now.

Some actions need a person the server can look up. To **give control** to someone or **hand off** to
someone, the target must be a `users.yaml` user, or a member who is signed in right now (a
live session). A person who only came in through a group, and has not logged in, cannot be picked.
A viewer, an auditor-only person or an unknown id answers `400 validation`.

### Groups, and the "overage" message

Groups are read from the ID token once, at login. If a groups claim is expected (the claim name is
not empty) and the token has none, the session is marked `groups_missing`:

- the server logs `auth: no groups claim in id token` (a Warn, once per login);
- `GET /api/me` returns `groups_missing: true`;
- the person gets no group rights. An approve or reject that failed *only* because of this answers
  `403 not_layer_owner` with the message **`no groups claim in token (IdP overage?)`**.

The usual cause is "overage": an IdP leaves the claim out when a person has too many groups.
Entra does this. Fix it at the IdP (send only the groups of this app, or fewer groups), or name
that person with a `user:` principal instead. People need to sign in again for a fix to take
effect. Note that `groups_missing` is true for any token without the claim, even for a person who
needs no groups; it changes nothing for them.

### Solo mode

For one data engineer who owns every layer. Set `PIPEROOM_SOLO_USER=<users.yaml id>`:

- that user may approve **every** layer;
- each approval must also send `confirm: "<pipeline_id>/<layer>"`, typed by hand, for example
  `"orders/kafka"`. Without it the answer is `400 validation`. The web app asks for it when the
  server says a solo confirm is needed; through the API, send `confirm` in the approve body;
- the server log records the grant as `solo` (storing it on the approval event comes with auth PR 4).

The typed text is a speed bump, not a control. So **the server refuses to start** when the solo
user is not in `users.yaml`, or when `owners.yaml` names **anyone else**, in `layers:`, a pipeline
`owners:` block or `auditors:` (a group counts as someone else). It cannot be left switched on by
mistake in a team. When it does start it logs a warning.

### Moving a person to a new IdP account

When someone's IdP account changes (new tenant, a recycled email, a rename that changes `sub`),
their next login is refused with `403 not_provisioned`: "this email is already linked to a
different account; ask an admin to unbind it". Remove the old binding:

```sh
# needs Go and this repo, and PIPEROOM_DB_URL in your shell must point at the room database:
go run ./cmd/devctl identities unbind priya
```

It prints `unbound "priya"; ...` when it removed a row, or says nothing changed. The next login
with a **verified** matching email binds the new account. It does not sign anyone out.

The bundled Postgres has no published port, so on a pilot host the same change is one SQL
statement (this is what the command runs):

```sh
docker compose -f deploy/docker-compose.pilot.yml exec postgres \
  psql -U piperoom -d piperoom -c "DELETE FROM identities WHERE user_id = 'priya'"
```

For a user who has a `sub:` in `users.yaml`, nothing is bound: edit the `sub:` in the file and
restart.

## Alerts

Alert senders call the signed routes `POST /api/alerts/{source}`, where `{source}` is
`alertmanager`, `grafana`, `pagerduty`, `datadog` or `generic`. Each one needs its secret in
`deploy/.env` (section 10 of the file); a source with an empty secret answers 401. Sender
setup for each system is in [internal/alertin/README.md](../internal/alertin/README.md).

These routes have **no `X-User`** and no session cookie: the sender's token or signature is the
login. They also **skip the Host check**, because the signature is their protection and
in-cluster senders often use an internal DNS name. That matters behind a proxy:

- If the proxy requires a login for everything, it must let `POST /api/alerts/<source>` through
  without one. PagerDuty, Datadog and Grafana Cloud cannot log in to your proxy.
- Expose **only those paths** to the senders, over HTTPS, ideally limited to the sender's
  published IP ranges. Do not expose the rest of the API.
- `POST /api/alerts/webhook` is a different route: the dev webhook, with no sender check. It
  exists in dev mode only; in `oidc` mode it is not mounted (`404`). Keep it closed at the proxy
  anyway. In `oidc` mode a custom sender uses the signed `POST /api/alerts/generic`
  (`GENERIC_ALERT_SECRET`).
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
curl -H 'X-User: alice' http://localhost:8090/api/rooms/<room id>/export > room.json   # dev mode
```

The export carries a SHA-256 over the JCS (RFC 8785) form of the room with the top-level
`integrity` member removed (also sent in the `X-Piperoom-Export-SHA256` header), so a plain
`sha256sum` of the file will not match it. Use it to check later that the file was not changed.
In `oidc` mode a request needs a session cookie, not `X-User`: export from the browser, or send
a cookie. Today a member may export, and a viewer may export only if named in `auditors:`.
From auth PR 4, members who are not owners, backups or auditors cannot export and must be added
to `auditors:`.

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

`--no-deps` keeps the bundled Postgres from starting; the image already sets `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1`.
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
| Auth | `PIPEROOM_AUTH_MODE=dev` and `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1` are set in the image (dev login, no password; see the top of this file). Set `PIPEROOM_AUTH_MODE=oidc` in `deploy/.env` for real login ([Single sign-on](#single-sign-on-oidc)); the server then ignores the non-loopback switch and logs a warning |
| Browser rules | The UI calls the API and WebSocket on its own origin, so there is no CORS at all (the server sends no `Access-Control-*` headers). Cross-origin browser `POST`s are refused with `403`. The server sends a CSP, `X-Content-Type-Options`, `Referrer-Policy` and `frame-ancestors 'none'` |

Build the image alone: `docker build -t piperoom:pilot .`

Behind a reverse proxy (an `oidc` setup, see [Behind a TLS reverse proxy](#behind-a-tls-reverse-proxy)),
set `PIPEROOM_PUBLIC_URL` (for example `https://piperoom.corp.example`) so WebSocket connections
from that address are accepted, and `PIPEROOM_ALLOWED_HOSTS` for any other `Host` value the
proxy sends. In `oidc` mode `PIPEROOM_PUBLIC_URL` must be `https`; in dev mode it may be `http`
or `https`.
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
| Exits with `auth: PIPEROOM_AUTH_MODE=oidc needs ...` | `oidc` mode needs `PIPEROOM_PUBLIC_URL` (https), `PIPEROOM_OIDC_ISSUER`, `PIPEROOM_OIDC_CLIENT_ID`, and `PIPEROOM_OIDC_CLIENT_SECRET` or `PIPEROOM_OIDC_PUBLIC_CLIENT=1`. The message names the missing one |
| Exits with `PIPEROOM_OIDC_SCOPES must include openid`, or with an error about `PIPEROOM_SESSION_IDLE`, `PIPEROOM_SESSION_MAX` or `PIPEROOM_TRUSTED_PROXIES` | fix the named variable (durations look like `30m` or `8h`; proxies are CIDRs like `10.0.0.0/8` or plain addresses) |
| Exits with `solo mode: ...` | `PIPEROOM_SOLO_USER` is not in `users.yaml`, or `owners.yaml` names someone else. Remove the variable or the other owners |
| Exits with `user id "u_..." ... reserved` or `bad email` | `users.yaml`: ids may not start with `u_`, and emails must be ASCII with no spaces (see [Upgrade notes](#upgrade-notes)) |
| Exits with `PIPEROOM_WEB_DIR ... has no index.html` | the image was built without the UI; rebuild with `up --build` |
| Exits with `open the data stack` | a URL or login in `deploy/.env` is wrong or unreachable from the container |
| Page does not load from another machine | `PIPEROOM_BIND` is still `127.0.0.1` |
| `421 bad_host` | the `Host` header is not allowed: add it to `PIPEROOM_ALLOWED_HOSTS` (tunnel or proxy that rewrites `Host`) |
| `403 proxy_identity` | in dev mode: more than one `X-User` value (several header lines, or a comma in one line), cookie or not; or `X-User` sent to `/auth/dev-login`; or `X-User` with a cookie for another user. A proxy needs `PIPEROOM_AUTH_MODE=oidc` (the git tag `pre-auth` is only for a proxy pilot that cannot use OIDC yet) |
| `400 bad_request` on a curl `POST` | add `-H 'Content-Type: application/json'` (not needed on the signed `/api/alerts/{source}` routes) |
| Exits with `refusing to listen on ... listen address is not loopback` | dev mode needs `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1`. The image and the compose file set it, so you see this only if the variable is overridden with an empty value (for example `docker run -e PIPEROOM_DEV_ALLOW_NONLOOPBACK=`) or with a value other than `1` |
| UI loads but live updates fail behind a proxy | the proxy does not pass WebSocket upgrades, or `PIPEROOM_PUBLIC_URL` is not set |
| No agent answer | the model provider's credentials are missing, or egress to its API is blocked (behind a proxy, set `HTTPS_PROXY` and `NO_PROXY`) |
| Exits at start with an error about the model provider, region, project or credentials | see [Model provider](#model-provider); the error names the setting |
| A password with `$` is cut short | write `$` as `$$` in `deploy/.env` |
| `login_expired` (`400` at `/auth/callback`; today the browser shows this JSON, after auth PR 4 the "sign-in expired" page) | the login cookie or its row is gone. A login lasts **10 minutes**; the person took longer, or opened `/auth/login` a second time (a second tab) so the first tab's login was replaced, or pressed back and sent the same callback twice (each login works once), or opened the callback in another browser. Also check that people open the exact `PIPEROOM_PUBLIC_URL` address (the cookie belongs to that host). Fix: start again from the site address, in one tab |
| `unauthenticated` (`401` at `/auth/callback`; today JSON in the browser, after auth PR 4 a sign-in failed page) | `state` did not match (log `auth: callback state mismatch`); the IdP refused the login, for example `access_denied` (log `auth: the identity provider refused the login`); the token endpoint said no, for example `invalid_grant`, which a redirect URI that does not match the one registered at the IdP also gives (log `auth: login refused: token endpoint said 400 invalid_grant`); or the ID token failed a check: signature, issuer, audience, expiry, nonce, or `azp` not equal to our client id (log `auth: login refused ...`). A token endpoint that answers 5xx gives `503 idp_unavailable` instead. Check `PIPEROOM_OIDC_ISSUER` and `PIPEROOM_OIDC_CLIENT_ID`, and the server clock (use NTP) |
| `not_provisioned` (`403` at login; today JSON in the browser, after auth PR 4 a "not set up" page) | the IdP login was fine but the gate says no: the person is not in `users.yaml`, not named in `owners.yaml` (`layers:`, a pipeline `owners:` block, `auditors:`), and not in `PIPEROOM_AUTH_VIEWER_GROUPS`. Find their id in the log (`auth: login refused by the gate`, `user=...`), add them, restart. Two other texts mean a **recycled or moved account**. "this email is already linked to a different account; ask an admin to unbind it" has two causes: the user is already bound to another IdP account (run `devctl identities unbind <user>`, see [Moving a person](#moving-a-person-to-a-new-idp-account)), or the email belongs to a `users.yaml` user who has a `sub:` pin and this account is not that `sub` (unbind does not help: fix the `sub:` in `users.yaml`, or the login is right to be refused). "this account is not the one pinned for <user>" is the same pin problem: fix the `sub:` |
| `session_expired` (`401`) after a good login | the idle (`PIPEROOM_SESSION_IDLE`) or maximum (`PIPEROOM_SESSION_MAX`) time ran out, or the person was removed from `users.yaml`, `owners.yaml` or the viewer groups ("this user is no longer allowed to sign in") |
| `rate_limited` (`429` on `/auth/login`) | more than 300 logins a minute from one client address. Behind a proxy this usually means everyone shares the proxy's address: set `PIPEROOM_TRUSTED_PROXIES`, and make the proxy send plain addresses (no port) in `X-Forwarded-For`. Wait a minute (`Retry-After: 60`) |
| `idp_unavailable` (`503`) | the server has not reached the IdP (discovery is retried in the background, 1 s up to 30 s apart, and is not fatal), or the IdP's token endpoint was down during the login. Check `PIPEROOM_OIDC_ISSUER`, DNS, firewall and `HTTPS_PROXY`/`NO_PROXY` (see [Outbound proxy](#outbound-proxy)); watch for `auth: oidc discovery succeeded`. People already signed in are not affected |
| `viewer` (`403`) | the person is a viewer: in `PIPEROOM_AUTH_VIEWER_GROUPS`, or only in `auditors:`. Viewers can read and watch, not change anything. To make them members, add them to `users.yaml` or as an approver or backup in `owners.yaml`, then restart |
| `not_allowed` (`403`) on export | a viewer who is not in `auditors:` (see [Roles](#roles)) |
| `403 not_layer_owner` "no groups claim in token (IdP overage?)" (`groups_missing`) | the ID token had no groups claim, so a group right could not apply. `GET /api/me` shows `groups_missing: true`; the log has `auth: no groups claim in id token`. Check the claim is in the **ID token** and its name equals `PIPEROOM_OIDC_GROUPS_CLAIM`; with Entra check for overage. Or name the person with `user:`. They must sign in again |
| `404` on `POST /api/alerts/webhook` | normal in `oidc` mode: that route is not mounted. Use the signed `/api/alerts/generic` |
| An alert sender gets 401 | its secret in `deploy/.env` is empty or wrong, or the proxy is asking it to log in (see [Alerts](#alerts)) |

## Upgrade notes

Moving to the version with OIDC login (auth PR 3):

- **`users.yaml` is checked harder at start-up.** The server now refuses to start if a user `id`
  starts with `u_` (that prefix is for IdP users with no entry), or contains a `:` or any
  whitespace, or if an `email:` has no `@`, contains a space, or has a non-ASCII character.
  Rename the id or fix the email before you upgrade. The example
  files in `deploy/config/` and `dev/` use neither.
- **Migration `00007`** runs on the first start. It adds two tables, `login_states` and
  `identities`, and an `email` column on `sessions`. A pilot that stays in dev mode does not use
  the new tables.
- A pilot that stays in dev mode needs no other change. A proxy pilot that was held on the git
  tag `pre-auth` can move to the current code once it switches to `oidc` mode.
- The new settings are in [Single sign-on](#2-settings-in-deployenv). Compose already passes them
  to the container.

## Not covered here

- Database role and TLS. The bundled Postgres creates `piperoom` through the image's
  `POSTGRES_USER`, which makes it a **superuser** of that container. It is reachable only on the
  compose network, but the server does not need superuser rights. With your own database
  (`PIPEROOM_DB_URL`), use a role that owns only the `piperoom` database and
  `sslmode=verify-full` with the CA in `sslrootcert` (files in `deploy/certs` are mounted at
  `/certs`). `sslmode=require` encrypts but does not check the server certificate. The source and
  warehouse URLs take the read-only roles from
  [docs/pilot/permissions.md](../docs/pilot/permissions.md), with the same TLS advice.
- Helm `networkPolicy.enabled` defaults to `false`. The chart's policy does not work in every
  cluster as it stands: ingress is allowed only from the `ingress-nginx` namespace, so GKE/ALB
  load balancers that send traffic straight to pod IPs, other ingress controllers, in-cluster
  alert senders and Prometheus are blocked until you add them to `ingressFrom`; and its egress
  groups with an empty `to` open their ports to any address. Turn it on once you have set
  `ingressFrom` for your ingress and narrowed each egress `to` to your CIDRs (see the comments
  in `deploy/helm/piperoom/values.yaml`).
- Images are pinned by tag, not digest (`node:24-alpine`, `golang:1.27-alpine`,
  `distroless/static-debian13:nonroot`, `postgres:17`). Pin digests for a regulated site.
- The UI loads two fonts from Google Fonts. On a network with no internet the page still works
  with fallback fonts; the CSP allows only those two hosts for this.
- TLS: put a reverse proxy in front for HTTPS. The server speaks plain HTTP.
