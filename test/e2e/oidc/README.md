# OIDC end-to-end test (Keycloak)

Proves that `PIPEROOM_AUTH_MODE=oidc` works against a real identity provider, not only the fake IdP of the unit
tests. A Go test (build tag `e2e`) acts as the browser, with an HTTP client and a cookie jar:

`GET /auth/login` -> Keycloak login page -> `POST` to the login form's `action` -> `GET /auth/callback` -> `GET /api/me`

## Run it

```bash
test/e2e/oidc/run.sh                # build the image from ./Dockerfile, up, wait, test, down -v
NO_BUILD=1 test/e2e/oidc/run.sh     # use an image you built already ($PIPEROOM_E2E_IMAGE, default piperoom:e2e)
KEEP=1 test/e2e/oidc/run.sh         # leave the stack up to poke at it; then:
docker compose -f test/e2e/oidc/docker-compose.yml down -v
```

Needs Docker with compose v2, Go, bash (Git Bash on Windows) and free host ports **8443** and **8444** on
127.0.0.1. The script:

1. `go run ./test/e2e/oidc/certgen -out test/e2e/oidc/.certs` writes a throwaway CA and a `localhost` server
   certificate (7 days, regenerated every run, gitignored).
2. `docker compose up -d --build --wait` (compose project `piperoom-o7-e2e`) and waits for every health check.
3. `go test -tags e2e -count=1 -v ./...` in this folder. The test also waits (up to 4 min) until Keycloak serves
   the expected issuer and the server's own OIDC discovery has succeeded.
4. On exit, saves `docker compose logs` to `.logs/compose.log` and runs `down -v`.

Without the tag, `go build ./...`, `go vet ./...` and `go test ./...` do not compile the test.

## The stack

| Service | Image | Role |
|---|---|---|
| `keycloak` | `docker.io/keycloak/keycloak:26.3.4` (Docker Hub mirror of quay.io) | `start-dev --import-realm` with `keycloak/piperoom-realm.json` |
| `postgres` | `postgres:17` | room database: sessions, login states, identities |
| `caddy` | `caddy:2.10.2-alpine` | TLS on `https://localhost:8443` (piperoom) and `https://localhost:8444` (Keycloak) with the certgen certificate |
| `piperoom` | built from the repo `Dockerfile` | `PIPEROOM_AUTH_MODE=oidc`, `PIPEROOM_PUBLIC_URL=https://localhost:8443`, confidential client + PKCE |

Realm `piperoom`: client `piperoom` (confidential, secret `piperoom-e2e-test-only-secret`, PKCE `S256` required,
redirect URI `https://localhost:8443/auth/callback`), and a group membership mapper that puts `groups` (short
names, not full paths) into the ID token. Every secret and password here is a test-only value.

| Keycloak user | Password | Groups | Piperoom config | Expected |
|---|---|---|---|---|
| alice | `alice-e2e-password` | `dataeng-oncall` | `owners.yaml`: `kafka-connect` and `kafka` = `[group:dataeng-oncall]` | member, id `u_…`, layers `[kafka-connect, kafka]` |
| bob | `bob-e2e-password` | none | not named anywhere | refused, `not_provisioned` |
| carol | `carol-e2e-password` | `piperoom-viewers` | `PIPEROOM_AUTH_VIEWER_GROUPS=piperoom-viewers` | viewer |

`config/users.yaml` lists only `dave` (who never logs in), so the three Keycloak users go through the IdP-only
paths of `docs/design/auth.md` §2.1.

## One issuer for everyone

The issuer string must be the same for the test on the host (it follows the browser redirect) and for the server
in its container (discovery, token call, `iss` check of the ID token). The usual trap is
`http://keycloak:8080/...` inside compose versus `http://localhost:8080/...` on the host: two issuers, and either
the browser cannot resolve the name or the ID token's `iss` does not match.

How this stack solves it:

- **One URL:** the issuer is `https://localhost:8444/realms/piperoom`. `KC_HOSTNAME=https://localhost:8444`
  pins it in Keycloak, so the discovery document, the authorization endpoint and the token endpoint all use it,
  whatever Host the request came with. `KC_PROXY_HEADERS=xforwarded` lets Keycloak see that Caddy terminated TLS.
- **Same meaning of `localhost`:** the piperoom container runs with `network_mode: service:caddy`, so it shares
  Caddy's network namespace. Inside it, `localhost:8444` is Caddy, which proxies to `keycloak:8080`; on the host,
  `localhost:8444` is Caddy's published port. Caddy reaches the server at `127.0.0.1:8090`.
- **Same ports:** the host ports are published as `127.0.0.1:8443:8443` and `127.0.0.1:8444:8444`. The port is
  part of both origins (the issuer and `PIPEROOM_PUBLIC_URL`), so the host port must equal the container port.
- **Trust:** the server trusts the test CA through `SSL_CERT_FILE=/e2e-certs/ca.crt` (Go reads it on Linux); the
  test loads `.certs/ca.crt` into its own `RootCAs`. Nothing is installed in a system trust store.
- `PIPEROOM_TRUSTED_PROXIES=127.0.0.1/32`: Caddy is the peer, so the login rate limit and the request log key on
  the real client address from `X-Forwarded-For`.

There is no CDC stack. `SOURCE_DB_URL` and `WAREHOUSE_DB_URL` point at the room database so the server's
start-up connections succeed; Kafka, Connect and Schema Registry are unreachable and not needed for login. No
model call is made, so no API key is set. `PIPEROOM_WATCH_MODE=off`.

## What the test checks

Codes and paths are the ones in `docs/api.md` and `internal/httpapi/auth_oidc.go`. A failed browser login is a
`302` to `/?login_error=<code>`, never a JSON page; API errors are `{"error":{"code":…}}`.

| Test | Check |
|---|---|
| `TestAliceMemberWithGroupLayers` | `/auth/login` redirects to Keycloak with `code_challenge_method=S256`, `code_challenge`, `nonce`, `state`; sets `__Host-piperoom_login` (Secure, HttpOnly, SameSite=Lax, Path=/). The callback redirects to the `return` path, sets `__Host-piperoom_session` with the same attributes and clears the login cookie. `/api/me`: `role=member`, id `u_`+12 hex, `layers=[kafka-connect, kafka]`, `groups_missing=false`, name from the `name` claim. `/api/people` 200 |
| … `callback replay in the same browser` | the same callback URL again: `/?login_error=login_expired` (login cookie spent) |
| … `callback replay with the stolen login cookie` | code, state and the old login cookie value all replayed: `/?login_error=login_expired` (the `login_states` row holding state, nonce and PKCE verifier is single use) |
| … `logout revokes the session` | `POST /auth/logout` -> `204`; `/api/me` -> `401 session_expired`; the old session token sent by hand -> `401 session_expired` with `auth_mode: oidc` (revoked server-side, not only dropped from the jar) |
| `TestBobNotProvisioned` | valid Keycloak login, but the gate refuses: `/?login_error=not_provisioned`, no session cookie, `/api/me` -> `401 session_expired` |
| `TestCarolViewer` | `role=viewer`, no layers; `GET /api/rooms` 200; `POST /api/rooms` -> `403 viewer`; `GET /api/people` -> `403 viewer` |
| `TestBadStateAndCrossLogin/no_login_cookie` | `/auth/callback` without the login cookie: `/?login_error=login_expired` |
| … `tampered_state` | a changed `state`: `/?login_error=unauthenticated`; the attempt spends the row, so the genuine callback (login cookie put back) is then `login_expired` |
| … `code of an older login with the state of a newer one` | login 2 replaces login 1's row; login 1's code sent with login 2's state is refused (`unauthenticated`: Keycloak answers `invalid_grant` because the code is bound to login 1's PKCE challenge); login 1's own callback is `login_expired` |

Not covered here: a token whose nonce differs while state and PKCE match cannot be produced with a real IdP
without breaking PKCE first; the nonce comparison is covered by the unit tests in `internal/auth`. Keycloak's own
SSO session survives a Piperoom logout by design (RP-initiated logout is later, design §1.3).

## Building behind a TLS-intercepting proxy

`docker build` of the repo `Dockerfile` downloads npm and Go modules. Where outbound HTTPS is re-signed by a
corporate proxy, `npm ci` / `go mod download` fail with `x509: certificate signed by unknown authority`. Build the
image yourself with the proxy CA available to the build stages (do not commit such a Dockerfile), then run with
`NO_BUILD=1`. For example, with the CA bundle at `/path/ca-bundle.crt`:

```bash
sed -E '/^FROM (node|golang)/a COPY --from=ca ca-bundle.crt /tmp/ca.crt\nENV SSL_CERT_FILE=/tmp/ca.crt NODE_EXTRA_CA_CERTS=/tmp/ca.crt' \
  Dockerfile > /tmp/Dockerfile.ca
docker build --build-context ca=/path -f /tmp/Dockerfile.ca -t piperoom:e2e .
NO_BUILD=1 test/e2e/oidc/run.sh
```
