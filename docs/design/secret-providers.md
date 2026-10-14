# Design: secret managers (rev 1)

**Status (2026-10-14):** S1–S3 built in `internal/secrets` (parser, registry with cache and shared fetch, lease
renewer, auth-failure retry helper; files, env, Vault/OpenBao, Kubernetes, AWS Secrets Manager, SSM) and
`internal/secrets/{azure,gcp}`. The six tier-1 cards with setup scripts are in `internal/integrations/secretkinds`,
and `secret://` in environment variables is resolved at start. S4 in part: `cmd/server` passes the
registry an `OnChange` that rebuilds the clients of the rows reading a changed file or a lost lease
(`integrations.Service.SecretChanged`), and a failed lease renewal is retried while a quarter of its time is left;
`RetryAfterAuthFailure` is still unused by the clients (a rotated static secret is picked up on the next change
notice, enable or restart, not on a login failure). `SOURCE_DB_PASSWORD` / `WAREHOUSE_DB_PASSWORD` are built (§6).
Not built: the §9 canary test over every agent tool, a
mounted-files card, Vault AWS/Azure/GCP auth, hosted cross-account access, Link, and tier 2 (S6). See
`tasks/I-integrations.md`.

Part of the integrations work: `docs/design/integrations.md` (server) and `docs/design/integrations-ux.md`
(product). Product decision (2026-10-14): Piperoom must work with **every** secret manager an enterprise uses, in
both deployment shapes (in the customer's network and hosted). The text below is the original design. Every library call is marked
**verify with `go doc`** until checked (AGENTS.md §4 rule 9).

## 1. Goal

Wherever a form asks for a password, an API key or a private key, the person can instead point to a secret that
already lives in their vault. Piperoom stores the **reference**, never the value, and reads the value at run time.

"Works with all of them" is met in three layers, so no customer is ever stuck:

1. **Built-in providers** for the managers most enterprises use (§3, tier 1 and tier 2).
2. **Mounted files**: a universal adapter. Every major manager can write secrets to files in a pod or on a host
   (Kubernetes Secrets Store CSI driver, External Secrets Operator, Vault Agent, Conjur and Akeyless sidecars,
   systemd credentials). Piperoom reads the file and re-reads it when it changes. This covers any manager we have
   not built, including in-house ones.
3. **A provider interface** (§2) small enough that a new built-in provider is one package of a few hundred lines.

## 2. The provider interface

New package `internal/secrets` (owner: the `internal/integrations` owner, §8).

```go
// Ref names one secret value in one provider. It is stored; the value never is.
type Ref struct {
	Provider string // the secret_manager row's `name` field (unique per workspace), "files", or a PIPEROOM_SECRET_PROVIDERS id
	Path     string // provider-native name: ARN, Vault path, Key Vault secret name, ...
	Key      string // optional: one field of a JSON or key/value secret
	Version  string // optional: pin a version; empty = latest
}

// Value is a resolved secret. Data is held as netsec.Secret so it never prints.
type Value struct {
	Data      netsec.Secret
	Version   string
	ExpiresAt time.Time // zero = no lease; Vault dynamic credentials set it
	LeaseID   string    // Vault lease, for renewal
	Renewable bool
}

// Provider reads secrets from one manager. Every method takes a context with a deadline.
type Provider interface {
	// Resolve returns the whole secret at ref.Path and ignores ref.Key; the
	// Registry applies the key (ExtractKey), so one read serves several keys.
	Resolve(ctx context.Context, ref Ref) (Value, error)
	// List returns secret NAMES under prefix for the picker, at most limit. Never values.
	// Providers that cannot list return ErrListUnsupported; the UI then shows a text field.
	List(ctx context.Context, prefix string, limit int) ([]string, error)
	// Keys returns the field names of a JSON or key/value secret, for the key picker. Never values.
	Keys(ctx context.Context, ref Ref) ([]string, error)
	// Check runs the card's connection test (§5).
	Check(ctx context.Context, sample *Ref) []spec.Check
	// Close releases clients and stops lease renewal.
	Close()
}
```

The string form of a reference, used in the API, in exports and in environment variables:

```
secret://<provider-id>/<path>[#<key>][?version=<v>]
secret://aws-sm-prod/prod/piperoom/orders-db#password
secret://vault-corp/database/creds/piperoom-ro#password        (Vault dynamic credentials)
secret://files/run/secrets/kafka/password                      (mounted file)
```

Rules for the parser: the provider id matches `^[a-z0-9][a-z0-9_-]{0,62}$`; the path is percent-decoded once and
must not contain `..` segments; a `files` path must be under one of the allowed mount roots (§3, `files`).

## 3. Providers

| Tier | Provider | Auth Piperoom uses (preferred first) | Lists names | Notes |
|---|---|---|---|---|
| 1 | **AWS Secrets Manager** | the shared AWS config (`wiring.LoadAWS`): IRSA, task or instance role; hosted: cross-account `AssumeRole` with an external ID (`integrations.md` §6.2) | yes | JSON secrets: `#key` picks a field. Module `aws-sdk-go-v2/service/secretsmanager` (**verify** `GetSecretValue`, `ListSecrets`) |
| 1 | **AWS SSM Parameter Store** | same as above | yes, by path | `SecureString`, `WithDecryption`. Module `aws-sdk-go-v2/service/ssm` (**verify** `GetParameter`) |
| 1 | **HashiCorp Vault / OpenBao** | Kubernetes auth, AWS IAM auth, Azure or GCP auth, JWT/OIDC, AppRole; a token only as the last resort | yes (KV `LIST`) | KV v1 and v2, Enterprise namespaces, and the **database secrets engine** for short-lived Postgres logins (§4.2). Plain HTTP API with `net/http`, so no Vault module is needed |
| 1 | **Azure Key Vault** | managed identity or workload identity federation; client secret fallback | yes | Modules `azidentity`, `azsecrets` (**verify**) |
| 1 | **Google Secret Manager** | application default credentials or workload identity federation; hosted: the customer grants Piperoom's service account `secretmanager.secretAccessor` on named secrets | yes | Module `cloud.google.com/go/secretmanager` (**verify**) |
| 1 | **Kubernetes Secrets** | in-cluster service account; RBAC `get` on named secrets only | yes, if RBAC allows `list` | Plain HTTPS to the API server with the mounted service-account token. No `client-go` (too heavy for one GET) |
| 1 | **Mounted files** (universal) | the file system | yes, under the allowed roots | Roots set by `PIPEROOM_SECRET_FILE_ROOTS` (default `/run/secrets,/var/run/secrets,/mnt/secrets-store`). Re-read on change (poll every 30 s, so no `fsnotify` dependency) |
| 2 | CyberArk Conjur / Secrets Manager | host identity, JWT, AWS IAM authn | yes | REST API, `net/http` |
| 2 | Akeyless | Kubernetes, AWS IAM, Azure AD, GCP auth | yes | REST API |
| 2 | 1Password (Connect server or service account) | Connect token, service-account token | yes (items) | REST API |
| 2 | Doppler, Infisical | service token, machine identity | yes | REST API |
| 2 | Delinea Secret Server, Bitwarden Secrets Manager | OAuth client credentials, access token | yes | REST API |

Tier 1 ships first. Tier 2 providers use only `net/http`, so each is small. Until a tier-2 provider ships, those
customers use **mounted files**, which their managers already support.

New Go modules (AGENTS.md §4 rule 10): the AWS service clients (the AWS SDK is already a dependency), the Azure
and Google clients. Each is justified by a tier-1 provider. Vault, Kubernetes and every tier-2 provider use the
standard library.

## 4. Runtime

### 4.1 Resolving and caching

- A secret is resolved when its integration's client is built (`integrations.md` §4.5, `Build`). The value goes
  straight into the client constructor as `netsec.Secret` (`internal/clients/netsec/netsec.go:34-50`).
- Values are cached **in memory only**, per reference, for `PIPEROOM_SECRET_CACHE_TTL` (default 5 min). They are
  never written to the database, a file, a log, an event or the browser.
- Each resolve has a 5 s deadline. Two builds that need the same reference share one in-flight request.

### 4.2 Rotation and leases

- **Rotated static secrets.** When a target rejects the login (Postgres `28P01`, a Kafka SASL authentication
  failure, HTTP 401 from Connect or the Registry), the client asks for the secret again, bypassing the cache,
  **once**. If the value changed, the client is rebuilt through the no-restart swap and the call is retried
  once. If the value did not change, the error goes up as it is. Errors are compared with `errors.As`, never by
  string (AGENTS.md §4 rule 3).
- **Expiring secrets** (Vault dynamic database credentials, Vault tokens). Piperoom renews the lease at two thirds of
  its time to live. If renewal fails, it asks for new credentials and swaps the client. The renewal goroutine stops
  when the provider is closed or the server context ends (no goroutine leak, AGENTS.md §7).
- **Mounted files.** A changed file is treated as a rotation.
- The health page (`integrations-ux.md` §4) shows each secret's version, when it was last read and, for leases, when
  it expires. It never shows the value.

### 4.3 When the secret manager is down

- A client that is already built keeps working with the value it has. Nothing is torn down because the vault is
  unreachable.
- A new build that needs an uncached secret fails, and the card shows "secret manager unreachable".
- There is **no fallback to a stored copy**: storing one would defeat the vault.

## 5. Connecting a secret manager (the card)

A secret manager is a card in the catalog, category `secret_manager`. Unlike other categories, a workspace may have
**several** (for example AWS Secrets Manager for databases and Vault for Kafka), so the `UNIQUE (category, scope)`
rule in `integrations.md` §4.3 excludes this category.

The card follows the usual pattern: pick provider → choose how Piperoom logs in (workload identity preselected when
Piperoom detects it is running with one) → **Setup script** → **Test** → **Save**.

**Setup script, per provider.** The least-privilege grant, scoped to a path prefix the admin picks (default
`piperoom/`):

| Provider | Script |
|---|---|
| AWS | IAM policy JSON: `secretsmanager:GetSecretValue` and `DescribeSecret` on `arn:…:secret:piperoom/*`, `ListSecrets` (which cannot be scoped), `kms:Decrypt` on the key, via `secretsmanager.<region>.amazonaws.com` |
| Vault | policy HCL: `read` on `secret/data/piperoom/*`, `list` on `secret/metadata/piperoom/*`; the auth role binding for the chosen method |
| Azure | `az role assignment create --role "Key Vault Secrets User" --scope <vault>` for the managed identity |
| Google | `gcloud secrets add-iam-policy-binding … --role roles/secretmanager.secretAccessor` per secret, or on a folder via IAM conditions |
| Kubernetes | a `Role` with `get` on the named secrets (`resourceNames`), and its `RoleBinding` |
| Files | the CSI `SecretProviderClass` or External Secrets `ExternalSecret` YAML for their manager |

**Checks.** Each reports pass, warn, fail or skipped, with a fix, like every other card:

1. Reach the manager, with TLS.
2. Log in with the chosen method.
3. Read the sample reference (if given). Report its version and whether it is JSON with the expected key. **Never**
   the value.
4. Too much access? Warn if Piperoom can read outside the prefix or can write, where the manager can say so without
   writing: Vault `sys/capabilities-self` on a path outside the prefix, Google `testIamPermissions`. AWS and Azure
   cannot answer this without extra rights, so the check is `skipped` with the reason.
5. For leases: the lease is renewable.

## 6. How references appear elsewhere

- **In forms.** Every `secret` field in a card (`integrations.md` §4.1) gets a toggle: *From a secret manager* (the
  default when one is connected) or *Enter value*. The picker lists names under the prefix, then the keys of the
  chosen secret. The person can also paste a `secret://` reference.
- **In storage.** The integration's `config` jsonb holds `{"password": {"$ref": "secret://…"}}`. The encrypted
  `secrets` column is used only for typed-in values.
- **In environment variables** (operators who configure with env). Any secret variable, for example
  `SOURCE_DB_PASSWORD` or `KAFKA_SASL_PASSWORD`, may hold a `secret://` reference, resolved at start. A `secret://`
  reference may name an env-configured provider, set with `PIPEROOM_SECRET_PROVIDERS` (for example
  `vault-corp=vault:https://vault.corp:8200;auth=kubernetes;role=piperoom`). This is a contract change to
  `internal/config/config.go` (§8).
- **In URLs.** Database URLs carry their password. The form splits a pasted URL into fields, so the password can be a
  reference. For env, `SOURCE_DB_URL` may leave the password out and set `SOURCE_DB_PASSWORD` to a reference.
- **In Terraform and YAML exports.** References are exported as they are; typed-in values are exported as
  `<set me>`, never as the value.

## 7. Both deployment shapes

| | In the customer's network | Hosted |
|---|---|---|
| Who calls the manager | the Piperoom server, with its own workload identity | **AWS, Azure, Google:** the hosted service, with a per-workspace cross-account role, federated identity or service account the customer trusts. **Vault, Kubernetes, files, private managers:** Piperoom Link, inside the customer's network |
| Where values live | server memory | server memory, or **only in Link** (below) |
| Customer controls | the manager's own policy and audit log (CloudTrail, Vault audit, and so on) show every read by Piperoom's identity | the same, plus they can revoke the trust at any time |

**Strict hosted mode ("secrets never leave our network").** Some enterprises will accept hosted Piperoom only if no
credential leaves their network. For them, Piperoom Link resolves the secret **and opens the connection** to the
database or cluster itself; the hosted service sends Link read requests (the same read-only tool calls), and Link
returns the results. Link then needs the client code, so it is the same binary as the server in a `link` mode.
This is phase 3 and needs its own design review, because it moves tool execution into the customer network.

Workspaces are isolated: a reference may only name a provider row of the same workspace, which the resolver checks
before any call.

## 8. Contract changes and PR split

Contract changes (lead commit):

- `internal/config/config.go`: `SecretProviders` (`PIPEROOM_SECRET_PROVIDERS`), `SecretCacheTTL`
  (`PIPEROOM_SECRET_CACHE_TTL`), `SecretFileRoots` (`PIPEROOM_SECRET_FILE_ROOTS`); doc comments on every secret field
  saying it may be a `secret://` reference.
- `docs/api.md`: `GET /api/integrations/{id}/secrets?prefix=` (names), `GET /api/integrations/{id}/secrets/keys?ref=`
  (key names), and the `{"$ref": …}` field form.
- AGENTS.md §2: `internal/secrets/**` joins the `internal/integrations/**` owner row.

| PR | Owner | What | Done when |
|---|---|---|---|
| S1 | integrations owner | `internal/secrets`: `Ref` parser, `Provider`, cache with single-flight, lease renewer, re-resolve-on-auth-failure helper; `files` and `env` providers | table tests for the parser (bad ids, `..`, encodings); race test for the cache; renewer stops on ctx cancel |
| S2 | integrations owner | AWS Secrets Manager, SSM, Vault (KV v1/v2, namespaces, database engine, Kubernetes/AWS/AppRole/JWT auth), Kubernetes | `httptest` fakes per provider: 403, 404, timeout, lease expiry, rotated value |
| S3 | integrations owner | Azure Key Vault, Google Secret Manager | same |
| S4 | A4 | wiring uses `secrets` for every `netsec.Secret` field, and the auth-failure rebuild | a rotated password is picked up without a restart (unit test with fakes) |
| S5 | A6 | secret field toggle, the name and key pickers, provider cards with setup scripts | Vitest for the picker; no value ever rendered |
| S6 | integrations owner | tier 2: Conjur, Akeyless, 1Password, Doppler, Infisical, Delinea, Bitwarden, one PR each | same as S2 |

S1 can start at once. S2, S3 and S5 can run in parallel after S1.

## 9. Security notes for C4/C9

- The picker shows names; names can be sensitive too (`acquisition-target-db`), so listing is limited to the prefix,
  and only admins see the picker.
- A `files` reference could read any file the process can read. The allowed roots (§3) and the `..` rule (§2) stop
  that, and the check runs after symlinks are resolved (`filepath.EvalSymlinks`).
- A vault token typed in as a last resort is itself a secret: it is stored sealed (`integrations.md` §4.3) and shown
  as a warning on the health page ("static token: prefer workload identity").
- Secret values must never reach the model. Resolved values exist only inside client constructors; no tool or event
  carries them. A unit test runs every tool against fakes holding a canary secret and fails if the canary appears
  in any `Result`, event or log line.
