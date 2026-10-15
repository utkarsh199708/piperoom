# Snowflake warehouse client

Implements `clients.Warehouse` for Snowflake. Piperoom uses it for
`warehouse_freshness`, `warehouse_row`, the sink health in `pipeline_map`, and the
warehouse leg of `trace_record`.

## Turn it on

```
WAREHOUSE_KIND=snowflake
WAREHOUSE_DB_URL='svc_piperoom:<password>@myorg-myaccount/ANALYTICS/PUBLIC?warehouse=PIPEROOM_WH&role=PIPEROOM_RO'
```

`WAREHOUSE_DB_URL` is a [gosnowflake DSN](https://pkg.go.dev/github.com/snowflakedb/gosnowflake/v2#hdr-Connection_String):
`user:password@account/database/schema?warehouse=...&role=...`. `WAREHOUSE_KIND`
defaults to `postgres`. Any value other than `postgres` or `snowflake` stops the
server at startup. The DSN holds the password: it is never logged, and startup errors
are scrubbed of it (`snowflake.Redact`).

TLS revocation: gosnowflake v2.3.0 checks no certificate revocation by default. The client
turns on fail-open OCSP (v1's behaviour) unless the DSN sets `ocspFailOpen`,
`disableOCSPChecks` or a `certRevocationCheckMode` itself; `ocspFailOpen=false` makes it
fail-closed. `SF_DISABLE_OCSP_CHECKS=true` in the environment switches fail-open OCSP off.

The DSN `protocol` parameter must be `https`: a DSN with `protocol=http` (or any other protocol) is refused at
startup, because the login, password or key-pair JWT and session token would travel in clear
text. Values the DSN sets explicitly (`ocspFailOpen`, `disableOCSPChecks`,
`certRevocationCheckMode`) are the operator's choice and are kept as written. The client
does not second-guess them, except that `disableOCSPChecks=true` logs a warning at startup
(without the DSN). `tracing` and `clientConfigFile` are refused.

Token endpoints must be `https` too (#496). Exactly these DSN parameters are checked at startup:

- `oauthTokenRequestUrl` and `oauthAuthorizationUrl`, when set, must be absolute `https://` URLs
  without credentials in them (`user:password@`). The token endpoint receives the client secret
  and returns the access token.
- `workloadIdentityHost` (the AWS STS endpoint for workload identity) must not name a scheme
  other than `https`. A bare host (`sts.example.com`) is fine: the driver prefixes `https://`.
  The STS calls carry the signed AWS session token and return temporary credentials or the
  web-identity JWT used to log in.

The error names the parameter, never the value. Left unset, the driver uses the account host over
the (https) `protocol`, or the regional STS endpoint. An Okta `authenticator` URL must be `https`
too (the driver refuses it otherwise). `oauthRedirectUri` is not checked: the driver only reads
its port for a local listener, the IdP must have the URI registered, and the code it carries is
bound to the PKCE verifier; an explicit `http` value for another host is still accepted.

Always URL-encode the password (for example with `url.QueryEscape`). The driver
unescapes it, so a raw `%` breaks it, a raw `+` turns into a space, and a raw `?`,
`@`, `/` or `:` can end the password early.

## Grants

The client does not enforce read-only. The role in the DSN does. Give it only
`USAGE` on the warehouse, database and schema, and `SELECT` on each sink table. The
exact SQL is in [docs/pilot/permissions.md](../../../docs/pilot/permissions.md)
(Warehouse, Snowflake section).

## How names and queries work

- `sink_tables` and `sink_ts_column` in `owners.yaml` are the only inputs. Use
  `schema.table`, or `db.schema.table` for a table outside the DSN's database.
- Every part must match `^[A-Za-z_][A-Za-z0-9_$]*$` (AGENTS.md section 4, rule 6; the
  `warehouse_*` tools use the same pattern when `WAREHOUSE_KIND=snowflake`, and the
  Postgres pattern without `$` otherwise). Anything else is
  refused before a query runs.
- Snowflake stores unquoted names in upper case. The client folds each part to upper
  case and then double-quotes it, so `analytics.orders` runs as
  `"ANALYTICS"."ORDERS"`. Reserved words such as `ORDER` or `TIMESTAMP` work.
  A table or column that was created with quotes and a lower- or mixed-case name
  cannot be reached, with one exception: the primary key column. `PrimaryKeyColumn`
  returns it spelled as Snowflake stores it (`ID` if unquoted, `id` if created as `"id"`),
  and `Row` looks the declared key up and quotes it exactly as stored, so a quoted `"id"`
  key, an unquoted `ID` key and a table with both are told apart. A caller that names the
  key in another case (the trace passes the source's `id`) is mapped to the one key column
  that matches ignoring case; any other name is upper-cased as before. In the returned row,
  column names are lower-cased unless two columns would collide (`id` and `ID`), which keep
  their exact names. `Row` runs one extra `SHOW PRIMARY KEYS` per call.
- Freshness is `SELECT MAX("TS") FROM "SCHEMA"."TABLE"`, returned in UTC.
  `TIMESTAMP_NTZ` is read as UTC wall time. `TIMESTAMP_LTZ` and `TIMESTAMP_TZ` are exact instants.
  The column must be a timestamp type.
- Row lookups use a `?` bind parameter for the key and return column names in lower
  case, like the Postgres client.
- Every call has a 10 second deadline. A missing table, or one the role cannot see,
  maps to `clients.ErrNotFound`.

Not supported: key-pair or OAuth setup helpers (put those options in the DSN as the driver
documents), and tables whose names need quoting.
