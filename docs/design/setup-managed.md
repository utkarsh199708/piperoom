# Setup on managed clouds: MSK Connect, Confluent Cloud, Aiven (U13-0; main @ `2681f65b`, 2026-10-19; rev 3 after the #660 reviews)

Owner: LEAD-ARCH. Parent design: `docs/design/setup-agent.md`; plan: `docs/product/setup-2026-10.md` (§4 row U13,
§7 items 1 and 6); board: `tasks/U-board.md` rows U13-0 to U13-R; the earlier MSK design `docs/design/msk.md`
(rev 4, read-only scope; its PR 1, the shared `aws.Config` and `AWS_MSK_IAM`, is merged in `internal/wiring/aws.go`
and `internal/clients/netsec`; its PR 2, `CONNECT_KIND=msk` and `internal/clients/mskconnect`, is **not** merged:
`internal/clients/` has no `mskconnect` and `config.go` has no `CONNECT_KIND`). The contracts are the merged U0
ones. Vocabulary as in the parent: runner, ops, catalogue, tier floors T0–T3, classes, validated or templated
artifacts, the ladder, the guard, the `Credentials` frame.

**Conflicts with the board are listed in §9 as proposed amendments.** The contract part of this row (parent §4
item 7) lands in **one separate leads PR, "U12-0/U13-0 contracts"**, merged before U12a, U13a and U13c start; the
board marks U13-0 done only when that PR merges. §10 lists the decisions the owner makes.

**Rev 3** (round 2): the Aiven token age is a card rule, not a check; D5 is T3 when no price is returned; D6
records the watcher identity's `DescribeConnector` access as an exception to parent §3.7; D9's default is a
`read_only` API token or an unreachable Connect layer, never a full-rights user.
**Rev 2** answers the #660 reviews: cloud setup credentials are entered on the runner only, never through the room's
secret field (S6, §2); the cassette scrubber rewrites response bodies by key and URL userinfo (S5, §5); the Confluent
read account is created by the person, not by an org-level setup key; no global `CONFLUENT_*` server variables; the
S3 digest check is "size only" stated honestly; `iam:CreateServiceLinkedRole` instead of ENI permissions; the board
conflicts are listed (A2, §9); one spelling, `unreachable_layers` (A4).

**What was verified and how.** AWS SDK names below were checked with `go doc` on
`aws-sdk-go-v2/service/kafkaconnect v1.39.5`, `service/kafka`, `service/sts`, `service/ec2`, `service/iam`,
`service/s3` and `credentials/stscreds` on 2026-10-19 in a scratch module (outside the repo; the repo's `go.mod`
pins `aws-sdk-go-v2 v1.43.0` and `config v1.32.31`, no `service/kafkaconnect` yet). Confluent Cloud paths were read
from the generated SDK docs and OpenAPI in `confluentinc/ccloud-sdk-go-v2` (`connect/v1`); Aiven paths and the auth
header from `aiven/aiven-client` (`aiven/client/client.py`). The vendors' documentation sites were not reachable
from this session, so **role names, scopes, response schemas and plugin names that only the docs state are marked
unverified** and are confirmed by the U13a recording session on the sandbox accounts before any row relies on them.

## 1. Decisions in one table

| Question (board U13-0) | Decision |
|---|---|
| Where the runner runs | on a bastion or laptop that holds the cloud credentials and reaches the database; **the server never holds or carries a cloud credential**: cloud setup credentials are entered on the runner's terminal or environment only, and the room's secret route refuses their names (§2) |
| AWS auth | the SDK default chain through the shared `aws.Config` pattern (`wiring.LoadAWS`): SSO, a named profile, an assumed role, an instance or task role; **never a long-lived key stored by Piperoom** (§2.1) |
| Confluent auth | a **Cloud API key** of a setup service account for the Connect v1 API (basic auth over https to `api.confluent.cloud`), a **cluster API key** of the same account for the Kafka REST v3 ACLs; the watcher's read-only service account and keys are created **by the person** from a card, so the setup key needs a role on one cluster only (§2.2) |
| Aiven auth | an **application-user token** scoped to the one project, `max_age_seconds` ≤ 24 h as a rule (`Authorization: aivenv1 <token>`); the watcher uses the Connect service's own REST API with a service user, not the Aiven API (§2.3) |
| Clients | three packages (C21): `internal/clients/mskconnect` over the SDK, `internal/clients/confluentcloud` and `internal/clients/aiven` over `net/http`; each implements `clients.ConnectClient` (status, config, plugins) so the Q tools and the watcher run unchanged, plus a `Setup*` write interface used only by the ops (§3) |
| Recorded fakes | `httptest` cassettes recorded on the sandbox with `//go:build manual`, scrubbed at record time in **requests and responses** (ids, hostnames, every secret-named JSON value, URL userinfo), replayed in `go test ./...`; a cassette with a credential-shaped string anywhere fails the loader (§5) |
| Sandboxes | one account per provider held by the owner (plan §7 item 6), keys to the leads only, a `piperoom-sandbox` tag or label on every resource, torn down by a script after each manual run (§6) |
| Watching afterwards | MSK: the server's own IAM identity with the read policy of `msk.md` §4 (needs `CONNECT_KIND=msk`, §9 C1); Confluent: the person-created read-only account's keys registered through the `Credentials` frame and a `CONNECT_KIND=confluent` client; Aiven: the Connect REST API over basic auth with a service user (today's REST client) (§7) |

## 2. Authentication per provider

**One rule for all three (S6):** a cloud setup credential (an AWS session, a Confluent Cloud or cluster API key, an
Aiven token) is entered on the runner's terminal (`secret_set` prompt) or in the environment the runner was started
from, **never** through `POST /api/rooms/{id}/setup/secret`: that route forwards a value through the server's memory
and request path (parent §2.4), which is fine for a database password and wrong for a credential that can create
cloud resources. The server's secret route refuses the names `ccloud_api_key`, `ccloud_api_secret`,
`ccloud_cluster_key`, `ccloud_cluster_secret`, `aiven_token` and every name starting `aws_` or `AWS_` with
`400 validation` ("enter cloud credentials on the runner"); the runner's store refuses `AWS_*`/`aws_*` names outright
(the SDK chain is the only AWS credential source); a table test covers each name on both sides. At room close the
runner **drops the cloud setup names from its store** and the close card tells the person to delete the setup key or
token and its service account; the Aiven token's 24-hour age makes a forgotten one expire anyway.

### 2.1 AWS MSK Connect: SigV4 over the shared AWS config

- The runner loads **one** `aws.Config` at start the way `wiring.LoadAWS` does
  (`awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))`, verified), with the region from the
  `SetupTarget` of kind `aws` (`Address: <account id>/<region>`, §9 C2) or `AWS_REGION`, and fetches credentials
  once (5 s) so a broken chain stops pairing, not a step. Every SDK client is `NewFromConfig(cfg)`; SigV4 is the
  SDK's; the runner signs nothing itself.
- **Which chain.** In this order, the SDK's own: environment variables, the shared config and credentials files
  with `AWS_PROFILE` (an **SSO profile** after `aws sso login` is the recommended laptop path and is what the user
  page tells people to do), `AWS_ROLE_ARN` with `AWS_WEB_IDENTITY_TOKEN_FILE` (a CI or bastion with OIDC), the
  container credentials endpoint, IMDS (a bastion EC2 instance role). Cross-account: `stscreds.NewAssumeRoleProvider
  (stsClient, roleARN)` (verified) when the person lists a `role_arn` on the target; the runner never asks for an
  external id in the room.
- **Never long-lived keys in Piperoom.** A chain that resolves to static keys in the person's own
  `~/.aws/credentials` is theirs, outside the project directory and never read by any op (`file_read` is confined to
  the project dir). The server holds no AWS key for setup; for watching it uses its own identity (§7).
- **Identity and class.** `env_detect` calls `sts.GetCallerIdentity` (verified: `Account`, `Arn`, `UserId`) and
  reports the account id and the ARN's role or user name (never the full ARN of a user, which can carry a person's
  name, in `Summary`; it is in `Data`). The account id is what `PIPEROOM_SETUP_PROD_PATTERNS` matches for the
  `production` class, combined with the person's floor.
- Secrets in connector configs: MSK Connect has no secret provider of its own; the database password is a
  **materialisation point in the API call** (parent §2.4: "cloud API calls"), placed into
  `CreateConnectorInput.ConnectorConfiguration` on the runner from `${secret:}` references, and the `mskconnect`
  client redacts every key `actions.LooksSecret` matches in `DescribeConnectorOutput.ConnectorConfiguration` before
  it reaches `Data` (as the REST client's `connect_config` redaction does). A config-provider plugin
  (Secrets Manager) bundled into the custom plugin is a U13 follow-up, not the first version (§10 D6).

### 2.2 Confluent Cloud: API keys

Two kinds of key, two scopes, both **basic auth over https only** (`netsec`'s rule; the client refuses `http`):

- A **Cloud API key** (`username: <key>`, `password: <secret>`) authenticates to `https://api.confluent.cloud`:
  the Connect v1 API (plugins, validate, create, status, delete). It belongs to a service account the person
  creates for the setup (`piperoom-setup-<pipeline>`) with the least role that can create a managed connector on
  **one** cluster and nothing on others. The role name is **unverified** (the Confluent RBAC catalogue has
  `CloudClusterAdmin`, `Operator`, `DeveloperManage` and others; which one suffices for `createConnectv1Connector`
  on one cluster is read from the sandbox in U13a and written into `docs/pilot` by U13f). The setup key is entered
  on the runner (§2) as `${secret:ccloud_api_key}`/`${secret:ccloud_api_secret}`.
- A **cluster API key** (the same service account, scoped to the Kafka cluster) authenticates to the cluster's own
  REST endpoint for ACLs (`${secret:ccloud_cluster_key}`/`_secret`); Confluent's managed connectors then run with
  `kafka.auth.mode: SERVICE_ACCOUNT` and the service account id, so no key sits in the connector config
  (**unverified** key names; `ccloud_list_plugins` and the validate call tell).
- **The watcher's account is created by the person, not the runner** (S-review, least privilege): creating service
  accounts and API keys through IAM v2 needs an organisation-level role, which the setup key must not hold. The
  room shows a card with the exact steps (service account `piperoom-ro-<pipeline>`; one cluster API key; one Cloud
  API key with a read-only role on the cluster, **unverified** which: `Operator` or a viewer role is the
  hypothesis), the person types the four values on the runner's terminal (`secret_set`), and the runner applies the
  ACLs with `ccloud_acl_create` using the setup account's cluster key: `DESCRIBE` and `READ` on topics
  `<topic_prefix>` (prefixed), `DESCRIBE` on the connector's consumer group, `DESCRIBE_CONFIGS` on the topics. Those
  four values are what **Register read credentials** sends later (§7).

### 2.3 Aiven: tokens

- `aiven-client` sends `authorization: aivenv1 <token>` (verified) with `https://api.aiven.io/v1` as the prefix.
  Tokens are per user; an **application user** (organisation-level, `POST /v1/organization/{org}/application-users/
  {user}/access-tokens`, verified path) is the service-account equivalent and is what the setup uses, made a member
  of **one project** with the least project role that can create a Kafka Connect service and connectors (the roles
  are `admin`, `operator`, `developer`, `read_only`; which of `operator` or `developer` suffices is **unverified**
  until U13a). **Card rule:** the card tells the person to create the token with `max_age_seconds` ≤ 86400 and
  shows the exact `avn user access-token create --max-age-seconds 86400 --description piperoom-setup` line; the
  runner cannot verify a token's age (no introspection endpoint is verified here; if U13a finds one, the check
  becomes real and this sentence changes), so the rule is the card's and the close card repeats "revoke the setup
  token". A token under 20 bytes is refused. It is entered on the runner as `${secret:aiven_token}` (§2).
- **Read-only where possible**: the watcher does not use an Aiven token at all. An Aiven Kafka Connect service
  exposes the standard Connect REST API on `https://<service>-<project>.aivencloud.com:443` with the service's
  users (basic auth). **Unverified**: Aiven Kafka Connect service users may all carry full rights on the Connect REST
  API; if so, no service user is registered for the watcher, and status reads go through the `aiven` client with a
  `read_only`-role application-user token (`GET .../connectors/{name}/status`, verified path) or the Connect layer
  is marked unreachable (§10 D9). Decided after U13a.
- The token is in a header, never a URL (U13d's "Done when").

## 3. Provider operations the setup needs

Per provider: what setup needs (plan §4 table), the call, verification status, and the op. Reads are tools
through the remote client (T0); writes are ops behind proposals (T2, T3 on `production`). Ops not on the board's
rows are marked **(new)** and listed in §9 A2.

### 3.1 AWS MSK Connect (`internal/clients/mskconnect`, ops `msk_*`)

| Need | SDK call (package `service/...`) | Verified | Op |
|---|---|---|---|
| who am I, which account | `sts.GetCallerIdentity` → `Account`, `Arn` | yes | `env_detect` |
| the MSK cluster: state, version, auth | `kafka.DescribeClusterV2{ClusterArn}`; `kafka.GetBootstrapBrokers{ClusterArn}` → `BootstrapBrokerStringSaslIam` (IAM), `...SaslScram`, `...Tls` | yes (the output field list) | `msk_describe_cluster` [T0] |
| the cluster's network | `ec2.DescribeSubnets`, `ec2.DescribeSecurityGroups`, `ec2.DescribeRouteTables` for the cluster's subnets and the connector's intended subnets and groups | names yes; the "no route to the database" verdict is a rule over their output, tested on cassettes | `msk_vpc_check` [T0] **(new)** |
| plugins | `kafkaconnect.ListCustomPlugins{NamePrefix, MaxResults, NextToken}`; `DescribeCustomPlugin` → `CustomPluginState` (`CREATING`, `ACTIVE`, ...), `LatestRevision`, `StateDescription` | yes | `msk_list_plugins` [T0] |
| the plugin archive the person uploaded | `s3.HeadObject{Bucket, Key}` (exists, `ContentLength`, `VersionId`) | yes | `msk_plugin_object_check` [T0] **(new)**; the runner **never uploads** (no `s3:PutObject` in the policy): the person uploads the Debezium plugin zip whose SHA-512 the knowledge index pins. **The op checks size only**: S3 exposes `ChecksumSHA256` (not SHA-512), only with `ChecksumMode: ENABLED`, and for a multipart upload it is a composite of part checksums, not the file digest; so the preview of `msk_create_plugin` says "size matches the index; digest unverified" and the person confirms. Pinning SHA-256 in the index and handling the single-part case is a later improvement (§9 A2) |
| the service execution role | `iam.GetRole{RoleName}`; `iam.SimulatePrincipalPolicy{PolicySourceArn, ActionNames}` for `kafka-cluster:Connect`, `kafka-cluster:DescribeCluster`, `logs:CreateLogStream`, `logs:PutLogEvents` | names yes; semantics of the simulation on MSK IAM actions unverified | `msk_role_check` [T0] **(new)** |
| connector status | `kafkaconnect.DescribeConnector{ConnectorArn}` → `ConnectorState`, `StateDescription{Code, Message}`, `ConnectorConfiguration`, `LogDelivery`; `ListConnectors{ConnectorNamePrefix}` for name → ARN (`msk.md` §3a) | yes | `connect.status` through the remote client; the ladder's rung 1 |
| worker logs | `cloudwatchlogs.FilterLogEvents` on the delivered log group (`msk.md` §3e) | yes (msk.md) | `connect_worker_logs` (Q3c) over the remote client |
| create the custom plugin | `kafkaconnect.CreateCustomPlugin{Name, ContentType: ZIP | JAR, Location: {S3Location: {BucketArn, FileKey, ObjectVersion}}}` | yes | `msk_create_plugin` [T2]; async: the op returns the ARN and `CREATING`; `msk_wait {kind, arn}` **(new)** polls `DescribeCustomPlugin` to `ACTIVE` (deadline 10 min) |
| the worker configuration | `kafkaconnect.CreateWorkerConfiguration{Name, PropertiesFileContent}` | yes | `msk_create_worker_config` [T2]; the properties are rendered from the driver's template under the properties key allow-list (converters, `config.providers` absent in v1, `offset.storage.topic` family absent: MSK Connect manages them) |
| create the connector | `kafkaconnect.CreateConnector{ConnectorName, KafkaConnectVersion, Capacity{ProvisionedCapacity{McuCount: 1, WorkerCount: 1}}, KafkaCluster{ApacheKafkaCluster{BootstrapServers, Vpc{Subnets, SecurityGroups}}}, KafkaClusterClientAuthentication{AuthenticationType: IAM}, KafkaClusterEncryptionInTransit{EncryptionType: TLS}, Plugins: [{CustomPlugin{CustomPluginArn, Revision}}], ServiceExecutionRoleArn, WorkerConfiguration{WorkerConfigurationArn, Revision}, ConnectorConfiguration, LogDelivery{WorkerLogDelivery{CloudWatchLogs{Enabled: true, LogGroup}}}}` → `ConnectorArn`, `ConnectorState` | yes (every field name) | `msk_create_connector` [T2]; async (`CREATING` for 5–15 minutes); `msk_wait` polls to `RUNNING` or `FAILED` with `StateDescription` |
| delete (rollback) | `kafkaconnect.DeleteConnector{ConnectorArn, CurrentVersion}`; `DeleteCustomPlugin`, `DeleteWorkerConfiguration` | yes | `msk_delete_connector` [T2; T3 on production; receipt-created only]; plugin and worker config deletes are rollback-only and receipt-bound |
| the log group | `cloudwatchlogs.CreateLogGroup` | name unverified in this session (msk.md verified `FilterLogEvents` only) | `msk_create_log_group` [T2] **(new)**, optional: the person may name an existing group instead |

Capacity is provisioned 1 MCU × 1 worker in v1; `AutoScaling` fields exist (verified) and are refused as params.
`KafkaConnectVersion` is from the knowledge index (`2.7.1` family; the exact strings MSK Connect accepts are
unverified and come from the sandbox). `NetworkType` stays the default.

### 3.2 Confluent Cloud (`internal/clients/confluentcloud`, ops `ccloud_*`)

Base `https://api.confluent.cloud`; every Connect path is under
`/connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}` (verified from the SDK docs):

| Need | Method and path | Verified | Op |
|---|---|---|---|
| plugins | `GET /connector-plugins` | yes | `ccloud_list_plugins` [T0] |
| validate a config | `PUT /connector-plugins/{plugin_name}/config/validate` with the config as the JSON body | path yes; the response shape (`configs[].value.errors`, like Connect's own `config/validate`) unverified | the Q4a `connect_validate_config` through `clients.ConfigValidator` over this client |
| create | `POST /connectors` with `{"name", "config": {...}}` | yes | `ccloud_create_connector` [T2], validate-then-create, never without a green validate |
| status | `GET /connectors/{connector_name}/status` | yes | `connect.status` (ladder rung 1) |
| list, read, config | `GET /connectors`, `GET /connectors/{name}`, `GET /connectors/{name}/config` | yes | `connect.list`, `connect.config` |
| delete (rollback) | `DELETE /connectors/{connector_name}` | yes | `ccloud_delete_connector` [T2; T3 on production; receipt-created only] |
| pause, resume, offsets | `PUT .../pause`, `PUT .../resume`, `GET .../offsets`, `POST .../offsets/request` | yes | not in U13 (Q5c's lifecycle on managed is a follow-up) |
| the cluster, environment | Kafka clusters v2 `GET /cmk/v2/clusters/{id}` (bootstrap endpoint, REST endpoint) | path unverified | `ccloud_cluster` [T0] **(new)** |
| ACLs for the connector's and the watcher's accounts | Kafka REST v3 on the cluster's REST endpoint: `POST /kafka/v3/clusters/{cluster_id}/acls` with the cluster API key | path unverified | `ccloud_acl_create` [T2], ACL table fixed per template (the Strimzi design's §6 tables, same rows) |
| service accounts and keys | IAM v2 (`/iam/v2/service-accounts`, `/iam/v2/api-keys`; paths unverified) | — | **no op** (rev 1 had `ccloud_create_read_account`; the person creates the read account from a card, §2.2) |

The managed Debezium plugin names (`PostgresCdcSourceV2`, `MySqlCdcSourceV2`, ...) and the required
`kafka.auth.mode`/`kafka.service.account.id` keys are **unverified**: `ccloud_list_plugins` is the source of truth
at run time and the knowledge index (C22) carries the names the sandbox returned, dated. Managed connectors on
Confluent Cloud need the database reachable from Confluent's egress IPs or a private link; `net_probe` from the
runner cannot prove that, so the plan's "connect" phase has a check that reads the status's trace for
`Connection refused`/`timeout` and asks the person to allow-list the egress ranges (`setup.blocked`), never a
security-group write.

### 3.3 Aiven (`internal/clients/aiven`, ops `aiven_*`)

Base `https://api.aiven.io/v1`; paths verified from `aiven-client`:

| Need | Method and path | Verified | Op |
|---|---|---|---|
| the service | `GET /project/{project}/service/{service}` (state, plan, `service_uri`, components, users) | yes | `aiven_service_get` [T0]; the client **drops `service_uri` userinfo, `users[].password`, `access_cert`, `access_key` and `connection_info` secrets** before `Data` |
| plugins | `GET /project/{project}/service/{service}/available-connectors` | yes | `aiven_list_plugins` [T0] |
| connectors and status | `GET /project/{project}/service/{service}/connectors`; `GET .../connectors/{name}/status` | yes | `connect.list`, `connect.status` |
| create a connector | `POST /project/{project}/service/{service}/connectors` with the config | yes | `aiven_create_connector` [T2]; the Connect REST `config/validate` on the service's own REST endpoint is the validate step (Q4a over basic auth) |
| create a Connect service | `POST /project/{project}/service` (`service_type: kafka_connect`, `plan`, `cloud`, `service_name`) | yes | `aiven_create_service` [T2; T3 on production; the plan name from a small allow-list; the cloud region from the target] **(new; billable, §10 D5)** |
| integration (Kafka ↔ Connect) | `POST /project/{project}/integration` (`integration_type: kafka_connect`, `source_service`, `dest_service`) | yes | `aiven_integration_create` [T2] |
| delete (rollback) | `DELETE .../connectors/{name}`; `DELETE /project/{project}/service/{service}` | connector path implied by the client's `delete_kafka_connector` (unverified exactly); service delete unverified | `aiven_delete_connector` [T2]; a **service delete is never an op** (the guard's "deleting a cloud resource" row); the rollback card names the service to delete by hand |
| service users (the watcher's read user) | `POST /project/{project}/service/{service}/user` | unverified | `aiven_create_read_user` [T2] **(new)**; the response carries the password: the client stores it in the runner's secret store and returns the name only, and the cassette scrubber rewrites it (§5) |
| Kafka ACLs for the read user | Aiven Kafka ACL API (`POST /project/{project}/service/{service}/acl`, unverified) | unverified | `aiven_acl_create` [T2] **(new)** |

Aiven secrets: the connector config carries the database password; Aiven stores it and masks it on reads
(unverified: the client redacts with `actions.LooksSecret` regardless).

## 4. Tiers, classes and the guard

| Class source | Rule |
|---|---|
| `production` | the account id (AWS), environment or cluster id (Confluent), project name (Aiven) matches `PIPEROOM_SETUP_PROD_PATTERNS`, or the person's floor |
| `shared` | every managed target otherwise; **never `local`** |

Every read op is T0 and needs no approval; every write is T2 (approval by an overlapping pipeline's owners, else a
tenant admin; the driver in solo or standalone mode) and T3 on `production` with the typed confirm and the
runner's terminal "y". Guard rows added to the parent table, tested per row at the four check points: delete a
connector not created by this room (every class); delete a custom plugin, worker configuration or Aiven service
(data or shared resources: `production` refused; `shared` only as the rollback of a receipt-created resource);
`UpdateConnector` (capacity) and `RestartConnector` do not exist as ops in U13. The guard row "deleting a cloud
resource" from plan §3.6 covers the rest.

Deadlines: every API call 10 s except `msk_wait`'s polling (op deadline up to 15 min, one step); throttling
(`TooManyRequestsException`, HTTP 429 with `Retry-After`) is retried with backoff three times inside the op and
then reported as a failed step, never looped by the model.

## 5. The recorded-fakes harness (U13a, `internal/setup/targets/managed/replay`)

- **Record** with `//go:build manual` and the sandbox credentials in the environment: each client takes an
  `http.RoundTripper`; the recording transport writes one cassette per test (`testdata/<provider>/<case>.json`:
  request method, path, query with ids rewritten, request body scrubbed, status, response headers except `Date`,
  `x-amzn-RequestId`, `Set-Cookie`, response body scrubbed). For the AWS SDK the transport is set on
  `aws.Config.HTTPClient` (verified field); SigV4 headers (`Authorization`, `X-Amz-Date`, `X-Amz-Security-Token`)
  are dropped at record time and the replayer matches on method, path and a hash of the normalised body.
- **Scrub at record time, not replay time, requests and responses alike (S5).** Identifiers: account ids (12
  digits) → `000000000000`; ARNs' account and region → fixed; S3 bucket names, Confluent environment (`env-xxxxx`),
  cluster (`lkc-xxxxx`) and service-account ids, Aiven project and service names, every hostname under
  `amazonaws.com`, `confluent.cloud`, `aivencloud.com` → fixed placeholders. Secrets: in **every** JSON body, request
  or response, every value whose key `actions.LooksSecret` matches, plus a **per-provider key table** (`secret`,
  `password`, `service_uri`, `connection_info`, `access_key`, `access_cert`, `users[].password`, `token`,
  `ConnectorConfiguration.*` for keys `LooksSecret` matches) → `REDACTED`; every `Authorization` header →
  `REDACTED`; **URL userinfo** (`https://user:pass@host`) in any string value → `https://REDACTED@host`. The
  scrubber is a table in code with a test per row, and each provider's create-key, create-user and describe
  cassette (the IAM v2 `spec.secret` shape, Aiven `service_uri` and `users[]`, `DescribeConnector`
  `ConnectorConfiguration`) has a dedicated test asserting the response was rewritten.
- **The loader test** (board U13a "Done when"): every cassette, request and response, is scanned at test time with
  `redact.Text`'s patterns plus the AWS key shapes (`AKIA[0-9A-Z]{16}`, 40-byte secrets), Confluent key shapes,
  `aivenv1 `, userinfo in URLs, and any JSON value under a key of the table above that is not `REDACTED`; a hit
  fails the test that loads it, so a cassette that slipped past the scrubber cannot merge. The patterns cannot catch
  a random password under an unknown key; the key table and the per-cassette tests are the defence, and a new
  provider endpoint adds its keys to the table in the same PR.
- **Replay** in `go test ./...` without network: an unmatched request fails the test with the method and path; the
  cassettes cover 200, 400 (`BadRequestException`), 403 (`ForbiddenException`, `UnauthorizedException`), 404
  (`NotFoundException`), 409 (`ConflictException`, an existing name), 429/`TooManyRequestsException`, a cut body,
  and the async state sequences (`CREATING` × n → `ACTIVE`/`RUNNING`, and → `FAILED` with a `StateDescription`).
- **Evals** (U13e) replay the same cassettes through the runner recordings of `evals/setup`.
- Re-recording is a manual row dated in the PR; a cassette older than 180 days is flagged by a test as stale (a
  warning, not a failure).

## 6. Sandbox accounts (plan §7 item 6)

| Provider | What the owner provisions | Who holds the keys | Teardown |
|---|---|---|---|
| AWS | one account (or an OU member) with: an MSK Serverless or a small provisioned cluster with IAM auth in a VPC, an RDS Postgres in the same VPC (the U13-R source), an S3 bucket for plugin archives, a service execution role, a CloudWatch log group; an SSO permission set `PiperoomSetup` with the write policy of §8 for the leads, and a role `PiperoomWatch` with the read policy | the leads (SSO; no static keys are created) | `dev/sandbox/aws-teardown.sh` deletes connectors, plugins and worker configs tagged `piperoom-sandbox=true`; the cluster and RDS stay (standing cost is the owner's call, §10 D7; a stopped RDS is the default between runs) |
| Confluent Cloud | one organisation with one environment and one Basic cluster; a service account per role (`piperoom-setup`, `piperoom-ro`) with their keys, created by hand | the leads | the script deletes connectors by name prefix `it_` |
| Aiven | one project on the free or startup plan with a Kafka service; an application user with a project-scoped token | the leads | the script deletes connectors with prefix `it_` and the Connect service the run created when `--delete-service` is passed by hand |

Keys are given to the leads only, through the owner's password manager, never in the repo, CI secrets or Slack;
the manual rows (U13b1, U13c, U13d, U13-R) run on a lead's machine. CI never has a cloud credential: the harness of
§5 is CI's whole exposure.

## 7. How the server watches a managed pipeline afterwards

Parent §3.7: a managed target is `shared` or `production`, so the team server watches, with read-only credentials
that reach it only through the `Credentials` frame after the person's click and terminal "y".

| Provider | Kafka reads | Connect reads | What the `Credentials` frame carries | `connections:` written by `pipeline_register` |
|---|---|---|---|---|
| MSK | `AWS_MSK_IAM` with the **server's own identity** (IRSA, an instance role or the chain) over `wiring.LoadAWS`, reaching the cluster's IAM listener (9098) from inside the VPC or over VPC connectivity | `CONNECT_KIND=msk` (msk.md PR 2, **not merged**: §9 C1) with `kafkaconnect:ListConnectors`, `DescribeConnector`, `logs:FilterLogEvents`; **note (D6)**: `DescribeConnector` returns `ConnectorConfiguration` with the materialised DB password, so the watcher identity receives it on every status read; the client redacts it before `Data` and stores nothing, an exception to parent §3.7 recorded in §10 D6 | only the RDS read role's password (`source_create_read_role` on the runner) | `kafka_brokers: {value: <IAM bootstrap>}`, `kafka_security_protocol: {value: SASL_SSL}`, `kafka_sasl_mechanism: {value: AWS_MSK_IAM}`, `source_db_url: {file: ...}`; the registration card names the IAM role the server runs as and the policy it needs |
| Confluent Cloud | SASL_SSL PLAIN with the `piperoom-ro` cluster API key (ACLs of §2.2) | `CONNECT_KIND=confluent` (new, §9 C1) with the `piperoom-ro` Cloud API key, **per pipeline** from `connections:` (no server-wide Confluent variables: a long-lived key shared across tenants on the server is exactly what tenancy §3.2 forbids) | `kafka_sasl_username`, `kafka_sasl_password` (the cluster key), `ccloud_api_key`, `ccloud_api_secret` (the person-created read account's keys, typed on the runner), the DB read role's password | `kafka_brokers`, `kafka_security_protocol: {value: SASL_SSL}`, `kafka_sasl_mechanism: {value: PLAIN}`, the two `kafka_sasl_*` files, `connect_kind: {value: confluent}`, `connect_url: {value: https://api.confluent.cloud/connect/v1/environments/<env>/clusters/<lkc>}`, `connect_username`, `connect_password` (the Cloud key, `file:`) |
| Aiven | SASL_SSL SCRAM-SHA-256 or TLS client certs with a Kafka service user the setup created with read ACLs (`aiven_create_read_user`, `aiven_acl_create`) | **today's REST client** against `https://<connect service>-<project>.aivencloud.com:443` with basic auth of a Connect service user (§2.3's caveat, §10 D9) | the Kafka user's password or cert and key, the Connect user's password, the DB read role's password | `kafka_brokers`, `kafka_security_protocol: {value: SASL_SSL}`, `kafka_sasl_mechanism: {value: SCRAM-SHA-256}`, `kafka_tls_ca_file` (the project CA, `GET /project/{project}/kms/ca`, unverified; the service's `connection_info` is the fallback), `connect_url`, `connect_username`, `connect_password` |

Key names in the frame match the `tenancy.md` §3.2 keys so `pipeline_register` can map one to one; the frame's
`Secret.Name` rule (`^[a-zA-Z_][a-zA-Z0-9_]*$`) holds for all of them. Every value is generated or created by the
runner's op (`CreateReadRole`, the read user ops) or typed on the runner for the read account the person created;
the person's own setup credentials (the SSO session, the setup service account's keys, the Aiven token) are
**never** registered: a test asserts the `RegisterCredentials.Names` set the server may request excludes them.

Where the server cannot reach the Kafka listener (an MSK cluster with no VPC connectivity from the server's
network), the ladder's Kafka rungs still pass through the runner's remote client; `pipeline_register` then records
`setup: {watched_by: server, unreachable_layers: [kafka]}` (§9 C4) so the watcher skips those layers with a
reason rather than alerting forever; the registration card lists them.

## 8. IAM policy for the setup identity (AWS)

Read set (the watcher's and the probes'): `msk.md` §4's policy plus `kafka:DescribeClusterV2`, `kafka:GetBootstrapBrokers`,
`kafkaconnect:ListCustomPlugins`, `kafkaconnect:DescribeCustomPlugin`, `kafkaconnect:ListWorkerConfigurations`,
`kafkaconnect:DescribeWorkerConfiguration`, `ec2:DescribeSubnets`, `ec2:DescribeSecurityGroups`,
`ec2:DescribeRouteTables`, `s3:GetObject` and `s3:GetObjectAttributes` on the plugin bucket, `iam:GetRole` and
`iam:SimulatePrincipalPolicy` on the service execution role, `sts:GetCallerIdentity`. Write set (setup only):
`kafkaconnect:CreateCustomPlugin`, `CreateWorkerConfiguration`, `CreateConnector`, `DeleteConnector`,
`DeleteCustomPlugin`, `DeleteWorkerConfiguration`, `TagResource`, `iam:PassRole` on the service execution role
(required by `CreateConnector`), `logs:CreateLogGroup` on `/piperoom/*`, and `iam:CreateServiceLinkedRole` for
`kafkaconnect.amazonaws.com` (the first use of MSK Connect in an account creates `AWSServiceRoleForKafkaConnect`;
**unverified** whether the caller needs the grant or the console has usually created it; the sandbox's first run
tells). MSK Connect's ENIs in the connector's subnets are created by that service-linked role, so the setup
identity gets **no** `ec2:CreateNetworkInterface` unless the sandbox shows otherwise. Resource ARNs are scoped to
the one cluster, bucket, role and log group prefix; no `*` action. The exact action names are the AWS IAM names and
are checked against the sandbox's CloudTrail after the first manual run (U13b1's "manual check dated in the PR").

## 9. Open questions / contract impact

**A. Conflicts with the board (proposed amendments, carried by the leads' contracts PR):**

- **A2, ops beyond board rows U13b2, U13c, U13d.** New on U13b2: `msk_vpc_check`, `msk_role_check`,
  `msk_plugin_object_check`, `msk_create_log_group`, `msk_wait` (+1 day, 2.5). New on U13c: `ccloud_cluster`
  (+0.25 day); `ccloud_create_read_account` is **not** added (the person creates the account, §2.2). New on U13d:
  `aiven_create_service` (if D5 allows it), `aiven_create_read_user`, `aiven_acl_create` (+0.75 day, 2.75). U13a
  gains the response-body scrubber and per-cassette tests (+0.5 day, 1.5). §7's watching depends on the read-user
  ops, so they are not optional.
- **A watcher follow-up row (A7)**: skip the layers in `setup.unreachable_layers` with a reason in the room (C4).

**B. Open questions:** none beyond §10; rev 1's Q3, Q6 are owner decisions D6, D5 there; Q5 (unverified role names
and paths) is settled by U13a's recording session and written into the knowledge index and `docs/pilot` (U13f)
with the date; Q7 (MSK VPC reachability) stays a person's change, covered by the `setup/msk-vpc-no-route`
scenario.

**C. Contract impact (the same separate leads PR as the Strimzi design's §11 C-list):**

- **C1, `CONNECT_KIND`.** `config.go` gains `ConnectKind string // CONNECT_KIND: rest (default) | msk | confluent
  | aiven` (folding msk.md PR 2's stalled `msk` into one enum; `aiven` is the `read_only`-token status client of
  D9; `strimzi` later per the Strimzi design's D4) with the msk.md
  §5.2 `MSK_CONNECT_*` variables; **no** server-wide `CONFLUENT_*` variables. `tenancy.md` §3.2 gains the
  per-pipeline keys `connect_kind` (literal), `ccloud_api_key`, `ccloud_api_secret` (`file:` only), and the loader
  refuses `connect_kind: confluent` without both. The client packages are C21's (board); the wiring line is A4's,
  asked in thread.
- **C2, `SetupTarget.Address` for cloud accounts** (doc comment only): kind `aws` → `<account id>/<region>[/<cluster
  arn>]`; kind `confluent` → `<environment id>/<cluster id>`; kind `aiven` → `<project>/<service>`.
  `PIPEROOM_SETUP_PROD_PATTERNS` matches the first segment. The server's denylist skips DNS resolution for these
  non-host addresses; the runner's dial hook stays the guarantee.
- **C3, the secret route's refused names** (`docs/api.md` `POST /api/rooms/{id}/setup/secret`): `ccloud_*`,
  `aiven_token`, `aws_*`/`AWS_*` answer `400 validation` with "enter cloud credentials on the runner" (§2).
- **C4, owners `setup:` block** gains `unreachable_layers: []` (U0 item 5, A3's loader); shared with the Strimzi
  design's C8.
- **C5, `Welcome.Fingerprint`/`PublicOrigin` and the `fingerprint` field of `POST /api/setup`** are the Strimzi
  design's C6/C7 and apply to managed rooms too (one rule for every setup room).

## 10. Owner decisions (recommended default first)

| # | Decision | Recommended default | Alternative |
|---|---|---|---|
| D5 | Billable `aiven_create_service` | **allow it**, T2 with the plan name and, when the service-types API returns it (`GET /project/{project}/service_types`, unverified), the monthly price in the preview; **when the price is not returned the step is T3 with the typed confirm on every class**; T3 on a production project regardless | require an existing Connect service (the form asks for its name; U13d loses one op and 0.25 day) |
| D6 | MSK secrets in v1 | **accept** the DB password materialised into `ConnectorConfiguration` (encrypted at rest by the service, readable by any `DescribeConnector` principal), redacted on every read, stated on the user page. **This is an explicit exception to `setup-agent.md` §3.7** ("the model never sees them" holds; "credentials never cross to a team server except by the person's click" does not): the server's watcher identity calls `DescribeConnector` (§7) and so receives the DB password in the response, which the `mskconnect` client redacts before `Data` and never stores; it is written down here and on the registration card | the MSK config-provider route: a Secrets Manager provider jar bundled into the custom plugin archive plus `secretsmanager:GetSecretValue` on the execution role; a later U13 row, which removes the exception |
| D7 | Sandbox accounts and standing cost | one account per provider as §6; AWS MSK cluster plus RDS **kept** between runs with RDS stopped; keys through the owner's password manager to the leads only | tear the AWS cluster down after each run (adds about 30 minutes per manual run and a provisioning script) |
| D8 | Confluent read account | **the person creates** `piperoom-ro-<pipeline>` and its two keys from a card; the runner applies the ACLs only, so the setup key needs a role on one cluster | the runner creates it through IAM v2 (the setup key then needs an organisation-level role, against least privilege) |
| D9 | Aiven watcher Connect user | if Aiven Connect service users are all full-rights (unverified), **do not register one**: the watcher reads connector status through the `aiven` client with an application-user token of the **`read_only` project role** (`GET .../connectors/{name}/status`, verified path; `connect_kind: aiven` in `connections:`, one more value of §9 C1), or, if the owner prefers no second token, the Connect layer is marked `unreachable_layers: [kafka-connect]` and the card says so | accept a full-rights Connect service user for the watcher (the server's REST client only reads, but a leaked credential could create connectors); not recommended |

(D1–D4 and D10 are in the Strimzi design's §12, so one owner reply can answer both pages.)
