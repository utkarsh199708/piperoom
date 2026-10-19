# Setup on managed clouds: MSK Connect, Confluent Cloud, Aiven (U13-0; main @ `2681f65b`, 2026-10-19)

Owner: LEAD-ARCH. Parent design: `docs/design/setup-agent.md`; plan: `docs/product/setup-2026-10.md` (§4 row U13,
§7 items 1 and 6); board: `tasks/U-board.md` rows U13-0 to U13-R; the earlier MSK design `docs/design/msk.md`
(rev 4, read-only scope; its PR 1, the shared `aws.Config` and `AWS_MSK_IAM`, is merged in `internal/wiring/aws.go`
and `internal/clients/netsec`; its PR 2, `CONNECT_KIND=msk` and `internal/clients/mskconnect`, is **not** merged:
`internal/clients/` has no `mskconnect` and `config.go` has no `CONNECT_KIND`). The contracts are the merged U0
ones; conflicts are in §9. Vocabulary as in the parent: runner, ops, catalogue, tier floors T0–T3, classes,
validated or templated artifacts, the ladder, the guard, the `Credentials` frame.

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
| Where the runner runs | on a bastion or laptop that holds the cloud credentials and reaches the database; the server never holds a cloud credential; the plan's §2.2 |
| AWS auth | the SDK default chain through the shared `aws.Config` pattern (`wiring.LoadAWS`): SSO, a named profile, an assumed role, an instance or task role; **never a long-lived key stored by Piperoom** (the runner's secret store refuses `AWS_*` names) (§2.1) |
| Confluent auth | a **Cloud API key** of a service account for the Connect v1 and IAM v2 APIs (basic auth over https to `api.confluent.cloud`), a **cluster API key** of the same account for the Kafka REST v3 ACLs; the watcher gets a second service account with read scopes only (§2.2) |
| Aiven auth | an **application-user token** scoped to the one project (`Authorization: aivenv1 <token>`); the watcher uses the Connect service's own REST API with a read-only service user, not the Aiven API (§2.3) |
| Clients | three packages (C21): `internal/clients/mskconnect` over the SDK, `internal/clients/confluentcloud` and `internal/clients/aiven` over `net/http`; each implements `clients.ConnectClient` (status, config, plugins) so the Q tools and the watcher run unchanged, plus a `Setup*` write interface used only by the ops (§3) |
| Recorded fakes | `httptest` cassettes recorded on the sandbox with `//go:build manual`, scrubbed at record time (account ids, ARNs, keys, tokens, hostnames), replayed in `go test ./...`; a cassette with a credential-looking string fails the loader (§5) |
| Sandboxes | one account per provider held by the owner (plan §7 item 6), keys to the leads only, a `piperoom-sandbox` tag or label on every resource, torn down by a script after each manual run (§6) |
| Watching afterwards | MSK: the server's own IAM identity with the read policy of `msk.md` §4 (needs `CONNECT_KIND=msk`, §9 Q1); Confluent: a read-only service account's keys registered through the `Credentials` frame and a `CONNECT_KIND=confluent` client; Aiven: the Connect REST API over basic auth with a read-only service user (today's REST client) (§7) |

## 2. Authentication per provider

### 2.1 AWS MSK Connect: SigV4 over the shared AWS config

- The runner loads **one** `aws.Config` at start the way `wiring.LoadAWS` does
  (`awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))`, verified), with the region from the
  `SetupTarget` of kind `aws` (`Address: <account id>/<region>`, §9 Q2) or `AWS_REGION`, and fetches credentials
  once (5 s) so a broken chain stops pairing, not a step. Every SDK client is `NewFromConfig(cfg)`; SigV4 is the
  SDK's; the runner signs nothing itself.
- **Which chain.** In this order, the SDK's own: environment variables, the shared config and credentials files
  with `AWS_PROFILE` (an **SSO profile** after `aws sso login` is the recommended laptop path and is what the user
  page tells people to do), `AWS_ROLE_ARN` with `AWS_WEB_IDENTITY_TOKEN_FILE` (a CI or bastion with OIDC), the
  container credentials endpoint, IMDS (a bastion EC2 instance role). Cross-account: `stscreds.NewAssumeRoleProvider
  (stsClient, roleARN)` (verified) when the person lists a `role_arn` on the target; the runner never asks for an
  external id in the room.
- **Never long-lived keys in Piperoom.** The runner's secret store (`secret_set`) refuses the names `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` and any name starting `aws_` (a table test); the UI's secret field says
  so; a chain that resolves to static keys in the person's own `~/.aws/credentials` is theirs, outside the project
  directory and never read by any op (`file_read` is confined to the project dir). The server holds no AWS key for
  setup; for watching it uses its own identity (§7).
- **Identity and class.** `env_detect` calls `sts.GetCallerIdentity` (verified: `Account`, `Arn`, `UserId`) and
  reports the account id and the ARN's role or user name (never the full ARN of a user, which can carry a person's
  name, in `Summary`; it is in `Data`). The account id is what `PIPEROOM_SETUP_PROD_PATTERNS` matches for the
  `production` class, combined with the person's floor.
- Secrets in connector configs: MSK Connect has no secret provider of its own; the database password is a
  **materialisation point in the API call** (parent §2.4: "cloud API calls"), placed into
  `CreateConnectorInput.ConnectorConfiguration` on the runner from `${secret:}` references, and the `mskconnect`
  client redacts every key `actions.LooksSecret` matches in `DescribeConnectorOutput.ConnectorConfiguration` before
  it reaches `Data` (as the REST client's `connect_config` redaction does). A config-provider plugin
  (`SecretsManager`) bundled into the custom plugin is a U13 follow-up, not the first version (§9 Q3).

### 2.2 Confluent Cloud: API keys

Two kinds of key, two scopes, both **basic auth over https only** (`netsec`'s rule; the client refuses `http`):

- A **Cloud API key** (`username: <key>`, `password: <secret>`) authenticates to `https://api.confluent.cloud`:
  the Connect v1 API (plugins, validate, create, status, delete) and the IAM v2 API (service accounts, API keys).
  It belongs to a service account the person creates for the setup (`piperoom-setup-<pipeline>`) with the least
  role that can create a managed connector on **one** cluster and nothing on others. The role name is
  **unverified** (the Confluent RBAC catalogue has `CloudClusterAdmin`, `Operator`, `DeveloperManage` and others;
  which one suffices for `createConnectv1Connector` on one cluster is read from the sandbox in U13a and written into
  `docs/pilot` by U13f). The key is a secret the person types into the room's secret field (`${secret:ccloud_api_key}`,
  `${secret:ccloud_api_secret}`) or sets on the runner's terminal; it never reaches the server.
- A **cluster API key** (the same service account, scoped to the Kafka cluster) authenticates to the cluster's own
  REST endpoint for ACLs and, for the watcher, SASL_SSL PLAIN on the bootstrap: that is how Confluent's `kafka.auth.mode:
  KAFKA_API_KEY` works, and the parent design's `netsec` PLAIN-over-TLS path already serves it.
- **Least scopes for the watcher** (§7): a second service account `piperoom-ro-<pipeline>` with a cluster API key
  and ACLs `DESCRIBE` and `READ` on topics `<topic_prefix>` (prefixed), `DESCRIBE` on the connector's consumer
  group, `DESCRIBE_CONFIGS` on the topics, plus a Cloud API key whose role can read connector status only
  (unverified which; `Operator` or a viewer role is the hypothesis).

### 2.3 Aiven: tokens

- `aiven-client` sends `authorization: aivenv1 <token>` (verified) with `https://api.aiven.io/v1` as the prefix.
  Tokens are per user; an **application user** (organisation-level, `POST /v1/organization/{org}/application-users/
  {user}/access-tokens`, verified path) is the service-account equivalent and is what the setup uses, made a member
  of **one project** with the `operator` role (the Aiven project roles are `admin`, `operator`, `developer`,
  `read_only`; which of `operator` or `developer` can create a Kafka Connect service and connectors is
  **unverified** until U13a). The token has a `max_age_seconds` and an optional `description`; the person creates it
  with a 24-hour age for the setup and types it into the secret field (`${secret:aiven_token}`).
- **Read-only where possible**: the watcher does not use an Aiven token at all. An Aiven Kafka Connect service
  exposes the standard Connect REST API on `https://<service>-<project>.aivencloud.com:443` with the service's
  users (basic auth); the setup creates a Connect service user with read-only... **unverified**: Aiven Kafka Connect
  service users may all carry the admin ACL on the Connect REST API. If so, the watcher's Connect user is a
  full user, the card says so, and the Aiven API token of `read_only` project role is the alternative for status
  reads through `GET .../connectors/{name}/status` (verified path). Decided after U13a.
- The token is in a header, never a URL (U13d's "Done when"); the client refuses a token under 20 bytes.

## 3. Provider operations the setup needs

Per provider: what setup needs (plan §4 table), the call, verification status, and the op. Reads are tools
through the remote client (T0); writes are ops behind proposals (T2, T3 on `production`).

### 3.1 AWS MSK Connect (`internal/clients/mskconnect`, ops `msk_*`)

| Need | SDK call (package `service/...`) | Verified | Op |
|---|---|---|---|
| who am I, which account | `sts.GetCallerIdentity` → `Account`, `Arn` | yes | `env_detect` |
| the MSK cluster: state, version, auth | `kafka.DescribeClusterV2{ClusterArn}`; `kafka.GetBootstrapBrokers{ClusterArn}` → `BootstrapBrokerStringSaslIam` (IAM), `...SaslScram`, `...Tls` | yes (the output field list) | `msk_describe_cluster` [T0] |
| the cluster's network | `ec2.DescribeSubnets`, `ec2.DescribeSecurityGroups`, `ec2.DescribeRouteTables` for the cluster's subnets and the connector's intended subnets and groups | names yes; the "no route to the database" verdict is a rule over their output, tested on cassettes | `msk_vpc_check` [T0] |
| plugins | `kafkaconnect.ListCustomPlugins{NamePrefix, MaxResults, NextToken}`; `DescribeCustomPlugin` → `CustomPluginState` (`CREATING`, `ACTIVE`, ...), `LatestRevision`, `StateDescription` | yes | `msk_list_plugins` [T0] |
| the plugin archive the person uploaded | `s3.HeadObject{Bucket, Key}` (exists, size, `VersionId`) | yes | `msk_plugin_object_check` [T0]; the runner **never uploads** (no `s3:PutObject` in the policy): the person uploads the Debezium plugin zip whose SHA-512 the knowledge index pins, and the op checks the digest when `ChecksumSHA256` is present, else reports "size only" |
| the service execution role | `iam.GetRole{RoleName}`; `iam.SimulatePrincipalPolicy{PolicySourceArn, ActionNames}` for `kafka-cluster:Connect`, `kafka-cluster:DescribeCluster`, `logs:CreateLogStream`, `logs:PutLogEvents` | names yes; semantics of the simulation on MSK IAM actions unverified | `msk_role_check` [T0] |
| connector status | `kafkaconnect.DescribeConnector{ConnectorArn}` → `ConnectorState`, `StateDescription{Code, Message}`, `ConnectorConfiguration`, `LogDelivery`; `ListConnectors{ConnectorNamePrefix}` for name → ARN (`msk.md` §3a) | yes | `connect.status` through the remote client; the ladder's rung 1 |
| worker logs | `cloudwatchlogs.FilterLogEvents` on the delivered log group (`msk.md` §3e) | yes (msk.md) | `connect_worker_logs` (Q3c) over the remote client |
| create the custom plugin | `kafkaconnect.CreateCustomPlugin{Name, ContentType: ZIP | JAR, Location: {S3Location: {BucketArn, FileKey, ObjectVersion}}}` | yes | `msk_create_plugin` [T2]; async: the op returns the ARN and `CREATING`; `k8s_wait`'s sibling `msk_wait {kind, arn}` polls `DescribeCustomPlugin` to `ACTIVE` (deadline 10 min) |
| the worker configuration | `kafkaconnect.CreateWorkerConfiguration{Name, PropertiesFileContent}` | yes | `msk_create_worker_config` [T2]; the properties are rendered from the driver's template under the properties key allow-list (converters, `config.providers` absent in v1, `offset.storage.topic` family absent: MSK Connect manages them) |
| create the connector | `kafkaconnect.CreateConnector{ConnectorName, KafkaConnectVersion, Capacity{ProvisionedCapacity{McuCount: 1, WorkerCount: 1}}, KafkaCluster{ApacheKafkaCluster{BootstrapServers, Vpc{Subnets, SecurityGroups}}}, KafkaClusterClientAuthentication{AuthenticationType: IAM}, KafkaClusterEncryptionInTransit{EncryptionType: TLS}, Plugins: [{CustomPlugin{CustomPluginArn, Revision}}], ServiceExecutionRoleArn, WorkerConfiguration{WorkerConfigurationArn, Revision}, ConnectorConfiguration, LogDelivery{WorkerLogDelivery{CloudWatchLogs{Enabled: true, LogGroup}}}}` → `ConnectorArn`, `ConnectorState` | yes (every field name) | `msk_create_connector` [T2]; async (`CREATING` for 5–15 minutes); `msk_wait` polls to `RUNNING` or `FAILED` with `StateDescription` |
| delete (rollback) | `kafkaconnect.DeleteConnector{ConnectorArn, CurrentVersion}`; `DeleteCustomPlugin`, `DeleteWorkerConfiguration` | yes | `msk_delete_connector` [T2; T3 on production; receipt-created only]; plugin and worker config deletes are rollback-only and receipt-bound |
| the log group | `cloudwatchlogs.CreateLogGroup` | name unverified in this session (msk.md verified `FilterLogEvents` only) | `msk_create_log_group` [T2], optional: the person may name an existing group instead |

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
| the cluster, environment | Kafka clusters v2 `GET /cmk/v2/clusters/{id}` (bootstrap endpoint, REST endpoint) | path unverified | `ccloud_cluster` [T0] |
| service account and its keys for the watcher | IAM v2 `POST /iam/v2/service-accounts`, `POST /iam/v2/api-keys` (with `spec.resource` the cluster for a cluster key, none for a Cloud key) | paths unverified (named by Confluent's public reference; U13a records them) | `ccloud_create_read_account` [T2]: returns only secret names; the values go to the runner's store |
| ACLs for the connector's and the watcher's accounts | Kafka REST v3 on the cluster's REST endpoint: `POST /kafka/v3/clusters/{cluster_id}/acls` with the cluster API key | path unverified | `ccloud_acl_create` [T2], ACL table fixed per template (the Strimzi design's §6 tables, same rows) |

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
| the service | `GET /project/{project}/service/{service}` (state, plan, `service_uri`, components, users) | yes | `aiven_service_get` [T0] |
| plugins | `GET /project/{project}/service/{service}/available-connectors` | yes | `aiven_list_plugins` [T0] |
| connectors and status | `GET /project/{project}/service/{service}/connectors`; `GET .../connectors/{name}/status` | yes | `connect.list`, `connect.status` |
| create a connector | `POST /project/{project}/service/{service}/connectors` with the config | yes | `aiven_create_connector` [T2]; the Connect REST `config/validate` on the service's own REST endpoint is the validate step (Q4a over basic auth) |
| create a Connect service | `POST /project/{project}/service` (`service_type: kafka_connect`, `plan`, `cloud`, `service_name`) | yes | `aiven_create_service` [T2; T3 on production; the plan name from a small allow-list; the cloud region from the target] |
| integration (Kafka ↔ Connect) | `POST /project/{project}/integration` (`integration_type: kafka_connect`, `source_service`, `dest_service`) | yes | `aiven_integration_create` [T2] |
| delete (rollback) | `DELETE .../connectors/{name}`; `DELETE /project/{project}/service/{service}` | connector path implied by the client's `delete_kafka_connector` (unverified exactly); service delete unverified | `aiven_delete_connector` [T2]; a **service delete is never an op** (data: the guard's "deleting a cloud resource" row); the rollback card names the service to delete by hand |
| service users (the watcher's read user) | `POST /project/{project}/service/{service}/user` | unverified | `aiven_create_read_user` [T2] |

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
  request method, path, query with ids rewritten, request body with secrets rewritten, status, response headers
  except `Date`, `x-amzn-RequestId`, `Set-Cookie`, response body). For the AWS SDK the transport is set on
  `aws.Config.HTTPClient` (verified field); SigV4 headers (`Authorization`, `X-Amz-Date`, `X-Amz-Security-Token`)
  are dropped at record time and the replayer matches on method, path and a hash of the normalised body.
- **Scrub at record time, not replay time**: account ids (12 digits) → `000000000000`; ARNs' account and
  region → fixed; S3 bucket names, Confluent environment (`env-xxxxx`), cluster (`lkc-xxxxx`) and service-account
  ids, Aiven project and service names, every hostname under `amazonaws.com`, `confluent.cloud`, `aivencloud.com`
  → fixed placeholders; every value of a request key `actions.LooksSecret` matches and every `Authorization`
  header → `REDACTED`. The scrubber is a table in code with a test per row.
- **The loader test** (board U13a "Done when"): every cassette is scanned at test time with `redact.Text`'s patterns
  plus the AWS key shapes (`AKIA[0-9A-Z]{16}`, 40-byte secrets), Confluent key shapes and `aivenv1 `; a hit fails
  the test that loads it, so a cassette that slipped past the scrubber cannot merge.
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
| AWS | one account (or an OU member) with: an MSK Serverless or a small provisioned cluster with IAM auth in a VPC, an RDS Postgres in the same VPC (the U13-R source), an S3 bucket for plugin archives, a service execution role, a CloudWatch log group; an SSO permission set `PiperoomSetup` with the write policy of §8 for the leads, and a role `PiperoomWatch` with the read policy | the leads (SSO; no static keys are created) | `dev/sandbox/aws-teardown.sh` deletes connectors, plugins and worker configs tagged `piperoom-sandbox=true`; the cluster and RDS stay (cost is the owner's call; a stopped RDS is the default between runs) |
| Confluent Cloud | one organisation with one environment and one Basic cluster; a service account per role (`piperoom-setup`, `piperoom-ro`) with their keys | the leads | the script deletes connectors by name prefix `it_` and API keys the run created |
| Aiven | one project on the free or startup plan with a Kafka service; an application user with a project-scoped token | the leads | the script deletes connectors with prefix `it_` and the Connect service the run created when `--delete-service` is passed by hand |

Keys are given to the leads only, through the owner's password manager, never in the repo, CI secrets or Slack;
the manual rows (U13b1, U13c, U13d, U13-R) run on a lead's machine. CI never has a cloud credential: the harness of
§5 is CI's whole exposure.

## 7. How the server watches a managed pipeline afterwards

Parent §3.7: a managed target is `shared` or `production`, so the team server watches, with read-only credentials
that reach it only through the `Credentials` frame after the person's click and terminal "y".

| Provider | Kafka reads | Connect reads | What the `Credentials` frame carries | `connections:` written by `pipeline_register` |
|---|---|---|---|---|
| MSK | `AWS_MSK_IAM` with the **server's own identity** (IRSA, an instance role or the chain) over `wiring.LoadAWS`, reaching the cluster's IAM listener (9098) from inside the VPC or over VPC connectivity | `CONNECT_KIND=msk` (msk.md PR 2, **not merged**: §9 Q1) with `kafkaconnect:ListConnectors`, `DescribeConnector`, `logs:FilterLogEvents` | only the RDS read role's password (`source_create_read_role` on the runner) | `kafka_brokers: {value: <IAM bootstrap>}`, `kafka_security_protocol: {value: SASL_SSL}`, `kafka_sasl_mechanism: {value: AWS_MSK_IAM}`, `source_db_url: {file: ...}`; the registration card names the IAM role the server runs as and the policy it needs |
| Confluent Cloud | SASL_SSL PLAIN with the `piperoom-ro` cluster API key (ACLs of §2.2) | `CONNECT_KIND=confluent` (new, §9 Q1) with the `piperoom-ro` Cloud API key | `kafka_sasl_username`, `kafka_sasl_password` (the cluster key), `ccloud_api_key`, `ccloud_api_secret`, the DB read role's password | `kafka_brokers`, `kafka_security_protocol: {value: SASL_SSL}`, `kafka_sasl_mechanism: {value: PLAIN}`, the two `kafka_sasl_*` files, `connect_url: {value: https://api.confluent.cloud/connect/v1/environments/<env>/clusters/<lkc>}`, `connect_username`, `connect_password` (the Cloud key, `file:`) |
| Aiven | SASL_SSL SCRAM-SHA-256 or TLS client certs with a Kafka service user the setup created read-only via ACLs (`aiven_acl_create`, Aiven's Kafka ACL API, **unverified** path) | **today's REST client** against `https://<connect service>-<project>.aivencloud.com:443` with basic auth of a Connect service user (§2.3's caveat) | the Kafka user's password or cert and key, the Connect user's password, the DB read role's password | `kafka_brokers`, `kafka_security_protocol: {value: SASL_SSL}`, `kafka_sasl_mechanism: {value: SCRAM-SHA-256}`, `kafka_tls_ca_file` (the project CA, `GET /project/{project}/kms/ca`? unverified; the service's `connection_info` is the fallback), `connect_url`, `connect_username`, `connect_password` |

Key names in the frame match the `tenancy.md` §3.2 keys so `pipeline_register` can map one to one; the frame's
`Secret.Name` rule (`^[a-zA-Z_][a-zA-Z0-9_]*$`) holds for all of them. Every value is generated or created by the
runner's op (`CreateReadRole`, the read account or user ops) and returned as a name only; the person's own setup
credentials (the SSO session, the setup service account's keys, the Aiven token) are **never** registered: a test
asserts the `RegisterCredentials.Names` set the server may request excludes them.

Where the server cannot reach the Kafka listener (an MSK cluster with no VPC connectivity from the server's
network), the ladder's Kafka rungs still pass through the runner's remote client; `pipeline_register` then records
`watched_by: server` with `layers_unreachable: [kafka]` on the card (§9 Q4) so the watcher's Kafka checks are
skipped with a reason rather than alerting forever.

## 8. IAM policy for the setup identity (AWS)

Read set (the watcher's and the probes'): `msk.md` §4's policy plus `kafka:DescribeClusterV2`, `kafka:GetBootstrapBrokers`,
`kafkaconnect:ListCustomPlugins`, `kafkaconnect:DescribeCustomPlugin`, `kafkaconnect:ListWorkerConfigurations`,
`kafkaconnect:DescribeWorkerConfiguration`, `ec2:DescribeSubnets`, `ec2:DescribeSecurityGroups`,
`ec2:DescribeRouteTables`, `s3:GetObject` and `s3:GetObjectAttributes` on the plugin bucket, `iam:GetRole` and
`iam:SimulatePrincipalPolicy` on the service execution role, `sts:GetCallerIdentity`. Write set (setup only):
`kafkaconnect:CreateCustomPlugin`, `CreateWorkerConfiguration`, `CreateConnector`, `DeleteConnector`,
`DeleteCustomPlugin`, `DeleteWorkerConfiguration`, `TagResource`, `iam:PassRole` on the service execution role
(required by `CreateConnector`), `logs:CreateLogGroup` on `/piperoom/*`, `ec2:CreateNetworkInterface` and its
describe/delete pair (MSK Connect creates ENIs in the connector's subnets on the caller's behalf; **unverified**
which principal needs it: the sandbox run tells and U13f documents it). Resource ARNs are scoped to the one
cluster, bucket, role and log group prefix; no `*` action. The exact action names are the AWS IAM names and are
checked against the sandbox's CloudTrail after the first manual run (U13b1's "manual check dated in the PR").

## 9. Open questions / contract impact

1. **`CONNECT_KIND` for managed watchers.** Watching MSK Connect needs msk.md's PR 2 (`CONNECT_KIND=msk`,
   `internal/clients/mskconnect`, wiring in `buildA2Clients`), which never landed; Confluent Cloud needs a
   `CONNECT_KIND=confluent` of the same shape; Aiven needs nothing (REST). Proposal: `config.go` gains `ConnectKind
   string // CONNECT_KIND: rest (default) | msk | confluent` with the msk.md §5.2 variables (`MSK_CONNECT_*`) and
   `CONFLUENT_ENVIRONMENT_ID`, `CONFLUENT_CLUSTER_ID`, `CONFLUENT_API_KEY`, `CONFLUENT_API_SECRET` (secret), and the
   per-pipeline `connections:` keys of §7 (`connect_kind` literal, `ccloud_api_key`, `ccloud_api_secret`) in
   `tenancy.md` §3.2. The client packages are C21's (board); the wiring line is A4's, asked in thread. A leads PR
   carries the config and `tenancy.md` change before U13c lands.
2. **`SetupTarget.Address` for cloud accounts.** U0 says "an account id"; the runner also needs a region (AWS), an
   environment and cluster id (Confluent) and a project and service name (Aiven). Proposal, doc-comment only: kind
   `aws` → `<account id>/<region>[/<cluster arn>]`; kind `confluent` → `<environment id>/<cluster id>`; kind
   `aiven` → `<project>/<service>`. `PIPEROOM_SETUP_PROD_PATTERNS` matches the first segment.
3. **Secrets in MSK Connect configs.** v1 materialises the database password into `ConnectorConfiguration` (it is
   encrypted at rest by the service and readable by any principal with `DescribeConnector`). A config-provider jar
   bundled into the custom plugin would keep it in Secrets Manager; that needs the person's archive to include it
   and a `secretsmanager:GetSecretValue` grant on the execution role. Later row; the user page says what v1 does.
4. **Unreachable layers after registration.** §7's last paragraph needs a per-layer "unreachable from the server"
   mark so the watcher skips with a reason. Proposal: the owners `setup:` block gains `unreachable_layers: []`
   (U0 item 5, A3's loader, a leads PR for the schema line); until then the card text alone carries it.
5. **The Confluent and Aiven role names and the unverified paths** (§2.2, §2.3, §3.2, §3.3): settled by U13a's
   recording session and written into the knowledge index and `docs/pilot` (U13f) with the date; no row claims a
   role or a path this page marks unverified.
6. **Aiven service creation cost.** `aiven_create_service` creates a billable service on the person's project;
   it is T2 with the plan name in the preview and the monthly price the service API returns (`GET
   /project/{project}/service_types`, unverified) when available. Keep, or refuse creation and require an existing
   Connect service? Decided by the owner before U13d.
7. **MSK Connect networking.** The connector runs in the person's VPC subnets; the database must be reachable from
   them and from MSK Connect's ENIs. `msk_vpc_check` reads routes and groups but cannot prove reachability; a
   failed connector with a connection timeout is the signal, and the fix (a security-group rule) is a person's
   change, never an op. Noted; the scenario `setup/msk-vpc-no-route` (U13e) covers it.
