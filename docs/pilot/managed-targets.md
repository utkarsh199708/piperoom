# Managed targets: MSK Connect, Confluent Cloud and Aiven

For the engineer who will let `piperoom-setup` create a Debezium connector on a managed Kafka Connect service: AWS MSK
Connect, Confluent Cloud or Aiven. It says what each target needs from you first, which credentials the runner uses and
where you type them, the IAM policy (AWS), the API key scopes (Confluent) and the token scope (Aiven) the setup needs,
the read-only identity the Piperoom server watches with afterwards, what approvals each step needs, how to undo it, and
what is not done or not confirmed.

Checked against this branch at `e993f6b` (rows U13a to U13e). Every claim names the file and line it comes from
(`path:line`), or a section of a design document (`docs/design/setup-managed.md` is "the design" below). If a file
changes, update the claim and its citation in the same PR. Where the repository does not say something, this page says
so (section 9) instead of guessing. **Nothing here was run against a real cloud account**: every cassette the code is
tested with is hand-written (`"recorded": false`), and the manual sandbox checks of rows U13b1, U13c and U13d were not
run (section 9).

## Status on this release: written, not yet wired

| Piece | State | Where |
|---|---|---|
| The MSK ops (`msk_*`), the Confluent ops (`ccloud_*`), the Aiven ops (`aiven_*`) | code and unit tests over cassettes exist | `internal/setup/targets/managed/msk.go:81`; `internal/setup/targets/managed/confluent.go:71`; `internal/setup/targets/managed/aiven.go:55` |
| The three clients | code and cassette tests exist | `internal/clients/mskconnect/client.go:110`; `internal/clients/confluentcloud/client.go:138`; `internal/clients/aiven/client.go:119` |
| Registering the ops in the runner | **done**: a Welcome target of kind `aws`, `confluent` or `aiven` registers that provider's ops (no host target in the room). AWS: the runner's default chain, loaded once at pairing in the target's region with a 5 s credential fetch. Confluent and Aiven: `ccloud_api_key`/`ccloud_api_secret`, `ccloud_cluster_key`/`ccloud_cluster_secret` and `aiven_token` read from the runner's secret store at each call, typed on the runner's terminal with `secret_set`; the runner refuses those names (and `aws_*`) from the room's secret frame | `internal/setup/setupcmd/managed.go` (`registerManaged`); `internal/setup/runner/secretops.go` (`IsCloudSetupSecret`) |
| The server's action catalogue and read tools for the agent | done: every `msk_`, `ccloud_` and `aiven_` write is a proposable T2 step (no preview op: the runner registers none) and every read is a read tool; a connector a write names must be one of the pipeline's | `internal/setup/actions/specs_targets.go`, `internal/setup/tools/read_targets.go` |
| The verification ladder | `managed.Ladder` exists; `plan.Ladder` (what the server uses) refuses the managed targets by design, and only the evals' fake ladder calls `managed.Ladder` | `internal/setup/targets/managed/ladder.go:28`; `internal/setup/plan/ladder.go:39`; `evals/setup/ladder.go:252` |
| The server's watcher on a managed pipeline (`CONNECT_KIND=msk`, `confluent`, `aiven`) | **not wired**: the setting parses, nothing calls it | `internal/config/config.go:31` |

So today a room cannot propose a managed step. The rest of this page describes the ops as written.

## 1. The three targets and what you prepare

The room's target is one `events.SetupTarget` whose address names the account, not a host
(`internal/events/events.go:1687-1689`):

| Target | Kind and address | What you prepare first | Source |
|---|---|---|---|
| MSK Connect | `aws`, `<account id>/<region>[/<cluster arn>]` | an MSK cluster with its IAM listener on; the Debezium plugin archive (`.zip` or `.jar`) uploaded by you to an S3 bucket (the runner never uploads); a service execution role that trusts `kafkaconnect.amazonaws.com`; a CloudWatch log group, if you want worker logs | the design §3.1; `internal/setup/targets/managed/msk.go:719-722` (no IAM listener refuses the connector); `internal/clients/mskconnect/validate.go:24-26` (archive key shape) |
| Confluent Cloud | `confluent`, `<environment id>/<cluster id>` | a Kafka cluster; a service account for the setup with a Cloud API key and a cluster API key; the managed connector's own service account | the design §2.2; `internal/setup/targets/managed/confluent.go:78-81` |
| Aiven | `aiven`, `<project>/<service>` (the Kafka Connect service) | a Kafka service and a Kafka Connect service in one project; an application-user token | the design §2.3; `internal/setup/targets/managed/aiven.go:62-65` |

An op runs only in a room whose targets hold that address: an MSK op checks the `aws` target's account and region, a
Confluent or Aiven op its `<a>/<b>` address (`internal/setup/targets/managed/managed.go:163-174`;
`internal/setup/targets/managed/msk.go:201-206`; `internal/setup/targets/managed/confluent.go:164-169`).

## 2. Credentials: on the runner, never in the room

- **Cloud setup credentials are entered on the runner's terminal or in its environment, never through the room.** The
  server's secret route refuses the names `ccloud_*`, `aiven_token` and `aws_*`/`AWS_*` with "enter cloud credentials on
  the runner" (`docs/api.md:1602`; the design §2).
- **AWS: the SDK's default credential chain** (an SSO profile after `aws sso login`, a named profile, an assumed role,
  an instance role), loaded into one `aws.Config` with the target's region; Piperoom stores no AWS key (the design
  §2.1). The client needs a region and refuses a config without one (`internal/clients/mskconnect/client.go:110-113`).
  Before every MSK op the runner reads `sts.GetCallerIdentity` and refuses the op when the account is not the target's,
  so a profile pointed at another account cannot run there under this target's class
  (`internal/setup/targets/managed/msk.go:238-247`; test `internal/setup/targets/managed/msk_test.go:156`).
- **Confluent: HTTP basic auth over https only.** The key is the username, the secret the password, per the Connect v1
  OpenAPI's security scheme (`internal/clients/confluentcloud/client.go:20-22`). `netsec.HTTP` refuses a login over
  `http://` and URL userinfo, the client refuses any non-https base, and the auth transport sends the login to the API
  host only (`internal/clients/confluentcloud/client.go:158-199`; test
  `internal/clients/confluentcloud/client_test.go:53`). The ACL client takes only an https endpoint under
  `confluent.cloud` with no path (`internal/clients/confluentcloud/api.go:268-287`).
- **Aiven: the token in a header, never a URL.** The client sets `Authorization: aivenv1 <token>` on https requests to
  the API host and refuses any other request (`internal/clients/aiven/client.go:159-166`), as aiven-client's
  `_execute` sends it (`internal/clients/aiven/client.go:16-19`). A token shorter than 20 bytes or with white space is
  refused (`internal/clients/aiven/client.go:56`, `internal/clients/aiven/client.go:130-132`). Test:
  `internal/clients/aiven/client_test.go:56`.
- **A database password in a connector config is a `${secret:name}` reference.** A value under a key that names a
  credential (`actions.LooksSecret`) must be a whole reference, any other value must not carry one, and the runner
  materialises the reference only for the API call; Data and Summary carry the reference
  (`internal/setup/targets/managed/managed.go:188-232`; test `internal/setup/targets/managed/msk_test.go:169`).
- **What MSK returns:** `DescribeConnector` returns the materialised password in `ConnectorConfiguration`; the client
  passes it through `actions.RedactConfig` before any value leaves it (`internal/clients/mskconnect/read.go:355`), an
  accepted exception recorded in the design §10 D6.

## 3. AWS: the IAM policy

### The setup identity (your SSO role, for example `PiperoomSetup`)

Each op calls these AWS APIs. The IAM action is the API's name under its service prefix; the design lists the same set
(the design §8), with resource ARNs scoped to the one cluster, bucket, role and log group prefix.

| Op | APIs it calls | Source |
|---|---|---|
| every op | `sts:GetCallerIdentity` | `internal/setup/targets/managed/msk.go:238-247`; `internal/clients/mskconnect/read.go:156` |
| `msk_describe_cluster`, and `msk_create_connector` (for the IAM bootstrap brokers) | `kafka:DescribeClusterV2`, `kafka:GetBootstrapBrokers` | `internal/clients/mskconnect/read.go:186-226`; `internal/setup/targets/managed/msk.go:715` |
| `msk_list_plugins` | `kafkaconnect:ListCustomPlugins` | `internal/clients/mskconnect/read.go:229` |
| `msk_wait` (plugin) | `kafkaconnect:DescribeCustomPlugin` | `internal/clients/mskconnect/read.go:265` |
| `msk_describe_connector`, `msk_wait` (connector), `msk_delete_connector` | `kafkaconnect:ListConnectors`, `kafkaconnect:DescribeConnector` | `internal/clients/mskconnect/read.go:294-375` |
| `msk_vpc_check` | `ec2:DescribeSubnets`, `ec2:DescribeSecurityGroups`, `ec2:DescribeRouteTables` | `internal/clients/mskconnect/read.go:397-470` |
| `msk_role_check` | `iam:GetRole`, `iam:SimulatePrincipalPolicy` | `internal/clients/mskconnect/read.go:517-562` |
| `msk_plugin_object_check`, `msk_create_plugin` (its object check first) | `s3:GetObject` (HeadObject is authorised by it) | `internal/clients/mskconnect/read.go:566`; `internal/setup/targets/managed/msk.go:683-686` |
| `msk_create_plugin` | `kafkaconnect:CreateCustomPlugin`, `kafkaconnect:TagResource` (tags on create) | `internal/clients/mskconnect/write.go:108-135` |
| `msk_create_worker_config` | `kafkaconnect:CreateWorkerConfiguration`, `kafkaconnect:TagResource` | `internal/clients/mskconnect/write.go:138-163` |
| `msk_create_connector` | `kafkaconnect:CreateConnector`, `kafkaconnect:TagResource`, `iam:PassRole` on the service execution role, and `iam:CreateServiceLinkedRole` for `kafkaconnect.amazonaws.com` on an account's first connector | `internal/clients/mskconnect/write.go:213-251`; the design §8 |
| `msk_delete_connector` | `kafkaconnect:ListTagsForResource`, `kafkaconnect:DeleteConnector` | `internal/setup/targets/managed/msk.go:750-777` |

`kafkaconnect:ListTagsForResource` is **not** in the design's §8 list: `msk_delete_connector` reads the tags to refuse a
connector this room did not create (section 6), so add it to the read set. No `ec2:CreateNetworkInterface`: MSK Connect's
service-linked role creates the ENIs (the design §8). No `s3:PutObject`: the runner never uploads (the design §3.1). The
exact action names are to be checked against the sandbox's CloudTrail after the first manual run (the design §8; not
done, section 9).

### The service execution role (the connector runs as it)

`msk_role_check` reads the role and refuses unless its trust policy names `kafkaconnect.amazonaws.com` and IAM's
simulation allows `kafka-cluster:Connect`, `kafka-cluster:DescribeCluster`, `logs:CreateLogStream` and
`logs:PutLogEvents` (`internal/setup/targets/managed/msk.go:78`; `internal/clients/mskconnect/read.go:533-538`). The
simulation's semantics for the `kafka-cluster:*` actions are not confirmed (the design §3.1). The role also needs to read
the plugin archive when MSK builds the plugin (`internal/setup/targets/managed/msk.go:614`).

### The read-only identity the server watches with

The server's own identity (IRSA, an instance role or the chain) with `kafkaconnect:ListConnectors`,
`kafkaconnect:DescribeConnector` and `logs:FilterLogEvents`, plus the MSK read policy of `docs/design/msk.md` §4 (the
design §7). `DescribeConnector` returns the materialised database password to that identity (the design §10 D6); the
client redacts it before any Data (`internal/clients/mskconnect/read.go:355`). Not wired today (status table).

## 4. Confluent Cloud: API key scopes

| Key | Of which account | Used for | Source |
|---|---|---|---|
| Cloud API key | the setup service account (`piperoom-setup-<pipeline>`) | Connect v1 (plugins, validate, create, status, config) and the CMK v2 cluster read, on `https://api.confluent.cloud` | `internal/clients/confluentcloud/client.go:13-22`; the design §2.2 |
| cluster API key | the same setup account, scoped to the cluster | Kafka REST v3 `createKafkaAcls` on the cluster's own REST endpoint | `internal/clients/confluentcloud/api.go:291-296`; the design §2.2 |
| the connector's service account | the managed connector runs as it (`kafka.auth.mode: SERVICE_ACCOUNT`) | its topics; `ccloud_acl_create`'s connector template grants CREATE, WRITE and DESCRIBE on the topic prefix and DESCRIBE on the cluster | `internal/setup/targets/managed/confluent.go:271-280` |
| the watcher's read account (`piperoom-ro-<pipeline>`) | **you create it** from the room's card, with one cluster key and one Cloud key | the server's reads after setup; `ccloud_acl_create`'s watcher template grants DESCRIBE, READ and DESCRIBE_CONFIGS on the prefix and DESCRIBE on the connector's group | `internal/setup/targets/managed/confluent.go:261-270`; the design §2.2, §7, §10 D8 |

**Which RBAC role the setup's Cloud key needs is unverified**: the design names `CloudClusterAdmin`, `Operator` and
`DeveloperManage` as candidates and leaves it to the sandbox run (the design §2.2). The watcher's Cloud key role is
unverified too (`Operator` or a viewer role, the design §2.2). No op creates service accounts or keys (no IAM v2 call,
the design §3.2). ACL rows are limited to a closed set: ALLOW only, host `*`, a `User:sa-...` principal, no wildcard
resource (`internal/clients/confluentcloud/api.go:224-260`).

`ccloud_create_connector` validates the config against the managed plugin first and creates only when validation
reports no error (`internal/setup/targets/managed/confluent.go:347-372`); a validate result keeps the config names and
error messages, never the echoed values (`internal/clients/confluentcloud/api.go:52-90`). A connection-refused or
timeout trace in a status adds the hint that the database must be reachable from Confluent's egress, a change only you
make (`internal/setup/targets/managed/confluent.go:325-340`; the design §3.2).

## 5. Aiven: the token scope

- **An application-user token**, the service-account equivalent, made a member of **one project** with the least
  project role that can create connectors and integrations. Whether `operator` or `developer` suffices is unverified
  (the design §2.3).
- **Card rule, not a check:** create the token with `max_age_seconds` at most 86400 (`avn user access-token create
  --max-age-seconds 86400 --description piperoom-setup`); the runner cannot read a token's age (the design §2.3).
- **The watcher does not use the setup token.** The server reads the Connect service's own REST API with a service user,
  or, if Aiven Connect service users all carry full rights, a `read_only`-role token or no Connect reads at all (the
  design §2.3, §10 D9; not decided, not wired).
- The client never decodes a service's URIs, passwords or certificates: `Service` holds name, type, state, plan, cloud
  and user names only (`internal/clients/aiven/api.go:13-60`; test `internal/clients/aiven/client_test.go:29`).
- `aiven_create_connector` checks the connector class against the service's available connectors before creating, and
  `aiven_integration_create` checks that the source is a Kafka service (`internal/setup/targets/managed/aiven.go:201-253`).

## 6. Tiers, classes and the guard

| Ops | Floor | On a production target | Source |
|---|---|---|---|
| `msk_describe_cluster`, `msk_list_plugins`, `msk_describe_connector`, `msk_vpc_check`, `msk_role_check`, `msk_plugin_object_check`, `msk_wait` | T0 | T0 | `internal/setup/targets/managed/msk.go:124-143`; `internal/setup/targets/managed/msk.go:186-191` |
| `msk_create_plugin`, `msk_create_worker_config`, `msk_create_connector`, `msk_delete_connector` | T2 | T3 | `internal/setup/targets/managed/msk.go:144-162`; `internal/setup/targets/managed/msk.go:194-199` |
| `ccloud_list_plugins`, `ccloud_connector_status`, `ccloud_cluster` | T0 | T0 | `internal/setup/targets/managed/confluent.go:112-117` |
| `ccloud_create_connector`, `ccloud_acl_create` | T2 | T3 | `internal/setup/targets/managed/confluent.go:118-125`; `internal/setup/targets/managed/confluent.go:156-161` |
| `aiven_service_get`, `aiven_list_plugins`, `aiven_connector_status` | T0 | T0 | `internal/setup/targets/managed/aiven.go:78-83` |
| `aiven_create_connector`, `aiven_integration_create` | T2 | T3 | `internal/setup/targets/managed/aiven.go:84-89` |

- **The class.** A managed target is `shared`, or `production` when the AWS account id, the Confluent environment or
  cluster id, or the Aiven project matches `PIPEROOM_SETUP_PROD_PATTERNS`, or when you set its floor to production; a
  `local` floor cannot lower it (`internal/setup/targets/managed/managed.go:108-124`; `internal/config/config.go:343`;
  tests `internal/setup/targets/managed/msk_test.go:83`, `internal/setup/targets/managed/confluent_test.go:185`).
- **A T2 step** needs an approver of the parent design's §3.6 (an overlapping pipeline's owners, else a tenant admin;
  the driver in solo or standalone mode); **a T3 step** adds the typed confirm and the runner's terminal "y" (the
  design §4).
- **Deletes reach only what this room created.** The create ops tag what they create with `piperoom-room=<room id>` and
  `piperoom-created-by=piperoom-setup`; `msk_delete_connector` refuses a connector without this room's tag, on every
  class (`internal/setup/targets/managed/managed.go:66-68`; `internal/setup/targets/managed/msk.go:761-764`; test
  `internal/setup/targets/managed/msk_test.go:240`).
- **The worker configuration is templated.** `msk_create_worker_config` renders its properties from a template under a
  key allow-list (converters from a fixed class list, `schemas.enable`, the two offset flush intervals) and refuses
  `config.providers*`, the internal topic settings, `group.id`, `bootstrap.servers`, `plugin.path` and the REST keys by
  name (`internal/setup/targets/managed/worker.go:26-62`; test `internal/setup/targets/managed/msk_test.go:304`).
- **The connector's capacity is fixed** at 1 MCU and 1 worker with IAM authentication and TLS in transit; the bootstrap
  servers are the cluster's IAM listener, never a param (`internal/clients/mskconnect/write.go:213-251`;
  `internal/setup/targets/managed/msk.go:715-722`).
- **Throttling** is retried three times inside a call, then the step fails (`internal/clients/mskconnect/client.go:65`;
  `internal/clients/confluentcloud/client.go:64-67`; `internal/clients/aiven/client.go:58-62`).

## 7. How to undo

| Step | Undo | Source |
|---|---|---|
| `msk_create_connector` | `msk_delete_connector` (T2; T3 on production) for a connector this room created; its preview names it as the rollback | `internal/setup/targets/managed/msk.go:161`; `internal/setup/targets/managed/msk.go:801-812` |
| `msk_create_plugin`, `msk_create_worker_config` | **no op**: delete the custom plugin and the worker configuration by hand in the console (the design §3.1 keeps those deletes rollback-only and receipt-bound; not built) | the design §3.1 |
| `ccloud_create_connector`, `ccloud_acl_create` | **no op**: delete the connector and the ACLs by hand (Confluent connectors carry no tag the op could check) | section 8 |
| `aiven_create_connector`, `aiven_integration_create` | **no op**: delete them by hand; a service delete is never an op (the design §3.3) | the design §3.3 |

At room close, delete the setup key or token and its service account (the design §2).

## 8. Known limits

- **Not wired** (the status table). Treat this page as the contract of the ops, not as a feature you can use from a room.
- **`msk_wait` waits about 8 seconds per step**, not 15 minutes: the catalogue gives every op at most its 10-second
  default, so the op stops one poll plus 2 seconds before its deadline and returns `done: false` for the plan to issue it
  again (`internal/setup/targets/managed/msk.go:660-663`; `internal/setup/ops/op.go:472-478`).
- **No log group op**: name an existing CloudWatch log group, or none (the design §3.1's optional
  `msk_create_log_group` is not built).
- **`msk_vpc_check` reads only the connector's side**: route tables and security group egress. The database's own
  security group, a network ACL and DNS are not read, and a group rule that names another group does not count as
  allowing (`internal/setup/targets/managed/vpc.go:69`; `internal/setup/targets/managed/vpc.go:83-88`).
- **The plugin archive's digest is not verified**: the object check compares size only (S3 exposes no SHA-512;
  `internal/setup/targets/managed/msk.go:618`; the design §3.1).
- **Confluent and Aiven have no delete op** (section 7), and no Aiven service create, read user or ACL op (the design
  §3.3's additions are not built).
- **The ops' previews** have the design's `Op.Preview` signature, but the catalogue does not call previews yet
  (`internal/setup/targets/managed/msk.go:221-234`).

## 9. What this page could not cite or confirm

- **Any live run.** The cassettes are hand-written from the SDK's and the vendors' documented shapes
  (`internal/clients/mskconnect/client_test.go:20`; `internal/clients/confluentcloud/cases_test.go:31`;
  `internal/clients/aiven/cases_test.go:27`); the manual tests that record real ones exist and were not run
  (`internal/clients/mskconnect/manual_test.go`, `internal/clients/confluentcloud/manual_test.go`,
  `internal/clients/aiven/manual_test.go`).
- **IAM action names checked against CloudTrail** (the design §8): not done.
- **The Confluent RBAC role** of the setup key and of the watcher's key, and whether the Postgres CDC plugin accepts
  `kafka.auth.mode: SERVICE_ACCOUNT` (the Connect v1 OpenAPI's create example uses `kafka.api.key`/`kafka.api.secret`):
  unverified (the design §2.2, §3.2).
- **The Aiven project role** of the setup token, the shape of `available-connectors` and of the connector status, and
  which service setting governs the available connectors: unverified (`internal/clients/aiven/client.go:21-27`; the
  design §2.3).
- **Whether `iam:CreateServiceLinkedRole` is needed by the caller** on an account's first MSK connector: unverified (the
  design §8).
