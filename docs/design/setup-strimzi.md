# Setup on Kubernetes with Strimzi: design (U12-0; main @ `2681f65b`, 2026-10-19)

Owner: LEAD-ARCH. Parent design: `docs/design/setup-agent.md` (the runner, tiers T0–T3, `os.Root`, the
`Credentials` frame); plan: `docs/product/setup-2026-10.md` (§4 row U12, §7 item 1: Strimzi is last by the measured
order and this design starts early so pulling it forward costs no rework); board: `tasks/U-board.md` rows U12-0 to
U12-R. The contracts this page codes against are the merged U0 ones: `internal/setup/protocol/protocol.go`
(`Welcome`, `Hello`, `OpRequest`, `OpResult`, the `Refuse*` codes), `events.SetupTarget` (kind `kubernetes`,
address `<context>/<namespace>`), `events.SetupReceipt` (`Kind: "http"`) and the routes of `docs/api.md` "Setup
rooms". Where this page and merged code differ, the code wins. Conflicts with U0 or the board are in §10.

The vocabulary is the parent design's: a **runner** executes typed **ops** from a **catalogue**; an op has a **tier
floor** raised to the **class** of the resolved target; an executed artifact is **templated or validated**; the
**ladder** verifies; the **guard** refuses the destructive subset on production.

## 1. Decisions in one table

| Question (board U12-0) | Decision |
|---|---|
| CRD versions | `kafka.strimzi.io/v1` (Strimzi ≥ 0.49, stored version since 1.0.0) is the supported API; `v1beta2` is read for detection only and refused as a target (§2) |
| Where the runner runs | an in-cluster **Job** from the Helm chart (`setup.runner.enabled=true`), one per setup room, in the **target namespace**; a laptop runner with a kubeconfig is not a U12 target (§10 Q1) |
| RBAC | one `Role` and `RoleBinding` in that namespace, a `ServiceAccount` with `automountServiceAccountToken: true` only on the Job's pod; nothing cluster-scoped; `get`/`patch`/`delete` on Secrets and ConfigMaps by `resourceNames` only (§4) |
| Pairing code | a short-lived `Secret` the person creates with `kubectl create secret ... --from-literal` (never in a manifest or Helm value), mounted as a file, read once, **deleted by the runner** after `Welcome` (§3.1) |
| Local confirmation | the runner prints the `Welcome` to its log with a fingerprint; the person confirms by creating a second `Secret` whose value is that fingerprint; `--yes` stays refused off loopback, so a Job never self-confirms (§3.2) |
| `kubectl` vs the API | the **Kubernetes API over `net/http`** with the mounted ServiceAccount token, no `kubectl` in the image, no `client-go` module (§5) |
| Manifest allow-list | `k8s_apply` renders four kinds from templates; a field allow-list per kind; everything that touches the pod is refused (§6) |
| Tiers | reads T0; every write in a namespace is at least T2 (a namespace is a shared system); T3 when the context, namespace or cluster matches `PIPEROOM_SETUP_PROD_PATTERNS` or the person said so (§7) |
| Rollback | delete what this room created, by receipt, in reverse order; the built image stays in the registry and the rollback card says so (§8) |

## 2. Strimzi versions and how the runner detects them

**Facts** (Strimzi `CHANGELOG.md`, read 2026-10-19): the `v1` API was introduced in 0.49.0 (and `.spec` made
required in `v1`); 1.0.0 made `v1` the Cluster Operator's API and the stored version and **removed `v1beta2`** (and
`v1alpha1`/`v1beta2` of `KafkaTopic` and `KafkaUser`) from the CRDs; 0.23.0 had removed `v1alpha1` and `v1beta1`;
0.46.0 removed ZooKeeper-based clusters (KRaft only, `KafkaNodePool` required); 1.3.0 requires Kubernetes ≥ 1.32;
the newest entries are 1.4.0 (Kafka 4.2.x, 4.3.x). Release dates are not in the changelog; they are **unverified**
here and are not needed by the code.

**Supported:** Strimzi **1.0.0 and newer** on Kubernetes ≥ 1.30, every CR at `kafka.strimzi.io/v1`:
`Kafka`, `KafkaNodePool` (read only), `KafkaConnect`, `KafkaConnector`, `KafkaTopic`, `KafkaUser`. Strimzi 0.49.x
serves `v1` too and works, but is not in the test matrix. Strimzi ≤ 0.48 (`v1beta2` only) is detected and refused
with the reason "Strimzi 0.48 or older serves only v1beta2; upgrade the operator" and a link; the driver does not
carry two template sets, because the operator the plan expects an enterprise cluster to run is 1.x and a dual set
doubles every validator test for a shrinking population. `KafkaMirrorMaker2`, `KafkaBridge`, `KafkaRebalance` are
never read or written.

**Detection** (`env_detect` in the Job, T0, three API reads, all inside the namespace or on discovery endpoints every
authenticated principal may read):

1. `GET /apis/kafka.strimzi.io` → `APIGroup.versions[]`; `v1` must be present; `preferredVersion` is recorded.
   Only `v1beta2` → refused as above. No group → "Strimzi is not installed on this cluster".
2. `GET /apis/kafka.strimzi.io/v1/namespaces/<ns>/kafkas` → the one `Kafka` the person named (`Env.Targets` kind
   `kubernetes` carries `<context>/<namespace>`; the cluster name is a `SetupRequest` target of kind `kafka` with
   address `strimzi://<name>`, §10 Q2). Its `status.conditions[type=Ready]`, `status.observedGeneration`,
   `status.kafkaVersion`, `status.listeners[]` (`name`, `bootstrapServers`, `certificates`) and `spec.kafka.version`
   give the Kafka version, the bootstrap address and whether the operator watches this namespace: a `Kafka` whose
   `status.observedGeneration` is missing or behind `metadata.generation` for more than the detect timeout means
   the operator does not reconcile here, and the plan stops with that question. The runner cannot read the operator's
   own namespace (no cluster-scoped RBAC), so this is the only operator probe it has.
3. The operator version comes from the `Kafka` CR's `status` label values the operator writes? **No**: Strimzi does
   not write its version into `status`. The runner reads the label `app.kubernetes.io/version` the Helm chart sets
   on the operator pods only when the operator runs in the target namespace (common in a one-namespace install);
   otherwise the operator version is "unknown (≥ 1.0 by API)", and the known-issues index (Q1) matches on the Kafka
   version instead. Recorded on `setup.runner_attached.tools` as `strimzi: "1.3.0"` or `strimzi: ">=1.0 (api v1)"`.

The `Hello.Tools` map carries `kubernetes: "<serverVersion.gitVersion from GET /version>"` and the `strimzi` entry
above; `docker`, `kubectl` and the cloud CLIs are absent in the Job.

## 3. The in-cluster runner Job

The Helm chart (C1, U12c) renders, with `setup.runner.enabled=true` and `setup.runner.namespace=<ns>`:

- a `ServiceAccount` `piperoom-setup-<room>` in `<ns>`;
- the `Role` and `RoleBinding` of §4;
- a `Job` `piperoom-setup-<room>` in `<ns>`: the Piperoom image (it ships `piperoom-setup`, U1-22), one container,
  `args: ["--server", "<origin>", "--code-file", "/var/run/piperoom/pairing/code", "--confirm-secret",
  "piperoom-setup-<room>-confirm", "--project", "/work"]`, `restartPolicy: Never`, `backoffLimit: 0`,
  `activeDeadlineSeconds: 7200` (longer than the 45 min plan wall clock plus the ladder's waits, so the server's
  bound ends the setup, not the Job), `ttlSecondsAfterFinished: 3600`, `automountServiceAccountToken: true`,
  `runAsNonRoot`, read-only root filesystem, an `emptyDir` at `/work` (the project directory: templates render
  there, the op log `.piperoom/ops.jsonl` and the secret store `.piperoom/secrets.env` live there and die with the
  pod; the record of the setup is the room log and the receipts, as for any runner), no `hostPath`, no privileges,
  resources `100m/256Mi` requests. The pairing Secret is mounted at `/var/run/piperoom/pairing` as a projected
  volume with `optional: false`.

`<room>` is the room id without its `r_` prefix (the value `POST /api/setup` returned); the UI's **Set up** page for
the Strimzi target shows the three commands of §3.1 and §3.2 instead of the one `piperoom-setup --server --code`
line, and `command` in the `POST /api/setup` response carries them (§10 Q3).

### 3.1 How the pairing code reaches the Job

The code is never in a manifest, a Helm value or an environment variable (environment values show in `kubectl
describe` and in every pod inspection). The person runs, on their own machine, with their own `kubectl`:

```
kubectl -n <ns> create secret generic piperoom-setup-<room>-pairing --from-literal=code=<CODE>
helm upgrade --install piperoom-setup-<room> piperoom/piperoom -n <ns> --set setup.runner.enabled=true --set setup.runner.room=<room> ...
```

(`--from-literal` keeps the code out of files; `--from-file=code=/dev/stdin` is the alternative the page shows for
shells with history.) The runner reads `/var/run/piperoom/pairing/code` once, sends `Pair{Code}`, and **deletes the
Secret** (`DELETE .../secrets/piperoom-setup-<room>-pairing`, allowed by `resourceNames`) as soon as it has the
`Welcome`, before it waits for confirmation. A code is single-use on the server anyway (`protocol.CodeTTL`, 10
minutes); deleting the Secret removes the copy that would otherwise outlive the pairing in etcd and backups. A Job
that starts after the code expired reads `PairingExpired` from the server and exits 2 with the message in its log;
the person creates a new code from the room's **Pair again** button (the `replaced` outcome applies only to a runner
that was attached).

The Secret's name is fixed by the template so the `Role` can name it; a wrong name is a mount failure the Job's
events show, not a silent fallback.

### 3.2 How the local confirmation is given

`setup-agent.md` §2.1: the runner prints the server origin, the room title and the creator and waits for `y/N`;
`--yes` is refused unless the server is loopback. A Job has no terminal and the server is never loopback, so the
Job never self-confirms. The person's "y" is a second Kubernetes write with their own credentials:

1. After `Welcome` the runner writes to its log (and nowhere else) the block it would have printed on a terminal:
   the origin it dialed, `Welcome.Title`, `Welcome.PipelineID`, `Welcome.Tenant`, `Welcome.Creator`, the targets with
   their classes, and a **fingerprint**: the first 12 hex characters of SHA-256 over
   `origin \n room_id \n pipeline_id \n creator.id`. Then it polls `GET .../secrets/piperoom-setup-<room>-confirm`
   every 2 s for at most `protocol.CodeTTL`.
2. The person reads it: `kubectl -n <ns> logs job/piperoom-setup-<room>`; the **Set up** page shows the same
   fingerprint next to the room title so the two can be compared without trusting the log alone.
3. The person confirms: `kubectl -n <ns> create secret generic piperoom-setup-<room>-confirm
   --from-literal=fingerprint=<the 12 characters>`. The runner compares in constant time, sends `Hello`, and deletes
   that Secret too. A mismatch or the timeout is `PairingRefused` (the runner reports it, the server emits
   `setup.pairing {outcome: refused}`), and the Job exits 3.

Why a Secret and not an annotation on the Job or a ConfigMap: the confirmation must be something only a person with
write access to the namespace can produce (same bar as the code), and `create` on a named Secret is the narrowest
write the person's own kubeconfig already has; the runner needs only `get` and `delete` on two fixed names.
Why not `kubectl attach -i`: a Job's stdin is not reliable across restarts and proxies, and a confirmation that
depends on a live TTY cannot be audited; the Secret create is in the audit log of the API server.

Every later T3 op (§7) also needs a terminal "y" by the parent design. In a Job that "y" is the same mechanism with
a per-op name: the runner logs the op, its target and its `params_hash` prefix and waits for
`piperoom-setup-<room>-confirm-<step_index>` with `fingerprint=<params_hash prefix>`; without it in 10 minutes it
refuses with `RefuseNotConfirmed`. The Role grants `get` and `delete` on the name pattern by listing the names for
step indexes 0 to 11 (the plan's 12-step bound), since `resourceNames` has no globs.

### 3.3 Reach, secrets and the project directory in a Job

- The runner's `net.Dialer` `Control` hook (parent §2.6) applies unchanged; the API server address from
  `KUBERNETES_SERVICE_HOST` is added to the runner's own target registry as `shared` at start, and the Kafka
  bootstrap of the `Kafka` CR's internal listener is a target the server registered from the `SetupRequest`.
- The secret store is the `emptyDir`; `secret_set` through the UI route works as on a laptop. The database password
  reaches the connector only as a Kubernetes Secret (`k8s_secret_apply`, §6) referenced from the connector config
  with Strimzi's `KubernetesSecretConfigProvider` syntax `${secrets:<ns>/<secret>:<key>}`; the rendered
  `KafkaConnect` enables that provider and nothing else (`config.providers: secrets`,
  `config.providers.secrets.class: io.strimzi.kafka.KubernetesSecretConfigProvider`), and the operator binds the
  Connect pods a Role to read that one Secret, which the template also renders (`Role`/`RoleBinding` kind is allowed
  only through this template, §6).
- No op in the Job can read the pairing or confirm Secrets back (they are deleted), and `k8s_get` refuses `secrets`
  as a kind (§5); the only Secret read is the KafkaUser's credentials Secret in §9.

## 4. RBAC for exactly one namespace

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: piperoom-setup-<room>, namespace: <ns>}
rules:
- apiGroups: [kafka.strimzi.io]
  resources: [kafkas, kafkanodepools, kafkatopics]
  verbs: [get, list, watch]                       # detection, bootstrap, version, topic existence (ladder rung 3)
- apiGroups: [kafka.strimzi.io]
  resources: [kafkaconnects, kafkaconnectors, kafkausers]
  verbs: [get, list, watch, create, patch, delete] # create: the three CRs this room makes; patch: server-side apply
                                                   # (preview with dryRun=All, then the same request); delete: rollback,
                                                   # limited to receipt-created names by the guard, not by RBAC (no globs)
- apiGroups: [""]
  resources: [secrets]
  verbs: [create]                                  # the DB credentials Secret (k8s_secret_apply); create cannot be
                                                   # narrowed by name, so it is the one broad verb on secrets
- apiGroups: [""]
  resources: [secrets]
  resourceNames: [piperoom-setup-<room>-pairing, piperoom-setup-<room>-confirm,
                  piperoom-setup-<room>-confirm-0, ..., piperoom-setup-<room>-confirm-11,
                  <pipeline>-db-credentials, <pipeline>-piperoom-ro]
  verbs: [get, patch, delete]                      # pairing and confirm: read once, delete; db-credentials: SSA and
                                                   # rollback; piperoom-ro: the KafkaUser's generated password (§9)
- apiGroups: [""]
  resources: [configmaps]
  verbs: [create]
- apiGroups: [""]
  resources: [configmaps]
  resourceNames: [<pipeline>-connect-logging]
  verbs: [get, patch, delete]                      # the only ConfigMap template (log4j levels)
- apiGroups: [rbac.authorization.k8s.io]
  resources: [roles, rolebindings]
  verbs: [create]
- apiGroups: [rbac.authorization.k8s.io]
  resources: [roles, rolebindings]
  resourceNames: [<pipeline>-connect-secrets]
  verbs: [get, patch, delete]                      # the Role that lets the Connect pods read <pipeline>-db-credentials;
                                                   # a Role may grant only what the creator holds (escalation check),
                                                   # so this Role's rule is get on that one Secret by name
- apiGroups: [""]
  resources: [pods, pods/log, events]
  verbs: [get, list]                               # k8s_logs (Connect and build pods), k8s_events
- apiGroups: [events.k8s.io]
  resources: [events]
  verbs: [get, list]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: piperoom-setup-<room>, namespace: <ns>}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: piperoom-setup-<room>}
subjects: [{kind: ServiceAccount, name: piperoom-setup-<room>, namespace: <ns>}]
```

Why each line, and what is absent:

- No `ClusterRole`, no `namespaces`, `nodes`, `customresourcedefinitions`, no `*` anywhere; `helm template` with
  `setup.runner.enabled=true` renders nothing cluster-scoped (U12c's test asserts it).
- `list` on `kafkas`, `kafkanodepools`, `kafkatopics` is needed because the runner does not know names in advance;
  `list` on `secrets` is **never** granted (it would read every Secret in the namespace), which is why the names
  the runner must read are fixed by the template.
- `pods/exec`, `pods/portforward`, `pods/attach`: absent. The runner never runs a command in a pod.
- `deployments`, `strimzipodsets`, `services`, `ingresses`: absent. Strimzi owns the Connect pods; the runner reads
  them through `pods` and the CR status only. Exposing the Connect REST or a listener is the person's change (§9).
- `delete` on `kafkaconnects`/`kafkaconnectors`/`kafkausers` is broad by RBAC (no name globs for resources the room
  creates) and narrow by the guard: `k8s_delete` refuses a name without a `k8s_apply` receipt in this room's log
  (parent §3.5 "created by this room, by receipt") on every class, and is refused outright on `production` for
  `kafkaconnects` and `kafkausers` (§7). The escalation check of the API server also bounds the Role the runner
  creates for the Connect pods.
- The Job's ServiceAccount token is projected with `expirationSeconds: 3600` and the API server rotates it; the
  runner re-reads the file per request (the standard in-cluster rule), so a 2-hour Job keeps working.

## 5. `kubectl` or the Kubernetes API: the API, over `net/http`

**Decision: the runner talks to the API server directly** with the mounted ServiceAccount token and CA
(`/var/run/secrets/kubernetes.io/serviceaccount/{token,ca.crt,namespace}`, `KUBERNETES_SERVICE_HOST`,
`KUBERNETES_SERVICE_PORT`), through the runner's one `*http.Client` (10 s per request, `io.LimitReader` 4 MB,
status checked, bodies closed, the dial `Control` hook) and `encoding/json`. No `kubectl` binary in the image, no
`k8s.io/client-go` (it would add about forty modules for five verbs on nine resources; AGENTS.md §4 rule 10).

Why not `kubectl` argv: the Compose driver builds `docker compose` argv because Docker has no stable API that
`os/exec` can avoid and the CLI is what the person has; here the CLI is a second code path over the same REST API,
the image would have to ship and pin it, `kubectl apply` reads files the runner would first have to write to disk
(one more executed artifact to confine), and its output is text where the API returns structured status the
ladder's predicates need. Every op below is one or two HTTP requests whose receipt is `SetupReceipt{Kind: "http",
Target: "<method> <path>"}` with the response body's MAC; the `Argv` field stays empty. The U12a unit tests run
against an `httptest` API server that serves discovery, the CRs and `pods/log`, with table cases for 401, 403 (a
rule the Role does not grant), 404, 409 (a conflict on apply), 422 (the webhook or schema rejects a field), 429
(`Retry-After` honoured once) and a cut body.

Ops (names from the parent's §5 table, tier floor in brackets):

| Op | Requests | Notes |
|---|---|---|
| `env_detect` [T0] | §2 | also `GET /version` |
| `k8s_get {kind, name?}` [T0] | `GET` one or a list of `kafkas`, `kafkanodepools`, `kafkaconnects`, `kafkaconnectors`, `kafkatopics`, `kafkausers`, `pods` | `secrets`, `configmaps`, `events` and any other kind are `RefuseSchema`; the namespace is `Env.Targets`', never a param; `Data` is the object with `metadata.managedFields` dropped and every `stringData`/`data` field absent by kind |
| `k8s_events {involved?}` [T0] | `GET .../events?fieldSelector=involvedObject.name=<name>` | 100 newest, capped, redacted, injection-scanned on the server |
| `k8s_logs {pod, container?, tail?}` [T0] | `GET .../pods/<pod>/log?tailLines=200&container=` | only pods whose labels carry `strimzi.io/cluster=<connect name>` or `strimzi.io/kind=KafkaConnect` of this room's KafkaConnect, or the build pod `<name>-connect-build`; 200 lines, 64 KiB, newest last |
| `strimzi_build_status {name}` [T0] | `GET .../kafkaconnects/<name>` → `status.conditions`, `status.connectorPlugins[]`, the build pod's phase and, on failure, its log tail | Kaniko (Kubernetes) or Buildah (OpenShift) output is untrusted text |
| `k8s_apply {kind, template, params}` [T2] | preview: `PATCH .../<plural>/<name>?fieldManager=piperoom-setup&force=false&dryRun=All` with `Content-Type: application/apply-patch+yaml`; execute: the same without `dryRun` | server-side apply, so the preview is the API server's own validation (the CRD schema, Strimzi's webhook if installed) and the receipt's target is the one URL; `force=false` makes a field another manager owns a 409, shown in the preview |
| `k8s_secret_apply {name, keys}` [T2] | the same SSA path on `secrets` | values are `${secret:<name>}` references materialised on the runner into `stringData`; the preview shows key names only |
| `k8s_delete {kind, name}` [T2] | `DELETE .../<plural>/<name>` with `propagationPolicy=Foreground` | rollback only; receipt-created names only (§4) |
| `k8s_wait {kind, name, condition, timeout}` [T0] | `GET` polling every 5 s, or `watch=true` with `resourceVersion` | Ready, or a connector `status.connectorStatus.connector.state`; ends at the op deadline |

## 6. The manifest field allow-list

`k8s_apply` renders exactly these templates; a `kind` or `template` outside the table is `RefuseSchema`; a
rendered object is validated field by field in `Validate` (no I/O) against the allow-list before the dry run, and
a field outside it is refused even when the template could not have produced it (the test renders each template
with every param and asserts the allow-list is complete; a second test feeds hand-made objects with each refused
field and asserts the refusal names it).

| Kind (`kafka.strimzi.io/v1` unless noted) | Template | Fields the runner may set | Always refused |
|---|---|---|---|
| `KafkaConnect` | `connect` | `metadata.name` (`<pipeline>-connect`), `metadata.annotations["strimzi.io/use-connector-resources"]="true"` (fixed), `spec.replicas` (1–3), `spec.version` (equal to the Kafka CR's `status.kafkaVersion` major.minor, or absent), `spec.bootstrapServers` (from the Kafka CR's listener status only), `spec.tls.trustedCertificates[0]` (`{secretName: <kafka>-cluster-ca-cert, pattern: "*.crt"}`), `spec.authentication` (`type: tls` with `certificateAndKey` from `<pipeline>-connect` KafkaUser's Secret, or `type: scram-sha-512` with `passwordSecret` from it), `spec.config` under a key allow-list (`group.id`, `offset.storage.topic`, `config.storage.topic`, `status.storage.topic`, the three `*.replication.factor` (−1 or 1–3), `key.converter`, `value.converter`, `*.converter.schemas.enable`, `*.converter.schema.registry.url` (a registered target), `config.providers: secrets`, `config.providers.secrets.class: io.strimzi.kafka.KubernetesSecretConfigProvider`), `spec.build.output` (`type: docker`, `image: <registry>/<repo>:piperoom-<pipeline>-<plugins digest>`, `pushSecret`; the registry host must be an `Env.Targets` entry of kind `registry`, `§10 Q4`), `spec.build.plugins[]` (name and artifacts **only from the knowledge index**: `type: maven` with `group`, `artifact`, `version`, or `type: tgz`/`jar` with `url` on `PIPEROOM_SETUP_PLUGIN_HOSTS` and the index's `sha512sum`), `spec.resources` (requests and limits within `100m–2` CPU, `512Mi–4Gi` memory), `spec.logging` (`type: inline`, loggers from a level allow-list, or `type: external` naming `<pipeline>-connect-logging`), `spec.metricsConfig` (the template's JMX exporter ConfigMap only) | `spec.image` (the operator picks the image for `spec.version`; a user image means an unvalidated artifact), `spec.template` entirely (pod, container, `securityContext`, `hostPath`, `hostNetwork`, `serviceAccountName`, `initContainers`, `volumes`, `env`, `tolerations`, `affinity`), `spec.externalConfiguration`, `spec.jvmOptions` other than `-Xms`/`-Xmx` within the resource limits, `spec.rack`, `spec.clientRackInitImage`, `spec.build.output.type: imagestream` (OpenShift, not in the matrix), `spec.build.resources`, any `config.providers.*.class` but the one above, any `config` key with `ssl.`, `sasl.`, `plugin.path`, `rest.`, `listeners`, `admin.`, `producer.`/`consumer.` prefixes, any `metadata.labels`/`annotations` beyond the one fixed annotation |
| `KafkaConnector` | `connector` | `metadata.name` (a connector name of the provisional pipeline), `metadata.labels["strimzi.io/cluster"]` (`<pipeline>-connect`), `spec.class` (an `io.debezium.connector.*` class the knowledge index lists for the plugin), `spec.tasksMax` (1 for a Debezium source), `spec.config` (the source driver's `ConnectorConfig` after Q4a's validate, where every secret is `${secrets:<ns>/<pipeline>-db-credentials:<key>}`; `setup_config_help` keys for SMTs and sinks, U3), `spec.autoRestart` (`enabled: true, maxRestarts: 3`) | `spec.state`/`spec.pause` (lifecycle is a typed op through the Q5c actions over the remote Connect client, not a manifest edit), `spec.listOffsets`/`alterOffsets`, any config key that `actions.LooksSecret` matches with a literal value, `connector.class` in `config` (it is `spec.class`), `database.hostname` not in `Env.Targets` |
| `KafkaUser` | `connect-user`, `read-user` | `metadata.name` (`<pipeline>-connect`, `<pipeline>-piperoom-ro`), `metadata.labels["strimzi.io/cluster"]`, `spec.authentication.type` (`tls` or `scram-sha-512`, whichever the Kafka listener uses), `spec.authorization` (`type: simple`, ACLs from a fixed table per template: the Connect user gets Create/Describe/Read/Write on `<topic_prefix>` prefixed topics and the three internal topics, Read/Describe on its group, Describe on the cluster; the read user gets Describe/Read on `<topic_prefix>*` and `DescribeConfigs`, Describe on `<pipeline>-connect` group, Describe on the cluster), `spec.quotas` (absent) | `spec.authorization.acls[]` outside the table, `type: tls-external`, `spec.template` |
| `KafkaTopic` | `canary-topic` | `metadata.name`, `metadata.labels["strimzi.io/cluster"]`, `spec.topicName` (`<topic_prefix>.<schema>.<canary table>`), `spec.partitions` (1), `spec.replicas` (≤ the broker count), `spec.config` (`cleanup.policy`, `retention.ms` only) | anything else; the op exists only because Strimzi clusters often run with `auto.create.topics.enable=false` and the Topic Operator owns topics; when auto-create is on the plan skips it |
| `Secret` (core/v1) | `db-credentials` | `metadata.name` (`<pipeline>-db-credentials`), `type: Opaque`, `stringData` keys from the source driver's `ConnectorConfig` secret keys, values materialised from `${secret:}` | any other `type`, `data`, labels, annotations, ownerReferences |
| `ConfigMap` (core/v1) | `connect-logging` | `metadata.name`, `data["log4j.properties"]` rendered from a level map (`io.debezium`, `org.apache.kafka.connect`, root) | any other key or free content |
| `Role`, `RoleBinding` (rbac) | `connect-secrets` | `metadata.name` (`<pipeline>-connect-secrets`), one rule `get` on `secrets` with `resourceNames: [<pipeline>-db-credentials]`, the binding to the Connect pods' ServiceAccount `<pipeline>-connect-connect` | any other verb, resource, name or subject |

What is refused is tested per field (board U12a) and the refusal text names the field path. A template change is a
PR to `internal/setup/targets/strimzi` and its allow-list test, never a parameter.

## 7. Tiers and the guard per op

Classes: a Kubernetes target is **never `local`** (a Job runs in a cluster other people share; the parent's
`local` rule covers loopback and a local engine only), so every write floor is T2. `production` when the
`SetupRequest` target class says so, or `PIPEROOM_SETUP_PROD_PATTERNS` matches the context name, the namespace
(`<context>/<namespace>` is matched as one string) or the Kafka cluster name.

| Op | Floor | On `production` |
|---|---|---|
| `env_detect`, `k8s_get`, `k8s_events`, `k8s_logs`, `strimzi_build_status`, `k8s_wait` | T0 | T0 (reads need no confirm; logs are capped and scanned) |
| `k8s_secret_apply`, `k8s_apply` (`Secret`, `ConfigMap`, `Role`/`RoleBinding`, `KafkaTopic`, `KafkaUser`) | T2 | T3: approval, typed confirm `<pipeline>/kafka`, the per-op confirm Secret of §3.2 |
| `k8s_apply` `KafkaConnect` | T2 | T3; a `KafkaConnect` whose name exists and is not receipt-created is refused on every class (`RefuseGuard`: adopting someone's Connect cluster is never a setup step) |
| `k8s_apply` `KafkaConnector` | T2 | T3 |
| `k8s_delete` `KafkaConnector`, `KafkaTopic`, `Secret`, `ConfigMap`, `Role`/`RoleBinding` | T2, receipt-created only | T3 |
| `k8s_delete` `KafkaConnect`, `KafkaUser` | T2, receipt-created only | **refused** (guard row: a Connect cluster or a user on production is removed by a person) |
| anything on `Kafka`, `KafkaNodePool`, namespaces, the operator | no op exists | – |

The guard rows join the parent's table (plan §3.6) with a test per row at the four check points. Rebuilding the
Connect image (a `KafkaConnect` apply whose `build.plugins` changed) restarts the Connect pods: on `production` it
needs the maintenance window like a database restart (`setup.maintenance_window`), because every connector on that
Connect cluster stops during the roll; the preview says so with the pod count.

## 8. Failure and rollback

Failure signatures the driver registers with the plan package (each a `known-issues` entry and an eval scenario,
U12d):

| Step | How it fails | What the loop sees | Fix the plan may propose |
|---|---|---|---|
| detect | operator not watching, `v1beta2` only, Kafka not Ready | `env_detect` result | stop and ask (`setup.blocked {reason: refused}`) or wait for Ready (`k8s_wait`) |
| build | `pushSecret` missing or denied, artifact `sha512sum` mismatch, Maven artifact unresolved, Kaniko OOM | `KafkaConnect` condition `NotReady` with `reason: ...Build...`, the build pod log tail | correct the registry target (a driver click), the plugin version (knowledge index), the build resources (within bounds); never a raw artifact URL |
| connect | pods `CrashLoopBackOff`, TLS trust or SCRAM failure, group id in use | `k8s_logs` of the Connect pod, `k8s_events` | authentication type matching the listener, a fresh `group.id`, the Connect user's ACLs |
| connector | `KafkaConnector` `NotReady` (`ConnectRestException`), task `FAILED` with a trace | `status.connectorStatus`, `status.conditions[].message` (untrusted text, wrapped) | the Q4a validate verdict, the source prerequisites (publication, `wal_level`, grants: the Postgres driver's checks over the remote client) |
| ladder | no canary event: topic auto-create off, ACL denies Write, snapshot not finished | rungs 3–5 | `KafkaTopic` template, the Connect user's ACL table, wait |

Bounds are the parent's (3 attempts per step, 12 steps, 45 min, token budget); a Strimzi build takes 2–8 minutes,
so `k8s_wait` on the build has a 10-minute op deadline and counts as one step.

Rollback (`PreviewResult.Rollback` per step, run in reverse by "roll back this setup", each step through the guard
again): `KafkaConnector` → `k8s_delete`; `KafkaTopic` → `k8s_delete` (the topic is deleted by the Topic Operator;
on `production` the guard refuses it because it is data, so the card says "topic kept"); `KafkaConnect` →
`k8s_delete` (the built image stays in the registry; the card names the tag and that the person deletes it); `Role`,
`RoleBinding`, `Secret`, `ConfigMap`, `KafkaUser` → `k8s_delete`. The source-side steps (publication, canary table,
read role) roll back through their own ops as on Compose. A rollback never touches the `Kafka` CR. The Job then
exits 0 and the room closes `rolled back`.

## 9. Verification and watching afterwards

**Ladder rung 1 source** is the `KafkaConnector` CR: `status.conditions[type=Ready].status == "True"` and
`status.connectorStatus.connector.state == "RUNNING"`; rung 2 is `status.connectorStatus.tasks[].state`; rung 3
is the `KafkaTopic` (when the Topic Operator runs) or the remote Kafka client's topic list; rungs 4–7 run as on
Compose through the remote clients, from inside the cluster: the Job reaches the internal listener and the
`<pipeline>-connect-api:8083` Service, which a team server cannot. `Env.Targets` therefore carries both the internal
bootstrap (the runner's) and, when the person listed one, an external listener (the server's); the ladder runner
issues each rung's read through whichever client can reach (parent §3.4 step 2).

**Watching** (parent §3.7, class `shared` or `production`): the team server needs three things it does not get
for free on Strimzi:

1. **Kafka**: a listener reachable from the server (`type: route`, `loadbalancer`, `nodeport` or `ingress` on the
   `Kafka` CR), which only the cluster's owner can add (the runner has no op on `Kafka`). The setup creates the
   `KafkaUser` `<pipeline>-piperoom-ro` (the read ACL table of §6); the User Operator writes its password or
   certificate into the Secret of the same name; the runner reads that Secret (the one `get` by name in §4), stores
   the values in its own store under `kafka_sasl_password` or `kafka_tls_cert_file`/`kafka_tls_key_file` plus the
   cluster CA, and `CreateReadRole`'s contract holds: only the names come back. **Register read credentials** then
   sends them in the `Credentials` frame after the §3.2 confirm Secret (`piperoom-setup-<room>-confirm-creds`,
   one more fixed name), into `PIPEROOM_SECRETS_DIR/<pipeline>/`, and `pipeline_register` writes
   `connections: {kafka_brokers: {value: <external bootstrap>}, kafka_security_protocol: {value: SASL_SSL | SSL},
   kafka_sasl_mechanism: {value: SCRAM-SHA-512}, kafka_sasl_username: {value: <pipeline>-piperoom-ro},
   kafka_sasl_password: {file: ...}, kafka_tls_ca_file: {file: ...}}` (the `tenancy.md` §3.2 keys).
2. **Connect**: the Connect REST API is a ClusterIP Service. Options: the person exposes it with an Ingress and basic
   auth and registers it as a target (`connect_url`, `connect_username`, `connect_password` in `connections:`), the
   U12 path; or a later `CONNECT_KIND=strimzi` client on the server that reads `KafkaConnector` status over the
   Kubernetes API with a read-only ServiceAccount token the person issues (§10 Q5). The registration card names
   which one is in place; with neither, the pipeline registers with `watched_by: local` semantics for the Connect
   layer and the card says the server sees Kafka only.
3. **Registry**: `schema_registry_url` when the person listed one; Strimzi ships none.

## 10. Tests and sandbox

- **Unit (no cluster)**: the allow-list per field (§6), the ACL tables, the `argv`-free rule (no `os/exec` import in
  the package, an AST test like `wiring/readonly_ast_test.go`), the API client against `httptest` (discovery, SSA
  dry run, the status codes of §5, token re-read, the 4 MB cap), the fingerprint and the confirm-Secret flow with a
  fake API server and a fake Piperoom server, `k8s_get` refusing `secrets`, `k8s_logs` refusing a pod outside the
  room's Connect cluster, `k8s_delete` refusing a name without a receipt.
- **Integration (nightly, `kind`, `ubuntu-latest`, 20-minute budget, U12c)**: `kind` with a local registry
  (`registry:2` on `localhost:5001`, the kind docs' pattern) so the Strimzi build can push; the Strimzi operator
  at the pinned version in `dev/strimzi/VERSION` (`kubectl create -f https://strimzi.io/install/latest?namespace=
  <ns>` pinned by the `strimzi-cluster-operator-<version>.yaml` asset instead, so the run is reproducible); a
  one-broker KRaft `Kafka` with a `KafkaNodePool` and a `scram-sha-512` internal listener; the Helm chart's Job;
  the recorded-model agent from an empty namespace to `pipeline live` with the nonce canary. A second leg applies
  each refused field and asserts the refusal before any request; a third breaks the build (wrong `sha512sum`) and
  asserts the `setup.blocked` question names the artifact.
- **Real run (U12-R)**: the `kind` leg's export, and one run on a real cluster the owner names (manual, dated): the
  plan's §7 item 6 lists no Kubernetes sandbox; `kind` on the owner's machine or a small managed cluster the owner
  provides is the open item (Q6).

## 11. Open questions / contract impact

1. **Laptop runner with a kubeconfig.** Out of U12: parsing kubeconfig without `client-go` means re-implementing
   `exec` credential plugins (EKS, GKE, AKS all use them). If a pilot needs it, the choice is `client-go` as a new
   module (listed with why) or `kubectl` argv for that mode only; both are a new design row.
2. **`SetupTarget` for the cluster name.** U0's `SetupTarget.Address` is "a kubectl context and namespace" for
   kind `kubernetes`; the Strimzi `Kafka` CR name has no field. Proposal: kind `kafka` with address
   `strimzi://<name>` (no port, resolved by the runner from the CR's listener status) — a doc-comment change on
   `events.SetupTarget` in a leads PR, no type change.
3. **`POST /api/setup` `command` for the Strimzi target.** `docs/api.md` describes `command` as the one
   `piperoom-setup --server --code` line; for `target: strimzi` it is the three commands of §3.1–§3.2 (create the
   pairing Secret, `helm upgrade --install`, and later the confirm Secret). Proposal: `command` stays one string
   (lines joined by `\n`), and the response gains `fingerprint` (§3.2) for setup rooms whose target is `strimzi`;
   `docs/api.md` row change in a leads PR.
4. **Registry as a target kind.** `SetupTarget.Kind` lists no `registry`; the build output image must be bound to a
   registered host. Proposal: add `registry` to the kind list in the `SetupTarget` doc comment (U0, leads PR) and to
   the targets route's accepted kinds (A5, U1-5b follow-up).
5. **Watching Connect on Strimzi.** §9 item 2: a `CONNECT_KIND=strimzi` client (status from the `KafkaConnector` CR
   over the Kubernetes API with a read-only token) would make the server's Connect layer work without an Ingress. It
   is a new config value and a new client package; decided after the first real run shows whether pilots expose
   Connect.
6. **A Kubernetes sandbox.** Plan §7 item 6 names cloud accounts and databases but no cluster; U12-R's real run
   needs one. Ask the owner: a `kind` run on the dev machine counts for Linux, but "a real cluster" means a managed
   one with a registry the build can push to.
7. **Runner image and Job ownership.** The Job, Role and ServiceAccount are chart templates (C1, U12c) rendered
   from the pipeline id and room id; the names in §4's `resourceNames` are fixed by them. The runner (A9) and the
   Strimzi driver (C20) read those names from flags, never derive them, so a rename is a chart change only.
8. **Strimzi webhook.** Strimzi has no admission webhook today; the SSA dry run validates against the CRD schema
   only, so a value the operator rejects at reconcile (an unknown `config` key is passed through to Connect, which
   ignores it) shows up as a `NotReady` condition, not in the preview. The key allow-list of §6 is what keeps the
   preview honest. Noted, no action.
