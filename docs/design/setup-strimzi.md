# Setup on Kubernetes with Strimzi: design (U12-0; main @ `2681f65b`, 2026-10-19; rev 2 after the #660 reviews)

Owner: LEAD-ARCH. Parent design: `docs/design/setup-agent.md` (the runner, tiers T0–T3, `os.Root`, the
`Credentials` frame); plan: `docs/product/setup-2026-10.md` (§4 row U12, §7 item 1: Strimzi is last by the measured
order and this design starts early so pulling it forward costs no rework); board: `tasks/U-board.md` rows U12-0 to
U12-R. The contracts this page codes against are the merged U0 ones: `internal/setup/protocol/protocol.go`
(`Welcome`, `Hello`, `OpRequest`, `OpResult`, the `Refuse*` codes), `events.SetupTarget`, `events.SetupReceipt`
(`Kind: "http"`) and the routes of `docs/api.md` "Setup rooms". Where this page and merged code differ, the code
wins.

**Conflicts with U0, the board and the parent design are listed in §11 as proposed amendments.** The contract part
of this row (parent §4 item 7) lands in **one separate leads PR, "U12-0/U13-0 contracts"**, merged before U12a, U13a
and U13c start; the board marks U12-0 done only when that PR merges. §12 lists the decisions the owner makes.

**Rev 2** answers the two reviews of #660: no `create` on any core or RBAC resource, the chart pre-creates what the
runner patches (S1, §3, §4); every runner-read Secret is in `resourceNames` (S2); the fingerprint hashes the
server's public origin and a server nonce (S3, S4, §3.2); a pre-existing confirm Secret is refused (S4); the
conflicts with board U12a and parent §2.5/§5 are listed (A1, §11); `image_registry` instead of `registry` (A3).

The vocabulary is the parent design's: a **runner** executes typed **ops** from a **catalogue**; an op has a **tier
floor** raised to the **class** of the resolved target; an executed artifact is **templated or validated**; the
**ladder** verifies; the **guard** refuses the destructive subset on production.

## 1. Decisions in one table

| Question (board U12-0) | Decision |
|---|---|
| CRD versions | `kafka.strimzi.io/v1` (Strimzi ≥ 0.49, stored version since 1.0.0) is the supported API; `v1beta2` is read for detection only and refused as a target (§2) |
| Where the runner runs | an in-cluster **Job** from the Helm chart (`setup.runner.enabled=true`), one per setup room, in the **target namespace**; a laptop runner with a kubeconfig is not a U12 target (§12 D2) |
| RBAC | one `Role` and `RoleBinding` in that namespace; **no `create` on any core or RBAC resource**; `get`/`patch`/`delete` on Secrets, ConfigMaps and the room's own CRs by `resourceNames`; `create` only on the four Strimzi kinds the room makes; `escalate`, `bind`, `impersonate` never granted; nothing cluster-scoped (§4) |
| Pairing code | a short-lived `Secret` the person creates with `kubectl create secret ... --from-literal` (never in a manifest or Helm value), mounted as a file, read once, **deleted by the runner** after `Welcome` (§3.1) |
| Local confirmation | the runner logs the `Welcome` with a fingerprint over the server's public origin, the room and a **server nonce**; the person confirms by creating a second `Secret` carrying that fingerprint; a confirm Secret that exists before the `Welcome` is refused; `--yes` stays refused off loopback, and RBAC (no secret create) makes self-confirmation impossible (§3.2) |
| `kubectl` vs the API | the **Kubernetes API over `net/http`** with the mounted ServiceAccount token, no `kubectl` in the image, no `client-go` module (§5; a conflict with board U12a, §11) |
| Manifest allow-list | `k8s_apply` renders six kinds from templates (two of them patches of chart-created objects); a field allow-list per kind; everything that touches the pod is refused (§6) |
| Tiers | reads T0; every write in a namespace is at least T2 (a namespace is a shared system); T3 when the context, namespace or cluster matches `PIPEROOM_SETUP_PROD_PATTERNS` or the person said so (§7) |
| Rollback | delete what this room created, by receipt, in reverse order; the built image stays in the registry and the card says so; the close card gives `helm uninstall` for the Job's SA, Role and RoleBinding (§8) |

## 2. Strimzi versions and how the runner detects them

**Facts** (Strimzi `CHANGELOG.md` and `api/.../KafkaStatus.java`, read 2026-10-19): the `v1` API was introduced in
0.49.0 (and `.spec` made required in `v1`); 1.0.0 made `v1` the Cluster Operator's API and the stored version and
**removed `v1beta2`** (and `v1alpha1`/`v1beta2` of `KafkaTopic` and `KafkaUser`) from the CRDs; 0.23.0 had removed
`v1alpha1` and `v1beta1`; 0.46.0 removed ZooKeeper-based clusters (KRaft only, `KafkaNodePool` required); 0.37.0
added version fields to the `Kafka` status, and `KafkaStatus` carries `operatorLastSuccessfulVersion` ("the version
of the Strimzi Cluster Operator which performed the last successful reconciliation"), `kafkaVersion`,
`kafkaMetadataVersion`, `listeners`, `clusterId`; 1.3.0 requires Kubernetes ≥ 1.32; the newest entries are 1.4.0
(Kafka 4.2.x, 4.3.x). Release dates are not in the changelog (unverified, not needed by code). The Kubernetes
minimum of 1.0.x to 1.2.x is **unverified** here; the user page (U12e) states the matrix per Strimzi release from
the Strimzi docs.

**Supported:** Strimzi **1.0.0 and newer**, every CR at `kafka.strimzi.io/v1`: `Kafka`, `KafkaNodePool` (read
only), `KafkaConnect`, `KafkaConnector`, `KafkaTopic`, `KafkaUser`. Strimzi 0.49.x serves `v1` too and works, but is
not in the test matrix. Strimzi ≤ 0.48 (`v1beta2` only) is detected and refused with the reason "Strimzi 0.48 or
older serves only v1beta2; upgrade the operator" and a link; the driver does not carry two template sets, because
the operator an enterprise cluster runs is 1.x and a dual set doubles every validator test for a shrinking
population (§12 D1). `KafkaMirrorMaker2`, `KafkaBridge`, `KafkaRebalance` are never read or written.

**Detection** (`env_detect` in the Job, T0, three API reads, all inside the namespace or on discovery endpoints every
authenticated principal may read):

1. `GET /apis/kafka.strimzi.io` → `APIGroup.versions[]`; `v1` must be present; `preferredVersion` is recorded.
   Only `v1beta2` → refused as above. No group → "Strimzi is not installed on this cluster".
2. `GET /apis/kafka.strimzi.io/v1/namespaces/<ns>/kafkas` → the one `Kafka` the person named (`Env.Targets` kind
   `kubernetes` carries `<context>/<namespace>`; the cluster name is a `SetupRequest` target of kind `kafka` with
   address `strimzi://<name>`, §11 C5). Its `status.conditions[type=Ready]`, `status.observedGeneration`,
   `status.kafkaVersion`, `status.listeners[]` (`name`, `bootstrapServers`, `certificates`) and `spec.kafka.version`
   give the Kafka version, the bootstrap address and whether the operator watches this namespace: a `Kafka` whose
   `status.observedGeneration` is missing or behind `metadata.generation` for more than the detect timeout means
   the operator does not reconcile here, and the plan stops with that question. The runner cannot read the operator's
   own namespace (no cluster-scoped RBAC), so this is the only operator probe it has.
3. The operator version is `status.operatorLastSuccessfulVersion` of that `Kafka` CR (verified field). It is
   reported in `env_detect`'s `Data` as `strimzi_version` and the Kubernetes server version (`GET /version`) as
   `kubernetes_version`; the known-issues index (Q1) matches on both. (Rev 1 put them in `Hello.Tools`; `Tools` is
   documented as tools found on `PATH`, so they move to `env_detect` and need no contract change; `Hello.Tools` in
   a Job is empty.)

## 3. The in-cluster runner Job

The Helm chart (C1, U12c) renders, with `setup.runner.enabled=true`, `setup.runner.room=<room>`,
`setup.runner.pipeline=<pipeline>` and `setup.runner.namespace=<ns>`:

- a `ServiceAccount` `piperoom-setup-<room>` in `<ns>`;
- the `Role` and `RoleBinding` of §4;
- **the objects the runner may only patch** (S1): the `Secret` `<pipeline>-db-credentials` (empty, `type: Opaque`;
  a Secret's `type` is immutable, so no later patch can turn it into a `kubernetes.io/service-account-token`
  Secret), the `ConfigMap` `<pipeline>-connect-logging` (empty), and the `Role`/`RoleBinding`
  `<pipeline>-connect-secrets` (one rule: `get` on `secrets` with `resourceNames: [<pipeline>-db-credentials]`,
  bound to the Connect pods' ServiceAccount `<pipeline>-connect-connect`; the chart renders it, the runner never
  touches RBAC);
- a `Job` `piperoom-setup-<room>` in `<ns>`: the Piperoom image (it ships `piperoom-setup`, U1-22), one container,
  `args: ["--server", "<PIPEROOM_PUBLIC_URL>", "--code-file", "/var/run/piperoom/pairing/code",
  "--confirm-secret-prefix", "piperoom-setup-<room>-confirm", "--project", "/work"]`, `restartPolicy: Never`,
  `backoffLimit: 0`, `activeDeadlineSeconds: 7200` (longer than the 45 min plan wall clock plus the ladder's waits,
  so the server's bound ends the setup, not the Job), `ttlSecondsAfterFinished: 3600`,
  `automountServiceAccountToken: true` (a projected token volume with `expirationSeconds: 3600`; the runner re-reads
  the file per request), `runAsNonRoot`, read-only root filesystem, an `emptyDir` at `/work` (the project directory:
  templates render there, the op log `.piperoom/ops.jsonl` and the secret store `.piperoom/secrets.env` live there
  and die with the pod; the record of the setup is the room log and the receipts), no `hostPath`, no privileges,
  resources `100m/256Mi` requests. The pairing Secret is mounted at `/var/run/piperoom/pairing` as a projected
  volume with `optional: false`.

`<room>` is the room id without its `r_` prefix (the value `POST /api/setup` returned). The UI's **Set up** page for
the Strimzi target shows the three commands of §3.1 and §3.2 instead of the one `piperoom-setup --server --code`
line, and `command` in the `POST /api/setup` response carries them joined by `\n` (§11 C6).

**Teardown.** `ttlSecondsAfterFinished` removes the Job only. The ServiceAccount, Role, RoleBinding and the
pre-created objects outlive the room, so the room's close card and the rollback card both give
`helm uninstall piperoom-setup-<room> -n <ns>`; after S1 the leftover Role holds no `create` on core or RBAC
resources, so the window between close and uninstall grants nothing beyond patching the room's own objects.

**Pairing hijack.** Anyone with `get` on Secrets in the namespace can read the pairing code and pair first; the
real Job then fails with `bad_code`, but the room shows an attached runner. The room therefore shows the runner's
`Hello.OS`/label as "in-cluster Job" versus "laptop", so a person who started a Job and sees a laptop runner
notices; the `setup.pairing` audit carries the source address.

### 3.1 How the pairing code reaches the Job

The code is never in a manifest, a Helm value or an environment variable (environment values show in `kubectl
describe` and in every pod inspection). The person runs, on their own machine, with their own `kubectl`:

```
kubectl -n <ns> create secret generic piperoom-setup-<room>-pairing --from-literal=code=<CODE>
helm upgrade --install piperoom-setup-<room> piperoom/piperoom -n <ns> --set setup.runner.enabled=true --set setup.runner.room=<room> --set setup.runner.pipeline=<pipeline> ...
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
Job never self-confirms, and after S1 it cannot: it has no `create` on Secrets. The person's "y" is a second
Kubernetes write with their own credentials:

1. **The fingerprint is computed by the server, for every setup room** (not only Strimzi), and sent in `Welcome`
   and shown on the **Set up** page (§11 C6, C7): the first 12 hex characters of SHA-256 over
   `public_origin \n room_id \n pipeline_id \n creator.id \n nonce`, where `public_origin` is the server's configured
   `PIPEROOM_PUBLIC_URL` (never the URL the Job dialed: an in-cluster Job usually dials a Service address while the
   browser knows only the public origin, and two fingerprints that never match teach people to ignore the check)
   and `nonce` is 16 random bytes the server draws when it issues the pairing code, kept with the code, never shown
   as such (so the fingerprint cannot be precomputed by anyone who knows the public room fields). The runner
   recomputes nothing: it checks that `Welcome.PublicOrigin` equals its `--server` value (a Job is started with the
   public origin; a mismatch is `PairingRefused` with both values in the log) and logs the `Welcome` block: origin,
   `Title`, `PipelineID`, `Tenant`, `Creator`, the targets with their classes, and the fingerprint. A terminal runner
   prints the same block and asks `y/N`; one rule for both.
2. **Before it logs the block** the runner does `GET .../secrets/piperoom-setup-<room>-confirm`. If the Secret
   exists, the runner deletes it, logs "a confirm Secret existed before the Welcome; refused" and exits 3
   (`PairingRefused`). It then records the time it logged the block.
3. The person reads it: `kubectl -n <ns> logs job/piperoom-setup-<room>`, compares the fingerprint with the page,
   and confirms: `kubectl -n <ns> create secret generic piperoom-setup-<room>-confirm
   --from-literal=fingerprint=<the 12 characters>`. The runner polls every 2 s for at most `protocol.CodeTTL`;
   on a hit it compares in constant time and checks `metadata.creationTimestamp` is **after** the time it logged
   the block (API-server time versus the pod's clock: a 30 s skew allowance, and any Secret older than the block
   is refused). A match sends `Hello` and deletes the Secret; a mismatch, an early timestamp or the timeout is
   `PairingRefused` (the server emits `setup.pairing {outcome: refused}`), and the Job exits 3.

**Residual risk, stated:** a principal with `pods/log` and Secret `create` in the namespace can read the
fingerprint from the log and race the person to create the confirm Secret. Such a principal already controls the
namespace (it could replace the Job's image), so the check does not defend against it; it defends against a Job
pointed at a server the person did not intend, and against a pre-created confirmation.

Why a Secret and not an annotation on the Job or a ConfigMap: the confirmation must be something only a person with
write access to the namespace can produce (the same bar as the code), and `create` on a named Secret is the
narrowest write the person's own kubeconfig already has; the runner needs only `get` and `delete` on fixed names.
Why not `kubectl attach -i`: a Job's stdin is not reliable across restarts and proxies, and a confirmation that
depends on a live TTY cannot be audited; the Secret create is in the audit log of the API server.

Every later T3 op (§7) also needs a terminal "y" by the parent design. In a Job that "y" is the same mechanism with
a per-op name: the runner logs the op, its target and its `params_hash` prefix and waits for
`piperoom-setup-<room>-confirm-<step_index>` with `fingerprint=<params_hash prefix>`, with the same pre-existence
and timestamp rules; without it in 10 minutes it refuses with `RefuseNotConfirmed`. "Roll back this setup" on a
production target is T3 too; its steps use `piperoom-setup-<room>-confirm-rollback-<step_index>`. The Role lists
both name families for step indexes 0 to 11 (the plan's 12-step bound), since `resourceNames` has no globs. The
credentials confirmation (§9) is `piperoom-setup-<room>-confirm-creds`.

### 3.3 Reach, secrets and the project directory in a Job

- The runner's `net.Dialer` `Control` hook (parent §2.6) applies unchanged; the API server address from
  `KUBERNETES_SERVICE_HOST` is added to the runner's own target registry as `shared` at start, and the Kafka
  bootstrap of the `Kafka` CR's internal listener is a target the server registered from the `SetupRequest`.
- The secret store is the `emptyDir`; `secret_set` through the UI route works as on a laptop. The database password
  reaches the connector only through the chart-created Secret `<pipeline>-db-credentials` (`k8s_secret_apply`
  patches its `stringData`, §6) referenced from the connector config with Strimzi's `KubernetesSecretConfigProvider`
  syntax `${secrets:<ns>/<secret>:<key>}`; the rendered `KafkaConnect` enables that provider and nothing else
  (`config.providers: secrets`, `config.providers.secrets.class: io.strimzi.kafka.KubernetesSecretConfigProvider`),
  and the chart's `<pipeline>-connect-secrets` Role lets the Connect pods read that one Secret.
- No op in the Job can read the pairing or confirm Secrets back (they are deleted), and `k8s_get` refuses `secrets`
  as a kind (§5); the only Secret the runner reads is the KafkaUser's credentials Secret in §9, by name.

## 4. RBAC for exactly one namespace

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: piperoom-setup-<room>, namespace: <ns>}
rules:
- apiGroups: [kafka.strimzi.io]
  resources: [kafkas, kafkanodepools]
  verbs: [get, list, watch]                       # detection, bootstrap, version, listener status
- apiGroups: [kafka.strimzi.io]
  resources: [kafkaconnects, kafkaconnectors, kafkausers, kafkatopics]
  verbs: [create, list, watch]                     # create cannot be narrowed by name; list/watch for status polling
- apiGroups: [kafka.strimzi.io]
  resources: [kafkaconnects]
  resourceNames: [<pipeline>-connect]
  verbs: [get, patch, delete]                      # server-side apply (dryRun=All preview, then the same request);
                                                   # delete for rollback. By name, so adopting another Connect cluster
                                                   # is refused by RBAC, not only by the guard
- apiGroups: [kafka.strimzi.io]
  resources: [kafkaconnectors]
  resourceNames: [<connector names of the provisional pipeline>]
  verbs: [get, patch, delete]
- apiGroups: [kafka.strimzi.io]
  resources: [kafkausers]
  resourceNames: [<pipeline>-connect, <pipeline>-piperoom-ro]
  verbs: [get, patch, delete]
- apiGroups: [kafka.strimzi.io]
  resources: [kafkatopics]
  resourceNames: [<pipeline>-canary]
  verbs: [get, patch, delete]
- apiGroups: [""]
  resources: [secrets]
  resourceNames: [piperoom-setup-<room>-pairing, piperoom-setup-<room>-confirm,
                  piperoom-setup-<room>-confirm-0, ..., piperoom-setup-<room>-confirm-11,
                  piperoom-setup-<room>-confirm-rollback-0, ..., piperoom-setup-<room>-confirm-rollback-11,
                  piperoom-setup-<room>-confirm-creds,
                  <pipeline>-db-credentials, <pipeline>-piperoom-ro]
  verbs: [get, patch, delete]                      # pairing and confirms: read once, delete; db-credentials: patch the
                                                   # chart's empty Opaque Secret (type immutable), delete on rollback;
                                                   # piperoom-ro: the KafkaUser's generated password or certificate (§9)
- apiGroups: [""]
  resources: [configmaps]
  resourceNames: [<pipeline>-connect-logging]
  verbs: [get, patch]                              # the chart's ConfigMap; the only ConfigMap template (log4j levels)
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

- **No `create` on `secrets`, `configmaps`, `roles`, `rolebindings`, `serviceaccounts`** (S1). With Secret create a
  runner could make a `kubernetes.io/service-account-token` Secret for any ServiceAccount in the namespace (the
  Strimzi operator's, in a one-namespace install) and read it back under an allowed name; with RoleBinding create
  it could bind its own Role to `system:authenticated`; with either it could forge its own confirm Secret. The chart
  pre-creates the objects the runner patches (§3), so the runner needs none of those verbs. The verbs
  **`escalate`, `bind` and `impersonate` are never granted**, no `*` appears anywhere, and nothing is cluster-scoped;
  the U12c `helm template` test asserts the Role's `verbs`, `resources` and `resourceNames` **exactly** against a
  golden file for a given room, pipeline and connector list, not only the absence of `*`.
- `list` on `kafkas`, `kafkanodepools` is needed because the runner does not know the cluster name's spelling
  until detection; `list` on `secrets` is **never** granted (it would read every Secret in the namespace), which is
  why every Secret the runner reads has a chart-fixed name.
- `create` on the four Strimzi kinds is broad by necessity; `get`/`patch`/`delete` are by name, so the runner can
  neither read another team's `KafkaConnect` nor adopt it (`:294` of rev 1 relied on the guard alone; RBAC now
  enforces it too). The connector names come from the provisional pipeline (`SetupRequest.Connectors`; the chart
  takes them as a value and the `setup.pipeline_extended` route cannot add one the Role does not name: the card
  says "redeploy the Job with the new connector list").
- `pods/exec`, `pods/portforward`, `pods/attach`: absent. The runner never runs a command in a pod. `deployments`,
  `strimzipodsets`, `services`, `ingresses`: absent. Strimzi owns the Connect pods; the runner reads them through
  `pods` and the CR status only. Exposing the Connect REST or a listener is the person's change (§9).
- **Residual risk, stated:** a `KafkaUser`'s ACLs are not bounded by RBAC. A runner that can patch
  `<pipeline>-piperoom-ro` and `get` its Secret could ask the User Operator for broader ACLs. Only the §6 ACL
  tables (fixed per template, tested) bound this; a server that sends a hand-made ACL list is refused by the
  validator, and the parent's §7 "malicious server" paragraph covers the rest.

## 5. `kubectl` or the Kubernetes API: the API, over `net/http`

**Decision: the runner talks to the API server directly** with the mounted ServiceAccount token and CA
(`/var/run/secrets/kubernetes.io/serviceaccount/{token,ca.crt,namespace}`, `KUBERNETES_SERVICE_HOST`,
`KUBERNETES_SERVICE_PORT`), through the runner's one `*http.Client` (10 s per request, `io.LimitReader` 4 MB,
status checked, bodies closed, the dial `Control` hook) and `encoding/json`. No `kubectl` binary in the image, no
`k8s.io/client-go` (it would add about forty modules for five verbs on nine resources; AGENTS.md §4 rule 10).
This conflicts with board U12a and parent §5, which say `kubectl` argv and `--dry-run=server`; §11 A1 proposes the
amendment, and the lead's review agreed with the decision.

Why not `kubectl` argv: the Compose driver builds `docker compose` argv because Docker has no stable API that
`os/exec` can avoid and the CLI is what the person has; here the CLI is a second code path over the same REST API,
the image would have to ship and pin it, `kubectl apply` reads files the runner would first have to write to disk
(one more executed artifact to confine), and its output is text where the API returns structured status the
ladder's predicates need. Every op below is one or two HTTP requests whose receipt is `SetupReceipt{Kind: "http",
Target: "<method> <path>"}` with the response body's MAC; the `Argv` field stays empty. The U12a unit tests run
against an `httptest` API server that serves discovery, the CRs and `pods/log`, with table cases for 401, 403 (a
rule the Role does not grant), 404, 409 (a conflict on apply), 422 (the schema rejects a field), 429 (`Retry-After`
honoured once) and a cut body.

Ops (tier floor in brackets):

| Op | Requests | Notes |
|---|---|---|
| `env_detect` [T0] | §2 | also `GET /version` |
| `k8s_get {kind, name?}` [T0] | `GET` one or a list of `kafkas`, `kafkanodepools`, `kafkaconnects`, `kafkaconnectors`, `kafkatopics`, `kafkausers`, `pods` | `secrets`, `configmaps`, `events` and any other kind are `RefuseSchema`; the namespace is `Env.Targets`', never a param; `Data` is the object with `metadata.managedFields` dropped |
| `k8s_events {involved?}` [T0] | `GET .../events?fieldSelector=involvedObject.name=<name>` | 100 newest, capped, redacted, injection-scanned on the server |
| `k8s_logs {pod, container?, tail?}` [T0] | `GET .../pods/<pod>/log?tailLines=200&container=` | only pods whose labels carry `strimzi.io/cluster=<pipeline>-connect`, or the build pod `<pipeline>-connect-connect-build`; 200 lines, 64 KiB, newest last |
| `strimzi_build_status {name}` [T0] | `GET .../kafkaconnects/<name>` → `status.conditions`, `status.connectorPlugins[]`, the build pod's phase and, on failure, its log tail | Kaniko (Kubernetes) or Buildah (OpenShift) output is untrusted text |
| `k8s_apply {kind, template, params}` [T2] | preview: `PATCH .../<plural>/<name>?fieldManager=piperoom-setup&force=false&dryRun=All` with `Content-Type: application/apply-patch+yaml`; execute: the same without `dryRun` | server-side apply, so the preview is the API server's own validation (the CRD schema); `force=false` makes a field another manager owns a 409, shown in the preview; for `Secret` and `ConfigMap` the object exists (chart-created) and the patch fills `stringData`/`data` |
| `k8s_secret_apply {keys}` [T2] | the SSA path on `secrets/<pipeline>-db-credentials` | values are `${secret:<name>}` references materialised on the runner into `stringData`; the preview shows key names only |
| `k8s_delete {kind, name}` [T2] | `DELETE .../<plural>/<name>` with `propagationPolicy=Foreground` | rollback only; receipt-created names only, and by RBAC only the names of §4 |
| `k8s_wait {kind, name, condition, timeout}` [T0] | `GET` polling every 5 s, or `watch=true` with `resourceVersion` | Ready, or a connector `status.connectorStatus.connector.state`; ends at the op deadline |

## 6. The manifest field allow-list

`k8s_apply` renders exactly these templates; a `kind` or `template` outside the table is `RefuseSchema`; a
rendered object is validated field by field in `Validate` (no I/O) against the allow-list before the dry run, and
a field outside it is refused even when the template could not have produced it (the test renders each template
with every param and asserts the allow-list is complete; a second test feeds hand-made objects with each refused
field and asserts the refusal names it).

| Kind (`kafka.strimzi.io/v1` unless noted) | Template | Fields the runner may set | Always refused |
|---|---|---|---|
| `KafkaConnect` | `connect` | `metadata.name` (`<pipeline>-connect`), `metadata.annotations["strimzi.io/use-connector-resources"]="true"` (fixed), `spec.replicas` (1–3), `spec.version` (equal to the Kafka CR's `status.kafkaVersion` major.minor, or absent), `spec.bootstrapServers` (from the Kafka CR's listener status only), `spec.tls.trustedCertificates[0]` (`{secretName: <kafka>-cluster-ca-cert, pattern: "*.crt"}`), `spec.authentication` (`type: tls` with `certificateAndKey` from `<pipeline>-connect` KafkaUser's Secret, or `type: scram-sha-512` with `passwordSecret` from it), `spec.config` under a key allow-list (`group.id`, `offset.storage.topic`, `config.storage.topic`, `status.storage.topic`, the three `*.replication.factor` (−1 or 1–3), `key.converter`, `value.converter`, `*.converter.schemas.enable`, `*.converter.schema.registry.url` (a registered target), `config.providers: secrets`, `config.providers.secrets.class: io.strimzi.kafka.KubernetesSecretConfigProvider`), `spec.build.output` (`type: docker`, `image: <registry>/<repo>:piperoom-<pipeline>-<plugins digest>`, `pushSecret`; the registry host must be an `Env.Targets` entry of kind `image_registry`, §11 C5), `spec.build.plugins[]` (name and artifacts **only from the knowledge index**: `type: tgz`/`jar`/`zip` with `url` on `PIPEROOM_SETUP_PLUGIN_HOSTS` and the index's `sha512sum`; `type: maven` only if the owner decides so, §12 D10, since Strimzi's maven artifact type carries no checksum), `spec.resources` (requests and limits within `100m–2` CPU, `512Mi–4Gi` memory), `spec.logging` (`type: inline`, loggers from a level allow-list, or `type: external` naming `<pipeline>-connect-logging`), `spec.metricsConfig` (the template's JMX exporter ConfigMap only) | `spec.image` (the operator picks the image for `spec.version`; a user image means an unvalidated artifact), `spec.template` entirely (pod, container, `securityContext`, `hostPath`, `hostNetwork`, `serviceAccountName`, `initContainers`, `volumes`, `env`, `tolerations`, `affinity`), `spec.externalConfiguration` (deprecated in Strimzi 0.44.0; whether it survived into the `v1` schema is unverified; refused either way), `spec.jvmOptions` other than `-Xms`/`-Xmx` within the resource limits, `spec.rack`, `spec.clientRackInitImage`, `spec.build.output.type: imagestream` (OpenShift, not in the matrix), `spec.build.resources`, any `config.providers.*.class` but the one above, any `config` key with `ssl.`, `sasl.`, `plugin.path`, `rest.`, `listeners`, `admin.`, `producer.`/`consumer.` prefixes, any `metadata.labels`/`annotations` beyond the one fixed annotation |
| `KafkaConnector` | `connector` | `metadata.name` (a connector name of the provisional pipeline), `metadata.labels["strimzi.io/cluster"]` (`<pipeline>-connect`), `spec.class` (an `io.debezium.connector.*` class the knowledge index lists for the plugin), `spec.tasksMax` (1 for a Debezium source), `spec.config` (the source driver's `ConnectorConfig` after Q4a's validate, where every secret is `${secrets:<ns>/<pipeline>-db-credentials:<key>}`; `setup_config_help` keys for SMTs and sinks, U3), `spec.autoRestart` (`enabled: true, maxRestarts: 3`) | `spec.state`/`spec.pause` (lifecycle is a typed op through the Q5c actions over the remote Connect client, not a manifest edit), `spec.listOffsets`/`alterOffsets`, any config key that `actions.LooksSecret` matches with a literal value, `connector.class` in `config` (it is `spec.class`), `database.hostname` not in `Env.Targets` |
| `KafkaUser` | `connect-user`, `read-user` | `metadata.name` (`<pipeline>-connect`, `<pipeline>-piperoom-ro`), `metadata.labels["strimzi.io/cluster"]`, `spec.authentication.type` (`tls` or `scram-sha-512`, whichever the Kafka listener uses), `spec.authorization` (`type: simple`, ACLs from a fixed table per template: the Connect user gets Create/Describe/Read/Write on `<topic_prefix>` prefixed topics and the three internal topics, Read/Describe on its group, Describe on the cluster; the read user gets Describe/Read on `<topic_prefix>*` and `DescribeConfigs`, Describe on `<pipeline>-connect` group, Describe on the cluster) | `spec.authorization.acls[]` outside the table, `type: tls-external`, `spec.quotas`, `spec.template` |
| `KafkaTopic` | `canary-topic` | `metadata.name` (`<pipeline>-canary`), `metadata.labels["strimzi.io/cluster"]`, `spec.topicName` (`<topic_prefix>.<schema>.<canary table>`), `spec.partitions` (1), `spec.replicas` (≤ the broker count), `spec.config` (`cleanup.policy`, `retention.ms` only) | anything else; the op exists only because Strimzi clusters often run with `auto.create.topics.enable=false` and the Topic Operator owns topics; when auto-create is on the plan skips it |
| `Secret` (core/v1), **patch of the chart's object** | `db-credentials` | `stringData` keys from the source driver's `ConnectorConfig` secret keys, values materialised from `${secret:}` | `type`, `data`, labels, annotations, ownerReferences, any other name |
| `ConfigMap` (core/v1), **patch of the chart's object** | `connect-logging` | `data["log4j.properties"]` rendered from a level map (`io.debezium`, `org.apache.kafka.connect`, root) | any other key or free content |

`Role`/`RoleBinding` are not a runner kind (rev 1 had them; S1 moved them to the chart). What is refused is tested
per field (board U12a) and the refusal text names the field path. A template change is a PR to
`internal/setup/targets/strimzi` and its allow-list test, never a parameter.

## 7. Tiers and the guard per op

Classes: a Kubernetes target is **never `local`** (a Job runs in a cluster other people share; the parent's
`local` rule covers loopback and a local engine only), so every write floor is T2. `production` when the
`SetupRequest` target class says so, or `PIPEROOM_SETUP_PROD_PATTERNS` matches the context name, the namespace
(`<context>/<namespace>` is matched as one string) or the Kafka cluster name.

| Op | Floor | On `production` |
|---|---|---|
| `env_detect`, `k8s_get`, `k8s_events`, `k8s_logs`, `strimzi_build_status`, `k8s_wait` | T0 | T0 (reads need no confirm; logs are capped and scanned) |
| `k8s_secret_apply`, `k8s_apply` (`Secret`, `ConfigMap`, `KafkaTopic`, `KafkaUser`) | T2 | T3: approval, typed confirm `<pipeline>/kafka`, the per-op confirm Secret of §3.2 |
| `k8s_apply` `KafkaConnect` | T2 | T3; a `KafkaConnect` whose name exists and is not receipt-created is refused on every class (`RefuseGuard`, and 403 by RBAC: adopting someone's Connect cluster is never a setup step) |
| `k8s_apply` `KafkaConnector` | T2 | T3 |
| `k8s_delete` `KafkaConnector`, `KafkaTopic` | T2, receipt-created only | T3 |
| `k8s_delete` `KafkaConnect`, `KafkaUser`, `Secret` (clearing `<pipeline>-db-credentials` is a patch to empty, not a delete) | T2, receipt-created only | **refused** (guard row: a Connect cluster or a user on production is removed by a person) |
| anything on `Kafka`, `KafkaNodePool`, namespaces, RBAC, the operator | no op exists | – |

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
| build | `pushSecret` missing or denied, artifact `sha512sum` mismatch, artifact unresolved, Kaniko OOM | `KafkaConnect` condition `NotReady` with `reason: ...Build...`, the build pod log tail | correct the registry target (a driver click), the plugin version (knowledge index), the build resources (within bounds); never a raw artifact URL |
| connect | pods `CrashLoopBackOff`, TLS trust or SCRAM failure, group id in use | `k8s_logs` of the Connect pod, `k8s_events` | authentication type matching the listener, a fresh `group.id`, the Connect user's ACLs |
| connector | `KafkaConnector` `NotReady` (`ConnectRestException`), task `FAILED` with a trace | `status.connectorStatus`, `status.conditions[].message` (untrusted text, wrapped) | the Q4a validate verdict, the source prerequisites (publication, `wal_level`, grants: the Postgres driver's checks over the remote client) |
| ladder | no canary event: topic auto-create off, ACL denies Write, snapshot not finished | rungs 3–5 | `KafkaTopic` template, the Connect user's ACL table, wait |

Bounds are the parent's (3 attempts per step, 12 steps, 45 min, token budget); a Strimzi build takes 2–8 minutes,
so `k8s_wait` on the build has a 10-minute op deadline and counts as one step.

Rollback (`PreviewResult.Rollback` per step, run in reverse by "roll back this setup", each step through the guard
again, T3 confirms under the `-confirm-rollback-<i>` names): `KafkaConnector` → `k8s_delete`; `KafkaTopic` →
`k8s_delete` (the topic is deleted by the Topic Operator; on `production` the guard refuses it because it is data,
so the card says "topic kept"); `KafkaConnect` → `k8s_delete` (the built image stays in the registry; the card names
the tag and that the person deletes it); `KafkaUser` → `k8s_delete`; the db-credentials Secret and the logging
ConfigMap → a patch back to empty (they are the chart's; `helm uninstall` removes them). The source-side steps
(publication, canary table, read role) roll back through their own ops as on Compose. A rollback never touches
the `Kafka` CR. The Job then exits 0, the room closes `rolled back`, and the card gives
`helm uninstall piperoom-setup-<room> -n <ns>`.

## 9. Verification and watching afterwards

**Ladder rung 1 source** is the `KafkaConnector` CR: `status.conditions[type=Ready].status == "True"` and
`status.connectorStatus.connector.state == "RUNNING"`; rung 2 is `status.connectorStatus.tasks[].state`; rung 3
is the `KafkaTopic` (when the Topic Operator runs) or the remote Kafka client's topic list; rungs 4–7 run as on
Compose through the remote clients, from inside the cluster: the Job reaches the internal listener and the
`<pipeline>-connect-connect-api:8083` Service, which a team server cannot. `Env.Targets` therefore carries both the
internal bootstrap (the runner's) and, when the person listed one, an external listener (the server's); the ladder
runner issues each rung's read through whichever client can reach (parent §3.4 step 2).

**Watching** (parent §3.7, class `shared` or `production`): the team server needs three things it does not get
for free on Strimzi:

1. **Kafka**: a listener reachable from the server (`type: route`, `loadbalancer`, `nodeport` or `ingress` on the
   `Kafka` CR), which only the cluster's owner can add (the runner has no op on `Kafka`). The setup creates the
   `KafkaUser` `<pipeline>-piperoom-ro` (the read ACL table of §6); the User Operator writes its password or
   certificate into the Secret of the same name; the runner reads that Secret (a `get` by name in §4), stores the
   values in its own store under `kafka_sasl_password` or `kafka_tls_cert_file`/`kafka_tls_key_file` plus the
   cluster CA, and `CreateReadRole`'s contract holds: only the names come back. **Register read credentials** then
   sends them in the `Credentials` frame after the `piperoom-setup-<room>-confirm-creds` Secret (§3.2 rules), into
   `PIPEROOM_SECRETS_DIR/<pipeline>/`, and `pipeline_register` writes
   `connections: {kafka_brokers: {value: <external bootstrap>}, kafka_security_protocol: {value: SASL_SSL | SSL},
   kafka_sasl_mechanism: {value: SCRAM-SHA-512}, kafka_sasl_username: {value: <pipeline>-piperoom-ro},
   kafka_sasl_password: {file: ...}, kafka_tls_ca_file: {file: ...}}` (the `tenancy.md` §3.2 keys).
2. **Connect**: the Connect REST API is a ClusterIP Service. Options: the person exposes it with an Ingress and basic
   auth and registers it as a target (`connect_url`, `connect_username`, `connect_password` in `connections:`), the
   U12 path; or a later `CONNECT_KIND=strimzi` client on the server that reads `KafkaConnector` status over the
   Kubernetes API with a read-only ServiceAccount token the person issues (§12 D4). With neither, the pipeline
   registers with `setup.unreachable_layers: [kafka-connect]` (the same mechanism as the managed design's §7, one
   meaning of `watched_by`), the watcher skips that layer with a reason, and the card says the server sees Kafka
   only.
3. **Registry**: `schema_registry_url` when the person listed one; Strimzi ships none.

## 10. Tests and sandbox

- **Unit (no cluster)**: the allow-list per field (§6), the ACL tables, the `argv`-free rule (no `os/exec` import in
  the package, an AST test like `wiring/readonly_ast_test.go`), the API client against `httptest` (discovery, SSA
  dry run, the status codes of §5, token re-read, the 4 MB cap), the fingerprint flow with a fake API server and a
  fake Piperoom server (origin mismatch, pre-existing confirm Secret, early `creationTimestamp`, timeout, match),
  `k8s_get` refusing `secrets`, `k8s_logs` refusing a pod outside the room's Connect cluster, `k8s_delete` refusing
  a name without a receipt; in the chart, the golden-file Role test (§4).
- **Integration (nightly, `kind`, `ubuntu-latest`, 20-minute budget, U12c)**: `kind` with a local registry
  (`registry:2` on `localhost:5001`, the kind docs' pattern) so the Strimzi build can push; the Strimzi operator
  at the pinned version in `dev/strimzi/VERSION` (installed from the release's `strimzi-cluster-operator-<version>.yaml`
  asset, not `install/latest`, so the run is reproducible); a one-broker KRaft `Kafka` with a `KafkaNodePool` and a
  `scram-sha-512` internal listener; the Helm chart's Job; the recorded-model agent from an empty namespace to
  `pipeline live` with the nonce canary. A second leg applies each refused field and asserts the refusal before any
  request; a third breaks the build (wrong `sha512sum`) and asserts the `setup.blocked` question names the artifact.
- **Real run (U12-R)**: the `kind` leg's export, and one run on a real cluster the owner names (manual, dated): the
  plan's §7 item 6 lists no Kubernetes sandbox (§12 D3).

## 11. Open questions / contract impact

**A. Conflicts with the board and the parent design (proposed amendments, carried by the leads' contracts PR):**

- **A1, board U12a and parent §5 Strimzi row.** They say `kubectl` argv from typed params, `--dry-run=server` as
  the preview, unit tests with a fake `kubectl` on `PATH`. This page: the Kubernetes API over `net/http`, server-side
  apply with `dryRun=All` as the preview, unit tests against an `httptest` API server, no `kubectl` in the image
  (§5). Amend U12a's PR column and Done-when accordingly; the AST test becomes "no `os/exec` in the package".
- **A2, board U12a and parent §2.5 table.** They allow `k8s_apply` kinds `KafkaConnect`, `KafkaConnector`, `Secret`,
  `ConfigMap` only. This page adds `KafkaUser` (the Connect user and the watcher's read user) and `KafkaTopic` (the
  canary topic when auto-create is off), and turns `Secret`/`ConfigMap` into patches of chart-created objects
  (§6). Amend both; `Role`/`RoleBinding` stay out of the runner.
- **A3, ops not on any board row.** `k8s_delete`, `k8s_wait`, the `env_detect` Strimzi probes (§2), the KafkaUser
  Secret read of §9 and the `-confirm-*` Secret handling of §3.2 are new. Proposed: U12a gains `k8s_delete`,
  `k8s_wait` and the confirm flow (+1 day, 3.5); U12b gains the `KafkaUser` and `KafkaTopic` templates and the
  Secret read (+0.5 day, 2.5).
- **A4, board U12c.** The chart now also pre-creates the empty `Opaque` Secret, the ConfigMap and the
  `<pipeline>-connect-secrets` Role/RoleBinding, takes the connector list as a value, and carries the golden-file
  Role test (+0.5 day, 2.0).
- **A5, parent §3.7 and the managed design §7.** The Strimzi Connect layer uses `unreachable_layers` (C4 below),
  not a `watched_by: local` variant.

**B. Open questions:** none beyond §12; rev 1's Q1, Q5, Q6 are owner decisions D2, D4, D3 there.

**C. Contract impact (one separate leads PR, "U12-0/U13-0 contracts", merged before U12a, U13a and U13c):**

- **C5, `events.SetupTarget` doc comment.** Kind `kafka` with address `strimzi://<name>` for a Strimzi cluster (no
  port; the runner resolves the listener from the CR's status); a new kind **`image_registry`** for the build output
  registry host (U0's `registry` means the Schema Registry and stays so). The server's denylist and resolve step
  (parent §2.6) skip DNS resolution for non-host addresses (`strimzi://`, account ids); the runner's dial hook stays
  the guarantee. Doc-comment only, no type change; the targets route (A5) accepts the new kind.
- **C6, `docs/api.md` `POST /api/setup`.** The response gains `fingerprint` for **every** setup room (§3.2); for
  `target: strimzi`, `command` is the three lines of §3.1–§3.2 joined by `\n`. The row states the fingerprint's
  inputs (public origin, room, pipeline, creator, server nonce) so the UI and both runner modes show one rule.
- **C7, `protocol.Welcome`.** Gains `PublicOrigin string` (the server's `PIPEROOM_PUBLIC_URL`) and `Fingerprint
  string` (§3.2). A field addition to the contract file; every existing fake compiles. `Hello.Tools` is unchanged
  (the Strimzi and Kubernetes versions go to `env_detect`, §2).
- **C8, owners `setup:` block.** Gains `unreachable_layers: []` (shared with the managed design's C-list); the
  watcher (A7) gets a follow-up row to skip those layers with a reason.

## 12. Owner decisions (recommended default first)

| # | Decision | Recommended default | Alternative |
|---|---|---|---|
| D1 | Strimzi version floor | support ≥ 1.0 (`v1` API); refuse ≤ 0.48 with an upgrade message; 0.49.x works but is untested | carry a `v1beta2` template set for 0.4x pilots (+3 days, doubles the validator tests) |
| D2 | Laptop runner with a kubeconfig | out of U12; the Job is the only Strimzi mode | if a pilot needs it: `client-go` as a new module (exec credential plugins for EKS/GKE/AKS) or `kubectl` argv for that mode only; a new design row either way |
| D3 | U12-R sandbox | `kind` on the dev machine for the Linux real run **plus** one small managed cluster (EKS or GKE, one node pool) with an image registry the build can push to, owned like the plan §7 item 6 accounts | `kind` only (then "a real cluster" in U12-R's Done-when is dropped) |
| D4 | Watching Connect on Strimzi | U12 requires an Ingress with basic auth for the Connect REST API, registered as a target; without it the Connect layer is `unreachable_layers` | fund a `CONNECT_KIND=strimzi` client (status from the `KafkaConnector` CR over the Kubernetes API with a read-only token on the server): a new config value and client package, after the first real run |
| D10 | `maven` plugin artifacts | **refuse** `type: maven` in U12; only `tgz`/`jar`/`zip` artifacts with the index's `sha512sum` (the parent §2.5 rule holds) | allow `maven` with Maven Central as the trust root and the coordinates pinned in the index (no checksum; a documented exception to §2.5) |

(The numbering continues the managed design's D5–D9 so one owner reply can answer both pages.)
