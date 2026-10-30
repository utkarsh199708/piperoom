# Strimzi target: setup on Kubernetes with the Strimzi operator

For the platform engineer who wants a setup room to build a Debezium pipeline on a Kubernetes cluster where the
Strimzi operator already runs Kafka. It says what you install, what the in-cluster runner can and cannot touch, how
you confirm the pairing, what each step writes, how to undo it, and what is not finished.

Checked against branch `claude/serene-maxwell-1dgs4o` after the U12c commits. Claims name the file they come from; the
design is `docs/design/setup-strimzi.md` (cited as "design §n"). Nothing on this page was run on a real cluster; the
nightly kind job (section 8) is the first place it runs.

## Status on this release

| Piece | State | Where |
|---|---|---|
| A setup room with target `strimzi` | accepted by `POST /api/setup` | `internal/room/setup.go` |
| The Helm chart's two setup releases (paused shells, runner Role, Job) | done, golden-tested | `deploy/helm/piperoom/templates/setup-pipeline.yaml`, `setup-runner.yaml` |
| The runner's Strimzi driver: `k8s_get`, `k8s_events`, `k8s_logs`, `strimzi_build_status` (T0), `preview_k8s_apply` (T0), `k8s_apply` and `k8s_secret_apply` (T2) | done, unit-tested against a fake API server | `internal/setup/targets/strimzi/` |
| In-cluster pairing: code from a Secret, confirmation by a Secret with the fingerprint | done in the runner (`--code-file`, `--confirm-secret-prefix`) | `internal/setup/targets/strimzi/confirm.go`, `internal/setup/setupcmd/incluster.go` |
| Registering the Strimzi driver in the Job | done, **but it fires only when the server's `Welcome` names a `kubernetes` target**, and the server does not fill `Welcome.Targets` yet | `internal/setup/setupcmd/targets.go`; `internal/httpapi/runners.go` (A5) |
| The server's action catalogue (`k8s_apply` as a proposable step) and read tools for the agent | **not done**: no `k8s_*` entry in the environment specs or the setup read tools | `internal/setup/actions/specs.go`, `internal/setup/tools/read.go` |
| `${secret:name}` values for `k8s_secret_apply` in the Job | **not wired**: the Job passes no secret lookup, so the op refuses there | `internal/setup/setupcmd/incluster.go` |
| Verification ladder | `Ladder(source, "strimzi", sink)` reads Connect's REST status; `StrimziLadder` reads the `KafkaConnector` CR instead and is **not called** by the room's ladder runner yet | `internal/setup/plan/ladder.go`, `ladder_strimzi.go` |
| Rollback ops (`k8s_delete`, re-pause) | **not done**; the preview names them as the rollback | `internal/setup/targets/strimzi/apply.go` |

So today a room cannot yet drive a Strimzi setup end to end. What follows is the contract of the pieces that exist,
so a pilot can install the chart, check the RBAC and review what the runner would do.

## 1. What you need

- Kubernetes 1.30 or later (the `ValidatingAdmissionPolicy` of section 6 is GA there), `kubectl` and `helm` 3 with
  your own credentials for the target namespace.
- The Strimzi operator watching that namespace, at a release that serves `kafka.strimzi.io/v1` (the driver speaks v1
  only: `internal/setup/targets/strimzi/strimzi.go`, `apiGroupPath`). The nightly pins 0.49.0.
- A `Kafka` CR in the namespace whose listener the Connect cluster will use, with `authorization: simple` if the
  users' ACLs should mean anything.
- A container registry the Strimzi build can push to, and a docker-registry Secret for it (section 2). The registry
  must be one of the room's `image_registry` targets (`internal/setup/targets/strimzi/templates_connect.go`).
- Network from the cluster to the plugin host (`repo1.maven.org` unless `PIPEROOM_SETUP_PLUGIN_HOSTS` names others).

## 2. What you install

Two Helm releases of the Piperoom chart in the target namespace, plus two Secrets you make yourself because they hold
credentials (design §3, §3.1; `deploy/helm/README.md`, "Setup on Kubernetes with Strimzi"):

```bash
kubectl -n data create secret docker-registry orders-registry-push --docker-server=registry.example.com --docker-username=ci --docker-password="$REGISTRY_PASSWORD"
```

```bash
helm upgrade --install piperoom-pipeline-orders deploy/helm/piperoom -n data --set setup.pipeline.enabled=true \
  --set setup.pipeline.id=orders --set setup.pipeline.kafka=prod-kafka --set 'setup.pipeline.connectors={orders-src}'
```

```bash
kubectl -n data create secret generic piperoom-setup-<room>-pairing --from-literal=code=<CODE>
```

```bash
helm upgrade --install piperoom-setup-<room> deploy/helm/piperoom -n data --set setup.runner.enabled=true \
  --set setup.runner.room=<ROOM ID without r_> --set setup.runner.pipeline=orders --set setup.runner.kafka=prod-kafka \
  --set 'setup.runner.connectors={orders-src}' --set setup.runner.serverURL=https://piperoom.example.com
```

The **pipeline release** (`piperoom-pipeline-<pipeline>`) lives as long as the pipeline. It pre-creates every object
the runner will touch, with names fixed at install:

| Object | Name | State at install |
|---|---|---|
| `KafkaConnect` | `<pipeline>-connect` | paused shell (`strimzi.io/pause-reconciliation: "true"`), `use-connector-resources` on |
| `KafkaConnector` | each name in `setup.pipeline.connectors` | paused shell |
| `KafkaUser` | `<pipeline>-connect`, `<pipeline>-piperoom-ro` | paused shells |
| `KafkaTopic` | `<pipeline>-canary` | paused shell |
| `Secret` | `<pipeline>-db-credentials` | empty, `type: Opaque` (a type cannot change later) |
| `ConfigMap` | `<pipeline>-connect-logging` | empty |
| `ConfigMap` | `<pipeline>-connect-metrics` | the JMX exporter rules, fixed, never patched |
| `Role`/`RoleBinding` | `<pipeline>-connect-secrets` | the Connect pods may `get` the one credentials Secret |
| `ValidatingAdmissionPolicy` + binding (opt-in) | `piperoom-pipeline-<pipeline>` | section 6 |

Paused, the operators create nothing: no pods, no Kafka users, no topic. The runner fills each spec and removes the
pause annotation as the last step.

The **room release** (`piperoom-setup-<room>`) is the runner of one room: a ServiceAccount, its Role and RoleBinding,
and a Job. The close card uninstalls it; the pipeline needs nothing in it.

The chart refuses a release whose name is not one of those two, both blocks in one release, a missing name and a
connector listed twice (`deploy/helm/piperoom/templates/_setup_validate.tpl`).

## 3. Pairing and your confirmation

The code never appears in a value, a manifest or an environment variable. The Job reads it once from the mounted
Secret, sends it, and deletes that Secret as soon as the server's `Welcome` arrives. Then (design §3.2;
`internal/setup/targets/strimzi/confirm.go`):

1. If `piperoom-setup-<room>-confirm` already exists, the Job deletes it and exits: a confirmation made before the
   Welcome is refused.
2. The Job logs the Welcome block: server origin, room, pipeline, creator, targets and a 12-character fingerprint.
   It refuses a Welcome whose public origin is not its `--server` value (`internal/setup/runner/confirm.go`).
3. You compare the fingerprint with the room's **Set up** page and confirm:

   ```bash
   kubectl -n data logs job/piperoom-setup-<room>
   ```

   ```bash
   kubectl -n data create secret generic piperoom-setup-<room>-confirm --from-literal=fingerprint=<12 characters>
   ```

4. The Job polls every 2 seconds for up to 10 minutes. It accepts the Secret only if the fingerprint matches and the
   Secret was created no more than 30 seconds before the block was logged (clock allowance), deletes it, and pairs.
   A wrong fingerprint, an early Secret or no Secret in time ends the Job.

Residual risk, as the design states it: someone with `pods/log` and Secret `create` in the namespace can read the
fingerprint and confirm before you. Such a principal already controls the namespace.

## 4. What the runner can touch

The runner's Role (`deploy/helm/piperoom/templates/setup-runner.yaml`; pinned by
`deploy/helm/piperoom/ci/setup/runner.golden.yaml` and `check.py`):

- **No `create` on anything**, no `update`, no `deletecollection`, no `escalate`, `bind` or `impersonate`, no `*`,
  nothing cluster-scoped.
- `get`/`watch`/`patch` by name on the pipeline release's CRs; `delete` only on the connectors and the canary topic
  (rollback); `get`/`patch`/`delete` on the pairing and confirm Secrets, the credentials Secret and the read user's
  Secret; `get`/`patch` on the logging ConfigMap, `get` on the metrics ConfigMap; `get`/`watch` and `pods/log` on the
  Connect pods `<pipeline>-connect-connect-0..2` and the build pod only.
- `list` on events, namespace-wide: events cannot be scoped by name. Other objects' event messages in the namespace
  are readable (names, reasons, messages; never data).
- No `pods/exec`, `attach` or `portforward`. The runner runs no command in any pod and the driver package does not
  import `os/exec` (an AST test).

The runner process also refuses, before any request, a kind or a name outside the chart's set
(`internal/setup/targets/strimzi/strimzi.go`, `allowedNames`).

## 5. What each step writes

`k8s_apply` takes a template name and typed params, never a manifest (`internal/setup/targets/strimzi/templates.go`,
`templates_connect.go`):

| Template | Object | What the params choose |
|---|---|---|
| `connect` | `KafkaConnect` | replicas (1 to 3), the Kafka listener, TLS and the auth type, the registry and repository, the Debezium release, the plugins (source connectors and the JDBC sink), converter schemas, a resources size |
| `connector` | `KafkaConnector` | which chart connector, the plugin kind, the connector config |
| `canary-topic` | `KafkaTopic` | the topic prefix, schema and table of the canary |
| `connect-user`, `read-user` | `KafkaUser` | the topic prefix and the auth type; the ACLs are a fixed table per template |
| `connect-logging` | `ConfigMap` | log levels for three loggers |

Rules that hold for every render:

- **Plugins come from the knowledge index only.** Each artifact's URL and `sha512sum` are the index's pinned values;
  a plugin host outside the allowed list is refused. The image tag is `piperoom-<pipeline>-<digest>` of the plugin
  set, so a different set is a different image.
- **The bootstrap address is read from the `Kafka` CR's listener status** just before the preview and the write,
  never from params.
- **Credentials are references.** A connector config value that looks like a credential must be a
  `${secrets:<ns>/<pipeline>-db-credentials:<key>}` reference (Strimzi's `KubernetesSecretConfigProvider`, the only
  config provider the `KafkaConnect` may enable). `k8s_secret_apply` writes that Secret's keys from the runner's
  secret store; the values never reach the server.
- **The field allow-list** refuses `spec.image`, `spec.template`, `spec.externalConfiguration`, a push Secret other
  than `<pipeline>-registry-push`, a `KafkaUser` password source and `cluster` or `transactionalId` ACLs, among others
  (`internal/setup/targets/strimzi/allowlist.go`).

A write is three requests (design §5): a server-side dry run, the patch with a `resourceVersion` test (a concurrent
change is a conflict, not an overwrite), and a read-back that must equal the dry run's object before the pause
annotation is removed. A read-back that differs (a webhook or controller added a field) leaves the object paused.

Tiers: the reads and `preview_k8s_apply` are T0; `k8s_apply` and `k8s_secret_apply` are T2 (an approved step).

## 6. Production targets (D11)

RBAC bounds objects, not fields. With `setup.pipeline.admissionPolicy=true` the pipeline release adds a
`ValidatingAdmissionPolicy` and a binding on this pipeline's named `KafkaConnect` and `KafkaUser` that refuse the
fields a compromised runner could use to mount any Secret or mint a Kafka superuser. It is cluster-scoped, so the
install needs cluster-admin. On a `production` target the runner refuses T2 writes unless it can read the binding
`piperoom-pipeline-<pipeline>`, or, where its Role cannot read it, the step's card confirms the policy is bound
(`internal/setup/targets/strimzi/apply.go`, `productionGate`).

## 7. Undo and teardown

- **Close card:** `helm uninstall piperoom-setup-<room> -n <ns>`. The Job's `ttlSecondsAfterFinished` removes only the
  Job; the leftover Role has no `create` and reaches only the pipeline release's objects.
- **Rollback card:** the runner's rollback ops are not written yet (status table). By hand: delete the connector
  (`kubectl -n <ns> delete kafkaconnector <name>`), then `helm uninstall piperoom-pipeline-<pipeline> -n <ns>`. Note that
  uninstalling the pipeline release makes the Topic Operator delete the canary topic and the User Operator the two
  users, and the Connect cluster stops.
- The image the build pushed stays in your registry.

## 8. How it is tested

- Unit, no cluster: the field allow-list, the templates, the three-request write and its refusals, the production
  gate, the Secret confirmation (pre-existing, mismatch, early timestamp, timeout, match), the runner's external
  confirmation and origin check (`go test ./internal/setup/...`).
- Chart: `deploy/helm/piperoom/ci/setup/golden.sh` (golden renders, every CR paused, the Role's verbs and names, the
  Job's pod security) and kubeconform in CI; `internal/setup/targets/strimzi/chart_names_test.go` pins the chart's
  names to the driver's.
- Nightly on kind (`.github/workflows/setup-strimzi-nightly.yml`, schedule only): the pinned operator and a one-broker
  KRaft Kafka, the pipeline release (asserting the shells create nothing), the runner Role checked with
  `kubectl auth can-i` against `deploy/helm/piperoom/ci/setup/kind/rbac-expect.txt`, and the driver's writes with the
  runner's own token. Not in it yet: the Job pairing with a server from the shells to "pipeline live", and the broken
  build leg (a kind registry needs kaniko's `--insecure`, which the allow-list refuses).
- The real run on a cluster the owner names (U12-R) has not happened.

## 9. Known gaps

1. The server sends no targets in the `Welcome`, so the Job never registers the Strimzi driver until it does.
2. The server's action catalogue and read tools have no `k8s_*` entries: the agent cannot propose or read through
   them from a room.
3. The room's ladder runner does not call `StrimziLadder`, so verification reads Connect's REST status, which a
   ClusterIP-only Connect service does not expose to the server.
4. `k8s_secret_apply` refuses in the Job until its `${secret:}` lookup is wired to the runner's secret store.
5. No rollback ops; per-step T3 confirmations by Secret (design §3.2, `-confirm-<n>`) are not implemented (T3 over a
   remote runner is off anyway).
6. The paused `KafkaConnect` shell carries only `replicas` and `bootstrapServers`; whether the `v1` schema needs more
   in a shell is unverified until the first kind run.
