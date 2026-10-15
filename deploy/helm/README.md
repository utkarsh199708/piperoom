# Piperoom Helm chart

Runs the Piperoom pilot image (Go server plus built web UI) on Kubernetes.
Postgres for the room state is **external**; the chart does not install one.
The variable names and meanings are those of `deploy/pilot.env.example` and
`internal/config/config.go`.

## Install

1. Build the image from the repo root and push it where the cluster can pull it:
   `docker build -t registry.example.com/piperoom:pilot . && docker push ...`
2. Read `docs/pilot/permissions.md` and create the read-only database/Kafka accounts.
3. Create the Secret (below) and a values file, for example:

   ```yaml
   image: {repository: registry.example.com/piperoom, tag: pilot}
   ingress:
     enabled: true
     className: nginx
     host: piperoom.corp.example
     tlsSecretName: piperoom-tls
   env:
     PIPEROOM_OIDC_ISSUER: https://idp.corp.example
     PIPEROOM_OIDC_CLIENT_ID: piperoom
     KAFKA_BROKERS: kafka-0.kafka:9092
     CONNECT_URL: http://connect:8083
     SCHEMA_REGISTRY_URL: http://schema-registry:8081
   networkPolicy: {enabled: true}
   ```

   A complete example is in `deploy/helm/piperoom/ci/oidc-values.yaml`.
4. Install, giving the two YAML files (start from `deploy/config/*.example.yaml`):

   ```sh
   helm install piperoom deploy/helm/piperoom -n piperoom --create-namespace -f my-values.yaml \
     --set-file files.owners=owners.yaml --set-file files.users=users.yaml
   ```

   Register `https://<host>/auth/callback` as the redirect URI at the identity provider.

## Required Secret

Create it yourself (kubectl, External Secrets, Sealed Secrets); the chart only references it
(`secrets.existingSecret`, default `piperoom-secrets`). Keys are the environment variable names.

```sh
kubectl -n piperoom create secret generic piperoom-secrets \
  --from-literal=PIPEROOM_DB_URL='postgres://piperoom:...@db.internal:5432/piperoom?sslmode=require' \
  --from-literal=SOURCE_DB_URL='postgres://ro_user:...@src.internal:5432/orders?sslmode=require' \
  --from-literal=WAREHOUSE_DB_URL='postgres://ro_user:...@wh.internal:5432/warehouse?sslmode=require' \
  --from-literal=ANTHROPIC_API_KEY='...' \
  --from-literal=PIPEROOM_OIDC_CLIENT_SECRET='...'
```

`PIPEROOM_DB_URL`, `SOURCE_DB_URL` and `WAREHOUSE_DB_URL` are required (the pod does not start
without them). The keys in `secrets.optionalKeys` (API key, OIDC client secret, Kafka / Registry /
Connect passwords, Slack token, alert secrets, AWS keys) are used when present. Use `secrets.refs`
to read a variable from a different Secret. With Bedrock on EKS prefer IRSA
(`serviceAccount.annotations`) over AWS keys.

## Behaviour worth knowing

- **Auth** defaults to `PIPEROOM_AUTH_MODE=oidc`. In oidc mode the render fails unless
  `PIPEROOM_PUBLIC_URL` is an `https://` URL (the chart derives it from `ingress.host` when
  `ingress.tlsSecretName` is set) and `PIPEROOM_OIDC_ISSUER` and `PIPEROOM_OIDC_CLIENT_ID` are
  set. The chart sets `PIPEROOM_DEV_ALLOW_NONLOOPBACK` to an empty value, so the image's
  dev default does not apply.
- **Dev mode is blocked.** The chart always renders `PIPEROOM_AUTH_MODE` (an empty value means
  oidc) and `PIPEROOM_DEV_ALLOW_NONLOOPBACK` (empty in oidc mode, overriding the image). Any
  other mode fails the render unless `allowDevMode=true`, and dev mode with an Ingress always
  fails. To try dev mode, install with `--set env.PIPEROOM_AUTH_MODE=dev --set allowDevMode=true`
  and no Ingress, then `kubectl port-forward svc/<release>-piperoom 8090:8090`. From R9 the server
  also needs the acknowledgement for dev mode off loopback, so add
  `--set env.PIPEROOM_DEV_ACK=dev-login-has-no-password` (the chart never sets it for you; any
  other non-empty value stops the start; leave it unset in oidc mode).
- **Guard rails.** The render also fails when `env` holds a secret-looking name (PASSWORD, SECRET,
  TOKEN, `_KEY`, `_DB_URL`...), a variable that is also listed under `secrets`, a chart-managed
  variable (`PIPEROOM_HTTP_ADDR`, `OWNERS_FILE`, `USERS_FILE`), a `*_PROXY` URL with a user name
  or password (`://user:pass@`; use `secrets.refs`), or when `PIPEROOM_PUBLIC_URL`
  has a different host than `ingress.host` and `ingress.host` is not in `PIPEROOM_ALLOWED_HOSTS`.
  `values.schema.json` rejects wrong types (for example `networkPolicy.egress.kafka.ports=9092`
  must be a list) and empty egress `ports` lists.
- **With an Ingress** set `env.PIPEROOM_TRUSTED_PROXIES` to the ingress controller pod CIDR.
  Without it the server sees the controller's address for every user, so all users share one
  login rate-limit bucket.
- **Probes** call `/healthz` (liveness) and `/readyz` (readiness, also pings the room database)
  with `Host: localhost`. Exact `GET /healthz` and `GET /readyz` requests skip the Host allow-list,
  so kubelet and load balancers that send the pod IP as `Host` work without a configured host.
  The chart still sends `Host: localhost`, but this is optional. Probe responses contain only fixed
  status and reason values, never raw errors. On GKE with the Gateway API, `host` is optional and
  can be kept if your health-check routing needs it:

  ```yaml
  apiVersion: networking.gke.io/v1
  kind: HealthCheckPolicy
  metadata:
    name: piperoom
  spec:
    default:
      config:
        type: HTTP
        httpHealthCheck:
          portSpecification: USE_SERVING_PORT
          requestPath: /readyz
          host: piperoom.corp.example   # optional; the probe route also accepts the pod IP
    targetRef:
      group: ""
      kind: Service
      name: piperoom-piperoom           # the chart's Service name
  ```

  A classic GKE Ingress `BackendConfig` and AWS target group health checks may not set the Host;
  that is fine because the probe routes accept the backend IP. No intermediary is needed just to
  satisfy the Host allow-list.
- **Replicas.** `replicaCount` defaults to 1 (strategy `Recreate`). More replicas are safe (no
  duplicate agent runs, no duplicate fix executions, no lost or reordered events in the load runs), but
  they **do not raise the room event write rate**: it stays at the single-replica figure. They spread
  viewers, CPU and the per-process lock. Failover (a replica killed under load) was not measured
  (`reviews/capacity-2026-10.md` §9.2, §9.4). A first pilot should run one replica
  ([scale statement](../../docs/pilot/setup.md#supported-scale)). Postgres is the only coordination
  service (design: `docs/design/replicas.md`):
  - each room's agent runs on one replica, which holds a lease row renewed every 10 s (TTL 30 s).
    Steers and comments sent to any replica reach that agent through the event log. When a pod
    stops, its rooms are taken over by another replica (at the next 10 s scan after a graceful stop,
    after the 30 s lease after a crash). An open room (a shadow room excepted: its run is marked `interrupted`) is paused with a "server restarted"
    message until a driver steers, someone mentions `@agent`, a suggestion is sent, or a member confirms
    or disputes the hypothesis, as after a restart of a single server. With
    `PIPEROOM_RESUME_ON_TAKEOVER=1` an agent that was working resumes on another replica (a new
    run that gets the room's unanswered input and its open proposals), at most 3 times per room per
    hour;
  - an approved fix runs exactly once: the executor claims the proposal in Postgres first. A step
    that was running when its pod died is never run again; after about 2.5 minutes it is recorded
    as failed with "outcome unknown";
  - the watcher, the retention purge, the auth prune and the Slack mirror run on one elected leader
    each (a Postgres advisory lock); another replica takes over within about 20 s of losing it;
  - every replica serves every room's WebSocket from the shared event log (one LISTEN connection per
    pod, polling every 2 s while it reconnects), so the load balancer needs no sticky sessions;
  - `PIPEROOM_MAX_AGENTS` is per replica; `PIPEROOM_MAX_AGENTS_GLOBAL` caps working agents across
    all replicas.

  With `replicaCount > 1` the strategy is `RollingUpdate` (`rollingUpdate.maxSurge` 1,
  `maxUnavailable` 0) and a `PodDisruptionBudget` keeps `podDisruptionBudget.minAvailable` (1) pods
  through node drains. Each pod uses about 5 Postgres connections more than the request pool (one
  LISTEN, one per leader loop it holds, short-lived lock connections), so size `max_connections`
  for replicas x (pool + 5). Limits: who is viewing a room (presence) is per replica, so people on
  different pods do not see each other in the member list; the room token budget
  (`PIPEROOM_ROOM_TOKEN_BUDGET`) and the login and alert rate limits are counted per replica; two
  conflicting driver commands sent to two replicas in the same instant (two people taking control)
  are serialized: one wins and the other gets `409 conflict` and retries.
- **Security context**: non-root (uid 65532), read-only root file system, all capabilities dropped,
  seccomp `RuntimeDefault`. `/tmp` is a small in-memory emptyDir; `/config` is the read-only
  owners/users ConfigMap. Kafka/Connect TLS files: mount a Secret with `extraVolumes` and
  `extraVolumeMounts` and point `KAFKA_TLS_*_FILE` at it.
- **Ingress**: default annotations raise the ingress-nginx proxy timeouts so WebSockets stay up
  (nginx upgrades the connection itself). Other controllers need their own settings.
- **NetworkPolicy** (`networkPolicy.enabled=true`): ingress only on 8090 from
  `networkPolicy.ingressFrom` (default: namespace `ingress-nginx`); egress to kube-dns and to the
  ports for Postgres 5432, Kafka 9092/9093/9094/9096/9098 (plaintext, TLS, MSK TLS, SCRAM and
  IAM), Connect 8083, Schema Registry 8081 and HTTPS 443 (IdP, model API, Slack). An opt-in
  `egress.metadata` group (null by default) opens the cloud metadata server for instance roles
  (`169.254.169.254/32` port 80) or GKE Workload Identity (`169.254.169.252/32` port 988); see
  the examples in `values.yaml`. Those groups have an empty `to`, meaning any address on that port; set
  `to` to your CIDRs or selectors, and change `ports` if your services listen elsewhere. Add
  other rules with `extraEgress`. Caveats: in-cluster alert senders (Alertmanager, Grafana)
  need their namespace in `ingressFrom`; DNS egress assumes `k8s-app: kube-dns` (NodeLocal
  DNSCache needs an `extraEgress` rule); 443 to any address is a data-exfiltration path, so
  narrow `egress.https.to` to your IdP and model endpoints; an `HTTPS_PROXY` on another port
  needs an `extraEgress` rule.
- Setting `owners`/`users` in values stores them in a ConfigMap, so keep real user data in a
  ConfigMap you manage (`files.existingConfigMap`) if you do not want it in the release.

## Important values

| Value | Default | Meaning |
|---|---|---|
| `image.repository`, `image.tag`, `image.digest` | `piperoom`, `pilot`, empty | image to run; digest wins |
| `replicaCount` | `1` | pods; more than 1 is safe but adds no write capacity, see above |
| `rollingUpdate.maxSurge`, `.maxUnavailable` | `1`, `0` | rollout with more than one replica |
| `podDisruptionBudget.enabled`, `.minAvailable` | `true`, `1` | PDB, rendered only with more than one replica |
| `env.PIPEROOM_MAX_AGENTS`, `env.PIPEROOM_MAX_AGENTS_GLOBAL` | empty (3 per replica, no global cap) | agent cost caps |
| `secrets.existingSecret` | `piperoom-secrets` | Secret with the env-named keys |
| `secrets.requiredKeys` / `optionalKeys` / `refs` | see values.yaml | which variables come from Secrets |
| `env.*` | see values.yaml | every plain server variable; `""` = server default |
| `env.PIPEROOM_AUTH_MODE` | `oidc` | `dev` only for a private tunnel |
| `env.PIPEROOM_PUBLIC_URL` | derived from ingress | https URL users open |
| `files.owners`, `files.users`, `files.existingConfigMap` | empty | owners.yaml / users.yaml |
| `ingress.enabled`, `.host`, `.className`, `.tlsSecretName`, `.annotations` | off | optional Ingress |
| `networkPolicy.enabled`, `.ingressFrom`, `.egress.*`, `.extraEgress` | off | traffic limits |
| `resources` | 100m / 256Mi, memory limit 1Gi | pod resources |
| `extraVolumes`, `extraVolumeMounts` | empty | TLS files, cloud key files |
| `serviceAccount.annotations` | empty | IRSA / workload identity |

## Upgrade

```sh
helm upgrade piperoom deploy/helm/piperoom -n piperoom -f my-values.yaml \
  --set-file files.owners=owners.yaml --set-file files.users=users.yaml
```

Pass the same `-f`/`--set-file` inputs every time. A changed owners/users ConfigMap restarts the
pod (checksum annotation). The server applies database migrations at start, one pod at a time
(an advisory lock). With one replica the Deployment is `Recreate`, so there is a short gap during an
upgrade; with more it rolls one pod at a time with no gap, and old and new pods share the database
for a moment, so a release whose migrations the old version cannot run against needs
`replicaCount: 1` for that upgrade (its notes say so). Roll back with `helm rollback piperoom <rev>`
(migrations are not undone).

## Checks

`deploy/helm/piperoom/ci/` has two values files: `oidc-values.yaml` (a valid production-style
setup, also a good starting point for your own values) and `dev-values.yaml` (the throwaway
passwordless try-out). The chart does not render with its bare defaults, because oidc mode needs
a public URL, an issuer and a client id.

```sh
for f in oidc dev; do
  helm lint deploy/helm/piperoom -f deploy/helm/piperoom/ci/$f-values.yaml
  helm template piperoom deploy/helm/piperoom -f deploy/helm/piperoom/ci/$f-values.yaml | kubeconform -strict -summary
done
# several replicas: RollingUpdate and a PodDisruptionBudget
helm template piperoom deploy/helm/piperoom -f deploy/helm/piperoom/ci/oidc-values.yaml --set replicaCount=3 \
  | grep -E 'kind: PodDisruptionBudget|type: RollingUpdate'
```
