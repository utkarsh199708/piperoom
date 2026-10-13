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

- **Auth** defaults to `PIPEROOM_AUTH_MODE=oidc`. The image sets
  `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1`; in oidc mode the server ignores it and logs a warning at
  every start (expected). oidc needs an https `PIPEROOM_PUBLIC_URL`; the chart derives it from
  `ingress.host` when `ingress.tlsSecretName` is set.
- **Probes** call `/healthz` (liveness) and `/readyz` (readiness, also pings the room database)
  with `Host: localhost`. The server checks the Host header on every route including probes, and
  kubelet would otherwise send the pod IP and get 421.
- **One replica.** `replicaCount` defaults to 1 and the Deployment strategy is `Recreate`. The agent
  runner, watcher, retention sweep and action executor run in every server process and the code has
  no leader election or lock, so a second replica would duplicate investigations, watcher checks
  and approved fixes. Do not scale up until that changes.
- **Security context**: non-root (uid 65532), read-only root file system, all capabilities dropped,
  seccomp `RuntimeDefault`. `/tmp` is a small in-memory emptyDir; `/config` is the read-only
  owners/users ConfigMap. Kafka/Connect TLS files: mount a Secret with `extraVolumes` and
  `extraVolumeMounts` and point `KAFKA_TLS_*_FILE` at it.
- **Ingress**: default annotations raise the ingress-nginx proxy timeouts so WebSockets stay up
  (nginx upgrades the connection itself). Other controllers need their own settings.
- **NetworkPolicy** (`networkPolicy.enabled=true`): ingress only on 8090 from
  `networkPolicy.ingressFrom` (default: namespace `ingress-nginx`); egress to kube-dns and to the
  ports for Postgres 5432, Kafka 9092/9093, Connect 8083, Schema Registry 8081 and HTTPS 443
  (IdP, model API, Slack). Those groups have an empty `to`, meaning any address on that port; set
  `to` to your CIDRs or selectors, and change `ports` if your services listen elsewhere. Add
  other rules with `extraEgress`.
- Setting `owners`/`users` in values stores them in a ConfigMap, so keep real user data in a
  ConfigMap you manage (`files.existingConfigMap`) if you do not want it in the release.

## Important values

| Value | Default | Meaning |
|---|---|---|
| `image.repository`, `image.tag`, `image.digest` | `piperoom`, `pilot`, empty | image to run; digest wins |
| `replicaCount` | `1` | keep 1, see above |
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
pod (checksum annotation). The server applies database migrations at start; the Deployment is
`Recreate`, so there is a short gap during an upgrade. Roll back with `helm rollback piperoom <rev>`
(migrations are not undone).

## Checks

```sh
helm lint deploy/helm/piperoom
helm template piperoom deploy/helm/piperoom | kubeconform -strict -summary
helm template piperoom deploy/helm/piperoom --set ingress.enabled=true --set networkPolicy.enabled=true | kubeconform -strict -summary
```
