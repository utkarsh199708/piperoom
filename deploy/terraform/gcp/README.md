# Piperoom pilot on GCP (Terraform)

A private pilot in one VPC of one project:

```
 people ──HTTPS──► Global HTTPS load balancer ──(Cloud Armor allow-list, optional)
                    managed cert, HTTP→HTTPS redirect, WebSocket timeout
                              │  :8090, health check GET /healthz
                              ▼
   IAP ──SSH:22──► VM (no public IP)  e2-standard-2, Debian 12 + Docker
                    runs deploy/docker-compose.pilot.yml (server only)
                       │  secrets from Secret Manager at boot
                       ├──► Cloud SQL Postgres 16 (private IP only, backups on)
                       └──► Cloud NAT (one fixed IP) ──► model API / IdP / your pipeline
```

What this does **not** do: it does not create your pipeline (source Postgres, Kafka,
Connect, Schema Registry, warehouse), and it does not connect the VPC to it. Those are reached
over the internet from the NAT address in the `nat_egress_ip` output, so allow-list that address
at the pipeline. For private connectivity (VPN, Interconnect, peering) add it to this VPC
yourself. Read `docs/pilot/permissions.md` first and create the read-only accounts it lists.

Everything is ordinary Terraform (provider `hashicorp/google` `~> 6.50`, Terraform 1.5 or newer;
`.terraform.lock.hcl` is committed). **Not tested against a real project**: it passes
`terraform validate`, `fmt` and `tflint`, but no `apply` or `plan` was run (see
[Verification status](#verification-status)).

## Files

| File | Holds |
|---|---|
| `main.tf` | APIs, VPC, subnet, Cloud NAT, firewall rules, Cloud SQL, VM service account and its roles |
| `secrets.tf` | Secret Manager secrets (no versions) and the per-secret accessor bindings |
| `vm.tf` | the VM, its metadata, the unmanaged instance group |
| `lb.tf` | load balancer, managed certificate, health check, redirect, Cloud Armor policy |
| `startup.sh` | boot script: installs Docker, fetches secrets, writes the env file, starts the stack |
| `variables.tf`, `outputs.tf`, `terraform.tfvars.example` | inputs and outputs |

## Prerequisites

- A **dedicated GCP project** with billing, and an account that can create the resources above
  (for a trial: `roles/owner` on that project).
- `gcloud` (logged in: `gcloud auth login`, `gcloud auth application-default login`) and
  Terraform 1.5 or newer. Docker, to build the image.
- A **domain name** you control (a DNS A record is created by hand, step 7).
- An **OIDC client** at your identity provider with redirect URI `https://<domain>/auth/callback`
  (see `deploy/README.md`, "Single sign-on (OIDC)"). The stack always runs in `oidc` mode: the
  dev login has no password, so it is never used behind a public load balancer.
- `owners.yaml` and `users.yaml` for your team (start from `deploy/config/*.example.yaml`).
- If you use `model_provider = "vertex"`: the Claude model enabled for your project in Vertex AI
  Model Garden, in the region you choose. If `anthropic`: an API key.
- To SSH later: `roles/iap.tunnelResourceAccessor` and `roles/compute.osLogin` (or
  `osAdminLogin` for sudo) for each person, plus `roles/iam.serviceAccountUser` on the VM service
  account if they need sudo.

## Step by step

### 1. Configure

```bash
cd deploy/terraform/gcp
cp terraform.tfvars.example terraform.tfvars   # edit: project_id, domain, image, OIDC, provider
gcloud config set project <project_id>
```

### 2. Build and push the image

The VM does not build from source; it pulls an image. From the repository root:

```bash
gcloud services enable artifactregistry.googleapis.com
gcloud artifacts repositories create piperoom --repository-format=docker --location=us-central1
gcloud auth configure-docker us-central1-docker.pkg.dev
docker build -t us-central1-docker.pkg.dev/<project_id>/piperoom/piperoom:pilot .
docker push  us-central1-docker.pkg.dev/<project_id>/piperoom/piperoom:pilot
```

Put that name in `piperoom_image` (a `@sha256:` digest is better than a moving tag). The VM
service account gets `roles/artifactregistry.reader` by default
(`grant_artifact_registry_reader`).

### 3. Apply

```bash
terraform init
terraform plan -out tfplan      # read it
terraform apply tfplan
```

The first apply takes 10 to 20 minutes (Cloud SQL and the private-services peering are the slow
parts). **The VM boots and its start-up script stops with "cannot read the latest version of
secret" until you add the secret values (step 5). That is expected.**

### 4. Create the database user

Terraform creates the instance and the empty `piperoom` database, but not a user, because a user
resource would put its password into the state. Pick a hex password (no special characters, so it
is safe inside a URL) and keep it only in your password manager:

```bash
PGPW=$(openssl rand -hex 24)
gcloud sql users create piperoom --instance "$(terraform output -raw cloudsql_instance)" --password "$PGPW"
```

The server runs its own migrations at start, so the `piperoom` user must own the database. Give
yourself a way in once and hand ownership over (the VM has `psql` installed by the start-up
script):

```bash
gcloud sql users set-password postgres --instance "$(terraform output -raw cloudsql_instance)" --prompt-for-password
# then, from the VM (step 8 shows how to SSH):  psql "host=<cloudsql_private_ip> user=postgres dbname=piperoom sslmode=require"
#   GRANT piperoom TO postgres;  ALTER DATABASE piperoom OWNER TO piperoom;  GRANT ALL ON SCHEMA public TO piperoom;
```

(This ownership step was written from the Cloud SQL docs, not run. If `ALTER DATABASE ... OWNER`
is refused, `GRANT ALL ON SCHEMA public TO piperoom;` alone is usually enough on Postgres 16.)

### 5. Set the secret values

Terraform created these secrets **empty** (`terraform output secret_ids`). Add one version each,
reading from stdin so nothing lands in shell history or a file:

```bash
P=my-piperoom-pilot   # your project id
# 1. Room database URL (shape: terraform output db_url_template)
printf '%s' "postgres://piperoom:$PGPW@$(terraform output -raw cloudsql_private_ip):5432/piperoom?sslmode=require" \
  | gcloud secrets versions add piperoom-db-url --project $P --data-file=-

# 2. OIDC client secret (type it; Ctrl-D to finish)
gcloud secrets versions add piperoom-oidc-client-secret --project $P --data-file=-

# 3. Only when model_provider = "anthropic"
gcloud secrets versions add piperoom-anthropic-api-key --project $P --data-file=-

# 4. Who owns which layer, and who may log in
gcloud secrets versions add piperoom-owners-yaml --project $P --data-file=owners.yaml
gcloud secrets versions add piperoom-users-yaml  --project $P --data-file=users.yaml

# 5. Pipeline settings: KEY=VALUE lines, same rules as deploy/pilot.env.example
gcloud secrets versions add piperoom-pipeline-env --project $P --data-file=pipeline.env
```

`pipeline.env` holds `SOURCE_DB_URL`, `WAREHOUSE_DB_URL`, `KAFKA_BROKERS`, `CONNECT_URL`,
`SCHEMA_REGISTRY_URL`, the Kafka/Connect/Registry logins, Slack and alert secrets. Settings that
this stack manages itself (auth mode, public URL, OIDC id, database URL, bind, proxies, provider,
model, Vertex project) are dropped from it with a log line, so the secret cannot weaken them.
Use `--data-file=-` with a here-doc instead of a file if you do not want `pipeline.env` on disk.
Delete the local copies afterwards.

Value rules: the single-line secrets (`db-url`, `oidc-client-secret`, `anthropic-api-key`) must
not contain a single quote or a newline; `$`, `#` and spaces are fine.

### 6. Start (or restart) the stack

The boot script reads the secrets, so run it again now that they exist. Either reset the VM:

```bash
gcloud compute instances reset "$(terraform output -raw vm_name)" --zone "$(terraform output -raw vm_zone)"
```

or SSH in (step 8) and run `sudo google_metadata_script_runner startup`. Use the same command
whenever you change a secret value (secrets are read at start, not live).

### 7. DNS and the certificate

```bash
terraform output dns_a_record
```

Create that A record at your DNS provider (TTL 300 is fine). Google issues the managed certificate
once the name resolves to the load balancer; this takes from 10 minutes to about an hour. Check:

```bash
gcloud compute ssl-certificates list --global      # MANAGED_STATUS: PROVISIONING -> ACTIVE
curl -sS -o /dev/null -w '%{http_code}\n' https://<domain>/healthz   # 200
```

Until the certificate is active, HTTPS fails with a TLS error. Then open `https://<domain>` and
sign in. If you set `allowed_cidrs`, test from an allowed address; others get 403.

### 8. SSH (through IAP only)

```bash
$(terraform output -raw iap_ssh_command)
# which is:  gcloud compute ssh piperoom-vm --zone <zone> --project <project> --tunnel-through-iap
```

On the VM:

```bash
cd /opt/piperoom
sudo docker compose -f docker-compose.pilot.yml -f docker-compose.override.yml ps
sudo docker compose -f docker-compose.pilot.yml -f docker-compose.override.yml logs --tail 100 piperoom
sudo journalctl -u google-startup-scripts.service --no-pager | tail -50      # boot script output
```

Container logs are also in Cloud Logging (resource type "gce_instance", log `gcplogs-docker-driver`).

### Upgrading

Push a new image, change `piperoom_image`, `terraform apply` (this updates the VM's metadata in
place and does not restart it), then run `sudo google_metadata_script_runner startup` on the VM:
it pulls the image and recreates the container. The same command picks up a re-pushed tag.

### Destroy

```bash
# 1. Allow deletion of Cloud SQL (flips both guards), and wait for it to apply.
terraform apply -var db_deletion_protection=false
# 2. (Optional) export anything you want to keep: GET /api/rooms/{id}/export, or a Cloud SQL export.
terraform destroy -var db_deletion_protection=false
```

`gcloud secrets` values are deleted with their secrets. The managed Google APIs are left enabled
(`disable_on_destroy = false`). The Artifact Registry repository from step 2 and any DNS record
were created by hand and are yours to remove. Cloud SQL automated backups are deleted with the
instance. If destroy stops on the private-services peering, run it again; the connection is set
to be abandoned, and the peering disappears with the VPC.

## Variables you will most likely touch

All variables have descriptions and validation in `variables.tf`.

| Variable | Default | Purpose |
|---|---|---|
| `project_id`, `domain`, `piperoom_image`, `oidc_issuer`, `oidc_client_id` | required | where, address, image, login |
| `region` | `us-central1` | everything except the global load balancer |
| `model_provider` | `vertex` | `vertex` uses the VM's service account; `anthropic` uses the API-key secret |
| `model`, `vertex_region` | `null` | `PIPEROOM_MODEL`, `VERTEX_REGION` (null region = `region`) |
| `allowed_cidrs` | `[]` | non-empty creates a Cloud Armor allow-list (default deny, 403) |
| `backend_timeout_sec` | `3600` | longest WebSocket lifetime through the load balancer |
| `db_deletion_protection` | `true` | Cloud SQL guard (Terraform and API) |
| `db_availability_type` | `ZONAL` | `REGIONAL` = high availability, about double the SQL compute cost |
| `extra_env` | `{}` | extra **non-secret** server settings |

Using Vertex means no key anywhere: the VM service account is the credential, found through the
metadata server by Google application default credentials. The base `VERTEX_PROJECT_ID` is the
stack's project. `global`, `us` and `eu` as `vertex_region` are multi-region endpoints; choose a
single region if data must stay in one place.

## IAM granted to the VM service account (and why)

| Role | Scope | Why | Condition |
|---|---|---|---|
| `roles/secretmanager.secretAccessor` | each of this stack's secrets only | read values at boot; cannot read other secrets in the project | always |
| `roles/logging.logWriter` | project | Docker `gcplogs` driver and the VM write logs | always |
| `roles/monitoring.metricWriter` | project | Ops Agent host metrics | `install_ops_agent` |
| `roles/aiplatform.user` | project | call Claude on Vertex AI (prediction only; no model admin, no data access) | `model_provider = vertex` |
| `roles/artifactregistry.reader` | project | pull the image | `grant_artifact_registry_reader` |

Not granted: `roles/cloudsql.client` (the VM connects to the private IP with password and TLS,
not through the Cloud SQL connector or proxy; add it if you switch to the proxy), any compute or
storage admin, and anything on the pipeline. The VM's OAuth scope is `cloud-platform`, which is
only the outer limit; the roles above are what count.

## Security notes

- **No public IP on the VM.** Its only ingress is IAP SSH (`35.235.240.0/20`, port 22) and the
  Google load-balancer/health-check ranges on port 8090 (`130.211.0.0/22`, `35.191.0.0/16`).
  Nothing else gets in. Egress is Cloud NAT on one reserved address. SSH uses OS Login; project
  and instance SSH keys are blocked. The VM is a Shielded VM.
- The server listens on `0.0.0.0:8090` inside the VPC over plain HTTP, as `deploy/README.md`
  allows for oidc mode behind a TLS proxy. The load balancer terminates TLS (TLS 1.2 or newer).
  `PIPEROOM_TRUSTED_PROXIES` is set to the Google front-end ranges plus the load balancer's own
  address, so the server sees real client addresses for its login rate limit.
- Cloud SQL has no public IP, refuses non-TLS connections (`ENCRYPTED_ONLY`), has daily backups
  and point-in-time recovery. Its server certificate is signed by a per-instance CA, so the URL
  uses `sslmode=require` (encrypted, server identity not verified). Verifying it needs the
  instance CA in the container, which the compose file does not mount; the connection is private
  to the VPC either way.
- The env file `/opt/piperoom/.env` is `0600 root`. The compose file hands the container only the
  variables it lists, so the secrets reach the server process and nothing else. Anyone who can
  SSH with sudo can read it: grant `osAdminLogin` sparingly.
- **Alerts** (`POST /api/alerts/<source>`) arrive through the same load balancer. With
  `allowed_cidrs` set, your alert senders' addresses must be in it, or their posts get 403.
- The load balancer logs 10% of requests to Cloud Logging. Request URLs and client addresses are
  in those logs.
- Cloud Armor is not a login: the OIDC login and the signed alert routes are the authentication.

### What ends up in Terraform state

Secret **values** never do: the secrets have no Terraform-managed versions, the OIDC client
secret and database password are not variables, and no `google_sql_user` is declared. The state
does contain: the project id, resource names and IPs, the Cloud SQL private IP, the full text of
the start-up script and the compose file (instance metadata), the non-secret settings (public
URL, OIDC issuer and client id, Vertex project and region, `extra_env`) and Secret Manager
secret *ids* (not contents). Treat the state as sensitive anyway, keep it in a private versioned
GCS bucket (the commented `backend "gcs"` block in `versions.tf`), and do not put secrets in
`extra_env`.

Also visible to anyone with `compute.instances.get`: that same instance metadata. It holds
no secret.

The secret values do exist in three places you control: Secret Manager (encrypted at rest,
audit-logged), `/opt/piperoom/.env` on the VM, and the container's environment.

## Monthly cost estimate (approximate)

Region **us-central1**, on-demand list prices as I know them, **estimated 2026-10-13**, USD, 730
hours a month, default variables, zonal Cloud SQL, no committed-use or sustained-use discounts.
Prices were not read from the live Google price list; treat the total as plus or minus 20% and
check the [pricing calculator](https://cloud.google.com/products/calculator) for your region
before you commit.

| Item | Basis | $/month |
|---|---|---|
| VM `e2-standard-2` | 2 vCPU, 8 GB, about $0.067/h | 49 |
| VM boot disk | 30 GB pd-balanced, $0.10/GB | 3 |
| Cloud SQL `db-custom-1-3840`, zonal | 1 vCPU + 3.75 GB | 49 |
| Cloud SQL storage + backups | 20 GB SSD ($0.17/GB) + a few GB of backups | 5 |
| Load balancer forwarding rules | first 5 rules, $0.025/h in total | 18 |
| Load balancer + NAT static IPs in use | 2 × about $3.65 | 7 |
| Load balancer data processing | about 10 GB at $0.008 to $0.012/GB | 1 |
| Cloud NAT | gateway about $1/VM + data $0.045/GB for about 20 GB | 2 |
| Secret Manager | 6 secrets × $0.06 + access operations | 1 |
| Logging, monitoring | inside the free allowance at pilot volume | 0 |
| IAP, private services access, managed certificate | no charge | 0 |
| **Total (infrastructure)** | | **about 135** |

Variations: `db_availability_type = "REGIONAL"` adds about 50. Cloud Armor (`allowed_cidrs`) adds
about 6 to 8 (policy $5 + $1 per rule + $0.75 per million requests). Egress to the internet is
$0.12/GB after the free allowance, small here. **Model usage is not included**: Vertex AI or the
Anthropic API is billed per token, `PIPEROOM_ROOM_TOKEN_BUDGET` caps a room, and
`PIPEROOM_MAX_AGENTS` caps parallel runs. A quieter trial can cut cost by using a smaller VM
(`e2-medium`, about 25 less) and `db-g1-small` (about 25 less).

## Verification status

Done on the machine that wrote this: `terraform fmt -check -recursive`, `terraform init
-backend=false`, `terraform validate` and `tflint` (default `terraform` ruleset, recommended
preset), `bash -n startup.sh`. **Not done**: `terraform plan` or `apply` against a real project,
running `startup.sh` on a Debian VM, and a Cloud Armor, certificate or WebSocket test through the
real load balancer. Expect to fix small things on the first real apply; report them.

Known limits:

- The boot script installs Docker and the Ops Agent from the internet (Docker apt repository,
  `dl.google.com`) through Cloud NAT, so the first boot needs working egress; a restricted
  egress policy needs an internal mirror. The Ops Agent install script is downloaded and run as
  root; set `install_ops_agent = false` to skip it.
- One VM, one zone: no high availability for the application. A VM restart means about a minute
  of downtime; the room state is in Cloud SQL and survives.
- Database user, ownership grant and secret values are manual steps by design (they keep
  passwords out of state).
- The `google` TFLint ruleset is not enabled (it could not be downloaded in the build
  sandbox); only the bundled `terraform` rules ran.
