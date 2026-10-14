# ---- Project and naming ------------------------------------------------------

variable "project_id" {
  description = "GCP project that will hold the pilot. Use a dedicated project."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "project_id must be a valid GCP project id (6-30 chars, lower case, digits, hyphens)."
  }
}

variable "region" {
  description = "Region for the VM, Cloud SQL, subnet and NAT. The load balancer is global."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Zone for the VM. null means <region>-a."
  type        = string
  default     = null
}

variable "name_prefix" {
  description = "Prefix for every resource name. Lower case letters, digits and hyphens."
  type        = string
  default     = "piperoom"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,20}$", var.name_prefix))
    error_message = "name_prefix must start with a letter and be 2-21 chars of a-z, 0-9, hyphen."
  }
}

variable "labels" {
  description = "Extra labels for resources that support them."
  type        = map(string)
  default     = {}
}

variable "enable_apis" {
  description = "Enable the Google APIs this stack needs. Set false if the project already manages them."
  type        = bool
  default     = true
}

# ---- Public address and sign-in ----------------------------------------------

variable "domain" {
  description = "Fully qualified host name people use, for example piperoom.corp.example. You create the DNS A record (see README); the Google-managed certificate is issued for this name."
  type        = string

  validation {
    condition     = can(regex("^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\\.)+[a-z]{2,}$", var.domain))
    error_message = "domain must be a lower-case fully qualified domain name, without scheme or path."
  }
}

variable "oidc_issuer" {
  description = "OIDC issuer URL, exactly as in your identity provider's discovery document. The stack always runs in oidc mode (dev login has no password)."
  type        = string

  validation {
    condition     = can(regex("^https://", var.oidc_issuer))
    error_message = "oidc_issuer must be an https URL."
  }
}

variable "oidc_client_id" {
  description = "OIDC client id registered for Piperoom. The redirect URI to register is https://<domain>/auth/callback. The client SECRET is not a variable: store it in Secret Manager (see README)."
  type        = string

  validation {
    condition     = length(var.oidc_client_id) > 0
    error_message = "oidc_client_id must not be empty."
  }
}

variable "allowed_cidrs" {
  description = "Optional Cloud Armor allow-list: only these source CIDRs may reach the load balancer; everything else gets 403. Empty list means no Cloud Armor policy. Alert senders (Alertmanager, PagerDuty ...) must be in the list too."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for c in var.allowed_cidrs : can(cidrnetmask(c)) || can(regex("^[0-9a-fA-F:]+/[0-9]{1,3}$", c))])
    error_message = "Every allowed_cidrs entry must be a CIDR such as 203.0.113.0/24."
  }
}

variable "backend_timeout_sec" {
  description = "Load balancer backend timeout. For WebSockets this is the longest a socket may stay open, so keep it high; the browser reconnects and replays after a close."
  type        = number
  default     = 3600

  validation {
    condition     = var.backend_timeout_sec >= 30 && var.backend_timeout_sec <= 86400
    error_message = "backend_timeout_sec must be between 30 and 86400."
  }
}

# ---- Network ------------------------------------------------------------------

variable "subnet_cidr" {
  description = "CIDR of the VM subnet."
  type        = string
  default     = "10.10.0.0/24"

  validation {
    condition     = can(cidrnetmask(var.subnet_cidr))
    error_message = "subnet_cidr must be an IPv4 CIDR."
  }
}

variable "private_services_prefix_length" {
  description = "Prefix length of the range reserved for private services access (Cloud SQL). Google allocates it from free space in the VPC."
  type        = number
  default     = 20

  validation {
    condition     = var.private_services_prefix_length >= 16 && var.private_services_prefix_length <= 24
    error_message = "private_services_prefix_length must be between 16 and 24."
  }
}

# ---- VM -----------------------------------------------------------------------

variable "machine_type" {
  description = "Machine type of the Piperoom VM."
  type        = string
  default     = "e2-standard-2"
}

variable "boot_disk_gb" {
  description = "Boot disk size in GB (holds the Docker images and logs)."
  type        = number
  default     = 30

  validation {
    condition     = var.boot_disk_gb >= 20
    error_message = "boot_disk_gb must be at least 20."
  }
}

variable "vm_image" {
  description = "Name of the Debian 12 boot image in project debian-cloud, for example debian-12-bookworm-v20260101 (a NAMED image, not the moving debian-12 family, so a re-apply never swaps the OS under the VM). Look up a current name with: gcloud compute images list --project debian-cloud --no-standard-images --filter=\"family=debian-12\" --sort-by=~creationTimestamp --limit=3 . Changing it recreates the boot disk."
  type        = string

  validation {
    condition     = can(regex("^debian-12-bookworm-v[0-9]{8}$", var.vm_image))
    error_message = "vm_image must be a named Debian 12 image such as debian-12-bookworm-v20260101 (not a family)."
  }
}

variable "piperoom_image" {
  description = "Container image the VM runs, PINNED BY DIGEST, for example europe-west4-docker.pkg.dev/<project>/piperoom/piperoom@sha256:<64 hex>. The VM does not build from source: build and push the repository Dockerfile first and copy the digest from the push output (README step 2). A tag is refused because it can move and the VM would run whatever it points to at the next boot."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-zA-Z0-9._/:-]*@sha256:[0-9a-f]{64}$", var.piperoom_image))
    error_message = "piperoom_image must be a registry path pinned by digest: <registry>/<path>@sha256:<64 lower-case hex>. Tags are not accepted."
  }
}

variable "grant_artifact_registry_reader" {
  description = "Give the VM service account roles/artifactregistry.reader on the ONE Artifact Registry repository that holds piperoom_image (the repository is taken from the image path, which must be <location>-docker.pkg.dev/<project>/<repo>/...). Set false if the image is public, in gcr.io or elsewhere, or the repository has its own IAM binding."
  type        = bool
  default     = true
}

# ---- Pinned packages on the VM -------------------------------------------------
# startup.sh installs these exact apt versions from the Docker repository and holds
# them. To upgrade, change the value (check: apt-cache madison docker-ce on the VM
# or https://download.docker.com/linux/debian/dists/bookworm/stable/binary-amd64/Packages)
# and re-run the start-up script on a new VM. The defaults are the ones tested with
# docker-compose.pilot.yml; compose must stay at 2.24 or newer for "!reset".

variable "docker_version" {
  description = "Exact apt version of docker-ce and docker-ce-cli."
  type        = string
  default     = "5:29.8.2-1~debian.12~bookworm"

  validation {
    condition     = can(regex("^[0-9]+:[0-9][0-9A-Za-z.+~-]*$", var.docker_version))
    error_message = "docker_version must be an exact apt version such as 5:29.8.2-1~debian.12~bookworm."
  }
}

variable "containerd_version" {
  description = "Exact apt version of containerd.io."
  type        = string
  default     = "2.3.6-1~debian.12~bookworm"

  validation {
    condition     = can(regex("^[0-9][0-9A-Za-z.+~-]*$", var.containerd_version))
    error_message = "containerd_version must be an exact apt version such as 2.3.6-1~debian.12~bookworm."
  }
}

variable "docker_compose_plugin_version" {
  description = "Exact apt version of docker-compose-plugin (must be 2.24 or newer; startup.sh checks)."
  type        = string
  default     = "5.5.1-1~debian.12~bookworm"

  validation {
    condition     = can(regex("^[0-9][0-9A-Za-z.+~-]*$", var.docker_compose_plugin_version))
    error_message = "docker_compose_plugin_version must be an exact apt version such as 5.5.1-1~debian.12~bookworm."
  }
}

variable "ops_agent_repo_suffix" {
  description = "REPO_SUFFIX for the Ops Agent install script: the major version whose apt repository is used (\"2\" = google-cloud-ops-agent-bookworm-2, which only holds 2.x releases)."
  type        = string
  default     = "2"

  validation {
    condition     = can(regex("^[0-9]+$", var.ops_agent_repo_suffix))
    error_message = "ops_agent_repo_suffix must be a major version number such as 2."
  }
}

variable "install_ops_agent" {
  description = "Install the Google Cloud Ops Agent on the VM for host metrics (needs roles/monitoring.metricWriter). Container logs go to Cloud Logging either way."
  type        = bool
  default     = true
}

# ---- Model provider -----------------------------------------------------------

variable "model_provider" {
  description = "Where the agent's model runs: \"anthropic\" (API key from Secret Manager) or \"vertex\" (Vertex AI using the VM's service account, no key)."
  type        = string
  default     = "vertex"

  validation {
    condition     = contains(["anthropic", "vertex"], var.model_provider)
    error_message = "model_provider must be \"anthropic\" or \"vertex\"."
  }
}

variable "model" {
  description = "PIPEROOM_MODEL. REQUIRED when model_provider is vertex (checked at plan time): a Vertex model id from Model Garden. For anthropic, null keeps the server default."
  type        = string
  default     = null
}

variable "vertex_region" {
  description = "VERTEX_REGION. REQUIRED when model_provider is vertex (checked at plan time): a region where your Claude model is served (see Model Garden), or global/us/eu (multi-region endpoints: the request may be served anywhere in that area). There is no default because var.region may not serve the model."
  type        = string
  default     = null
}

# ---- Cloud SQL ----------------------------------------------------------------

variable "db_tier" {
  description = "Cloud SQL machine tier (ENTERPRISE edition)."
  type        = string
  default     = "db-custom-1-3840"
}

variable "db_disk_gb" {
  description = "Initial Cloud SQL SSD size in GB. It grows automatically."
  type        = number
  default     = 20

  validation {
    condition     = var.db_disk_gb >= 10
    error_message = "db_disk_gb must be at least 10."
  }
}

variable "db_availability_type" {
  description = "ZONAL (cheaper) or REGIONAL (high availability, roughly doubles the instance cost)."
  type        = string
  default     = "ZONAL"

  validation {
    condition     = contains(["ZONAL", "REGIONAL"], var.db_availability_type)
    error_message = "db_availability_type must be ZONAL or REGIONAL."
  }
}

variable "db_deletion_protection" {
  description = "Block deletion of the Cloud SQL instance (both the Terraform guard and the API flag). Set false and apply once before terraform destroy."
  type        = bool
  default     = true
}

variable "db_backup_start_time" {
  description = "Daily backup window start, UTC, HH:MM."
  type        = string
  default     = "02:00"

  validation {
    condition     = can(regex("^([01][0-9]|2[0-3]):[0-5][0-9]$", var.db_backup_start_time))
    error_message = "db_backup_start_time must look like 02:00."
  }
}

variable "db_retained_backups" {
  description = "How many automated backups to keep."
  type        = number
  default     = 7

  validation {
    condition     = var.db_retained_backups >= 1 && var.db_retained_backups <= 365
    error_message = "db_retained_backups must be between 1 and 365."
  }
}

# ---- Extra server settings ----------------------------------------------------

variable "extra_env" {
  description = "Additional NON-secret server settings as NAME => value, for example PIPEROOM_WATCH_MODE = \"log\". They are stored in VM metadata (readable by anyone with compute.instances.get) and in Terraform state. Put secrets and pipeline URLs with passwords in the pipeline-env secret instead. Only names listed in docker-compose.pilot.yml reach the container."
  type        = map(string)
  default     = {}

  validation {
    condition     = alltrue([for k, v in var.extra_env : can(regex("^[A-Z][A-Z0-9_]*$", k)) && !can(regex("[\\n\\r]", v))])
    error_message = "extra_env names must be UPPER_SNAKE_CASE and values must be single-line."
  }

  validation {
    condition = length(setintersection(toset(keys(var.extra_env)), toset([
      "PIPEROOM_DB_URL", "PIPEROOM_AUTH_MODE", "PIPEROOM_PUBLIC_URL", "PIPEROOM_OIDC_ISSUER",
      "PIPEROOM_OIDC_CLIENT_ID", "PIPEROOM_OIDC_CLIENT_SECRET", "PIPEROOM_MODEL_PROVIDER", "PIPEROOM_MODEL",
      "PIPEROOM_BIND", "PIPEROOM_PORT", "PIPEROOM_HTTP_ADDR", "PIPEROOM_DEV_ALLOW_NONLOOPBACK", "PIPEROOM_TRUSTED_PROXIES", "VERTEX_PROJECT_ID", "VERTEX_REGION",
      "ANTHROPIC_API_KEY", "PIPEROOM_POSTGRES_PASSWORD",
    ]))) == 0
    error_message = "extra_env must not set a variable that this stack manages itself (database, auth, public URL, bind, proxies, model provider, keys)."
  }
}
