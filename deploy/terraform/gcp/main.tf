locals {
  zone   = coalesce(var.zone, "${var.region}-a")
  vertex = var.model_provider == "vertex"
  labels = merge({ app = "piperoom", managed-by = "terraform" }, var.labels)

  # Google front ends: health-check probes and the load balancer's proxies reach
  # the VM from these ranges (https://cloud.google.com/load-balancing/docs/firewall-rules).
  google_lb_ranges = ["130.211.0.0/22", "35.191.0.0/16"]
  iap_range        = "35.235.240.0/20"

  # <location>-docker.pkg.dev/<project>/<repository>/<image>@sha256:... -> its parts,
  # or null when the image is not in Artifact Registry.
  ar_parts = regexall("^([a-z0-9-]+)-docker\\.pkg\\.dev/([^/]+)/([^/]+)/", var.piperoom_image)
  ar_repo = length(local.ar_parts) == 1 ? {
    location   = local.ar_parts[0][0]
    project    = local.ar_parts[0][1]
    repository = local.ar_parts[0][2]
  } : null

  server_port = 8090
  vm_tag      = "${var.name_prefix}-vm"

  apis = toset(concat(
    [
      "compute.googleapis.com",
      "iam.googleapis.com",
      "iap.googleapis.com",
      "logging.googleapis.com",
      "monitoring.googleapis.com",
      "secretmanager.googleapis.com",
      "servicenetworking.googleapis.com",
      "sqladmin.googleapis.com",
    ],
    local.vertex ? ["aiplatform.googleapis.com"] : [],
    var.grant_artifact_registry_reader ? ["artifactregistry.googleapis.com"] : [],
  ))
}

resource "google_project_service" "apis" {
  for_each = var.enable_apis ? local.apis : toset([])

  service            = each.value
  disable_on_destroy = false
}

# ---- Network -----------------------------------------------------------------

resource "google_compute_network" "vpc" {
  name                    = "${var.name_prefix}-vpc"
  auto_create_subnetworks = false

  depends_on = [google_project_service.apis]
}

resource "google_compute_subnetwork" "vm" {
  name                     = "${var.name_prefix}-vm"
  region                   = var.region
  network                  = google_compute_network.vpc.id
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true
}

# Egress for the VM (image pulls, model API, IdP, your pipeline) goes through
# Cloud NAT on ONE reserved address, so you can allow-list it at the pipeline.
resource "google_compute_address" "nat" {
  name         = "${var.name_prefix}-nat"
  region       = var.region
  address_type = "EXTERNAL"
  labels       = local.labels

  depends_on = [google_project_service.apis]
}

resource "google_compute_router" "nat" {
  name    = "${var.name_prefix}-router"
  region  = var.region
  network = google_compute_network.vpc.id
}

resource "google_compute_router_nat" "nat" {
  name                               = "${var.name_prefix}-nat"
  region                             = var.region
  router                             = google_compute_router.nat.name
  nat_ip_allocate_option             = "MANUAL_ONLY"
  nat_ips                            = [google_compute_address.nat.self_link]
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

# SSH only through Identity-Aware Proxy.
resource "google_compute_firewall" "iap_ssh" {
  name      = "${var.name_prefix}-allow-iap-ssh"
  network   = google_compute_network.vpc.name
  direction = "INGRESS"
  priority  = 1000

  source_ranges = [local.iap_range]
  target_tags   = [local.vm_tag]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

# Load balancer traffic and health checks, on the server port only.
resource "google_compute_firewall" "lb_to_server" {
  name      = "${var.name_prefix}-allow-lb"
  network   = google_compute_network.vpc.name
  direction = "INGRESS"
  priority  = 1000

  source_ranges = local.google_lb_ranges
  target_tags   = [local.vm_tag]

  allow {
    protocol = "tcp"
    ports    = [tostring(local.server_port)]
  }
}

# ---- Cloud SQL (private IP only) --------------------------------------------

resource "google_compute_global_address" "private_services" {
  name          = "${var.name_prefix}-private-services"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = var.private_services_prefix_length
  network       = google_compute_network.vpc.id
}

resource "google_service_networking_connection" "private_services" {
  network                 = google_compute_network.vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_services.name]

  # Without this, terraform destroy often fails with "producer services are
  # still using this connection". The peering is removed with the VPC.
  deletion_policy = "ABANDON"

  depends_on = [google_project_service.apis]
}

resource "google_sql_database_instance" "db" {
  name                = "${var.name_prefix}-pg"
  region              = var.region
  database_version    = "POSTGRES_16"
  deletion_protection = var.db_deletion_protection

  settings {
    tier                        = var.db_tier
    edition                     = "ENTERPRISE"
    availability_type           = var.db_availability_type
    disk_type                   = "PD_SSD"
    disk_size                   = var.db_disk_gb
    disk_autoresize             = true
    deletion_protection_enabled = var.db_deletion_protection
    user_labels                 = local.labels

    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.vpc.id
      # Plain-text connections are refused. Cloud SQL's server certificate is
      # signed by a per-instance CA, so the DB URL uses sslmode=require.
      ssl_mode = "ENCRYPTED_ONLY"
    }

    backup_configuration {
      enabled                        = true
      start_time                     = var.db_backup_start_time
      point_in_time_recovery_enabled = true
      transaction_log_retention_days = min(7, var.db_retained_backups)

      backup_retention_settings {
        retained_backups = var.db_retained_backups
        retention_unit   = "COUNT"
      }
    }

    maintenance_window {
      day          = 7
      hour         = 3
      update_track = "stable"
    }

    insights_config {
      query_insights_enabled = true
    }
  }

  depends_on = [google_service_networking_connection.private_services, google_project_service.apis]
}

resource "google_sql_database" "piperoom" {
  name     = "piperoom"
  instance = google_sql_database_instance.db.name
}

# No google_sql_user on purpose: its password would be written to Terraform
# state. The README creates the user with gcloud instead.

# ---- Service account of the VM ----------------------------------------------

resource "google_service_account" "vm" {
  account_id   = "${var.name_prefix}-vm"
  display_name = "Piperoom pilot VM"

  depends_on = [google_project_service.apis]
}

# Container logs (docker gcplogs driver) and the VM's own logs.
resource "google_project_iam_member" "log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.vm.email}"
}

# Ops Agent host metrics. Only needed when the agent is installed.
resource "google_project_iam_member" "metric_writer" {
  count = var.install_ops_agent ? 1 : 0

  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.vm.email}"
}

# Vertex AI: lets the server call Claude on Vertex with the VM's own identity,
# so no key file exists. Only granted when model_provider is vertex.
resource "google_project_iam_member" "vertex_user" {
  count = local.vertex ? 1 : 0

  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.vm.email}"
}

# Pull the image from Artifact Registry. Granted on the ONE repository that holds
# the image (read from the image path), not on the project, so the VM cannot read
# other repositories.
resource "google_artifact_registry_repository_iam_member" "ar_reader" {
  count = var.grant_artifact_registry_reader && local.ar_repo != null ? 1 : 0

  project    = local.ar_repo.project
  location   = local.ar_repo.location
  repository = local.ar_repo.repository
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.vm.email}"

  depends_on = [google_project_service.apis]
}

# roles/cloudsql.client is NOT granted: the VM connects to the private IP
# directly with a password and TLS, not through the Cloud SQL connector or proxy.
