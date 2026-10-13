locals {
  public_url = "https://${var.domain}"

  # Settings that are not secret. They are written to the instance metadata and
  # turned into the env file by startup.sh. Secrets are fetched at boot instead.
  managed_env = merge(
    {
      PIPEROOM_AUTH_MODE      = "oidc"
      PIPEROOM_PUBLIC_URL     = local.public_url
      PIPEROOM_OIDC_ISSUER    = var.oidc_issuer
      PIPEROOM_OIDC_CLIENT_ID = var.oidc_client_id
      PIPEROOM_MODEL_PROVIDER = var.model_provider
      PIPEROOM_BIND           = "0.0.0.0"
      PIPEROOM_PORT           = tostring(local.server_port)
      PIPEROOM_HTTP_ADDR      = "0.0.0.0:8090"
      # Dev-mode switch; forced off so no setting can make it listen openly.
      PIPEROOM_DEV_ALLOW_NONLOOPBACK = "0"
      PIPEROOM_TRUSTED_PROXIES       = join(",", concat(local.google_lb_ranges, ["${google_compute_global_address.lb.address}/32"]))
    },
    var.model != null ? { PIPEROOM_MODEL = var.model } : {},
    local.vertex ? {
      VERTEX_PROJECT_ID = var.project_id
      VERTEX_REGION     = var.vertex_region
    } : {},
    var.extra_env,
  )

  # ENVVAR=secret-id lines for secrets whose whole value is one variable.
  secret_var_map = { for k, v in local.secret_defs : v.env => google_secret_manager_secret.this[k].secret_id }
}

resource "google_compute_instance" "piperoom" {
  name                      = "${var.name_prefix}-vm"
  zone                      = local.zone
  machine_type              = var.machine_type
  tags                      = [local.vm_tag]
  labels                    = local.labels
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image  = "projects/debian-cloud/global/images/family/debian-12"
      size   = var.boot_disk_gb
      type   = "pd-balanced"
      labels = local.labels
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.vm.id
    # No access_config block: the VM has no public IP.
  }

  service_account {
    email = google_service_account.vm.email
    # The scope is a coarse outer limit; IAM roles above are what restrict access.
    scopes = ["cloud-platform"]
  }

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  metadata = {
    # IAM-based SSH through IAP. No project-wide or per-instance SSH keys.
    enable-oslogin         = "TRUE"
    block-project-ssh-keys = "TRUE"

    startup-script = file("${path.module}/startup.sh")

    # The compose file is deploy/docker-compose.pilot.yml, unchanged.
    piperoom-compose-b64    = base64encode(file("${path.module}/../../docker-compose.pilot.yml"))
    piperoom-image          = var.piperoom_image
    piperoom-env            = join("\n", [for k in sort(keys(local.managed_env)) : "${k}=${local.managed_env[k]}"])
    piperoom-secret-vars    = join("\n", [for k in sort(keys(local.secret_var_map)) : "${k}=${local.secret_var_map[k]}"])
    piperoom-secret-envfile = google_secret_manager_secret.this["pipeline-env"].secret_id
    piperoom-secret-owners  = google_secret_manager_secret.this["owners-yaml"].secret_id
    piperoom-secret-users   = google_secret_manager_secret.this["users-yaml"].secret_id
    piperoom-ops-agent      = tostring(var.install_ops_agent)
  }

  lifecycle {
    precondition {
      condition     = !local.vertex || var.model != null
      error_message = "model_provider = \"vertex\" needs model set to a Vertex model id: the server default is an Anthropic API id and does not work on Vertex."
    }
    precondition {
      condition     = !local.vertex || var.vertex_region != null
      error_message = "model_provider = \"vertex\" needs vertex_region set to a region (or global/us/eu) where your Claude model is served; check Model Garden."
    }
  }

  depends_on = [
    google_compute_router_nat.nat,
    google_secret_manager_secret_iam_member.vm_accessor,
    google_project_iam_member.log_writer,
  ]
}

# Unmanaged group: one fixed VM, no autoscaling. The named port tells the load
# balancer which port the backend service uses.
resource "google_compute_instance_group" "piperoom" {
  name      = "${var.name_prefix}-ig"
  zone      = local.zone
  instances = [google_compute_instance.piperoom.self_link]

  named_port {
    name = "http"
    port = local.server_port
  }
}
