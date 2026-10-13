# Global external Application Load Balancer: HTTPS with a Google-managed
# certificate, HTTP redirected to HTTPS, one unmanaged instance group behind it.

resource "google_compute_global_address" "lb" {
  name   = "${var.name_prefix}-lb-ip"
  labels = local.labels

  depends_on = [google_project_service.apis]
}

# The managed certificate is issued only after DNS for var.domain points at the
# load balancer address (README step 7); status stays PROVISIONING until then.
# The name carries a hash of the domain so a domain change can create the new
# certificate before the old one is destroyed.
resource "google_compute_managed_ssl_certificate" "lb" {
  name = "${var.name_prefix}-cert-${substr(md5(var.domain), 0, 6)}"

  managed {
    domains = [var.domain]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_compute_ssl_policy" "tls12" {
  name            = "${var.name_prefix}-tls12"
  profile         = "MODERN"
  min_tls_version = "TLS_1_2"
}

# The server's real liveness endpoint: GET /healthz (internal/httpapi/health.go).
# It answers while the process serves HTTP and does not touch the database, so a
# database blip does not take the whole load balancer backend out. It is exempt
# from the Host and auth checks. /readyz also pings the database; use it if you
# prefer the load balancer to stop sending traffic when the database is down.
resource "google_compute_health_check" "server" {
  name                = "${var.name_prefix}-hc"
  check_interval_sec  = 10
  timeout_sec         = 5
  healthy_threshold   = 2
  unhealthy_threshold = 3

  http_health_check {
    port         = local.server_port
    request_path = "/healthz"
  }
}

resource "google_compute_security_policy" "allow_list" {
  count = length(var.allowed_cidrs) > 0 ? 1 : 0

  name        = "${var.name_prefix}-allow-list"
  description = "Only listed source ranges may reach Piperoom."

  # A rule matches at most 10 ranges, so the list is split into chunks.
  dynamic "rule" {
    for_each = { for i, c in chunklist(var.allowed_cidrs, 10) : i => c }

    content {
      priority    = 1000 + rule.key
      action      = "allow"
      description = "Allowed source ranges ${rule.key + 1}"

      match {
        versioned_expr = "SRC_IPS_V1"

        config {
          src_ip_ranges = rule.value
        }
      }
    }
  }

  rule {
    priority    = 2147483647
    action      = "deny(403)"
    description = "Default: deny"

    match {
      versioned_expr = "SRC_IPS_V1"

      config {
        src_ip_ranges = ["*"]
      }
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_compute_backend_service" "piperoom" {
  name                  = "${var.name_prefix}-backend"
  protocol              = "HTTP"
  port_name             = "http"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  # WebSocket upgrades (/api/rooms/<id>/ws) work on this backend; the timeout is
  # the longest a socket may stay open.
  timeout_sec      = var.backend_timeout_sec
  health_checks    = [google_compute_health_check.server.id]
  security_policy  = try(google_compute_security_policy.allow_list[0].id, null)
  session_affinity = "NONE"
  enable_cdn       = false

  connection_draining_timeout_sec = 30

  backend {
    group           = google_compute_instance_group.piperoom.id
    balancing_mode  = "UTILIZATION"
    max_utilization = 0.8
    capacity_scaler = 1.0
  }

  log_config {
    enable      = true
    sample_rate = 0.1
  }
}

resource "google_compute_url_map" "https" {
  name            = "${var.name_prefix}-https"
  default_service = google_compute_backend_service.piperoom.id
}

resource "google_compute_target_https_proxy" "https" {
  name             = "${var.name_prefix}-https-proxy"
  url_map          = google_compute_url_map.https.id
  ssl_certificates = [google_compute_managed_ssl_certificate.lb.id]
  ssl_policy       = google_compute_ssl_policy.tls12.id
}

resource "google_compute_global_forwarding_rule" "https" {
  name                  = "${var.name_prefix}-https"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  ip_protocol           = "TCP"
  port_range            = "443"
  ip_address            = google_compute_global_address.lb.id
  target                = google_compute_target_https_proxy.https.id
  labels                = local.labels
}

# HTTP -> HTTPS redirect (no backend is involved).
resource "google_compute_url_map" "redirect" {
  name = "${var.name_prefix}-http-redirect"

  default_url_redirect {
    https_redirect         = true
    redirect_response_code = "MOVED_PERMANENTLY_DEFAULT"
    strip_query            = false
  }
}

resource "google_compute_target_http_proxy" "redirect" {
  name    = "${var.name_prefix}-http-proxy"
  url_map = google_compute_url_map.redirect.id
}

resource "google_compute_global_forwarding_rule" "http" {
  name                  = "${var.name_prefix}-http"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  ip_protocol           = "TCP"
  port_range            = "80"
  ip_address            = google_compute_global_address.lb.id
  target                = google_compute_target_http_proxy.redirect.id
  labels                = local.labels
}
