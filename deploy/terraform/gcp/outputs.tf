output "dns_a_record" {
  description = "Create this DNS A record so the managed certificate can be issued."
  value       = "${var.domain}.  A  ${google_compute_global_address.lb.address}"
}

output "lb_ip_address" {
  description = "Public IPv4 address of the HTTPS load balancer."
  value       = google_compute_global_address.lb.address
}

output "public_url" {
  description = "Where people open Piperoom. Register <public_url>/auth/callback as the OIDC redirect URI."
  value       = local.public_url
}

output "oidc_redirect_uri" {
  description = "Redirect URI to register at the identity provider."
  value       = "${local.public_url}/auth/callback"
}

output "nat_egress_ip" {
  description = "The one address all outbound traffic from the VM comes from. Allow-list it at your pipeline (source DB, Kafka, Connect, warehouse)."
  value       = google_compute_address.nat.address
}

output "iap_ssh_command" {
  description = "SSH to the VM through IAP (needs roles/iap.tunnelResourceAccessor and an OS Login role)."
  value       = "gcloud compute ssh ${google_compute_instance.piperoom.name} --zone ${local.zone} --project ${var.project_id} --tunnel-through-iap"
}

output "vm_name" {
  description = "Name of the Piperoom VM."
  value       = google_compute_instance.piperoom.name
}

output "vm_zone" {
  description = "Zone of the Piperoom VM."
  value       = local.zone
}

output "vm_service_account" {
  description = "Service account the VM runs as."
  value       = google_service_account.vm.email
}

output "cloudsql_instance" {
  description = "Cloud SQL instance name (for gcloud sql users create)."
  value       = google_sql_database_instance.db.name
}

output "cloudsql_private_ip" {
  description = "Private IP address of Cloud SQL."
  value       = google_sql_database_instance.db.private_ip_address
}

output "db_url_template" {
  description = "Shape of the value to store in the db-url secret. Replace PASSWORD with a hex password (no special characters)."
  value       = "postgres://piperoom:PASSWORD@${google_sql_database_instance.db.private_ip_address}:5432/piperoom?sslmode=require"
}

output "secret_ids" {
  description = "Secret Manager secrets whose values you must add (README step 5)."
  value       = { for k, s in google_secret_manager_secret.this : k => s.secret_id }
}
