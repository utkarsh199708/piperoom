# Secret Manager: Terraform creates the secret CONTAINERS and the IAM binding.
# It never creates a version, so no secret value is ever in Terraform state or in
# a .tfvars file. You add the values with gcloud (README step 5).

locals {
  secret_defs = merge(
    {
      db-url = {
        env  = "PIPEROOM_DB_URL"
        desc = "Piperoom room database URL (postgres://piperoom:PASSWORD@<cloud sql private ip>:5432/piperoom?sslmode=require)."
      }
      oidc-client-secret = {
        env  = "PIPEROOM_OIDC_CLIENT_SECRET"
        desc = "OIDC client secret for the Piperoom login."
      }
    },
    var.model_provider == "anthropic" ? {
      anthropic-api-key = {
        env  = "ANTHROPIC_API_KEY"
        desc = "Anthropic API key for the agent."
      }
    } : {},
  )

  # Whole-file secrets: not mapped to one variable.
  file_secret_defs = {
    pipeline-env = "KEY=VALUE lines appended to the server env file: SOURCE_DB_URL, WAREHOUSE_DB_URL, KAFKA_*, CONNECT_*, SCHEMA_REGISTRY_*, SLACK_*, alert secrets."
    owners-yaml  = "Contents of owners.yaml (who owns which layer)."
    users-yaml   = "Contents of users.yaml (who may log in, and their roles)."
  }

  all_secret_ids = merge(
    { for k, v in local.secret_defs : k => v.desc },
    local.file_secret_defs,
  )
}

resource "google_secret_manager_secret" "this" {
  for_each = local.all_secret_ids

  secret_id = "${var.name_prefix}-${each.key}"
  labels    = local.labels

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis]
}

# Least privilege: the VM account can read exactly these secrets, not the project's others.
resource "google_secret_manager_secret_iam_member" "vm_accessor" {
  for_each = google_secret_manager_secret.this

  secret_id = each.value.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.vm.email}"
}
