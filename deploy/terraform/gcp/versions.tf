terraform {
  required_version = ">= 1.5.0, < 2.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.50"
    }
  }

  # State holds no secret values (see README, "What ends up in Terraform
  # state"), but it does hold project ids, IPs and the VM's metadata. Keep it in
  # a private, versioned bucket for anything beyond a throw-away trial:
  #
  # backend "gcs" {
  #   bucket = "my-tfstate-bucket"
  #   prefix = "piperoom-pilot"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
