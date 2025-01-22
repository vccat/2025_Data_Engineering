terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.16.0"
    }
  }
}

provider "google" {

  project     = "dezoomcamp2025-448509"
  region      = "europe-west3"
}

resource "google_storage_bucket" "bucket0" {
  name          = "dezoomcamp2025-448509-bucket0"
  location      = "EU"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}
