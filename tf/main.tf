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

resource "google_storage_bucket" "bucket_module03" {
  name          = "dezoomcamp2025-448509-bucket03"
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

resource "google_bigquery_dataset" "dataset_module03" {
  dataset_id                  = "dezoomamp_dataset03"  
  friendly_name               = "Dataset_module03"  
  description                 = "This is the dataset used for zoomcamp module 03."
  location                    = "EU"     

  labels = {
    env = "zoomcamp"
  }
}
