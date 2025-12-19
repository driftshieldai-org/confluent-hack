resource "google_project_service" "gcp_services" {
  for_each = toset(["artifactregistry.googleapis.com",
              "storage.googleapis.com",
              "bigquery.googleapis.com",
              "iam.googleapis.com"])

  project = var.project_id
  service = each.key
  disable_on_destroy         = false
  disable_dependent_services = false
}

# 1. Create GCS Bucket
resource "google_storage_bucket" "bucket" {
  name                        = var.bucket_name
  project                     = var.project_id
  location                    = var.region
  storage_class               = var.storage_class
  }


# 2. Create Artifact Registry (GAR)
resource "google_artifact_registry_repository" "my_repo" {
  location      = var.region
  repository_id = var.gar_repo_name
  description   = "Docker repository created via Terraform"
  format        = "DOCKER"
}

# 3. Create BigQuery Dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = var.dataset_id
  description                 = "This is a dataset to store streaming data"
  location                    = var.region
}

# 4. Create BigQuery Table
resource "google_bigquery_table" "table" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = var.train_table_id
  schema =file("${path.module}/schemas/${var.train_table_id}.json")
  deletion_protection=false
}

