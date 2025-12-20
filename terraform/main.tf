resource "google_project_service" "gcp_services" {
  for_each = toset(["artifactregistry.googleapis.com",
              "storage.googleapis.com",
              "bigquery.googleapis.com",
              "iam.googleapis.com",
              "dataflow.googleapis.com"])
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

resource "google_bigquery_table" "stream_table" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = var.stream_table_id
  schema =file("${path.module}/schemas/${var.stream_table_id}.json")
  deletion_protection=false
  time_partitioning {
    type  = "DAY"
    field = "insert_timestamp"
  }
}

resource "google_bigquery_table" "anomalies_table" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = var.anomalies_table_id
  schema =file("${path.module}/schemas/${var.anomalies_table_id}.json")
  deletion_protection=false
  time_partitioning {
    type  = "DAY"
    field = "timestamp" 
    expiration_ms  = 7776000000
  }
}

resource "google_bigquery_table" "anomalies_summary_table" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = var.anomalies_summ_table_id
  schema =file("${path.module}/schemas/${var.anomalies_summ_table_id}.json")
  deletion_protection=false
  time_partitioning {
    type  = "DAY"
    field = "window_timestamp" 
    expiration_ms  = 7776000000
  }
}

resource "google_service_account" "gcp_sa" {
  account_id   = "driftshieldai-sa"
  project      = var.project_id
  display_name = "driftshieldai Service Account"
  description  = "Service account used in driftshieldai application"
}

resource "google_project_iam_member" "sa_roles" {
  for_each = toset(["roles/artifactregistry.reader","roles/bigquery.dataEditor","roles/bigquery.jobUser","roles/bigquery.readSessionUser","roles/run.invoker","roles/dataflow.worker","roles/storage.objectUser","roles/storage.bucketViewer","roles/aiplatform.user"])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.gcp_sa.email}"
}

resource "google_compute_network" "vpc_network" {
  name                    = var.vpc_network
  auto_create_subnetworks = false
  project      = var.project_id
}

resource "google_compute_subnetwork" "subnet" {
  name          =  var.subnet_name
  ip_cidr_range = "10.0.1.0/24"
  region        = var.region
  network       = google_compute_network.vpc_network.id
  private_ip_google_access = true
}
