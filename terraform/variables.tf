variable "project_id" {
  description = "The GCP Project ID"
  type        = string
}

variable "region" {
  description = "The GCP Region"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Name of the GCS bucket"
  type        = string
}

variable "gar_repo_name" {
  description = "Name of the Artifact Registry Repo"
  type        = string
  default     = "driftshield-ai-docker"
}

variable "storage_class" {
  description = "The Storage Class of the new bucket."
  type        = string
  default     = "STANDARD"
}

variable "dataset_id" {
  description = "Name of the BQ dataset"
  type        = string
}

variable "train_table_id" {
  description = "Name of the table for training"
  type        = string
}

variable "stream_table_id" {
  description = "Name of the table used for streaming"
  type        = string
}

variable "anomalies_table_id" {
  description = "Name of the table used for storing anomalies"
  type        = string
}

variable "anomalies_summ_table_id" {
  description = "Name of the table used for storing anomalies summary"
  type        = string
}
