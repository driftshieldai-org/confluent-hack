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

variable "repo_name" {
  description = "Name of the Artifact Registry Repo"
  type        = string
  default     = "driftshield-ai-docker"
}

variable "training_image_name" {
  description = "Name of the Artifact Registry training Repo"
  type        = string
  default     = "vertexai-custom"
}

variable "service_account_id" {
  description = "The GCP service account used for application"
  type        = string
}

variable "stream_table" {
  description = "Name of the table used for streaming"
  type        = string
}

variable "anomaly_table" {
  description = "Name of the table used for storing anomalies"
  type        = string
}

variable "anomaly_summ_table" {
  description = "Name of the table used for storing anomalies summary"
  type        = string
}

variable "bootstrap_servers" {
  description = "Name of the bootstrap server of confluent"
  type        = string
}

variable "kafka_topic" {
  description = "Name of the kafka topic"
  type        = string
}

