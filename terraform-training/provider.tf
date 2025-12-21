terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.0"
    }
  }

  # This stores the "save file" for your infrastructure.
  backend "gcs" {
    bucket  = "aipartnercatalyst-confluent-tf-state" 
    prefix  = "train/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
