resource "google_vertex_ai_custom_job" "custom_training_job" {
  display_name = "driftshield-training"
  location     = var.region
  project      = var.project_id

  job_spec {
    # Define the worker pool (compute resources and container)
    worker_pool_specs {
      machine_spec {
        machine_type = "n1-standard-4"
      }

      replica_count = 1

      container_spec {
        # The Artifact Registry URI: region-docker.pkg.dev/project_id/repo_name/image_name:tag
        image_uri = "us-central1-docker.pkg.dev/${var.project_id}/${var.repo_name}/${var.training_image_name}:latest"        
      }
    }

    base_output_directory {
      output_uri_prefix = "gs://${var.bucket_name}/models/"
    }
  }
}

