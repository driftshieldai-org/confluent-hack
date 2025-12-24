resource "null_resource" "submit_vertex_job" {
  # Trigger the job whenever the container image digest changes
  triggers = {
    dir_sha = sha256(join("", [
      for f in fileset("${path.module}/../train", "**") : 
      filesha256("${path.module}/../train/${f}")
    ]))
  }

  provisioner "local-exec" {
    command = <<EOT
      gcloud ai custom-jobs create \
        --project=${var.train_project_id} \
        --region=${var.region} \
        --display-name="driftshieldai-training-${formatdate("YYYYMMDD-hhmm", timestamp())}" \
        --worker-pool-spec=machine-type=n1-standard-4,replica-count=1,container-image-uri=us-central1-docker.pkg.dev/${var.project_id}/${var.repo_name}/${var.training_image_name}:latest \
        --service-account=${var.service_account_id}
    EOT
  }
}

