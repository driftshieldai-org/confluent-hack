#resource "null_resource" "submit_vertex_job" {
#  # Trigger the job whenever the container image digest changes
#  triggers = {
#    dir_sha = sha256(join("", [
#      for f in fileset("${path.module}/../train", "**") : 
#      filesha256("${path.module}/../train/${f}")
#    ]))
#  }
#
#  provisioner "local-exec" {
#    command = <<EOT
#      gcloud ai custom-jobs create \
#        --project=${var.project_id} \
#        --region=${var.region} \
#        --display-name="driftshieldai-training-${formatdate("YYYYMMDD-hhmm", timestamp())}" \
#        --worker-pool-spec=machine-type=n1-standard-1,replica-count=1,container-image-uri=us-central1-docker.pkg.dev/${var.project_id}/${var.repo_name}/${var.training_image_name}:latest \
#        --service-account=${var.service_account_id}
#    EOT
#  }
#}

resource "google_storage_bucket_object" "template" {
  bucket = var.bucket_name
  name   = "dags/dataflow/template/realtime_stream_anomaly.json"
  source = "dataflow/template/realtime_stream_anomaly.json"
}

resource "google_dataflow_flex_template_job" "job" {
  provider                = google-beta
  name                    = "driftshieldai-df-job-${formatdate("YYYYMMDD-hhmm", timestamp())}"
  region                  = var.region
  project                 = var.project_id
  container_spec_gcs_path = "gs://${var.bucket_name}/dataflow/template/realtime_stream_anomaly.json"
  temp_location           = "gs://${var.bucket_name}/dataflow/temp"
  staging_location        = "gs://${var.bucket_name}/dataflow/staging"

  parameters = {
    bootstrap_servers=var.bootstrap_servers,
    kafka_topic = var.kafka_topic,
    output_table = var.stream_table,
    model_dir = "gs://${var.bucket_name}/models",
    anomaly_output_table = var.anomaly_table,
    summary_output_table = var.anomaly_summ_table,
    api_project = var.project_id,
    api_region=var.region
  }
  enable_streaming_engine = true

  # Optional overrides
  max_workers             =  1
  machine_type            =  "n1-standard-1"
  ip_configuration        = "WORKER_IP_PRIVATE"
  service_account_email   = var.service_account_id
  on_delete               = "cancel"

  depends_on = [ google_storage_bucket_object.template]
}


