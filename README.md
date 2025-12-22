Driftshield AI
Driftshield AI is an end-to-end anomaly detection and monitoring platform. It ingests streaming data from Confluent, processes it via Google Cloud Dataflow, performs real-time anomaly detection with Gemini AI, and visualizes results through a Cloud Run-hosted UI.

🏗 Project Architecture
Ingestion & Processing: Dataflow jobs consume streams from Confluent, identify anomalies, and generate summaries using Gemini.
Storage: Processed data and anomalies are stored in BigQuery.
ML Training: Custom model training is handled via Vertex AI.
UI: A frontend service running on Cloud Run provides a dashboard for anomaly monitoring.
Infrastructure: Managed entirely via Terraform across three logical layers (Base, Jobs, and Training).

📁 Repository Structure
├── .github/workflows/
│   └── deploy.yml            # CI/CD Pipeline (GitHub Actions)
├── dataflow/
│   ├── src/                  # Beam source code (Confluent -> BigQuery + Gemini)
│   └── template/             # Dataflow Flex Template definitions
├── run-ui/                   # Cloud Run frontend service source code
├── terraform/                # Base Infra: APIs, SAs, GCS Buckets, BQ Datasets, IAM
├── terraform-jobs/           # Jobs Infra: Dataflow Job & Cloud Run Service deployment
├── terraform-training/       # Training Infra: Vertex AI Custom Training jobs
├── train/                    # ML Model training source code 
└── README.md

🚀 CI/CD Pipeline Overview
The project uses GitHub Actions (Driftshield Deploy) to automate infrastructure and application deployment. The pipeline is designed to be "change-aware," meaning it only builds and deploys components that have modified files.

Pipeline Stages:
Terraform Base: Sets up foundational GCP resources.
Path Filtering: Detects which modules (train, dataflow, ui, terraform) changed.
Docker Build & Push:
Builds images for Vertex AI Training, Dataflow, and Cloud Run UI.
Pushes images to Google Artifact Registry (GAR).
Terraform Jobs: Deploys/Updates Dataflow jobs and Cloud Run services using the new images.
Terraform Training: Triggers or updates Vertex AI training configurations.
🛠 Setup & Requirements
1. Google Cloud Platform Setup
Create a GCP Project.
Enable Artifact Registry and create a repository named driftshield-ai-docker.
Create a Service Account with necessary permissions (Editor/Owner for Terraform).
2. GitHub Secrets
Navigate to your repository Settings > Secrets and variables > Actions and add the following:

Secret Name	Description
GCP_PROJECT_ID	Your Google Cloud Project ID
GCP_SA_KEY	The JSON key of your Google Cloud Service Account
💻 Local Development
Prerequisites
Terraform v1.6.0+
Google Cloud SDK (gcloud)
Docker
Java 11 (for Dataflow development)
Deploying Manually
If you need to run Terraform from your local machine:

# 1. Initialize and apply base infra
cd terraform
terraform init
terraform apply

# 2. Build a specific image (example: UI)
docker build -t us-central1-docker.pkg.dev/[PROJECT_ID]/driftshield-ai-docker/anomalyui:latest ./run-ui
docker push us-central1-docker.pkg.dev/[PROJECT_ID]/driftshield-ai-docker/anomalyui:latest

# 3. Deploy the job
cd ../terraform-jobs
terraform init
terraform apply
🔍 Key Components
Dataflow Job
The Dataflow pipeline handles:

Streaming ingestion from Confluent.
Windowing and transformation.
Integration with Gemini AI to analyze data patterns and generate natural language summaries of detected anomalies.
Output to BigQuery.
Cloud Run UI
A containerized service located in run-ui/ that queries BigQuery to provide a real-time visual representation of the "Driftshield" protection status and anomaly logs.

Vertex AI Training
The train/ folder contains the logic to build a custom container for model training. Terraform in terraform-training/ manages the lifecycle of these training jobs within the Vertex AI ecosystem.

🛠 Troubleshooting
Permissions: Ensure the GitHub Service Account has iam.serviceAccountUser on the Dataflow/Cloud Run controller accounts.
APIs: If Terraform fails, ensure APIs like dataflow.googleapis.com, aiplatform.googleapis.com, and run.googleapis.com are enabled.
Path Filters: If a job doesn't run, check if your changes were committed to the specific sub-folders defined in the changes_check job of the workflow.
