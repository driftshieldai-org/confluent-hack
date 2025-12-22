# Driftshield AI
Driftshield AI is an end-to-end anomaly detection and monitoring platform. It ingests streaming data from Confluent, processes it via Google Cloud Dataflow, performs real-time anomaly detection with Vertex AI models,summaries it with Gemini and visualizes results through a Cloud Run-hosted UI and alerting through mail.

# 🏗 Project Architecture

Driftshield AI follows a modern Serverless Event-Driven Architecture on Google Cloud Platform. It is designed to handle high-velocity streaming data with real-time AI intervention.

**🔄 Data Flow Overview:**  
Ingestion → 2. Processing → 3. AI Enrichment → 4. Storage → 5. Visualization

**🧱 Core Components:**  
1. Streaming Ingestion (Confluent Cloud):  
* Acts as the central nervous system.  
* Collects raw event data (transactions) and streams it into GCP via a dedicated Kafka topic.
2. Real-time Processing (Google Cloud Dataflow):  
* Apache Beam Pipeline: A pyhton-based pipeline running as a Dataflow Flex Template.
* Windowing: Groups incoming data streams into one minute time windows.
* Anomaly Detection: Compares incoming patterns for 5 minute against ML model trained.
3. Generative AI Layer (Gemini 2.5 Flash):
* When an anomaly is detected, the Dataflow pipeline sends the metadata to Gemini AI.
* Gemini generates a Natural Language Summary explaining why the data is considered anomalous and suggests potential root causes.
4. Analytical Storage (Google BigQuery):
* Raw Data: All processed events are stored for long-term analysis.
* Anomalies Table: A dedicated table for high-severity events, including the Gemini-generated insights.
* Time-series Partitioning: Optimized for fast querying by the frontend.
5. Monitoring Dashboard (Cloud Run):
* A frontend hosted on Cloud Run.
* Provides a live "security cockpit" showing the "Driftshield Status."
* Queries BigQuery directly to visualize anomaly trends and system health.
6. Continuous Learning (Vertex AI):
* Training Pipeline: Uses the historical data in BigQuery to retrain anomaly detection models.
* Custom Containers: Training logic is packaged in Docker and managed via terraform-training.
7. Mail Alert:
* A mail alert will be sent along with anomaly data on monitoring dashboard

**🛠 Infrastructure Management (IaC):**  
The project is split into three Terraform layers to ensure stability and modularity:

1. Base Layer (terraform/):
   Sets up the "foundation" (Networking, Service Accounts, BigQuery Datasets, GCS Buckets).
3. Jobs Layer (terraform-jobs/):
   Deploys the "active" components (Dataflow Jobs, Cloud Run Services).
5. Training Layer (terraform-training/):
   Manages the "intelligence" (Vertex AI custom jobs and model registry).
   
**🎨 Visual Architecture Map:**
<pre>
[ Confluent ] 
      |
      ▼
[ Cloud Dataflow ] <───> [ Gemini AI (Vertex AI) ]
      |──────────┐         (Insight Generation)
      |    [ Mail Alert ]
      ▼       
[ BigQuery Storage ]
      |
      ├──────────────┐
      ▼              ▼
[ Cloud Run UI ]  [ Vertex AI Training ]
 (Live Dashboard)   (Model Re-tuning) </pre>


# 🚀 CI/CD Pipeline Overview:
The project uses GitHub Actions (Driftshield Deploy) to automate infrastructure and application deployment. The pipeline is designed to be "change-aware," meaning it only builds and deploys components that have modified files.

<pre> .
├── .github/
│   └── workflows/
│       └── deploy.yml          # CI/CD Pipeline (GitHub Actions)
├── dataflow/
│   ├── src/                    # Apache Beam source code
│   └── template/               # Dataflow Flex Template metadata
├── run-ui/                     # Cloud Run dashboard source code
├── terraform/                  # Base Infra (IAM, Buckets, BigQuery)
├── terraform-jobs/             # App Infra (Dataflow Jobs, Cloud Run)
├── terraform-training/        # AI Infra (Vertex AI Training Jobs)
├── train/                      # ML Model training logic & Dockerfile
└── README.md </pre>

**Pipeline Stages:**  
  
1. Terraform Base:  Sets up foundational GCP resources.  
2. Path Filtering:  Detects which modules (train, dataflow, ui, terraform) changed.  
3. Docker Build & Push:
* Builds images for Vertex AI Training, Dataflow, and Cloud Run UI.
* Pushes images to Google Artifact Registry (GAR).
4. Terraform Jobs: Deploys/Updates Dataflow jobs and Cloud Run services using the new images.  
5. Terraform Training: Triggers or updates Vertex AI training configurations.  

**🛠 Setup & Requirements**
1. Google Cloud Platform Setup
* Create a GCP Project.
* Create a Service Account with necessary permissions.
2. GitHub Secrets
Navigate to your repository Settings > Secrets and variables > Actions and add the following:

Secret Name	Description
* GCP_PROJECT_ID:	Google Cloud Project ID
* GCP_SA_KEY:	The JSON key of Google Cloud Service Account

**🔍 Key Components**
1. Dataflow Job:  
The Dataflow pipeline handles:

* Streaming ingestion from Confluent.
* Windowing and transformation.
* Integration with Gemini AI to analyze data patterns and generate natural language summaries of detected anomalies.
* Output to BigQuery.
2. Cloud Run UI:  
A containerized service located in run-ui/ that queries BigQuery to provide a real-time visual representation of the "Driftshield" protection status and anomaly logs.

3. Vertex AI Training:  
The train/ folder contains the logic to build a custom container for model training. Terraform in terraform-training/ manages the lifecycle of these training jobs within the Vertex AI ecosystem.

**🛠 Troubleshooting**
* Permissions: Ensure the GitHub Service Account has necessary permission.
* Path Filters: If a job doesn't run, check if changes were committed to the specific sub-folders defined in the changes_check job of the workflow.
