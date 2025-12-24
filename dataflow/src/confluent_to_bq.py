import argparse
import logging
import warnings
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.transforms.userstate import BagStateSpec, ReadModifyWriteStateSpec, TimerSpec, on_timer
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.coders import VarIntCoder, TupleCoder, FloatCoder
from google.cloud import storage # Import storage for GCS operations
import pickle
import pandas as pd
import time
import os
from datetime import datetime, timedelta, timezone # Import timezone
import vertexai
from vertexai.generative_models import GenerativeModel
from google.cloud import secretmanager
import base64
from email.mime.text import MIMEText
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build



def get_secret(project_id, secret_id, version_id="latest"):
    """
    Fetches a secret from Google Secret Manager.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logging.error(f"Failed to access secret {secret_id}: {e}")
        raise


class ParseKafkaMessage(beam.DoFn):
    """
    Parses the incoming Kafka message (assumed to be bytes) into a Python dictionary.
    The message is expected to be a JSON string.
    """
    def process(self, element, *args, **kwargs):
        try:
            # Kafka messages are often tuples of (key, value)
            message_value = element.value 
            
            # Decode the byte string to a UTF-8 string and then parse as JSON
            parsed_data = json.loads(message_value.decode('utf-8'))
            
            # The output of this DoFn will be a single element PCollection
            yield parsed_data
        except Exception as e:
            logging.error(f"Failed to parse message: {element}. Error: {e}")
            

class DetectVendorAnomaliesDoFn(beam.DoFn):
    """
    A stateful DoFn to detect anomalies based on per-minute counts for each vendor.
    It maintains the last 5 minutes of counts to calculate rolling features.
    """
    # Define a state specification. This will hold tuples of (timestamp, count).
    # We use VarIntCoder for the timestamp (as seconds) and the count.
    COUNTS_STATE = BagStateSpec('counts', TupleCoder((VarIntCoder(), VarIntCoder())))
    # Define a state to hold the last seen timestamp for a vendor
    LAST_SEEN_STATE = ReadModifyWriteStateSpec('last_seen', VarIntCoder())
    # Define state to track the last time an anomaly was raised to suppress duplicates
    LAST_ANOMALY_TS_STATE = ReadModifyWriteStateSpec('last_anomaly_ts', VarIntCoder())
    # Define state to track the last time an "Unknown Vendor" alert was sent
    LAST_UNKNOWN_VENDOR_ALERT_TS_STATE = ReadModifyWriteStateSpec('last_unknown_vendor_alert_ts', VarIntCoder())
    # Define state to track the first time a vendor is seen to suppress initial anomalies
    FIRST_SEEN_TS_STATE = ReadModifyWriteStateSpec('first_seen_ts', VarIntCoder())
    # Define a timer that will fire to check for silent vendors
    SILENCE_TIMER = TimerSpec('silence_timer', beam.TimeDomain.WATERMARK)
    
    def __init__(self, model_dir):
        self.model_dir = model_dir
        # Model artifacts will be loaded lazily in setup()
        self.model = None
        self.scaler = None
        self.vendor_encoder = None
        self.training_stats = None


    def setup(self):
        """Load artifacts on each worker."""
        if not self.model_dir.startswith("gs://"):
            raise ValueError(f"Invalid GCS path for model_dir: {self.model_dir}")

        storage_client = storage.Client()
        path_parts = self.model_dir.replace("gs://", "").split("/", 1)
        bucket_name = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ""
        bucket = storage_client.bucket(bucket_name)

        def load_pickle(filename):
            blob_path = f"{prefix.rstrip('/')}/{filename}"
            blob = bucket.blob(blob_path)
            logging.info(f"Loading artifact from gs://{bucket_name}/{blob_path}")
            serialized_obj = blob.download_as_string()
            return pickle.loads(serialized_obj)

        self.model = load_pickle('model.pkl')
        self.scaler = load_pickle('scaler.pkl')
        self.vendor_encoder = load_pickle('vendor_encoder.pkl')
        self.training_stats = load_pickle('training_stats.pkl')

    def process(self, 
                element, 
                counts_state=beam.DoFn.StateParam(COUNTS_STATE),
                last_seen_state=beam.DoFn.StateParam(LAST_SEEN_STATE),
                last_anomaly_ts_state=beam.DoFn.StateParam(LAST_ANOMALY_TS_STATE),
                last_unknown_vendor_alert_ts_state=beam.DoFn.StateParam(LAST_UNKNOWN_VENDOR_ALERT_TS_STATE),
                first_seen_ts_state=beam.DoFn.StateParam(FIRST_SEEN_TS_STATE),
                silence_timer=beam.DoFn.TimerParam(SILENCE_TIMER)):
        
        vendor_id, (timestamp_sec, count) = element
        current_timestamp = datetime.fromtimestamp(timestamp_sec)

        # Check if this is the first time we're seeing this vendor
        first_seen_ts = first_seen_ts_state.read()
        if first_seen_ts is None:
            # It's the first record, so store the timestamp and don't predict yet.
            first_seen_ts_state.write(timestamp_sec)
            first_seen_ts = timestamp_sec # Use it in this same process call

        # 1. Update State
        counts_state.add((timestamp_sec, count))
        
        # 2. Prune old data (keep last 5 minutes)
        cutoff_sec = timestamp_sec - (5 * 60)
        all_counts_list = list(counts_state.read())
        
        recent_counts_tuples = [c for c in all_counts_list if c[0] >= cutoff_sec]
        
        # Optimization: Only write back if we actually removed something
        if len(recent_counts_tuples) < len(all_counts_list):
            counts_state.clear()
            for c in recent_counts_tuples:
                counts_state.add(c)

        # 3. Predict (Changed logic: Allow prediction even if we only have 1 data point)
        if len(recent_counts_tuples) >= 1:
            counts_series = pd.Series([c[1] for c in recent_counts_tuples])

            # --- SAFE ENCODING ---
            try:
                # Ensure input type matches training (int)
                clean_vendor_id = vendor_id
                encoded_vendor = self.vendor_encoder.transform([clean_vendor_id])[0]
            except (ValueError, IndexError, TypeError):
                # If the vendor ID is unknown, check if we should send a daily alert.
                last_alert_ts = last_unknown_vendor_alert_ts_state.read()
                # Suppress for 24 hours (86400 seconds)
                if last_alert_ts is None or (timestamp_sec - last_alert_ts > 86400):
                    logging.warning(f"Unknown Vendor ID: {vendor_id}. Generating daily anomaly.")
                    yield {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "Metric": "UNKNOWN_VENDOR_ID",
                        "severity": "medium",
                        "vendor_id": vendor_id,
                        "count": count,
                        "score": None,
                        "details": f"Vendor ID '{vendor_id}' was not present in the training data."
                    }
                    # Update state to start new 24-hour suppression window
                    last_unknown_vendor_alert_ts_state.write(timestamp_sec)
                return

            # --- CREATE FEATURES ---
            features = {
                'record_count_1min': count,
                'rolling_mean_5min_vendor': counts_series.mean(),
                # If only 1 record, std is 0.0
                'rolling_std_5min_vendor': counts_series.std() if len(counts_series) > 1 else 0.0,
                'vendor_id_encoded': encoded_vendor,
                'hour_of_day': current_timestamp.hour,
                'day_of_week': current_timestamp.weekday()
            }

            # --- ENFORCE COLUMN ORDER (CRITICAL) ---
            # This MUST match the X.columns order from your training script
            EXPECTED_COLUMNS = [
                'record_count_1min', 
                'rolling_mean_5min_vendor', 
                'rolling_std_5min_vendor', 
                'vendor_id_encoded', 
                'hour_of_day', 
                'day_of_week'
            ]
            
            features_df = pd.DataFrame([features])
            features_df = features_df[EXPECTED_COLUMNS] # Reorder columns

            # --- SCALE & PREDICT ---
            X_scaled = self.scaler.transform(features_df)
            prediction = self.model.predict(X_scaled)
            
            # Get the raw score (-infinity to +infinity). 
            # Negative scores are anomalies. Positive are normal.
            score = self.model.decision_function(X_scaled)[0]
            
            # Log the score for debugging (Look at "Worker Logs" to see these)
            # logging.info(f"Vendor {vendor_id} Score: {score} (Pred: {prediction[0]})")

            if prediction[0] == -1:
                # Check suppression state before raising a new anomaly
                last_anomaly_ts = last_anomaly_ts_state.read()
                # Suppress for 60 minutes (3600 seconds)
                if last_anomaly_ts is None or (timestamp_sec - last_anomaly_ts > 3600):
                    # Determine severity based on the anomaly score. Lower scores are more anomalous.

                    # NEW: Suppress anomaly if it's within the first 5 minutes for this vendor
                    if (timestamp_sec - first_seen_ts) < 300: # 5 minutes = 300 seconds
                        logging.info(f"Suppressing initial anomaly for new vendor '{vendor_id}' within 5-minute grace period.")
                        return


                    severity = "high" if score < -0.15 else "medium"
                    anomaly_record = {
                        "timestamp": current_timestamp.isoformat(),
                        "Metric": "STATISTICAL_OUTLIER",
                        "severity": severity,
                        "vendor_id": vendor_id,
                        "count": int(counts_series.sum()),
                        "score": float(score),
                        "details": json.dumps({
                            "realtime_mean_last_5m": features['rolling_mean_5min_vendor'],
                            "realtime_std_dev_last_5m": features['rolling_std_5min_vendor'],
                            "training_baseline_mean": self.training_stats.get('mean_record_count_1min', 0.0),
                            "training_baseline_std_dev": self.training_stats.get('std_record_count_1min', 0.0)
                        })
                    }
                    logging.warning(f"ANOMALY DETECTED: {json.dumps(anomaly_record)}")
                    yield anomaly_record
                    
                    # Update state to start new suppression window
                    last_anomaly_ts_state.write(timestamp_sec)
                else:
                    logging.info(f"Suppressing repeated STATISTICAL_OUTLIER for vendor '{vendor_id}'.")
        
        # 4. Reset Timer
        now_sec = time.time()
        last_seen_state.write(timestamp_sec)
        silence_timer.set(now_sec + 10 * 60)
		
    @on_timer(SILENCE_TIMER)
    def check_for_silence(self,
                          timer_ts=beam.DoFn.TimestampParam,
                          vendor_id=beam.DoFn.KeyParam,
                          first_seen_ts_state=beam.DoFn.StateParam(FIRST_SEEN_TS_STATE),
                          last_seen_state=beam.DoFn.StateParam(LAST_SEEN_STATE),
                          silence_timer=beam.DoFn.TimerParam(SILENCE_TIMER)):
        
        last_seen_sec = last_seen_state.read()
        
        # If we haven't seen data, raise anomaly
        # Note: In RealTime domain, we compare against current time
        import time
        now_sec = time.time()
        
        # Check if the last known event was more than 10 mins ago
        if last_seen_sec and (now_sec - last_seen_sec >= 10 * 60):
            silence_duration_sec = now_sec - last_seen_sec

            # Determine severity based on silence duration
            if silence_duration_sec >= 180 * 60: # > 180 mins
                severity = "critical"
                next_check_delay_sec = 180 * 60
                
            elif silence_duration_sec >= 60 * 60: # > 60 mins
                severity = "high"
                next_check_delay_sec = 120 * 60 + 10 
            else: # > 10 mins
                severity = "medium"
                next_check_delay_sec = 50 * 60 + 10

            anomaly_record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "Metric": "NO_DATA_RECEIVED",
                "severity": severity,
                "vendor_id": vendor_id,
                "count": 0,
                "score": None,
                "details": f"No data seen for vendor '{vendor_id}' in over {int(silence_duration_sec)} seconds."
            }
            logging.warning(f"SILENCE ANOMALY: {json.dumps(anomaly_record)}")
            yield anomaly_record
            
            # NEW: Reset the first_seen timestamp so the grace period applies if the vendor returns.
            logging.info(f"Resetting first_seen_ts for silent vendor '{vendor_id}'.")
            first_seen_ts_state.clear()

            # Reset the timer to check again in another 10 minutes.
            silence_timer.set(now_sec + next_check_delay_sec)

class DetectNullAnomaliesDoFn(beam.DoFn):
    """
    A stateful DoFn to detect anomalies based on the global per-minute count of null dropoff_datetimes.
    It maintains the last 5 minutes of counts to calculate rolling features.
    """
    COUNTS_STATE = BagStateSpec('counts', TupleCoder((VarIntCoder(), VarIntCoder())))
    LAST_ANOMALY_TS_STATE = ReadModifyWriteStateSpec('last_anomaly_ts', VarIntCoder())

    def __init__(self, model_dir):
        self.model_dir = model_dir
        self.model = None
        self.scaler = None
        self.training_stats = None

    def setup(self):
        """Load artifacts on each worker."""
        if not self.model_dir.startswith("gs://"):
            raise ValueError(f"Invalid GCS path for model_dir: {self.model_dir}")

        storage_client = storage.Client()
        path_parts = self.model_dir.replace("gs://", "").split("/", 1)
        bucket_name = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ""
        bucket = storage_client.bucket(bucket_name)

        def load_pickle(filename):
            blob_path = f"{prefix.rstrip('/')}/{filename}"
            blob = bucket.blob(blob_path)
            logging.info(f"Loading artifact from gs://{bucket_name}/{blob_path}")
            serialized_obj = blob.download_as_string()
            return pickle.loads(serialized_obj)

        self.model = load_pickle('model.pkl')
        self.scaler = load_pickle('scaler.pkl')
        self.training_stats = load_pickle('training_stats.pkl')
        # No vendor_encoder for the global model

    def process(self,
                element,
                counts_state=beam.DoFn.StateParam(COUNTS_STATE),
                last_anomaly_ts_state=beam.DoFn.StateParam(LAST_ANOMALY_TS_STATE)):

        key, (timestamp_sec, count) = element
        current_timestamp = datetime.fromtimestamp(timestamp_sec)

        # 1. Update State
        counts_state.add((timestamp_sec, count))

        # 2. Prune old data
        cutoff_sec = timestamp_sec - (5 * 60)
        all_counts_list = list(counts_state.read())
        recent_counts_tuples = [c for c in all_counts_list if c[0] >= cutoff_sec]
        if len(recent_counts_tuples) < len(all_counts_list):
            counts_state.clear()
            for c in recent_counts_tuples:
                counts_state.add(c)

        # 3. Predict
        if len(recent_counts_tuples) >= 1:
            counts_series = pd.Series([c[1] for c in recent_counts_tuples])

            # --- CREATE FEATURES (No vendor encoding) ---
            features = {
                'record_count_1min': count,
                'rolling_mean_5min': counts_series.mean(),
                'rolling_std_5min': counts_series.std() if len(counts_series) > 1 else 0.0,
                'hour_of_day': current_timestamp.hour,
                'day_of_week': current_timestamp.weekday()
            }

            # --- ENFORCE COLUMN ORDER (CRITICAL) ---
            EXPECTED_COLUMNS = [
                'record_count_1min', 'rolling_mean_5min', 'rolling_std_5min',
                'hour_of_day', 'day_of_week'
            ]

            features_df = pd.DataFrame([features])
            features_df = features_df[EXPECTED_COLUMNS]

            # --- SCALE & PREDICT ---
            X_scaled = self.scaler.transform(features_df)
            prediction = self.model.predict(X_scaled)
            score = self.model.decision_function(X_scaled)[0]

            if prediction[0] == -1:
                # Check suppression state before raising a new anomaly
                last_anomaly_ts = last_anomaly_ts_state.read()
                # Suppress for 60 minutes (3600 seconds)
                if last_anomaly_ts is None or (timestamp_sec - last_anomaly_ts > 3600):
                    severity = "high" if score < -0.15 else "medium"
                    anomaly_record = {
                        "timestamp": current_timestamp.isoformat(),
                        "Metric": "NULL_DROPOFF_COUNT_OUTLIER",
                        "severity": severity,
                        "vendor_id": "global", # This is a global metric
                        "count": int(counts_series.sum()),
                        "score": float(score),
                        "details": json.dumps({
                            "realtime_mean_last_5m": features['rolling_mean_5min'],
                            "realtime_std_dev_last_5m": features['rolling_std_5min'],
                            "training_baseline_mean": self.training_stats.get('mean_record_count_1min', 0.0),
                            "training_baseline_std_dev": self.training_stats.get('std_record_count_1min', 0.0)
                        })
                    }
                    logging.warning(f"NULL COUNT ANOMALY DETECTED: {json.dumps(anomaly_record)}")
                    yield anomaly_record

                    # Update state to start new suppression window
                    last_anomaly_ts_state.write(timestamp_sec)
                else:
                    logging.info(f"Suppressing repeated NULL_DROPOFF_COUNT_OUTLIER for key '{key}'.")


class SummarizeAnomaliesWithGeminiFn(beam.DoFn):
    """
    A DoFn that takes a list of anomalies from a time window, formats them into a prompt,
    and calls the Gemini API to generate a summary.
    """
    def __init__(self, project_id, location, sender_email, recipient_email,secret_id):
        self.project_id = project_id
        self.location = location
        self.model = None
        self.sender_email = sender_email
        self.recipient_email = recipient_email
        self.gmail_service = None
        self.secret_id = secret_id

    def setup(self):
        """Initialize the Vertex AI client and model on each worker."""
        vertexai.init(project=self.project_id, location=self.location)
        self.model = GenerativeModel("gemini-2.5-flash")

        try:
            # 1. Initialize Secret Manager Client
            client = secretmanager.SecretManagerServiceClient()

            # 2. Access the secret version
            # secret_id format: "projects/PROJECT_ID/secrets/SECRET_NAME/versions/latest"
            response = client.access_secret_version(request={"name": self.secret_id})
            payload = json.loads(response.payload.data.decode("UTF-8"))
    
            # 3. Create Credentials object with Refresh Token logic
            # This object automatically handles expiry by using the refresh_token
            creds = Credentials(
                    token=payload.get('access_token'),
                    refresh_token=payload.get('refresh_token'),
                    token_uri="https://oauth2.googleapis.com/token",
                    client_id=payload.get('client_id'),
                    client_secret=payload.get('client_secret')
                )
		
		    # Build the Gmail service
            self.gmail_service = build('gmail', 'v1', credentials=creds)

        except Exception as e:
            logging.error(f"Failed to initialize Gmail Service: {e}")


    def process(self, anomaly_list, window=beam.DoFn.WindowParam):
        if not anomaly_list:
            return

        # Convert the list of anomaly dicts to a JSON string for the prompt
        anomalies_json_str = json.dumps(anomaly_list, indent=2)
        window_end_ts = datetime.fromtimestamp(window.end.micros / 1e6, tz=timezone.utc)

        prompt = f"""
        You are a data pipeline monitoring expert for a taxi trip dataset.
        The following is a list of anomalies detected in a streaming data pipeline over the last minute, ending at {window_end_ts.isoformat()}.

        Your task is to provide a concise, human-readable summary of these issues.

        - Group similar anomalies (e.g., multiple statistical outliers for the same vendor).
        - Highlight the most severe or impactful problems first.
        - Mention the specific metrics (e.g., 'STATISTICAL_OUTLIER', 'NO_DATA_RECEIVED') and the affected vendors or global counts.
        - Provide a brief, high-level interpretation of what these anomalies might mean (e.g., "Vendor 2 is sending an unusually high number of records," or "The global count of null drop-off times is spiking, suggesting a data quality issue.").

        Anomalies Detected:
        {anomalies_json_str}

        Please generate the summary now.
        """

        try:
            response = self.model.generate_content(prompt)
            summary_text = response.text

			# Log the summary with a specific identifier for monitoring and alerting.
            # This will appear as a structured log in Cloud Logging.
            logging.info(json.dumps({
                "identifier": "anomaly_summary",
                "summary": summary_text,
                "anomaly_count": len(anomaly_list)
            }))
			
            
            # SEND EMAIL IMMEDIATELY
            if self.gmail_service:
                try:
                    subject = f"Anomalies Alert: {anomaly_count} Anomalies Detected"
                    
                    message = MIMEText(summary_text)
                    message['to'] = self.recipient_email
                    message['from'] = self.sender_email
                    message['subject'] = subject

                    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
                    
                    self.gmail_service.users().messages().send(
                        userId='me', 
                        body={'raw': raw_message}
                    ).execute()
                
                    logging.info(f"Email sent successfully to {self.recipient_email}")
                except Exception as e:
                    logging.error(f"Gmail Send Failed: {e}")
			
            yield {
                "window_timestamp": window_end_ts.isoformat(),
                "summary_text": summary_text,
                "anomaly_count": len(anomaly_list),
                "raw_anomalies_json": anomalies_json_str
            }

        except Exception as e:
            logging.error(f"Failed to generate summary with Gemini: {e}")
            


def run(argv=None):
    """Main entry point; defines and runs the streaming pipeline."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--bootstrap_servers',
        required=True,
        help='Confluent Kafka bootstrap server(s) (e.g., "host:port,host2:port2")'
    )
    parser.add_argument(
        '--kafka_topic',
        required=True,
        help='The Kafka topic to read from.'
    )
    parser.add_argument(
        '--output_table',
        required=True,
        help='BigQuery table to write to, in the format "PROJECT:DATASET.TABLE"'
    )
    parser.add_argument(
        '--model_dir',
        required=True,
        help='GCS path to the directory with saved model artifacts (e.g., gs://bucket/models/).'
    )
    parser.add_argument(
        '--anomaly_output_table',
        required=True,
        help='BigQuery table to write anomalies to, in the format "PROJECT:DATASET.TABLE"'
    )
    parser.add_argument(
        '--summary_output_table',
        required=True,
        help='BigQuery table to write Gemini summaries to, in the format "PROJECT:DATASET.TABLE"'
    )
    parser.add_argument(
        '--api_project',
        required=True,
        help='GCP project ID where the pipeline and Vertex AI are running.'
    )
    parser.add_argument(
        '--api_region',
        required=True,
        help='GCP region where the pipeline and Vertex AI are running.'
    )
    
    # The pipeline options will be automatically parsed from the command line
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_args.extend([
      '--runner=DataflowRunner',
      '--experiments=use_runner_v2',  # Required for Cross-Language transforms
      '--streaming'
	])

    pipeline_options = PipelineOptions(pipeline_args)
    
    warnings.filterwarnings(
    "ignore", 
    message="httplib2 transport does not support per-request timeout"
    )

    # Set a default timeout for all Google Cloud client library interactions
    # to prevent the "httplib2.transport does not support per-request timeout" warning
    # and avoid potential hanging requests.


    # Define the BigQuery table schema
    table_schema = {
        'fields': [
            {'name': 'vendor_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'pickup_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'dropoff_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'insert_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        ]
    }
    
    # Define the schema for the anomaly output table
    anomaly_table_schema = {
        'fields': [
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'Metric', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'severity', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'vendor_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'score', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'details', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    }

    # Define the schema for the Gemini summary output table
    summary_table_schema = {
        'fields': [
            {'name': 'window_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'summary_text', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'anomaly_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'raw_anomalies_json', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    }

    api_key = get_secret(known_args.api_project, 'KAFKA_KEY')
    api_secret = get_secret(known_args.api_project, 'KAFKA_SECRET')
    sasl_jaas_config = (
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{api_key}" '
        f'password="{api_secret}";'
    )
    kafka_consumer_config = {
        'bootstrap.servers': known_args.bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.jaas.config': sasl_jaas_config,
        'ssl.endpoint.identification.algorithm': 'https',
		'group.id': 'bigquery-consumer-group-v1',  
    	'auto.offset.reset': 'earliest'          
    }

    SECRET_PATH = "projects/aipartnercatalyst-confluent-01/secrets/gmail-oauth-creds/versions/latest"
    SENDER = "driftshieldai@gmail.com"
    RECIPIENT = "driftshieldai@gmail.com"

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # 1. Read from Confluent, parse, and assign event timestamps
        messages_with_ts = (
            pipeline
            | 'ReadFromKafka' >> ReadFromKafka(
                consumer_config=kafka_consumer_config,
                topics=[known_args.kafka_topic],
                with_metadata=True
            )
            | 'ParseMessage' >> beam.ParDo(ParseKafkaMessage())
            # Add the insert_timestamp to the message using the current time.
            | 'AddProcessingTimestamp' >> beam.Map(
                lambda msg: {**msg, 'insert_timestamp': datetime.now(timezone.utc).isoformat()})
            # Add a timestamp to each element for windowing. This is the crucial step.
            # The 'insert_timestamp' from the message is used as the event time.
            | 'AddEventTimestamp' >> beam.Map(
                lambda msg: beam.window.TimestampedValue(msg, datetime.fromisoformat(msg['insert_timestamp']).timestamp())
            )
        )

        # Construct full paths to the specific model directories
        # Ensure the base path ends with a slash for proper joining
        base_model_dir = known_args.model_dir if known_args.model_dir.endswith('/') else known_args.model_dir + '/'
        vendor_model_path = os.path.join(base_model_dir, 'record_count_by_vendor/')
        null_count_model_path = os.path.join(base_model_dir, 'null_dropoff_global/')

        # 2. BRANCH 1: Perform real-time anomaly detection for record count per vendor
        vendor_anomalies = (
            messages_with_ts
            # Group into 1-minute fixed windows
            | 'WindowIntoMinutes' >> beam.WindowInto(beam.window.FixedWindows(60))

            # Create (vendor_id, 1) pairs for counting
            | 'MapVendorToCount' >> beam.Map(lambda msg: (msg['vendor_id'], 1))
            
            # Count records per vendor per minute window
            | 'CountPerMinute' >> beam.CombinePerKey(sum)
            
            # Add the window timestamp to the element for stateful processing
            | 'AddWindowTimestamp' >> beam.Map(
                lambda kv, win=beam.DoFn.WindowParam: (kv[0], (int(win.end), kv[1]))
            )
            
            | 'ResultToGlobal' >> beam.WindowInto(beam.window.GlobalWindows())
            
            # Detect anomalies using the stateful DoFn
            | 'DetectVendorAnomalies' >> beam.ParDo(DetectVendorAnomaliesDoFn(vendor_model_path))
        )
        
        # 3. BRANCH 2: Perform real-time anomaly detection for global null dropoff_datetime count
        null_count_anomalies = (
            messages_with_ts
            | 'FilterNullDropoff' >> beam.Filter(lambda msg: msg.get('dropoff_datetime') is None)
            
            # Group into 1-minute fixed windows
            | 'WindowNullsIntoMinutes' >> beam.WindowInto(beam.window.FixedWindows(60))

            # Create ('global_null_dropoff', 1) pairs for counting
            | 'MapNullToCount' >> beam.Map(lambda msg: ('global_null_dropoff', 1))
            
            # Count records per minute window
            | 'CountNullsPerMinute' >> beam.CombinePerKey(sum)
            
            # Add the window timestamp to the element for stateful processing
            | 'AddNullWindowTimestamp' >> beam.Map(
                lambda kv, win=beam.DoFn.WindowParam: (kv[0], (int(win.end), kv[1]))
            )
            
            | 'NullResultToGlobal' >> beam.WindowInto(beam.window.GlobalWindows())
            
            # Detect anomalies using the new stateful DoFn for null counts
            | 'DetectNullCountAnomalies' >> beam.ParDo(DetectNullAnomaliesDoFn(null_count_model_path))
        )
        
        # 4. SINK 1: Flatten and write all detected anomalies to a dedicated BigQuery table
        all_anomalies = (
            (vendor_anomalies, null_count_anomalies)
            | 'FlattenAnomalies' >> beam.Flatten()
        )
        
        (all_anomalies
            | 'WriteAnomaliesToTable' >> beam.io.WriteToBigQuery(
                table=known_args.anomaly_output_table,
                schema=anomaly_table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                # Force streaming inserts
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
                # Disable the shuffle required for deduplication
                ignore_insert_ids=True 
            ))
                
        # 5. BRANCH 3: Summarize anomalies with Gemini every minute
        (all_anomalies
            # Window all anomalies into fixed 1-minute windows based on their event time
            | 'WindowAnomaliesForSummary' >> beam.WindowInto(beam.window.FixedWindows(60))
            
            # Group all anomalies in the window into a single list
            | 'GroupAnomaliesIntoList' >> beam.CombineGlobally(beam.combiners.ToListCombineFn()).without_defaults()
            
            # Call Gemini to summarize the list of anomalies
            | 'SummarizeWithGemini' >> beam.ParDo(SummarizeAnomaliesWithGeminiFn(
                project_id=known_args.api_project,
                location=known_args.api_region,
				sender_email=SENDER, 
				recipient_email=RECIPIENT,
				secret_id=SECRET_PATH
            ))
            
            # SINK 3: Write summaries to their own BigQuery table
            | 'WriteSummaryToBQ' >> beam.io.WriteToBigQuery(
                table=known_args.summary_output_table,
                schema=summary_table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                # Force streaming inserts
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
                # Disable the shuffle required for deduplication
                ignore_insert_ids=True 
            ))

        # 6. SINK 4: Write raw parsed messages to BigQuery for archival
        (messages_with_ts
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            table=known_args.output_table,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # Force streaming inserts
            method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            # Disable the shuffle required for deduplication
            ignore_insert_ids=True 
        ))

 
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
