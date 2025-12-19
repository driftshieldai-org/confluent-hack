import argparse
import os
import pandas as pd
from google.cloud import bigquery, storage
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, LabelEncoder
import pickle

# Set up argument parsing
parser = argparse.ArgumentParser()
parser.add_argument(
    '--table_id',
    type=str,
    default='aipartnercatalyst-confluent-01.driftshield_ai_dataset.trip_data_2021', # table used for training
    help='Full BigQuery table ID to read data from (e.g., project.dataset.table)'
)
parser.add_argument(
    '--billing-project',
    type=str,
    default='aipartnercatalyst-confluent-01',
    help='table ID where execution cost will be charged'
)
parser.add_argument(
    '--model-dir',
    dest='model_dir',
    default='gs://aipartnercatalyst-confluent-bucket/models/',
    type=str,
    help='GCS path to save the trained model.'
)
parser.add_argument(
    '--contamination',
    type=float,
    default=0.05,
    help='The expected proportion of anomalies in the dataset (e.g., 0.05 for 5%).'
)

def save_artifact(storage_client, model_dir, filename, artifact):
    """Serializes an artifact and uploads it to a blob in the specified GCS path."""
    if not model_dir.startswith("gs://"):
        raise ValueError(f"Invalid GCS path for model_dir: {model_dir}")
    
    path_parts = model_dir.replace("gs://", "").split("/", 1)
    bucket_name = path_parts[0]
    prefix = ""
    if len(path_parts) > 1 and path_parts[1]:
        prefix = path_parts[1] if path_parts[1].endswith('/') else path_parts[1] + '/'

    bucket = storage_client.bucket(bucket_name)
    blob_path = f"{prefix}{filename}"
    blob = bucket.blob(blob_path)
    print(f"Saving artifact to gs://{bucket_name}/{blob_path}")
    serialized_artifact = pickle.dumps(artifact)
    blob.upload_from_string(serialized_artifact)

def train_record_count_by_vendor(args, client, storage_client):
    """Trains the model for record count anomalies per vendor."""
    print("Starting training for: record_count_by_vendor")
    print("="*50)

    # 1. Load data from BigQuery
    query = f"""
        SELECT
            TIMESTAMP_TRUNC(insert_timestamp, MINUTE) as time_bucket_minute,
            vendor_id,
            COUNT(vendor_id) as record_count_minute
        FROM `{args.table_id}`
        GROUP BY 1, 2
        ORDER BY 1, 2"""
    
    try:
        df_minute_level = client.query(query).to_dataframe(
            dtypes={"record_count_minute": "int64"}
        )
        print(f"Successfully loaded {len(df_minute_level)} minute-level rows from BigQuery.")
    except Exception as e:
        print(f"Failed to load data for record_count_by_vendor model: {e}")
        return

    if df_minute_level.empty:
        print("No data found for record_count_by_vendor model. Skipping.")
        return

    # 2. Feature Engineering
    df = df_minute_level.copy()
    df.rename(columns={'record_count_minute': 'record_count_1min', 'time_bucket_minute': 'time_bucket_1min'}, inplace=True)
    df.sort_values(by=['vendor_id', 'time_bucket_1min'], inplace=True)
    df.set_index('time_bucket_1min', inplace=True)

    print(f"Using {len(df)} 1-minute interval rows per vendor for feature engineering.")

    df['rolling_mean_5min_vendor'] = df.groupby('vendor_id')['record_count_1min'].rolling(window='5min').mean().reset_index(level=0, drop=True)
    df['rolling_std_5min_vendor'] = df.groupby('vendor_id')['record_count_1min'].rolling(window='5min').std().reset_index(level=0, drop=True)
    df['hour_of_day'] = df.index.hour
    df['day_of_week'] = df.index.dayofweek

    df.dropna(inplace=True)
    print(f"Data shape after feature engineering and dropping NaNs: {df.shape}")

    le_vendor = LabelEncoder()
    df['vendor_id_encoded'] = le_vendor.fit_transform(df['vendor_id'])

    features = [
        'record_count_1min', 'rolling_mean_5min_vendor', 'rolling_std_5min_vendor',
        'vendor_id_encoded', 'hour_of_day', 'day_of_week'
    ]
    X = df[features]
    print(f"Features used for training: {features}")

    # 3. Preprocessing, Training, and Saving
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(n_estimators=100, contamination=args.contamination, random_state=42)
    model.fit(X_scaled)

    training_stats = {
        'mean_record_count_1min': df['record_count_1min'].mean(),
        'std_record_count_1min': df['record_count_1min'].std()
    }
    print(f"Overall training stats: {training_stats}")

    output_dir = os.path.join(args.model_dir, 'record_count_by_vendor')
    save_artifact(storage_client, output_dir, 'model.pkl', model)
    save_artifact(storage_client, output_dir, 'scaler.pkl', scaler)
    save_artifact(storage_client, output_dir, 'vendor_encoder.pkl', le_vendor)
    save_artifact(storage_client, output_dir, 'training_stats.pkl', training_stats)

def train_null_dropoff_global(args, client, storage_client):
    """Trains the model for global null dropoff_datetime count anomalies."""
    print("\n" + "="*50)
    print("Starting training for: null_dropoff_global")
    print("="*50)

    # 1. Load data from BigQuery
    query = f"""
        SELECT
            TIMESTAMP_TRUNC(insert_timestamp, MINUTE) as time_bucket_minute,
            COUNTIF(dropoff_datetime IS NULL) as record_count_minute
        FROM `{args.table_id}`
        GROUP BY 1
        ORDER BY 1"""
    
    try:
        df_minute_level = client.query(query).to_dataframe(
            dtypes={"record_count_minute": "int64"}
        )
        print(f"Successfully loaded {len(df_minute_level)} minute-level rows from BigQuery.")
    except Exception as e:
        print(f"Failed to load data for null_dropoff_global model: {e}")
        return

    if df_minute_level.empty:
        print("No data found for null_dropoff_global model. Skipping.")
        return

    # 2. Feature Engineering
    df = df_minute_level.copy()
    df.rename(columns={'record_count_minute': 'record_count_1min', 'time_bucket_minute': 'time_bucket_1min'}, inplace=True)
    df.sort_values(by='time_bucket_1min', inplace=True)
    df.set_index('time_bucket_1min', inplace=True)

    print(f"Using {len(df)} 1-minute global interval rows for feature engineering.")

    df['rolling_mean_5min'] = df['record_count_1min'].rolling(window='5min').mean()
    df['rolling_std_5min'] = df['record_count_1min'].rolling(window='5min').std()
    df['hour_of_day'] = df.index.hour
    df['day_of_week'] = df.index.dayofweek

    df.dropna(inplace=True)
    print(f"Data shape after feature engineering and dropping NaNs: {df.shape}")

    features = [
        'record_count_1min', 'rolling_mean_5min', 'rolling_std_5min',
        'hour_of_day', 'day_of_week'
    ]
    X = df[features]
    print(f"Features used for training: {features}")

    # 3. Preprocessing, Training, and Saving
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(n_estimators=100, contamination=args.contamination, random_state=42)
    model.fit(X_scaled)

    training_stats = {
        'mean_record_count_1min': df['record_count_1min'].mean(),
        'std_record_count_1min': df['record_count_1min'].std()
    }
    print(f"Overall training stats: {training_stats}")

    output_dir = os.path.join(args.model_dir, 'null_dropoff_global')
    save_artifact(storage_client, output_dir, 'model.pkl', model)
    save_artifact(storage_client, output_dir, 'scaler.pkl', scaler)
    save_artifact(storage_client, output_dir, 'training_stats.pkl', training_stats)

def main():
    """Main training routine to train all models."""
    # Use parse_known_args() to allow Vertex AI to pass its own arguments
    # without causing the script to fail. This should be inside main().
    args, _ = parser.parse_known_args()

    print(f"Starting training job. Reading data from {args.table_id}")
    print(f"Base model directory: {args.model_dir}")

    # Explicitly set the project for billing and permissions.
    client = bigquery.Client(project=args.billing_project)
    storage_client = storage.Client(project=args.billing_project)

    # Train both models
    train_record_count_by_vendor(args, client, storage_client)
    train_null_dropoff_global(args, client, storage_client)

    print("Training job finished successfully.")

if __name__ == '__main__':
    main()
