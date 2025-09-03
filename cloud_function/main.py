from google.cloud import bigquery
import os

PROJECT_ID = os.environ.get("PROJECT_ID", "your-project-id")
BQ_DATASET = os.environ.get("BQ_DATASET", "my_dataset")
BQ_TABLE = os.environ.get("BQ_TABLE", "streaming_table")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "my-streaming-bucket")

def load_gcs_to_bq(request):
    """Triggered by Cloud Scheduler → loads GCS JSON into BigQuery."""
    client = bigquery.Client()
    GCS_PATH = f"gs://{BUCKET_NAME}/streaming_data/*.json"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_uri(
        GCS_PATH,
        f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
        job_config=job_config,
    )

    print("⏳ Starting load job...")
    load_job.result()
    print("✅ Data loaded into BigQuery table.")

    return "Load job completed!"
