from google.cloud import bigquery
import config

client = bigquery.Client()
GCS_PATH = f"gs://{config.BUCKET_NAME}/streaming_data/*.json"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
)

load_job = client.load_table_from_uri(
    GCS_PATH,
    f"{config.PROJECT_ID}.{config.BQ_DATASET}.{config.BQ_TABLE}",
    job_config=job_config,
)

print("⏳ Starting load job...")
load_job.result()
print("✅ Data loaded into BigQuery table.")
