# Pub/Sub → GCS → BigQuery Streaming Pipeline

This project demonstrates a simple data pipeline in GCP:

1. **publisher.py** → Generates random events & publishes to Pub/Sub.
2. **subscriber_to_gcs.py** → Consumes messages & writes JSON to GCS.
3. **load_gcs_to_bq.py** → Loads JSON data from GCS into BigQuery.

---

## 🚀 Setup

1. Enable APIs:
   - Pub/Sub
   - Cloud Storage
   - BigQuery

2. Create resources:
   ```bash
   gcloud pubsub topics create my-topic
   gcloud pubsub subscriptions create my-sub --topic=my-topic
   gsutil mb gs://my-streaming-bucket
   bq --location=US mk -d my_dataset
   bq mk --table my_dataset.streaming_table user_id:INTEGER,event:STRING,amount:FLOAT,timestamp:TIMESTAMP
