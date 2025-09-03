import json
import time
from google.cloud import pubsub_v1, storage
import config

storage_client = storage.Client()
bucket = storage_client.bucket(config.BUCKET_NAME)
OUTPUT_FILE = f"streaming_data/records_{int(time.time())}.json"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(config.PROJECT_ID, config.SUBSCRIPTION_ID)

buffer = []

def callback(message):
    global buffer
    record = json.loads(message.data.decode("utf-8"))
    buffer.append(record)
    message.ack()
    print(f"📥 Received: {record}")

    if len(buffer) >= 10:
        blob = bucket.blob(OUTPUT_FILE)
        blob.upload_from_string("\n".join([json.dumps(r) for r in buffer]))
        print(f"☁️ Uploaded {len(buffer)} records to GCS: {OUTPUT_FILE}")
        buffer.clear()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print("🚀 Listening for messages...")

with subscriber:
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
