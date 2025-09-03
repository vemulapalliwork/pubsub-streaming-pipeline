import time
import json
import random
from google.cloud import pubsub_v1
import config

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(config.PROJECT_ID, config.TOPIC_ID)

def get_random_record():
    return {
        "user_id": random.randint(1000, 9999),
        "event": random.choice(["click", "view", "purchase"]),
        "amount": round(random.uniform(10.0, 500.0), 2),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

def publish_data():
    while True:
        record = get_random_record()
        data = json.dumps(record).encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(f"✅ Published: {record}")
        time.sleep(2)

if __name__ == "__main__":
    publish_data()
