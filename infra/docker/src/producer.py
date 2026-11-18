import json
import os
import random
import sys
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "raw_events")

print(f"Producer đang khởi tạo kết nối tới Kafka Broker: {KAFKA_BROKER}", flush=True)

producer = None
max_retries = 10
for i in range(max_retries):
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Producer đã kết nối thành công tới Kafka!", flush=True)
        break
    except Exception as e:
        print(f"Producer: Kafka chưa ready (lần {i+1}/{max_retries}): {e}", flush=True)
        time.sleep(2 ** i if (2 ** i) < 30 else 30)

if not producer:
    sys.exit(1)

actions = ["click", "view_product", "add_to_cart", "checkout"]
event_id = 1

print("Producer bắt đầu phát dữ liệu giả lập...", flush=True)
while True:
    payload = {
        "event_id": event_id,
        "user_id": random.randint(1000, 9999),
        "action": random.choice(actions),
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send(TOPIC_NAME, value=payload)
    producer.flush()
    print(f"[PRODUCER SENT]: {payload}", flush=True)
    event_id += 1
    time.sleep(2)