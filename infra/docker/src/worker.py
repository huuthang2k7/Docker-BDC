import json
import os
import sys
import time
from kafka import KafkaConsumer

KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "raw_events")
OUTPUT_DIR = "/app/data/raw"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "consumed_events.jsonl")

if not KAFKA_BROKER:
    print("LỖI: Thiếu biến môi trường bắt buộc KAFKA_BROKER!", file=sys.stderr, flush=True)
    sys.exit(1)

os.makedirs(OUTPUT_DIR, exist_ok=True)
print(f"Worker khởi tạo kết nối tới Kafka Broker: {KAFKA_BROKER}", flush=True)

max_retries = 10
consumer = None
for i in range(max_retries):
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            group_id='p1_worker_group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print("Worker đã kết nối thành công tới Kafka!", flush=True)
        break
    except Exception as e:
        print(f"Worker: Kafka chưa ready (lần {i+1}/{max_retries}): {e}", flush=True)
        time.sleep(2 ** i if (2 ** i) < 30 else 30)

if not consumer:
    sys.exit(1)

print("Worker bắt đầu ghi dữ liệu ra disk...", flush=True)
with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
    for message in consumer:
        data = message.value
        print(f"[WORKER RECEIVED]: {data}", flush=True)
        f.write(json.dumps(data) + "\n")
        f.flush()  # Đảm bảo flush dữ liệu ngay lập tức xuống host disk