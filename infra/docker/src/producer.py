import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

EVENT_INTERVAL_SECONDS = 2 
running = True

def handle_shutdown(signum, frame):
    global running
    print(f"\nNhận tín hiệu dừng ({signum}), đang đóng producer an toàn...", flush=True)
    running = False

def interruptible_sleep(seconds):
    end = time.monotonic() + seconds
    while running:
        remaining = end - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.5, remaining))

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "").strip()
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events").strip()

if not KAFKA_BROKER:
    print("LỖI: Thiếu biến môi trường bắt buộc KAFKA_BROKER!", file=sys.stderr, flush=True)
    sys.exit(1)


def connect_producer(max_retries=10):
    delay = 2
    for attempt in range(1, max_retries + 1):
        if not running:
            sys.exit(0)
        try:
            print(f"Producer kết nối tới Kafka Broker: {KAFKA_BROKER} (lần {attempt}/{max_retries})...", flush=True)
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            print("Producer đã kết nối thành công tới Kafka!", flush=True)
            return producer
        except Exception as e:
            print(f"Cảnh báo: chưa kết nối được Kafka ({e!r}).", file=sys.stderr, flush=True)
            if attempt < max_retries:
                print(f"Thử lại sau {delay}s...", file=sys.stderr, flush=True)
                interruptible_sleep(delay)
                delay = min(delay * 2, 16)

    print(f"LỖI: Không thể kết nối tới Kafka Broker ({KAFKA_BROKER}) sau {max_retries} lần thử!", file=sys.stderr, flush=True)
    sys.exit(1)

def main():
    producer = connect_producer()
    run_id = uuid.uuid4().hex[:8]  
    counter = 0
    actions = ["view_product", "add_to_cart", "click", "checkout"]
    exit_code = 0

    print(f"Producer bắt đầu phát dữ liệu (run_id={run_id}, topic={KAFKA_TOPIC})...", flush=True)
    try:
        while running:
            counter += 1
            event = {
                "event_id": f"{run_id}-{counter}",
                "user_id": random.randint(1000, 9999),
                "action": random.choice(actions),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            future = producer.send(KAFKA_TOPIC, value=event)
            future.get(timeout=10)
            print(f"[PRODUCER SENT]: {event}", flush=True)
            interruptible_sleep(EVENT_INTERVAL_SECONDS)
    except KafkaError as e:
        print(f"LỖI: Mất kết nối Kafka khi đang gửi dữ liệu ({e!r})", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        try:
            producer.flush(timeout=5)
        except Exception as e:
            print(f"Cảnh báo: flush không hoàn tất ({e!r})", file=sys.stderr, flush=True)
        producer.close(timeout=5)
        print("Producer đã đóng kết nối và thoát sạch.", flush=True)
    sys.exit(exit_code)

if __name__ == "__main__":
    main()