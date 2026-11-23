import json
import os
import signal
import sys
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError

running = True
HEARTBEAT_FILE = "/tmp/worker_heartbeat"


def touch_heartbeat():
    """Cập nhật timestamp file heartbeat để K8s startup/liveness/readiness probe kiểm tra."""
    with open(HEARTBEAT_FILE, "a", encoding="utf-8"):
        os.utime(HEARTBEAT_FILE, None)


def handle_shutdown(signum, frame):
    global running
    print(f"\nNhận tín hiệu dừng ({signum}), đang đóng worker an toàn...", flush=True)
    running = False

def interruptible_sleep(seconds):
    """Ngủ nhưng thoát sớm nếu đã nhận tín hiệu dừng."""
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
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "worker-group").strip()
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "/app/data/raw").strip()
EVENT_ID_FIELD = os.getenv("EVENT_ID_FIELD", "event_id").strip()
if not KAFKA_BROKER:
    print("LỖI: Thiếu biến môi trường bắt buộc KAFKA_BROKER!", file=sys.stderr, flush=True)
    sys.exit(1)

os.makedirs(RAW_DATA_PATH, exist_ok=True)
OUTPUT_FILE = os.path.join(RAW_DATA_PATH, "consumed_events.jsonl")

def extract_event_id(line):
    """Lấy event_id từ một dòng JSON; trả về None nếu không parse được hoặc không có trường này."""
    try:
        obj = json.loads(line)
    except ValueError:
        return None
    if isinstance(obj, dict) and obj.get(EVENT_ID_FIELD) is not None:
        return str(obj[EVENT_ID_FIELD])
    return None

def load_seen_ids(path):
    seen = set()
    if not os.path.exists(path):
        return seen
    good_len = 0
    with open(path, "rb") as f:
        for raw in f:
            if not raw.endswith(b"\n"):
                break
            good_len += len(raw)
            event_id = extract_event_id(raw.decode("utf-8", errors="replace"))
            if event_id is not None:
                seen.add(event_id)
    if good_len < os.path.getsize(path):
        with open(path, "rb+") as f:
            f.truncate(good_len)
            f.flush()
            os.fsync(f.fileno())
        print("Đã cắt dòng cuối dở dang do lần chạy trước bị dừng đột ngột.", flush=True)
    print(f"Đã nạp {len(seen)} event_id từ {path} để chống trùng lặp.", flush=True)
    return seen

def connect_consumer(max_retries=10):
    delay = 2
    for attempt in range(1, max_retries + 1):
        if not running:
            sys.exit(0)
        try:
            print(f"Worker khởi tạo kết nối tới Kafka Broker: {KAFKA_BROKER} (lần {attempt}/{max_retries})...", flush=True)
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
            )
            print("Worker đã kết nối thành công tới Kafka!", flush=True)
            touch_heartbeat()
            return consumer
        except Exception as e:
            print(f"Cảnh báo: chưa kết nối được Kafka ({e!r}).", file=sys.stderr, flush=True)
            if attempt < max_retries:
                print(f"Thử lại sau {delay}s...", file=sys.stderr, flush=True)
                interruptible_sleep(delay)
                delay = min(delay * 2, 16)

    print(f"LỖI: Không thể kết nối tới Kafka Broker ({KAFKA_BROKER}) sau {max_retries} lần thử!", file=sys.stderr, flush=True)
    sys.exit(1)

def main():
    seen_ids = load_seen_ids(OUTPUT_FILE)
    consumer = connect_consumer()
    print(f"Worker bắt đầu ghi dữ liệu raw vào {OUTPUT_FILE}...", flush=True)

    try:
        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            while running:
                records = consumer.poll(timeout_ms=1000)
                touch_heartbeat() 

                if not records:
                    continue

                for messages in records.values():
                    for msg in messages:
                        raw_line = msg.value.strip()
                        if not raw_line:
                            continue
                        event_id = extract_event_id(raw_line)
                        if event_id is not None:
                            if event_id in seen_ids:
                                print(f"[WORKER SKIP DUPLICATE] partition={msg.partition} offset={msg.offset} event_id={event_id}", flush=True)
                                continue
                            seen_ids.add(event_id)
                        f.write(raw_line + "\n")
                        print(f"[WORKER RECEIVED] partition={msg.partition} offset={msg.offset}: {raw_line}", flush=True)
                f.flush()
                os.fsync(f.fileno())
                consumer.commit()
    except KafkaError as e:
        print(f"LỖI Kafka trong quá trình consume: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        if os.path.exists(HEARTBEAT_FILE):
            os.remove(HEARTBEAT_FILE)
        consumer.close()
        print("Worker đã đóng kết nối Kafka và thoát sạch.", flush=True)


if __name__ == "__main__":
    main()