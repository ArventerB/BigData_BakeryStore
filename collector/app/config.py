import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9095")
TOPIC_PREFIX = os.getenv("TOPIC_PREFIX", "shop")

TOPIC_CLICKSTREAM = f"{TOPIC_PREFIX}.clickstream"
TOPIC_ORDERS = f"{TOPIC_PREFIX}.orders"
TOPIC_PAYMENTS = f"{TOPIC_PREFIX}.payments"
TOPIC_DLQ = f"{TOPIC_PREFIX}.dlq"

PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "client.id": "collector-api",
    # acks=all + idempotence: không mất, không trùng khi producer retry
    "acks": os.getenv("PRODUCER_ACKS", "all"),
    "enable.idempotence": os.getenv("PRODUCER_ACKS", "all") == "all",
    "linger.ms": int(os.getenv("PRODUCER_LINGER_MS", "10")),
    "batch.size": int(os.getenv("PRODUCER_BATCH_SIZE", "65536")),
    "compression.type": os.getenv("PRODUCER_COMPRESSION", "lz4"),
    "queue.buffering.max.messages": 500_000,
}

CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")

# Veracity: số event_id gần nhất được nhớ để phát hiện trùng (trong 1 tiến trình)
DEDUP_CACHE_SIZE = int(os.getenv("DEDUP_CACHE_SIZE", "200000"))
# Event có event_time lệch về tương lai quá ngưỡng này bị coi là lỗi đồng hồ -> DLQ
MAX_FUTURE_SKEW_SEC = int(os.getenv("MAX_FUTURE_SKEW_SEC", "300"))
# Event đến trễ hơn ngưỡng này vẫn nhận nhưng được gắn cờ is_late=true
LATE_THRESHOLD_SEC = int(os.getenv("LATE_THRESHOLD_SEC", "3600"))
