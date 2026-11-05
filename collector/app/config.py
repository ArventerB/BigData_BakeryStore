"""Cấu hình Collector, đọc từ biến môi trường (hoặc file .env cạnh nơi chạy lệnh).

Thông số producer lấy theo chuẩn của TV1 trong scripts/producer_config.py, đổi sang tên khoá
của librdkafka (linger_ms -> linger.ms). Khi TV1 đổi chuẩn thì sửa cả ở đây.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except ValueError:
        raise SystemExit(f"Biến môi trường {name} phải là số nguyên, đang là {os.getenv(name)!r}")


def _in_docker() -> bool:
    return Path("/.dockerenv").exists()


# Broker: ưu tiên biến môi trường; nếu không có thì tự chọn theo nơi đang chạy
BOOTSTRAP_HOST = "localhost:9092,localhost:9094,localhost:9096"
BOOTSTRAP_DOCKER = "kafka-1:29092,kafka-2:29092,kafka-3:29092"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or (BOOTSTRAP_DOCKER if _in_docker() else BOOTSTRAP_HOST)

PORT = _env_int("PORT", 8000)

# Topic (do TV1 tạo bằng scripts/create_topics.ps1, Kafka không tự tạo topic)
TOPIC_PREFIX = os.getenv("TOPIC_PREFIX", "bakery")
TOPIC_USER_EVENTS = f"{TOPIC_PREFIX}.user-events"
TOPIC_ORDERS = f"{TOPIC_PREFIX}.orders"
TOPIC_PAYMENTS = f"{TOPIC_PREFIX}.payments"
TOPIC_DLQ = f"{TOPIC_PREFIX}.dlq"

# Kiểm tra dữ liệu
DEDUP_CACHE_SIZE = _env_int("DEDUP_CACHE_SIZE", 50_000)          # số event_id gần nhất được nhớ
MAX_FUTURE_SKEW_SECONDS = _env_int("MAX_FUTURE_SKEW_SECONDS", 300)  # event_time được phép sớm hơn giờ server 5 phút
LATE_THRESHOLD_SECONDS = _env_int("LATE_THRESHOLD_SECONDS", 3600)   # trễ hơn 1 giờ thì gắn is_late=true
MAX_BATCH_SIZE = _env_int("MAX_BATCH_SIZE", 5000)                  # số event tối đa trong một request

# Thời gian tối đa chờ Kafka xác nhận một lô; quá hạn thì trả 503 để client gửi lại
DELIVERY_TIMEOUT_SECONDS = _env_int("DELIVERY_TIMEOUT_SECONDS", 30)

# Web tiệm bánh chạy ở domain khác nên phải cho phép CORS. "*" = mọi domain (đủ cho đồ án)
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]

_COMMON = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "bakery-collector",
    # librdkafka tự kết nối lại khi mất broker; hai dòng dưới chỉ chỉnh nhịp thử lại
    "reconnect.backoff.ms": 100,
    "reconnect.backoff.max.ms": 5000,
    # một message không gửi được trong 20s thì báo lỗi về delivery callback
    "message.timeout.ms": 20_000,
}

# orders / payments / dlq: không được mất -> chờ đủ ISR ghi (acks=all), bật idempotence để retry không sinh bản trùng
_CRITICAL_ACKS = os.getenv("CRITICAL_ACKS", "all")
CRITICAL_PRODUCER_CONFIG = {
    **_COMMON,
    "acks": _CRITICAL_ACKS,
    # idempotence chỉ dùng được với acks=all (khi thí nghiệm đổi acks thì tự tắt)
    "enable.idempotence": _CRITICAL_ACKS in ("all", "-1"),
    "retries": 5,
    "max.in.flight.requests.per.connection": 1,
    "batch.size": _env_int("CRITICAL_BATCH_SIZE", 65_536),
    "linger.ms": _env_int("CRITICAL_LINGER_MS", 10),
    "compression.type": os.getenv("COMPRESSION", "lz4"),
}

# user-events (clickstream): lưu lượng lớn, mất vài event không ảnh hưởng doanh thu -> acks=1, batch lớn
HIGH_THROUGHPUT_PRODUCER_CONFIG = {
    **_COMMON,
    "acks": os.getenv("CLICK_ACKS", "1"),
    "batch.size": _env_int("CLICK_BATCH_SIZE", 131_072),
    "linger.ms": _env_int("CLICK_LINGER_MS", 20),
    "compression.type": os.getenv("COMPRESSION", "lz4"),
}
