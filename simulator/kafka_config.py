"""Kết nối Kafka theo hạ tầng của TV1 (scripts/).

- Topic: scripts/create_topics.ps1 (bakery.user-events | bakery.orders | bakery.payments | bakery.dlq)
- Broker + cấu hình producer: scripts/producer_config.py, viết cho kafka-python (tên khoá kiểu linger_ms).
  Simulator dùng confluent-kafka (librdkafka) nên đổi sang tên khoá kiểu linger.ms.
  Đọc trực tiếp file của TV1 để khi TV1 đổi cấu hình thì simulator đổi theo.
"""
import importlib.util
from pathlib import Path

from generator import CLICK_TYPES

PRODUCER_CONFIG_PATH = Path(__file__).resolve().parent.parent / "scripts" / "producer_config.py"

TOPIC_PREFIX = "bakery"
TOPIC_SUFFIX = {"order_placed": "orders", "payment": "payments", **{t: "user-events" for t in CLICK_TYPES}}

# kafka-python -> librdkafka
_KEY_MAP = {
    "acks": "acks",
    "retries": "retries",
    "max_in_flight_requests_per_connection": "max.in.flight.requests.per.connection",
    "batch_size": "batch.size",
    "linger_ms": "linger.ms",
    "compression_type": "compression.type",
}


def topic_for(event, prefix: str = TOPIC_PREFIX) -> str:
    t = event.get("event_type") if isinstance(event, dict) else None
    return f"{prefix}.{TOPIC_SUFFIX.get(t, 'dlq')}"


def _load_tv1(path: Path = PRODUCER_CONFIG_PATH):
    spec = importlib.util.spec_from_file_location("tv1_producer_config", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def to_librdkafka(conf: dict) -> dict:
    out = {_KEY_MAP[k]: v for k, v in conf.items() if k in _KEY_MAP}
    out["acks"] = str(out.get("acks", "all"))
    # idempotence chỉ bật được với acks=all; nó giữ đúng thứ tự khi retry như max_in_flight=1 của TV1
    out["enable.idempotence"] = out["acks"] in ("all", "-1")
    return out


def tv1_profiles(path: Path = PRODUCER_CONFIG_PATH) -> tuple[str, dict, dict]:
    """Trả về (bootstrap, cấu hình đơn hàng/thanh toán, cấu hình user-events)."""
    mod = _load_tv1(path)
    bootstrap = ",".join(mod.BOOTSTRAP_SERVERS_HOST)
    return bootstrap, to_librdkafka(mod.CRITICAL_PRODUCER_CONFIG), to_librdkafka(mod.HIGH_THROUGHPUT_PRODUCER_CONFIG)


def default_bootstrap() -> str:
    try:
        return tv1_profiles()[0]
    except (OSError, AttributeError):
        return "localhost:9092,localhost:9094,localhost:9096"
