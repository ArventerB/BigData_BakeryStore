import json
import logging
import threading
from collections import Counter

from confluent_kafka import Producer

from . import config

log = logging.getLogger("collector.producer")


class KafkaSink:
    """Bọc confluent_kafka.Producer: gửi bất đồng bộ, đếm số bản ghi giao thành công/thất bại."""

    def __init__(self, conf: dict):
        self._producer = Producer(conf)
        self.delivered = Counter()
        self.failed = Counter()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        # Thread riêng gọi poll() để xử lý callback giao nhận
        self._poller = threading.Thread(target=self._poll_loop, daemon=True)
        self._poller.start()

    def _poll_loop(self):
        while not self._stop.is_set():
            self._producer.poll(0.1)

    def _on_delivery(self, err, msg):
        with self._lock:
            if err is not None:
                self.failed[msg.topic()] += 1
                log.error("Gửi thất bại topic=%s: %s", msg.topic(), err)
            else:
                self.delivered[msg.topic()] += 1

    def send(self, topic: str, key: str, value: dict):
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        while True:
            try:
                self._producer.produce(topic, key=key.encode("utf-8"), value=payload, on_delivery=self._on_delivery)
                return
            except BufferError:
                # Hàng đợi nội bộ đầy (back-pressure): chờ broker xác nhận bớt rồi thử lại
                self._producer.poll(0.05)

    def queue_len(self) -> int:
        return len(self._producer)

    def close(self, timeout: float = 10):
        self._producer.flush(timeout)
        self._stop.set()


def build_sink() -> KafkaSink:
    return KafkaSink(config.PRODUCER_CONFIG)
