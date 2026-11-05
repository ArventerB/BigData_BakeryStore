"""Bọc Kafka producer (confluent-kafka) để dùng được trong FastAPI async.

- Hai producer: CRITICAL (acks=all, idempotence) cho orders/payments/dlq và HIGH_THROUGHPUT (acks=1)
  cho user-events, theo chuẩn của TV1.
- produce() của librdkafka không chặn: message vào hàng đợi nội bộ, được gom batch rồi gửi.
  Kết quả (thành công / lỗi) trả về qua delivery callback, chạy trong luồng poll riêng.
  Mỗi lần send() trả về một asyncio.Future, callback sẽ hoàn tất Future đó, nên main.py
  có thể await đến khi Kafka xác nhận rồi mới trả lời client (không trả 200 khi dữ liệu chưa an toàn).
- Kết nối lại: librdkafka tự kết nối lại broker khi mất mạng / broker chết và tự chuyển sang leader mới;
  message chưa gửi được nằm trong hàng đợi và được thử lại đến khi hết message.timeout.ms.
"""
import asyncio
import json
import logging
import threading
import time

from confluent_kafka import KafkaException, Producer

from .config import CRITICAL_PRODUCER_CONFIG, HIGH_THROUGHPUT_PRODUCER_CONFIG, TOPIC_USER_EVENTS

log = logging.getLogger("collector.producer")


def serialize(value) -> bytes:
    """JSON UTF-8, giữ nguyên tiếng Việt (ensure_ascii=False), bỏ khoảng trắng thừa."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")


def _finish(fut: asyncio.Future, result=None, error: Exception | None = None):
    # Future có thể đã bị huỷ (request quá hạn) trước khi Kafka trả lời
    if fut.done():
        return
    if error is not None:
        fut.set_exception(error)
    else:
        fut.set_result(result)


class KafkaSender:
    BUFFER_RETRIES = 50  # hàng đợi nội bộ đầy thì chờ tối đa ~5s cho nó vơi bớt

    def __init__(self):
        self._critical = Producer(CRITICAL_PRODUCER_CONFIG)
        self._fast = Producer(HIGH_THROUGHPUT_PRODUCER_CONFIG)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._poll_loop, name="kafka-poll", daemon=True)

    # ---------------------------------------------------------------- vòng đời
    def start(self):
        self._thread.start()
        log.info("Đã khởi động Kafka producer: %s", CRITICAL_PRODUCER_CONFIG["bootstrap.servers"])

    def _poll_loop(self):
        """poll() là nơi librdkafka gọi delivery callback; phải gọi liên tục."""
        while not self._stop.is_set():
            self._critical.poll(0.05)
            self._fast.poll(0.05)

    def close(self, timeout: float = 10.0):
        """Dừng luồng poll và đẩy nốt message còn trong hàng đợi trước khi tắt."""
        self._stop.set()
        self._thread.join(timeout=2)
        left = self._critical.flush(timeout) + self._fast.flush(timeout)
        if left:
            log.warning("Tắt Collector khi còn %d message chưa gửi được", left)
        else:
            log.info("Đã gửi hết message, đóng producer")

    # ---------------------------------------------------------------- gửi
    def _producer_for(self, topic: str) -> Producer:
        return self._fast if topic == TOPIC_USER_EVENTS else self._critical

    def send(self, topic: str, key: str | None, value) -> asyncio.Future:
        """Đưa một message vào hàng đợi. Future trả về (partition, offset) khi broker xác nhận,
        hoặc ném KafkaException khi gửi thất bại. Phải gọi trong event loop."""
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        def on_delivery(err, msg):
            # chạy trong luồng poll (hoặc luồng gọi poll ở dưới) -> chuyển kết quả về event loop
            if err is not None:
                loop.call_soon_threadsafe(_finish, fut, None, KafkaException(err))
            else:
                loop.call_soon_threadsafe(_finish, fut, (msg.partition(), msg.offset()))

        producer = self._producer_for(topic)
        data = serialize(value)
        # key = user_id: mọi event của một khách vào cùng partition nên giữ đúng thứ tự
        kbytes = key.encode("utf-8") if key else None
        for _ in range(self.BUFFER_RETRIES):
            try:
                producer.produce(topic, value=data, key=kbytes, on_delivery=on_delivery)
                return fut
            except BufferError:
                # hàng đợi nội bộ đầy (Kafka chậm / mất kết nối): chờ một chút cho message cũ đi bớt
                producer.poll(0.1)
            except KafkaException as e:
                _finish(fut, error=e)
                return fut
        _finish(fut, error=KafkaException("Hàng đợi producer đầy quá lâu, Kafka không nhận kịp"))
        return fut

    # ---------------------------------------------------------------- kiểm tra kết nối
    def ping(self, timeout: float = 3.0) -> dict:
        """Lấy metadata cụm (danh sách broker, topic). Ném KafkaException nếu không kết nối được."""
        t0 = time.perf_counter()
        md = self._critical.list_topics(timeout=timeout)
        return {
            "brokers": sorted(f"{b.host}:{b.port}" for b in md.brokers.values()),
            "topics": sorted(t for t in md.topics if not t.startswith("__")),
            "latency_ms": round((time.perf_counter() - t0) * 1000, 1),
        }
