"""Collector API: cửa nhận event duy nhất từ web, app, backend và simulator rồi đẩy vào Kafka.

Luồng xử lý một request POST /events:
  1. Đọc body JSON: một object {...} hoặc một lô [{...}, {...}].
  2. Với từng event: kiểm schema (schemas.py) -> kiểm trùng event_id (dedup.py).
       hợp lệ  -> thêm ingest_time, is_late -> topic theo event_type, key = user_id
       lỗi     -> {reason, detail, ingest_time, raw} -> bakery.dlq (không làm hỏng cả lô)
  3. Chờ Kafka xác nhận toàn bộ lô rồi mới trả 200 {"received", "accepted", "rejected"}.
     Nếu Kafka không nhận được thì trả 503 để client gửi lại, tránh báo thành công giả gây mất dữ liệu.

Chạy local (trong thư mục collector/):  python -m app.main
Swagger: http://localhost:8000/docs
"""
import asyncio
import json
import logging
import time
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from . import config
from .dedup import DedupCache
from .producer import KafkaSender
from .schemas import REQUIRED_TOPICS, ROUTES, validate_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("collector")

LATE_THRESHOLD = timedelta(seconds=config.LATE_THRESHOLD_SECONDS)


def iso_utc(t: datetime) -> str:
    return t.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Metrics:
    """Bộ đếm từ lúc khởi động. Chỉ đếm message Kafka đã xác nhận, nên khớp với số đọc được từ topic."""

    def __init__(self):
        self.started = time.time()
        self.requests = 0
        self.failed_requests = 0
        self.accepted_by_topic: Counter = Counter()
        self.accepted_by_event_type: Counter = Counter()
        self.rejected_by_reason: Counter = Counter()

    def snapshot(self, dedup_size: int) -> dict:
        return {
            "started_at": iso_utc(datetime.fromtimestamp(self.started, timezone.utc)),
            "uptime_seconds": round(time.time() - self.started, 1),
            "requests_total": self.requests,
            "failed_requests_total": self.failed_requests,
            "accepted_total": sum(self.accepted_by_topic.values()),
            "rejected_total": sum(self.rejected_by_reason.values()),
            "accepted_by_topic": dict(self.accepted_by_topic),
            "accepted_by_event_type": dict(self.accepted_by_event_type),
            "rejected_by_reason": dict(self.rejected_by_reason),
            "dedup_cache_size": dedup_size,
        }


sender: KafkaSender | None = None
dedup = DedupCache(config.DEDUP_CACHE_SIZE)
metrics = Metrics()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global sender
    sender = KafkaSender()
    sender.start()
    log.info("Collector sẵn sàng. Topic: %s", ", ".join(REQUIRED_TOPICS))
    yield
    log.info("Đang tắt Collector...")
    await asyncio.to_thread(sender.close)


app = FastAPI(
    title="Bakery Collector API",
    version="1.0.0",
    description="Nhận event hành vi và giao dịch của tiệm bánh, kiểm tra data contract, đẩy vào Kafka.",
    lifespan=lifespan,
)
# web tiệm bánh chạy ở domain/cổng khác nên trình duyệt cần CORS mới gọi được /events
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.post("/events")
async def ingest(request: Request):
    """Nhận 1 event hoặc 1 lô event. Body đọc thủ công (không ràng buộc Content-Type)
    để nhận được cả navigator.sendBeacon của trình duyệt, vốn gửi text/plain."""
    metrics.requests += 1
    body = await request.body()
    try:
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="Body không phải JSON hợp lệ (UTF-8)")

    events = payload if isinstance(payload, list) else [payload]
    if len(events) > config.MAX_BATCH_SIZE:
        raise HTTPException(status_code=413, detail=f"Lô tối đa {config.MAX_BATCH_SIZE} event")

    now = datetime.now(timezone.utc)
    ingest_time = iso_utc(now)
    pending = []  # (future, topic, event_type hoặc None, lý do lỗi hoặc None, event_id đã ghi vào dedup)

    for raw in events:
        model, reason, detail = validate_event(raw)
        if model is not None:
            eid = str(model.event_id)
            if dedup.add(eid):
                out = dict(raw)
                out["ingest_time"] = ingest_time
                out["is_late"] = now - model.event_time > LATE_THRESHOLD  # đến muộn (app offline rồi đồng bộ lại)
                topic = ROUTES[model.event_type]
                pending.append((sender.send(topic, model.user_id, out), topic, model.event_type, None, eid))
                continue
            reason, detail = "duplicate_event_id", f"event_id={eid} đã được nhận trước đó"

        dlq_msg = {"reason": reason, "detail": detail, "ingest_time": ingest_time, "raw": raw}
        uid = raw.get("user_id") if isinstance(raw, dict) else None
        key = uid if isinstance(uid, str) and uid else None
        pending.append((sender.send(config.TOPIC_DLQ, key, dlq_msg), config.TOPIC_DLQ, None, reason, None))

    # chờ Kafka xác nhận cả lô
    futures = [p[0] for p in pending]
    try:
        results = await asyncio.wait_for(asyncio.gather(*futures, return_exceptions=True),
                                         timeout=config.DELIVERY_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        results = [f.result() if f.done() and not f.cancelled() and f.exception() is None
                   else TimeoutError("quá hạn chờ Kafka") for f in futures]

    accepted = rejected = failed = 0
    first_error = None
    for (_, topic, etype, reason, eid), res in zip(pending, results):
        if isinstance(res, BaseException):
            failed += 1
            first_error = first_error or res
            if eid:
                dedup.discard(eid)  # chưa vào Kafka thì lần gửi lại không được tính là trùng
        elif reason is None:
            accepted += 1
            metrics.accepted_by_topic[topic] += 1
            metrics.accepted_by_event_type[etype] += 1
        else:
            rejected += 1
            metrics.rejected_by_reason[reason] += 1

    if failed:
        metrics.failed_requests += 1
        log.error("Kafka không nhận %d/%d message: %s", failed, len(events), first_error)
        return JSONResponse(status_code=503, content={
            "error": "kafka_unavailable",
            "detail": f"Kafka không xác nhận {failed}/{len(events)} event, hãy gửi lại cả lô",
            "received": len(events), "accepted": accepted, "rejected": rejected, "failed": failed,
        })
    return {"received": len(events), "accepted": accepted, "rejected": rejected}


@app.get("/health")
async def health():
    """API còn sống và kết nối được Kafka, đủ topic cần thiết."""
    try:
        info = await asyncio.to_thread(sender.ping, 3.0)
    except Exception as e:  # noqa: BLE001 - mọi lỗi kết nối đều là "Kafka không tới được"
        return JSONResponse(status_code=503, content={"status": "degraded", "kafka": "unreachable", "error": str(e)})
    missing = [t for t in REQUIRED_TOPICS if t not in info["topics"]]
    body = {
        "status": "ok" if not missing else "degraded",
        "kafka": "ok",
        "brokers": info["brokers"],
        "metadata_latency_ms": info["latency_ms"],
        "missing_topics": missing,
    }
    return JSONResponse(status_code=200 if not missing else 503, content=body)


@app.get("/metrics")
async def get_metrics():
    """Số event đã nhận từ lúc khởi động. Đối chiếu rejected_by_reason với dirty_by_kind của simulator."""
    return metrics.snapshot(len(dedup))


@app.get("/", include_in_schema=False)
async def root():
    return {"service": "bakery-collector", "docs": "/docs", "events": "POST /events", "health": "/health",
            "metrics": "/metrics"}


if __name__ == "__main__":
    import uvicorn

    # 1 worker: bộ nhớ chống trùng nằm trong tiến trình (xem dedup.py)
    uvicorn.run("app.main:app", host="0.0.0.0", port=config.PORT)
