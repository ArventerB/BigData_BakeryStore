"""Collector REST API: cổng nhận event từ web/app/simulator, kiểm tra schema rồi đẩy vào Kafka.

Endpoint:
    POST /events     nhận 1 event (object) hoặc 1 lô event (array)
    GET  /health     kiểm tra sống
    GET  /metrics    số liệu nhận/từ chối/đã giao theo topic
"""
import json
import logging
import time
from collections import Counter, OrderedDict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError

from . import config
from .producer import build_sink
from .schemas import ROUTES

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("collector")

state: dict[str, Any] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    state["sink"] = build_sink()
    state["accepted"] = Counter()
    state["rejected"] = Counter()  # theo lý do
    state["seen_ids"] = OrderedDict()
    state["started_at"] = time.time()
    log.info("Collector kết nối Kafka: %s", config.KAFKA_BOOTSTRAP)
    yield
    state["sink"].close()


app = FastAPI(title="Ingestion Collector API", version="1.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=config.CORS_ORIGINS, allow_methods=["*"], allow_headers=["*"])


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _is_duplicate(event_id: str) -> bool:
    seen: OrderedDict = state["seen_ids"]
    if event_id in seen:
        return True
    seen[event_id] = None
    if len(seen) > config.DEDUP_CACHE_SIZE:
        seen.popitem(last=False)
    return False


def _to_dlq(raw: Any, reason: str, detail: str = ""):
    state["rejected"][reason] += 1
    key = raw.get("user_id", "unknown") if isinstance(raw, dict) else "unknown"
    state["sink"].send(config.TOPIC_DLQ, str(key), {
        "reason": reason,
        "detail": detail[:1000],
        "ingest_time": _now().isoformat(),
        "raw": raw,
    })


def _process(raw: Any) -> bool:
    """Trả về True nếu event hợp lệ và đã đưa vào topic chính."""
    if not isinstance(raw, dict):
        _to_dlq(raw, "not_an_object")
        return False

    route = ROUTES.get(raw.get("event_type"))
    if route is None:
        _to_dlq(raw, "unknown_event_type", str(raw.get("event_type")))
        return False
    model, topic = route

    try:
        event = model.model_validate(raw)
    except ValidationError as e:
        _to_dlq(raw, "schema_violation", str(e.errors(include_url=False)))
        return False

    now = _now()
    if event.event_time.tzinfo is None:
        _to_dlq(raw, "missing_timezone")
        return False
    skew = (event.event_time - now).total_seconds()
    if skew > config.MAX_FUTURE_SKEW_SEC:
        _to_dlq(raw, "event_time_in_future", f"skew={skew:.0f}s")
        return False

    if _is_duplicate(str(event.event_id)):
        _to_dlq(raw, "duplicate_event_id")
        return False

    record = event.model_dump(mode="json")
    record["ingest_time"] = now.isoformat()
    record["is_late"] = -skew > config.LATE_THRESHOLD_SEC
    state["sink"].send(topic, event.user_id, record)
    state["accepted"][topic] += 1
    return True


# async: chạy trên một event loop duy nhất nên các bộ đếm/dedup cache không bị tranh chấp luồng
# Đọc body thô thay vì Body(...) để nhận cả text/plain (navigator.sendBeacon không gửi được application/json
# sang origin khác mà không bị chặn CORS)
@app.post("/events")
async def ingest(request: Request):
    try:
        payload = json.loads(await request.body())
    except ValueError:
        raise HTTPException(status_code=400, detail="Body không phải JSON hợp lệ")
    events = payload if isinstance(payload, list) else [payload]
    ok = sum(_process(e) for e in events)
    return {"received": len(events), "accepted": ok, "rejected": len(events) - ok}


@app.get("/health")
def health():
    return {"status": "ok", "kafka_queue": state["sink"].queue_len()}


@app.get("/metrics")
def metrics():
    sink = state["sink"]
    uptime = time.time() - state["started_at"]
    total = sum(state["accepted"].values()) + sum(state["rejected"].values())
    return {
        "uptime_sec": round(uptime, 1),
        "received_total": total,
        "avg_events_per_sec": round(total / uptime, 1) if uptime else 0,
        "accepted_by_topic": dict(state["accepted"]),
        "rejected_by_reason": dict(state["rejected"]),
        "delivered_by_topic": dict(sink.delivered),
        "delivery_failed_by_topic": dict(sink.failed),
        "kafka_queue": sink.queue_len(),
    }
