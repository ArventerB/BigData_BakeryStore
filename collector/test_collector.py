"""Kiểm thử nhanh Collector đang chạy: gửi event sạch, event bẩn, event trùng rồi đối chiếu kết quả.

Chạy (Collector và Kafka phải đang chạy):
    python collector/test_collector.py                         # kiểm phản hồi API và /metrics
    python collector/test_collector.py --kafka                 # đọc thêm Kafka xem từng event nằm ở topic nào
    python collector/test_collector.py --url http://localhost:8010

Thoát với mã 0 nếu mọi trường hợp đúng như mong đợi, 1 nếu có trường hợp sai.
"""
import argparse
import copy
import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import requests

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

TOPIC_PREFIX = os.getenv("TOPIC_PREFIX", "bakery")
DLQ = f"{TOPIC_PREFIX}.dlq"


def now_iso(delta: timedelta = timedelta()) -> str:
    t = datetime.now(timezone.utc) + delta
    return t.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def base(event_type: str, user_id: str = "U0000001") -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": now_iso(),
        "user_id": user_id,
        "session_id": uuid.uuid4().hex[:16],
        "source": "web",
        "device": "desktop",
        "schema_version": 1,
    }


def page_view() -> dict:
    return {**base("page_view"), "page": "/"}


def product_view() -> dict:
    return {**base("product_view"), "page": "/product/P001", "product_id": "P001",
            "category": "banh_mi", "price": 25000.0}


def order() -> dict:
    return {**base("order_placed"), "order_id": f"O{uuid.uuid4().hex[:12].upper()}", "store_id": "HCM-Q1",
            "items": [{"product_id": "P001", "category": "banh_mi", "unit_price": 25000.0, "quantity": 2}],
            "total_amount": 50000.0, "voucher": None, "delivery_type": "pickup"}


def payment(order_event: dict) -> dict:
    return {**base("payment"), "order_id": order_event["order_id"], "payment_id": "PAY-TEST-1",
            "payment_method": "momo", "status": "success", "amount": order_event["total_amount"]}


def build_cases() -> list[dict]:
    """Mỗi trường hợp: tên, payload gửi đi, kết quả mong đợi (None = vào topic chính, chuỗi = lý do DLQ)."""
    o = order()
    cases = [
        ("page_view hợp lệ", page_view(), None, f"{TOPIC_PREFIX}.user-events"),
        ("product_view hợp lệ", product_view(), None, f"{TOPIC_PREFIX}.user-events"),
        ("order_placed hợp lệ", o, None, f"{TOPIC_PREFIX}.orders"),
        ("payment hợp lệ", payment(o), None, f"{TOPIC_PREFIX}.payments"),
    ]

    missing = page_view(); missing.pop("user_id")
    naive = page_view(); naive["event_time"] = naive["event_time"].rstrip("Z")
    future = page_view(); future["event_time"] = now_iso(timedelta(hours=1))
    bad_fmt = page_view(); bad_fmt["event_time"] = "01/10/2026 08:30"
    neg = product_view(); neg["price"] = -25000.0
    neg_order = order(); neg_order["total_amount"] = -1.0
    empty = order(); empty["items"] = []
    wrong_type = {**base("add_to_cart"), "product_id": "P001", "category": "banh_mi", "price": 25000.0,
                  "quantity": "hai"}
    extra = page_view(); extra["coupon_code"] = "HACK100"
    unknown = page_view(); unknown["event_type"] = "clik"
    late = page_view(); late["event_time"] = now_iso(-timedelta(hours=3))

    cases += [
        ("event đến trễ 3 giờ (vẫn hợp lệ, is_late=true)", late, None, f"{TOPIC_PREFIX}.user-events"),
        ("thiếu user_id", missing, "schema_violation", DLQ),
        ("event_time thiếu múi giờ", naive, "missing_timezone", DLQ),
        ("event_time ở tương lai 1 giờ", future, "event_time_in_future", DLQ),
        ("event_time sai định dạng", bad_fmt, "schema_violation", DLQ),
        ("giá âm", neg, "schema_violation", DLQ),
        ("tổng tiền đơn âm", neg_order, "schema_violation", DLQ),
        ("đơn hàng không có sản phẩm", empty, "schema_violation", DLQ),
        ("quantity là chữ", wrong_type, "schema_violation", DLQ),
        ("thừa trường ngoài contract", extra, "schema_violation", DLQ),
        ("event_type gõ sai", unknown, "unknown_event_type", DLQ),
        ("phần tử không phải object", json.dumps(page_view()), "not_an_object", DLQ),
    ]
    dup = page_view()
    cases += [
        ("bản gốc của event sẽ gửi trùng", dup, None, f"{TOPIC_PREFIX}.user-events"),
        ("gửi lại cùng event_id", copy.deepcopy(dup), "duplicate_event_id", DLQ),
    ]
    return [{"name": n, "payload": p, "expect": e, "topic": t} for n, p, e, t in cases]


def event_id_of(payload) -> str | None:
    if isinstance(payload, dict):
        return payload.get("event_id")
    try:
        return json.loads(payload).get("event_id")  # trường hợp not_an_object: chuỗi JSON
    except (ValueError, AttributeError):
        return None


# ---------------------------------------------------------------- đọc Kafka để xác nhận
def kafka_watermarks(consumer, topics) -> dict:
    from confluent_kafka import TopicPartition
    md = consumer.list_topics(timeout=5)
    marks = {}
    for t in topics:
        if t not in md.topics:
            print(f"  [!] Không thấy topic {t} trên Kafka")
            continue
        for p in md.topics[t].partitions:
            _, high = consumer.get_watermark_offsets(TopicPartition(t, p), timeout=5)
            marks[(t, p)] = high
    return marks


def kafka_read_new(consumer, before: dict, after: dict) -> list[tuple[str, dict]]:
    """Đọc đúng các message mới ghi giữa hai mốc offset."""
    from confluent_kafka import TopicPartition
    todo = {k: (before[k], after[k]) for k in after if after[k] > before.get(k, 0)}
    consumer.assign([TopicPartition(t, p, lo) for (t, p), (lo, _) in todo.items()])
    out, remaining = [], sum(hi - lo for lo, hi in todo.values())
    deadline = time.time() + 15
    while remaining > 0 and time.time() < deadline:
        m = consumer.poll(1.0)
        if m is None or m.error():
            continue
        if m.offset() < todo[(m.topic(), m.partition())][1]:
            out.append((m.topic(), json.loads(m.value())))
            remaining -= 1
    return out


def main():
    ap = argparse.ArgumentParser(description="Kiểm thử Collector API")
    ap.add_argument("--url", default=os.getenv("COLLECTOR_URL", "http://localhost:8000"))
    ap.add_argument("--kafka", action="store_true", help="đọc Kafka để xác nhận từng event nằm đúng topic")
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS",
                                                     "localhost:9092,localhost:9094,localhost:9096"))
    args = ap.parse_args()
    url = args.url.rstrip("/")

    print(f"== Collector: {url}")
    try:
        h = requests.get(f"{url}/health", timeout=10)
    except requests.ConnectionError:
        print("  [X] Không kết nối được Collector. Đã chạy chưa? (python -m app.main trong collector/)")
        return 1
    print(f"  /health -> {h.status_code} {h.json()}")
    if h.status_code != 200:
        print("  [X] Collector chưa kết nối được Kafka hoặc thiếu topic")
        return 1

    consumer = None
    if args.kafka:
        from confluent_kafka import Consumer
        consumer = Consumer({"bootstrap.servers": args.bootstrap, "group.id": f"test-collector-{uuid.uuid4().hex[:8]}",
                             "enable.auto.commit": False})
        topics = [f"{TOPIC_PREFIX}.{s}" for s in ("user-events", "orders", "payments", "dlq")]
        before = kafka_watermarks(consumer, topics)

    m0 = requests.get(f"{url}/metrics", timeout=10).json()
    cases = build_cases()
    ok_all = True

    print("\n== 1. Gửi từng event một")
    for c in cases:
        r = requests.post(f"{url}/events", json=c["payload"], timeout=30)
        body = r.json()
        want = (1, 0) if c["expect"] is None else (0, 1)
        got = (body.get("accepted"), body.get("rejected"))
        ok = r.status_code == 200 and got == want
        c["ok"] = ok
        ok_all &= ok
        exp = "chấp nhận" if c["expect"] is None else f"DLQ ({c['expect']})"
        print(f"  [{'OK' if ok else 'SAI'}] {c['name']:<45} mong đợi {exp:<32} -> {r.status_code} {body}")

    print("\n== 2. Gửi một lô 5 event (3 sạch, 2 bẩn)")
    batch = [page_view(), product_view(), order(), {**page_view(), "event_type": "pageview"}, "không phải object"]
    r = requests.post(f"{url}/events", json=batch, timeout=30)
    body = r.json()
    ok = r.status_code == 200 and body == {"received": 5, "accepted": 3, "rejected": 2}
    ok_all &= ok
    print(f"  [{'OK' if ok else 'SAI'}] -> {r.status_code} {body}")

    print("\n== 3. Đối chiếu /metrics (chênh lệch trước và sau khi gửi)")
    m1 = requests.get(f"{url}/metrics", timeout=10).json()
    expect_reasons = {}
    for c in cases:
        if c["expect"]:
            expect_reasons[c["expect"]] = expect_reasons.get(c["expect"], 0) + 1
    expect_reasons["unknown_event_type"] += 1
    expect_reasons["not_an_object"] += 1
    for reason, n in sorted(expect_reasons.items()):
        got = m1["rejected_by_reason"].get(reason, 0) - m0["rejected_by_reason"].get(reason, 0)
        ok = got == n
        ok_all &= ok
        print(f"  [{'OK' if ok else 'SAI'}] {reason:<22} tăng {got} (mong đợi {n})")
    acc = m1["accepted_total"] - m0["accepted_total"]
    want_acc = sum(1 for c in cases if c["expect"] is None) + 3
    ok = acc == want_acc
    ok_all &= ok
    print(f"  [{'OK' if ok else 'SAI'}] accepted_total         tăng {acc} (mong đợi {want_acc})")

    if consumer is not None:
        print("\n== 4. Đọc lại Kafka")
        after = kafka_watermarks(consumer, list({t for t, _ in before}))
        found = {}
        for topic, rec in kafka_read_new(consumer, before, after):
            if topic == DLQ:
                eid = event_id_of(rec.get("raw"))
                found.setdefault(eid, []).append((topic, rec.get("reason"), rec.get("detail")))
            else:
                found.setdefault(rec.get("event_id"), []).append((topic, rec.get("is_late"), rec.get("ingest_time")))
        consumer.close()
        for c in cases:
            eid = event_id_of(c["payload"])
            hits = found.get(eid, [])
            if c["expect"] == "duplicate_event_id":
                ok = any(h[0] == DLQ and h[1] == "duplicate_event_id" for h in hits)
            elif c["expect"]:
                ok = any(h[0] == DLQ and h[1] == c["expect"] for h in hits)
            else:
                ok = any(h[0] == c["topic"] for h in hits)
            ok_all &= ok
            print(f"  [{'OK' if ok else 'SAI'}] {c['name']:<45} -> {hits[:2] if hits else 'không thấy trên Kafka'}")

    print("\n== KẾT QUẢ:", "TẤT CẢ ĐÚNG" if ok_all else "CÓ TRƯỜNG HỢP SAI")
    return 0 if ok_all else 1


if __name__ == "__main__":
    sys.exit(main())
