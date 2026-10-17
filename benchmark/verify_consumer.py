"""Consumer kiểm chứng tầng Ingestion.

Đọc các topic, đo:
  - throughput tiêu thụ theo từng giây
  - độ trễ end-to-end: thời điểm consumer nhận - timestamp Kafka (CreateTime do producer đặt)
  - consumer lag (số bản ghi còn chưa đọc)
  - số bản ghi theo topic, số event_id trùng, số event đến trễ
Tuỳ chọn --sink ghi dữ liệu ra JSONL phân vùng topic/date/hour để bàn giao cho nhóm Storage.

Ví dụ:
  python verify_consumer.py --idle-timeout 15                 # chạy song song với simulator, đo latency
  python verify_consumer.py --from-beginning --idle-timeout 10 # đếm toàn bộ dữ liệu đã có
  python verify_consumer.py --sink ../data/landing
"""
import argparse
import csv
import json
import os
import sys
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

# Console Windows mặc định cp1252 -> in tiếng Việt bị lỗi
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

DEFAULT_BOOTSTRAP = "localhost:9092,localhost:9094,localhost:9095"


def pct(values, p):
    if not values:
        return ""
    values = sorted(values)
    return round(values[min(len(values) - 1, int(len(values) * p / 100))], 1)


def total_lag(consumer: Consumer) -> int:
    lag = 0
    assigned = consumer.assignment()
    if not assigned:
        return 0
    for tp in consumer.position(assigned):
        lo, hi = consumer.get_watermark_offsets(tp, timeout=5, cached=False)
        pos = tp.offset if tp.offset >= 0 else lo
        lag += max(0, hi - pos)
    return lag


class JsonlSink:
    def __init__(self, root: str):
        self.root = Path(root)
        self.files = {}

    def write(self, topic: str, raw: bytes, ts_ms: int):
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        part = self.root / f"topic={topic}" / f"date={dt:%Y-%m-%d}" / f"hour={dt:%H}"
        f = self.files.get(part)
        if f is None:
            part.mkdir(parents=True, exist_ok=True)
            f = self.files[part] = open(part / f"part-{os.getpid()}.jsonl", "ab")
        f.write(raw + b"\n")

    def close(self):
        for f in self.files.values():
            f.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP))
    ap.add_argument("--topic-prefix", default=os.getenv("TOPIC_PREFIX", "shop"))
    ap.add_argument("--topics", default=None, help="danh sách topic, phân cách dấu phẩy")
    ap.add_argument("--group", default=None, help="consumer group (mặc định tạo mới mỗi lần chạy)")
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--duration", type=float, default=0, help="dừng sau N giây (0 = không giới hạn)")
    ap.add_argument("--idle-timeout", type=float, default=0, help="dừng khi N giây không có bản ghi mới")
    ap.add_argument("--sink", default=None, help="thư mục ghi JSONL phân vùng")
    ap.add_argument("--tag", default="")
    args = ap.parse_args()

    topics = args.topics.split(",") if args.topics else [
        f"{args.topic_prefix}.{t}" for t in ("clickstream", "orders", "payments", "dlq")]
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group or f"verify-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": True,
        "fetch.min.bytes": 1,
    })
    consumer.subscribe(topics)
    sink = JsonlSink(args.sink) if args.sink else None
    print(f"Đang đọc {topics} ... (Ctrl+C để dừng)", flush=True)

    per_topic = Counter()
    dlq_reasons = Counter()
    seen_ids: set[str] = set()
    duplicates = late = 0
    all_lat = []
    sec_count = defaultdict(int)
    sec_lat = defaultdict(list)
    lag_samples = {}

    start = time.time()
    last_msg = start
    last_print = last_lag = start
    try:
        while True:
            now = time.time()
            if args.duration and now - start >= args.duration:
                break
            if args.idle_timeout and per_topic and now - last_msg >= args.idle_timeout:
                break
            msgs = consumer.consume(num_messages=1000, timeout=0.5)
            now = time.time()
            for m in msgs:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        print("Lỗi:", m.error())
                    continue
                topic = m.topic()
                per_topic[topic] += 1
                ts_type, ts_ms = m.timestamp()
                lat = now * 1000 - ts_ms
                sec = int(now - start)
                sec_count[sec] += 1
                sec_lat[sec].append(lat)
                if len(all_lat) < 2_000_000:
                    all_lat.append(lat)
                try:
                    rec = json.loads(m.value())
                except ValueError:
                    rec = {}
                if topic.endswith(".dlq"):
                    dlq_reasons[rec.get("reason", "?")] += 1
                else:
                    eid = rec.get("event_id")
                    if eid in seen_ids:
                        duplicates += 1
                    elif eid:
                        seen_ids.add(eid)
                    late += bool(rec.get("is_late"))
                if sink:
                    sink.write(topic, m.value(), ts_ms)
            if msgs:
                last_msg = now
            if now - last_lag >= 5:
                lag_samples[int(now - start)] = total_lag(consumer)
                last_lag = now
            if now - last_print >= 5:
                s = int(now - start) - 1
                print(f"t={now - start:6.0f}s  nhận={sum(per_topic.values()):>10,}  "
                      f"tốc độ≈{sec_count.get(s, 0):>8,}/s  p95 latency={pct(sec_lat.get(s, []), 95)} ms  "
                      f"lag={lag_samples.get(max(lag_samples) if lag_samples else 0, 0):,}", flush=True)
                last_print = now
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        if sink:
            sink.close()

    elapsed = time.time() - start
    Path("results").mkdir(exist_ok=True)
    stamp = int(start)
    csv_path = Path("results") / f"consumer_{args.tag or 'run'}_{stamp}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["second", "records", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms", "lag"])
        for s in range(0, (max(sec_count) + 1) if sec_count else 0):
            lats = sec_lat.get(s, [])
            w.writerow([s, sec_count.get(s, 0), pct(lats, 50), pct(lats, 95), pct(lats, 99), lag_samples.get(s, "")])

    total = sum(per_topic.values())
    summary = {
        "tag": args.tag,
        "topics": topics,
        "records_total": total,
        "records_by_topic": dict(per_topic),
        "unique_event_ids": len(seen_ids),
        "duplicate_event_ids": duplicates,
        "late_events": late,
        "dlq_by_reason": dict(dlq_reasons),
        "elapsed_sec": round(elapsed, 1),
        "avg_consume_eps": round(total / elapsed, 1) if elapsed else 0,
        "e2e_latency_ms": {"p50": pct(all_lat, 50), "p95": pct(all_lat, 95), "p99": pct(all_lat, 99)},
        "per_second_csv": str(csv_path),
    }
    (Path("results") / f"consumer_{args.tag or 'run'}_{stamp}.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
