"""
Consumer kiem chung va do luong tang Ingestion (TV4 - Bakery Store).

Chuc nang:
  - Do Throughput tieu thu theo tung giay (events/second)
  - Do Do tre End-to-End (thoi diem nhan - thoi diem event_time/Kafka CreateTime) voi p50, p95, p99
  - Theo doi Consumer Lag (so luong ban ghi con ton dong tren tung partition)
  - Dem so ban ghi trung lap (event_id duplicate), ban ghi den tre (is_late), phan tich DLQ
  - Tu dong xuat file CSV theo tung giay va JSON tong ket vao thu muc results/

Vi du su dung:
  python verify_consumer.py --topics bakery.bench.p3 --idle-timeout 10 --tag bench_p3
  python verify_consumer.py --from-beginning --idle-timeout 5 --tag summary_all
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

# Console Windows UTF-8
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9096")
DEFAULT_TOPICS = [
    "bakery.user-events",
    "bakery.orders",
    "bakery.payments",
    "bakery.dlq"
]


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
        try:
            lo, hi = consumer.get_watermark_offsets(tp, timeout=2.0, cached=False)
            pos = tp.offset if tp.offset >= 0 else lo
            lag += max(0, hi - pos)
        except Exception:
            pass
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


def run_consumer(
    topics: list[str],
    bootstrap: str = DEFAULT_BOOTSTRAP,
    group: str | None = None,
    from_beginning: bool = False,
    duration: float = 0,
    idle_timeout: float = 0,
    sink_dir: str | None = None,
    tag: str = ""
) -> dict:
    group_id = group or f"verify-bakery-{uuid.uuid4().hex[:8]}"
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest" if from_beginning else "latest",
        "enable.auto.commit": True,
        "fetch.min.bytes": 1,
    })
    consumer.subscribe(topics)
    sink = JsonlSink(sink_dir) if sink_dir else None

    print(f"\n[Verify Consumer] Bat dau doc topics: {topics}")
    print(f" -> Bootstrap Servers: {bootstrap}")
    print(f" -> Group ID: {group_id}")
    print(f" -> Offset Reset: {'earliest' if from_beginning else 'latest'}")
    print(f" -> Dang lang nghe message... (Ctrl+C de dung)\n", flush=True)

    per_topic = Counter()
    dlq_reasons = Counter()
    seen_ids: set[str] = set()
    duplicates = 0
    late = 0
    all_lat = []
    sec_count = defaultdict(int)
    sec_lat = defaultdict(list)
    lag_samples = {}

    start = time.time()
    last_msg = start
    last_print = start
    last_lag = start

    try:
        while True:
            now = time.time()
            if duration and (now - start >= duration):
                break
            if idle_timeout and sum(per_topic.values()) > 0 and (now - last_msg >= idle_timeout):
                print(f"[!] Idle timeout ({idle_timeout}s khong co ban ghi moi). Dung consumer.")
                break

            msgs = consumer.consume(num_messages=1000, timeout=0.5)
            now = time.time()

            for m in msgs:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[!] Kafka Error: {m.error()}", flush=True)
                    continue

                topic = m.topic()
                per_topic[topic] += 1
                ts_type, ts_ms = m.timestamp()

                # Tinh latency end-to-end
                lat = max(0.0, now * 1000 - ts_ms) if ts_ms > 0 else 0.0
                sec = int(now - start)
                sec_count[sec] += 1
                sec_lat[sec].append(lat)
                if len(all_lat) < 500_000:
                    all_lat.append(lat)

                try:
                    rec = json.loads(m.value().decode("utf-8"))
                except Exception:
                    rec = {}

                if topic.endswith(".dlq"):
                    reason = rec.get("reason", "unknown_error")
                    dlq_reasons[reason] += 1
                else:
                    eid = rec.get("event_id")
                    if eid in seen_ids:
                        duplicates += 1
                    elif eid:
                        seen_ids.add(eid)
                    late += bool(rec.get("is_late"))

                if sink and ts_ms > 0:
                    sink.write(topic, m.value(), ts_ms)

            if msgs:
                last_msg = now

            if now - last_lag >= 3.0:
                cur_lag = total_lag(consumer)
                lag_samples[int(now - start)] = cur_lag
                last_lag = now

            if now - last_print >= 3.0:
                s = max(0, int(now - start) - 1)
                speed = sec_count.get(s, 0)
                p95_lat = pct(sec_lat.get(s, []), 95)
                cur_lag = lag_samples.get(max(lag_samples) if lag_samples else 0, 0)
                total_rcv = sum(per_topic.values())
                print(f"  t={now - start:>5.0f}s | Nhận={total_rcv:>8,} msgs | Tốc độ≈{speed:>7,}/s | p95 Latency={p95_lat:>5} ms | Lag={cur_lag:>6,}", flush=True)
                last_print = now

    except KeyboardInterrupt:
        print("\n[!] Nguoi dung yeu cau dung (Ctrl+C). Dang hoan tat...")
    finally:
        consumer.close()
        if sink:
            sink.close()

    elapsed = time.time() - start
    results_dir = Path("results")
    results_dir.mkdir(parents=True, exist_ok=True)
    stamp = int(start)
    tag_clean = tag or "run"

    # Ghi file CSV chi tiet tung giay
    csv_path = results_dir / f"consumer_{tag_clean}_{stamp}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["second", "records", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms", "lag"])
        max_sec = max(sec_count.keys()) if sec_count else 0
        for s in range(0, max_sec + 1):
            lats = sec_lat.get(s, [])
            w.writerow([
                s,
                sec_count.get(s, 0),
                pct(lats, 50),
                pct(lats, 95),
                pct(lats, 99),
                lag_samples.get(s, "")
            ])

    total_msgs = sum(per_topic.values())
    summary = {
        "tag": tag,
        "timestamp": stamp,
        "started_at": datetime.fromtimestamp(start, tz=timezone.utc).isoformat(),
        "topics": topics,
        "records_total": total_msgs,
        "records_by_topic": dict(per_topic),
        "unique_event_ids": len(seen_ids),
        "duplicate_event_ids": duplicates,
        "late_events": late,
        "dlq_by_reason": dict(dlq_reasons),
        "elapsed_sec": round(elapsed, 1),
        "avg_consume_eps": round(total_msgs / elapsed, 1) if elapsed else 0,
        "e2e_latency_ms": {
            "p50": pct(all_lat, 50),
            "p95": pct(all_lat, 95),
            "p99": pct(all_lat, 99)
        },
        "per_second_csv": str(csv_path),
    }

    json_path = results_dir / f"consumer_{tag_clean}_{stamp}.json"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n" + "=" * 60)
    print("TONG KET DO LUONG CONSUMER")
    print("=" * 60)
    print(f"Tong ban ghi tieu thu: {total_msgs:,}")
    print(f"Thoi gian do:          {elapsed:.1f}s")
    print(f"Throughput trung binh: {summary['avg_consume_eps']:,} eps")
    print(f"End-to-End Latency:    p50 = {summary['e2e_latency_ms']['p50']} ms | p95 = {summary['e2e_latency_ms']['p95']} ms | p99 = {summary['e2e_latency_ms']['p99']} ms")
    print(f"Ban ghi trung lap:     {duplicates} ({duplicates / (total_msgs or 1) * 100:.2f}%)")
    print(f"Ban ghi den tre:       {late}")
    print(f"Ban ghi DLQ:           {sum(dlq_reasons.values())}")
    print(f"File CSV:              {csv_path}")
    print(f"File JSON:             {json_path}")
    print("=" * 60 + "\n", flush=True)

    return summary


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    ap.add_argument("--topics", default=None, help="Danh sach topic, ngan cach bang dau phay (mac dinh doc 4 topic nghiep vu)")
    ap.add_argument("--group", default=None)
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--duration", type=float, default=0)
    ap.add_argument("--idle-timeout", type=float, default=0)
    ap.add_argument("--sink", default=None)
    ap.add_argument("--tag", default="")
    args = ap.parse_args()

    topics = [t.strip() for t in args.topics.split(",")] if args.topics else DEFAULT_TOPICS
    run_consumer(
        topics=topics,
        bootstrap=args.bootstrap,
        group=args.group,
        from_beginning=args.from_beginning,
        duration=args.duration,
        idle_timeout=args.idle_timeout,
        sink_dir=args.sink,
        tag=args.tag
    )


if __name__ == "__main__":
    main()

