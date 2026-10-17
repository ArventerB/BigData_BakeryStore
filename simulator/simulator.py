"""Bộ phát tải (load generator) cho tầng Ingestion.

Hai chế độ:
  --mode api    gửi lô JSON qua HTTP tới Collector API (đường đi giống web/app thật, có validate + DLQ)
  --mode kafka  ghi thẳng vào Kafka (đo giới hạn của Kafka, bỏ qua Collector)

Ví dụ:
  python simulator.py --mode api --rate 2000 --duration 60 --workers 4 --dirty-rate 0.03
  python simulator.py --mode kafka --rate 0 --duration 60 --workers 4 --topic shop.bench.p6
  python simulator.py --mode api --rate 1000 --duration 180 --peak-mult 5   # giờ cao điểm ở 1/3 giữa
"""
import argparse
import json
import multiprocessing as mp
import os
import queue
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from generator import EventGenerator, load_catalog

# Console Windows mặc định cp1252 -> in tiếng Việt bị lỗi
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

DEFAULT_BOOTSTRAP = "localhost:9092,localhost:9094,localhost:9095"
CLICK_TYPES = {"page_view", "search", "product_view", "add_to_cart", "remove_from_cart", "checkout_start"}
REPORT_EVERY = 5.0


def topic_for(event: dict, prefix: str) -> str:
    t = event.get("event_type")
    if t in CLICK_TYPES:
        return f"{prefix}.clickstream"
    if t == "order_placed":
        return f"{prefix}.orders"
    if t == "payment":
        return f"{prefix}.payments"
    return f"{prefix}.dlq"


def rate_at(args, elapsed: float) -> float:
    """Tốc độ mục tiêu (event/s) của 1 worker tại thời điểm elapsed. 0 = không giới hạn."""
    if args.rate <= 0:
        return 0
    base = args.rate / args.workers
    third = args.duration / 3
    if args.peak_mult != 1 and third <= elapsed < 2 * third:
        return base * args.peak_mult
    return base


# ---------------------------------------------------------------- senders
class ApiSender:
    def __init__(self, args):
        import httpx
        self.client = httpx.Client(base_url=args.api_url, timeout=30)
        self.batch = []
        self.batch_size = args.batch
        self.latencies = []
        self.errors = 0
        self.rejected = 0

    def send(self, event):
        self.batch.append(event)
        if len(self.batch) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.batch:
            return
        t0 = time.perf_counter()
        try:
            r = self.client.post("/events", json=self.batch)
            r.raise_for_status()
            self.rejected += r.json()["rejected"]
        except Exception as e:  # noqa: BLE001
            self.errors += len(self.batch)
            print(f"[api] lỗi gửi lô {len(self.batch)} event: {e}", flush=True)
        self.latencies.append((time.perf_counter() - t0) * 1000)
        self.batch = []

    def close(self):
        self.flush()
        self.client.close()


class KafkaSender:
    def __init__(self, args):
        from confluent_kafka import Producer
        self.prefix = args.topic_prefix
        self.fixed_topic = args.topic
        conf = {
            "bootstrap.servers": args.bootstrap,
            "acks": args.acks,
            "enable.idempotence": args.acks == "all",
            "linger.ms": args.linger_ms,
            "batch.size": args.batch_size,
            "compression.type": args.compression,
            "queue.buffering.max.messages": 1_000_000,
        }
        self.p = Producer(conf)
        self.latencies = []
        self.errors = 0
        self.rejected = 0

    def _cb(self, err, msg):
        if err is not None:
            self.errors += 1
        elif len(self.latencies) < 200_000:
            self.latencies.append(msg.latency() * 1000)  # thời gian từ produce() tới khi broker ack

    def send(self, event):
        topic = self.fixed_topic or topic_for(event, self.prefix)
        value = json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode()
        key = str(event.get("user_id", "unknown")).encode()
        while True:
            try:
                self.p.produce(topic, key=key, value=value, on_delivery=self._cb)
                break
            except BufferError:
                self.p.poll(0.05)
        self.p.poll(0)

    def flush(self):
        self.p.poll(0)

    def close(self):
        self.p.flush(60)


# ---------------------------------------------------------------- worker
def worker(wid: int, args, out: mp.Queue):
    gen = EventGenerator(load_catalog(), seed=(args.seed + wid) if args.seed is not None else None,
                         dirty_rate=args.dirty_rate, late_rate=args.late_rate)
    sender = ApiSender(args) if args.mode == "api" else KafkaSender(args)
    stream = gen.stream()

    sent, by_type = 0, Counter()
    start = last_report = time.perf_counter()
    window_sent = 0
    tick = 0.05
    budget = 0.0
    try:
        while True:
            now = time.perf_counter()
            elapsed = now - start
            if elapsed >= args.duration:
                break
            target = rate_at(args, elapsed)
            if target:
                budget += target * tick
                n = int(budget)
                budget -= n
            else:
                n = 500
            for _ in range(n):
                e = next(stream)
                sender.send(e)
                by_type[e.get("event_type", "?")] += 1
            sent += n
            window_sent += n
            if target:
                sender.flush()
                sleep = tick - (time.perf_counter() - now)
                if sleep > 0:
                    time.sleep(sleep)
            if now - last_report >= REPORT_EVERY:
                out.put({"type": "tick", "wid": wid, "t": round(elapsed, 1),
                         "rate": window_sent / (now - last_report)})
                window_sent, last_report = 0, now
    finally:
        sender.close()
    total_time = time.perf_counter() - start
    lat = sender.latencies
    out.put({"type": "done", "wid": wid, "sent": sent, "seconds": total_time, "by_type": dict(by_type),
             "errors": sender.errors, "rejected": sender.rejected, "dirty_injected": gen.dirty_injected,
             "latencies_sample": lat[:: max(1, len(lat) // 20_000)]})


def pct(values, p):
    if not values:
        return None
    values = sorted(values)
    return round(values[min(len(values) - 1, int(len(values) * p / 100))], 2)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["api", "kafka"], default="api")
    ap.add_argument("--rate", type=float, default=1000, help="tổng event/giây mục tiêu, 0 = tối đa")
    ap.add_argument("--duration", type=float, default=60, help="số giây chạy")
    ap.add_argument("--workers", type=int, default=2, help="số tiến trình producer song song")
    ap.add_argument("--peak-mult", type=float, default=1, help="hệ số nhân tốc độ ở 1/3 giữa thời gian (giờ cao điểm)")
    ap.add_argument("--dirty-rate", type=float, default=0.0, help="tỷ lệ event bẩn, ví dụ 0.03")
    ap.add_argument("--late-rate", type=float, default=0.0, help="tỷ lệ event đến trễ (>1h)")
    ap.add_argument("--seed", type=int, default=None)
    # api
    ap.add_argument("--api-url", default=os.getenv("COLLECTOR_URL", "http://localhost:8000"))
    ap.add_argument("--batch", type=int, default=200, help="số event mỗi request HTTP")
    # kafka
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP))
    ap.add_argument("--topic-prefix", default=os.getenv("TOPIC_PREFIX", "shop"))
    ap.add_argument("--topic", default=None, help="ghi mọi event vào 1 topic (dùng cho benchmark)")
    ap.add_argument("--acks", default="all", choices=["0", "1", "all"])
    ap.add_argument("--linger-ms", type=int, default=10)
    ap.add_argument("--batch-size", type=int, default=65536)
    ap.add_argument("--compression", default="lz4", choices=["none", "gzip", "snappy", "lz4", "zstd"])
    ap.add_argument("--tag", default="", help="nhãn thí nghiệm, ghi vào file kết quả")
    args = ap.parse_args()

    print(f"Bắt đầu: mode={args.mode} rate={args.rate or 'max'} duration={args.duration}s "
          f"workers={args.workers} dirty={args.dirty_rate} peak_mult={args.peak_mult}", flush=True)
    out: mp.Queue = mp.Queue()
    procs = [mp.Process(target=worker, args=(i, args, out)) for i in range(args.workers)]
    t_start = time.time()
    for p in procs:
        p.start()

    done, rates = [], {}
    while len(done) < len(procs):
        try:
            msg = out.get(timeout=1)
        except queue.Empty:
            if not any(p.is_alive() for p in procs) and out.empty():
                break
            continue
        if msg["type"] == "tick":
            rates[msg["wid"]] = msg["rate"]
            if msg["wid"] == 0:
                print(f"t={msg['t']:>6}s  tốc độ ≈ {sum(rates.values()):>10,.0f} event/s", flush=True)
        else:
            done.append(msg)
    for p in procs:
        p.join()

    sent = sum(d["sent"] for d in done)
    secs = max(d["seconds"] for d in done) if done else 0
    lats = [x for d in done for x in d["latencies_sample"]]
    by_type = Counter()
    for d in done:
        by_type.update(d["by_type"])
    summary = {
        "tag": args.tag,
        "started_at": datetime.fromtimestamp(t_start).isoformat(timespec="seconds"),
        "config": vars(args),
        "sent_total": sent,
        "duration_sec": round(secs, 2),
        "throughput_eps": round(sent / secs, 1) if secs else 0,
        "send_errors": sum(d["errors"] for d in done),
        "rejected_by_collector": sum(d["rejected"] for d in done),
        "dirty_injected": sum(d["dirty_injected"] for d in done),
        "by_event_type": dict(by_type),
        "ack_latency_ms": {"what": "HTTP batch round-trip" if args.mode == "api" else "Kafka broker ack",
                           "p50": pct(lats, 50), "p95": pct(lats, 95), "p99": pct(lats, 99)},
    }
    Path("results").mkdir(exist_ok=True)
    fname = Path("results") / f"sim_{args.mode}_{args.tag or 'run'}_{int(t_start)}.json"
    fname.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in summary.items() if k != "config"}, ensure_ascii=False, indent=2))
    print(f"Đã lưu kết quả: {fname}")


if __name__ == "__main__":
    main()
