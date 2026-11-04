"""
Load Generator (Bo phat tai) phuc vu do luong va benchmark Kafka cho TV4.
Co the chay doc lap hoac duoc goi boi run_benchmarks.py.

Ho tro tinh chinh cac thong so:
  - acks: '0', '1', 'all'
  - batch_size: 0, 65536, 131072
  - linger_ms: 0, 10, 20
  - compression: none, lz4, snappy, gzip
  - workers: so tien trinh producer chay song song
  - rate: toc do phat (event/s), 0 = toc do toi da
"""
import argparse
import copy
import json
import multiprocessing as mp
import os
import queue
import random
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Console Windows UTF-8
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9096")
CATALOG_PATH = Path(__file__).with_name("catalog.json")


def load_catalog(path: Path = CATALOG_PATH) -> dict:
    if not path.exists():
        # Fallback catalog toi thieu
        return {
            "stores": ["HCM-Q1", "HCM-Q3", "HN-HK"],
            "pages": ["/", "/menu", "/cart"],
            "search_terms": ["banh kem", "croissant"],
            "vouchers": ["FREESHIP"],
            "payment_methods": {"cash": 0.3, "momo": 0.4, "vnpay": 0.3},
            "products": [
                {"product_id": "BK001", "name": "Bánh kem dâu", "category": "banh_kem", "price": 280000},
                {"product_id": "BM003", "name": "Croissant bơ", "category": "banh_mi", "price": 32000},
            ]
        }
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class BakeryEventGenerator:
    def __init__(self, catalog: dict, seed: int | None = None):
        self.c = catalog
        self.rng = random.Random(seed)
        self.pay_methods = list(catalog["payment_methods"].keys())
        self.pay_weights = list(catalog["payment_methods"].values())

    def _base(self, etype: str, ctx: dict) -> dict:
        now = datetime.now(timezone.utc)
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": etype,
            "event_time": iso(now),
            "user_id": ctx["user_id"],
            "session_id": ctx["session_id"],
            "source": ctx["source"],
            "device": ctx["device"],
            "schema_version": 1,
        }

    def session(self) -> list[dict]:
        r = self.rng
        source = r.choices(["web", "app"], [0.45, 0.55])[0]
        ctx = {
            "user_id": f"U{r.randint(1, 100000):06d}",
            "session_id": uuid.uuid4().hex[:16],
            "source": source,
            "device": "mobile" if source == "app" else r.choices(["desktop", "mobile", "tablet"], [0.5, 0.4, 0.1])[0],
        }
        events = []

        # 1. Page view
        e = self._base("page_view", ctx)
        e["page"] = r.choice(self.c["pages"])
        events.append(e)

        # 2. Xem san pham / Add to cart
        p = r.choice(self.c["products"])
        e = self._base("product_view", ctx)
        e.update(product_id=p["product_id"], category=p["category"], price=p["price"])
        events.append(e)

        qty = r.choice([1, 2, 3])
        e = self._base("add_to_cart", ctx)
        e.update(product_id=p["product_id"], category=p["category"], price=p["price"], quantity=qty)
        events.append(e)

        # 3. Don hang (Order placed)
        total = p["price"] * qty
        order_id = f"O{uuid.uuid4().hex[:10].upper()}"
        e = self._base("order_placed", ctx)
        e.update(
            order_id=order_id,
            store_id=r.choice(self.c["stores"]),
            items=[{"product_id": p["product_id"], "category": p["category"], "unit_price": p["price"], "quantity": qty}],
            total_amount=total,
            voucher=None,
            delivery_type=r.choice(["pickup", "delivery"])
        )
        events.append(e)

        # 4. Thanh toan (Payment)
        e = self._base("payment", ctx)
        e.update(
            order_id=order_id,
            payment_method=r.choices(self.pay_methods, self.pay_weights)[0],
            status="success" if r.random() < 0.95 else "failed",
            amount=total
        )
        events.append(e)
        return events

    def stream(self):
        while True:
            yield from self.session()


def pct(values, p):
    if not values:
        return None
    values = sorted(values)
    return round(values[min(len(values) - 1, int(len(values) * p / 100))], 2)


def kafka_worker(wid: int, args_dict: dict, out_queue: mp.Queue):
    from confluent_kafka import Producer

    topic = args_dict["topic"]
    conf = {
        "bootstrap.servers": args_dict["bootstrap"],
        "acks": args_dict["acks"],
        "linger.ms": args_dict["linger_ms"],
        "compression.type": args_dict["compression"],
        "queue.buffering.max.messages": 1_000_000,
    }
    if args_dict["batch_size"] > 0:
        conf["batch.size"] = args_dict["batch_size"]
    if args_dict["acks"] == "all":
        conf["enable.idempotence"] = True

    p = Producer(conf)
    latencies = []
    errors = 0

    def delivery_cb(err, msg):
        nonlocal errors
        if err is not None:
            errors += 1
        elif len(latencies) < 150_000:
            latencies.append(msg.latency() * 1000)  # ms

    catalog = load_catalog()
    gen = BakeryEventGenerator(catalog, seed=(args_dict["seed"] or 42) + wid)
    stream = gen.stream()

    rate = args_dict["rate"]
    worker_rate = (rate / args_dict["workers"]) if rate > 0 else 0
    duration = args_dict["duration"]

    sent = 0
    by_type = Counter()
    start_time = last_report = time.perf_counter()
    window_sent = 0

    try:
        while True:
            now = time.perf_counter()
            elapsed = now - start_time
            if duration and elapsed >= duration:
                break

            # Rate limiting
            if worker_rate > 0:
                expected = worker_rate * elapsed
                if sent > expected:
                    sleep_sec = (sent - expected) / worker_rate
                    if sleep_sec > 0.001:
                        time.sleep(min(sleep_sec, 0.05))

            event = next(stream)
            value = json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            key = str(event.get("user_id", "unknown")).encode("utf-8")

            while True:
                try:
                    p.produce(topic, key=key, value=value, on_delivery=delivery_cb)
                    break
                except BufferError:
                    p.poll(0.02)

            p.poll(0)
            sent += 1
            window_sent += 1
            by_type[event["event_type"]] += 1

            if now - last_report >= 3.0:
                out_queue.put({
                    "type": "tick",
                    "wid": wid,
                    "t": round(elapsed, 1),
                    "rate": window_sent / (now - last_report)
                })
                window_sent = 0
                last_report = now
    finally:
        p.flush(timeout=30)

    total_time = time.perf_counter() - start_time
    out_queue.put({
        "type": "done",
        "wid": wid,
        "sent": sent,
        "seconds": total_time,
        "by_type": dict(by_type),
        "errors": errors,
        "latencies_sample": latencies[:: max(1, len(latencies) // 10_000)]
    })


def run_load_generator(
    topic: str = "bakery.bench.p3",
    bootstrap: str = DEFAULT_BOOTSTRAP,
    rate: float = 0,
    duration: float = 20,
    workers: int = 2,
    acks: str = "all",
    batch_size: int = 65536,
    linger_ms: int = 10,
    compression: str = "lz4",
    tag: str = "",
    seed: int = 42
) -> dict:
    args_dict = {
        "topic": topic,
        "bootstrap": bootstrap,
        "rate": rate,
        "duration": duration,
        "workers": workers,
        "acks": acks,
        "batch_size": batch_size,
        "linger_ms": linger_ms,
        "compression": compression,
        "tag": tag,
        "seed": seed
    }

    print(f"\n[Load Generator] Bat dau phat tai:")
    print(f" -> Topic: {topic} | Workers: {workers} | Target Rate: {'Max' if rate <= 0 else f'{rate} eps'}")
    print(f" -> Cấu hình: acks={acks}, batch_size={batch_size}, linger_ms={linger_ms}, compression={compression}")
    print(f" -> Thoi gian chay: {duration}s\n", flush=True)

    out_queue = mp.Queue()
    procs = [mp.Process(target=kafka_worker, args=(i, args_dict, out_queue)) for i in range(workers)]
    t_start = time.time()
    for p in procs:
        p.start()

    done, rates = [], {}
    while len(done) < len(procs):
        try:
            msg = out_queue.get(timeout=1)
        except queue.Empty:
            if not any(p.is_alive() for p in procs) and out_queue.empty():
                break
            continue
        if msg["type"] == "tick":
            rates[msg["wid"]] = msg["rate"]
            if msg["wid"] == 0:
                print(f"  t={msg['t']:>5.1f}s | Tốc độ tức thời ≈ {sum(rates.values()):>8,.0f} event/s", flush=True)
        else:
            done.append(msg)

    for p in procs:
        p.join()

    sent = sum(d["sent"] for d in done)
    secs = max(d["seconds"] for d in done) if done else duration
    all_lats = [x for d in done for x in d["latencies_sample"]]
    by_type = Counter()
    for d in done:
        by_type.update(d["by_type"])

    throughput = round(sent / secs, 1) if secs else 0
    p50 = pct(all_lats, 50)
    p95 = pct(all_lats, 95)
    p99 = pct(all_lats, 99)

    summary = {
        "tag": tag,
        "started_at": datetime.fromtimestamp(t_start, tz=timezone.utc).isoformat(),
        "config": args_dict,
        "sent_total": sent,
        "duration_sec": round(secs, 2),
        "throughput_eps": throughput,
        "send_errors": sum(d["errors"] for d in done),
        "by_event_type": dict(by_type),
        "ack_latency_ms": {"p50": p50, "p95": p95, "p99": p99},
    }

    results_dir = Path("results")
    results_dir.mkdir(parents=True, exist_ok=True)
    stamp = int(t_start)
    out_file = results_dir / f"sim_kafka_{tag or 'run'}_{stamp}.json"
    out_file.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[Load Generator] Hoan tat! Tong gui: {sent:,} events trong {secs:.1f}s -> Throughput: {throughput:,} eps | p95 Ack: {p95} ms")
    print(f" -> File ket qua: {out_file}\n", flush=True)
    return summary


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--topic", default="bakery.bench.p3")
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    ap.add_argument("--rate", type=float, default=0, help="event/s (0 = toi da)")
    ap.add_argument("--duration", type=float, default=20, help="thoi gian chay (giay)")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--acks", default="all", choices=["0", "1", "all"])
    ap.add_argument("--batch-size", type=int, default=65536)
    ap.add_argument("--linger-ms", type=int, default=10)
    ap.add_argument("--compression", default="lz4", choices=["none", "gzip", "snappy", "lz4", "zstd"])
    ap.add_argument("--tag", default="")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    run_load_generator(
        topic=args.topic,
        bootstrap=args.bootstrap,
        rate=args.rate,
        duration=args.duration,
        workers=args.workers,
        acks=args.acks,
        batch_size=args.batch_size,
        linger_ms=args.linger_ms,
        compression=args.compression,
        tag=args.tag,
        seed=args.seed
    )


if __name__ == "__main__":
    main()

