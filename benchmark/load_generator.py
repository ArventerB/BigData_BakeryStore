"""
Bộ phát tải (producer) cho thí nghiệm benchmark của TV4 – Bakery Store.

- Mỗi worker là 1 tiến trình riêng (tránh GIL của Python), dùng confluent-kafka (librdkafka).
- Trước khi đo, mỗi worker sinh sẵn một kho event tiệm bánh thật (page_view → product_view →
  add_to_cart → order_placed → payment, theo catalog.json) rồi gửi xoay vòng kho này. Nhờ vậy chi phí
  sinh JSON trong Python không lẫn vào số đo của Kafka (giống cách kafka-producer-perf-test dùng payload
  dựng sẵn). Key = user_id như hệ thống thật.
- Mỗi message mang header "tv4" = (run_id, worker_id, seq) để consumer kiểm chứng đếm chính xác
  số bản ghi mất / trùng (xem bench_consumer.py).
- Độ trễ ack = thời gian từ lúc gọi produce() tới khi broker xác nhận (msg.latency()), lấy mẫu reservoir.
- Số byte thực sự gửi qua mạng lấy từ thống kê nội bộ librdkafka (tx_bytes) để tính tỉ lệ nén.

Chạy độc lập (chỉ phía producer, không kiểm chứng phía consumer):
  python benchmark/load_generator.py --topic bakery.bench.p6 --workers 4 --duration 30
  python benchmark/load_generator.py --topic bakery.bench.p6 --rate 20000 --acks all
Thí nghiệm đầy đủ (có consumer kiểm chứng chạy song song) dùng run_benchmarks.py.
"""
import argparse
import json
import multiprocessing as mp
import queue
import random
import sys
import time
import traceback
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from bench_utils import DEFAULT_BOOTSTRAP, HEADER, HEADER_KEY, Reservoir, latency_summary

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

CATALOG_PATH = Path(__file__).with_name("catalog.json")
POOL_EVENTS = 20_000  # số event dựng sẵn mỗi worker


def load_catalog(path: Path = CATALOG_PATH) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class BakeryEventGenerator:
    """Sinh phiên mua hàng của khách tiệm bánh theo data contract schema_version=1."""

    def __init__(self, catalog: dict, seed: int | None = None):
        self.c = catalog
        self.rng = random.Random(seed)
        self.pay_methods = list(catalog["payment_methods"].keys())
        self.pay_weights = list(catalog["payment_methods"].values())

    def _base(self, etype: str, ctx: dict) -> dict:
        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "event_type": etype,
            "event_time": iso(datetime.now(timezone.utc)),
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
            "session_id": f"{r.getrandbits(64):016x}",
            "source": source,
            "device": "mobile" if source == "app" else r.choices(["desktop", "mobile", "tablet"], [0.5, 0.4, 0.1])[0],
        }
        events = []
        e = self._base("page_view", ctx)
        e["page"] = r.choice(self.c["pages"])
        events.append(e)

        p = r.choice(self.c["products"])
        e = self._base("product_view", ctx)
        e.update(product_id=p["product_id"], category=p["category"], price=p["price"])
        events.append(e)

        qty = r.choice([1, 2, 3])
        e = self._base("add_to_cart", ctx)
        e.update(product_id=p["product_id"], category=p["category"], price=p["price"], quantity=qty)
        events.append(e)

        total = p["price"] * qty
        order_id = f"O{r.getrandbits(40):010X}"
        e = self._base("order_placed", ctx)
        e.update(order_id=order_id, store_id=r.choice(self.c["stores"]),
                 items=[{"product_id": p["product_id"], "category": p["category"],
                         "unit_price": p["price"], "quantity": qty}],
                 total_amount=total, voucher=None, delivery_type=r.choice(["pickup", "delivery"]))
        events.append(e)

        e = self._base("payment", ctx)
        e.update(order_id=order_id, payment_method=r.choices(self.pay_methods, self.pay_weights)[0],
                 status="success" if r.random() < 0.95 else "failed", amount=total)
        events.append(e)
        return events

    def stream(self):
        while True:
            yield from self.session()


def build_pool(wid: int, seed: int, n: int = POOL_EVENTS) -> list[tuple[bytes, bytes]]:
    gen = BakeryEventGenerator(load_catalog(), seed=seed * 1000 + wid)
    it = gen.stream()
    pool = []
    for _ in range(n):
        e = next(it)
        pool.append((e["user_id"].encode(), json.dumps(e, ensure_ascii=False, separators=(",", ":")).encode()))
    return pool


def producer_conf(bootstrap: str, acks: str, batch_size: int, linger_ms: int, compression: str,
                  extra: dict | None = None) -> dict:
    conf = {
        "bootstrap.servers": bootstrap,
        "client.id": "tv4-benchmark",
        "acks": str(acks),
        "linger.ms": int(linger_ms),
        "batch.size": int(batch_size),
        "compression.type": compression,
        # idempotence (chống trùng khi retry, giữ thứ tự) chỉ bật được với acks=all
        "enable.idempotence": str(acks) == "all",
        "statistics.interval.ms": 1000,
    }
    if extra:
        conf.update(extra)
    return conf


def producer_worker(wid: int, cfg: dict, out_q: mp.Queue, go, start_at):
    try:
        _producer_worker(wid, cfg, out_q, go, start_at)
    except Exception:
        out_q.put({"type": "error", "wid": wid, "error": traceback.format_exc()})


def _producer_worker(wid: int, cfg: dict, out_q: mp.Queue, go, start_at):
    from confluent_kafka import Producer

    pool = build_pool(wid, cfg.get("seed", 42))
    stats: dict = {}

    def on_stats(js: str):
        try:
            d = json.loads(js)
            stats.update(tx=d.get("tx"), tx_bytes=d.get("tx_bytes"), txmsgs=d.get("txmsgs"),
                         txmsg_bytes=d.get("txmsg_bytes"))
        except ValueError:
            pass

    conf = producer_conf(cfg["bootstrap"], cfg["acks"], cfg["batch_size"], cfg["linger_ms"],
                         cfg["compression"], cfg.get("extra_conf"))
    conf["stats_cb"] = on_stats
    p = Producer(conf)
    p.list_topics(cfg["topic"], timeout=15)  # lấy metadata trước để không tính vào thời gian đo

    lat = Reservoir(30_000)
    errors = Counter()
    acked = 0

    def on_delivery(err, msg):
        nonlocal acked
        if err is not None:
            errors[err.name()] += 1
        else:
            acked += 1
            lat.add(msg.latency() * 1000.0)

    out_q.put({"type": "ready", "wid": wid})
    go.wait()
    while True:  # mọi worker bắt đầu cùng một thời điểm
        d = start_at.value - time.time()
        if d <= 0:
            break
        p.poll(min(d, 0.02))

    t0_wall = time.time()
    t0 = time.perf_counter()
    topic, run_id, duration = cfg["topic"], cfg["run_id"], cfg["duration"]
    rate = cfg["rate"] / cfg["workers"] if cfg["rate"] else 0.0
    produce, poll, pack = p.produce, p.poll, HEADER.pack
    n_pool, i, seq, raw_bytes = len(pool), 0, 0, 0
    sec_sent = Counter()

    while True:
        el = time.perf_counter() - t0
        if el >= duration:
            break
        if rate:
            n = int(rate * el) - seq  # số message đến hạn gửi theo tốc độ mục tiêu
            if n <= 0:
                poll(0.001)  # chờ ~1 ms, đồng thời phục vụ delivery callback
                continue
            n = min(n, 5_000)
        else:
            n = 500
        for _ in range(n):
            key, val = pool[i]
            i += 1
            if i == n_pool:
                i = 0
            hdr = [(HEADER_KEY, pack(run_id, wid, seq))]
            while True:
                try:
                    produce(topic, val, key, on_delivery=on_delivery, headers=hdr)
                    break
                except BufferError:  # hàng đợi nội bộ đầy: chờ broker ack bớt
                    poll(0.001)
            raw_bytes += len(val)
            seq += 1
        poll(0)
        sec_sent[int(el)] += n

    produce_sec = time.perf_counter() - t0
    not_flushed = p.flush(120)
    total_sec = time.perf_counter() - t0
    end = time.perf_counter() + 1.3  # đợi thêm 1 chu kỳ thống kê để lấy số byte cuối cùng
    while time.perf_counter() < end:
        p.poll(0.1)

    out_q.put({
        "type": "done", "wid": wid, "sent": seq, "acked": acked, "errors": dict(errors),
        "not_flushed": not_flushed, "t0_wall": t0_wall, "produce_sec": produce_sec, "total_sec": total_sec,
        "raw_bytes": raw_bytes, "stats": stats, "sec_sent": dict(sec_sent),
        "lat_sample": lat.data, "lat_n": lat.n,
    })


class ProducerGroup:
    """Quản lý N tiến trình producer: khởi tạo → chờ sẵn sàng → bắt đầu đồng loạt → thu kết quả."""

    def __init__(self, cfg: dict, start_at):
        self.cfg = cfg
        self.q: mp.Queue = mp.Queue()
        self.go = mp.Event()
        self.start_at = start_at
        self.procs = [mp.Process(target=producer_worker, args=(w, cfg, self.q, self.go, start_at), daemon=True)
                      for w in range(cfg["workers"])]

    def start(self, timeout: float = 90):
        for p in self.procs:
            p.start()
        ready = 0
        while ready < len(self.procs):
            msg = self.q.get(timeout=timeout)
            if msg["type"] == "error":
                raise RuntimeError(f"Producer {msg['wid']} lỗi:\n{msg['error']}")
            ready += msg["type"] == "ready"

    def release(self):
        self.go.set()

    def collect(self, timeout: float) -> list[dict]:
        done, deadline = [], time.time() + timeout
        while len(done) < len(self.procs):
            try:
                msg = self.q.get(timeout=max(1.0, deadline - time.time()))
            except queue.Empty:
                raise TimeoutError("Producer không kết thúc đúng hạn")
            if msg["type"] == "error":
                raise RuntimeError(f"Producer {msg['wid']} lỗi:\n{msg['error']}")
            if msg["type"] == "done":
                done.append(msg)
        for p in self.procs:
            p.join(10)
        return done


def summarize_producers(done: list[dict]) -> dict:
    sent = sum(d["sent"] for d in done)
    acked = sum(d["acked"] for d in done)
    errors = Counter()
    for d in done:
        errors.update(d["errors"])
    failed = sum(errors.values())
    total_sec = max(d["total_sec"] for d in done)
    produce_sec = max(d["produce_sec"] for d in done)
    raw_bytes = sum(d["raw_bytes"] for d in done)
    tx_bytes = sum((d["stats"].get("tx_bytes") or 0) for d in done)
    sec_sent = Counter()
    for d in done:
        sec_sent.update({int(k): v for k, v in d["sec_sent"].items()})
    return {
        "sent": sent,
        "acked": acked,
        "failed": failed,
        "not_flushed": sum(d["not_flushed"] for d in done),
        "errors_by_kind": dict(errors),
        "produce_sec": round(produce_sec, 3),
        "total_sec": round(total_sec, 3),
        # throughput = số bản ghi broker đã xác nhận / thời gian tới khi flush xong
        "throughput_eps": round(acked / total_sec, 1) if total_sec else 0,
        "offered_eps": round(sent / produce_sec, 1) if produce_sec else 0,
        "raw_mb_per_sec": round(raw_bytes / total_sec / 1e6, 2) if total_sec else 0,
        "raw_bytes_per_msg": round(raw_bytes / sent, 1) if sent else None,
        "wire_bytes_per_msg": round(tx_bytes / acked, 1) if acked and tx_bytes else None,
        "compression_ratio": round(raw_bytes / tx_bytes, 2) if tx_bytes else None,
        "ack_latency_ms": latency_summary([(d["lat_sample"], d["lat_n"]) for d in done]),
        "sec_sent": dict(sorted(sec_sent.items())),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--topic", default="bakery.bench.p6")
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    ap.add_argument("--rate", type=float, default=0, help="tổng event/giây, 0 = tối đa")
    ap.add_argument("--duration", type=float, default=20, help="số giây phát tải")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--acks", default="1", choices=["0", "1", "all"])
    ap.add_argument("--batch-size", type=int, default=65536)
    ap.add_argument("--linger-ms", type=int, default=10)
    ap.add_argument("--compression", default="lz4", choices=["none", "gzip", "snappy", "lz4", "zstd"])
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    cfg = {"topic": args.topic, "bootstrap": args.bootstrap, "rate": args.rate, "duration": args.duration,
           "workers": args.workers, "acks": args.acks, "batch_size": args.batch_size, "linger_ms": args.linger_ms,
           "compression": args.compression, "seed": args.seed, "run_id": int(time.time()) & 0xFFFFFFFF}
    start_at = mp.Value("d", 0.0)
    group = ProducerGroup(cfg, start_at)
    print(f"[Load Generator] topic={args.topic} workers={args.workers} rate={'max' if not args.rate else args.rate} "
          f"acks={args.acks} batch={args.batch_size} linger={args.linger_ms}ms nén={args.compression}", flush=True)
    group.start()
    start_at.value = time.time() + 0.5
    group.release()
    s = summarize_producers(group.collect(timeout=args.duration + 180))
    print(json.dumps({k: v for k, v in s.items() if k != "sec_sent"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
