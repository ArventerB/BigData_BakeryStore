"""Bộ phát tải (load generator) cho tầng Ingestion của tiệm bánh.

Ba chế độ:
  --mode api    gửi lô JSON qua HTTP tới Collector API (đường đi giống web/app thật, có validate + DLQ)
  --mode kafka  ghi thẳng vào Kafka (đo giới hạn của Kafka, bỏ qua Collector)
  --mode file   sinh tập dữ liệu lịch sử lớn ra file JSONL theo ngày (không cần Kafka)

Ví dụ:
  python simulator/simulator.py --mode api --rate 500 --duration 60 --dirty-rate 0.03
  python simulator/simulator.py --mode api --rate 1000 --duration 180 --profile peak --peak-mult 5
  python simulator/simulator.py --mode api --rate 800 --duration 3600 --profile daily --stats-csv --tag soak60
  python simulator/simulator.py --mode kafka --rate 0 --duration 60 --workers 4 --topic bakery.bench.p6
  python simulator/simulator.py --mode file --start 2026-10-01 --days 30 --sessions-per-day 20000 --gzip

Bấm Ctrl+C để dừng sớm: các worker gửi nốt dữ liệu còn trong bộ đệm rồi vẫn ghi file kết quả.
"""
import argparse
import csv
import gzip
import json
import math
import multiprocessing as mp
import os
import queue
import random
import signal
import sys
import time
from collections import Counter
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path

from generator import VN_TZ, EventGenerator, load_catalog
from kafka_config import TOPIC_PREFIX, default_bootstrap, topic_for, tv1_profiles
from traffic import PROFILES, Traffic

# Console Windows mặc định cp1252 -> in tiếng Việt bị lỗi
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
TICK = 0.05  # giây, chu kỳ cấp "token" gửi của mỗi worker


def pct(values, p):
    if not values:
        return None
    values = sorted(values)
    return round(values[min(len(values) - 1, int(len(values) * p / 100))], 2)


class Latency:
    """Giữ độ trễ của chu kỳ báo cáo hiện tại + một mẫu ngẫu nhiên (reservoir) cho cả lần chạy."""

    def __init__(self, keep: int = 20_000):
        self.window: list[float] = []
        self.sample: list[float] = []
        self.seen = 0
        self.keep = keep

    def add(self, ms: float):
        self.window.append(ms)
        self.seen += 1
        if len(self.sample) < self.keep:
            self.sample.append(ms)
        else:
            j = random.randrange(self.seen)
            if j < self.keep:
                self.sample[j] = ms

    def drain(self) -> list[float]:
        w, self.window = self.window, []
        return w


# ---------------------------------------------------------------- senders
class ApiSender:
    RETRIES = 3

    def __init__(self, args):
        import httpx
        self.httpx = httpx
        self.client = httpx.Client(base_url=args.api_url, timeout=30)
        self.batch = []
        self.batch_size = args.batch
        self.lat = Latency()
        self.errors = 0
        self.rejected = 0

    def send(self, event):
        self.batch.append(event)
        if len(self.batch) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.batch:
            return
        for attempt in range(self.RETRIES + 1):
            t0 = time.perf_counter()
            try:
                r = self.client.post("/events", json=self.batch)
                if r.status_code >= 500:
                    raise self.httpx.HTTPStatusError("server error", request=r.request, response=r)
                r.raise_for_status()
                self.rejected += r.json()["rejected"]
                self.lat.add((time.perf_counter() - t0) * 1000)
                break
            except (self.httpx.TransportError, self.httpx.HTTPStatusError) as e:
                retriable = not isinstance(e, self.httpx.HTTPStatusError) or e.response.status_code >= 500
                if retriable and attempt < self.RETRIES:
                    time.sleep(0.5 * 2 ** attempt)  # Collector đang khởi động lại / quá tải: chờ rồi thử lại
                    continue
                self.errors += len(self.batch)
                print(f"[api] bỏ lô {len(self.batch)} event sau {attempt + 1} lần thử: {e}", flush=True)
                break
        self.batch = []

    def close(self):
        self.flush()
        self.client.close()


class KafkaSender:
    """Ghi thẳng Kafka. Mặc định theo cấu hình của TV1 (scripts/producer_config.py):
    bakery.orders / payments / dlq dùng bộ "critical" (acks=all), bakery.user-events dùng bộ
    "high throughput" (acks=1, batch 128 KB). --acks / --linger-ms / --batch-size / --compression
    ghi đè cho mọi topic (dùng khi thí nghiệm). --topic ghi mọi event vào 1 topic bằng bộ critical.
    """

    def __init__(self, args):
        from confluent_kafka import Producer
        self.prefix = args.topic_prefix
        self.fixed_topic = args.topic
        _, critical, fast = tv1_profiles()
        overrides = {k: v for k, v in {"acks": args.acks, "linger.ms": args.linger_ms, "batch.size": args.batch_size,
                                       "compression.type": args.compression}.items() if v is not None}
        if "acks" in overrides:
            overrides["enable.idempotence"] = overrides["acks"] == "all"
        base = {"bootstrap.servers": args.bootstrap, "client.id": "simulator",
                "queue.buffering.max.messages": 1_000_000}
        self.p = Producer({**base, **critical, **overrides})
        # topic clickstream có producer riêng; khi --topic cố định thì chỉ dùng 1 producer
        self.p_fast = self.p if self.fixed_topic else Producer({**base, **fast, **overrides})
        self.fast_topic = f"{self.prefix}.user-events"
        self.lat = Latency()
        self.errors = 0
        self.rejected = 0

    def _cb(self, err, msg):
        if err is not None:
            self.errors += 1
        else:
            self.lat.add(msg.latency() * 1000)  # thời gian từ produce() tới khi broker ack

    def send(self, event):
        topic = self.fixed_topic or topic_for(event, self.prefix)
        value = json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode()
        key = str(event.get("user_id", "unknown") if isinstance(event, dict) else "unknown").encode()
        p = self.p_fast if topic == self.fast_topic else self.p
        while True:
            try:
                p.produce(topic, key=key, value=value, on_delivery=self._cb)
                break
            except BufferError:
                p.poll(0.05)  # hàng đợi nội bộ đầy: chờ broker ack bớt
        p.poll(0)

    def flush(self):
        self.p.poll(0)
        self.p_fast.poll(0)

    def close(self):
        self.p.flush(60)
        self.p_fast.flush(60)


# ---------------------------------------------------------------- realtime (api / kafka)
def make_traffic(args, catalog) -> Traffic:
    return Traffic(args.profile, args.duration, peak_mult=args.peak_mult, hourly=catalog.get("hourly_traffic"),
                   day_seconds=args.day_seconds, start_hour=args.start_hour)


def make_generator(args, catalog, wid: int) -> EventGenerator:
    return EventGenerator(catalog, seed=(args.seed + wid) if args.seed is not None else None,
                          dirty_rate=args.dirty_rate, late_rate=args.late_rate, n_users=args.users,
                          dirty_kinds=args.dirty_kinds)


def realtime_worker(wid: int, args, out: mp.Queue, stop):
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # Ctrl+C do tiến trình chính xử lý qua cờ stop
    catalog = load_catalog()
    gen = make_generator(args, catalog, wid)
    traffic = make_traffic(args, catalog)
    sender = ApiSender(args) if args.mode == "api" else KafkaSender(args)
    max_events = args.max_events // args.workers if args.max_events else 0

    sent, by_type = 0, Counter()
    w = {"sent": 0, "errors": 0, "rejected": 0, "t": 0.0}  # mốc đầu chu kỳ báo cáo hiện tại
    budget, pending = 0.0, []
    start = time.perf_counter()
    next_report = args.report_every
    mult, target, hour = 1.0, 0, None

    def report(t: float, window: float):
        lat = sender.lat.drain()
        out.put({"type": "tick", "wid": wid, "idx": round(next_report / args.report_every),
                 "t": round(t, 1), "window": window, "sent": sent - w["sent"], "target": target,
                 "mult": round(mult, 3), "hour": hour, "errors": sender.errors - w["errors"],
                 "rejected": sender.rejected - w["rejected"], "lat": lat[:: max(1, len(lat) // 2000)]})
        w.update(sent=sent, errors=sender.errors, rejected=sender.rejected, t=t)

    last = start
    try:
        while not stop.is_set():
            now = time.perf_counter()
            elapsed = now - start
            if elapsed >= args.duration or (max_events and sent >= max_events):
                break
            mult = traffic.multiplier(elapsed)
            target = args.rate / args.workers * mult if args.rate > 0 else 0
            if target:
                # cấp token theo thời gian thực đã trôi qua (không phải TICK cố định) để vòng lặp chậm
                # hơn TICK vẫn bù đủ; giới hạn 1 giây để không dồn tải sau khi bị treo lâu
                budget += target * min(now - last, 1.0)
                n = int(budget)
                budget -= n
            else:
                n = 500
            last = now
            if max_events:
                n = min(n, max_events - sent)
            hour = traffic.hour_at(elapsed)
            for _ in range(n):
                if not pending:
                    pending = gen.session(hour=int(hour) if hour is not None else None)
                    pending.reverse()
                e = pending.pop()
                sender.send(e)
                by_type[e.get("event_type", "?") if isinstance(e, dict) else "not_an_object"] += 1
            sent += n
            if target:
                sender.flush()
                sleep = TICK - (time.perf_counter() - now)
                if sleep > 0:
                    time.sleep(sleep)
            if elapsed >= next_report:
                sender.flush()
                t_now = time.perf_counter() - start
                report(t_now, t_now - w["t"])  # cửa sổ thực tế, kể cả khi báo cáo bị trễ
                next_report += args.report_every
        # gửi nốt phiên đang dở: không cắt cụt phiên và để số bẩn đã đếm khớp với số Collector từ chối
        while pending:
            e = pending.pop()
            sender.send(e)
            by_type[e.get("event_type", "?") if isinstance(e, dict) else "not_an_object"] += 1
            sent += 1
    finally:
        sender.close()
    # chu kỳ cuối (chưa đủ report_every giây)
    elapsed = time.perf_counter() - start
    if sent > w["sent"] or sender.lat.window:
        report(elapsed, max(elapsed - w["t"], 1e-3))
    out.put({"type": "done", "wid": wid, "sent": sent, "seconds": time.perf_counter() - start,
             "by_type": dict(by_type), "errors": sender.errors, "rejected": sender.rejected,
             "dirty_by_kind": dict(gen.dirty_by_kind), "late_sessions": gen.late_injected,
             "lat_sample": sender.lat.sample})


class StatsWriter:
    """Gộp số liệu từng chu kỳ của mọi worker thành 1 dòng CSV (dùng vẽ biểu đồ khi chạy dài)."""

    FIELDS = ["wall_time", "elapsed_sec", "virtual_hour", "rate_mult", "target_eps", "actual_eps",
              "sent_total", "send_errors", "rejected_by_collector", "lat_p50_ms", "lat_p95_ms", "lat_p99_ms"]

    def __init__(self, path: Path | None, workers: int):
        self.workers = workers
        self.rows: dict[int, list[dict]] = {}
        self.sent_total = 0
        self.f = open(path, "w", newline="", encoding="utf-8") if path else None
        self.w = csv.DictWriter(self.f, self.FIELDS) if self.f else None
        if self.w:
            self.w.writeheader()

    def add(self, tick: dict):
        self.rows.setdefault(tick["idx"], []).append(tick)
        if len(self.rows[tick["idx"]]) == self.workers:
            self._emit(tick["idx"])

    def _emit(self, idx: int):
        ticks = self.rows.pop(idx)
        sent = sum(t["sent"] for t in ticks)
        window = max(t["window"] for t in ticks)
        self.sent_total += sent
        lat = [x for t in ticks for x in t["lat"]]
        hour = ticks[0]["hour"]
        row = {
            "wall_time": datetime.now().isoformat(timespec="seconds"),
            "elapsed_sec": ticks[0]["t"],
            "virtual_hour": round(hour, 2) if hour is not None else "",
            "rate_mult": ticks[0]["mult"],
            "target_eps": round(sum(t["target"] for t in ticks), 1),
            "actual_eps": round(sent / window, 1),
            "sent_total": self.sent_total,
            "send_errors": sum(t["errors"] for t in ticks),
            "rejected_by_collector": sum(t["rejected"] for t in ticks),
            "lat_p50_ms": pct(lat, 50), "lat_p95_ms": pct(lat, 95), "lat_p99_ms": pct(lat, 99),
        }
        hour_txt = f" giờ ảo {row['virtual_hour']:>5}h" if hour is not None else ""
        print(f"t={row['elapsed_sec']:>7}s{hour_txt}  tốc độ ≈ {row['actual_eps']:>9,.0f} ev/s"
              f"  p95={row['lat_p95_ms']} ms  lỗi={row['send_errors']}  bị từ chối={row['rejected_by_collector']}",
              flush=True)
        if self.w:
            self.w.writerow(row)
            self.f.flush()  # chạy dài: ghi ngay để mở file xem giữa chừng được

    def close(self):
        for idx in sorted(self.rows):  # chu kỳ cuối có worker dừng trước khi kịp báo
            self._emit(idx)
        if self.f:
            self.f.close()


def collect(procs, out: mp.Queue, stop, on_tick=None) -> list[dict]:
    """Đọc hàng đợi kết quả tới khi mọi worker báo xong. Ctrl+C -> bật cờ stop, vẫn chờ worker kết thúc."""
    done = []
    while len(done) < len(procs):
        try:
            msg = out.get(timeout=1)
        except queue.Empty:
            if not any(p.is_alive() for p in procs) and out.empty():
                break
            continue
        except KeyboardInterrupt:
            if not stop.is_set():
                print("\nĐang dừng: chờ các worker gửi nốt bộ đệm...", flush=True)
                stop.set()
            continue
        if msg["type"] == "tick":
            if on_tick:
                on_tick(msg)
        else:
            done.append(msg)
    for p in procs:
        p.join()
    return done


def run_realtime(args, t_start: float, stamp: str) -> dict:
    out: mp.Queue = mp.Queue()
    stop = mp.Event()
    csv_path = RESULTS_DIR / f"sim_{args.mode}_{args.tag or 'run'}_{stamp}.csv" if args.stats_csv else None
    stats = StatsWriter(csv_path, args.workers)
    procs = [mp.Process(target=realtime_worker, args=(i, args, out, stop)) for i in range(args.workers)]
    for p in procs:
        p.start()
    done = collect(procs, out, stop, stats.add)
    stats.close()

    sent = sum(d["sent"] for d in done)
    secs = max(d["seconds"] for d in done) if done else 0
    lats = [x for d in done for x in d["lat_sample"]]
    by_type, dirty = Counter(), Counter()
    for d in done:
        by_type.update(d["by_type"])
        dirty.update(d["dirty_by_kind"])
    return {
        "sent_total": sent,
        "duration_sec": round(secs, 2),
        "throughput_eps": round(sent / secs, 1) if secs else 0,
        "stopped_early": stop.is_set(),
        "send_errors": sum(d["errors"] for d in done),
        "rejected_by_collector": sum(d["rejected"] for d in done),
        "dirty_injected": sum(dirty.values()),
        "dirty_by_kind": dict(dirty),
        "late_sessions": sum(d["late_sessions"] for d in done),
        "by_event_type": dict(by_type),
        "ack_latency_ms": {"what": "HTTP batch round-trip" if args.mode == "api" else "Kafka broker ack",
                           "p50": pct(lats, 50), "p95": pct(lats, 95), "p99": pct(lats, 99)},
        "stats_csv": str(csv_path) if csv_path else None,
    }


# ---------------------------------------------------------------- file (tập dữ liệu lịch sử)
def sessions_in_hour(rng: random.Random, lam: float) -> int:
    """Số phiên trong 1 giờ ~ Poisson(lam). λ lớn thì xấp xỉ bằng phân phối chuẩn cho nhanh."""
    if lam >= 30:
        return max(0, round(rng.gauss(lam, lam ** 0.5)))
    limit, k, p = math.exp(-lam), 0, rng.random()  # thuật toán Knuth
    while p > limit:
        k += 1
        p *= rng.random()
    return k


def _sort_key(e) -> str:
    t = e.get("event_time") if isinstance(e, dict) else None
    return t if isinstance(t, str) and t[:2] == "20" else ""


def file_worker(wid: int, args, days: list[str], out: mp.Queue, stop):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    catalog = load_catalog()
    gen = make_generator(args, catalog, wid)
    rng = random.Random(None if args.seed is None else args.seed * 1000 + wid)
    hourly = catalog["hourly_traffic"]
    weights = [h / sum(hourly) for h in hourly]
    weekend = catalog.get("weekend_traffic", 1.0)
    out_root = Path(args.out)
    per_day = {}
    by_type = Counter()
    for day_s in days:
        if stop.is_set():
            break
        day = date.fromisoformat(day_s)
        factor = weekend if day.weekday() >= 5 else 1.0
        folder = out_root / f"date={day_s}"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / ("events.jsonl.gz" if args.gzip else "events.jsonl")
        # gzip mức 6: nhanh hơn mức 9 mặc định ~35%, file chỉ lớn hơn ~5%
        opener = (lambda p, m, encoding: gzip.open(p, m, compresslevel=6, encoding=encoding)) if args.gzip else open
        n_events = n_sessions = n_orders = n_bytes = 0
        revenue = 0.0
        with opener(path, "wt", encoding="utf-8") as f:
            for hour in range(24):
                hour_start = datetime.combine(day, dtime(hour), tzinfo=VN_TZ)
                buf = []
                for _ in range(sessions_in_hour(rng, args.sessions_per_day * weights[hour] * factor)):
                    start = hour_start + timedelta(seconds=rng.uniform(0, 3600))
                    buf.extend(gen.session(start=start, hour=hour))
                    n_sessions += 1
                buf.sort(key=_sort_key)
                for e in buf:
                    line = json.dumps(e, ensure_ascii=False, separators=(",", ":")) + "\n"
                    f.write(line)
                    n_bytes += len(line.encode("utf-8"))
                    if isinstance(e, dict):
                        by_type[e.get("event_type", "?")] += 1
                        if e.get("event_type") == "payment" and e.get("status") == "success":
                            n_orders += 1
                            revenue += e.get("amount") or 0
                    else:
                        by_type["not_an_object"] += 1
                n_events += len(buf)
        per_day[day_s] = {"file": path.relative_to(out_root).as_posix(), "sessions": n_sessions, "events": n_events,
                          "paid_orders": n_orders, "revenue_vnd": round(revenue), "raw_bytes": n_bytes,
                          "file_bytes": path.stat().st_size}
        out.put({"type": "tick", "wid": wid, "day": day_s, "events": n_events})
    out.put({"type": "done", "wid": wid, "per_day": per_day, "by_type": dict(by_type),
             "dirty_by_kind": dict(gen.dirty_by_kind), "late_sessions": gen.late_injected})


def write_dims(args, out_root: Path) -> dict:
    """Bảng danh mục dạng CSV (dữ liệu có cấu trúc) đi kèm event JSONL (bán cấu trúc), để nhóm sau JOIN."""
    catalog = load_catalog()
    gen = EventGenerator(catalog, n_users=args.users)
    folder = out_root / "dim"
    folder.mkdir(parents=True, exist_ok=True)
    rows = {
        "products": [{k: p[k] for k in ("product_id", "name", "category", "price")} for p in catalog["products"]],
        "stores": [{"store_id": s, "city": s.split("-")[0]} for s in catalog["stores"]],
        "customers": (gen.customer_profile(i) for i in range(1, args.users + 1)),
    }
    counts = {}
    for name, data in rows.items():
        data = iter(data)
        first = next(data)
        with open(folder / f"{name}.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, list(first))
            w.writeheader()
            w.writerow(first)
            n = 1
            for row in data:
                w.writerow(row)
                n += 1
        counts[f"dim/{name}.csv"] = n
    return counts


def run_file(args, t_start: float, stamp: str) -> dict:
    start = date.fromisoformat(args.start)
    days = [(start + timedelta(days=i)).isoformat() for i in range(args.days)]
    workers = min(args.workers, len(days))
    out: mp.Queue = mp.Queue()
    stop = mp.Event()
    procs = [mp.Process(target=file_worker, args=(i, args, days[i::workers], out, stop)) for i in range(workers)]
    for p in procs:
        p.start()

    def on_tick(m):
        print(f"  xong ngày {m['day']}: {m['events']:>10,} event", flush=True)

    done = collect(procs, out, stop, on_tick)
    secs = time.time() - t_start
    per_day, by_type, dirty = {}, Counter(), Counter()
    for d in done:
        per_day.update(d["per_day"])
        by_type.update(d["by_type"])
        dirty.update(d["dirty_by_kind"])
    total = sum(v["events"] for v in per_day.values())
    dims = write_dims(args, Path(args.out))
    manifest = {
        "generated_at": datetime.fromtimestamp(t_start).isoformat(timespec="seconds"),
        "config": {k: v for k, v in vars(args).items() if k in
                   ("start", "days", "sessions_per_day", "users", "dirty_rate", "late_rate", "seed", "gzip")},
        "schema_version": 1,
        "events_total": total,
        "sessions_total": sum(v["sessions"] for v in per_day.values()),
        "raw_bytes": sum(v["raw_bytes"] for v in per_day.values()),
        "file_bytes": sum(v["file_bytes"] for v in per_day.values()),
        "late_sessions": sum(d["late_sessions"] for d in done),
        "dims": dims,
        "dirty_injected": sum(dirty.values()),
        "dirty_by_kind": dict(dirty),
        "by_event_type": dict(by_type),
        "days": dict(sorted(per_day.items())),
    }
    Path(args.out).mkdir(parents=True, exist_ok=True)
    (Path(args.out) / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2),
                                                  encoding="utf-8")
    return {
        "out_dir": str(Path(args.out).resolve()),
        "events_total": total,
        "sessions_total": manifest["sessions_total"],
        "raw_mb": round(manifest["raw_bytes"] / 2**20, 1),
        "file_mb": round(manifest["file_bytes"] / 2**20, 1),
        "duration_sec": round(secs, 2),
        "generate_eps": round(total / secs, 1) if secs else 0,
        "stopped_early": stop.is_set(),
        "dirty_injected": manifest["dirty_injected"],
        "dirty_by_kind": manifest["dirty_by_kind"],
        "by_event_type": manifest["by_event_type"],
    }


# ---------------------------------------------------------------- main
def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["api", "kafka", "file"], default="api")
    ap.add_argument("--workers", type=int, default=2, help="số tiến trình producer song song")
    ap.add_argument("--users", type=int, default=None,
                    help="số thành viên; mặc định 50 000, chế độ file thì tự tính ≈ 4 phiên/thành viên/tháng")
    ap.add_argument("--dirty-rate", type=float, default=0.0, help="tỷ lệ event bẩn, ví dụ 0.03")
    ap.add_argument("--dirty-kinds", default=None,
                    help="chỉ chèn các kiểu bẩn này, cách nhau dấu phẩy (mặc định: tất cả, xem generator.py)")
    ap.add_argument("--late-rate", type=float, default=0.0, help="tỷ lệ phiên gửi trễ 1–6 giờ")
    ap.add_argument("--seed", type=int, default=None, help="cố định để sinh lại đúng tập dữ liệu")
    ap.add_argument("--tag", default="", help="nhãn thí nghiệm, ghi vào tên file kết quả")
    g = ap.add_argument_group("realtime (api / kafka)")
    g.add_argument("--rate", type=float, default=1000, help="tổng event/giây mục tiêu, 0 = tối đa")
    g.add_argument("--duration", type=float, default=60, help="số giây chạy")
    g.add_argument("--max-events", type=int, default=0, help="dừng khi đã gửi đủ số event này (0 = không giới hạn)")
    g.add_argument("--profile", choices=PROFILES, default="constant", help="kịch bản lưu lượng, xem traffic.py")
    g.add_argument("--peak-mult", type=float, default=3, help="hệ số cao điểm cho profile peak / flash")
    g.add_argument("--day-seconds", type=float, default=None, help="profile daily: số giây thật cho 1 ngày ảo")
    g.add_argument("--start-hour", type=float, default=0, help="profile daily: giờ ảo lúc bắt đầu")
    g.add_argument("--report-every", type=float, default=5.0, help="chu kỳ in / ghi số liệu (giây)")
    g.add_argument("--stats-csv", action="store_true", help="ghi số liệu từng chu kỳ ra results/*.csv")
    g.add_argument("--api-url", default=os.getenv("COLLECTOR_URL", "http://localhost:8000"))
    g.add_argument("--batch", type=int, default=200, help="api: số event mỗi request HTTP")
    g.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP") or default_bootstrap(),
                   help="mặc định lấy từ scripts/producer_config.py của TV1")
    g.add_argument("--topic-prefix", default=os.getenv("TOPIC_PREFIX", TOPIC_PREFIX))
    g.add_argument("--topic", default=None, help="kafka: ghi mọi event vào 1 topic, ví dụ bakery.bench.p6")
    g.add_argument("--acks", default=None, choices=["0", "1", "all"], help="ghi đè cấu hình TV1 cho mọi topic")
    g.add_argument("--linger-ms", type=int, default=None)
    g.add_argument("--batch-size", type=int, default=None)
    g.add_argument("--compression", default=None, choices=["none", "gzip", "snappy", "lz4", "zstd"])
    g = ap.add_argument_group("file (tập dữ liệu lịch sử)")
    g.add_argument("--start", default=None, help="ngày đầu (YYYY-MM-DD), mặc định 30 ngày trước")
    g.add_argument("--days", type=int, default=7)
    g.add_argument("--sessions-per-day", type=int, default=10_000, help="số phiên trung bình mỗi ngày thường")
    g.add_argument("--out", default=None, help="thư mục ra, mặc định data/dataset_<tag hoặc thời gian>")
    g.add_argument("--gzip", action="store_true", help="nén file .jsonl.gz")
    args = ap.parse_args(argv)

    if args.workers < 1:
        ap.error("--workers phải >= 1")
    if not 0 <= args.dirty_rate < 1 or not 0 <= args.late_rate < 1:
        ap.error("--dirty-rate và --late-rate nằm trong [0, 1)")
    if args.dirty_kinds:
        args.dirty_kinds = [k.strip() for k in args.dirty_kinds.split(",") if k.strip()]
    if args.users is None:
        # thành viên chiếm ~75% số phiên; chọn số thành viên để mỗi người ~4 phiên / 30 ngày
        auto = round(args.sessions_per_day * 30 * 0.75 / 4)
        args.users = max(50_000, auto) if args.mode == "file" else 50_000
    if args.mode == "file":
        args.start = args.start or (date.today() - timedelta(days=30)).isoformat()
        try:
            date.fromisoformat(args.start)
        except ValueError:
            ap.error("--start phải có dạng YYYY-MM-DD")
    return args


def main(argv=None):
    args = parse_args(argv)
    t_start = time.time()
    stamp = str(int(t_start))
    if args.mode == "file":
        args.out = args.out or str(RESULTS_DIR.parent / "data" / f"dataset_{args.tag or stamp}")
        print(f"Sinh tập dữ liệu: {args.days} ngày từ {args.start}, ~{args.sessions_per_day:,} phiên/ngày, "
              f"workers={args.workers} dirty={args.dirty_rate} -> {args.out}", flush=True)
        result = run_file(args, t_start, stamp)
    else:
        if args.mode == "kafka" and args.dirty_rate:
            print("Lưu ý: chế độ kafka bỏ qua Collector nên dữ liệu bẩn đi thẳng vào topic, không bị lọc.")
        print(f"Bắt đầu: mode={args.mode} rate={args.rate or 'max'} duration={args.duration}s "
              f"workers={args.workers} profile={args.profile} dirty={args.dirty_rate}", flush=True)
        result = run_realtime(args, t_start, stamp)

    summary = {"tag": args.tag, "mode": args.mode,
               "started_at": datetime.fromtimestamp(t_start).isoformat(timespec="seconds"),
               **result, "config": vars(args)}
    RESULTS_DIR.mkdir(exist_ok=True)
    fname = RESULTS_DIR / f"sim_{args.mode}_{args.tag or 'run'}_{stamp}.json"
    fname.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in summary.items() if k != "config"}, ensure_ascii=False, indent=2))
    print(f"Đã lưu kết quả: {fname}")


if __name__ == "__main__":
    main()
