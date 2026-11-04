"""
Bộ điều phối thí nghiệm benchmark của TV4 – Bakery Store (chạy trên cụm Kafka KRaft 3 broker THẬT).

Mỗi lần chạy (run):
  1. Kiểm tra cụm khoẻ (3 broker, mọi partition đủ 3 bản sao in-sync) và dung lượng đĩa còn trống.
  2. Xoá + tạo lại topic benchmark (giữ đúng spec của TV1: RF=3, min.insync.replicas=2) để mỗi lần đo bắt đầu
     từ topic rỗng và dữ liệu cũ không chiếm đĩa.
  3. Khởi động consumer kiểm chứng (bench_consumer.py) tại offset đầu → khởi động N producer (load_generator.py)
     → mọi producer bắt đầu cùng một thời điểm.
  4. Producer gửi trong --duration giây rồi flush; consumer đọc tới khi lag = 0 (đã nhận hết những gì broker giữ).
  5. Ghi results/tv4/raw/<cấu hình>_r<lần>.json (tổng kết) và .csv (số liệu từng giây).

Không có chế độ "giả lập": nếu không kết nối được Kafka thì dừng và báo lỗi.

Ví dụ:
  python benchmark/run_benchmarks.py --list
  python benchmark/run_benchmarks.py --experiments pilot --reps 1 --duration 15
  python benchmark/run_benchmarks.py --experiments all --reps 3 --duration 30
  python benchmark/run_benchmarks.py --experiments fault --reps 2
Sau đó: python benchmark/summarize.py && python benchmark/plot_results.py
"""
import argparse
import csv
import json
import multiprocessing as mp
import os
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil

from bench_consumer import ConsumerGroup
from bench_utils import DEFAULT_BOOTSTRAP, latency_summary
from load_generator import ProducerGroup, summarize_producers

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "results" / "tv4"
LOG_FILE: Path | None = None

TOPIC_PARTITIONS = {"bakery.bench.p1": 1, "bakery.bench.p3": 3, "bakery.bench.p6": 6}
# như scripts/create_topics.ps1 của TV1; file.delete.delay.ms=1s để topic bị xoá giải phóng bộ nhớ ngay
TOPIC_CONFIG = {"min.insync.replicas": "2", "file.delete.delay.ms": "1000"}

# Cấu hình gốc: mỗi thí nghiệm chỉ thay đổi MỘT nhóm tham số so với cấu hình này
BASE = {"topic": "bakery.bench.p6", "workers": 4, "acks": "1", "batch_size": 65536, "linger_ms": 10,
        "compression": "lz4", "rate": 0}
# Hai bộ cấu hình producer của TV1 (scripts/producer_config.py)
TV1_CRITICAL = {"acks": "all", "batch_size": 65536, "linger_ms": 10, "compression": "lz4"}
TV1_CLICKSTREAM = {"acks": "1", "batch_size": 131072, "linger_ms": 20, "compression": "lz4"}


def experiments(latency_rate: int, load_rates: list[int]) -> dict:
    """Danh sách thí nghiệm: tên → (mô tả, [(nhãn, cấu hình ghi đè)])."""
    return {
        "E1_partitions": ("Số partition 1/3/6 (4 producer, acks=1, 64KB, 10ms, lz4, tốc độ tối đa)",
                          [(f"p{n}", {"topic": f"bakery.bench.p{n}"}) for n in (1, 3, 6)]),
        "E1b_partitions_producer_only": ("Như E1 nhưng KHÔNG chạy consumer kiểm chứng (tách ảnh hưởng tranh CPU)",
                                         [(f"p{n}", {"topic": f"bakery.bench.p{n}", "verify": False})
                                          for n in (1, 3, 6)]),
        "E2_producers": ("Số producer 1/2/4/8 trên topic 6 partition",
                         [(f"{w} producer", {"workers": w}) for w in (1, 2, 4, 8)]),
        "E3_batch": ("batch.size 16KB/64KB/256KB/1MB (linger 10ms, lz4)",
                     [(lbl, {"batch_size": b}) for lbl, b in
                      (("16KB", 16384), ("64KB", 65536), ("256KB", 262144), ("1MB", 1000000))]),
        "E4_linger": ("linger.ms 0/5/20/50 ở tốc độ tối đa (batch 128KB, lz4)",
                      [(f"{l}ms", {"batch_size": 131072, "linger_ms": l}) for l in (0, 5, 20, 50)]),
        "E5_compression": ("Thuật toán nén (cấu hình clickstream của TV1: acks=1, 128KB, 20ms)",
                           [(c, {**TV1_CLICKSTREAM, "compression": c})
                            for c in ("none", "gzip", "snappy", "lz4", "zstd")]),
        "E6_acks": ("acks 0/1/all (64KB, 10ms, lz4)",
                    [(f"acks={a}", {"acks": a}) for a in ("0", "1", "all")]),
        "E7_latency_linger": (f"Độ trễ theo linger.ms ở tải cố định {latency_rate:,} event/s (128KB, lz4, acks=1)",
                              [(f"{l}ms", {"batch_size": 131072, "linger_ms": l, "rate": latency_rate})
                               for l in (0, 5, 20, 50)]),
        "E8_load_curve": ("Độ trễ theo tải – cấu hình đơn hàng của TV1 (acks=all, 64KB, 10ms, lz4) trên topic 3 partition",
                          [(f"{r // 1000}k eps" if r else "tối đa", {**TV1_CRITICAL, "topic": "bakery.bench.p3", "rate": r})
                           for r in load_rates]),
    }


FAULT_RUNS = [  # tắt đột ngột (docker kill) broker đang làm leader partition 0 ở giây 30, bật lại ở giây 60
    ("acks=all", {**TV1_CRITICAL, "topic": "bakery.bench.p3", "rate": 5000, "workers": 2}),
    ("acks=1", {**TV1_CRITICAL, "acks": "1", "topic": "bakery.bench.p3", "rate": 5000, "workers": 2}),
]


def log(msg: str):
    line = f"[{datetime.now():%H:%M:%S}] {msg}"
    print(line, flush=True)
    if LOG_FILE:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def config_id(c: dict) -> str:
    rate = "max" if not c["rate"] else f"{c['rate'] // 1000}k"
    b = c["batch_size"]
    bl = f"{b // 1024}k" if b < 1_000_000 else "1m"
    return (f"p{TOPIC_PARTITIONS[c['topic']]}_w{c['workers']}_acks{c['acks']}_b{bl}_l{c['linger_ms']}"
            f"_{c['compression']}_{rate}" + ("_noverify" if c.get("verify") is False else ""))


# ------------------------------------------------------------------ Kafka admin
def admin():
    from confluent_kafka.admin import AdminClient
    return AdminClient({"bootstrap.servers": DEFAULT_BOOTSTRAP})


def cluster_state(adm, topics: list[str]) -> dict:
    md = adm.list_topics(timeout=10)
    st = {"brokers": sorted(md.brokers), "partitions": 0, "no_leader": 0, "under_replicated": 0, "leaders": {}}
    for t in topics:
        tm = md.topics.get(t)
        if tm is None or tm.error is not None:
            st["no_leader"] += 1
            continue
        for pid, pm in tm.partitions.items():
            st["partitions"] += 1
            st["leaders"][f"{t}:{pid}"] = pm.leader
            st["no_leader"] += pm.leader < 0
            st["under_replicated"] += len(pm.isrs) < len(pm.replicas)
    return st


def wait_healthy(adm, topics: list[str], timeout: float = 240) -> float:
    t0 = time.time()
    while True:
        try:
            st = cluster_state(adm, topics)
            if len(st["brokers"]) == 3 and st["no_leader"] == 0 and st["under_replicated"] == 0:
                return time.time() - t0
        except Exception:
            pass
        if time.time() - t0 > timeout:
            raise RuntimeError(f"Cụm Kafka chưa khoẻ sau {timeout}s: {topics}")
        time.sleep(1)


def reset_topic(adm, name: str):
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import NewTopic

    try:
        adm.delete_topics([name], operation_timeout=30)[name].result()
    except KafkaException:
        pass  # topic chưa tồn tại
    for _ in range(120):
        if name not in adm.list_topics(timeout=10).topics:
            break
        time.sleep(0.5)
    for _ in range(60):
        try:
            adm.create_topics([NewTopic(name, TOPIC_PARTITIONS[name], 3, config=TOPIC_CONFIG)],
                              operation_timeout=30)[name].result()
            break
        except KafkaException:
            time.sleep(1)  # topic cũ đang được xoá nốt
    wait_healthy(adm, [name], timeout=120)


def start_offsets(topics: list[str]) -> list[tuple[str, int, int]]:
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": DEFAULT_BOOTSTRAP, "group.id": "tv4-offsets"})
    out = []
    for t in topics:
        for _ in range(60):  # topic vừa tạo lại: chờ metadata lan tới mọi broker
            tm = c.list_topics(t, timeout=10).topics.get(t)
            if tm and tm.error is None and len(tm.partitions) == TOPIC_PARTITIONS.get(t, len(tm.partitions)) \
                    and all(pm.leader >= 0 for pm in tm.partitions.values()):
                break
            time.sleep(0.5)
        for pid in sorted(tm.partitions):
            for attempt in range(60):  # leader có thể chưa nạp xong partition vừa tạo
                try:
                    _, hi = c.get_watermark_offsets(TopicPartition(t, pid), timeout=10, cached=False)
                    break
                except Exception:
                    if attempt == 59:
                        raise
                    time.sleep(0.5)
            out.append((t, pid, hi))
    c.close()
    return out


def check_disk(path: str, min_free_gb: float):
    free = shutil.disk_usage(path).free / 1e9
    if free < min_free_gb:
        raise SystemExit(f"[DỪNG] Ổ {path} chỉ còn {free:.1f} GB (< {min_free_gb} GB). Dừng để tránh đầy đĩa.")
    return free


# ------------------------------------------------------------------ một lần chạy
def fault_injector(adm, topic: str, start_at: float, kill_at: float, restart_at: float, events: list,
                   stop: threading.Event):
    """Tắt đột ngột broker đang làm leader partition 0, rồi bật lại; ghi trạng thái cụm mỗi giây."""
    leader = cluster_state(adm, [topic])["leaders"][f"{topic}:0"]
    container = f"kafka-{leader}"
    killed = restarted = False
    while not stop.is_set():
        el = time.time() - start_at
        if not killed and el >= kill_at:
            subprocess.run(["docker", "kill", container], capture_output=True)
            events.append({"t": round(time.time() - start_at, 2), "event": f"docker kill {container}"})
            killed = True
        if killed and not restarted and el >= restart_at:
            subprocess.run(["docker", "start", container], capture_output=True)
            events.append({"t": round(time.time() - start_at, 2), "event": f"docker start {container}"})
            restarted = True
        try:
            st = cluster_state(adm, [topic])
            events.append({"t": round(time.time() - start_at, 2), "brokers": len(st["brokers"]),
                           "under_replicated": st["under_replicated"], "no_leader": st["no_leader"],
                           "leaders": list(st["leaders"].values())})
        except Exception as e:
            events.append({"t": round(time.time() - start_at, 2), "error": str(e)[:200]})
        stop.wait(1.0)


def run_once(cfg: dict, rep: int, exp_names: list[str], args, adm, fault: dict | None = None,
             duration: float | None = None, prefix: str = "") -> dict:
    free = check_disk(args.disk_path, args.min_free_gb)
    topic = cfg["topic"]
    cid = prefix + config_id(cfg) + (f"_fault_{fault['label'].replace('=', '')}" if fault else "")
    log(f"▶ {cid} | lần {rep} | {', '.join(exp_names)} | đĩa trống {free:.1f} GB")

    wait_healthy(adm, [])  # đủ 3 broker (topic có thể chưa tồn tại – reset_topic sẽ tạo)
    reset_topic(adm, topic)
    run_id = int(time.time() * 1000) & 0xFFFFFFFF
    duration = fault["duration"] if fault else (duration or args.duration)
    pcfg = {**cfg, "bootstrap": DEFAULT_BOOTSTRAP, "duration": duration, "run_id": run_id, "seed": 42 + rep}
    ccfg = {"bootstrap": DEFAULT_BOOTSTRAP, "run_id": run_id, "mode": "header", "warmup_sec": 3.0,
            "idle_timeout": 60.0 if fault else 20.0, "max_drain_sec": 600.0}

    verify = cfg.get("verify", True)
    start_at = mp.Value("d", 0.0)
    assigns = start_offsets([topic])
    consumers = ConsumerGroup(ccfg, assigns, start_at, n_proc=min(3, len(assigns))) if verify else None
    if consumers:
        consumers.start()
    producers = ProducerGroup(pcfg, start_at)
    producers.start()
    t_start = time.time() + 1.0
    start_at.value = t_start
    producers.release()

    # CPU toàn máy mỗi giây (gồm cả VM Docker chạy broker) – để giải thích giới hạn của thí nghiệm 1 máy
    cpu: dict[int, float] = {}
    stop_cpu = threading.Event()

    def cpu_loop():
        psutil.cpu_percent(None)
        while not stop_cpu.wait(max(0.0, 1.0 - (time.time() - t_start) % 1.0)):
            cpu[int(time.time() - t_start)] = psutil.cpu_percent(None)

    threading.Thread(target=cpu_loop, daemon=True).start()

    events, stop_fault, th = [], threading.Event(), None
    if fault:
        th = threading.Thread(target=fault_injector, daemon=True,
                              args=(admin(), topic, t_start, fault["kill_at"], fault["restart_at"], events, stop_fault))
        th.start()

    pdone = producers.collect(timeout=duration + 300)
    psum = summarize_producers(pdone)
    cdone = consumers.finish(timeout=900) if consumers else []
    stop_cpu.set()
    if th:
        # tiếp tục ghi trạng thái cụm tới khi đủ ISR trở lại
        try:
            heal = wait_healthy(adm, [topic], timeout=240)
        except RuntimeError:
            heal = None
        stop_fault.set()
        th.join(5)
    try:  # xoá dữ liệu của lần chạy ngay để Kafka giải phóng đĩa sớm (file bị xoá sau ~60s)
        adm.delete_topics([topic], operation_timeout=30)[topic].result()
    except Exception:
        pass
    received = sum(d["received"] for d in cdone)
    unique = sum(d["unique"] for d in cdone)
    dup = sum(d["duplicates"] for d in cdone)
    firsts = [d["first_msg"] for d in cdone if d["first_msg"]]
    lasts = [d["last_msg"] for d in cdone if d["last_msg"]]
    consume_span = (max(lasts) - min(firsts)) if firsts else 0
    # mất = đã gửi − nhận được (duy nhất) − bị producer báo lỗi (không được ghi) − chưa flush kịp
    lost_after_ack = (psum["sent"] - unique - psum["failed"] - psum["not_flushed"]) if verify else None

    # gộp số liệu từng giây
    sec_rows: dict[int, dict] = {}
    for s, n in psum["sec_sent"].items():
        sec_rows.setdefault(int(s), {})["produced"] = n
    for d in cdone:
        for s, (n, p50, p95, p99) in d["sec_rows"].items():
            row = sec_rows.setdefault(int(s), {})
            row["consumed"] = row.get("consumed", 0) + n
            # nhiều consumer: lấy phân vị lớn nhất (thận trọng)
            for k, v in (("e2e_p50_ms", p50), ("e2e_p95_ms", p95), ("e2e_p99_ms", p99)):
                if v is not None:
                    row[k] = max(row.get(k, 0), v)
    for s, v in cpu.items():
        sec_rows.setdefault(s, {})["host_cpu_pct"] = v
    secs = [s for s in sec_rows if s >= 0]
    if secs:
        span = range(0, max(secs) + 1)
        for d in cdone:  # lag: điền tiếp giá trị gần nhất cho từng consumer rồi cộng
            last = 0
            lag = {int(k): v for k, v in d["lag"].items()}
            for s in span:
                last = lag.get(s, last)
                sec_rows.setdefault(s, {})
                sec_rows[s]["lag"] = sec_rows[s].get("lag", 0) + last

    raw_dir = OUT_DIR / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{cid}_r{rep}"
    csv_path = raw_dir / f"{stem}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["second", "produced", "consumed", "e2e_p50_ms", "e2e_p95_ms", "e2e_p99_ms", "lag",
                    "host_cpu_pct"])
        for s in sorted(x for x in sec_rows if x >= 0):
            r = sec_rows[s]
            w.writerow([s, r.get("produced", 0), r.get("consumed", 0), r.get("e2e_p50_ms", ""),
                        r.get("e2e_p95_ms", ""), r.get("e2e_p99_ms", ""), r.get("lag", ""),
                        r.get("host_cpu_pct", "")])
    busy = [v for s, v in cpu.items() if 1 <= s < duration]  # trong lúc producer đang phát

    summary = {
        "config_id": cid, "rep": rep, "experiments": exp_names, "run_id": run_id,
        "started_at": datetime.fromtimestamp(t_start, tz=timezone.utc).isoformat(),
        "config": {**cfg, "partitions": TOPIC_PARTITIONS[topic], "duration": duration},
        "producer": {k: v for k, v in psum.items() if k != "sec_sent"},
        "host_cpu_pct": {"avg_while_producing": round(sum(busy) / len(busy), 1) if busy else None,
                         "max": max(busy, default=None), "logical_cpus": psutil.cpu_count()},
        "consumer": {
            "processes": len(cdone), "received": received, "unique": unique, "duplicates": dup,
            "foreign": sum(d["foreign"] for d in cdone), "lost_after_ack": lost_after_ack,
            "consume_eps": round(received / consume_span, 1) if consume_span else None,
            "drain_after_producer_sec": round(max(lasts) - (t_start + psum["total_sec"]), 2) if lasts else None,
            "max_lag": max((r.get("lag", 0) for r in sec_rows.values()), default=0) if verify else None,
            "timed_out": any(d["timed_out"] for d in cdone),
            "e2e_latency_ms": latency_summary([(d["lat_sample"], d["lat_n"]) for d in cdone]),
            "verified": verify,
        },
        "fault": {"label": fault["label"], "kill_at": fault["kill_at"], "restart_at": fault["restart_at"],
                  "time_to_full_isr_after_run_sec": heal, "timeline": events} if fault else None,
        "per_second_csv": str(csv_path.relative_to(ROOT)),
    }
    (raw_dir / f"{stem}.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    c, p = summary["consumer"], summary["producer"]
    log(f"  ✓ {p['throughput_eps']:>10,.0f} eps | ack p95 {p['ack_latency_ms']['p95']} ms | "
        f"e2e p95 {c['e2e_latency_ms']['p95']} ms | gửi {p['sent']:,} nhận {unique:,} "
        f"mất {lost_after_ack} trùng {dup} lỗi {p['failed']} | nén {p['compression_ratio']}x"
        f" | CPU {summary['host_cpu_pct']['avg_while_producing']}%"
        + (" | CONSUMER TIMEOUT" if c["timed_out"] else ""))
    return summary


# ------------------------------------------------------------------ main
def main():
    global LOG_FILE
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--experiments", default="all", help="all | pilot | fault | danh sách E1_partitions,E2_producers,...")
    ap.add_argument("--reps", type=int, default=3, help="số lần lặp mỗi cấu hình")
    ap.add_argument("--duration", type=float, default=30, help="số giây phát tải mỗi lần")
    ap.add_argument("--latency-rate", type=int, default=20000, help="tải cố định cho E7 (event/s)")
    ap.add_argument("--load-rates", default="5000,10000,20000,50000,100000,150000,200000,250000,300000,350000,400000,0",
                    help="các mức tải cho E8 (event/s, 0 = tối đa)")
    ap.add_argument("--disk-path", default=Path(__file__).resolve().anchor or "/",
                    help="ổ đĩa chứa dữ liệu Docker để kiểm tra dung lượng trống")
    ap.add_argument("--min-free-gb", type=float, default=8.0)
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args()

    exps = experiments(args.latency_rate, [int(x) for x in args.load_rates.split(",")])
    if args.list:
        for name, (desc, runs) in exps.items():
            print(f"{name}: {desc}")
            for lbl, ov in runs:
                print(f"    {lbl:<12} {config_id({**BASE, **ov})}")
        print("fault: thí nghiệm chịu lỗi (docker kill leader) – " + ", ".join(l for l, _ in FAULT_RUNS))
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "logs").mkdir(exist_ok=True)
    LOG_FILE = OUT_DIR / "logs" / f"run_{datetime.now():%Y%m%d_%H%M%S}_{args.experiments.replace(',', '+')}.log"

    adm = admin()
    try:
        st = cluster_state(adm, list(TOPIC_PARTITIONS))
    except Exception as e:
        raise SystemExit(f"[LỖI] Không kết nối được Kafka tại {DEFAULT_BOOTSTRAP}: {e}\n"
                         "Hãy bật Docker Desktop rồi chạy: cd scripts; docker compose up -d; .\\create_topics.ps1")
    log(f"Cụm Kafka: broker {st['brokers']} | bootstrap {DEFAULT_BOOTSTRAP}")
    log(f"Lặp {args.reps} lần | {args.duration}s mỗi lần | log: {LOG_FILE.relative_to(ROOT)}")

    try:
        _run(args, exps, adm)
    finally:  # trả lại 3 topic benchmark như TV1 đã tạo
        for t in TOPIC_PARTITIONS:
            try:
                reset_topic(adm, t)
            except Exception as e:
                log(f"[!] Không tạo lại được {t}: {e}")


def _run(args, exps: dict, adm):
    if args.experiments == "fault":
        for rep in range(1, args.reps + 1):
            for label, ov in FAULT_RUNS:
                run_once({**BASE, **ov}, rep, ["E9_fault"], args, adm,
                         fault={"label": label, "duration": 90, "kill_at": 30, "restart_at": 60})
        return

    if args.experiments == "pilot":
        selected = {"pilot": ("Chạy thử", [("p6", {}), ("p1", {"topic": "bakery.bench.p1"})])}
    elif args.experiments == "all":
        selected = exps
    else:
        selected = {k: exps[k] for k in args.experiments.split(",")}

    # Gộp các cấu hình trùng nhau giữa các thí nghiệm để không chạy lặp lại
    plan: dict[str, dict] = {}
    for name, (_, runs) in selected.items():
        for lbl, ov in runs:
            cfg = {**BASE, **ov}
            plan.setdefault(config_id(cfg), {"cfg": cfg, "exps": []})["exps"].append(f"{name}:{lbl}")
    log(f"{len(plan)} cấu hình khác nhau × {args.reps} lần = {len(plan) * args.reps} lần chạy")

    t0 = time.time()
    if args.experiments != "pilot":  # làm nóng JVM của broker, không tính vào kết quả
        run_once(dict(BASE), 0, ["warmup"], args, adm, duration=15, prefix="warmup_")
    for rep in range(1, args.reps + 1):  # xen kẽ các lần lặp để giảm sai lệch theo thời gian
        for i, (cid, item) in enumerate(plan.items(), 1):
            log(f"[{rep}/{args.reps}] [{i}/{len(plan)}]")
            run_once(item["cfg"], rep, item["exps"], args, adm)
    log(f"HOÀN TẤT sau {(time.time() - t0) / 60:.1f} phút")


if __name__ == "__main__":
    mp.freeze_support()
    main()
