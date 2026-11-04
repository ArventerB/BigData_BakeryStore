"""
Chạy liên tục (soak test) 30–60 phút – phối hợp TV3 (simulator) + TV4 (consumer kiểm chứng).

- Simulator của TV3 ghi thẳng vào 3 topic nghiệp vụ (bakery.user-events / orders / payments) bằng đúng 2 bộ cấu hình
  producer của TV1, lưu lượng theo profile "daily" (1 ngày ảo nén vào --duration giây: sáng/trưa/tối cao điểm,
  đêm gần như không có khách).
- Chèn có chủ đích một tỉ lệ nhỏ bản ghi trùng event_id (--dup-rate) để KIỂM CHỨNG consumer: số trùng phát hiện
  phải bằng số trùng simulator đã chèn.
- Consumer kiểm chứng (bench_consumer.py, mode event_id) đọc từ offset cuối hiện tại của 3 topic: đo throughput,
  độ trễ end-to-end, lag từng giây; đếm mất / trùng theo event_id.

Ví dụ:
  python benchmark/soak_test.py --duration 1800 --rate 2000
Kết quả: results/tv4/soak/soak_<tag>.json, soak_<tag>.csv (+ file của simulator).
"""
import argparse
import csv
import json
import multiprocessing as mp
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from bench_consumer import ConsumerGroup
from bench_utils import DEFAULT_BOOTSTRAP, latency_summary
from run_benchmarks import start_offsets

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "results" / "tv4" / "soak"
TOPICS = ["bakery.user-events", "bakery.orders", "bakery.payments"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--duration", type=int, default=1800)
    ap.add_argument("--rate", type=int, default=2000, help="event/s trung bình (profile daily nhân hệ số theo giờ)")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--dup-rate", type=float, default=0.005, help="tỉ lệ bản trùng chèn có chủ đích")
    ap.add_argument("--tag", default=None)
    args = ap.parse_args()
    tag = args.tag or f"soak{args.duration // 60}m"
    OUT.mkdir(parents=True, exist_ok=True)

    start_at = mp.Value("d", 0.0)
    assigns = start_offsets(TOPICS)
    consumers = ConsumerGroup({"bootstrap": DEFAULT_BOOTSTRAP, "mode": "event_id", "warmup_sec": 5.0,
                               "idle_timeout": 60.0, "max_drain_sec": 600.0, "run_id": int(time.time())},
                              assigns, start_at, n_proc=1)
    consumers.start()

    cmd = [sys.executable, str(ROOT / "simulator" / "simulator.py"), "--mode", "kafka",
           "--rate", str(args.rate), "--duration", str(args.duration), "--workers", str(args.workers),
           "--profile", "daily", "--day-seconds", str(args.duration), "--seed", "2026",
           "--dirty-kinds", "duplicate", "--dirty-rate", str(args.dup_rate),
           "--stats-csv", "--report-every", "10", "--tag", f"tv4-{tag}"]
    print("[Soak] Simulator TV3:", " ".join(cmd[1:]), flush=True)
    start_at.value = time.time()
    t_start = start_at.value
    sim = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace")
    (OUT / f"simulator_{tag}.log").write_text(sim.stdout + sim.stderr, encoding="utf-8")
    if sim.returncode != 0:
        raise SystemExit(f"Simulator lỗi (mã {sim.returncode}), xem {OUT / f'simulator_{tag}.log'}")

    # Lấy file kết quả mới nhất của simulator rồi chuyển vào thư mục soak
    sim_json = max((ROOT / "results").glob(f"sim_kafka_tv4-{tag}_*.json"), key=lambda p: p.stat().st_mtime)
    sim_csv = sim_json.with_suffix(".csv")
    sim_res = json.loads(sim_json.read_text(encoding="utf-8"))
    for p in (sim_json, sim_csv):
        if p.exists():
            shutil.move(str(p), OUT / p.name)

    cdone = consumers.finish(timeout=900)
    d = cdone[0]
    sent = sim_res["sent_total"]
    send_errors = sim_res.get("send_errors", 0)
    dup_injected = sim_res.get("dirty_by_kind", {}).get("duplicate", 0)
    lost = sent - send_errors - d["received"]

    csv_path = OUT / f"soak_{tag}.csv"
    lag = {int(k): v for k, v in d["lag"].items()}
    rows = {int(k): v for k, v in d["sec_rows"].items() if int(k) >= 0}
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["second", "consumed", "e2e_p50_ms", "e2e_p95_ms", "e2e_p99_ms", "lag"])
        last = 0
        for s in range(0, max(rows) + 1 if rows else 0):
            n, p50, p95, p99 = rows.get(s, [0, None, None, None])
            last = lag.get(s, last)
            w.writerow([s, n, p50 if p50 is not None else "", p95 if p95 is not None else "",
                        p99 if p99 is not None else "", last])

    summary = {
        "tag": tag,
        "started_at": datetime.fromtimestamp(t_start, tz=timezone.utc).isoformat(),
        "duration_sec": args.duration, "rate_avg_target": args.rate, "profile": "daily",
        "topics": TOPICS,
        "simulator": {k: sim_res.get(k) for k in ("sent_total", "throughput_eps", "send_errors", "dirty_by_kind",
                                                     "by_event_type", "ack_latency_ms")},
        "consumer": {
            "received": d["received"], "unique_event_ids": d["unique"], "duplicates_detected": d["duplicates"],
            "duplicates_injected": dup_injected, "records_by_topic": d["by_topic"],
            "lost": lost, "timed_out": d["timed_out"],
            "max_lag": max(lag.values(), default=0),
            "e2e_latency_ms": latency_summary([(d["lat_sample"], d["lat_n"])]),
        },
        "per_second_csv": str(csv_path.relative_to(ROOT)),
        "simulator_csv": str((OUT / sim_csv.name).relative_to(ROOT)),
    }
    (OUT / f"soak_{tag}.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    c = summary["consumer"]
    print(f"[Soak] gửi {sent:,} | nhận {c['received']:,} | mất {lost} | trùng phát hiện {c['duplicates_detected']} "
          f"/ đã chèn {dup_injected} | e2e p95 {c['e2e_latency_ms']['p95']} ms | lag max {c['max_lag']}", flush=True)


if __name__ == "__main__":
    mp.freeze_support()
    main()
