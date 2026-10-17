"""Vẽ biểu đồ từ file CSV của verify_consumer.py (dùng cho báo cáo).

  python plot_results.py results/consumer_run_1760000000.csv
  python plot_results.py --compare results/sim_*.json     # so sánh throughput giữa các thí nghiệm
"""
import argparse
import csv
import glob
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt

# Console Windows mặc định cp1252 -> in tiếng Việt bị lỗi
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def plot_timeseries(csv_path: str):
    rows = list(csv.DictReader(open(csv_path, encoding="utf-8")))
    sec = [int(r["second"]) for r in rows]
    rec = [int(r["records"]) for r in rows]
    p95 = [float(r["latency_p95_ms"]) if r["latency_p95_ms"] else None for r in rows]
    lag = [(int(r["second"]), int(r["lag"])) for r in rows if r["lag"]]

    fig, axes = plt.subplots(3, 1, figsize=(10, 9), sharex=True)
    axes[0].plot(sec, rec)
    axes[0].set_ylabel("bản ghi / giây")
    axes[0].set_title("Throughput tiêu thụ")
    axes[1].plot(sec, p95, color="tab:orange")
    axes[1].set_ylabel("ms")
    axes[1].set_title("Độ trễ end-to-end p95")
    if lag:
        axes[2].plot(*zip(*lag), color="tab:red", marker=".")
    axes[2].set_ylabel("bản ghi")
    axes[2].set_title("Consumer lag")
    axes[2].set_xlabel("giây")
    fig.tight_layout()
    out = Path(csv_path).with_suffix(".png")
    fig.savefig(out, dpi=150)
    print("Đã lưu", out)


def plot_compare(patterns: list[str]):
    files = sorted({f for p in patterns for f in glob.glob(p)})
    runs = [json.load(open(f, encoding="utf-8")) for f in files]
    labels = [r.get("tag") or Path(f).stem for r, f in zip(runs, files)]
    eps = [r["throughput_eps"] for r in runs]
    p95 = [r["ack_latency_ms"]["p95"] or 0 for r in runs]

    fig, ax1 = plt.subplots(figsize=(max(6, len(runs) * 1.2), 5))
    ax1.bar(labels, eps, color="tab:blue")
    ax1.set_ylabel("event / giây")
    ax2 = ax1.twinx()
    ax2.plot(labels, p95, color="tab:orange", marker="o")
    ax2.set_ylabel("ack latency p95 (ms)")
    ax1.set_title("So sánh thí nghiệm")
    plt.setp(ax1.get_xticklabels(), rotation=30, ha="right")
    fig.tight_layout()
    out = Path("results") / "compare.png"
    fig.savefig(out, dpi=150)
    print("Đã lưu", out)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", nargs="?")
    ap.add_argument("--compare", nargs="+")
    a = ap.parse_args()
    if a.compare:
        plot_compare(a.compare)
    elif a.csv:
        plot_timeseries(a.csv)
    else:
        ap.print_help()
