"""
Cong cu ve do thi truc quan hoa ket qua Benchmark cho TV4 (Bakery Store).

Chuc nang:
  1. Ve do thi chuoi thoi gian (Time-series 3 tang): Throughput, p95 Latency, Consumer Lag
  2. Ve bieu do so sanh doi chieu giua cac thi nghiem (Throughput EPS & p95 Latency)
  3. Ho tro xuat anh chat luong cao (DPI 200) phuc vu cheo bao cao cua TV5

Cach su dung:
  python plot_results.py results/consumer_run_123456789.csv
  python plot_results.py --compare results/sim_kafka_*.json --out compare_all.png
"""
import argparse
import csv
import glob
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt

# Console Windows UTF-8
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Style chung cho cac bieu do do an
plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Arial", "Segoe UI"]
plt.rcParams["axes.unicode_minus"] = False


def plot_timeseries(csv_path: str, output_path: str | None = None) -> Path:
    p = Path(csv_path)
    if not p.exists():
        raise FileNotFoundError(f"Khong tim thay file CSV: {csv_path}")

    with open(p, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print(f"[!] File CSV rong: {csv_path}")
        return p

    sec = [int(r["second"]) for r in rows]
    rec = [int(r["records"]) for r in rows]
    p95 = [float(r["latency_p95_ms"]) if r.get("latency_p95_ms") else None for r in rows]
    lag = [(int(r["second"]), int(r["lag"])) for r in rows if r.get("lag")]

    fig, axes = plt.subplots(3, 1, figsize=(11, 8.5), sharex=True)

    # 1. Throughput
    axes[0].plot(sec, rec, color="#1f77b4", linewidth=2, label="Records / giây")
    axes[0].set_ylabel("Throughput (eps)", fontsize=11, fontweight="bold")
    axes[0].set_title(f"Hiệu năng Tiêu thụ Thời gian thực: {p.stem}", fontsize=13, fontweight="bold", pad=10)
    axes[0].grid(True, linestyle="--", alpha=0.6)
    axes[0].legend(loc="upper right")

    # 2. End-to-End Latency p95
    p95_clean_sec = [s for s, v in zip(sec, p95) if v is not None]
    p95_clean_val = [v for v in p95 if v is not None]
    if p95_clean_val:
        axes[1].plot(p95_clean_sec, p95_clean_val, color="#ff7f0e", linewidth=2, label="p95 End-to-End Latency")
        axes[1].set_ylabel("Độ trễ p95 (ms)", fontsize=11, fontweight="bold")
        axes[1].grid(True, linestyle="--", alpha=0.6)
        axes[1].legend(loc="upper right")

    # 3. Consumer Lag
    if lag:
        lag_sec, lag_val = zip(*lag)
        axes[2].plot(lag_sec, lag_val, color="#d62728", marker="o", markersize=4, linestyle="-", label="Consumer Lag")
        axes[2].set_ylabel("Lag (bản ghi)", fontsize=11, fontweight="bold")
        axes[2].grid(True, linestyle="--", alpha=0.6)
        axes[2].legend(loc="upper right")
    else:
        axes[2].text(0.5, 0.5, "Không có tồn đọng (Lag = 0)", horizontalalignment="center", verticalalignment="center", transform=axes[2].transAxes, fontsize=12, color="green")
        axes[2].set_ylabel("Lag", fontsize=11)

    axes[2].set_xlabel("Thời gian đo (giây)", fontsize=11, fontweight="bold")
    fig.tight_layout()

    out = Path(output_path) if output_path else p.with_suffix(".png")
    fig.savefig(out, dpi=200)
    plt.close(fig)
    print(f"[V] Đã lưu đồ thị chuỗi thời gian: {out}")
    return out


def plot_compare(
    files: list[str],
    output_name: str = "compare.png",
    title: str = "So sánh Hiệu năng giữa các Thí nghiệm"
) -> Path:
    matched = []
    for pat in files:
        matched.extend(glob.glob(pat))
    matched = sorted(set(matched))

    if not matched:
        print("[!] Không tìm thấy file JSON nào khớp với mẫu.")
        return Path("results") / output_name

    runs = []
    for f in matched:
        try:
            with open(f, encoding="utf-8") as fp:
                runs.append((f, json.load(fp)))
        except Exception as e:
            print(f"[!] Lỗi đọc file {f}: {e}")

    labels = []
    eps_vals = []
    p95_vals = []

    for fpath, r in runs:
        tag = r.get("tag") or Path(fpath).stem.replace("sim_kafka_", "").replace("consumer_", "")
        labels.append(tag)
        eps = r.get("throughput_eps") or r.get("avg_consume_eps") or 0
        eps_vals.append(eps)
        p95 = 0
        if "ack_latency_ms" in r and isinstance(r["ack_latency_ms"], dict):
            p95 = r["ack_latency_ms"].get("p95") or 0
        elif "e2e_latency_ms" in r and isinstance(r["e2e_latency_ms"], dict):
            p95 = r["e2e_latency_ms"].get("p95") or 0
        p95_vals.append(float(p95) if p95 else 0.0)

    fig, ax1 = plt.subplots(figsize=(max(7, len(labels) * 1.5), 5.5))

    # Cột Throughput
    x = range(len(labels))
    bars = ax1.bar(x, eps_vals, width=0.45, color="#2b5c8f", alpha=0.85, label="Throughput (event/s)")
    ax1.set_ylabel("Throughput (events / giây)", color="#2b5c8f", fontsize=11, fontweight="bold")
    ax1.tick_params(axis="y", labelcolor="#2b5c8f")
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, rotation=25, ha="right", fontsize=10, fontweight="bold")

    # In giá trị lên đầu mỗi cột
    for bar in bars:
        height = bar.get_height()
        ax1.annotate(f"{height:,.0f}",
                     xy=(bar.get_x() + bar.get_width() / 2, height),
                     xytext=(0, 3), textcoords="offset points",
                     ha="center", va="bottom", fontsize=9, fontweight="bold", color="#2b5c8f")

    # Đường Độ trễ p95
    ax2 = ax1.twinx()
    lines = ax2.plot(x, p95_vals, color="#e6550d", marker="o", linewidth=2.5, markersize=7, label="Độ trễ p95 (ms)")
    ax2.set_ylabel("Độ trễ p95 (ms)", color="#e6550d", fontsize=11, fontweight="bold")
    ax2.tick_params(axis="y", labelcolor="#e6550d")
    ax2.grid(False)

    for i, v in enumerate(p95_vals):
        if v > 0:
            ax2.annotate(f"{v:.1f}ms", (i, v), textcoords="offset points", xytext=(0, 7), ha="center", fontsize=9, fontweight="bold", color="#e6550d")

    plt.title(title, fontsize=13, fontweight="bold", pad=15)
    fig.tight_layout()

    out_dir = Path("results")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / output_name
    fig.savefig(out_file, dpi=200)
    plt.close(fig)
    print(f"[V] Đã lưu biểu đồ so sánh: {out_file}")
    return out_file


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("csv", nargs="?", help="Đường dẫn file CSV để vẽ timeseries")
    ap.add_argument("--compare", nargs="+", help="Danh sách hoặc mẫu file JSON để so sánh đối chiếu")
    ap.add_argument("--out", default="compare.png", help="Tên file ảnh xuất ra")
    ap.add_argument("--title", default="So sánh Hiệu năng Thí nghiệm Ingestion", help="Tiêu đề biểu đồ")
    args = ap.parse_args()

    if args.compare:
        plot_compare(args.compare, output_name=args.out, title=args.title)
    elif args.csv:
        plot_timeseries(args.csv, output_path=args.out if args.out != "compare.png" else None)
    else:
        ap.print_help()


if __name__ == "__main__":
    main()

