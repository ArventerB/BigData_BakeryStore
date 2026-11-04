"""
Vẽ biểu đồ benchmark TV4 từ results/tv4/summary.json (+ CSV từng giây) → results/tv4/figures/*.png

Quy ước trình bày:
  - Không dùng biểu đồ 2 trục y: hai đại lượng khác đơn vị → hai khung riêng (small multiples).
  - Một chuỗi số liệu → một màu (xanh); nhiều chuỗi → màu theo thứ tự cố định + chú giải.
  - p50 / p95 / p99 là đại lượng có thứ tự → dải xanh nhạt → đậm.
  - Thanh sai số = ±1 độ lệch chuẩn qua các lần lặp. Bảng số chính xác nằm trong summary_tables.md.

Chạy: python benchmark/plot_results.py   (sau summarize.py)
"""
import csv
import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter  # noqa: E402

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "results" / "tv4"
FIG = OUT / "figures"

SURFACE, INK, INK2, MUTED = "#fcfcfb", "#0b0b0b", "#52514e", "#898781"
GRID, AXIS = "#e1e0d9", "#c3c2b7"
S1, S2, S3 = "#2a78d6", "#eb6834", "#1baf7a"           # màu nhóm theo thứ tự cố định
ORD = {"p50": "#86b6ef", "p95": "#2a78d6", "p99": "#104281"}  # dải thứ tự 1 sắc độ

plt.rcParams.update({
    "font.family": ["Segoe UI", "DejaVu Sans"], "font.size": 9.5, "axes.unicode_minus": False,
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": AXIS, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "legend.frameon": False, "legend.fontsize": 9,
})


def k_fmt(v, _=None):
    if v >= 1_000_000:
        return f"{v / 1e6:.1f}M".replace(".", ",")
    if v >= 1000:
        return f"{v / 1000:.0f}k"
    return f"{v:g}".replace(".", ",")


def style(ax, title=None, ylabel=None, xlabel=None, log=False, kfmt=False):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_linewidth(0.8)
    ax.grid(axis="y", color=GRID, linewidth=0.6, linestyle="-")
    ax.set_axisbelow(True)
    ax.tick_params(length=0, labelsize=9)
    if log:
        ax.set_yscale("log")
        ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1.0, 2.0, 5.0)))
        ax.yaxis.set_minor_formatter(NullFormatter())
    if kfmt or log:
        ax.yaxis.set_major_formatter(FuncFormatter(k_fmt))
    if title:
        ax.set_title(title, loc="left", fontsize=10.5, color=INK, fontweight="semibold", pad=8)
    if ylabel:
        ax.set_ylabel(ylabel)
    if xlabel:
        ax.set_xlabel(xlabel)


def label_ends(ax, x, values, names, log=False):
    """Nhãn trực tiếp ở cuối mỗi đường, đẩy giãn ra nếu quá sát nhau."""
    import math
    tr = (lambda v: math.log10(v)) if log else (lambda v: v)
    inv = (lambda v: 10 ** v) if log else (lambda v: v)
    lo, hi = ax.get_ylim()
    gap = (tr(hi) - tr(lo)) * 0.06
    pts = sorted(((tr(v), n) for v, n in zip(values, names) if v), key=lambda t: t[0])
    placed = []
    for y, n in pts:
        if placed and y - placed[-1][0] < gap:
            y = placed[-1][0] + gap
        placed.append((y, n))
    for y, n in placed:
        ax.annotate(n, (x, inv(y)), xytext=(8, 0), textcoords="offset points", va="center", fontsize=9,
                    color=INK2, annotation_clip=False)


def mean_std(rows, key):
    """Trung vị và sai số bất đối xứng tới min / max (bền với 1 lần chạy bất thường)."""
    med = [r[key]["median"] or 0 for r in rows]
    lo = [max(0.0, (r[key]["median"] or 0) - (r[key]["min"] or 0)) for r in rows]
    hi = [max(0.0, (r[key]["max"] or 0) - (r[key]["median"] or 0)) for r in rows]
    return med, [lo, hi]


def bars(ax, labels, means, stds, color=S1, offset=0.0, width=0.6, label=None):
    x = [i + offset for i in range(len(labels))]
    ax.bar(x, means, width=width, color=color, edgecolor=SURFACE, linewidth=2, label=label, zorder=2)
    ax.errorbar(x, means, yerr=stds, fmt="none", ecolor=INK2, elinewidth=1, capsize=3, zorder=3)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels)


def save(fig, name, title, note=None):
    fig.suptitle(title, x=0.01, ha="left", fontsize=12.5, color=INK, fontweight="bold")
    if note:
        fig.text(0.01, 0.005, note, fontsize=8, color=MUTED, ha="left", va="bottom")
    fig.tight_layout(rect=(0, 0.035 if note else 0, 1, 0.95))
    FIG.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG / name, dpi=180)
    plt.close(fig)
    print(f"[V] {FIG / name}")


NOTE = "Cột / điểm = trung vị 3 lần lặp, thanh sai số = khoảng min–max. Cụm Kafka 3 broker KRaft trên Docker, 1 máy (Ryzen 7 260, 16 luồng)."


def throughput_and_latency(exp, name, title, extra=None, lat_key="ack_p95_ms", lat_title="Độ trễ ack p95 (ms)",
                           log_lat=False):
    rows = exp["rows"]
    labels = [r["label"] for r in rows]
    n = 2 + (extra is not None)
    fig, axes = plt.subplots(1, n, figsize=(4.2 * n, 3.8))
    m, s = mean_std(rows, "throughput_eps")
    bars(axes[0], labels, m, s)
    style(axes[0], "Throughput producer (event/s)", kfmt=True)
    m, s = mean_std(rows, lat_key)
    bars(axes[1], labels, m, s)
    style(axes[1], lat_title, log=log_lat)
    if extra:
        key, ttl, digits = extra
        m, s = mean_std(rows, key)
        bars(axes[2], labels, m, s)
        style(axes[2], ttl)
    save(fig, name, title, NOTE)


def fig_partitions(exp, solo=None):
    rows = exp["rows"]
    labels = [f"{r['label']}" for r in rows]
    fig, axes = plt.subplots(1, 2, figsize=(10.4, 4.3))
    w = 0.27 if solo else 0.36
    off = (-w, 0, w) if solo else (-w / 2, w / 2)
    m, s = mean_std(rows, "throughput_eps")
    bars(axes[0], labels, m, s, color=S1, offset=off[0], width=w, label="Producer (có consumer chạy cùng)")
    m, s = mean_std(rows, "consume_eps")
    bars(axes[0], labels, m, s, color=S2, offset=off[1], width=w, label="Consumer đọc")
    if solo:
        srows = {r["label"]: r for r in solo["rows"]}
        m, s = mean_std([srows[l] for l in labels], "throughput_eps")
        bars(axes[0], labels, m, s, color=S3, offset=off[2], width=w, label="Producer (không có consumer)")
    axes[0].legend(loc="upper left", bbox_to_anchor=(0, -0.08), ncol=2)
    style(axes[0], "Throughput (event/s)", kfmt=True)
    m, s = mean_std(rows, "e2e_p95_ms")
    bars(axes[1], labels, m, s)
    style(axes[1], "Độ trễ end-to-end p95 (ms, thang log)", log=True)
    save(fig, "e1_partitions.png", "E1 – Số partition: 1 / 3 / 6 (4 producer, consumer group 3 thành viên)", NOTE)


def fig_latency_lines(exp, name, title, xlabel, x_key, log=False):
    rows = exp["rows"]
    xs = [r["config"][x_key] for r in rows]
    fig, ax = plt.subplots(figsize=(7.6, 4.0))
    ends = []
    for p in ("p50", "p95", "p99"):
        m, s = mean_std(rows, f"e2e_{p}_ms")
        ax.errorbar(xs, m, yerr=s, color=ORD[p], linewidth=2, marker="o", markersize=6,
                    markeredgecolor=SURFACE, markeredgewidth=1.5, capsize=3, elinewidth=1, label=f"e2e {p}")
        ends.append(m[-1])
    ax.set_xticks(xs)
    ax.set_xticklabels([str(x) for x in xs])
    style(ax, "Độ trễ end-to-end (ms" + (", thang log)" if log else ")"), xlabel=xlabel, log=log)
    ax.set_ylim(bottom=0)
    label_ends(ax, xs[-1], ends, ["p50", "p95", "p99"], log)
    ax.legend(loc="upper left")
    save(fig, name, title, NOTE)


def fig_load_curve(exp):
    maxrow = next((r for r in exp["rows"] if not r["config"]["rate"]), None)
    rows = [r for r in exp["rows"] if r["config"]["rate"]]
    target = [r["config"]["rate"] for r in rows]
    labels = [r["label"].replace(" eps", "") for r in rows]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    ax = axes[0]
    m, s = mean_std(rows, "throughput_eps")
    ax.plot(target, target, color=MUTED, linewidth=1, label="Tải mục tiêu (y = x)")
    ax.errorbar(target, m, yerr=s, color=S1, linewidth=2, marker="o", markersize=6,
                markeredgecolor=SURFACE, markeredgewidth=1.5, capsize=3, label="Thực tế broker xác nhận")
    if maxrow:
        mx = maxrow["throughput_eps"]["median"]
        ax.axhline(mx, color=S2, linewidth=1.2, label=f"Chạy hết tốc độ (vòng kín): {k_fmt(mx)}")
    ax.set_xscale("log")
    style(ax, "Throughput đạt được (event/s, thang log)", xlabel="Tải mục tiêu (event/s, thang log)", log=True)
    ax.legend(loc="lower right")
    ax = axes[1]
    ends = []
    for p in ("p50", "p95", "p99"):
        m, s = mean_std(rows, f"e2e_{p}_ms")
        ax.errorbar(target, m, yerr=s, color=ORD[p], linewidth=2, marker="o", markersize=6,
                    markeredgecolor=SURFACE, markeredgewidth=1.5, capsize=3, elinewidth=1, label=f"e2e {p}")
        ends.append(m[-1])
    ax.set_xscale("log")
    style(ax, "Độ trễ end-to-end (ms, thang log)", xlabel="Tải mục tiêu (event/s, thang log)", log=True)
    label_ends(ax, target[-1], ends, ["p50", "p95", "p99"], log=True)
    ax.legend(loc="best")
    show = {5000, 10000, 20000, 50000, 100000, 200000, 400000}  # tránh nhãn chồng nhau trên trục log
    for a in axes:
        a.set_xticks(target)
        a.set_xticklabels([l if t in show else "" for t, l in zip(target, labels)])
        a.xaxis.set_minor_formatter(NullFormatter())
    save(fig, "e8_load_curve.png",
         "E8 – Độ trễ theo tải: cấu hình đơn hàng của TV1 (acks=all, 64KB, 10ms, lz4), topic 3 partition", NOTE)


def read_csv(rel):
    with open(ROOT / rel, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def num(v):
    return float(v) if v not in ("", None) else None


def fig_e1_timeseries(exp):
    rows = {r["label"]: r for r in exp["rows"]}
    picks = [l for l in ("p1", "p6") if l in rows]
    if not picks:
        return
    raw = OUT / "raw"
    fig, axes = plt.subplots(2, len(picks), figsize=(5.4 * len(picks), 5.6), sharex=True, sharey="row", squeeze=False)
    for j, lbl in enumerate(picks):
        data = read_csv((raw / f"{rows[lbl]['config_id']}_r1.csv").relative_to(ROOT))
        sec = [int(r["second"]) for r in data]
        axes[0][j].plot(sec, [int(r["produced"]) for r in data], color=S1, linewidth=1.6, label="Producer gửi")
        axes[0][j].plot(sec, [int(r["consumed"]) for r in data], color=S2, linewidth=1.6, label="Consumer đọc")
        style(axes[0][j], f"{lbl}: event mỗi giây", kfmt=True)
        axes[1][j].plot(sec, [int(r["lag"] or 0) for r in data], color=S1, linewidth=1.6)
        style(axes[1][j], f"{lbl}: consumer lag (bản ghi tồn đọng)", xlabel="Giây kể từ lúc bắt đầu", kfmt=True)
    axes[0][0].legend(loc="upper right")
    dur = int(rows[picks[0]]["config"]["duration"])
    save(fig, "e1_timeseries_p1_vs_p6.png", "E1 – Diễn biến theo thời gian (lần lặp 1): 1 partition vs 6 partition",
         f"Producer phát {dur} giây; sau đó consumer tiếp tục đọc phần tồn đọng tới khi lag = 0.")


def fig_fault(fault_rows):
    reps = {}
    for f in fault_rows:
        reps.setdefault(f["label"], f)  # lần lặp đầu tiên của mỗi cấu hình
    if not reps:
        return
    labels = list(reps)
    fig, axes = plt.subplots(3, len(labels), figsize=(5.6 * len(labels), 7.6), sharex=True, sharey="row",
                             squeeze=False)
    for j, lbl in enumerate(labels):
        f = reps[lbl]
        data = read_csv(f["per_second_csv"])
        sec = [int(r["second"]) for r in data]
        axes[0][j].plot(sec, [int(r["consumed"]) for r in data], color=S1, linewidth=1.6)
        style(axes[0][j], f"{lbl}: consumer đọc (event/giây)", kfmt=True)
        # giây không nhận được bản ghi nào → NaN để đường bị ngắt, không nối thẳng qua khoảng trống
        p95 = [num(r["e2e_p95_ms"]) if int(r["consumed"] or 0) else float("nan") for r in data]
        axes[1][j].plot(sec, p95, color=S1, linewidth=1.6)
        style(axes[1][j], f"{lbl}: độ trễ e2e p95 mỗi giây (ms, log; ngắt = không nhận được gì)", log=True)
        axes[2][j].plot(sec, [int(r["lag"] or 0) for r in data], color=S1, linewidth=1.6)
        style(axes[2][j], f"{lbl}: consumer lag (theo high watermark)", xlabel="Giây kể từ lúc bắt đầu", kfmt=True)
        for i in range(3):
            for t, txt in ((30, f"tắt kafka-{f['killed_broker']}"), (60, "bật lại")):
                axes[i][j].axvline(t, color=INK2, linewidth=1)
                if i == 0:
                    axes[i][j].annotate(txt, (t, 1), xycoords=("data", "axes fraction"), xytext=(4, -4),
                                        textcoords="offset points", va="top", fontsize=8.5, color=INK2)
    save(fig, "e9_fault_tolerance.png",
         "E9 – Chịu lỗi: docker kill broker leader ở giây 30, bật lại ở giây 60 (tải 5.000 event/s, topic 3 partition)",
         "Lần lặp 1 của mỗi cấu hình. Số mất / trùng của mọi lần lặp nằm trong bảng E9.")


def fig_soak(soak):
    data = read_csv(soak["per_second_csv"])
    sim = read_csv(soak["simulator_csv"]) if soak.get("simulator_csv") and (ROOT / soak["simulator_csv"]).exists() else []
    minutes = [int(r["second"]) / 60 for r in data]
    fig, axes = plt.subplots(3, 1, figsize=(10.5, 8.2), sharex=True)
    # gộp consumer theo cửa sổ 10 giây cho dễ đọc
    win = 10
    buckets = {}
    for r in data:
        b = int(r["second"]) // win
        buckets[b] = buckets.get(b, 0) + int(r["consumed"])
    axes[0].plot([(b * win + win / 2) / 60 for b in sorted(buckets)], [buckets[b] / win for b in sorted(buckets)],
                 color=S1, linewidth=1.6, label="Consumer đọc (trung bình 10 giây)")
    if sim:
        axes[0].plot([float(r["elapsed_sec"]) / 60 for r in sim], [float(r["target_eps"]) for r in sim],
                     color=MUTED, linewidth=1, label="Tải mục tiêu của simulator")
    axes[0].legend(loc="upper left")
    style(axes[0], "Lưu lượng (event/giây)", kfmt=True)
    for p in ("p50", "p95"):
        pts = [(m, num(r[f"e2e_{p}_ms"])) for m, r in zip(minutes, data) if num(r[f"e2e_{p}_ms"])]
        axes[1].plot([a for a, _ in pts], [b for _, b in pts], color=ORD[p], linewidth=1, label=f"e2e {p}")
    axes[1].legend(loc="upper left")
    style(axes[1], "Độ trễ end-to-end mỗi giây (ms, thang log)", log=True)
    axes[2].plot(minutes, [int(r["lag"] or 0) for r in data], color=S1, linewidth=1.2)
    style(axes[2], "Consumer lag (bản ghi tồn đọng)", xlabel="Phút", kfmt=True)
    save(fig, "soak_timeseries.png",
         f"Chạy liên tục {soak['duration_sec'] // 60} phút – simulator TV3 (1 ngày ảo, cao điểm sáng/trưa/tối) → Kafka → consumer kiểm chứng")


def main():
    s = json.loads((OUT / "summary.json").read_text(encoding="utf-8"))
    E = s["experiments"]
    if "E1_partitions" in E:
        fig_partitions(E["E1_partitions"], E.get("E1b_partitions_producer_only"))
        fig_e1_timeseries(E["E1_partitions"])
    if "E2_producers" in E:
        throughput_and_latency(E["E2_producers"], "e2_producers.png",
                               "E2 – Số producer song song (topic 6 partition, acks=1, 64KB, 10ms, lz4)",
                               extra=("host_cpu_pct", "CPU toàn máy khi đang phát (%)", 0),
                               lat_title="Độ trễ ack p95 (ms, thang log)", log_lat=True)
    if "E3_batch" in E:
        throughput_and_latency(E["E3_batch"], "e3_batch_size.png", "E3 – batch.size (linger 10ms, lz4, acks=1)",
                               extra=("wire_bytes_per_msg", "Byte thực gửi qua mạng / bản ghi", 1))
    if "E4_linger" in E:
        throughput_and_latency(E["E4_linger"], "e4_linger_max.png",
                               "E4 – linger.ms ở tốc độ tối đa (batch 128KB, lz4, acks=1)",
                               extra=("wire_bytes_per_msg", "Byte thực gửi qua mạng / bản ghi", 1))
    if "E5_compression" in E:
        throughput_and_latency(E["E5_compression"], "e5_compression.png",
                               "E5 – Thuật toán nén (cấu hình clickstream của TV1: acks=1, 128KB, 20ms)",
                               extra=("wire_bytes_per_msg", "Byte thực gửi qua mạng / bản ghi", 1))
    if "E6_acks" in E:
        throughput_and_latency(E["E6_acks"], "e6_acks.png", "E6 – Mức xác nhận acks (64KB, 10ms, lz4)",
                               extra=("e2e_p95_ms", "Độ trễ end-to-end p95 (ms)", 1))
    if "E7_latency_linger" in E:
        rate = f"{E['E7_latency_linger']['rows'][0]['config']['rate']:,}".replace(",", ".")
        fig_latency_lines(E["E7_latency_linger"], "e7_latency_vs_linger.png",
                          f"E7 – Độ trễ theo linger.ms ở tải cố định {rate} event/s (128KB, lz4, acks=1)",
                          "linger.ms (ms)", x_key="linger_ms")
    if "E8_load_curve" in E:
        fig_load_curve(E["E8_load_curve"])
    if s.get("fault"):
        fig_fault(s["fault"])
    if s.get("soak"):
        fig_soak(s["soak"])


if __name__ == "__main__":
    main()
