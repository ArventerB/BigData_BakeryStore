"""Vẽ các hình sơ đồ / biểu đồ còn trống trong báo cáo (Chương 1–4).

Chạy:  python docs/report_figures/ve_hinh.py
Kết quả: docs/report_figures/hinh_*.png (200 dpi, nền trắng, chèn thẳng vào Word).

- Hình 3.1, 3.2 đọc số liệu thật từ results/profile_dataset_oct2026.json.
- Các hình còn lại là sơ đồ minh họa, nội dung bám theo mô tả trong báo cáo và mã nguồn.
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent

plt.rcParams["font.family"] = ["Segoe UI", "Arial", "DejaVu Sans"]
plt.rcParams["font.size"] = 9

# Bảng màu (chữ dùng màu mực, màu sắc chỉ để phân biệt thành phần)
INK, INK2, MUTED = "#0b0b0b", "#52514e", "#8a8984"
LINE, GRID = "#b9b8b2", "#e6e5e0"
GRAY_FILL, PANEL = "#f2f1ed", "#f8f8f6"
BLUE, BLUE_FILL = "#2a78d6", "#e3eefb"
ORANGE, ORANGE_FILL = "#eb6834", "#fde9e0"
AQUA, AQUA_FILL = "#1baf7a", "#dff4ec"
RED, RED_FILL = "#e34948", "#fbe4e4"


# ---------------------------------------------------------------- tiện ích vẽ
def canvas(w, h):
    """1 đơn vị toạ độ = 1 inch, gốc ở góc dưới trái."""
    fig = plt.figure(figsize=(w, h))
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, w)
    ax.set_ylim(0, h)
    ax.set_aspect("equal")
    ax.axis("off")
    return fig, ax


def box(ax, x, y, w, h, text="", fc=GRAY_FILL, ec=LINE, lw=1.2, fs=9, bold=False,
        color=INK, ls="-", r=0.08, z=2, va="center", ty=None):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle=f"round,pad=0,rounding_size={r}",
                                fc=fc, ec=ec, lw=lw, ls=ls, zorder=z))
    if text:
        ax.text(x + w / 2, ty if ty is not None else y + h / 2, text, ha="center", va=va,
                fontsize=fs, color=color, fontweight="bold" if bold else "normal",
                zorder=z + 1, linespacing=1.25)


def titled(ax, x, y, w, h, title, sub="", fc=GRAY_FILL, ec=LINE, lw=1.2, fs=9.5, ls="-"):
    """Hộp có dòng tiêu đề đậm và dòng phụ màu nhạt."""
    box(ax, x, y, w, h, fc=fc, ec=ec, lw=lw, ls=ls)
    if sub:
        ax.text(x + w / 2, y + h * 0.66, title, ha="center", va="center", fontsize=fs,
                fontweight="bold", color=INK, zorder=4)
        ax.text(x + w / 2, y + h * 0.3, sub, ha="center", va="center", fontsize=fs - 1.5,
                color=INK2, zorder=4, linespacing=1.2)
    else:
        ax.text(x + w / 2, y + h / 2, title, ha="center", va="center", fontsize=fs,
                fontweight="bold", color=INK, zorder=4, linespacing=1.2)


def arrow(ax, p1, p2, color=INK2, lw=1.3, ls="-", style="-|>", rad=0.0, z=5, ms=11):
    ax.add_patch(FancyArrowPatch(p1, p2, arrowstyle=style, mutation_scale=ms, color=color,
                                 lw=lw, ls=ls, shrinkA=0, shrinkB=0, zorder=z,
                                 connectionstyle=f"arc3,rad={rad}"))


def polyline(ax, pts, color=INK2, lw=1.3, ls="-", z=5):
    """Đường gấp khúc, mũi tên ở đoạn cuối."""
    xs, ys = zip(*pts[:-1])
    ax.plot(xs, ys, color=color, lw=lw, ls=ls, zorder=z, solid_capstyle="butt")
    arrow(ax, pts[-2], pts[-1], color=color, lw=lw, ls=ls, z=z)


def text(ax, x, y, s, fs=8, color=INK2, ha="center", va="center", bold=False, z=6, **kw):
    ax.text(x, y, s, fontsize=fs, color=color, ha=ha, va=va, zorder=z,
            fontweight="bold" if bold else "normal", linespacing=1.25, **kw)


def log_cells(ax, x, y, n, w, h, fills, labels=None, fs=7, ec=LINE, empty_from=None):
    """Một dãy ô liên tiếp (nhật ký / partition). fills: list màu nền cho từng ô."""
    for i in range(n):
        dashed = empty_from is not None and i >= empty_from
        ax.add_patch(Rectangle((x + i * w, y), w, h, fc="white" if dashed else fills[i],
                               ec=MUTED if dashed else ec, lw=0.9, ls="--" if dashed else "-",
                               zorder=3))
        if labels is not None and labels[i] != "":
            ax.text(x + i * w + w / 2, y + h / 2, labels[i], ha="center", va="center",
                    fontsize=fs, color=MUTED if dashed else INK, zorder=4)


def save(fig, name):
    fig.savefig(OUT / name, dpi=200, bbox_inches="tight", pad_inches=0.08, facecolor="white")
    plt.close(fig)
    print("saved", OUT / name)


def vn(n, d=0):
    """Định dạng số kiểu Việt Nam: 1.234.567,8"""
    from decimal import Decimal, ROUND_HALF_UP  # làm tròn nửa lên: 99,25 → 99,3 (khớp báo cáo)
    n = Decimal(str(n)).quantize(Decimal(1).scaleb(-d), rounding=ROUND_HALF_UP)
    s = f"{n:,.{d}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


# ---------------------------------------------------------------- Hình 1.1
def hinh_1_1():
    fig, ax = canvas(13, 2.95)
    stages = [("Sources", "Web, app, backend\nđơn hàng, Simulator"),
              ("Ingestion", "Collector API\n→ Apache Kafka"),
              ("Storage", "Data Lake"),
              ("Processing", "Làm sạch,\nbiến đổi"),
              ("Analytics", "Phân tích,\nmô hình"),
              ("Presentation", "Dashboard,\nbáo cáo"),
              ("Decision / Action", "Ra quyết định,\nhành động")]
    w, h, gap, x0, y = 1.55, 1.1, 0.3, 0.2, 1.0
    for i, (name, sub) in enumerate(stages):
        x = x0 + i * (w + gap)
        ours = i < 2
        titled(ax, x, y, w, h, name, sub, fc=BLUE_FILL if ours else GRAY_FILL,
               ec=BLUE if ours else LINE, lw=2 if ours else 1.2)
        if i:
            arrow(ax, (x - gap + 0.03, y + h / 2), (x - 0.03, y + h / 2),
                  color=BLUE if i == 1 else INK2)
    # khung phạm vi của nhóm
    xr = x0 + 2 * w + gap
    box(ax, x0 - 0.12, y - 0.15, xr - x0 + 0.24, h + 0.3, fc="none", ec=BLUE, lw=1.5, ls="--", z=1)
    text(ax, (x0 + xr) / 2, y + h + 0.42, "Phạm vi của nhóm: Sources → Ingestion",
         fs=10, color=BLUE, bold=True)
    xs = x0 + 2 * (w + gap)
    xe = x0 + 6 * (w + gap) + w
    text(ax, (xs + xe) / 2, y + h + 0.42, "Các giai đoạn sau – do các nhóm khác đảm nhiệm",
         fs=9.5, color=MUTED)
    ax.plot([xs, xe], [y + h + 0.22] * 2, color=LINE, lw=1)
    # điểm bàn giao
    gx = xs - gap / 2
    ax.plot([gx, gx], [y - 0.22, y - 0.42], color=ORANGE, lw=1.3)
    text(ax, gx, y - 0.68, "Điểm bàn giao: các topic Kafka bakery.*\n(user-events, orders, payments, dlq)",
         fs=8.5, color=INK)
    ax.plot(gx, y - 0.2, marker="^", ms=7, color=ORANGE, zorder=7)
    save(fig, "hinh_1_1_pipeline_7_giai_doan.png")


# ---------------------------------------------------------------- Hình 2.1
def hinh_2_1():
    fig, ax = canvas(12, 5.5)
    text(ax, 1.25, 5.2, "PRODUCER (publisher)", fs=9, bold=True)
    text(ax, 6.95, 5.2, "BROKER", fs=9, bold=True)
    text(ax, 10.55, 5.2, "CONSUMER (subscriber)", fs=9, bold=True)
    prods = [("Trình duyệt / app", "tracker.js"), ("Backend tiệm bánh", "đơn hàng, thanh toán"),
             ("Simulator", "mô phỏng tải")]
    for i, (t, s) in enumerate(prods):
        y = 3.7 - i * 1.2
        titled(ax, 0.2, y, 2.1, 0.8, t, s)
        arrow(ax, (2.3, y + 0.4), (2.9, 2.75))
    titled(ax, 2.9, 2.2, 1.75, 1.1, "Collector API", "FastAPI", fc=BLUE_FILL, ec=BLUE, lw=1.6)
    text(ax, 3.77, 1.98, "HTTP POST /events", fs=7.5)
    # broker
    box(ax, 5.5, 0.55, 2.9, 4.4, fc=PANEL, ec=INK2, lw=1.2, z=1)
    text(ax, 6.95, 4.7, "Cụm Kafka – 3 broker", fs=9, color=INK, bold=True)
    topics = [("bakery.user-events", BLUE_FILL, BLUE), ("bakery.orders", BLUE_FILL, BLUE),
              ("bakery.payments", BLUE_FILL, BLUE), ("bakery.dlq", RED_FILL, RED)]
    for i, (t, fc, ec) in enumerate(topics):
        y = 3.45 - i * 0.82
        log_cells(ax, 5.7, y, 8, 0.31, 0.5, [fc] * 8, ec=ec)
        text(ax, 6.95, y + 0.66, t, fs=8.5, color=INK)
    arrow(ax, (4.65, 2.75), (5.5, 2.75), color=BLUE)
    text(ax, 5.07, 3.05, "produce\nkey = user_id", fs=7)
    # consumers
    cons = [("Nhóm Storage", "group A → Data Lake"), ("Analytics", "group B → thống kê"),
            ("Alerting", "group C → cảnh báo\nthanh toán lỗi")]
    for i, (t, s) in enumerate(cons):
        y = 3.45 - i * 1.3
        titled(ax, 9.25, y, 2.6, 0.95, t, s, fc=AQUA_FILL, ec=AQUA)
        arrow(ax, (8.4, y + 0.47), (9.25, y + 0.47), color=AQUA)
    text(ax, 8.82, 4.62, "subscribe\n+ poll()", fs=7.5)
    text(ax, 6, 0.18, "Producer chỉ cần biết tên topic  ·  Consumer chỉ cần đăng ký topic  ·  "
                      "Mỗi consumer group giữ offset riêng, đọc độc lập, không cản trở nhau", fs=8.5)
    save(fig, "hinh_2_1_publish_subscribe.png")


# ---------------------------------------------------------------- Hình 2.2
def hinh_2_2():
    fig, ax = canvas(12, 5.0)
    t0, t1, xa, xb = 8 * 60, 10 * 60 + 30, 2.9, 11.7

    def X(hh, mm, ss=0.0):
        return xa + (hh * 60 + mm + ss / 60 - t0) / (t1 - t0) * (xb - xa)

    ytop, ybot = 3.9, 2.2
    for yl, lab, sub in [(ytop, "event_time", "lúc sự kiện xảy ra tại thiết bị\n(ISO 8601, bắt buộc có múi giờ)"),
                         (ybot, "ingest_time", "lúc Collector nhận được\n(giờ máy chủ, UTC)")]:
        ax.plot([xa, xb], [yl, yl], color=INK2, lw=1.3, zorder=2)
        arrow(ax, (xb - 0.05, yl), (xb + 0.15, yl), color=INK2)
        text(ax, 0.1, yl + 0.13, lab, fs=10, color=INK, ha="left", bold=True)
        text(ax, 0.1, yl - 0.3, sub, fs=7.5, ha="left")
        for m in range(t0, t1 + 1, 15):
            x = xa + (m - t0) / (t1 - t0) * (xb - xa)
            ax.plot([x, x], [yl - 0.06, yl + 0.06], color=INK2, lw=1)
            if m % 30 == 0:
                text(ax, x, yl - 0.25, f"{m // 60:02d}:{m % 60:02d}", fs=7.5, color=MUTED)
    events = [("E1", "page_view", (8, 15), (8, 15, 0.1), BLUE),
              ("E2", "add_to_cart", (8, 35), (8, 37), BLUE),
              ("E3", "order_placed", (8, 50), (10, 10), ORANGE)]
    for name, et, a, b, c in events:
        xa_, xb_ = X(*a), X(*b)
        ax.plot(xa_, ytop, "o", ms=9, color=c, zorder=6, mec="white", mew=1.5)
        ax.plot(xb_, ybot, "o", ms=9, color=c, zorder=6, mec="white", mew=1.5)
        arrow(ax, (xa_, ytop - 0.1), (xb_, ybot + 0.1), color=c, lw=1.4)
        text(ax, xa_, ytop + 0.3, f"{name}  {et}", fs=8, color=INK)
        text(ax, xb_ + 0.12, ybot + 0.28, name, fs=8, color=INK, ha="left", bold=True)
    # ngưỡng 1 giờ cho E3
    y = 1.5
    xs, xt, xe = X(8, 50), X(9, 50), X(10, 10)
    for x in (xs, xt, xe):
        ax.plot([x, x], [y - 0.08, y + 0.08], color=INK2, lw=1)
    arrow(ax, (xs, y), (xt, y), style="<|-|>", color=INK2, lw=1, ms=8)
    ax.plot([xt, xe], [y, y], color=ORANGE, lw=3, solid_capstyle="butt", zorder=5)
    text(ax, (xs + xt) / 2, y + 0.2, "ngưỡng 1 giờ (LATE_THRESHOLD_SECONDS = 3600)", fs=7.5)
    text(ax, xe + 0.1, y, "vượt 20 phút", fs=7.5, color=INK, ha="left")
    # chú giải
    notes = ("E1  trễ 0,1 s  → is_late = false\n"
             "E2  mạng chậm, trễ 2 phút  → is_late = false\n"
             "E3  app mất mạng rồi đồng bộ lại, trễ 1 giờ 20 phút > 1 giờ  → is_late = true")
    text(ax, 0.1, 0.55, notes, fs=8.5, color=INK, ha="left")
    text(ax, 7.4, 0.55, "is_late = (ingest_time − event_time) > 1 giờ\n"
                        "Sự kiện trễ vẫn vào topic nghiệp vụ, chỉ được gắn cờ.\n"
                        "event_time sớm hơn hiện tại quá 5 phút → DLQ (event_time_in_future).",
         fs=8, ha="left")
    save(fig, "hinh_2_2_event_time_ingest_time.png")


# ---------------------------------------------------------------- Hình 2.4 (ZooKeeper)
def hinh_2_4():
    fig, ax = canvas(10, 5.4)
    box(ax, 1.0, 3.55, 8.0, 1.45, fc=PANEL, ec=INK2, z=1)
    text(ax, 5.0, 4.75, "Cụm ZooKeeper (3–5 node) – hệ thống phân tán riêng, vận hành riêng",
         fs=9, color=INK, bold=True)
    for i, t in enumerate(["ZK 1", "ZK 2 (leader)", "ZK 3"]):
        box(ax, 2.2 + i * 2.1, 3.75, 1.5, 0.6, t, fc=GRAY_FILL, fs=8.5)
    brokers = [(0.6, "Broker 1", ""), (3.9, "Broker 2", "Controller"), (7.2, "Broker 3", "")]
    for x, t, s in brokers:
        ctl = bool(s)
        titled(ax, x, 0.95, 2.2, 0.9, t, s, fc=BLUE_FILL if ctl else GRAY_FILL,
               ec=BLUE if ctl else LINE, lw=1.6 if ctl else 1.2)
    # đăng ký znode
    for xc in (1.7, 8.3):
        arrow(ax, (xc, 1.85), (xc, 3.55), ls="--")
    text(ax, 1.78, 2.7, "đăng ký znode\n/brokers/ids", fs=7.5, ha="left")
    text(ax, 8.22, 2.7, "đăng ký znode\n/brokers/ids", fs=7.5, ha="right")
    # controller đọc / ghi metadata
    arrow(ax, (5.0, 1.85), (5.0, 3.55), style="<|-|>", color=BLUE, lw=1.5)
    text(ax, 5.1, 2.7, "đọc / ghi metadata\n(/controller, topic, ISR)", fs=7.5, ha="left", color=INK)
    # controller đẩy metadata
    arrow(ax, (3.9, 1.4), (2.8, 1.4), color=BLUE, lw=1.5)
    arrow(ax, (6.1, 1.4), (7.2, 1.4), color=BLUE, lw=1.5)
    text(ax, 3.35, 1.1, "push", fs=7.5)
    text(ax, 6.65, 1.1, "push", fs=7.5)
    text(ax, 5.0, 0.35, "Controller đẩy (push) UpdateMetadata / LeaderAndIsr sang các broker còn lại.\n"
                        "Controller mới phải tải lại toàn bộ metadata từ ZooKeeper → bầu lại chậm khi cụm lớn.",
         fs=8)
    save(fig, "hinh_2_4_kien_truc_zookeeper.png")


# ---------------------------------------------------------------- Hình 2.5 (KRaft)
def hinh_2_5():
    fig, ax = canvas(10, 5.4)
    nodes = [(0.4, "kafka-1", "Active Controller", 9), (3.7, "kafka-2", "Follower controller", 8),
             (7.0, "kafka-3", "Follower controller", 7)]
    for x, name, role, n in nodes:
        act = role.startswith("Active")
        box(ax, x, 1.3, 2.6, 2.5, fc=BLUE_FILL if act else GRAY_FILL, ec=BLUE if act else LINE,
            lw=1.8 if act else 1.2)
        text(ax, x + 1.3, 3.5, name, fs=10, color=INK, bold=True)
        text(ax, x + 1.3, 3.15, f"broker + controller\n({role})", fs=8)
        text(ax, x + 1.3, 2.45, "__cluster_metadata", fs=7.5, color=INK)
        log_cells(ax, x + 0.2, 1.95, 8, 0.275, 0.32,
                  [("#ffffff" if i >= n else (BLUE_FILL if not act else "#c9ddf6")) for i in range(8)],
                  labels=[str(i) if i < n else "" for i in range(8)], fs=6.5,
                  empty_from=None if n >= 8 else n)
        text(ax, x + 1.3, 1.6, "phục vụ topic bakery.*", fs=7.5)
    # follower kéo metadata từ active controller
    arrow(ax, (3.0, 2.85), (3.7, 2.85), color=BLUE, lw=1.5)
    arrow(ax, (1.7, 3.8), (8.3, 3.8), color=BLUE, lw=1.5, rad=-0.18)
    text(ax, 3.35, 3.1, "fetch", fs=7.5)
    text(ax, 5.0, 4.75, "Follower chủ động kéo (fetch) bản ghi metadata từ Active Controller", fs=8)
    text(ax, 5.0, 0.6, "Quorum 3 voter (KAFKA_CONTROLLER_QUORUM_VOTERS, listener CONTROLLER:9093) – chịu được 1 node sập.\n"
                       "Metadata là một topic nội bộ được nhân bản bằng Raft; snapshot định kỳ để nhật ký không phình. "
                       "Không cần ZooKeeper.", fs=8)
    save(fig, "hinh_2_5_kien_truc_kraft.png")


# ---------------------------------------------------------------- Hình 2.3 (topic / partition)
CUS = {"U0000123": (BLUE, BLUE_FILL), "A3f9c2e81b": (ORANGE, ORANGE_FILL), "U0004521": (AQUA, AQUA_FILL)}


def hinh_2_3():
    fig, ax = canvas(12.6, 5.0)
    recs = [("A3f9c2e81b", 0, 3.45), ("U0000123", 1, 2.35), ("U0004521", 2, 1.25)]
    for key, p, y in recs:
        c, f = CUS[key]
        box(ax, 0.2, y, 2.2, 0.6, f"key = {key}\norder_placed", fc=f, ec=c, fs=8)
        arrow(ax, (2.4, y + 0.3), (3.0, 2.45))
    titled(ax, 3.0, 1.9, 1.6, 1.1, "Partitioner", "hash(key) mod 3")
    box(ax, 5.2, 0.85, 7.3, 3.7, fc=PANEL, ec=INK2, z=1)
    text(ax, 8.85, 4.3, "Topic bakery.orders – 3 partition", fs=9.5, color=INK, bold=True)
    counts = {0: 7, 1: 9, 2: 6}
    cw = 0.5
    for key, p, y in recs:
        c, f = CUS[key]
        yy = 3.45 - p * 1.1
        n = counts[p]
        text(ax, 5.85, yy + 0.27, f"Partition {p}", fs=8.5, color=INK)
        fills = [GRAY_FILL] * (n - 1) + [f]
        log_cells(ax, 6.55, yy, n, cw, 0.55, fills, labels=[str(i) for i in range(n)], fs=7.5)
        ax.add_patch(Rectangle((6.55 + (n - 1) * cw, yy), cw, 0.55, fc="none", ec=c, lw=1.8, zorder=5))
        arrow(ax, (6.55 + n * cw + 0.05, yy + 0.27), (6.55 + n * cw + 0.45, yy + 0.27), color=MUTED, lw=1)
        arrow(ax, (4.6, 2.45), (5.3, yy + 0.27), color=c, lw=1.4)
    text(ax, 6.55 + 9 * cw + 0.5, 2.62, "ghi nối đuôi", fs=7.5, ha="left")
    text(ax, 6.8, 4.05, "offset →", fs=7.5, ha="left")
    # vị trí đọc của consumer trên partition 0
    xp = 6.55 + 4.5 * cw
    ax.plot(xp, 3.38, marker="^", ms=7, color=INK2, zorder=6)
    text(ax, xp, 3.2, "consumer đã đọc tới offset 4  →  lag = 2", fs=7, ha="left")
    text(ax, 6.3, 0.35, "Mỗi partition là nhật ký chỉ ghi nối đuôi, thứ tự tuyệt đối theo offset.  "
                      "Cùng key → cùng partition (khi số partition không đổi).", fs=8.5)
    save(fig, "hinh_2_3_topic_partition_offset.png")


# ---------------------------------------------------------------- Hình 2.6
def hinh_2_6():
    fig, ax = canvas(12, 5.2)
    rows = [("Broker 1", "Leader", 11, BLUE), ("Broker 2", "Follower 1", 11, INK2),
            ("Broker 3", "Follower 2 (đang trễ)", 9, INK2)]
    cw, x0 = 0.6, 2.9
    for i, (b, role, n, c) in enumerate(rows):
        y = 3.5 - i * 1.2
        titled(ax, 0.2, y - 0.05, 2.35, 0.7, b, role, fc=BLUE_FILL if i == 0 else GRAY_FILL,
               ec=BLUE if i == 0 else LINE, lw=1.6 if i == 0 else 1.2)
        fills = [BLUE_FILL] * 9 + [ORANGE_FILL] * 2
        log_cells(ax, x0, y, 11, cw, 0.6, fills, labels=[str(k) for k in range(11)], fs=8,
                  empty_from=n if n < 11 else None)
        last = "đến offset 10" if n == 11 else "đến offset 8"
        text(ax, x0 + 11 * cw + 0.15, y + 0.3, last, fs=8, ha="left", color=INK)
    text(ax, x0 + 5.5 * cw, 4.35, "offset", fs=7.5)
    # high watermark
    xh = x0 + 9 * cw
    ax.plot([xh, xh], [0.95, 4.55], color=BLUE, lw=1.8, ls="--", zorder=6)
    text(ax, xh, 4.75, "High Watermark = 8", fs=9, color=BLUE, bold=True)
    text(ax, x0 + 4.5 * cw, 0.8, "đã cam kết – consumer đọc được (0–8)", fs=8, color=INK)
    text(ax, xh + cw, 0.8, "chưa cam kết", fs=8, color=INK)
    # follower kéo dữ liệu
    xr = 11.2
    arrow(ax, (xr, 3.75), (xr, 2.65), color=INK2, rad=0)
    arrow(ax, (xr + 0.35, 3.75), (xr + 0.35, 1.45), color=INK2, rad=0)
    text(ax, xr + 0.18, 4.05, "fetch", fs=7.5)
    text(ax, 6, 0.2, "ISR = {Leader, Follower 1, Follower 2} khi Follower 2 vẫn bắt kịp trong replica.lag.time.max.ms (30 s) → HW = 8.\n"
                     "Nếu Follower 2 trễ quá 30 s: ISR = {Leader, Follower 1} → HW tiến lên 10. Leader mới chỉ được chọn trong ISR.",
         fs=8, color=INK)
    save(fig, "hinh_2_6_leader_follower_isr_hw.png")


# ---------------------------------------------------------------- Hình 2.7
def hinh_2_7():
    fig, ax = canvas(13, 4.8)
    text(ax, 3.2, 4.55, "Luồng ứng dụng – produce() không chặn", fs=8.5, bold=True)
    text(ax, 11.4, 4.55, "Luồng nền librdkafka", fs=8.5, bold=True)
    ax.plot([0.2, 6.3], [4.35, 4.35], color=LINE, lw=1)
    ax.plot([9.95, 12.9], [4.35, 4.35], color=LINE, lw=1)
    y, h = 2.9, 0.95
    titled(ax, 0.2, y, 1.9, h, "Ứng dụng", "produce(topic,\nkey, value)", fc=BLUE_FILL, ec=BLUE)
    titled(ax, 2.5, y, 1.6, h, "Serializer", "JSON → bytes")
    titled(ax, 4.5, y, 1.75, h, "Partitioner", "hash(key)\n→ partition")
    arrow(ax, (2.1, y + h / 2), (2.5, y + h / 2))
    arrow(ax, (4.1, y + h / 2), (4.5, y + h / 2))
    # RecordAccumulator
    box(ax, 6.65, 1.25, 3.0, 3.05, fc=PANEL, ec=INK2, z=1)
    text(ax, 8.15, 4.05, "RecordAccumulator", fs=9.5, color=INK, bold=True)
    queues = [("P0", [1.0, 0.5]), ("P1", [1.0, 1.0, 0.3]), ("P2", [0.7])]
    for i, (p, levels) in enumerate(queues):
        yy = 3.35 - i * 0.65
        text(ax, 6.9, yy + 0.22, p, fs=8.5, color=INK)
        for k, lv in enumerate(levels):
            bx = 9.35 - (k + 1) * 0.68
            ax.add_patch(Rectangle((bx, yy), 0.6, 0.45, fc="white", ec=INK2, lw=0.9, zorder=3))
            ax.add_patch(Rectangle((bx, yy), 0.6 * lv, 0.45, fc=BLUE_FILL if lv < 1 else "#c9ddf6",
                                   ec="none", zorder=3.5))
    text(ax, 8.15, 1.6, "batch gửi đi khi đầy (batch.size)\nhoặc chờ đủ linger.ms", fs=7.5)
    arrow(ax, (6.25, y + h / 2), (6.65, y + h / 2))
    titled(ax, 10.0, 2.6, 1.55, 1.55, "Sender", "nén batch (lz4)\ngom theo broker", fs=9.5)
    arrow(ax, (9.65, 3.37), (10.0, 3.37))
    titled(ax, 11.95, 2.6, 0.95, 1.55, "Broker\nleader", fc=AQUA_FILL, ec=AQUA, fs=9)
    arrow(ax, (11.55, 3.37), (11.95, 3.37))
    # phản hồi
    box(ax, 0.2, 0.35, 3.6, 0.75, "delivery callback(err, msg)\n→ Collector hoàn tất Future", fc=BLUE_FILL,
        ec=BLUE, fs=8.5)
    polyline(ax, [(12.42, 2.6), (12.42, 0.72), (3.8, 0.72)], color=AQUA, lw=1.4)
    text(ax, 8.1, 0.93, "ack theo mức acks (all / 1): partition, offset hoặc lỗi", fs=8)
    save(fig, "hinh_2_7_duong_di_ban_ghi_producer.png")


# ---------------------------------------------------------------- Mục 3.1.2: mô hình phiên
def so_do_mo_hinh_phien():
    """Đường đi của một phiên trong Simulator; xác suất lấy từ simulator/generator.py."""
    fig, ax = canvas(8.2, 5.45)
    # (A) trang đích
    titled(ax, 0.1, 3.8, 1.45, 0.9, "page_view", "trang đích (UTM)", fc=BLUE_FILL, ec=BLUE, lw=1.6)
    # thoát ngay
    box(ax, 0.1, 2.2, 1.45, 0.75, "Thoát ngay\n(xem thêm ≤ 1 trang)", fc="white", ec=MUTED, ls="--", fs=8,
        color=INK2)
    arrow(ax, (0.82, 3.8), (0.82, 2.95))
    text(ax, 0.92, 3.38, "30 %", fs=9, color=INK, bold=True, ha="left")
    # (B) vòng duyệt sản phẩm
    box(ax, 2.05, 3.0, 2.95, 2.15, fc=PANEL, ec=INK2, z=1)
    text(ax, 3.5, 4.9, "Duyệt sản phẩm – lặp 1–6 lần", fs=9.5, color=INK, bold=True)
    chips = [("page_view?", "15 %", True), ("search?  (văn bản tự do)", "30 %", True),
             ("product_view", "luôn có", False), ("add_to_cart?", "25–35 %", True)]
    for i, (name, prob, opt) in enumerate(chips):
        y = 4.32 - i * 0.4
        box(ax, 2.2, y, 2.3, 0.32, fc="white", ec=MUTED if opt else INK2, ls="--" if opt else "-", lw=1)
        text(ax, 2.3, y + 0.16, name, fs=8.5, color=INK, ha="left")
        text(ax, 4.42, y + 0.16, prob, fs=8.5, color=INK, ha="right", bold=True)
    arrow(ax, (4.62, 3.3), (4.62, 4.5), color=INK2, rad=0.5, lw=1.1)
    arrow(ax, (1.55, 4.25), (2.05, 4.25))
    text(ax, 1.8, 4.43, "70 %", fs=9, color=INK, bold=True)
    # (C) bớt hàng khỏi giỏ
    titled(ax, 5.55, 3.8, 2.5, 0.9, "remove_from_cart?", "10 % · khi giỏ có ≥ 2 sản phẩm",
           fc=GRAY_FILL, ec=MUTED, ls="--")
    arrow(ax, (5.0, 4.25), (5.55, 4.25))
    # sang hàng dưới
    polyline(ax, [(6.8, 3.8), (6.8, 2.0), (2.8, 2.0), (2.8, 1.5)])
    text(ax, 4.8, 2.17, "40 % số phiên có giỏ hàng", fs=9, color=INK, bold=True)
    text(ax, 4.8, 1.82, "còn lại bỏ giỏ → rời đi", fs=8, color=MUTED)
    # (D) (E) (F)
    titled(ax, 2.05, 0.6, 1.5, 0.9, "checkout_start", "bắt đầu thanh toán")
    titled(ax, 4.15, 0.6, 1.5, 0.9, "order_placed", "đặt hàng", fc=AQUA_FILL, ec=AQUA)
    titled(ax, 6.25, 0.6, 1.8, 0.9, "payment", "success / failed", fc=AQUA_FILL, ec=AQUA)
    arrow(ax, (3.55, 1.05), (4.15, 1.05))
    text(ax, 3.85, 1.22, "70 %", fs=9, color=INK, bold=True)
    text(ax, 3.85, 0.85, "30 %\nrời đi", fs=7.5, color=MUTED)
    arrow(ax, (5.65, 1.05), (6.25, 1.05))
    # thanh toán lỗi thì thử lại
    arrow(ax, (7.75, 0.6), (6.55, 0.6), color=RED, lw=1.2, rad=-0.45)
    text(ax, 7.15, 0.08, "lỗi → 50 % thử lại bằng phương thức khác", fs=8, color=INK)
    # chú giải
    text(ax, 0.1, 1.15, "Khung nét đứt:\nbước không bắt buộc\n\n%: xác suất xảy ra\ntại bước đó",
         fs=7.5, ha="left")
    save(fig, "so_do_mo_hinh_phien.png")


# ---------------------------------------------------------------- Hình 3.1
def hinh_3_1(prof):
    v = prof["value"]
    order = ["page_view", "product_view", "add_to_cart", "checkout_start", "order_placed", "payment_success"]
    vi = {"page_view": "xem trang", "product_view": "xem sản phẩm", "add_to_cart": "thêm vào giỏ",
          "checkout_start": "bắt đầu thanh toán", "order_placed": "đặt hàng",
          "payment_success": "thanh toán thành công"}
    pct = [v["funnel_pct_of_sessions"][k] for k in order]
    cnt = [v["funnel_sessions"][k] for k in order]
    sessions = prof["volume"]["sessions"]
    fig, ax = plt.subplots(figsize=(9, 4.2))
    ys = list(range(len(order)))[::-1]
    ax.barh(ys, pct, height=0.62, color=BLUE, zorder=3)
    for i, (y, p, c) in enumerate(zip(ys, pct, cnt)):
        lab = f"{vn(p, 1)} %   ({vn(c)} phiên)"
        if i:
            lab += f"   ·   {vn(c / cnt[i - 1] * 100, 1)} % của bước trước"
        ax.text(p + 1.2, y, lab, va="center", fontsize=8.5, color=INK)
    ax.set_yticks(ys)
    ax.set_yticklabels([f"{k}\n{vi[k]}" for k in order], fontsize=8.5, color=INK)
    ax.set_xlim(0, 160)
    ax.set_xticks([0, 25, 50, 75, 100])
    ax.set_xticklabels(["0 %", "25 %", "50 %", "75 %", "100 %"], fontsize=8, color=INK2)
    ax.set_xlabel(f"Tỷ lệ trên tổng {vn(sessions)} phiên (tập 30 ngày)", fontsize=8.5, color=INK2)
    ax.grid(axis="x", color=GRID, lw=0.8, zorder=0)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(LINE)
    ax.tick_params(axis="both", length=0)
    fig.tight_layout()
    save(fig, "hinh_3_1_pheu_chuyen_doi.png")


# ---------------------------------------------------------------- Hình 3.2
def hinh_3_2(prof):
    vel = prof["velocity"]
    by_h = vel["events_by_hour_vn"]
    hi, lo = vel["peak_hour_vn"], vel["quietest_hour_vn"]
    fig, ax = plt.subplots(figsize=(9, 4))
    vals = [x / 1e6 for x in by_h]
    colors = [ORANGE if h in (hi, lo) else BLUE for h in range(24)]
    ax.bar(range(24), vals, width=0.72, color=colors, zorder=3)
    ax.annotate(f"19h: {vn(by_h[hi])} sự kiện", (hi, vals[hi]), xytext=(hi - 6.8, 1.9),
                fontsize=8.5, color=INK, arrowprops=dict(arrowstyle="-", color=INK2, lw=0.8))
    ax.annotate(f"2h: {vn(by_h[lo])} sự kiện", (lo, vals[lo]), xytext=(-0.4, 0.42),
                fontsize=8.5, color=INK, arrowprops=dict(arrowstyle="-", color=INK2, lw=0.8))
    ax.text(-0.4, 1.85, f"cao điểm / thấp điểm = {vn(vel['peak_to_quiet_ratio'], 1)}×",
            fontsize=8.5, color=INK, fontweight="bold", va="top")
    ax.set_xticks(range(24))
    ax.set_xticklabels([f"{h}h" for h in range(24)], fontsize=7.5, color=INK2)
    ax.set_xlim(-0.6, 23.6)
    ax.set_ylim(0, 2.0)
    ax.set_yticks([0, 0.5, 1.0, 1.5])
    ax.set_ylabel("Số sự kiện (triệu)", fontsize=8.5, color=INK2)
    ax.set_xlabel("Giờ trong ngày (giờ Việt Nam), cộng dồn 30 ngày", fontsize=8.5, color=INK2)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: vn(v, 1)))
    ax.tick_params(axis="y", labelsize=8, colors=INK2, length=0)
    ax.tick_params(axis="x", length=0)
    ax.grid(axis="y", color=GRID, lw=0.8, zorder=0)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color(LINE)
    fig.tight_layout()
    save(fig, "hinh_3_2_luu_luong_theo_gio.png")


# ---------------------------------------------------------------- Hình 4.1
def hinh_4_1():
    fig, ax = canvas(14.2, 6.0)
    tiers = [(0.2, 2.9, "(1) Sources", "nguồn phát sinh sự kiện"),
             (3.8, 3.0, "(2) Gateway", "cổng thu nhận"),
             (7.5, 3.3, "(3) Message Broker", "hàng đợi phân tán"),
             (11.5, 2.6, "(4) Consumer", "đọc lại & bàn giao")]
    for x, w, t, s in tiers:
        box(ax, x, 0.8, w, 4.75, fc=PANEL, ec=LINE, z=1)
        text(ax, x + w / 2, 5.27, t, fs=10.5, color=INK, bold=True)
        text(ax, x + w / 2, 4.98, s, fs=8)
    # (1)
    srcs = [("tracker.js", "trình duyệt web", 3.85), ("Backend tiệm bánh", "đơn hàng, thanh toán", 2.75),
            ("Simulator", "--mode api | kafka", 1.65)]
    for t, s, y in srcs:
        titled(ax, 0.4, y, 2.5, 0.85, t, s)
        arrow(ax, (2.9, y + 0.42), (4.0, 3.0))
    # (2)
    box(ax, 4.0, 1.45, 2.6, 3.35, fc=BLUE_FILL, ec=BLUE, lw=1.6)
    text(ax, 5.3, 4.55, "Collector API (FastAPI)", fs=9, color=INK, bold=True)
    steps = ["validate – Pydantic v2", "dedup – event_id (LRU)", "enrich – ingest_time, is_late",
             "route – event_type → topic"]
    for i, s in enumerate(steps):
        yy = 3.8 - i * 0.62
        box(ax, 4.15, yy, 2.3, 0.45, s, fc="white", ec=BLUE, fs=7.8, lw=1)
        if i:
            arrow(ax, (5.3, yy + 0.62), (5.3, yy + 0.45), color=BLUE, lw=1, ms=8)
    text(ax, 5.3, 1.7, "lỗi → bakery.dlq kèm reason", fs=7.5, color=RED)
    # (3)
    for i in range(3):
        box(ax, 7.7 + i * 1.0, 3.85, 0.9, 0.55, f"kafka-{i + 1}", fc=GRAY_FILL, fs=8)
    text(ax, 9.15, 3.6, "KRaft · RF = 3 · min.insync.replicas = 2", fs=7.5)
    box(ax, 7.7, 1.55, 2.9, 1.85, fc="white", ec=LINE, z=2)
    tps = [("bakery.user-events", BLUE), ("bakery.orders", BLUE), ("bakery.payments", BLUE),
           ("bakery.dlq", RED), ("bakery.bench.p1 / p3 / p6", MUTED)]
    for i, (t, c) in enumerate(tps):
        yy = 3.15 - i * 0.33
        ax.add_patch(Rectangle((7.85, yy - 0.08), 0.16, 0.16, fc=c, ec="none", zorder=4))
        text(ax, 8.12, yy, t, fs=8, color=INK, ha="left")
    box(ax, 7.7, 0.95, 2.9, 0.45, "Kafka UI – giám sát cụm", fc=GRAY_FILL, fs=8)
    arrow(ax, (6.6, 2.6), (7.7, 2.6), color=BLUE, lw=1.5)
    # Simulator --mode kafka: ghi thẳng
    polyline(ax, [(2.9, 1.82), (3.45, 1.82), (3.45, 1.1), (7.0, 1.1), (7.0, 1.75), (7.7, 1.75)],
             color=INK2, ls="--", lw=1.1)
    text(ax, 5.25, 0.95, "Simulator --mode kafka: ghi thẳng (benchmark)", fs=7.5)
    # (4)
    cons = [("verify_consumer.py", "đếm, latency, lag, trùng", 3.75, GRAY_FILL, LINE, "-"),
            ("bench_consumer.py", "thí nghiệm E1–E9", 2.65, GRAY_FILL, LINE, "-"),
            ("Nhóm Storage", "Kafka Connect → Data Lake\n(điểm bàn giao)", 1.2, ORANGE_FILL, ORANGE, "--")]
    for t, s, y, fc, ec, ls in cons:
        titled(ax, 11.7, y, 2.2, 0.95 if ls == "-" else 1.15, t, s, fc=fc, ec=ec, ls=ls)
        arrow(ax, (10.6, 2.48), (11.7, y + 0.47), color=AQUA, lw=1.3)
    # giao thức giữa các tầng
    for x, s in [(3.45, "HTTP / JSON"), (7.15, "Kafka protocol\nLZ4, acks"), (11.1, "consume\n(poll)")]:
        text(ax, x, 0.38, s, fs=8, color=INK, bold=True)
    save(fig, "hinh_4_1_kien_truc_4_tang.png")


# ---------------------------------------------------------------- Hình 4.2
def hinh_4_2():
    fig, ax = canvas(12.5, 5.0)
    cus = [("A3f9c2e81b", "khách vãng lai", 0, ["page_view", "search", "product_view"]),
           ("U0000123", "thành viên", 1, ["page_view", "product_view", "add_to_cart", "checkout_start"]),
           ("U0004521", "thành viên", 2, ["page_view", "product_view", "search", "add_to_cart", "remove_from_cart"])]
    text(ax, 1.15, 4.65, "user_id (key)", fs=9, bold=True)
    titled(ax, 2.7, 1.95, 1.75, 1.1, "Partitioner", "hash(user_id) mod 3")
    box(ax, 5.0, 0.95, 7.3, 3.85, fc=PANEL, ec=INK2, z=1)
    text(ax, 8.65, 4.55, "Topic bakery.user-events", fs=9.5, color=INK, bold=True)
    cw = 1.12
    for key, kind, p, evs in cus:
        c, f = CUS[key]
        y = 3.55 - p * 1.1
        titled(ax, 0.2, y - 0.05, 1.9, 0.7, key, kind, fc=f, ec=c, fs=9)
        arrow(ax, (2.1, y + 0.3), (2.7, 2.5), color=c, lw=1.3)
        arrow(ax, (4.45, 2.5), (5.6, y + 0.3), color=c, lw=1.3)
        text(ax, 5.62, y + 0.3, f"P{p}", fs=9, color=INK, bold=True, ha="left")
        log_cells(ax, 6.0, y, len(evs), cw, 0.6, [f] * len(evs), labels=evs, fs=7.2, ec=c)
        for k in range(len(evs)):
            text(ax, 6.0 + k * cw + cw / 2, y + 0.72, f"offset {k}", fs=6.5, color=MUTED)
    # thứ tự của U0000123
    y1 = 3.55 - 1.1
    ax.add_patch(Rectangle((6.0, y1), 4 * cw, 0.6, fc="none", ec=BLUE, lw=2.2, zorder=6))
    arrow(ax, (6.05, y1 - 0.18), (6.0 + 4 * cw - 0.05, y1 - 0.18), color=BLUE, lw=1.4)
    text(ax, 6.0 + 4 * cw + 0.15, y1 + 0.3, "thứ tự hành trình\ncủa U0000123\nđược giữ nguyên", fs=7.5, color=INK, ha="left")
    text(ax, 6.25, 0.45, "order_placed và payment của U0000123 cũng vào partition 1 của bakery.orders / bakery.payments\n"
                         "(cùng key, cùng số partition, cùng hàm băm) – Processing có thể join theo khách.",
         fs=8, color=INK)
    save(fig, "hinh_4_2_anh_xa_key_partition.png")


# ---------------------------------------------------------------- Hình 4.3
def hinh_4_3():
    fig, ax = canvas(10, 10.4)
    cx, w, h = 2.7, 4.2, 0.75
    x = cx - w / 2

    def step(y, t, **kw):
        box(ax, x, y, w, h, t, fs=8.5, **kw)

    step(9.4, "POST /events\n(1 object hoặc mảng ≤ 5.000 event)", fc=BLUE_FILL, ec=BLUE, lw=1.6, bold=True)
    step(8.25, "Đọc body thô, json.loads()")
    arrow(ax, (cx, 9.4), (cx, 9.0))
    box(ax, 6.2, 8.25, 3.4, h, "Body không phải JSON → 400\nLô > 5.000 event → 413", fc=RED_FILL, ec=RED, fs=8)
    arrow(ax, (x + w, 8.62), (6.2, 8.62), color=RED)
    # vòng lặp từng event
    box(ax, x - 0.25, 2.95, w + 0.5, 4.95, fc="none", ec=INK2, ls="--", lw=1.1, z=1)
    text(ax, x - 0.1, 7.65, "Với từng phần tử của lô", fs=8.5, color=INK, ha="left", bold=True)
    step(6.6, "validate_event()\nschema · múi giờ · thời gian tương lai")
    step(5.35, "dedup.add(event_id)")
    step(4.4, "Làm giàu: ingest_time (UTC), is_late")
    step(3.25, "send(ROUTES[event_type], key = user_id)\n→ Future", fc=BLUE_FILL, ec=BLUE)
    arrow(ax, (cx, 8.25), (cx, 7.35))
    for y0, y1, lab in [(6.6, 6.1, "hợp lệ"), (5.35, 5.15, "mới"), (4.4, 4.0, "")]:
        arrow(ax, (cx, y0), (cx, y1))
        if lab:
            text(ax, cx + 0.1, (y0 + y1) / 2, lab, fs=7.5, ha="left")
    # DLQ
    box(ax, 6.2, 5.0, 3.4, 1.6, "Thông điệp DLQ\n{reason, detail, ingest_time, raw}\n→ send(bakery.dlq)\n→ Future",
        fc=RED_FILL, ec=RED, fs=8)
    arrow(ax, (x + w, 6.97), (6.2, 6.25), color=RED)
    arrow(ax, (x + w, 5.72), (6.2, 5.45), color=RED)
    text(ax, 5.45, 6.85, "lỗi (reason)", fs=7.5, color=INK)
    text(ax, 5.45, 5.38, "trùng", fs=7.5, color=INK)
    # chờ Kafka
    step(1.75, "await gather(mọi Future)\ntimeout 30 s", fc=GRAY_FILL, bold=True)
    arrow(ax, (cx, 3.25), (cx, 2.5))
    polyline(ax, [(7.9, 5.0), (7.9, 2.12), (x + w, 2.12)], color=RED)
    box(ax, 0.2, 0.2, 3.5, 0.85, "200 {received, accepted, rejected}", fc=AQUA_FILL, ec=AQUA, fs=8.5, bold=True)
    box(ax, 4.6, 0.2, 5.0, 0.85, "503 + dedup.discard(event_id của lô)\n→ client gửi lại, không bị coi là trùng",
        fc=ORANGE_FILL, ec=ORANGE, fs=8.5)
    arrow(ax, (cx - 0.6, 1.75), (1.95, 1.05), color=AQUA)
    arrow(ax, (cx + 0.6, 1.75), (6.0, 1.05), color=ORANGE)
    text(ax, 1.25, 1.42, "Kafka xác nhận hết", fs=7.5, color=INK)
    text(ax, 5.85, 1.42, "có Future lỗi / quá hạn", fs=7.5, color=INK)
    save(fig, "hinh_4_3_luong_xu_ly_post_events.png")


if __name__ == "__main__":
    prof = json.loads((ROOT / "results/profile_dataset_oct2026.json").read_text(encoding="utf-8"))
    hinh_1_1(); hinh_2_1(); hinh_2_2(); hinh_2_4(); hinh_2_5(); hinh_2_3(); hinh_2_6(); hinh_2_7()
    so_do_mo_hinh_phien(); hinh_3_1(prof); hinh_3_2(prof)
    hinh_4_1(); hinh_4_2(); hinh_4_3()
