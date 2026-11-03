"""Đo tập dữ liệu do simulator sinh ra (--mode file) theo 5V của Big Data, xuất báo cáo Markdown + JSON.

    Volume    số event / phiên / khách, dung lượng thô và nén
    Velocity  tốc độ phát sinh trung bình, giờ cao điểm, phút cao điểm, event đến trễ
    Variety   số schema khác nhau, kiểu dữ liệu lồng nhau, định dạng file, nguồn truy cập, văn bản tự do
    Veracity  kiểm lại từng event theo data contract (cùng luật với Collector), đối chiếu với số bẩn đã chèn
    Value     phễu chuyển đổi, doanh thu, KPI theo giờ / danh mục / cửa hàng / chiến dịch / phương thức thanh toán

Mỗi ngày (1 file) do 1 tiến trình đọc theo dòng (streaming, không nạp cả tập vào RAM), rồi gộp kết quả.

Ví dụ:
  python simulator/profile_dataset.py data/dataset_oct2026 --workers 4
"""
import argparse
import csv
import gzip
import json
import multiprocessing as mp
import re
import sys
import time
import uuid
from collections import Counter, OrderedDict, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from generator import CLICK_TYPES, EXPECTED_REASON, VN_TZ, strip_accents

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"

# ---------------------------------------------------------------- data contract (giống collector/schemas)
COMMON_REQUIRED = {"event_id", "event_type", "event_time", "user_id", "session_id", "source", "schema_version"}
COMMON_OPTIONAL = {"device"}
FIELDS = {
    **{t: {"page", "search_query", "product_id", "category", "price", "quantity"} for t in CLICK_TYPES},
    "order_placed": {"order_id", "store_id", "items", "total_amount", "voucher", "delivery_type"},
    "payment": {"order_id", "payment_method", "status", "amount"},
}
REQUIRED_EXTRA = {"order_placed": {"order_id", "store_id", "items", "total_amount", "delivery_type"},
                  "payment": {"order_id", "payment_method", "status", "amount"}}
ENUMS = {"source": {"web", "app", "simulator"}, "device": {"desktop", "mobile", "tablet", None},
         "delivery_type": {"pickup", "delivery"}, "status": {"success", "failed"},
         "payment_method": {"cash", "card", "momo", "zalopay", "vnpay", "bank_transfer"}}
FUNNEL = ["page_view", "product_view", "add_to_cart", "checkout_start", "order_placed", "payment_success"]
DEDUP_WINDOW = 200_000  # như DEDUP_CACHE_SIZE của Collector
FUTURE_SKEW = timedelta(hours=12)
LATE = timedelta(hours=1)


def _num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def parse_time(s):
    """Trả về datetime, None nếu sai định dạng; datetime không múi giờ vẫn trả về (để báo missing_timezone)."""
    if not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def check(e) -> tuple[str | None, datetime | None]:
    """Kiểm 1 event theo contract. Trả về (lý do lỗi hoặc None, event_time)."""
    if not isinstance(e, dict):
        return "not_an_object", None
    etype = e.get("event_type")
    if etype not in FIELDS:
        return "unknown_event_type", None
    keys = set(e)
    if not COMMON_REQUIRED | REQUIRED_EXTRA.get(etype, set()) <= keys:
        return "schema_violation", None
    if keys - COMMON_REQUIRED - COMMON_OPTIONAL - FIELDS[etype]:
        return "schema_violation", None
    try:
        uuid.UUID(str(e["event_id"]))
    except ValueError:
        return "schema_violation", None
    for k, allowed in ENUMS.items():
        if k in e and e[k] not in allowed:
            return "schema_violation", None
    if not all(isinstance(e[k], str) and 1 <= len(e[k]) <= 64 for k in ("user_id", "session_id")):
        return "schema_violation", None
    if e.get("price") is not None and not (_num(e["price"]) and e["price"] >= 0):
        return "schema_violation", None
    q = e.get("quantity")
    if q is not None and not (isinstance(q, int) and not isinstance(q, bool) and 1 <= q <= 100):
        return "schema_violation", None
    for k in ("total_amount", "amount"):
        if k in e and not (_num(e[k]) and e[k] > 0):
            return "schema_violation", None
    if etype == "order_placed":
        items = e["items"]
        if not isinstance(items, list) or not items or not all(
                isinstance(i, dict) and set(i) == {"product_id", "category", "unit_price", "quantity"}
                and _num(i["unit_price"]) and i["unit_price"] > 0 for i in items):
            return "schema_violation", None
    t = parse_time(e["event_time"])
    if t is None:
        return "schema_violation", None
    if t.tzinfo is None:
        return "missing_timezone", None
    return None, t


# ---------------------------------------------------------------- đọc 1 file (1 ngày)
def campaign_of(page: str | None) -> str:
    m = re.search(r"utm_source=([^&]+)", page or "")
    return m.group(1) if m else "direct"


def profile_file(path: str) -> dict:
    p = Path(path)
    opener = gzip.open if p.suffix == ".gz" else open
    day = re.search(r"date=(\d{4}-\d{2}-\d{2})", path)
    ref = datetime.fromisoformat(day.group(1)).replace(tzinfo=VN_TZ) if day else None

    a = {"events": 0, "raw_bytes": 0, "file_bytes": p.stat().st_size, "t_min": None, "t_max": None}
    issues, late = Counter(), 0
    by_type, schemas, sources, devices, campaigns_ = Counter(), Counter(), Counter(), Counter(), Counter()
    per_minute, per_hour = Counter(), Counter()
    queries, queries_no_accent = Counter(), 0
    seen = OrderedDict()
    sessions: dict[str, dict] = {}
    orders: dict[str, dict] = {}
    pay = defaultdict(Counter)  # method -> success/failed
    users, buyers = set(), Counter()
    revenue_by = defaultdict(Counter)  # chiều -> giá trị -> doanh thu
    max_valid = None

    with opener(p, "rt", encoding="utf-8") as f:
        for line in f:
            a["events"] += 1
            a["raw_bytes"] += len(line.encode("utf-8"))
            try:
                e = json.loads(line)
            except ValueError:
                issues["invalid_json"] += 1
                continue
            reason, t = check(e)
            if reason is None:
                # cùng thứ tự với Collector: lệch tương lai -> trùng id
                anchor = max_valid or ref
                if anchor is not None and t - anchor > FUTURE_SKEW:
                    reason = "event_time_in_future"
                elif e["event_id"] in seen:
                    reason = "duplicate_event_id"
            if reason:
                issues[reason] += 1
                continue
            seen[e["event_id"]] = None
            if len(seen) > DEDUP_WINDOW:
                seen.popitem(last=False)
            if max_valid is not None and t < max_valid - LATE:
                late += 1  # hợp lệ nhưng đến trễ / lệch thứ tự (Collector gắn is_late)
            max_valid = t if max_valid is None or t > max_valid else max_valid

            etype = e["event_type"]
            by_type[etype] += 1
            schemas[(etype, tuple(sorted(e)))] += 1
            sources[e["source"]] += 1
            devices[e.get("device")] += 1
            a["t_min"] = t if a["t_min"] is None or t < a["t_min"] else a["t_min"]
            a["t_max"] = t if a["t_max"] is None or t > a["t_max"] else a["t_max"]
            local = t.astimezone(VN_TZ)
            per_minute[local.strftime("%Y-%m-%d %H:%M")] += 1
            per_hour[local.hour] += 1
            users.add(e["user_id"])

            s = sessions.setdefault(e["session_id"], {"stages": set(), "campaign": None, "user": e["user_id"]})
            if etype == "page_view" and s["campaign"] is None:
                s["campaign"] = campaign_of(e.get("page"))
                campaigns_[s["campaign"]] += 1
            if etype in FUNNEL:
                s["stages"].add(etype)
            if etype == "search":
                queries[e["search_query"]] += 1
                queries_no_accent += strip_accents(e["search_query"]) == e["search_query"]
            elif etype == "order_placed":
                orders[e["order_id"]] = {"items": e["items"], "store": e["store_id"], "voucher": e.get("voucher"),
                                         "delivery": e["delivery_type"], "hour": local.hour,
                                         "campaign": s["campaign"] or "direct"}
            elif etype == "payment":
                pay[e["payment_method"]][e["status"]] += 1
                o = orders.get(e["order_id"])
                if e["status"] == "success" and o and not o.get("paid"):
                    o["paid"] = e["amount"]
                    s["stages"].add("payment_success")
                    buyers[e["user_id"]] += 1

    paid = [o for o in orders.values() if o.get("paid")]
    for o in paid:
        revenue_by["store"][o["store"]] += o["paid"]
        revenue_by["hour"][o["hour"]] += o["paid"]
        revenue_by["campaign"][o["campaign"]] += o["paid"]
        revenue_by["voucher"][o["voucher"] or "(không)"] += o["paid"]
        revenue_by["delivery"][o["delivery"]] += o["paid"]
        subtotal = sum(i["unit_price"] * i["quantity"] for i in o["items"]) or 1
        for i in o["items"]:  # chia doanh thu thực thu theo tỷ lệ giá trị từng dòng
            revenue_by["category"][i["category"]] += o["paid"] * i["unit_price"] * i["quantity"] / subtotal
            revenue_by["product"][i["product_id"]] += o["paid"] * i["unit_price"] * i["quantity"] / subtotal
    funnel = Counter()
    conv_by_campaign = defaultdict(Counter)
    for s in sessions.values():
        for st in s["stages"]:
            funnel[st] += 1
        c = s["campaign"] or "direct"
        conv_by_campaign[c]["sessions"] += 1
        conv_by_campaign[c]["paid"] += "payment_success" in s["stages"]
    a.update(issues=issues, late=late, by_type=by_type, schemas=schemas, sources=sources, devices=devices,
             campaigns=campaigns_, per_minute=per_minute, per_hour=per_hour, queries=queries,
             queries_no_accent=queries_no_accent, sessions=len(sessions), funnel=funnel,
             conv_by_campaign={k: dict(v) for k, v in conv_by_campaign.items()},
             pay={k: dict(v) for k, v in pay.items()}, users=users, buyers=buyers,
             revenue_by={k: dict(v) for k, v in revenue_by.items()},
             orders=len(orders), paid_orders=len(paid), revenue=sum(o["paid"] for o in paid))
    return a


# ---------------------------------------------------------------- gộp + báo cáo
def merge(parts: list[dict]) -> dict:
    m = {"events": 0, "raw_bytes": 0, "file_bytes": 0, "late": 0, "sessions": 0, "orders": 0, "paid_orders": 0,
         "revenue": 0.0, "queries_no_accent": 0, "t_min": None, "t_max": None, "users": set(), "buyers": Counter(),
         "pay": defaultdict(Counter), "revenue_by": defaultdict(Counter), "conv_by_campaign": defaultdict(Counter)}
    counters = ("issues", "by_type", "schemas", "sources", "devices", "campaigns", "per_minute", "per_hour",
                "queries", "funnel")
    for k in counters:
        m[k] = Counter()
    for p in parts:
        for k in ("events", "raw_bytes", "file_bytes", "late", "sessions", "orders", "paid_orders", "revenue",
                  "queries_no_accent"):
            m[k] += p[k]
        for k in counters:
            m[k].update(p[k])
        m["users"] |= p["users"]
        m["buyers"].update(p["buyers"])
        for k, v in p["pay"].items():
            m["pay"][k].update(v)
        for k, v in p["revenue_by"].items():
            m["revenue_by"][k].update(v)
        for k, v in p["conv_by_campaign"].items():
            m["conv_by_campaign"][k].update(v)
        if p["t_min"] and (m["t_min"] is None or p["t_min"] < m["t_min"]):
            m["t_min"] = p["t_min"]
        if p["t_max"] and (m["t_max"] is None or p["t_max"] > m["t_max"]):
            m["t_max"] = p["t_max"]
    return m


def load_tiers(root: Path) -> dict[str, str]:
    path = root / "dim" / "customers.csv"
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return {r["user_id"]: r["tier"] for r in csv.DictReader(f)}


def pct(a, b) -> float:
    return round(100 * a / b, 2) if b else 0.0


def build_report(root: Path, m: dict, manifest: dict, secs: float) -> dict:
    span = (m["t_max"] - m["t_min"]).total_seconds() if m["t_min"] else 0
    valid = sum(m["by_type"].values())
    peak_min, peak_cnt = max(m["per_minute"].items(), key=lambda kv: kv[1]) if m["per_minute"] else ("", 0)
    hours = [m["per_hour"].get(h, 0) for h in range(24)]
    tiers = load_tiers(root)
    tier_buyers = Counter(tiers.get(u, "guest" if u.startswith("A") else "member") for u in m["buyers"])
    tier_orders = Counter()
    for u, n in m["buyers"].items():
        tier_orders[tiers.get(u, "guest" if u.startswith("A") else "member")] += n
    members_seen = sum(1 for u in m["users"] if u.startswith("U"))
    expected = Counter()
    for kind, n in manifest.get("dirty_by_kind", {}).items():
        expected[EXPECTED_REASON[kind]] += n
    files = [name for pattern, name in (("date=*/*.jsonl.gz", "JSON Lines nén gzip (event)"),
                                        ("date=*/*.jsonl", "JSON Lines (event)"),
                                        ("dim/*.csv", "CSV (bảng dim)"), ("manifest.json", "JSON (manifest)"))
             if any(root.glob(pattern))]
    top = lambda c, n=5: [[k, round(v)] for k, v in sorted(c.items(), key=lambda kv: -kv[1])[:n]]  # noqa: E731
    f = m["funnel"]
    return {
        "dataset": str(root),
        "profiled_in_sec": round(secs, 1),
        "volume": {
            "events": m["events"], "valid_events": valid, "sessions": m["sessions"],
            "distinct_users": len(m["users"]), "members_seen": members_seen,
            "guests_seen": len(m["users"]) - members_seen,
            "raw_mb": round(m["raw_bytes"] / 2**20, 1), "file_mb": round(m["file_bytes"] / 2**20, 1),
            "compression_ratio": round(m["raw_bytes"] / m["file_bytes"], 1) if m["file_bytes"] else None,
            "days": len(manifest.get("days", {})) or None,
            "avg_event_bytes": round(m["raw_bytes"] / m["events"]) if m["events"] else 0,
        },
        "velocity": {
            "time_span_hours": round(span / 3600, 1),
            "avg_events_per_sec": round(valid / span, 2) if span else None,
            "peak_minute": peak_min, "peak_minute_events_per_sec": round(peak_cnt / 60, 2),
            "peak_hour_vn": hours.index(max(hours)), "quietest_hour_vn": hours.index(min(hours)),
            "peak_to_quiet_ratio": round(max(hours) / max(1, min(hours)), 1),
            "events_by_hour_vn": hours,
            "late_or_out_of_order_events": m["late"],
        },
        "variety": {
            "event_types": dict(m["by_type"]),
            "distinct_schemas": len(m["schemas"]),
            "schemas": sorted({f"{t}: {', '.join(k for k in keys if k not in COMMON_REQUIRED)}"
                               for (t, keys) in m["schemas"]}),
            "nested_array_field": "order_placed.items[] (product_id, category, unit_price, quantity)",
            "file_formats": files,
            "dim_tables": manifest.get("dims", {}),
            "sources": dict(m["sources"]), "devices": {str(k): v for k, v in m["devices"].items()},
            "traffic_channels": dict(m["campaigns"]),
            "distinct_search_queries": len(m["queries"]),
            "search_queries_without_accents_pct": pct(m["queries_no_accent"], sum(m["queries"].values())),
            "search_examples": [q for q, _ in m["queries"].most_common(400)][::40],
        },
        "veracity": {
            "invalid_events": sum(m["issues"].values()),
            "invalid_pct": pct(sum(m["issues"].values()), m["events"]),
            "detected_by_reason": dict(m["issues"]),
            "injected_by_reason": dict(expected),
            "detection_matches_injection": dict(m["issues"]) == dict(expected),
            "late_sessions_injected": manifest.get("late_sessions"),
        },
        "value": {
            "funnel_sessions": {s: f.get(s, 0) for s in FUNNEL},
            "funnel_pct_of_sessions": {s: pct(f.get(s, 0), m["sessions"]) for s in FUNNEL},
            "orders": m["orders"], "paid_orders": m["paid_orders"],
            "revenue_vnd": round(m["revenue"]),
            "avg_order_value_vnd": round(m["revenue"] / m["paid_orders"]) if m["paid_orders"] else 0,
            "revenue_by_category": top(m["revenue_by"]["category"], 10),
            "top_products": top(m["revenue_by"]["product"]),
            "revenue_by_store": top(m["revenue_by"]["store"], 10),
            "revenue_by_channel": top(m["revenue_by"]["campaign"], 10),
            "conversion_by_channel_pct": {k: pct(v["paid"], v["sessions"]) for k, v in
                                          sorted(m["conv_by_campaign"].items(), key=lambda kv: -kv[1]["sessions"])},
            "voucher_revenue": top(m["revenue_by"]["voucher"], 10),
            "delivery_revenue": top(m["revenue_by"]["delivery"]),
            "payment_fail_rate_pct": {k: pct(v.get("failed", 0), sum(v.values())) for k, v in
                                      sorted(m["pay"].items())},
            "buyers": len(m["buyers"]),
            "repeat_buyers_pct": pct(sum(1 for n in m["buyers"].values() if n > 1), len(m["buyers"])),
            "orders_per_buyer_by_tier": {k: round(tier_orders[k] / tier_buyers[k], 2) for k in tier_buyers},
        },
    }


def fmt(n) -> str:
    return f"{n:,.0f}".replace(",", ".") if isinstance(n, (int, float)) else str(n)


def to_markdown(r: dict) -> str:
    v, ve, va, vr, vl = r["volume"], r["velocity"], r["variety"], r["veracity"], r["value"]
    L = [f"# Hồ sơ 5V – `{Path(r['dataset']).name}`", "",
         f"Đọc toàn bộ tập dữ liệu trong {r['profiled_in_sec']} s. Số liệu do `simulator/profile_dataset.py` tính.", "",
         "## Volume", "",
         f"- {fmt(v['events'])} event ({fmt(v['valid_events'])} hợp lệ), {fmt(v['sessions'])} phiên, "
         f"{fmt(v['distinct_users'])} khách ({fmt(v['members_seen'])} thành viên, {fmt(v['guests_seen'])} vãng lai)",
         f"- Dung lượng: {fmt(v['raw_mb'])} MB JSON thô, {fmt(v['file_mb'])} MB sau nén "
         f"(×{v['compression_ratio']}), trung bình {v['avg_event_bytes']} byte/event, {v['days']} ngày", "",
         "## Velocity", "",
         f"- Trải dài {ve['time_span_hours']} giờ, trung bình {ve['avg_events_per_sec']} event/s",
         f"- Phút cao điểm {ve['peak_minute']}: {ve['peak_minute_events_per_sec']} event/s",
         f"- Giờ cao điểm {ve['peak_hour_vn']}h, vắng nhất {ve['quietest_hour_vn']}h, chênh ×{ve['peak_to_quiet_ratio']}",
         f"- {fmt(ve['late_or_out_of_order_events'])} event hợp lệ nhưng đến trễ / lệch thứ tự > 1 giờ", "",
         "## Variety", "",
         f"- {len(va['event_types'])} loại event, {va['distinct_schemas']} tổ hợp trường khác nhau; "
         f"trường lồng nhau: {va['nested_array_field']}",
         f"- Định dạng: {', '.join(va['file_formats'])}; bảng dim: "
         f"{', '.join(f'{k} ({fmt(n)} dòng)' for k, n in va['dim_tables'].items()) or '—'}",
         f"- Nguồn: {va['sources']}; thiết bị: {va['devices']}",
         f"- Kênh truy cập (utm): {va['traffic_channels']}",
         f"- {fmt(va['distinct_search_queries'])} câu tìm kiếm khác nhau, {va['search_queries_without_accents_pct']}% "
         f"gõ không dấu. Ví dụ: {', '.join(repr(q) for q in va['search_examples'][:8])}", "",
         "## Veracity", "",
         f"- {fmt(vr['invalid_events'])} event vi phạm contract ({vr['invalid_pct']}%)",
         "", "| Lý do | Phát hiện | Đã chèn |", "|---|---:|---:|"]
    for k in sorted(set(vr["detected_by_reason"]) | set(vr["injected_by_reason"])):
        L.append(f"| `{k}` | {fmt(vr['detected_by_reason'].get(k, 0))} | {fmt(vr['injected_by_reason'].get(k, 0))} |")
    L += ["", f"- Khớp hoàn toàn với số đã chèn: **{'có' if vr['detection_matches_injection'] else 'không'}**", "",
          "## Value", "", "| Bước phễu | Phiên | % phiên |", "|---|---:|---:|"]
    for s, n in vl["funnel_sessions"].items():
        L.append(f"| {s} | {fmt(n)} | {vl['funnel_pct_of_sessions'][s]} |")
    L += ["",
          f"- {fmt(vl['paid_orders'])} đơn đã thanh toán, doanh thu {fmt(vl['revenue_vnd'])} đ, "
          f"giá trị đơn trung bình {fmt(vl['avg_order_value_vnd'])} đ",
          f"- Doanh thu theo danh mục: {', '.join(f'{k} {fmt(x)}' for k, x in vl['revenue_by_category'])}",
          f"- Doanh thu theo cửa hàng: {', '.join(f'{k} {fmt(x)}' for k, x in vl['revenue_by_store'])}",
          f"- Tỷ lệ chuyển đổi theo kênh (%): {vl['conversion_by_channel_pct']}",
          f"- Tỷ lệ thanh toán lỗi theo phương thức (%): {vl['payment_fail_rate_pct']}",
          f"- {fmt(vl['buyers'])} người mua, {vl['repeat_buyers_pct']}% mua lại; "
          f"số đơn / người mua theo hạng: {vl['orders_per_buyer_by_tier']}", ""]
    return "\n".join(L)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dataset", help="thư mục do simulator --mode file tạo (có manifest.json)")
    ap.add_argument("--workers", type=int, default=max(1, min(8, (mp.cpu_count() or 2) - 1)))
    ap.add_argument("--out", default=None, help="nơi ghi báo cáo, mặc định results/")
    args = ap.parse_args(argv)
    root = Path(args.dataset)
    files = sorted(str(p) for p in root.glob("date=*/events.jsonl*"))
    if not files:
        ap.error(f"không thấy file date=*/events.jsonl[.gz] trong {root}")
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}

    t0 = time.time()
    print(f"Đọc {len(files)} file bằng {args.workers} tiến trình...", flush=True)
    with mp.Pool(args.workers) as pool:
        parts = pool.map(profile_file, files)
    report = build_report(root, merge(parts), manifest, time.time() - t0)

    out = Path(args.out) if args.out else RESULTS_DIR
    out.mkdir(parents=True, exist_ok=True)
    name = f"profile_{root.name}"
    (out / f"{name}.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md = to_markdown(report)
    (out / f"{name}.md").write_text(md, encoding="utf-8")
    print(md)
    print(f"Đã lưu: {out / (name + '.md')} và .json")
    return report


if __name__ == "__main__":
    main()
