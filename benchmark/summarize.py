"""
Tổng hợp kết quả benchmark TV4 từ results/tv4/raw (+ soak) → bảng số liệu.

Đầu ra (trong results/tv4/):
  summary_runs.csv     mỗi lần chạy 1 dòng (số liệu thô để tra cứu)
  summary_configs.csv  mỗi cấu hình 1 dòng: trung bình ± độ lệch chuẩn qua các lần lặp
  summary.json         dữ liệu đã gộp theo thí nghiệm (plot_results.py dùng)
  summary_tables.md    bảng Markdown từng thí nghiệm (đưa vào báo cáo)

Chạy: python benchmark/summarize.py
"""
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

from run_benchmarks import experiments

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "results" / "tv4"
RAW = OUT / "raw"

METRICS = {  # tên cột → đường dẫn trong JSON
    "throughput_eps": ("producer", "throughput_eps"),
    "consume_eps": ("consumer", "consume_eps"),
    "raw_mb_per_sec": ("producer", "raw_mb_per_sec"),
    "wire_bytes_per_msg": ("producer", "wire_bytes_per_msg"),
    "compression_ratio": ("producer", "compression_ratio"),
    "ack_p50_ms": ("producer", "ack_latency_ms", "p50"),
    "ack_p95_ms": ("producer", "ack_latency_ms", "p95"),
    "ack_p99_ms": ("producer", "ack_latency_ms", "p99"),
    "e2e_p50_ms": ("consumer", "e2e_latency_ms", "p50"),
    "e2e_p95_ms": ("consumer", "e2e_latency_ms", "p95"),
    "e2e_p99_ms": ("consumer", "e2e_latency_ms", "p99"),
    "max_lag": ("consumer", "max_lag"),
    "drain_sec": ("consumer", "drain_after_producer_sec"),
    "host_cpu_pct": ("host_cpu_pct", "avg_while_producing"),
}
COUNTS = {
    "sent": ("producer", "sent"),
    "received_unique": ("consumer", "unique"),
    "lost_after_ack": ("consumer", "lost_after_ack"),
    "duplicates": ("consumer", "duplicates"),
    "send_failed": ("producer", "failed"),
}


def dig(d, path):
    for k in path:
        if d is None:
            return None
        d = d.get(k)
    return d


def vn(x, digits=0) -> str:
    """Định dạng số kiểu Việt Nam: 317.096 ; 1.605,3"""
    if x is None:
        return "–"
    s = f"{x:,.{digits}f}"
    return s.replace(",", "_").replace(".", ",").replace("_", ".")


def pm(a: dict, key: str, digits=0, scale=1.0) -> str:
    """Trung vị (min–max) qua các lần lặp – bền với 1 lần chạy bất thường."""
    v = a[key]
    if v["median"] is None:
        return "–"
    s = vn(v["median"] / scale, digits)
    if v["n"] > 1 and v["max"] != v["min"]:
        s += f" ({vn(v['min'] / scale, digits)}–{vn(v['max'] / scale, digits)})"
    return s


def load_runs() -> list[dict]:
    runs = []
    for p in sorted(RAW.glob("*.json")):
        if p.name.startswith("warmup_"):
            continue
        runs.append(json.loads(p.read_text(encoding="utf-8")))
    return runs


def aggregate(group: list[dict]) -> dict:
    a = {}
    for k, path in METRICS.items():
        vals = [v for v in (dig(r, path) for r in group) if v is not None]
        a[k] = {"mean": statistics.fmean(vals) if vals else None,
                "std": statistics.stdev(vals) if len(vals) > 1 else 0.0,
                "median": statistics.median(vals) if vals else None,
                "min": min(vals) if vals else None, "max": max(vals) if vals else None, "n": len(vals),
                "by_rep": {r["rep"]: dig(r, path) for r in group}}
    for k, path in COUNTS.items():
        a[k] = sum(dig(r, path) or 0 for r in group)
    a["runs"] = len(group)
    a["verified"] = all(dig(r, ("consumer", "verified")) is not False for r in group)
    a["timed_out"] = sum(bool(dig(r, ("consumer", "timed_out"))) for r in group)
    return a


def fault_metrics(r: dict) -> dict:
    f = r["fault"]
    tl = f["timeline"]
    kill = next((e for e in tl if "docker kill" in e.get("event", "")), None)
    start = next((e for e in tl if "docker start" in e.get("event", "")), None)
    killed_id = int(kill["event"].split("kafka-")[1]) if kill else None
    states = [e for e in tl if "brokers" in e]
    t_reelect = t_isr = None
    if kill:
        after = [e for e in states if e["t"] > kill["t"]]
        # leader mới: không còn partition nào thiếu leader hoặc do broker đã chết giữ
        t_reelect = next((e["t"] - kill["t"] for e in after
                          if e["no_leader"] == 0 and killed_id not in e["leaders"]), None)
    if start:
        t_isr = next((e["t"] - start["t"] for e in states if e["t"] > start["t"] and e["brokers"] == 3
                      and e["under_replicated"] == 0), None)
    # throughput phía consumer trước / trong sự cố (từ CSV từng giây)
    rows = list(csv.DictReader(open(ROOT / r["per_second_csv"], encoding="utf-8")))
    cons = {int(x["second"]): int(x["consumed"] or 0) for x in rows}
    k, s = int(f["kill_at"]), int(f["restart_at"])
    before = [cons.get(i, 0) for i in range(5, k)]
    during = [cons.get(i, 0) for i in range(k, s)]
    worst_p95 = max((float(x["e2e_p95_ms"]) for x in rows if x["e2e_p95_ms"]), default=None)
    return {
        "label": f["label"], "rep": r["rep"], "killed_broker": killed_id,
        "sent": r["producer"]["sent"], "acked": r["producer"]["acked"],
        "received_unique": r["consumer"]["unique"], "lost_after_ack": r["consumer"]["lost_after_ack"],
        "duplicates": r["consumer"]["duplicates"], "send_failed": r["producer"]["failed"],
        "errors_by_kind": r["producer"]["errors_by_kind"],
        "leader_reelection_sec": round(t_reelect, 1) if t_reelect is not None else None,
        "isr_recovered_after_restart_sec": round(t_isr, 1) if t_isr is not None else None,
        "consume_eps_before": round(statistics.fmean(before)) if before else None,
        "consume_eps_min_during": min(during) if during else None,
        # số giây consumer không nhận được bản ghi nào trong 20 giây sau khi broker bị tắt
        "consume_stall_sec": sum(1 for i in range(k, k + 20) if cons.get(i, 0) == 0),
        "catchup_max_eps": max((cons.get(i, 0) for i in range(k, k + 20)), default=None),
        "ack_p95_ms": r["producer"]["ack_latency_ms"]["p95"],
        "e2e_p95_ms_overall": r["consumer"]["e2e_latency_ms"]["p95"],
        "e2e_p99_ms_overall": r["consumer"]["e2e_latency_ms"]["p99"],
        "e2e_p95_ms_worst_second": worst_p95,
        "max_lag": r["consumer"]["max_lag"],
        "per_second_csv": r["per_second_csv"],
    }


def main():
    runs = load_runs()
    normal = [r for r in runs if not r.get("fault")]
    faults = [r for r in runs if r.get("fault")]

    # ---- từng lần chạy
    with open(OUT / "summary_runs.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["config_id", "rep", "experiments", *METRICS, *COUNTS])
        for r in sorted(runs, key=lambda r: (r["config_id"], r["rep"])):
            w.writerow([r["config_id"], r["rep"], ";".join(r["experiments"]),
                        *[dig(r, p) for p in METRICS.values()], *[dig(r, p) for p in COUNTS.values()]])

    # ---- gộp theo cấu hình
    by_cfg = defaultdict(list)
    for r in normal:
        by_cfg[r["config_id"]].append(r)
    agg = {cid: aggregate(g) for cid, g in by_cfg.items()}
    cfg_of = {cid: g[0]["config"] for cid, g in by_cfg.items()}
    with open(OUT / "summary_configs.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["config_id", "runs", *[f"{m}_{s}" for m in METRICS for s in ("mean", "std")], *COUNTS])
        for cid, a in sorted(agg.items()):
            w.writerow([cid, a["runs"], *[a[m][s] for m in METRICS for s in ("mean", "std")],
                        *[a[c] for c in COUNTS]])

    # ---- gộp theo thí nghiệm, đúng thứ tự định nghĩa
    defs = experiments(20000, [5000, 10000, 20000, 50000, 100000, 150000, 200000])
    members = defaultdict(dict)
    for cid, g in by_cfg.items():
        for tag in g[0]["experiments"]:
            exp, lbl = tag.split(":", 1)
            members[exp][lbl] = cid
    exps = {}
    for exp in list(defs) + [e for e in members if e not in defs]:
        if exp not in members:
            continue
        order = [lbl for lbl, _ in defs.get(exp, ("", []))[1]]
        labels = sorted(members[exp], key=lambda l: order.index(l) if l in order else 99)
        if exp == "E8_load_curve":  # theo tải tăng dần, "tối đa" ở cuối
            labels = sorted(labels, key=lambda l: cfg_of[members[exp][l]]["rate"] or 10**9)
        exps[exp] = {"description": defs.get(exp, (exp,))[0],
                     "rows": [{"label": l, "config_id": members[exp][l], "config": cfg_of[members[exp][l]],
                               **agg[members[exp][l]]} for l in labels]}

    fault_rows = [fault_metrics(r) for r in sorted(faults, key=lambda r: (r["fault"]["label"], r["rep"]))]
    soak = None
    soak_files = sorted((OUT / "soak").glob("soak_*.json")) if (OUT / "soak").exists() else []
    if soak_files:
        soak = json.loads(soak_files[-1].read_text(encoding="utf-8"))

    checked = [r for r in normal if r["consumer"].get("verified", True)]  # E1–E8 có kiểm chứng (không gồm E9)
    totals = {"runs": len(runs), "configs": len(agg), "verified_runs": len(checked),
              "sent": sum(r["producer"]["sent"] for r in checked),
              "received_unique": sum(r["consumer"]["unique"] for r in checked),
              "lost_after_ack": sum(r["consumer"]["lost_after_ack"] or 0 for r in checked),
              "duplicates": sum(r["consumer"]["duplicates"] for r in checked),
              "send_failed": sum(r["producer"]["failed"] for r in checked),
              "timed_out": sum(bool(r["consumer"]["timed_out"]) for r in checked)}
    (OUT / "summary.json").write_text(json.dumps({"totals": totals, "experiments": exps, "fault": fault_rows,
                                                  "soak": soak}, ensure_ascii=False, indent=2), encoding="utf-8")

    # ---- bảng Markdown
    md = ["# Bảng số liệu benchmark TV4 (tự sinh bởi benchmark/summarize.py)", "",
          f"Tổng: {totals['runs']} lần chạy, {totals['configs']} cấu hình; {totals['verified_runs']} lần có consumer kiểm chứng. "
          f"Trong các lần kiểm chứng: đã gửi {vn(totals['sent'])} bản ghi; "
          f"consumer nhận đủ {vn(totals['received_unique'])} bản ghi duy nhất; mất sau ack: {totals['lost_after_ack']}; "
          f"trùng: {totals['duplicates']}; consumer quá hạn: {totals['timed_out']}.", "",
          "Giá trị = trung vị (min–max) qua các lần lặp. eps = event/giây. "
          "ack = produce() → broker xác nhận; e2e = produce() → consumer nhận.", ""]
    for exp, e in exps.items():
        md += [f"## {exp}: {e['description']}", "",
               "| Cấu hình | Lần | Throughput producer (eps) | Throughput consumer (eps) | MB/s (gốc) | "
               "Byte/msg trên mạng | Tỉ lệ nén | ack p95 (ms) | e2e p50 (ms) | e2e p95 (ms) | e2e p99 (ms) | "
               "Lag tối đa | CPU máy (%) | Mất | Trùng |",
               "|---|:-:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|:-:|:-:|"]
        for r in e["rows"]:
            md.append(f"| {r['label']} | {r['runs']} | {pm(r, 'throughput_eps')} | {pm(r, 'consume_eps')} | "
                      f"{pm(r, 'raw_mb_per_sec', 1)} | {pm(r, 'wire_bytes_per_msg', 1)} | "
                      f"{pm(r, 'compression_ratio', 2)} | {pm(r, 'ack_p95_ms', 1)} | {pm(r, 'e2e_p50_ms', 1)} | "
                      f"{pm(r, 'e2e_p95_ms', 1)} | {pm(r, 'e2e_p99_ms', 1)} | {pm(r, 'max_lag')} | "
                      f"{pm(r, 'host_cpu_pct')} | "
                      + (f"{r['lost_after_ack']} | {r['duplicates']} |" if r["verified"] else "– | – |"))
        md.append("")
    if fault_rows:
        md += ["## E9_fault: Tắt đột ngột 1 broker (docker kill) giây 30, bật lại giây 60", "",
               "| acks | Lần | Broker bị tắt | Đã gửi | Broker xác nhận | Consumer nhận | Mất sau ack | Trùng | "
               "Lỗi gửi | Bầu lại leader (s) | ISR đủ lại sau khi bật (s) | eps trước sự cố | eps thấp nhất khi sự cố | "
               "e2e p95 (ms) | e2e p95 giây tệ nhất (ms) |",
               "|---|:-:|:-:|--:|--:|--:|:-:|:-:|:-:|--:|--:|--:|--:|--:|--:|"]
        for f in fault_rows:
            md.append(f"| {f['label']} | {f['rep']} | kafka-{f['killed_broker']} | {vn(f['sent'])} | {vn(f['acked'])} | "
                      f"{vn(f['received_unique'])} | {f['lost_after_ack']} | {f['duplicates']} | {f['send_failed']} | "
                      f"{vn(f['leader_reelection_sec'], 1)} | {vn(f['isr_recovered_after_restart_sec'], 1)} | "
                      f"{vn(f['consume_eps_before'])} | {vn(f['consume_eps_min_during'])} | "
                      f"{vn(f['e2e_p95_ms_overall'], 1)} | {vn(f['e2e_p95_ms_worst_second'], 1)} |")
        md.append("")
    if soak:
        c, s = soak["consumer"], soak["simulator"]
        md += [f"## Soak: chạy liên tục {soak['duration_sec'] // 60} phút (simulator TV3, profile daily)", "",
               "| Đã gửi | Consumer nhận | Mất | Trùng chèn có chủ đích | Trùng phát hiện | Lag tối đa | "
               "e2e p50 (ms) | e2e p95 (ms) | e2e p99 (ms) | ack p95 simulator (ms) |",
               "|--:|--:|:-:|--:|--:|--:|--:|--:|--:|--:|",
               f"| {vn(s['sent_total'])} | {vn(c['received'])} | {c['lost']} | {vn(c['duplicates_injected'])} | "
               f"{vn(c['duplicates_detected'])} | {vn(c['max_lag'])} | {vn(c['e2e_latency_ms']['p50'], 1)} | "
               f"{vn(c['e2e_latency_ms']['p95'], 1)} | {vn(c['e2e_latency_ms']['p99'], 1)} | "
               f"{vn((s.get('ack_latency_ms') or {}).get('p95'), 1)} |", ""]
    (OUT / "summary_tables.md").write_text("\n".join(md), encoding="utf-8")
    print(f"[V] {totals['runs']} lần chạy → {OUT / 'summary_tables.md'}")
    print(f"    gửi {vn(totals['sent'])} | nhận {vn(totals['received_unique'])} | mất {totals['lost_after_ack']} | "
          f"trùng {totals['duplicates']} | timeout {totals['timed_out']}")


if __name__ == "__main__":
    main()
