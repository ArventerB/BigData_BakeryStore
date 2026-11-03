"""Tập dữ liệu sinh ra phải thể hiện được 5V – kiểm bằng chính profile_dataset.py."""
import csv
from collections import Counter

import pytest

import profile_dataset
import simulator
from generator import EventGenerator, load_catalog, strip_accents

CATALOG = load_catalog()


@pytest.fixture(scope="module")
def report(tmp_path_factory):
    root = tmp_path_factory.mktemp("ds")
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(simulator, "RESULTS_DIR", root / "results")
        simulator.main(["--mode", "file", "--start", "2026-10-01", "--days", "3", "--sessions-per-day", "3000",
                        "--workers", "3", "--dirty-rate", "0.03", "--late-rate", "0.01", "--users", "5000",
                        "--seed", "5", "--gzip", "--out", str(root / "data")])
    return profile_dataset.main([str(root / "data"), "--workers", "2", "--out", str(root / "results")]), root


def test_volume_counts_match_manifest(report):
    r, root = report
    assert r["volume"]["events"] > 40_000
    assert r["volume"]["compression_ratio"] > 3
    with open(root / "data" / "dim" / "customers.csv", encoding="utf-8") as f:
        assert sum(1 for _ in csv.DictReader(f)) == 5000


def test_velocity_has_daily_peaks(report):
    v = report[0]["velocity"]
    assert v["peak_hour_vn"] in (7, 8, 12, 18, 19, 20)
    assert v["peak_to_quiet_ratio"] > 10
    assert v["late_or_out_of_order_events"] > 0


def test_variety_formats_schemas_and_text(report):
    va = report[0]["variety"]
    assert len(va["event_types"]) == 8 and va["distinct_schemas"] >= 8
    assert any("CSV" in f for f in va["file_formats"]) and any("JSON Lines" in f for f in va["file_formats"])
    assert set(va["traffic_channels"]) == {c["source"] for c in CATALOG["campaigns"]}
    assert va["distinct_search_queries"] > 200
    assert 20 < va["search_queries_without_accents_pct"] < 70


def test_veracity_profiler_finds_exactly_what_was_injected(report):
    ve = report[0]["veracity"]
    assert ve["invalid_events"] > 0
    assert ve["detection_matches_injection"], (ve["detected_by_reason"], ve["injected_by_reason"])


def test_value_signals_are_discoverable(report):
    vl = report[0]["value"]
    funnel = list(vl["funnel_sessions"].values())
    assert funnel[1:] == sorted(funnel[1:], reverse=True)  # phễu giảm dần
    assert 5 < vl["funnel_pct_of_sessions"]["payment_success"] < 20
    conv = vl["conversion_by_channel_pct"]
    assert conv["google"] > conv["tiktok"]  # tìm kiếm có ý định mua cao hơn lướt video
    fail = vl["payment_fail_rate_pct"]
    assert fail["card"] > fail["cash"]
    tier = vl["orders_per_buyer_by_tier"]
    assert tier["loyal"] > tier["member"]
    assert vl["revenue_by_category"][0][0] == "banh_kem"


def test_search_text_and_landing_pages():
    gen = EventGenerator(CATALOG, seed=2)
    queries, pages = Counter(), Counter()
    for _ in range(3000):
        for e in gen.session():
            if e["event_type"] == "search":
                queries[e["search_query"]] += 1
            if e.get("page"):
                assert len(e["page"]) <= 256
                pages[e["page"].split("?")[0].split("/")[1]] += 1
                if "?" in e["page"]:
                    assert "utm_source=" in e["page"]
    assert any(q == strip_accents(q) and q != strip_accents(q).upper() for q in queries)
    assert {"product", "category", "promotions"} <= set(pages)


def test_customer_profile_is_stable_and_drives_store():
    a, b = EventGenerator(CATALOG, seed=1), EventGenerator(CATALOG, seed=99)
    assert a.customer_profile(123) == b.customer_profile(123)
    home, at_home = Counter(), 0
    for _ in range(4000):
        for e in a.session():
            if e["event_type"] == "order_placed" and e["user_id"][0] == "U":
                home[e["user_id"]] += 1
                at_home += e["store_id"] == a.customer_profile(int(e["user_id"][1:]))["home_store"]
    assert at_home / sum(home.values()) > 0.7


def test_strip_accents():
    assert strip_accents("Bánh mì đặc biệt") == "Banh mi dac biet"
