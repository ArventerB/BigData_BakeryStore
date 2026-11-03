"""Kiểm tra event sinh ra đúng data contract (docs/data_contract.md) và đúng logic nghiệp vụ."""
import json
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

import pytest

from generator import CLICK_TYPES, DIRTY_KINDS, VN_TZ, EventGenerator, load_catalog

CATALOG = load_catalog()
COMMON = {"event_id", "event_type", "event_time", "user_id", "session_id", "source", "device", "schema_version"}
EXTRA = {
    **{t: {"page", "search_query", "product_id", "category", "price", "quantity"} for t in CLICK_TYPES},
    "order_placed": {"order_id", "store_id", "items", "total_amount", "voucher", "delivery_type"},
    "payment": {"order_id", "payment_method", "status", "amount"},
}


def parse_time(s: str) -> datetime:
    t = datetime.fromisoformat(s.replace("Z", "+00:00"))
    assert t.tzinfo is not None
    return t


def sessions(n=2000, **kw):
    gen = EventGenerator(CATALOG, seed=7, **kw)
    return gen, [gen.session() for _ in range(n)]


def test_clean_events_follow_contract():
    _, all_sessions = sessions()
    for events in all_sessions:
        for e in events:
            assert set(e) <= COMMON | EXTRA[e["event_type"]], e
            assert COMMON - {"device"} <= set(e)
            uuid.UUID(e["event_id"])
            parse_time(e["event_time"])
            assert e["source"] in ("web", "app") and e["device"] in ("desktop", "mobile", "tablet")
            assert 1 <= len(e["user_id"]) <= 64 and e["user_id"][0] in "UA"
            if e.get("price") is not None:
                assert e["price"] >= 0
            if e.get("quantity") is not None:
                assert 1 <= e["quantity"] <= 100


def test_session_funnel_is_consistent():
    _, all_sessions = sessions()
    orders = 0
    for events in all_sessions:
        assert events[0]["event_type"] == "page_view"
        assert len({(e["user_id"], e["session_id"]) for e in events}) == 1
        types = [e["event_type"] for e in events]
        if "order_placed" not in types:
            assert "payment" not in types
            continue
        orders += 1
        assert types.index("checkout_start") < types.index("order_placed") < types.index("payment")
        order = events[types.index("order_placed")]
        subtotal = sum(i["unit_price"] * i["quantity"] for i in order["items"])
        discount = CATALOG["vouchers"][order["voucher"]] if order["voucher"] else 0
        assert order["total_amount"] == pytest.approx(subtotal * (1 - discount), abs=1)
        pays = [e for e in events if e["event_type"] == "payment"]
        assert all(p["order_id"] == order["order_id"] and p["amount"] == order["total_amount"] for p in pays)
        assert all(p["status"] == "failed" for p in pays[:-1])  # chỉ thử lại sau khi thất bại
    # tỷ lệ chuyển đổi hợp lý cho web bán lẻ
    assert 0.05 < orders / len(all_sessions) < 0.2


def test_same_seed_gives_same_data():
    a = EventGenerator(CATALOG, seed=1, dirty_rate=0.05)
    b = EventGenerator(CATALOG, seed=1, dirty_rate=0.05)
    start = datetime(2026, 10, 1, 8, tzinfo=VN_TZ)
    assert [a.session(start=start) for _ in range(50)] == [b.session(start=start) for _ in range(50)]


def test_backfill_time_moves_forward():
    gen = EventGenerator(CATALOG, seed=3)
    start = datetime(2026, 10, 1, 19, tzinfo=VN_TZ)
    for _ in range(200):
        times = [parse_time(e["event_time"]) for e in gen.session(start=start)]
        assert times == sorted(times)
        assert start <= times[0] and times[-1] < start + timedelta(hours=2)


def test_products_follow_time_of_day():
    gen = EventGenerator(CATALOG, seed=5)

    def categories(hour):
        c = Counter()
        for _ in range(1500):
            c.update(e["category"] for e in gen.session(hour=hour) if e["event_type"] == "product_view")
        return c

    morning, evening = categories(7), categories(19)
    assert morning["banh_mi"] > morning["banh_kem"]
    assert evening["banh_kem"] > evening["banh_mi"]


def test_late_sessions_are_shifted_back():
    gen = EventGenerator(CATALOG, seed=9, late_rate=1.0)
    now = datetime.now(timezone.utc)
    for e in gen.session():
        assert now - parse_time(e["event_time"]) > timedelta(hours=1)


@pytest.mark.parametrize("kind", DIRTY_KINDS)
def test_each_dirty_kind_breaks_the_contract(kind):
    gen = EventGenerator(CATALOG, seed=11)
    # lấy một phiên sạch có đặt hàng (để mọi kiểu bẩn đều áp dụng được) rồi làm hỏng toàn bộ
    clean = next(s for s in iter(gen.session, None) if any(e["event_type"] == "order_placed" for e in s))
    gen.dirty_rate, gen.dirty_kinds = 1.0, [kind]
    events = gen._maybe_dirty(clean)
    if kind == "duplicate":
        ids = [e["event_id"] for e in events]
        assert len(ids) == 2 * len(set(ids))
        return
    for bad in events:
        if kind == "not_an_object":
            assert isinstance(bad, str) and isinstance(json.loads(bad), dict)
            continue
        assert isinstance(bad, dict)
        if kind == "missing_field":
            assert "user_id" not in bad
        elif kind == "negative_price":
            assert any((bad.get(k) or 0) < 0 for k in ("price", "total_amount", "amount"))
        elif kind == "wrong_type":
            assert isinstance(bad["quantity"], str)
        elif kind == "extra_field":
            assert set(bad) - COMMON - EXTRA.get(bad["event_type"], set())
        elif kind == "empty_items":
            assert bad.get("items") == [] or "session_id" not in bad
        elif kind == "bad_time_format":
            with pytest.raises(ValueError):
                datetime.fromisoformat(bad["event_time"])
        elif kind == "unknown_event_type":
            assert bad["event_type"] not in EXTRA
        elif kind == "future_time":
            assert parse_time(bad["event_time"]) > datetime.now(timezone.utc) + timedelta(hours=12)
        elif kind == "naive_time":
            assert datetime.fromisoformat(bad["event_time"]).tzinfo is None


def test_dirty_rate_is_respected():
    gen, all_sessions = sessions(3000, dirty_rate=0.05)
    n = sum(len(s) for s in all_sessions) - gen.dirty_by_kind["duplicate"]  # bản trùng là event thêm vào
    assert gen.dirty_injected / n == pytest.approx(0.05, abs=0.01)
    assert set(gen.dirty_by_kind) == set(DIRTY_KINDS)


def test_unknown_dirty_kind_rejected():
    with pytest.raises(ValueError):
        EventGenerator(CATALOG, dirty_kinds=["khong_co"])


def test_fast_uuid_is_valid_v4():
    gen = EventGenerator(CATALOG, seed=8)
    for _ in range(2000):
        u = uuid.UUID(gen._uuid())
        assert u.version == 4 and u.variant == uuid.RFC_4122


def test_member_pool_scales_with_dataset_size():
    import simulator
    assert simulator.parse_args(["--mode", "file", "--sessions-per-day", "100000"]).users == 562_500
    assert simulator.parse_args(["--mode", "file", "--sessions-per-day", "1000"]).users == 50_000
    assert simulator.parse_args(["--mode", "api"]).users == 50_000
