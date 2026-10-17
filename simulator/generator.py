"""Sinh event hành vi người dùng + giao dịch theo phiên (session) giống người thật.

Một phiên đi theo phễu: page_view -> search/product_view -> add_to_cart -> checkout_start
-> order_placed -> payment, với xác suất rời bỏ ở mỗi bước.
Có thể chèn dữ liệu bẩn (dirty_rate) để kiểm tra Veracity ở Collector.
"""
import copy
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

CATALOG_PATH = Path(__file__).with_name("catalog.json")

DIRTY_KINDS = [
    "missing_field",       # thiếu user_id
    "negative_price",      # giá âm
    "unknown_event_type",  # event_type lạ
    "future_time",         # đồng hồ client sai, event_time ở tương lai
    "naive_time",          # thiếu múi giờ
    "duplicate",           # gửi lại cùng event_id
]


def load_catalog(path: Path = CATALOG_PATH) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class EventGenerator:
    def __init__(self, catalog: dict, seed: int | None = None, dirty_rate: float = 0.0,
                 late_rate: float = 0.0, n_users: int = 200_000):
        self.c = catalog
        self.rng = random.Random(seed)
        self.dirty_rate = dirty_rate
        self.late_rate = late_rate
        self.n_users = n_users
        self.pay_methods = list(catalog["payment_methods"].keys())
        self.pay_weights = list(catalog["payment_methods"].values())
        self.dirty_injected = 0

    # ---------- phiên người dùng ----------
    def _base(self, etype: str, ctx: dict) -> dict:
        now = datetime.now(timezone.utc)
        if self.late_rate and self.rng.random() < self.late_rate:
            now -= timedelta(hours=self.rng.uniform(1.1, 6))  # event đến trễ (mất mạng, app offline)
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": etype,
            "event_time": iso(now),
            "user_id": ctx["user_id"],
            "session_id": ctx["session_id"],
            "source": ctx["source"],
            "device": ctx["device"],
            "schema_version": 1,
        }

    def _product(self) -> dict:
        return self.rng.choice(self.c["products"])

    def session(self) -> list[dict]:
        r = self.rng
        source = r.choices(["web", "app"], [0.45, 0.55])[0]
        ctx = {
            "user_id": f"U{r.randint(1, self.n_users):07d}",
            "session_id": uuid.uuid4().hex[:16],
            "source": source,
            "device": "mobile" if source == "app" else r.choices(["desktop", "mobile", "tablet"], [0.5, 0.4, 0.1])[0],
        }
        events = []

        e = self._base("page_view", ctx)
        e["page"] = r.choice(self.c["pages"])
        events.append(e)

        cart: dict[str, dict] = {}
        for _ in range(r.randint(1, 6)):
            if r.random() < 0.3:
                e = self._base("search", ctx)
                e["search_query"] = r.choice(self.c["search_terms"])
                events.append(e)
            p = self._product()
            e = self._base("product_view", ctx)
            e.update(product_id=p["product_id"], category=p["category"], price=p["price"])
            events.append(e)
            if r.random() < 0.35:
                qty = r.choices([1, 2, 3, 4], [0.6, 0.25, 0.1, 0.05])[0]
                e = self._base("add_to_cart", ctx)
                e.update(product_id=p["product_id"], category=p["category"], price=p["price"], quantity=qty)
                events.append(e)
                item = cart.setdefault(p["product_id"], {"product_id": p["product_id"], "category": p["category"],
                                                         "unit_price": p["price"], "quantity": 0})
                item["quantity"] += qty

        if cart and r.random() < 0.1:
            pid = r.choice(list(cart))
            item = cart.pop(pid)
            e = self._base("remove_from_cart", ctx)
            e.update(product_id=pid, category=item["category"], price=item["unit_price"], quantity=item["quantity"])
            events.append(e)

        if not cart or r.random() > 0.6:
            return self._maybe_dirty(events)

        events.append(self._base("checkout_start", ctx))
        if r.random() > 0.8:
            return self._maybe_dirty(events)

        items = list(cart.values())
        total = sum(i["unit_price"] * i["quantity"] for i in items)
        voucher = r.choice(self.c["vouchers"]) if r.random() < 0.2 else None
        if voucher:
            total = round(total * 0.9)
        order_id = f"O{uuid.uuid4().hex[:12].upper()}"
        e = self._base("order_placed", ctx)
        e.update(order_id=order_id, store_id=r.choice(self.c["stores"]), items=items, total_amount=total,
                 voucher=voucher, delivery_type=r.choice(["pickup", "delivery"]))
        events.append(e)

        e = self._base("payment", ctx)
        e.update(order_id=order_id, payment_method=r.choices(self.pay_methods, self.pay_weights)[0],
                 status="success" if r.random() < 0.95 else "failed", amount=total)
        events.append(e)
        return self._maybe_dirty(events)

    # ---------- dữ liệu bẩn ----------
    def _maybe_dirty(self, events: list[dict]) -> list[dict]:
        if not self.dirty_rate:
            return events
        out = []
        for e in events:
            if self.rng.random() >= self.dirty_rate:
                out.append(e)
                continue
            self.dirty_injected += 1
            kind = self.rng.choice(DIRTY_KINDS)
            bad = copy.deepcopy(e)
            if kind == "missing_field":
                bad.pop("user_id", None)
            elif kind == "negative_price":
                key = next((k for k in ("price", "total_amount", "amount") if k in bad), None)
                bad[key or "price"] = -abs(bad.get(key, 1000) or 1000)
            elif kind == "unknown_event_type":
                bad["event_type"] = "clik"
            elif kind == "future_time":
                bad["event_time"] = iso(datetime.now(timezone.utc) + timedelta(days=1))
            elif kind == "naive_time":
                bad["event_time"] = bad["event_time"].rstrip("Z")
            elif kind == "duplicate":
                out.append(e)  # bản gốc hợp lệ, bản sau trùng event_id
            out.append(bad)
        return out

    def stream(self):
        """Dòng event vô hạn."""
        while True:
            yield from self.session()
