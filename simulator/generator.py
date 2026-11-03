"""Sinh event hành vi người dùng + giao dịch theo phiên (session) giống khách thật của tiệm bánh.

Một phiên đi theo phễu, có xác suất rời bỏ ở mỗi bước (~30% thoát ngay, ~10% phiên đặt hàng):
    page_view -> (search) -> product_view -> add_to_cart -> (remove_from_cart)
    -> checkout_start -> order_placed -> payment (thất bại thì có thể thử lại bằng phương thức khác)

- Khách: ~75% là thành viên `U…` (có nhóm khách quen quay lại nhiều, chốt đơn dễ hơn), ~25% khách vãng lai `A…`.
  Mỗi thành viên có hồ sơ cố định (cửa hàng gần nhà, hạng) – xem customer_profile().
- Sản phẩm được chọn theo khung giờ (sáng bánh mì/cà phê, tối bánh kem) đọc từ catalog.json.
- Nguồn truy cập (direct / google / facebook / tiktok / zalo / email) ghi vào URL trang đầu dạng utm;
  ô tìm kiếm là văn bản tự do (thêm hậu tố, bỏ dấu, gõ nhầm) và dẫn tới sản phẩm đúng danh mục.
- Chế độ realtime: event_time = lúc sinh. Chế độ backfill (truyền `start`): event_time = đầu phiên
  + thời gian suy nghĩ giữa các thao tác, dùng để sinh tập dữ liệu lịch sử.
- `dirty_rate` chèn dữ liệu bẩn, mỗi kiểu ứng với một lý do DLQ của Collector (xem EXPECTED_REASON).
"""
import copy
import json
import random
import unicodedata
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

CATALOG_PATH = Path(__file__).with_name("catalog.json")
VN_TZ = timezone(timedelta(hours=7))

CLICK_TYPES = ("page_view", "search", "product_view", "add_to_cart", "remove_from_cart", "checkout_start")

# kiểu dữ liệu bẩn -> lý do Collector phải trả về khi đưa vào bakery.dlq
EXPECTED_REASON = {
    "missing_field": "schema_violation",         # thiếu user_id
    "negative_price": "schema_violation",        # giá / tổng tiền âm
    "wrong_type": "schema_violation",            # quantity là chuỗi chữ
    "extra_field": "schema_violation",           # thêm trường lạ ngoài contract
    "empty_items": "schema_violation",           # đơn hàng không có sản phẩm
    "bad_time_format": "schema_violation",       # event_time kiểu dd/mm/yyyy
    "unknown_event_type": "unknown_event_type",  # gõ sai event_type
    "future_time": "event_time_in_future",       # đồng hồ client chạy nhanh 1 ngày
    "naive_time": "missing_timezone",            # event_time thiếu múi giờ
    "duplicate": "duplicate_event_id",           # client gửi lại cùng event_id
    "not_an_object": "not_an_object",            # phần tử không phải JSON object
}
DIRTY_KINDS = list(EXPECTED_REASON)


def load_catalog(path: Path = CATALOG_PATH) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFD", s.replace("đ", "d").replace("Đ", "D"))
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def day_part(hour: int) -> str:
    if 5 <= hour < 10:
        return "morning"
    if 10 <= hour < 14:
        return "noon"
    if 14 <= hour < 17:
        return "afternoon"
    if 17 <= hour < 24:
        return "evening"
    return "night"


class EventGenerator:
    def __init__(self, catalog: dict, seed: int | None = None, dirty_rate: float = 0.0,
                 late_rate: float = 0.0, n_users: int = 50_000, guest_rate: float = 0.25,
                 dirty_kinds: list[str] | None = None):
        self.c = catalog
        self.rng = random.Random(seed)
        self.dirty_rate = dirty_rate
        self.late_rate = late_rate
        self.n_users = n_users
        self.n_loyal = max(1, n_users // 20)
        self.guest_rate = guest_rate
        self.dirty_kinds = dirty_kinds or DIRTY_KINDS
        unknown = set(self.dirty_kinds) - set(EXPECTED_REASON)
        if unknown:
            raise ValueError(f"Kiểu dữ liệu bẩn không hỗ trợ: {sorted(unknown)}")
        self.pay_methods = list(catalog["payment_methods"])
        self.pay_weights = list(catalog["payment_methods"].values())
        self.pay_fail = catalog.get("payment_fail_rate", {})
        self.vouchers = catalog["vouchers"]
        self.products = catalog["products"]
        self.by_category: dict[str, list[dict]] = {}
        for p in self.products:
            self.by_category.setdefault(p["category"], []).append(p)
        self.search_terms = list(catalog["search_terms"].items())
        self.campaigns = catalog.get("campaigns") or [{"source": "direct", "weight": 1}]
        self.campaign_weights = [c["weight"] for c in self.campaigns]
        self._profiles: dict[int, dict] = {}
        # trọng số sản phẩm theo từng khung giờ, tính sẵn một lần
        boosts = catalog.get("category_boost", {})
        self._weights = {part: [boosts.get(part, {}).get(p["category"], 1.0) for p in self.products]
                         for part in ("morning", "noon", "afternoon", "evening", "night")}
        self.dirty_by_kind: Counter = Counter()
        self.late_injected = 0

    @property
    def dirty_injected(self) -> int:
        return sum(self.dirty_by_kind.values())

    # ---------- tiện ích ----------
    def _uuid(self) -> str:
        # sinh từ rng để cùng seed cho ra cùng tập dữ liệu; đặt bit version 4 / variant như uuid.UUID(version=4)
        # nhưng tự định dạng chuỗi cho nhanh (hàm này chạy cho mọi event)
        n = self.rng.getrandbits(128) & ~(0xC000 << 48) | (0x8000 << 48)
        h = f"{n & ~(0xF000 << 64) | (0x4000 << 64):032x}"
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"

    def _user_id(self) -> str:
        r = self.rng
        if r.random() < self.guest_rate:
            return f"A{r.getrandbits(40):010x}"
        # 5% thành viên là khách quen, chiếm 15% lượt truy cập của thành viên
        if r.random() < 0.15:
            return f"U{r.randint(1, self.n_loyal):07d}"
        return f"U{r.randint(1, self.n_users):07d}"

    def _product(self, part: str, category: str | None = None) -> dict:
        if category and category in self.by_category and self.rng.random() < 0.75:
            return self.rng.choice(self.by_category[category])  # xem sản phẩm đúng thứ vừa tìm
        return self.rng.choices(self.products, self._weights[part])[0]

    def customer_profile(self, idx: int) -> dict:
        """Hồ sơ cố định của thành viên U{idx}: không phụ thuộc seed, nên mọi worker và
        bảng dim/customers.csv cho cùng một kết quả."""
        prof = self._profiles.get(idx)
        if prof is None:
            r = random.Random(f"customer-{idx}")
            stores = self.c["stores"]
            home = r.choices(stores, [3 if s.startswith("HCM") else 2 for s in stores])[0]
            prof = {"user_id": f"U{idx:07d}", "tier": "loyal" if idx <= self.n_loyal else "member",
                    "home_store": home, "city": home.split("-")[0],
                    "member_since": (date(2023, 1, 1) + timedelta(days=r.randint(0, 1368))).isoformat()}
            self._profiles[idx] = prof
        return prof

    def _search_text(self) -> tuple[str, str]:
        """Văn bản tìm kiếm tự do (có hậu tố, không dấu, gõ sai) + danh mục khách thực sự muốn tìm."""
        r = self.rng
        term, category = r.choice(self.search_terms)
        q = term + r.choice(self.c.get("search_modifiers", [""]))
        if r.random() < 0.35:
            q = strip_accents(q)  # gõ không dấu
        if r.random() < 0.06 and len(q) > 3:
            i = r.randrange(len(q) - 1)
            q = q[:i] + q[i + 1] + q[i] + q[i + 2:]  # gõ nhầm 2 ký tự cạnh nhau
        if r.random() < 0.05:
            q = q.upper()
        return q, category

    def _landing_page(self, campaign: dict) -> str:
        page = campaign.get("landing") or self.rng.choice(self.c["pages"])
        if campaign["source"] == "direct":
            return page
        utm = f"utm_source={campaign['source']}&utm_medium={campaign.get('medium', 'referral')}"
        if campaign.get("campaign"):
            utm += f"&utm_campaign={campaign['campaign']}"
        return f"{page}?{utm}"

    def _browse_page(self) -> str:
        if self.rng.random() < 0.4:
            return f"/category/{self.rng.choice(list(self.by_category))}"
        return self.rng.choice(self.c["pages"])

    # ---------- phiên người dùng ----------
    def session(self, start: datetime | None = None, hour: int | None = None) -> list[dict]:
        """Sinh các event của một phiên.

        start: None = realtime (event_time là thời điểm hiện tại); có giá trị = backfill từ mốc đó.
        hour:  giờ trong ngày (0–23, giờ VN) dùng để chọn sản phẩm; mặc định lấy theo start/hiện tại.
        """
        r = self.rng
        if hour is None:
            hour = (start or datetime.now(timezone.utc)).astimezone(VN_TZ).hour
        part = day_part(hour)
        source = r.choices(["web", "app"], [0.45, 0.55])[0]
        ctx = {
            "user_id": self._user_id(),
            "session_id": r.getrandbits(64).to_bytes(8, "big").hex(),
            "source": source,
            "device": "mobile" if source == "app" else r.choices(["desktop", "mobile", "tablet"], [0.5, 0.4, 0.1])[0],
        }
        campaign = r.choices(self.campaigns, self.campaign_weights)[0]
        uid = ctx["user_id"]
        profile = self.customer_profile(int(uid[1:])) if uid[0] == "U" else None
        loyal = profile is not None and profile["tier"] == "loyal"
        clock = [start]  # thời điểm của thao tác gần nhất (chỉ dùng ở chế độ backfill)
        late = bool(self.late_rate) and r.random() < self.late_rate
        if late:
            # cả phiên bị gửi trễ (app offline rồi mới đồng bộ lại)
            self.late_injected += 1
            late_by = timedelta(hours=r.uniform(1.1, 6))

        def base(etype: str) -> dict:
            if start is None:
                t = datetime.now(timezone.utc)
            else:
                clock[0] += timedelta(seconds=r.expovariate(1 / 15))  # trung bình 15s giữa hai thao tác
                t = clock[0]
            if late:
                t -= late_by
            return {
                "event_id": self._uuid(),
                "event_type": etype,
                "event_time": iso(t),
                "user_id": ctx["user_id"],
                "session_id": ctx["session_id"],
                "source": ctx["source"],
                "device": ctx["device"],
                "schema_version": 1,
            }

        events = []
        e = base("page_view")
        e["page"] = self._landing_page(campaign)  # trang đích, kèm utm nếu đến từ chiến dịch
        events.append(e)

        # thoát ngay (bounce): chỉ xem trang chủ / khuyến mãi rồi rời đi
        if r.random() < 0.3:
            if r.random() < 0.3:
                e = base("page_view")
                e["page"] = self._browse_page()
                events.append(e)
            return self._maybe_dirty(events)

        cart: dict[str, dict] = {}
        # khách quen dễ chốt hơn; mỗi kênh có ý định mua khác nhau (tìm Google > lướt TikTok)
        add_prob = (0.35 if loyal else 0.25) * campaign.get("intent", 1.0)
        interest = campaign.get("interest")
        for _ in range(r.randint(1, 6)):
            if r.random() < 0.15:
                e = base("page_view")
                e["page"] = self._browse_page()
                events.append(e)
            wanted = None
            if r.random() < 0.3:
                e = base("search")
                e["search_query"], wanted = self._search_text()
                events.append(e)
            if wanted is None and interest and r.random() < 0.5:
                wanted = interest  # xem đúng sản phẩm được quảng cáo
            p = self._product(part, wanted)
            e = base("product_view")
            e.update(page=f"/product/{p['product_id']}", product_id=p["product_id"], category=p["category"],
                     price=float(p["price"]))
            events.append(e)
            if r.random() < add_prob:
                qty = r.choices([1, 2, 3, 4], [0.6, 0.25, 0.1, 0.05])[0]
                e = base("add_to_cart")
                e.update(product_id=p["product_id"], category=p["category"], price=float(p["price"]), quantity=qty)
                events.append(e)
                item = cart.setdefault(p["product_id"], {"product_id": p["product_id"], "category": p["category"],
                                                         "unit_price": float(p["price"]), "quantity": 0})
                item["quantity"] += qty

        if len(cart) > 1 and r.random() < 0.1:
            pid = r.choice(list(cart))
            item = cart.pop(pid)
            e = base("remove_from_cart")
            e.update(product_id=pid, category=item["category"], price=item["unit_price"], quantity=item["quantity"])
            events.append(e)

        # bỏ giỏ hàng
        if not cart or r.random() > 0.4:
            return self._maybe_dirty(events)

        events.append(base("checkout_start"))
        if r.random() > 0.7:
            return self._maybe_dirty(events)

        items = list(cart.values())
        subtotal = sum(i["unit_price"] * i["quantity"] for i in items)
        # khách đến từ chiến dịch có voucher thì phần lớn dùng voucher đó
        if campaign.get("voucher") and r.random() < 0.6:
            voucher = campaign["voucher"]
        else:
            voucher = r.choice(list(self.vouchers)) if r.random() < 0.1 else None
        total = float(round(subtotal * (1 - self.vouchers[voucher]))) if voucher else subtotal
        # thành viên thường mua ở cửa hàng gần nhà; bánh kem phần lớn giao tận nơi
        store = profile["home_store"] if profile and r.random() < 0.75 else r.choice(self.c["stores"])
        has_cake = any(i["category"] == "banh_kem" for i in items)
        delivery = "delivery" if r.random() < (0.65 if has_cake else 0.35) else "pickup"
        order_id = f"O{r.getrandbits(48):012X}"
        e = base("order_placed")
        e.update(order_id=order_id, store_id=store, items=items, total_amount=total,
                 voucher=voucher, delivery_type=delivery)
        events.append(e)

        method = r.choices(self.pay_methods, self.pay_weights)[0]
        for attempt in range(2):
            ok = r.random() >= self.pay_fail.get(method, 0.07)
            e = base("payment")
            e.update(order_id=order_id, payment_method=method, status="success" if ok else "failed", amount=total)
            events.append(e)
            # thanh toán lỗi: một nửa số khách thử lại bằng phương thức khác
            if ok or attempt == 1 or r.random() < 0.5:
                break
            method = r.choice([m for m in self.pay_methods if m != method])
        return self._maybe_dirty(events)

    # ---------- dữ liệu bẩn ----------
    def _corrupt(self, e: dict, kind: str):
        """Trả về bản hỏng của e theo kiểu kind."""
        if kind == "not_an_object":
            return json.dumps(e, ensure_ascii=False)  # gửi nhầm chuỗi JSON thay vì object
        bad = copy.deepcopy(e)
        if kind == "missing_field":
            bad.pop("user_id", None)
        elif kind == "negative_price":
            key = next((k for k in ("price", "total_amount", "amount") if k in bad), "price")
            bad[key] = -abs(bad.get(key) or 1000.0)
        elif kind == "wrong_type":
            bad["quantity"] = "hai"
        elif kind == "extra_field":
            bad["coupon_code"] = "HACK100"
        elif kind == "empty_items":
            if "items" in bad:
                bad["items"] = []
            else:  # event không phải đơn hàng: thiếu trường bắt buộc session_id
                bad.pop("session_id", None)
        elif kind == "bad_time_format":
            t = datetime.fromisoformat(bad["event_time"].replace("Z", "+00:00")).astimezone(VN_TZ)
            bad["event_time"] = t.strftime("%d/%m/%Y %H:%M")
        elif kind == "unknown_event_type":
            bad["event_type"] = self.rng.choice(["clik", "pageview", "add_cart"])
        elif kind == "future_time":
            t = datetime.fromisoformat(bad["event_time"].replace("Z", "+00:00"))
            bad["event_time"] = iso(t + timedelta(days=1))  # lệch theo chính event (không theo giờ máy) để tái lập được
        elif kind == "naive_time":
            bad["event_time"] = bad["event_time"].rstrip("Z")
        # duplicate: giữ nguyên nội dung, cùng event_id với bản gốc
        return bad

    def _maybe_dirty(self, events: list[dict]) -> list:
        if not self.dirty_rate:
            return events
        out = []
        for e in events:
            if self.rng.random() >= self.dirty_rate:
                out.append(e)
                continue
            kind = self.rng.choice(self.dirty_kinds)
            self.dirty_by_kind[kind] += 1
            if kind == "duplicate":
                out.append(e)  # bản gốc hợp lệ đi trước, bản trùng đi sau
            out.append(self._corrupt(e, kind))
        return out

    def stream(self):
        """Dòng event vô hạn ở chế độ realtime."""
        while True:
            yield from self.session()
