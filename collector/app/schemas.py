"""Data contract của các event đi vào Kafka. Đây là NGUỒN CHUẨN của contract:
nhóm Storage đọc Kafka dựa trên các model dưới đây. Đổi schema thì phải báo nhóm Storage.

Luật kiểm tra khớp với simulator/profile_dataset.py (TV3) và danh sách lý do DLQ trong simulator/generator.py:
    not_an_object       phần tử trong lô không phải JSON object
    unknown_event_type  event_type không có trong EVENT_MODELS
    schema_violation    thiếu / thừa trường, sai kiểu, sai enum, tiền âm, đơn hàng rỗng, sai định dạng thời gian
    missing_timezone    event_time không có múi giờ (vd "2026-10-01T08:30:00")
    event_time_in_future event_time sớm hơn giờ server quá MAX_FUTURE_SKEW_SECONDS
    duplicate_event_id  (kiểm ở dedup.py) event_id đã nhận trước đó
"""
import uuid
from datetime import datetime, timedelta, timezone
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from pydantic_core import PydanticCustomError

from .config import MAX_FUTURE_SKEW_SECONDS, TOPIC_DLQ, TOPIC_ORDERS, TOPIC_PAYMENTS, TOPIC_USER_EVENTS

# ---------------------------------------------------------------- kiểu dùng chung
Id64 = Annotated[str, Field(min_length=1, max_length=64)]
# strict=True: không chấp nhận chuỗi "1000" hay True/False thay cho số
Price = Annotated[float, Field(strict=True, ge=0)]       # giá xem / giá trong giỏ: được bằng 0 (quà tặng)
Money = Annotated[float, Field(strict=True, gt=0)]       # tiền đơn hàng / thanh toán: phải dương
Quantity = Annotated[int, Field(strict=True, ge=1, le=100)]

PaymentMethod = Literal["cash", "card", "momo", "zalopay", "vnpay", "bank_transfer"]


class BaseEvent(BaseModel):
    """Trường chung của mọi event. extra="forbid": có trường lạ ngoài contract là lỗi."""
    model_config = ConfigDict(extra="forbid")

    event_id: uuid.UUID
    event_type: str
    event_time: datetime
    user_id: Id64
    session_id: Id64
    source: Literal["web", "app", "simulator"]
    device: Optional[Literal["desktop", "mobile", "tablet"]] = None
    schema_version: Annotated[int, Field(strict=True, ge=1)]

    @field_validator("event_time", mode="before")
    @classmethod
    def _check_event_time(cls, v):
        """Bắt buộc ISO 8601 có múi giờ và không ở tương lai.
        Dùng PydanticCustomError với mã riêng để main.py phân biệt được lý do DLQ."""
        if not isinstance(v, str):
            raise PydanticCustomError("time_format", "event_time phải là chuỗi ISO 8601")
        try:
            t = datetime.fromisoformat(v)  # Python 3.11+ hiểu được hậu tố "Z"
        except ValueError:
            raise PydanticCustomError("time_format", "event_time không đúng định dạng ISO 8601: {value}", {"value": v})
        if t.tzinfo is None:
            raise PydanticCustomError("missing_timezone", "event_time thiếu múi giờ (cần Z hoặc +07:00)")
        if t - datetime.now(timezone.utc) > timedelta(seconds=MAX_FUTURE_SKEW_SECONDS):
            raise PydanticCustomError("event_time_in_future",
                                      "event_time sớm hơn giờ server quá {sec} giây", {"sec": MAX_FUTURE_SKEW_SECONDS})
        return t


# ---------------------------------------------------------------- hành vi (clickstream)
class _ClickEvent(BaseEvent):
    """Các trường tuỳ chọn của nhóm hành vi; từng loại bên dưới bắt buộc thêm trường riêng."""
    page: Optional[str] = Field(None, max_length=2048)
    search_query: Optional[str] = Field(None, max_length=256)
    product_id: Optional[Id64] = None
    category: Optional[Id64] = None
    price: Optional[Price] = None
    quantity: Optional[Quantity] = None


class PageViewEvent(_ClickEvent):
    event_type: Literal["page_view"]
    page: str = Field(min_length=1, max_length=2048)


class SearchEvent(_ClickEvent):
    event_type: Literal["search"]
    search_query: str = Field(min_length=1, max_length=256)


class ProductViewEvent(_ClickEvent):
    event_type: Literal["product_view"]
    product_id: Id64
    category: Id64
    price: Price


class CartEvent(_ClickEvent):
    event_type: Literal["add_to_cart", "remove_from_cart"]
    product_id: Id64
    category: Id64
    price: Price
    quantity: Quantity


class CheckoutStartEvent(_ClickEvent):
    event_type: Literal["checkout_start"]


# ---------------------------------------------------------------- giao dịch
class OrderItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product_id: Id64
    category: Id64
    unit_price: Money
    quantity: Quantity


class OrderPlacedEvent(BaseEvent):
    event_type: Literal["order_placed"]
    order_id: Id64
    store_id: Id64
    items: list[OrderItem] = Field(min_length=1)
    total_amount: Money
    voucher: Optional[str] = Field(None, max_length=64)
    delivery_type: Literal["pickup", "delivery"]


class PaymentEvent(BaseEvent):
    event_type: Literal["payment"]
    payment_id: Optional[Id64] = None  # backend thật có mã giao dịch; simulator không gửi
    order_id: Id64
    payment_method: PaymentMethod
    status: Literal["success", "failed"]
    amount: Money


# ---------------------------------------------------------------- bảng tra
EVENT_MODELS: dict[str, type[BaseEvent]] = {
    "page_view": PageViewEvent,
    "search": SearchEvent,
    "product_view": ProductViewEvent,
    "add_to_cart": CartEvent,
    "remove_from_cart": CartEvent,
    "checkout_start": CheckoutStartEvent,
    "order_placed": OrderPlacedEvent,
    "payment": PaymentEvent,
}

# event_type -> topic đích
ROUTES: dict[str, str] = {t: TOPIC_USER_EVENTS for t in EVENT_MODELS}
ROUTES.update({"order_placed": TOPIC_ORDERS, "payment": TOPIC_PAYMENTS})

REQUIRED_TOPICS = sorted({*ROUTES.values(), TOPIC_DLQ})

# mã lỗi riêng trong validator -> lý do DLQ (chỉ dùng khi đó là lỗi DUY NHẤT của event)
_CUSTOM_REASONS = {"missing_timezone", "event_time_in_future"}


def validate_event(raw) -> tuple[Optional[BaseEvent], Optional[str], Optional[str]]:
    """Kiểm một event. Trả về (model, None, None) nếu hợp lệ, hoặc (None, lý do DLQ, chi tiết)."""
    if not isinstance(raw, dict):
        return None, "not_an_object", f"phần tử kiểu {type(raw).__name__}, cần JSON object"
    etype = raw.get("event_type")
    model = EVENT_MODELS.get(etype) if isinstance(etype, str) else None
    if model is None:
        return None, "unknown_event_type", f"event_type={etype!r} không có trong contract"
    try:
        return model.model_validate(raw), None, None
    except ValidationError as e:
        errors = e.errors(include_url=False)
        types = {err["type"] for err in errors}
        # vừa thiếu múi giờ vừa sai trường khác -> tính là schema_violation (giống profile_dataset.py)
        reason = types.pop() if len(types) == 1 and types <= _CUSTOM_REASONS else "schema_violation"
        detail = "; ".join(f"{'.'.join(str(p) for p in err['loc']) or '-'}: {err['msg']}" for err in errors[:5])
        return None, reason, detail[:500]
