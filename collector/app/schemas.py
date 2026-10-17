"""Data contract của tầng Ingestion (schema_version = 1).

Mỗi event_type thuộc đúng một nhóm và được ghi vào một topic:
    clickstream : page_view, search, product_view, add_to_cart, remove_from_cart, checkout_start
    orders      : order_placed
    payments    : payment
Chi tiết trường dữ liệu xem docs/data_contract.md.
"""
from datetime import datetime
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from . import config

CLICK_TYPES = ("page_view", "search", "product_view", "add_to_cart", "remove_from_cart", "checkout_start")


class BaseEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    event_type: str
    event_time: datetime  # ISO-8601, phải có múi giờ, ví dụ 2026-10-17T08:30:00.123Z
    user_id: str = Field(min_length=1, max_length=64)
    session_id: str = Field(min_length=1, max_length=64)
    source: Literal["web", "app", "simulator"]
    device: Optional[Literal["desktop", "mobile", "tablet"]] = None
    schema_version: Literal[1] = 1


class ClickEvent(BaseEvent):
    event_type: Literal[CLICK_TYPES]  # type: ignore[valid-type]
    page: Optional[str] = Field(default=None, max_length=256)
    search_query: Optional[str] = Field(default=None, max_length=256)
    product_id: Optional[str] = None
    category: Optional[str] = None
    price: Optional[float] = Field(default=None, ge=0)
    quantity: Optional[int] = Field(default=None, ge=1, le=100)


class OrderItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product_id: str
    category: str
    unit_price: float = Field(gt=0)
    quantity: int = Field(ge=1, le=100)


class OrderEvent(BaseEvent):
    event_type: Literal["order_placed"]
    order_id: str
    store_id: str
    items: list[OrderItem] = Field(min_length=1)
    total_amount: float = Field(gt=0)
    voucher: Optional[str] = None
    delivery_type: Literal["pickup", "delivery"]


class PaymentEvent(BaseEvent):
    event_type: Literal["payment"]
    order_id: str
    payment_method: Literal["cash", "card", "momo", "zalopay", "vnpay", "bank_transfer"]
    status: Literal["success", "failed"]
    amount: float = Field(gt=0)


# event_type -> (model, topic)
ROUTES: dict[str, tuple[type[BaseEvent], str]] = {
    **{t: (ClickEvent, config.TOPIC_CLICKSTREAM) for t in CLICK_TYPES},
    "order_placed": (OrderEvent, config.TOPIC_ORDERS),
    "payment": (PaymentEvent, config.TOPIC_PAYMENTS),
}
