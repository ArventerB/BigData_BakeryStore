# Data contract – tầng Ingestion (schema_version = 1)

Tài liệu này là **giao diện bàn giao** giữa nhóm Sources → Ingestion và nhóm Ingestion → Storage.
Nguồn sự thật trong code: [`collector/app/schemas.py`](../collector/app/schemas.py).

## 1. Topic

| Topic | Nội dung | Partition | Replication | min.insync | Retention | Key |
|---|---|---|---|---|---|---|
| `shop.clickstream` | hành vi duyệt web/app | 6 | 3 | 2 | 7 ngày | `user_id` |
| `shop.orders` | đơn hàng | 6 | 3 | 2 | 7 ngày | `user_id` |
| `shop.payments` | thanh toán | 6 | 3 | 2 | 7 ngày | `user_id` |
| `shop.dlq` | bản ghi lỗi (dead-letter) | 3 | 3 | 2 | 14 ngày | `user_id` hoặc `unknown` |
| `shop.bench.p{1,3,6}` | chỉ dùng cho thí nghiệm | 1/3/6 | 3 | 2 | 1 ngày | — |

- Định dạng value: **JSON UTF-8**, mỗi message là một event.
- Key = `user_id` nên mọi event của cùng một người nằm cùng partition, giữ đúng thứ tự.
- Producer dùng `acks=all` + `enable.idempotence=true`: không mất và không trùng do retry.

## 2. Trường chung (mọi event)

| Trường | Kiểu | Bắt buộc | Mô tả |
|---|---|---|---|
| `event_id` | UUID string | ✔ | định danh duy nhất, dùng để khử trùng |
| `event_type` | string | ✔ | xem mục 3 |
| `event_time` | ISO-8601 có múi giờ | ✔ | thời điểm phát sinh tại client |
| `user_id` | string (1–64) | ✔ | id khách, khách vãng lai dạng `A…` |
| `session_id` | string (1–64) | ✔ | phiên truy cập |
| `source` | `web` \| `app` \| `simulator` | ✔ | kênh phát sinh |
| `device` | `desktop` \| `mobile` \| `tablet` | | |
| `schema_version` | int = 1 | ✔ | |
| `ingest_time` | ISO-8601 UTC | (Collector thêm) | thời điểm Collector nhận |
| `is_late` | bool | (Collector thêm) | `true` nếu đến trễ > 1 giờ |

## 3. Trường riêng

**clickstream** – `page_view`, `search`, `product_view`, `add_to_cart`, `remove_from_cart`, `checkout_start`

`page`, `search_query`, `product_id`, `category`, `price` (≥ 0), `quantity` (1–100) — tuỳ loại event.

**orders** – `order_placed`

`order_id`, `store_id`, `items[]` (`product_id`, `category`, `unit_price` > 0, `quantity`), `total_amount` > 0,
`voucher` (nullable), `delivery_type` (`pickup` \| `delivery`).

**payments** – `payment`

`order_id`, `payment_method` (`cash`, `card`, `momo`, `zalopay`, `vnpay`, `bank_transfer`),
`status` (`success` \| `failed`), `amount` > 0.

## 4. Quy tắc chất lượng (Veracity) → `shop.dlq`

| `reason` | Điều kiện |
|---|---|
| `not_an_object` | phần tử không phải JSON object |
| `unknown_event_type` | `event_type` không thuộc danh sách |
| `schema_violation` | thiếu trường, sai kiểu, giá trị âm, thừa trường lạ |
| `missing_timezone` | `event_time` không có múi giờ |
| `event_time_in_future` | `event_time` lệch tương lai > 5 phút |
| `duplicate_event_id` | `event_id` đã thấy gần đây (cache 200k id / tiến trình) |

Bản ghi DLQ: `{"reason", "detail", "ingest_time", "raw": <bản ghi gốc>}`.

## 5. Ví dụ

```json
{"event_id":"3f1c0b8e-2a51-4c1e-9d0a-6b8f6f0e2d11","event_type":"add_to_cart",
 "event_time":"2026-10-17T08:30:12.345Z","user_id":"U0012345","session_id":"9a8b7c6d5e4f3a2b",
 "source":"app","device":"mobile","schema_version":1,
 "product_id":"BK001","category":"banh_kem","price":280000.0,"quantity":1,
 "ingest_time":"2026-10-17T08:30:12.401+00:00","is_late":false}
```

```json
{"event_id":"b2e0...","event_type":"order_placed","event_time":"2026-10-17T08:31:02.100Z",
 "user_id":"U0012345","session_id":"9a8b7c6d5e4f3a2b","source":"app","device":"mobile","schema_version":1,
 "order_id":"O4F2A9C1B7E3D","store_id":"HCM-Q1",
 "items":[{"product_id":"BK001","category":"banh_kem","unit_price":280000.0,"quantity":1}],
 "total_amount":280000.0,"voucher":null,"delivery_type":"pickup",
 "ingest_time":"2026-10-17T08:31:02.150+00:00","is_late":false}
```
