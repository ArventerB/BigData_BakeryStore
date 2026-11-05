# Collector API (TV2)

Cửa nhận event duy nhất của hệ thống. Web (`web-tracking/tracker.js`), backend đơn hàng/thanh toán và
simulator (`--mode api`) đều gửi vào `POST /events`. Collector kiểm tra data contract, chống trùng, rồi đẩy
vào Kafka. Event lỗi không bị vứt đi mà vào `bakery.dlq` kèm lý do.

```
client ──POST /events──> validate (schemas.py) ──> dedup (dedup.py) ──> Kafka (producer.py)
                             │ lỗi                     │ trùng
                             └────────────> bakery.dlq <┘
```

## Chạy

```bash
# Cách 1: cùng cụm Kafka (trong thư mục scripts/)
docker compose up -d --build
#   máy có web The Sweets chạy cùng (mobile_backend chiếm cổng 8000): đặt COLLECTOR_PORT=8010 trong scripts/.env

# Cách 2: chạy trên máy host để sửa code cho nhanh
cd collector
pip install -r requirements.txt
python -m app.main                     # cổng lấy từ PORT, mặc định 8000

# Kiểm thử (Collector + Kafka đang chạy)
python collector/test_collector.py --kafka [--url http://localhost:8010]
```

Swagger: `http://localhost:8000/docs`. Cấu hình qua biến môi trường, xem `.env.example`.

## Endpoint

| Endpoint | Ý nghĩa |
|---|---|
| `POST /events` | Body là 1 object hoặc 1 mảng (tối đa 5000). Trả `200 {"received","accepted","rejected"}` sau khi Kafka đã xác nhận cả lô. Kafka không nhận được thì trả `503` để client gửi lại. Body không phải JSON thì trả `400`. |
| `GET /health` | `200` nếu kết nối được Kafka và đủ 4 topic, ngược lại `503`. |
| `GET /metrics` | Đếm từ lúc khởi động: `accepted_by_topic`, `accepted_by_event_type`, `rejected_by_reason`, `failed_requests_total`. |

## Event hợp lệ

- Định tuyến: `order_placed` vào `bakery.orders`, `payment` vào `bakery.payments`, các loại hành vi còn lại
  (`page_view`, `search`, `product_view`, `add_to_cart`, `remove_from_cart`, `checkout_start`) vào `bakery.user-events`.
- Key là `user_id`, nên event của một khách nằm cùng partition và giữ đúng thứ tự.
- Collector thêm 2 trường: `ingest_time` (giờ server nhận, UTC) và `is_late` (`event_time` cũ hơn 1 giờ).
- Producer theo chuẩn của TV1 (`scripts/producer_config.py`): orders, payments và dlq dùng `acks=all` có
  idempotence; user-events dùng `acks=1`, batch 128 KB, `linger.ms=20`. Tất cả nén lz4.

## Message trong DLQ

```json
{"reason": "schema_violation", "detail": "price: Input should be greater than or equal to 0",
 "ingest_time": "2026-11-05T17:15:54.100Z", "raw": { ...event gốc... }}
```

| `reason` | Khi nào |
|---|---|
| `not_an_object` | phần tử trong lô không phải JSON object |
| `unknown_event_type` | `event_type` không có trong contract |
| `schema_violation` | thiếu hoặc thừa trường, sai kiểu, sai enum, tiền âm, đơn hàng rỗng, `event_time` sai định dạng |
| `missing_timezone` | `event_time` không có múi giờ |
| `event_time_in_future` | `event_time` sớm hơn giờ server quá 5 phút |
| `duplicate_event_id` | `event_id` đã nhận trong 50.000 id gần nhất |

Tên trường `reason` và `raw` phải giữ nguyên, vì `benchmark/verify_consumer.py` (TV4) đếm DLQ theo `reason`.

## Kết quả đã kiểm (05/11/2026, cụm 3 broker trên Docker Desktop)

- `test_collector.py --kafka`: 18 trường hợp đơn lẻ và 1 lô hỗn hợp đều đúng, cả ở phản hồi API, `/metrics` lẫn khi đọc lại Kafka.
- `simulator.py --mode api --rate 1000 --duration 30 --dirty-rate 0.03`: gửi 29.962 event (khoảng 997 event/s),
  bị từ chối 911, đúng bằng số bẩn đã chèn (911). Từng `reason` khớp với `dirty_by_kind`. p95 khứ hồi một lô là 124 ms.
- Tắt `kafka-2` 15 giây giữa lúc chạy 800 event/s: gửi 31.983 event, 0 lỗi gửi, 0 request bị trả 503.

## Giới hạn cần biết

- Bộ nhớ chống trùng nằm trong tiến trình, nên chỉ chạy 1 worker. Restart Collector thì mất bộ nhớ này.
- Nếu client gửi lại cả lô sau khi nhận 503, những event trong lô đã vào Kafka từ lần trước sẽ bị
  coi là trùng và vào DLQ. Topic chính vẫn không có bản trùng.
