# Simulator – bộ sinh dữ liệu tiệm bánh (TV3)

Sinh event hành vi người dùng và giao dịch giống khách thật, rồi đẩy vào tầng Ingestion.
Event tuân theo data contract `schema_version = 1` (bản mẫu: `docs/data_contract.md` ở nhánh `mau`).
Kafka theo hạ tầng của TV1 trong [`scripts/`](../scripts/README.md): 3 broker `localhost:9092,9094,9096`,
topic `bakery.user-events | bakery.orders | bakery.payments | bakery.dlq`.
Đặc trưng 5V của dữ liệu và số liệu đo: [`docs/tv3_simulator.md`](../docs/tv3_simulator.md).

| File | Nội dung |
|---|---|
| `catalog.json` | sản phẩm, cửa hàng, voucher, chiến dịch (utm), từ khoá tìm kiếm, phương thức thanh toán, lưu lượng theo giờ |
| `generator.py` | sinh event theo phiên (phễu mua hàng), chèn dữ liệu bẩn, event đến trễ |
| `traffic.py` | kịch bản lưu lượng: `constant`, `peak`, `daily`, `flash` |
| `kafka_config.py` | tên topic + cấu hình producer, đọc từ `scripts/producer_config.py` của TV1 |
| `simulator.py` | CLI: gửi qua Collector API, ghi thẳng Kafka, hoặc sinh tập dữ liệu ra file |
| `profile_dataset.py` | đo tập dữ liệu theo 5V, xuất báo cáo `results/profile_*.md` |
| `tests/` | kiểm thử contract, phễu, dữ liệu bẩn, kịch bản lưu lượng, topic của TV1, 5V |

## Cài đặt

```bash
pip install -r simulator/requirements.txt
python -m pytest simulator/tests        # cần pip install pytest
```

Chạy lệnh ở thư mục gốc repo. Kết quả ghi vào `results/` (file `.json` tổng kết, `.csv` theo chu kỳ nếu có `--stats-csv`).
Dừng sớm bằng Ctrl+C: dữ liệu trong bộ đệm vẫn được gửi nốt và file kết quả vẫn được ghi.

## Ba chế độ

| `--mode` | Đi đường nào | Dùng khi |
|---|---|---|
| `api` (mặc định) | Simulator → Collector `/events` → Kafka / DLQ | demo, kiểm tra Veracity, chạy dài |
| `kafka` | Simulator → Kafka (bỏ qua Collector) | đo giới hạn Kafka, thí nghiệm partition/producer |
| `file` | Simulator → `data/dataset_*/date=YYYY-MM-DD/events.jsonl[.gz]` + `dim/*.csv` | sinh tập dữ liệu lịch sử lớn, bàn giao nhóm Storage |

Chế độ `kafka` mặc định dùng đúng 2 bộ cấu hình producer của TV1: `bakery.user-events` dùng
`acks=1, batch 128 KB, linger 20 ms`; `bakery.orders / payments / dlq` dùng `acks=all, retries=5, batch 64 KB,
linger 10 ms` (bật idempotence để giữ thứ tự khi retry, thay cho `max_in_flight=1`). Cả hai đều nén `lz4`.
Các tham số `--acks / --linger-ms / --batch-size / --compression` sẽ ghi đè cấu hình này cho mọi topic.

## Kịch bản dùng trong đồ án

```bash
# Mốc 1 – chạy thử end-to-end
python simulator/simulator.py --mode api --rate 500 --duration 60

# Dữ liệu bẩn 3%: số "rejected_by_collector" phải bằng "dirty_injected"
python simulator/simulator.py --mode api --rate 1000 --duration 60 --dirty-rate 0.03 --tag dirty

# Giờ cao điểm: 1/3 giữa thời gian chạy tăng gấp 5 lần
python simulator/simulator.py --mode api --rate 1000 --duration 180 --profile peak --peak-mult 5 --stats-csv --tag peak

# Flash sale: tăng vọt 8 lần ở giữa rồi giảm dần
python simulator/simulator.py --mode api --rate 800 --duration 300 --profile flash --peak-mult 8 --stats-csv --tag flash

# Chạy liên tục 60 phút (cùng TV4), 1 giờ thật = 1 ngày ảo, có dữ liệu bẩn và đến trễ
python simulator/simulator.py --mode api --rate 800 --duration 3600 --workers 2 --profile daily --day-seconds 3600 \
    --dirty-rate 0.02 --late-rate 0.01 --stats-csv --report-every 10 --tag soak60

# Ghi thẳng Kafka theo cấu hình TV1 (topic bakery.*)
python simulator/simulator.py --mode kafka --rate 2000 --duration 60 --workers 2 --tag kafka

# Thí nghiệm của TV4/TV1: ghi tối đa vào topic benchmark bakery.bench.p1 / p3 / p6
python simulator/simulator.py --mode kafka --rate 0 --duration 60 --workers 4 --topic bakery.bench.p6 --tag p6
python simulator/simulator.py --mode kafka --rate 0 --duration 60 --linger-ms 50 --compression zstd --tag zstd

# Tập dữ liệu lớn (Volume): 30 ngày × ~100k phiên/ngày thường, tái lập được nhờ --seed
python simulator/simulator.py --mode file --start 2026-10-01 --days 30 --sessions-per-day 100000 \
    --workers 7 --dirty-rate 0.02 --late-rate 0.01 --seed 2026 --gzip --tag oct2026
# rồi đo 5V của tập vừa sinh -> results/profile_dataset_oct2026.md
python simulator/profile_dataset.py data/dataset_oct2026 --workers 7
```

Các tham số chính (xem đầy đủ: `python simulator/simulator.py -h`):

| Tham số | Ý nghĩa |
|---|---|
| `--rate`, `--duration`, `--workers` | tổng event/giây (0 = tối đa), số giây, số tiến trình song song |
| `--max-events` | dừng khi gửi đủ số event (thay cho duration) |
| `--profile`, `--peak-mult` | kịch bản lưu lượng và hệ số cao điểm |
| `--day-seconds`, `--start-hour` | `daily`: số giây thật cho 1 ngày ảo, giờ ảo lúc bắt đầu |
| `--dirty-rate`, `--dirty-kinds` | tỷ lệ event bẩn; giới hạn kiểu bẩn, ví dụ `--dirty-kinds duplicate,naive_time` |
| `--late-rate` | tỷ lệ phiên có `event_time` lùi 1–6 giờ (Collector gắn `is_late=true`) |
| `--seed` | cố định để sinh lại đúng dữ liệu |
| `--stats-csv`, `--report-every` | ghi số liệu từng chu kỳ ra CSV (để vẽ biểu đồ khi chạy dài) |
| `--batch` | `api`: số event tối đa mỗi request (khi `--rate 0`) |
| `--acks`, `--linger-ms`, `--batch-size`, `--compression`, `--topic` | `kafka`: cấu hình producer |
| `--start`, `--days`, `--sessions-per-day`, `--out`, `--gzip` | `file`: khoảng ngày, quy mô, nơi ghi |

## Dữ liệu sinh ra trông như thế nào

- **Phiên theo phễu**: `page_view` → (`search`) → `product_view` → `add_to_cart` → (`remove_from_cart`)
  → `checkout_start` → `order_placed` → `payment`. Khoảng 30% phiên thoát ngay, khoảng 11% phiên có đơn đã thanh toán.
- **Khách**: khoảng 75% là thành viên `U0000001…` (`--users`; chế độ file tự tính để mỗi người khoảng 4 phiên/tháng;
  5% là khách quen, chiếm 15% lượt truy cập và chốt đơn dễ hơn), khoảng 25% là khách vãng lai `A…`. Mỗi thành viên có cửa hàng gần nhà (75% đơn đặt ở đó),
  xem `dim/customers.csv`.
- **Nguồn truy cập**: direct / google / facebook / tiktok / zalo / email, ghi vào URL trang đầu dạng
  `/promotions?utm_source=facebook&utm_medium=cpc&utm_campaign=banh-kem-sinh-nhat`. Mỗi kênh có ý định mua khác
  nhau, và khách từ chiến dịch có voucher thì hay dùng voucher đó.
- **Văn bản tự do**: ô tìm kiếm có hậu tố ("bánh kem cho bé"), gõ không dấu ("banh mi gia re"), gõ nhầm, viết hoa.
  Sau khi tìm, khách thường xem sản phẩm đúng danh mục vừa tìm.
- **Thanh toán**: tỷ lệ lỗi khác nhau theo phương thức (thẻ khoảng 12%, tiền mặt khoảng 1%); một nửa số khách bị lỗi
  thử lại bằng phương thức khác.
- **Theo giờ**: sáng ưu tiên bánh mì và cà phê, tối ưu tiên bánh kem. Lưu lượng có cao điểm sáng, trưa, tối
  (`hourly_traffic` trong `catalog.json`). Cuối tuần đông hơn ×1,35 (chế độ `file`).
- **Nhất quán nghiệp vụ**: `total_amount` = tổng `unit_price × quantity` trừ voucher;
  `payment.amount` = `total_amount`; `payment.order_id` trỏ đúng về đơn.
- **Key Kafka** = `user_id` nên các event của một khách nằm cùng partition, giữ đúng thứ tự.

## Dữ liệu bẩn ↔ lý do DLQ của Collector

| Kiểu (`--dirty-kinds`) | Làm hỏng thế nào | `reason` trong `bakery.dlq` |
|---|---|---|
| `missing_field` | bỏ `user_id` | `schema_violation` |
| `negative_price` | `price` / `total_amount` / `amount` âm | `schema_violation` |
| `wrong_type` | `quantity = "hai"` | `schema_violation` |
| `extra_field` | thêm `coupon_code` ngoài contract | `schema_violation` |
| `empty_items` | đơn hàng `items = []` (event khác thì bỏ `session_id`) | `schema_violation` |
| `bad_time_format` | `event_time = "01/10/2026 08:30"` | `schema_violation` |
| `unknown_event_type` | `clik`, `pageview`, `add_cart` | `unknown_event_type` |
| `future_time` | `event_time` + 1 ngày | `event_time_in_future` |
| `naive_time` | bỏ `Z` (không có múi giờ) | `missing_timezone` |
| `duplicate` | gửi lại cùng `event_id` | `duplicate_event_id` |
| `not_an_object` | gửi chuỗi JSON thay vì object | `not_an_object` |

File kết quả có `dirty_by_kind`, dùng để đối chiếu với `rejected_by_reason` ở `GET /metrics` của Collector.

## File kết quả

- `results/sim_<mode>_<tag>_<ts>.json`: tổng số đã gửi, throughput, lỗi gửi, số bị Collector từ chối,
  dữ liệu bẩn theo kiểu, đếm theo `event_type`, độ trễ p50/p95/p99 (`api`: thời gian khứ hồi HTTP của cả lô;
  `kafka`: thời gian chờ broker ack).
- `results/sim_<mode>_<tag>_<ts>.csv` (khi có `--stats-csv`), mỗi chu kỳ một dòng:
  `wall_time, elapsed_sec, virtual_hour, rate_mult, target_eps, actual_eps, sent_total, send_errors,
  rejected_by_collector, lat_p50_ms, lat_p95_ms, lat_p99_ms`. Ghép theo `wall_time` với CSV của consumer (TV4)
  để vẽ tốc độ gửi so với lag/latency.
- Chế độ `file`: `manifest.json` trong thư mục dữ liệu ghi số phiên, số event, dung lượng, số đơn đã thanh toán và
  doanh thu của từng ngày, để nhóm Storage đối soát sau khi nạp. Thư mục `dim/` có `products.csv`, `stores.csv`,
  `customers.csv` để JOIN với event.
- `results/profile_<dataset>.md` / `.json` (từ `profile_dataset.py`): số liệu 5V của tập dữ liệu, gồm bảng
  đối chiếu lỗi phát hiện được với lỗi đã chèn, phễu chuyển đổi và các KPI doanh thu.
