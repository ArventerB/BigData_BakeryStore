# Big Data – Sources → Ingestion

Thu thập **dữ liệu hành vi người dùng và giao dịch theo thời gian thực** từ web/app tiệm bánh
(tái sử dụng đồ án Công nghệ phần mềm) vào **Apache Kafka**.

Đồ án môn Nhập môn Dữ liệu lớn – SGU. Phạm vi nhóm: giai đoạn **Sources → Ingestion** của pipeline.

```
[Web/App tiệm bánh] --tracker.js--> 
                                   \
[Python Simulator, N tiến trình] ---> [Collector API (FastAPI)] --validate--> [Kafka 3 broker, KRaft]
                       \                                     \--lỗi--> shop.dlq
                        \--(chế độ kafka: ghi thẳng)-------------------> shop.clickstream | shop.orders | shop.payments
                                                                                │
                                         [verify_consumer] <────────────────────┘ đo throughput, latency, lag, trùng
                                                │──> results/*.csv, *.json, *.png (minh chứng báo cáo)
                                                └──> data/landing/topic=…/date=…/hour=…/*.jsonl (bàn giao nhóm Storage)
```

## Cấu trúc thư mục

| Thư mục | Nội dung | Người phụ trách |
|---|---|---|
| `docker-compose.yml`, `scripts/` | Kafka 3 broker (KRaft), Kafka UI, tạo topic | TV1 – Kafka |
| `collector/` | REST API nhận event, kiểm tra schema, khử trùng, đẩy Kafka/DLQ | TV2 – Tích hợp |
| `web-tracking/tracker.js` | Đoạn JS gắn vào web tiệm bánh | TV2 – Tích hợp |
| `simulator/` | Bộ sinh tải theo phiên người dùng, chèn dữ liệu bẩn, giờ cao điểm | TV3 – Simulator |
| `benchmark/` | Consumer kiểm chứng + vẽ biểu đồ | TV4 – Đo lường |
| `docs/` | Data contract, sơ đồ, báo cáo | TV5 – Báo cáo |
| `results/` | Kết quả thí nghiệm (không commit, trừ khi chọn lọc đưa vào báo cáo) | |

## Yêu cầu môi trường

- **Docker Desktop** (Windows bật WSL2), cấp tối thiểu **6 GB RAM** cho Docker
- **Python 3.11+**
- Cổng trống: 8000, 8080, 9092, 9094, 9095

## Chạy nhanh

```bash
# 1. Khởi động Kafka + Kafka UI + Collector (lần đầu mất vài phút để tải image)
docker compose up -d --build
docker compose ps            # init-topics phải "Exited (0)", các service khác "running/healthy"

# 2. Cài thư viện Python cho simulator/benchmark (chạy ở thư mục gốc repo)
python -m venv .venv
.venv\Scripts\activate       # Linux/macOS: source .venv/bin/activate
pip install -r simulator/requirements.txt -r benchmark/requirements.txt

# 3. Terminal A – mở consumer kiểm chứng trước
python benchmark/verify_consumer.py --idle-timeout 15 --tag demo

# 4. Terminal B – phát 2000 event/s trong 60s, 3% dữ liệu bẩn
python simulator/simulator.py --mode api --rate 2000 --duration 60 --workers 4 --dirty-rate 0.03 --tag demo

# 5. Vẽ biểu đồ cho báo cáo
python benchmark/plot_results.py results/consumer_demo_<timestamp>.csv
```

Giao diện quan sát:
- Kafka UI: <http://localhost:8080> (topic, partition, leader/replica, message, consumer lag)
- Collector API (Swagger): <http://localhost:8000/docs>, số liệu: <http://localhost:8000/metrics>

Dừng: `docker compose down` (giữ dữ liệu) hoặc `docker compose down -v` (xoá sạch dữ liệu Kafka).

## Tích hợp web tiệm bánh

Xem chú thích đầu file [`web-tracking/tracker.js`](web-tracking/tracker.js). Tóm tắt:
- Frontend gọi `ShopTracker.track("product_view", {...})` cho các hành vi duyệt web.
- `order_placed` và `payment` nên gửi từ **backend** sau khi lưu DB thành công (POST JSON tới `/events`),
  để dữ liệu giao dịch không bị giả mạo hay mất khi người dùng đóng tab.
- Danh mục sản phẩm của simulator nằm ở [`simulator/catalog.json`](simulator/catalog.json). Nên đồng bộ `product_id`
  với DB của web để dữ liệu thật và dữ liệu mô phỏng khớp nhau.

## Data contract

Schema, topic, quy tắc DLQ, ví dụ bản ghi: [`docs/data_contract.md`](docs/data_contract.md).

## Kịch bản thí nghiệm

Mỗi lần chạy, simulator lưu `results/sim_*.json`, consumer lưu `results/consumer_*.json` + `.csv`.
Đặt `--tag` để dễ phân biệt.

| # | Mục tiêu | Lệnh mẫu |
|---|---|---|
| 1 | Throughput theo số partition | `python simulator/simulator.py --mode kafka --rate 0 --duration 60 --workers 4 --topic shop.bench.p1 --tag p1` (lặp lại với `p3`, `p6`) |
| 2 | Số producer song song | như trên, đổi `--workers 1/2/4/8` |
| 3 | Tinh chỉnh producer | `--linger-ms 0/10/50`, `--batch-size 16384/65536/262144`, `--compression none/snappy/lz4/zstd` |
| 4 | Độ bền / chịu lỗi | chạy `--acks all`, giữa chừng `docker stop kafka2`, sau đó `docker start kafka2`. So sánh `sent_total` với `records_total` của consumer (`--from-beginning`) |
| 5 | Chạy liên tục + giờ cao điểm | `--mode api --rate 1000 --duration 1800 --peak-mult 5`, consumer chạy song song, vẽ lag/latency theo thời gian |
| 6 | Veracity | `--dirty-rate 0.05`: so sánh `dirty_injected` của simulator với `/metrics` → `rejected_by_reason` |

So sánh nhiều lần chạy: `python benchmark/plot_results.py --compare "results/sim_kafka_p*.json"`.

> Lưu ý đo latency: chạy consumer **song song** với simulator (không dùng `--from-beginning`),
> nếu không latency sẽ bao gồm thời gian dữ liệu nằm chờ trong Kafka.

## Bàn giao cho nhóm Ingestion → Storage

- Cách 1 (khuyến nghị): nhóm sau đọc trực tiếp các topic `shop.*` (Kafka Connect / Spark Structured Streaming).
- Cách 2 (độc lập): `python benchmark/verify_consumer.py --from-beginning --idle-timeout 10 --sink data/landing`
  tạo JSONL phân vùng `topic=/date=/hour=`.

## Phân công

| Thành viên | Vai trò |
|---|---|
| TV1 | Nhóm trưởng, hạ tầng Kafka |
| TV2 | Collector API, tích hợp web |
| TV3 | Simulator |
| TV4 | Benchmark, đo lường |
| TV5 | Báo cáo, sơ đồ, minh chứng |

*(Điền tên và MSSV.)*
