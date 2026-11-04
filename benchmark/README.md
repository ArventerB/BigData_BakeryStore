# Hướng dẫn Phân hệ Đo lường & Benchmark – Thành viên 4 (Phong)

Thư mục này chứa toàn bộ mã nguồn kiểm chứng, đo lường và thực nghiệm hiệu năng cho hệ thống Thu nhận Dữ liệu Tiệm Bánh (**Bakery Store Streaming Pipeline**).

---

## 1. Danh mục các file trong `benchmark/`

| Tên file | Chức năng chính |
|---|---|
| **`run_benchmarks.py`** | **Script tự động hóa:** Chạy trọn gói 6 kịch bản benchmark quy chuẩn, tự động xuất log, CSV, JSON, vẽ biểu đồ so sánh và tạo báo cáo cho TV5. |
| **`verify_consumer.py`** | **Consumer kiểm chứng:** Đọc message từ Kafka, đo Throughput thời gian thực (EPS), End-to-End Latency ($p_{50}, p_{95}, p_{99}$), Consumer Lag, đếm bản ghi trùng lặp và phân tích DLQ. |
| **`load_generator.py`** | **Bộ phát tải (Load Generator):** Sinh event đơn hàng/clickstream tiệm bánh và đẩy thẳng vào Kafka với các bộ cấu hình tham số tùy chỉnh (`acks`, `batch_size`, `linger_ms`, `compression`). |
| **`plot_results.py`** | **Công cụ trực quan hóa:** Vẽ đồ thị chuỗi thời gian 3 tầng (Throughput, $p_{95}$ Latency, Lag) và biểu đồ cột kép so sánh đối chiếu giữa các thí nghiệm. |
| **`catalog.json`** | Danh mục sản phẩm, cửa hàng và phương thức thanh toán của tiệm bánh dùng để sinh dữ liệu. |
| **`requirements.txt`** | Danh mục thư viện Python phụ thuộc (`confluent-kafka`, `matplotlib`). |

---

## 2. Hướng dẫn Khởi chạy & Vận hành

### Bước 1: Cài đặt thư viện phụ thuộc
Đảm bảo đã kích hoạt môi trường Python và cài đặt:
```powershell
pip install -r benchmark/requirements.txt
```

### Bước 2: Khởi động cụm Kafka (Nếu chưa bật)
Mở Docker Desktop, sau đó mở terminal tại thư mục gốc dự án:
```powershell
cd scripts
docker compose up -d
.\create_topics.ps1
cd ..
```
*(Kiểm tra Kafka UI tại [http://localhost:8080](http://localhost:8080) trạng thái `3 Online / 0 Offline`)*

---

### Bước 3: Chạy Tự động Toàn bộ 6 Kịch bản Benchmark
Chỉ cần chạy 1 lệnh duy nhất:
```powershell
python benchmark/run_benchmarks.py --duration 20
```
> *Mỗi kịch bản sẽ chạy trong 20 giây. Toàn bộ quá trình diễn ra khoảng 2-3 phút.*

#### Kết quả tự động sinh ra trong thư mục `results/`:
1. **`results/benchmark_partition_scaling.png`**: Biểu đồ so sánh hiệu năng theo số lượng phân vùng (1 vs 3 vs 6 partitions).
2. **`results/benchmark_producer_tuning.png`**: Biểu đồ đánh giá tinh chỉnh tham số Producer (Unbatched vs Critical vs High Throughput).
3. **`results/tv4_benchmark_report.md`**: Báo cáo tổng kết đầy đủ bảng số liệu và phân tích chuyên sâu gửi cho **TV5** đưa vào Chương 6 & 7 của Báo cáo.
4. Các file đồ thị chuỗi thời gian `.png`, file dữ liệu thô `.csv` từng giây, và file `.json` cấu hình.

---

## 3. Chạy từng công cụ độc lập (Nâng cao)

### A. Chạy Consumer kiểm chứng độc lập
Để theo dõi và đo lường liên tục luồng dữ liệu nghiệp vụ:
```powershell
# Đọc toàn bộ các topic nghiệp vụ tiệm bánh:
python benchmark/verify_consumer.py --topics bakery.user-events,bakery.orders,bakery.payments,bakery.dlq --idle-timeout 10

# Đọc topic benchmark riêng lẻ từ đầu:
python benchmark/verify_consumer.py --topics bakery.bench.p3 --from-beginning --tag test_p3
```

### B. Chạy Bộ phát tải độc lập
```powershell
# Phát tải tối đa vào topic 3 partition trong 30 giây:
python benchmark/load_generator.py --topic bakery.bench.p3 --workers 4 --duration 30 --tag manual_test
```

### C. Vẽ lại biểu đồ từ file CSV hoặc JSON
```powershell
# Vẽ đồ thị chuỗi thời gian từ file CSV:
python benchmark/plot_results.py results/consumer_run_123456789.csv

# So sánh các file kết quả JSON:
python benchmark/plot_results.py --compare results/sim_kafka_*.json --out results/my_compare.png
```

