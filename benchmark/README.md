# Benchmark – Đo lường & thí nghiệm hiệu năng (TV4)

Bộ công cụ đo throughput, độ trễ, consumer lag và **đếm chính xác bản ghi mất / trùng** trên cụm Kafka KRaft
3 broker thật của TV1. Kết quả nằm ở [`results/tv4/`](../results/tv4/) (bảng số:
[`summary_tables.md`](../results/tv4/summary_tables.md)), báo cáo phần TV4 ở [`docs/TV4.docx`](../docs/TV4.docx).

> Không có chế độ "giả lập số liệu". Nếu không kết nối được Kafka, script dừng và báo lỗi.

## Các file

| File | Vai trò |
|---|---|
| `run_benchmarks.py` | Điều phối ma trận thí nghiệm E1–E8 và thí nghiệm chịu lỗi E9: reset topic, chạy consumer kiểm chứng song song với N producer, ghi JSON/CSV thô |
| `load_generator.py` | Producer đa tiến trình (confluent-kafka). Gửi event tiệm bánh dựng sẵn, key = `user_id`, gắn header `tv4 = (run_id, worker, seq)` |
| `bench_consumer.py` | Consumer kiểm chứng: gán partition tại offset đầu lần chạy, đo e2e latency / lag mỗi giây, đếm mất / trùng theo header hoặc `event_id` |
| `soak_test.py` | Chạy liên tục 30–60 phút: simulator của TV3 → 3 topic nghiệp vụ → consumer kiểm chứng (có chèn trùng có chủ đích để kiểm tra consumer) |
| `summarize.py` | Gộp các lần lặp → `summary_*.csv`, `summary.json`, `summary_tables.md` |
| `plot_results.py` | Vẽ biểu đồ → `results/tv4/figures/*.png` |
| `verify_consumer.py` | Consumer đọc topic nghiệp vụ và in log (`--show N` in chi tiết N bản ghi), đếm trùng, DLQ |
| `bench_utils.py` | Lấy mẫu reservoir, phân vị, định dạng header |
| `docker-compose.ramdisk.yml` | Override đặt log của broker lên RAM, chỉ dùng cho E1–E8 (xem mục "Vì sao dùng RAM") |
| `catalog.json` | Sản phẩm, cửa hàng, phương thức thanh toán để sinh event |

## Định nghĩa số đo

| Số đo | Cách tính |
|---|---|
| Throughput producer | số bản ghi broker **đã xác nhận** ÷ thời gian từ lúc bắt đầu tới khi `flush()` xong |
| Throughput consumer | số bản ghi consumer nhận ÷ thời gian từ bản ghi đầu tới bản ghi cuối |
| Độ trễ ack | `produce()` → broker xác nhận (`msg.latency()` của librdkafka) |
| Độ trễ end-to-end (e2e) | thời điểm consumer nhận − `CreateTime` producer gán lúc `produce()`; cùng 1 máy nên cùng đồng hồ. Bỏ 3 giây khởi động |
| Consumer lag | Σ(high watermark − vị trí đã đọc) trên mọi partition, hỏi broker mỗi giây ở luồng riêng |
| Mất sau ack | đã gửi − nhận được (duy nhất) − bị producer báo lỗi − chưa flush |
| Trùng | số `(worker, seq)` (hoặc `event_id`) xuất hiện quá 1 lần |
| Byte trên mạng / bản ghi | `tx_bytes` (thống kê librdkafka) ÷ số bản ghi; tỉ lệ nén = byte JSON gốc ÷ `tx_bytes` |

Phân vị lấy trên mẫu reservoir (tối đa 100.000 giá trị mỗi tiến trình, ngẫu nhiên đều). Mỗi cấu hình chạy 3 lần,
xen kẽ giữa các cấu hình để giảm sai lệch theo thời gian. Kết quả là trung bình ± độ lệch chuẩn.

## Cách chạy lại

```powershell
# 0. Môi trường (Python 3.12)
python -m venv .venv; .\.venv\Scripts\pip install -r benchmark\requirements.txt -r simulator\requirements.txt

# 1. Ma trận tốc độ tối đa E1–E8: broker ghi log lên RAM
docker compose -f scripts/docker-compose.yml -f benchmark/docker-compose.ramdisk.yml up -d --force-recreate --renew-anon-volumes kafka-1 kafka-2 kafka-3
#    file của topic đã xoá được dọn sau 1s thay vì 60s (cấu hình động, chỉ trên cụm RAM) để tmpfs 2 GB không bị đầy
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-default --alter --add-config log.segment.delete.delay.ms=1000
.\.venv\Scripts\python benchmark\run_benchmarks.py --list                       # xem danh sách cấu hình
.\.venv\Scripts\python benchmark\run_benchmarks.py --experiments all --reps 3 --duration 15

# 2. Trả lại cấu hình đĩa thật của TV1 cho thí nghiệm chịu lỗi và chạy dài
docker compose -f scripts/docker-compose.yml up -d --force-recreate kafka-1 kafka-2 kafka-3
cd scripts; .\create_topics.ps1; cd ..
.\.venv\Scripts\python benchmark\run_benchmarks.py --experiments fault --reps 2 --min-free-gb 5
.\.venv\Scripts\python benchmark\soak_test.py --duration 1800 --rate 2000

# 3. Đo bổ sung E8 ở tải cao (trên cụm RAM như bước 1)
.\.venv\Scripts\python benchmark\run_benchmarks.py --experiments E8_load_curve --load-rates 250000,300000,350000,400000,0 --reps 3 --duration 15

# 4. Tổng hợp + biểu đồ
.\.venv\Scripts\python benchmark\summarize.py
.\.venv\Scripts\python benchmark\plot_results.py

# Consumer đọc topic nghiệp vụ và in log
.\.venv\Scripts\python benchmark\verify_consumer.py --from-beginning --idle-timeout 10 --show 20 --tag business
```

## Vì sao ma trận E1–E8 dùng RAM

Docker Desktop lưu dữ liệu trong một đĩa ảo (`docker_data.vhdx`, trên máy này nằm ở ổ D). File này **không sparse**:
nó phình theo tổng lượng đã ghi chứ không theo dung lượng Kafka đang giữ. Ba lần chạy đầu trên đĩa làm file
tăng từ 4,1 lên 15,7 GB dù Kafka đã xoá hết dữ liệu. Ma trận 90 lần chạy ở 300–500k event/s ghi hàng trăm GB,
nên log của broker được đặt lên tmpfs bằng file override (không sửa file của TV1). Mọi cấu hình Kafka khác giữ
nguyên. Kafka mặc định không `fsync` mà ghi vào page cache, nên ảnh hưởng của việc này tới thông lượng là nhỏ.
Báo cáo có so sánh với 3 lần đo trên đĩa thật ([`results/tv4/raw_disk/`](../results/tv4/raw_disk/)).

Mỗi lần chạy, harness xoá và tạo lại topic benchmark (giữ RF=3, `min.insync.replicas=2`; thêm
`file.delete.delay.ms=1000` để giải phóng bộ nhớ ngay), và dừng hẳn nếu ổ đĩa còn dưới `--min-free-gb`.
