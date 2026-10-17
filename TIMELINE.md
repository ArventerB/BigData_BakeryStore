# Timeline & phân công – Đồ án Big Data (Sources → Ingestion)

Thời gian: **3 tuần, 19/10/2026 – 08/11/2026**.
Code tham khảo nằm ở nhánh `mau`. Nhánh `main` là nơi nhóm tự làm.

## Cấu trúc thư mục

| Thư mục | Nội dung | Phụ trách |
|---|---|---|
| `scripts/` | Docker Compose, script tạo topic Kafka | TV1 |
| `collector/` | API nhận event, kiểm tra schema, đẩy vào Kafka | TV2 |
| `web-tracking/` | Đoạn tracking gắn vào web tiệm bánh | TV2 |
| `simulator/` | Script sinh dữ liệu người dùng và giao dịch | TV3 |
| `benchmark/` | Consumer kiểm chứng, đo throughput/latency, vẽ biểu đồ | TV4 |
| `results/` | Kết quả thí nghiệm (log, CSV, ảnh) | TV4 |
| `docs/` | Data contract, sơ đồ kiến trúc, báo cáo | TV5 |

## Phân công

| Thành viên | Họ tên – MSSV | Vai trò |
|---|---|---|
| TV1 | | Nhóm trưởng, hạ tầng Kafka |
| TV2 | | Collector API, tích hợp web tiệm bánh |
| TV3 | | Simulator, dữ liệu |
| TV4 | | Đo lường, thí nghiệm |
| TV5 | | Báo cáo, sơ đồ, minh chứng |

## Tuần 1 (19/10 – 25/10): Thiết kế và dựng nền

- [ ] **Cả nhóm** – Chốt đề tài, ranh giới đầu vào/đầu ra, schema event, danh sách topic
- [ ] **Cả nhóm** – Liên hệ nhóm Ingestion → Storage để thống nhất data contract
- [ ] **TV1** – Cài Docker Desktop, dựng Kafka (3 broker) + Kafka UI, tạo topic
- [ ] **TV2** – Dựng khung Collector API, endpoint nhận event
- [ ] **TV3** – Viết simulator bản đầu (sinh event theo phiên người dùng)
- [ ] **TV4** – Viết consumer đọc topic, in log
- [ ] **TV5** – Viết Chương 1 (Giới thiệu), Chương 2 (Cơ sở lý thuyết Kafka/streaming)
- [ ] **Mốc 1 (25/10):** chạy được simulator → API → Kafka → consumer

## Tuần 2 (26/10 – 01/11): Hoàn thiện chức năng

- [ ] **TV2** – Gắn tracking vào web tiệm bánh thật, backend gửi đơn hàng/thanh toán
- [ ] **TV2** – Kiểm tra dữ liệu lỗi, đẩy sang topic DLQ
- [ ] **TV3** – Thêm dữ liệu bẩn, kịch bản giờ cao điểm, sinh tập dữ liệu lớn
- [ ] **TV4** – Đo throughput, latency, consumer lag, đếm mất/trùng
- [ ] **TV1** – Kiểm tra replication, cấu hình producer (`acks`, `linger.ms`, nén)
- [ ] **TV5** – Viết Chương 3 (Dữ liệu), Chương 4 (Thiết kế), Chương 5 (Cài đặt)
- [ ] **Mốc 2 (01/11):** hệ thống đủ chức năng
- [ ] **Cả nhóm** – Họp thuyết trình chéo: mỗi người giải thích phần mình

## Tuần 3 (02/11 – 08/11): Thực nghiệm, báo cáo, demo

- [ ] **TV4 + TV1** – Thí nghiệm số partition (1/3/6) và số producer
- [ ] **TV4** – Thí nghiệm tinh chỉnh producer (batch, linger, compression)
- [ ] **TV1** – Thí nghiệm chịu lỗi: tắt 1 broker, chứng minh không mất dữ liệu
- [ ] **TV3 + TV4** – Chạy liên tục 30–60 phút, vẽ biểu đồ lag/latency
- [ ] **TV2** – Quay video demo từ web thật đến Kafka UI
- [ ] **TV5** – Viết Chương 6–9, phụ lục (hướng dẫn chạy, phân công, sử dụng AI)
- [ ] **TV1** – Viết README hướng dẫn chạy
- [ ] **Cả nhóm** – Review báo cáo, tập demo, chuẩn bị trả lời câu hỏi
- [ ] **08/11:** Xuất PDF, nộp

## Quy ước làm việc

- Mỗi người làm trên nhánh riêng (`tv1-kafka`, `tv2-collector`, …), tạo Pull Request vào `main`.
- Mỗi thành viên gửi TV5 ghi chú kỹ thuật phần mình (1–2 trang) trước ngày 01/11.
- Họp nhóm ngắn mỗi tuần, đánh dấu checklist ở file này.
