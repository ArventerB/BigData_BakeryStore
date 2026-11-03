# BẢN GHI CHÚ KỸ THUẬT (TECHNICAL NOTES) – THÀNH VIÊN 1
**Phụ trách:** Hạ tầng Apache Kafka (Ingestion Layer)  
**Dự án:** Hệ thống Thu nhận & Xử lý Dữ liệu Tiệm Bánh (Bakery Store Streaming Pipeline)  
**Người nhận:** Thành viên 5 (Tổng hợp vào Chương 4 – Thiết kế & Chương 5 – Cài đặt)

---

## 1. TỔNG QUAN HẠ TẦNG KAFKA

Hạ tầng lưu trữ và đệm thông điệp của dự án được xây dựng dựa trên **Apache Kafka 3.8.0**, vận hành hoàn toàn trên nền tảng **Docker & Docker Compose**. 

Thay vì sử dụng kiến trúc cũ phụ thuộc vào Apache ZooKeeper, hệ thống triển khai kiến trúc hiện đại **Kafka KRaft (Kafka Raft Metadata Mode)**.

```
                    +------------------------------------------+
                    |          Kafka UI (Cổng :8080)           |
                    +--------------------+---------------------+
                                         | (Quản lý trực quan)
        +--------------------------------+--------------------------------+
        |                                |                                |
+-------v--------+               +-------v--------+               +-------v--------+
|    Broker 1    |     Raft      |    Broker 2    |     Raft      |    Broker 3    |
| (Node ID: 1)   |<------------->| (Node ID: 2)   |<------------->| (Node ID: 3)   |
| Port: 9092     |    Quorum     | Port: 9094     |    Quorum     | Port: 9096     |
+-------+--------+               +-------+--------+               +-------+--------+
        |                                |                                |
  [Data Volume 1]                  [Data Volume 2]                  [Data Volume 3]
```

### Ưu điểm của kiến trúc KRaft trong đồ án:
1. **Loại bỏ ZooKeeper:** Giảm bớt số lượng container cần quản lý, tiết kiệm RAM/CPU trên máy phát triển.
2. **Metadata nhất quán:** Metadata về topic, partition, phân quyền được lưu ngay trong một topic nội bộ chuyên dụng (`__cluster_metadata`), áp dụng thuật toán đồng thuận Raft.
3. **Phục hồi cực nhanh:** Khi một broker gặp sự cố, việc bầu chọn Controller mới diễn ra gần như tức thì (tính bằng mili-giây).

---

## 2. THIẾT KẾ ĐỘ TIN CẬY & KHẢ NĂNG CHỊU LỖI (FAULT TOLERANCE)

Hệ thống được thiết kế theo tiêu chuẩn đảm bảo tính toàn vẹn dữ liệu cho giao dịch tiệm bánh (Zero Data Loss):

* **Số lượng Broker:** $N = 3$ nodes.
* **Replication Factor (RF) = 3:** Mọi phân vùng (partition) đều có đúng 3 bản sao lưu phân bổ đều trên cả 3 Broker (mỗi broker giữ 1 bản).
* **Quorum Controller:** Cả 3 Broker đều đóng vai trò vừa là `broker` vừa là `controller` (`KAFKA_PROCESS_ROLES: 'broker,controller'`). Nhóm bầu chọn gồm 3 thành viên: `1@kafka-1:9093, 2@kafka-2:9093, 3@kafka-3:9093`.
* **Khả năng chịu lỗi:** Cụm 3 nodes áp dụng quy tắc đa số ($Q = \lfloor N/2 \rfloor + 1 = 2$). Hệ thống chịu được việc **1 broker bất kỳ bị sập đột ngột** mà toàn bộ cụm vẫn duy trì đọc/ghi bình thường mà không mất dữ liệu.
* **`min.insync.replicas = 2`:** Khi Producer gửi tin nhắn với mức xác nhận cao nhất, bắt buộc phải có ít nhất 2 bản sao (Leader + 1 Replica) ghi nhận thành công thì tin nhắn mới được coi là hợp lệ.

---

## 3. THIẾT KẾ DANH MỤC TOPIC (DATA CONTRACT & PARTITIONING)

Dữ liệu mô phỏng của tiệm bánh được phân loại thành các Topic chuyên biệt:

| Tên Topic | Số Partition | Replication Factor | Min ISR | Mục đích & Nghiệp vụ |
|---|:---:|:---:|:---:|---|
| **`bakery.user-events`** | 3 | 3 | 2 | Dữ liệu hành vi người dùng (xem bánh, bấm giỏ hàng, tìm kiếm). Tần suất cực cao. |
| **`bakery.orders`** | 3 | 3 | 2 | Đơn đặt hàng từ khách. Yêu cầu toàn vẹn 100%. |
| **`bakery.payments`** | 3 | 3 | 2 | Trạng thái thanh toán (MoMo, VNPay, COD, Thẻ ngân hàng). |
| **`bakery.dlq`** | 3 | 3 | 2 | **Dead Letter Queue:** Chứa các bản ghi lỗi, dữ liệu bẩn từ Simulator để cách ly và phân tích nguyên nhân. |
| **`bakery.bench.p1`** | 1 | 3 | 2 | Phục vụ bài thí nghiệm đo lường hiệu năng với 1 partition. |
| **`bakery.bench.p3`** | 3 | 3 | 2 | Phục vụ bài thí nghiệm đo lường hiệu năng với 3 partitions. |
| **`bakery.bench.p6`** | 6 | 3 | 2 | Phục vụ bài thí nghiệm đo lường hiệu năng với 6 partitions. |

### Chiến lược Partitioning & Đảm bảo thứ tự (Message Ordering):
- Mọi message gửi vào Kafka đều gắn kèm **Message Key** (ví dụ: `key = order_id` hoặc `key = user_id`).
- Kafka sử dụng giải thuật băm mặc định `MurmurHash2(key) % num_partitions` để ánh xạ message vào partition.
- **Ý nghĩa:** Đảm bảo toàn bộ các sự kiện thuộc về cùng một đơn hàng hoặc cùng một khách hàng luôn rơi vào **cùng một partition duy nhất**, từ đó bảo toàn tuyệt đối thứ tự thời gian của sự kiện.

---

## 4. CHIẾN LƯỢC CẤU HÌNH PRODUCER TỐI ƯU

Để cân bằng giữa **Độ tin cậy dữ liệu (Reliability)** và **Tốc độ truyền tải (Throughput)**, hạ tầng quy định 2 bộ cấu hình:

### 4.1. Cấu hình cho Đơn hàng & Thanh toán (Yêu cầu toàn vẹn tối đa)
- `acks = "all"` (hoặc `-1`): Producer chỉ nhận phản hồi thành công khi dữ liệu đã được ghi vào Leader và tối thiểu `min.insync.replicas` (2 brokers).
- `retries = 5`: Tự động gửi lại nếu gặp gián đoạn kết nối tạm thời.
- `max_in_flight_requests_per_connection = 1`: Ngăn ngừa việc xáo trộn thứ tự tin nhắn khi có retry.
- `compression_type = "lz4"`: Nén payload trước khi gửi qua mạng. Thuật toán LZ4 cho tốc độ nén/giải nén cực nhanh, tốn rất ít CPU.

### 4.2. Cấu hình cho Dữ liệu Sự kiện / Clickstream (Yêu cầu tốc độ cao)
- `batch_size = 65536` (64 KB) hoặc `131072` (128 KB): Tăng kích thước bộ đệm để gom nhiều sự kiện.
- `linger_ms = 10` (hoặc `20` ms): Cho phép Producer chờ tối đa 10ms gom tin nhắn vào batch thay vì gửi từng bản ghi đơn lẻ, giúp throughput tăng gấp 10–20 lần so với mặc định.

---

## 5. HƯỚNG DẪN TV5 ĐƯA VÀO BÁO CÁO ĐỒ ÁN

- **Đưa vào Chương 4 (Thiết kế hệ thống):**
  - Chép Mục 1 (Sơ đồ kiến trúc KRaft 3 Broker).
  - Chép Mục 3 (Bảng danh mục Topic và cơ chế Partition Key).
- **Đưa vào Chương 5 (Cài đặt hệ thống):**
  - Trích dẫn file `scripts/docker-compose.yml` và giải thích các biến môi trường `KAFKA_PROCESS_ROLES`, `KAFKA_CONTROLLER_QUORUM_VOTERS`.
  - Trích dẫn bảng cấu hình Producer ở Mục 4 và giải thích lý do lựa chọn `acks=all`, `linger.ms=10`, `lz4`.
- **Đưa vào Phụ lục:**
  - Hướng dẫn khởi chạy hệ thống từ file `scripts/README.md`.

