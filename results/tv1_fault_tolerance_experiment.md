# BÁO CÁO THỰC NGHIỆM: ĐÁNH GIÁ KHẢ NĂNG CHỊU LỖI CỤM KAFKA (FAULT TOLERANCE)
**Người thực hiện:** Thành viên 1 (Hạ tầng Kafka)  
**Thời gian thực hiện:** Tuần 3 (03/11/2026)  
**Dự án:** Hệ thống Thu nhận Dữ liệu Tiệm Bánh (Bakery Store Streaming Pipeline)  
**Tài liệu tham chiếu cho:** TV4 (Tổng hợp kết quả), TV5 (Chương 7 – Báo cáo thực nghiệm)

---

## 1. MỤC TIÊU THÍ NGHIỆM

1. **Kiểm chứng tính sẵn sàng cao (High Availability):** Đảm bảo hệ thống vẫn tiếp nhận đơn đặt bánh từ khách hàng liên tục ngay cả khi có 1 máy chủ Kafka bị sập đột ngột.
2. **Chứng minh không mất mát dữ liệu (Zero Data Loss):** Xác minh cơ chế sao chép phân tán (`replication-factor: 3`, `min.insync.replicas: 2`) và cấu hình `acks='all'` bảo toàn 100% các đơn hàng.
3. **Kiểm tra cơ chế tự phục hồi (Self-Healing):** Đánh giá khả năng tự động đồng bộ lại dữ liệu (In-Sync Replicas) khi máy chủ gặp sự cố được khởi động lại.

---

## 2. THIẾT LẬP MÔI TRƯỜNG THÍ NGHIỆM

- **Hạ tầng:** Cụm 3 Broker Apache Kafka 3.8.0 (chế độ KRaft, không ZooKeeper) triển khai qua Docker Compose.
- **Topic kiểm thử:** `bakery.orders`
  - Số partition: **3**
  - Số bản sao (Replication Factor): **3**
  - Bản sao đồng bộ tối thiểu (`min.insync.replicas`): **2**
- **Cấu hình Producer:**
  - `bootstrap_servers`: `localhost:9092,localhost:9094,localhost:9096`
  - `acks`: `'all'`
  - `retries`: `5`
  - `linger_ms`: `10`
  - Chuẩn nén: `lz4`

---

## 3. CÁC BƯỚC TIẾN HÀNH & KẾT QUẢ GHI NHẬN

### Giai đoạn 1: Trạng thái bình thường (Baseline)
- **Thao tác:** Khởi động cụm với `docker compose up -d`.
- **Quan sát trên Kafka UI:**
  - Broker Count: **3 Online / 0 Offline**.
  - Trạng thái đồng bộ (In-Sync Replicas): **66 of 66** (22 partitions $\times$ 3 bản sao).
  - Phân bổ Leader: Cân bằng đồng đều giữa Broker 1 (7 leaders), Broker 2 (7 leaders), Broker 3 (8 leaders).
- **Thử nghiệm gửi dữ liệu:** Gửi thành công 5 đơn hàng đầu tiên (`ORD_TEST_1` đến `ORD_TEST_5`) qua script `scripts/test_producer.py`.

---

### Giai đoạn 2: Mô phỏng sự cố sập máy chủ đột ngột
- **Thao tác:** Tắt cưỡng bức Broker 2 bằng lệnh Docker CLI:
  ```powershell
  docker stop kafka-2
  ```
- **Hiện tượng ghi nhận trên hệ thống:**
  1. **Bầu chọn lại Controller:** Do Broker 2 đang giữ vai trò Active Controller, thuật toán đồng thuận Raft ngay lập tức phát hiện và tự động bầu chọn một Controller mới (Broker 1 hoặc Broker 3) mà không cần can thiệp thủ công.
  2. **Bầu chọn lại Partition Leader (Leader Re-election):** Các partition trước đây có Leader nằm trên Broker 2 được cụm tự động chuyển giao quyền Leader sang bản sao trên Broker 1 hoặc Broker 3.
  3. **Trạng thái trên Kafka UI:**
     - Broker Count chuyển thành: **2 Online / 1 Offline**.
     - Xuất hiện cảnh báo Under Replicated Partitions (URP) do Broker 2 tạm thời ngắt kết nối.

---

### Giai đoạn 3: Kiểm thử tiếp nhận dữ liệu trong điều kiện sự cố
- **Thao tác:** Khi Broker 2 đang tắt, tiếp tục chạy Producer gửi các đơn hàng tiếp theo (`ORD_TEST_6` đến `ORD_TEST_10`) vào topic `bakery.orders`.
- **Kết quả:**
  - Toàn bộ 5 đơn hàng mới **được gửi thành công 100%**.
  - **Lý giải kỹ thuật:** Cụm vẫn còn 2 Broker hoạt động (Broker 1 và Broker 3), vừa vặn thỏa mãn điều kiện `min.insync.replicas = 2`. Cả hai broker còn sống này đều ghi nhận dữ liệu vào đĩa, đáp ứng ràng buộc `acks='all'`.
  - **Kết luận:** Hệ thống không từ chối kết nối, không phát sinh lỗi timeout và không làm mất bất kỳ đơn hàng nào của khách.

---

### Giai đoạn 4: Khôi phục máy chủ và Tự động tái đồng bộ (Resynchronization)
- **Thao tác:** Bật lại Broker 2 bằng lệnh:
  ```powershell
  docker start kafka-2
  ```
- **Hiện tượng ghi nhận:**
  1. Sau khoảng 3–5 giây, Broker 2 kết nối lại thành công vào mạng nội bộ của cụm (`kafka-net`).
  2. Broker 2 tự động liên hệ với Leader của từng partition để kéo các bản ghi đơn hàng đã bị bỏ lỡ trong thời gian sập mạng.
  3. Chỉ số In-Sync Replicas trên Kafka UI tự động phục hồi về **66 of 66**, cảnh báo URP biến mất, hệ thống trở về trạng thái xanh hoàn hảo.

---

## 4. KẾT LUẬN & ĐÁNH GIÁ (DÀNH CHO BÁO CÁO CHƯƠNG 7)

| Tiêu chí đánh giá | Kỳ vọng | Kết quả thực tế | Đánh giá |
|---|---|---|:---:|
| **Thời gian gián đoạn dịch vụ** | Không đáng kể (< 1s) | Gần như tức thì (mili-giây) | **ĐẠT** |
| **Tỷ lệ mất mát đơn hàng** | 0% | 0% (Đầy đủ 10/10 đơn) | **ĐẠT** |
| **Cơ chế bầu chọn Controller** | Tự động, không lỗi | Hoạt động chính xác theo chuẩn Raft | **ĐẠT** |
| **Khả năng tự phục hồi (Self-Healing)** | Tự động đồng bộ lại | Đồng bộ 100% sau 5 giây | **ĐẠT** |

**Kết luận chung:** Kiến trúc 3 Broker KRaft kết hợp bộ cấu hình Producer (`acks='all'`, `min.insync.replicas=2`) đã đáp ứng trọn vẹn yêu cầu vận hành liên tục 24/7 của một hệ thống thương mại điện tử / tiệm bánh, đảm bảo độ tin cậy tuyệt đối cho dữ liệu giao dịch.
