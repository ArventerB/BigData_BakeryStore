# Hạ tầng Apache Kafka (KRaft) & Kafka UI – Tiệm bánh (Bakery Store)

Thư mục này chứa toàn bộ cấu hình hạ tầng trung tâm cho dự án, sử dụng cụm **Apache Kafka 3.8.0** chạy chế độ **KRaft (3 Brokers)** không cần ZooKeeper và giao diện quản lý **Kafka UI**.

---

## 1. Yêu cầu môi trường
- Đã cài đặt và đang chạy **Docker Desktop**.
- Hỗ trợ cả PowerShell (Windows) và Bash (Linux/macOS).

---

## 2. Hướng dẫn khởi động nhanh

Mở terminal tại thư mục `scripts/`:

### Bước 1: Khởi động cụm 3 Broker + Kafka UI
```powershell
docker compose up -d
```
> *Chờ khoảng 10–15 giây để 3 Broker hoàn tất bầu chọn Controller (Quorum).*

### Bước 2: Tự động khởi tạo danh sách Topic
```powershell
.\create_topics.ps1
```

### Bước 3: Kiểm tra giao diện quản lý
Mở trình duyệt truy cập:
👉 **[http://localhost:8080](http://localhost:8080)**
- Kiểm tra cụm `kraft-cluster`: trạng thái `3 Online / 0 Offline`.
- Kiểm tra danh sách Topic đã được tạo đầy đủ.

---

## 3. Thông số kết nối Kafka (Dành cho TV2, TV3, TV4)

Mọi ứng dụng (Collector API, Simulator, Benchmark Consumer) sử dụng các thông số sau để kết nối:

| Môi trường chạy ứng dụng | Bootstrap Servers |
|---|---|
| **Chạy ngoài máy Host** (chạy script Python trực tiếp trên máy) | `localhost:9092,localhost:9094,localhost:9096` |
| **Chạy trong Docker Container** (chung mạng `kafka-net`) | `kafka-1:29092,kafka-2:29092,kafka-3:29092` |

- **Kafka UI:** `http://localhost:8080`

---

## 4. Danh sách các Topic & Cấu hình

| Tên Topic | Partitions | Replicas | Min ISR | Mục đích sử dụng |
|---|---|---|---|---|
| `bakery.user-events` | 3 | 3 | 2 | Dữ liệu clickstream: xem sản phẩm, thêm giỏ hàng |
| `bakery.orders` | 3 | 3 | 2 | Đơn đặt hàng từ khách hàng |
| `bakery.payments` | 3 | 3 | 2 | Trạng thái giao dịch thanh toán |
| `bakery.dlq` | 3 | 3 | 2 | Dead Letter Queue: chứa dữ liệu lỗi, sai schema |
| `bakery.bench.p1` | 1 | 3 | 2 | Dùng cho thí nghiệm đo hiệu năng 1 partition (Tuần 3) |
| `bakery.bench.p3` | 3 | 3 | 2 | Dùng cho thí nghiệm đo hiệu năng 3 partition (Tuần 3) |
| `bakery.bench.p6` | 6 | 3 | 2 | Dùng cho thí nghiệm đo hiệu năng 6 partition (Tuần 3) |

---

## 5. Các lệnh quản lý thông dụng

- **Xem trạng thái các container:**
  ```powershell
  docker compose ps
  ```

- **Xem log của một broker (ví dụ broker 1):**
  ```powershell
  docker logs kafka-1 --tail 50 -f
  ```

- **Dừng hệ thống (vẫn giữ dữ liệu):**
  ```powershell
  docker compose down
  ```

- **Reset trắng dữ liệu (xóa toàn bộ topic và message để làm lại từ đầu):**
  ```powershell
  docker compose down -v
  ```

- **Thử nghiệm chịu lỗi (Tắt thử 1 broker kiểm tra failover):**
  ```powershell
  docker stop kafka-2   # Tắt broker 2
  docker start kafka-2  # Bật lại broker 2
  ```

