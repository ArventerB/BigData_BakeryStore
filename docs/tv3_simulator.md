# Ghi chú kỹ thuật TV3 – Simulator và dữ liệu

Dùng cho Chương 3 (Dữ liệu và yêu cầu), Chương 5 (Cài đặt) và Chương 6 (Thực nghiệm).
Code: [`simulator/`](../simulator/). Hướng dẫn chạy: [`simulator/README.md`](../simulator/README.md).
Báo cáo 5V đầy đủ của tập dữ liệu: `results/profile_dataset_oct2026.md` (sinh bằng `simulator/profile_dataset.py`).
Tải tập dữ liệu đầy đủ (898 MB): <https://github.com/ArventerB/BigData_BakeryStore/releases/tag/dataset-oct2026>.

## 1. Vì sao cần simulator

Web tiệm bánh thật chỉ có vài người dùng thử, không đủ để chứng minh bài toán cần Big Data
(đề bài mục 5.2). Simulator đóng vai một chuỗi 7 cửa hàng (TP.HCM, Hà Nội, Đà Nẵng) với web và app.
Nó sinh event **đúng cùng data contract** với tracker của web, nên Collector và Kafka không phân biệt được
event thật với event mô phỏng. Đầu ra đi theo 3 đường:

| Chế độ | Đường đi | Dùng để |
|---|---|---|
| `api` | Simulator → Collector `/events` → Kafka `bakery.*` / `bakery.dlq` | demo, kiểm tra Veracity, chạy dài |
| `kafka` | Simulator → Kafka (cấu hình producer của TV1) | đo giới hạn Kafka, thí nghiệm partition |
| `file` | Simulator → `date=YYYY-MM-DD/events.jsonl.gz` + `dim/*.csv` | tập dữ liệu lịch sử lớn, bàn giao nhóm Storage |

Topic, broker và cấu hình producer **đọc trực tiếp từ `scripts/` của TV1**: `bakery.user-events`
(`acks=1`, batch 128 KB, linger 20 ms) và `bakery.orders / payments / dlq` (`acks=all`, retries 5, idempotence).
Có test so tên topic với `scripts/create_topics.ps1`, nên nếu TV1 đổi tên topic mà simulator không đổi theo thì test sẽ báo.

## 2. Mô hình hoá hành vi khách

```
page_view (trang đích, có utm) ─┬─ 30% thoát ngay
                                └─> search? (văn bản tự do) -> product_view (1–6 lần) -> add_to_cart
                                    -> remove_from_cart? -> checkout_start -> order_placed
                                    -> payment (lỗi theo phương thức, 50% thử lại phương thức khác)
```

| Yếu tố | Cách mô phỏng | Phân tích được gì (Value) |
|---|---|---|
| Khách | 75% thành viên `U…` (5% khách quen chiếm 15% lượt), 25% vãng lai `A…`; số thành viên co giãn theo quy mô (~4 phiên/người/tháng) | phân khúc khách, tỷ lệ mua lại |
| Cửa hàng | mỗi thành viên có cửa hàng gần nhà, 75% đơn đặt ở đó | doanh thu theo vùng |
| Nguồn truy cập | direct, google, facebook, tiktok, zalo, email (ghi vào utm); mỗi kênh có ý định mua và danh mục quan tâm riêng | hiệu quả kênh marketing |
| Voucher | khách từ chiến dịch có voucher thì 60% dùng voucher đó | hiệu quả khuyến mãi |
| Theo giờ | sáng ưu tiên bánh mì / cà phê, tối ưu tiên bánh kem; 24 hệ số lưu lượng theo giờ; cuối tuần ×1,35 | giờ cao điểm, gợi ý sản phẩm theo giờ |
| Tìm kiếm | từ khoá + hậu tố, 35% gõ không dấu, 6% gõ nhầm, 5% viết hoa; sau khi tìm thì xem đúng danh mục | phân tích văn bản, tìm kiếm → chuyển đổi |
| Thanh toán | tỷ lệ lỗi: thẻ 12%, vnpay 9%, momo 5%, tiền mặt 1% | phương thức nào gây mất đơn |
| Giao hàng | đơn có bánh kem 65% giao tận nơi, còn lại 35% | nhu cầu giao hàng |
| Nhất quán | tổng tiền = Σ đơn giá × số lượng − voucher; `payment.amount` = tổng đơn; key Kafka = `user_id` | |

Toàn bộ tham số nằm trong `catalog.json`, nên đổi miền dữ liệu không cần sửa code.

## 3. Dữ liệu đạt 5V – số liệu đo trên tập `dataset_oct2026`

Lệnh sinh (tái lập được nhờ `--seed`; chi tiết ở README):

```
python simulator/simulator.py --mode file --start 2026-10-01 --days 30 --sessions-per-day 100000 \
    --workers 7 --dirty-rate 0.02 --late-rate 0.01 --seed 2026 --gzip --tag oct2026
python simulator/profile_dataset.py data/dataset_oct2026 --workers 7
```

### Volume

| Chỉ số | Giá trị |
|---|---|
| Số event | **19 012 828** (30 ngày, khoảng 634k event/ngày) |
| Số phiên / khách | 3 268 743 phiên, 1 366 583 khách (549 358 thành viên + 817 225 vãng lai) |
| Dung lượng | **5,26 GB** JSON thô, 877 MB nén gzip (×6,0), 290 byte/event |
| Bảng dim | `customers.csv` 562 500 dòng, `products.csv` 15, `stores.csv` 7 |
| Thời gian sinh | 404 s, 7 tiến trình song song, khoảng 47 000 event/s |
| Thời gian đọc lại toàn bộ để đo | 489 s, 7 tiến trình |

Đọc 19 triệu dòng bằng Python thuần mất 8 phút dù đã chạy 7 tiến trình, và tập này chỉ là 1 tháng
của một chuỗi nhỏ. Đây là lý do các nhóm sau cần lưu trữ phân tán (Data Lake, chia phân vùng theo ngày) và xử lý
phân tán (Spark), không thể chỉ dùng một file và Pandas.

### Velocity

| Chỉ số | Giá trị |
|---|---|
| Tập lịch sử | trung bình 7,1 event/s; phút cao điểm (25/10, 19:28) 22,9 event/s; giờ đông nhất (19h) gấp 36 lần giờ vắng nhất (2h) |
| Simulator → Kafka thật, có điều tốc | 3 000 ev/s mục tiêu → đạt 2 992 ev/s, 0 lỗi, ack p95 105 ms |
| Giờ cao điểm trên Kafka thật (`peak ×3`) | 5 000 → khoảng 13 000 → 5 000 ev/s, 0 lỗi (2 worker; muốn đạt đủ 15k thì thêm worker) |
| Ghi tối đa vào `bakery.bench.p1` / `p6` | 21 510 / 28 756 ev/s (4 worker); có 6 partition thì nhanh hơn khoảng 34% |
| Qua Collector (HTTP), tối đa | khoảng 28 000 ev/s, khứ hồi p50 59 ms / p95 81 ms |
| Chạy liên tục 10 phút qua Collector, `daily` | 892 193 event, 0 lỗi, p95 7,4 ms |
| Đến trễ / lệch thứ tự | 170 565 event (0,9%) có `event_time` trễ hơn 1 giờ so với dòng dữ liệu |

### Variety

- **3 loại dữ liệu**: event JSON Lines (bán cấu trúc), bảng dim CSV (có cấu trúc), văn bản tự do
  (câu tìm kiếm, URL kèm utm).
- **8 loại event với 8 schema khác nhau** chia ra 3 topic; `order_placed.items[]` là mảng lồng nhau.
- **2 nguồn**: app 55%, web 45%. **3 loại thiết bị**: mobile 73%, desktop 22%, tablet 5%.
  **6 kênh truy cập** ghi qua utm.
- **7 550 câu tìm kiếm khác nhau** từ 15 từ khoá gốc, 45% gõ không dấu. Ví dụ: `banh kem dau it ngot`,
  `bánh trung thu cho bé`, `COOKIE Q1`.

### Veracity

Dữ liệu chứa 2% event vi phạm contract, gồm 11 kiểu hỏng. Mỗi kiểu ứng với một lý do DLQ
(bảng chi tiết ở README), cùng 1% phiên gửi trễ 1–6 giờ. `profile_dataset.py` kiểm lại từng event theo cùng luật với Collector:

| Lý do | Phát hiện | Đã chèn |
|---|---:|---:|
| `schema_violation` | 207 029 | 207 029 |
| `event_time_in_future` | 34 525 | 34 525 |
| `missing_timezone` | 34 541 | 34 541 |
| `unknown_event_type` | 34 496 | 34 496 |
| `duplicate_event_id` | 34 467 | 34 467 |
| `not_an_object` | 34 480 | 34 480 |
| **Tổng** | **379 538** | **379 538** |

Khi gửi realtime qua Collector, số bị từ chối cũng bằng đúng số bẩn đã chèn
(ví dụ 753 / 753, với 0 lỗi gửi).

### Value

| KPI | Giá trị |
|---|---|
| Phễu (% phiên) | xem sản phẩm 70,1% → thêm giỏ 43,6% → checkout 17,3% → đặt đơn 12,1% → thanh toán 11,5% |
| Doanh thu | 126,8 tỷ đ / 376 289 đơn, giá trị đơn trung bình 336 875 đ |
| Theo danh mục | bánh kem chiếm 70% doanh thu (89,1 tỷ), sau đó là cookie 10,0 tỷ, bánh mì 8,6 tỷ |
| Theo kênh (chuyển đổi) | zalo 13,3%, facebook 12,9%, google 12,3%, direct 11,2%, tiktok 7,8% |
| Thanh toán lỗi | thẻ 11,7%, vnpay 9,1%, momo 5,0%, tiền mặt 1,0% |
| Khách hàng | 18% người mua quay lại mua tiếp; khách quen 2,51 đơn/tháng so với thành viên thường 1,22 |
| Cửa hàng | 5 cửa hàng HCM khoảng 19,1–19,3 tỷ mỗi cửa hàng; Đà Nẵng 15,5 tỷ, Hà Nội 15,3 tỷ |

Mỗi KPI ứng với một quyết định nghiệp vụ, ví dụ: giảm ngân sách TikTok, ưu tiên ví điện tử để giảm đơn lỗi khi
thanh toán, chuẩn bị nhiều bánh kem cho khung 18–20h. Đây là đầu vào cho các nhóm Analytics và Decision.

## 4. Kiểm chứng với Kafka thật (cụm 3 broker của TV1)

Sau khi gửi 137 268 event qua chế độ `kafka`, mình đọc lại toàn bộ topic bằng consumer:

| Kiểm tra | Kết quả |
|---|---|
| Số bản ghi | 137 268 gửi = 137 268 đọc lại (`user-events` 131 347, `orders` 2 909, `payments` 3 012) |
| Topic | chỉ có 7 topic `bakery.*` của TV1, 3 partition × 3 replica; không có topic lạ bị tự tạo |
| Trùng `event_id` | 0 |
| Key `user_id` | mỗi khách chỉ nằm trên 1 partition; không có event nào lệch thứ tự trong cùng phiên |
| Benchmark | offset của `bakery.bench.p1` / `p6` bằng đúng số đã gửi (712 023 / 643 014) |

## 5. Lỗi đã phát hiện và sửa trong quá trình test

1. Khi hết giờ, worker dừng giữa phiên nên số bẩn đã đếm lệch 1 so với số bị từ chối.
   Đã sửa để worker gửi nốt phiên đang dở.
2. Kiểu bẩn `future_time` lấy theo giờ máy, nên cùng seed vẫn ra dữ liệu khác.
   Đã sửa để lệch theo chính `event_time` của event.
3. Điều tốc cấp token cố định 50 ms mỗi vòng, nên khi gửi Kafka chậm thì thiếu khoảng 5% tốc độ.
   Đã sửa để cấp theo thời gian thực tế đã trôi qua (đạt 2 992/3 000 ev/s).
4. CSV theo chu kỳ chia cho độ dài chu kỳ cố định, nên khi báo cáo bị trễ thì tính event vào nhầm chu kỳ.
   Đã sửa để dùng cửa sổ thời gian thực tế.
5. Tập khách cố định 50k cho 3,3 triệu phiên làm mỗi khách quen đặt 103 đơn/tháng, không thực tế.
   Đã sửa để số thành viên co giãn theo quy mô (2,51 đơn/tháng).
6. Nén gzip mức 9 chiếm một nửa thời gian sinh. Đã chuyển sang mức 6: nhanh hơn khoảng 35%, file chỉ lớn hơn khoảng 5%.

## 6. Kiểm thử tự động

`python -m pytest simulator/tests` gồm 41 test:
- event đúng contract, phễu và tổng tiền nhất quán, cùng seed thì cùng dữ liệu, UUID đúng chuẩn v4;
- từng kiểu bẩn làm hỏng đúng chỗ;
- các profile lưu lượng; file dữ liệu khớp manifest;
- topic và cấu hình producer khớp với TV1;
- bộ test 5V: sinh một tập nhỏ rồi chạy profiler, yêu cầu Veracity khớp tuyệt đối, phễu giảm dần, google chuyển đổi
  cao hơn tiktok, thẻ lỗi nhiều hơn tiền mặt, khách quen mua nhiều hơn.

## 7. Hạn chế

- Dữ liệu là mô phỏng: hành vi được mô hình bằng xác suất do nhóm tự đặt, chưa hiệu chỉnh theo log thật của tiệm.
- Các số đo trên chạy trên 1 máy Windows, Kafka trong Docker Desktop. Độ trễ mạng giữa các container trên Windows làm
  độ trễ ack cao hơn so với chạy trên Linux.
- Chưa chạy liên tục 30–60 phút với Kafka thật cùng consumer của TV4 (việc của tuần 3).
