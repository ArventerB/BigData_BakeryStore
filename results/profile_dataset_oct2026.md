# Hồ sơ 5V – `dataset_oct2026`

Đọc toàn bộ tập dữ liệu trong 488.6 s. Số liệu do `simulator/profile_dataset.py` tính.

## Volume

- 19.012.828 event (18.633.290 hợp lệ), 3.268.743 phiên, 1.366.583 khách (549.358 thành viên, 817.225 vãng lai)
- Dung lượng: 5.261 MB JSON thô, 877 MB sau nén (×6.0), trung bình 290 byte/event, 30 ngày

## Velocity

- Trải dài 725.2 giờ, trung bình 7.14 event/s
- Phút cao điểm 2026-10-25 19:28: 22.85 event/s
- Giờ cao điểm 19h, vắng nhất 2h, chênh ×36.2
- 170.565 event hợp lệ nhưng đến trễ / lệch thứ tự > 1 giờ

## Variety

- 8 loại event, 8 tổ hợp trường khác nhau; trường lồng nhau: order_placed.items[] (product_id, category, unit_price, quantity)
- Định dạng: JSON Lines nén gzip (event), CSV (bảng dim), JSON (manifest); bảng dim: dim/products.csv (15 dòng), dim/stores.csv (7 dòng), dim/customers.csv (562.500 dòng)
- Nguồn: {'app': 10248487, 'web': 8384803}; thiết bị: {'mobile': 13603500, 'desktop': 4188760, 'tablet': 841030}
- Kênh truy cập (utm): {'direct': 1473240, 'zalo': 257953, 'tiktok': 320635, 'google': 579859, 'facebook': 451344, 'email': 161108}
- 7.550 câu tìm kiếm khác nhau, 45.1% gõ không dấu. Ví dụ: 'cookie', 'bánh trung thu cho bé', 'mousse khuyến mãi', 'tiramisu khuyến mãi', 'banh kem dau it ngot', 'bong lan trung muoi it ngot', 'tiramisu gan day', 'COOKIE Q1'

## Veracity

- 379.538 event vi phạm contract (2.0%)

| Lý do | Phát hiện | Đã chèn |
|---|---:|---:|
| `duplicate_event_id` | 34.467 | 34.467 |
| `event_time_in_future` | 34.525 | 34.525 |
| `missing_timezone` | 34.541 | 34.541 |
| `not_an_object` | 34.480 | 34.480 |
| `schema_violation` | 207.029 | 207.029 |
| `unknown_event_type` | 34.496 | 34.496 |

- Khớp hoàn toàn với số đã chèn: **có**

## Value

| Bước phễu | Phiên | % phiên |
|---|---:|---:|
| page_view | 3.244.139 | 99.25 |
| product_view | 2.290.548 | 70.07 |
| add_to_cart | 1.424.337 | 43.57 |
| checkout_start | 564.658 | 17.27 |
| order_placed | 395.560 | 12.1 |
| payment_success | 376.289 | 11.51 |

- 376.289 đơn đã thanh toán, doanh thu 126.762.375.100 đ, giá trị đơn trung bình 336.875 đ
- Doanh thu theo danh mục: banh_kem 89.081.015.000, cookie 10.049.123.000, banh_mi 8.564.424.900, banh_ngot 8.147.880.200, banh_trung_thu 5.960.230.750, do_uong 4.959.701.250
- Doanh thu theo cửa hàng: HCM-TD 19.302.161.850, HCM-Q3 19.271.847.200, HCM-Q7 19.204.320.100, HCM-Q1 19.103.740.550, HCM-Q5 19.074.842.900, DN-HC 15.471.369.000, HN-HK 15.334.093.500
- Tỷ lệ chuyển đổi theo kênh (%): {'direct': 11.24, 'google': 12.32, 'facebook': 12.86, 'tiktok': 7.82, 'zalo': 13.33, 'email': 11.81}
- Tỷ lệ thanh toán lỗi theo phương thức (%): {'bank_transfer': 6.82, 'card': 11.74, 'cash': 0.95, 'momo': 5.04, 'vnpay': 9.06, 'zalopay': 6.19}
- 297.905 người mua, 17.98% mua lại; số đơn / người mua theo hạng: {'member': 1.22, 'guest': 1.0, 'loyal': 2.51}
