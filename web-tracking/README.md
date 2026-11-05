# web-tracking (TV2)

`tracker.js` gắn vào web The Sweets để gửi hành vi khách lên Collector (`POST /events`).

## Gắn vào web

Chép `tracker.js` vào thư mục `public/` của frontend, rồi thêm vào `index.html`:

```html
<script src="/tracker.js"></script>
<script>
  BakeryTracker.init({ endpoint: "http://localhost:8000/events", debug: true }); // tự gửi page_view trang đầu
</script>
```

Gọi ở các chỗ tương ứng trong frontend:

| Hàm | Gọi khi |
|---|---|
| `trackPageView(page?)` | đổi route (SPA). Trang đầu tiên được gửi tự động |
| `trackSearch(query)` | khách bấm tìm kiếm |
| `trackProductView({product_id, category, price})` | mở trang chi tiết sản phẩm |
| `trackAddToCart({product_id, category, price, quantity})` | thêm vào giỏ |
| `trackRemoveFromCart({...})` | xoá khỏi giỏ |
| `trackCheckoutStart()` | bấm "Thanh toán" (gửi ngay cả lô) |
| `setUserId(id)` / `resetUser()` | đăng nhập / đăng xuất |
| `sendBatch()` | gửi ngay các event đang chờ |

- `user_id` lưu trong `localStorage` (khách vãng lai là `A` + 10 ký tự hex). `session_id` sang phiên mới sau 30 phút không thao tác.
- Event được gom theo lô: gửi khi đủ 10 event, sau mỗi 3 giây, hoặc khi khách rời trang (dùng `sendBeacon`).

## Đơn hàng và thanh toán

`order_placed` và `payment` do **backend** gửi sau khi lưu đơn và thanh toán vào DB, không gửi từ trình duyệt,
vì số tiền phía trình duyệt có thể bị sửa. Frontend gửi kèm `BakeryTracker.getIdentity()` khi đặt hàng, để
backend dùng cùng `user_id` và `session_id`. Trường bắt buộc của từng loại event xem trong `collector/app/schemas.py`.
