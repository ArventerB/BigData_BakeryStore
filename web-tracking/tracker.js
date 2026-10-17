/*
 * Tracker gắn vào web tiệm bánh (đồ án Công nghệ phần mềm) để gửi hành vi người dùng về Collector API.
 *
 * Cách dùng:
 *   <script src="tracker.js"></script>
 *   <script>
 *     ShopTracker.init({ endpoint: "http://localhost:8000/events", userId: currentUser?.id });
 *     ShopTracker.track("page_view", { page: location.pathname });
 *     ShopTracker.track("product_view", { product_id: "BK001", category: "banh_kem", price: 280000 });
 *     ShopTracker.track("add_to_cart", { product_id: "BK001", category: "banh_kem", price: 280000, quantity: 1 });
 *   </script>
 *
 * Event được gom lô (tối đa 20 event hoặc 3 giây) rồi gửi một request, khi đóng tab dùng sendBeacon.
 * Đơn hàng (order_placed) và thanh toán (payment) nên gửi từ BACKEND sau khi lưu DB thành công
 * để đảm bảo dữ liệu giao dịch chính xác (xem docs/data_contract.md).
 */
(function (global) {
  var cfg = { endpoint: "http://localhost:8000/events", batchSize: 20, flushMs: 3000, userId: null };
  var queue = [];
  var timer = null;

  function uuid() {
    if (global.crypto && crypto.randomUUID) return crypto.randomUUID();
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
      var r = (Math.random() * 16) | 0;
      return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
    });
  }

  function sessionId() {
    var k = "shop_sid", v = sessionStorage.getItem(k);
    if (!v) { v = uuid().replace(/-/g, "").slice(0, 16); sessionStorage.setItem(k, v); }
    return v;
  }

  function anonymousId() {
    var k = "shop_anon", v = localStorage.getItem(k);
    if (!v) { v = "A" + uuid().replace(/-/g, "").slice(0, 12); localStorage.setItem(k, v); }
    return v;
  }

  function device() {
    var w = global.innerWidth || 1024;
    return w < 768 ? "mobile" : w < 1100 ? "tablet" : "desktop";
  }

  function flush(useBeacon) {
    if (!queue.length) return;
    var body = JSON.stringify(queue);
    queue = [];
    if (useBeacon && navigator.sendBeacon) {
      // text/plain để tránh preflight CORS; Collector vẫn đọc body như JSON
      navigator.sendBeacon(cfg.endpoint, new Blob([body], { type: "text/plain" }));
    } else {
      fetch(cfg.endpoint, { method: "POST", headers: { "Content-Type": "application/json" }, body: body, keepalive: true })
        .catch(function (e) { console.warn("[tracker] gửi lỗi", e); });
    }
  }

  var ShopTracker = {
    init: function (opts) {
      for (var k in opts) cfg[k] = opts[k];
      global.addEventListener("pagehide", function () { flush(true); });
    },
    setUser: function (id) { cfg.userId = id; },
    track: function (eventType, props) {
      var e = {
        event_id: uuid(),
        event_type: eventType,
        event_time: new Date().toISOString(),
        user_id: String(cfg.userId || anonymousId()),
        session_id: sessionId(),
        source: "web",
        device: device(),
        schema_version: 1
      };
      for (var k in props || {}) e[k] = props[k];
      queue.push(e);
      if (queue.length >= cfg.batchSize) flush(false);
      else if (!timer) timer = setTimeout(function () { timer = null; flush(false); }, cfg.flushMs);
    },
    flush: flush
  };

  global.ShopTracker = ShopTracker;
})(window);
