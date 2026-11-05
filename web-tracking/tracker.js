/*!
 * tracker.js - gắn vào web tiệm bánh The Sweets để gửi hành vi người dùng về Collector API (TV2).
 *
 * Cách dùng (đặt trước </body>):
 *   <script src="/tracker.js"></script>
 *   <script>
 *     BakeryTracker.init({ endpoint: "http://localhost:8000/events" });   // tự gửi page_view trang đầu
 *     BakeryTracker.trackProductView({ product_id: "P001", category: "banh_mi", price: 25000 });
 *     BakeryTracker.trackAddToCart({ product_id: "P001", category: "banh_mi", price: 25000, quantity: 2 });
 *     BakeryTracker.setUserId("U0000123");   // sau khi khách đăng nhập
 *   </script>
 *
 * Event sinh ra theo đúng data contract trong collector/app/schemas.py.
 * Đơn hàng (order_placed) và thanh toán (payment) KHÔNG gửi từ trình duyệt mà do backend gửi,
 * vì số tiền phía trình duyệt có thể bị sửa.
 *
 * Gửi theo lô: event được gom vào hàng đợi, gửi khi đủ batchSize, sau mỗi flushInterval,
 * hoặc ngay khi khách rời trang (dùng fetch keepalive / sendBeacon để không mất event cuối).
 */
(function (window) {
  "use strict";

  var SCHEMA_VERSION = 1;
  var UID_KEY = "bakery_uid";
  var SESSION_KEY = "bakery_session";
  var SESSION_TIMEOUT_MS = 30 * 60 * 1000; // 30 phút không thao tác thì sang phiên mới

  var cfg = {
    endpoint: "http://localhost:8000/events",
    source: "web",
    batchSize: 10,
    flushInterval: 3000,
    autoPageView: true,
    debug: false,
  };
  var queue = [];
  var timer = null;
  var started = false;

  // ------------------------------------------------------------ tiện ích
  function log() {
    if (cfg.debug && window.console) {
      var args = Array.prototype.slice.call(arguments);
      args.unshift("[BakeryTracker]");
      console.log.apply(console, args);
    }
  }

  function safeGet(storage, key) {
    try { return storage.getItem(key); } catch (e) { return null; } // trình duyệt chặn storage (ẩn danh)
  }

  function safeSet(storage, key, value) {
    try { storage.setItem(key, value); } catch (e) { /* bỏ qua, vẫn chạy được */ }
  }

  function randomHex(bytes) {
    var arr = new Uint8Array(bytes);
    if (window.crypto && window.crypto.getRandomValues) {
      window.crypto.getRandomValues(arr);
    } else {
      for (var i = 0; i < bytes; i++) arr[i] = Math.floor(Math.random() * 256);
    }
    var s = "";
    for (var j = 0; j < arr.length; j++) s += ("0" + arr[j].toString(16)).slice(-2);
    return s;
  }

  function uuid4() {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      return window.crypto.randomUUID(); // chỉ có trên HTTPS hoặc localhost
    }
    var h = randomHex(16).split("");
    h[12] = "4";                                            // version 4
    h[16] = "89ab"[parseInt(h[16], 16) & 3];                // variant RFC 4122
    h = h.join("");
    return h.slice(0, 8) + "-" + h.slice(8, 12) + "-" + h.slice(12, 16) + "-" + h.slice(16, 20) + "-" + h.slice(20);
  }

  function detectDevice() {
    var ua = navigator.userAgent || "";
    if (/iPad|Tablet|PlayBook|Silk|(Android(?!.*Mobile))/i.test(ua)) return "tablet";
    if (/Mobi|iPhone|iPod|Android|Windows Phone/i.test(ua)) return "mobile";
    return "desktop";
  }

  // ------------------------------------------------------------ định danh khách và phiên
  function getUserId() {
    var uid = safeGet(window.localStorage, UID_KEY);
    if (!uid) {
      uid = "A" + randomHex(5); // khách vãng lai: "A" + 10 ký tự hex, giống simulator
      safeSet(window.localStorage, UID_KEY, uid);
    }
    return uid;
  }

  function getSessionId() {
    var now = Date.now();
    var s = null;
    try { s = JSON.parse(safeGet(window.localStorage, SESSION_KEY) || "null"); } catch (e) { s = null; }
    if (!s || !s.id || now - s.last > SESSION_TIMEOUT_MS) {
      s = { id: randomHex(8), last: now };
      log("phiên mới", s.id);
    }
    s.last = now;
    safeSet(window.localStorage, SESSION_KEY, JSON.stringify(s));
    return s.id;
  }

  function baseEvent(type) {
    return {
      event_id: uuid4(),
      event_type: type,
      event_time: new Date().toISOString(), // luôn có hậu tố Z (UTC)
      user_id: getUserId(),
      session_id: getSessionId(),
      source: cfg.source,
      device: detectDevice(),
      schema_version: SCHEMA_VERSION,
    };
  }

  function currentPage() {
    return (window.location.pathname || "/") + (window.location.search || "");
  }

  function toNumber(v) {
    var n = typeof v === "number" ? v : parseFloat(v);
    return isFinite(n) ? n : 0;
  }

  // ------------------------------------------------------------ gửi
  function enqueue(event) {
    queue.push(event);
    log("thêm", event.event_type, event);
    if (queue.length >= cfg.batchSize) sendBatch();
  }

  /**
   * Gửi toàn bộ event đang chờ. useBeacon = true khi trang sắp đóng.
   * Lỗi mạng: đưa lô trở lại hàng đợi để gửi lần sau (Collector có chống trùng theo event_id).
   */
  function sendBatch(useBeacon) {
    if (!queue.length) return;
    var batch = queue.splice(0, queue.length);
    var body = JSON.stringify(batch);

    if (useBeacon && navigator.sendBeacon) {
      // text/plain để trình duyệt không cần preflight CORS; Collector tự đọc JSON từ body
      var ok = navigator.sendBeacon(cfg.endpoint, new Blob([body], { type: "text/plain" }));
      if (ok) return;
    }
    if (!window.fetch) return;
    window.fetch(cfg.endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: body,
      keepalive: body.length < 60000, // keepalive giới hạn 64 KB
    }).then(function (res) {
      if (res.status >= 500) throw new Error("Collector trả " + res.status);
      return res.json();
    }).then(function (data) {
      log("đã gửi", data);
    }).catch(function (err) {
      log("gửi lỗi, sẽ thử lại", err);
      queue = batch.concat(queue).slice(-500); // giữ tối đa 500 event để không phình bộ nhớ
    });
  }

  // ------------------------------------------------------------ API công khai
  var Tracker = {
    /** Cấu hình và bắt đầu theo dõi. options: endpoint, source, batchSize, flushInterval, autoPageView, debug */
    init: function (options) {
      options = options || {};
      for (var k in options) if (Object.prototype.hasOwnProperty.call(options, k)) cfg[k] = options[k];
      if (started) return Tracker;
      started = true;
      timer = window.setInterval(function () { sendBatch(false); }, cfg.flushInterval);
      // khách đóng tab / chuyển app: gửi nốt bằng beacon
      document.addEventListener("visibilitychange", function () {
        if (document.visibilityState === "hidden") sendBatch(true);
      });
      window.addEventListener("pagehide", function () { sendBatch(true); });
      if (cfg.autoPageView) Tracker.trackPageView();
      log("khởi động", cfg);
      return Tracker;
    },

    /** Gọi sau khi khách đăng nhập để gắn event về đúng tài khoản (mã khách trong DB). */
    setUserId: function (userId) {
      if (userId) safeSet(window.localStorage, UID_KEY, String(userId));
    },

    /** Khách đăng xuất: quay về mã khách vãng lai mới. */
    resetUser: function () {
      try { window.localStorage.removeItem(UID_KEY); window.localStorage.removeItem(SESSION_KEY); } catch (e) { /* bỏ qua */ }
    },

    /** page: đường dẫn trang, mặc định lấy từ thanh địa chỉ (gồm cả ?utm_...). Gọi lại khi SPA đổi route. */
    trackPageView: function (page) {
      var e = baseEvent("page_view");
      e.page = page || currentPage();
      enqueue(e);
    },

    trackSearch: function (query) {
      if (!query) return;
      var e = baseEvent("search");
      e.search_query = String(query).slice(0, 256);
      e.page = currentPage();
      enqueue(e);
    },

    /** product: { product_id, category, price } */
    trackProductView: function (product) {
      var e = baseEvent("product_view");
      e.page = currentPage();
      e.product_id = String(product.product_id);
      e.category = String(product.category);
      e.price = toNumber(product.price);
      enqueue(e);
    },

    /** item: { product_id, category, price, quantity } */
    trackAddToCart: function (item) {
      var e = baseEvent("add_to_cart");
      e.product_id = String(item.product_id);
      e.category = String(item.category);
      e.price = toNumber(item.price);
      e.quantity = Math.max(1, Math.round(toNumber(item.quantity) || 1));
      enqueue(e);
    },

    /** item: { product_id, category, price, quantity } */
    trackRemoveFromCart: function (item) {
      var e = baseEvent("remove_from_cart");
      e.product_id = String(item.product_id);
      e.category = String(item.category);
      e.price = toNumber(item.price);
      e.quantity = Math.max(1, Math.round(toNumber(item.quantity) || 1));
      enqueue(e);
    },

    trackCheckoutStart: function () {
      enqueue(baseEvent("checkout_start"));
      sendBatch(false); // khách sắp chuyển trang thanh toán: gửi ngay
    },

    sendBatch: function () { sendBatch(false); },

    /** Thông tin để backend gắn vào order_placed / payment cho cùng phiên. */
    getIdentity: function () {
      return { user_id: getUserId(), session_id: getSessionId(), device: detectDevice() };
    },
  };

  window.BakeryTracker = Tracker;
})(window);
