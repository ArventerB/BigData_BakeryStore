"""
Consumer kiểm chứng (measurement consumer) cho benchmark TV4 – Bakery Store.

- Gán (assign) trực tiếp từng partition tại offset bắt đầu của lần chạy → chỉ đọc đúng bản ghi của lần chạy đó,
  không phụ thuộc consumer group / rebalance.
- Đo mỗi giây: số bản ghi nhận được, độ trễ end-to-end p50/p95/p99, consumer lag.
    độ trễ end-to-end = thời điểm ứng dụng consumer nhận bản ghi − CreateTime (producer gán lúc gọi produce()).
    Producer và consumer chạy cùng một máy nên dùng chung đồng hồ, không lệch giờ.
    consumer lag = tổng (high watermark − vị trí đã đọc) trên các partition, hỏi broker mỗi giây (luồng riêng).
- Đếm chính xác mất / trùng:
    mode "header"   : dựa vào header tv4 = (run_id, worker, seq) do load_generator.py gắn.
    mode "event_id" : dựa vào trường event_id trong JSON (dùng với simulator của TV3).
- Có thể chạy nhiều tiến trình consumer, mỗi tiến trình giữ một nhóm partition riêng.
"""
import json
import multiprocessing as mp
import queue
import threading
import time
import traceback
from collections import Counter

from bench_utils import HEADER, Reservoir, pct


def consumer_worker(cid: int, cfg: dict, assignments: list, done_evt, start_at, out_q: mp.Queue):
    try:
        _consumer_worker(cid, cfg, assignments, done_evt, start_at, out_q)
    except Exception:
        out_q.put({"type": "error", "cid": cid, "error": traceback.format_exc()})


def _consumer_worker(cid: int, cfg: dict, assignments: list, done_evt, start_at, out_q: mp.Queue):
    from confluent_kafka import Consumer, TopicPartition

    bootstrap, run_id, mode = cfg["bootstrap"], cfg.get("run_id", 0), cfg["mode"]
    warmup = cfg.get("warmup_sec", 3.0)
    idle_timeout = cfg.get("idle_timeout", 20.0)
    max_drain = cfg.get("max_drain_sec", 600.0)

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"tv4-verify-{run_id}-{cid}",
                  "client.id": f"tv4-verify-{cid}", "enable.auto.commit": False,
                  "enable.partition.eof": False})
    tps = [TopicPartition(t, p, off) for t, p, off in assignments]
    c.assign(tps)
    pos = {(t, p): off for t, p, off in assignments}

    # Luồng đo lag: dùng 1 client riêng để hỏi high watermark, không chặn vòng đọc chính
    wm = Consumer({"bootstrap.servers": bootstrap, "group.id": f"tv4-wm-{run_id}-{cid}",
                   "client.id": f"tv4-wm-{cid}", "enable.auto.commit": False})
    lag_series: dict[int, int] = {}
    lag_state = {"last": None, "t": 0.0}
    stop_lag = threading.Event()

    def lag_loop():
        while not stop_lag.is_set():
            t = time.time()
            total, ok = 0, True
            for (tp, pn) in list(pos):
                try:
                    _, hi = wm.get_watermark_offsets(TopicPartition(tp, pn), timeout=2.0, cached=False)
                    total += max(0, hi - pos[(tp, pn)])
                except Exception:
                    ok = False  # broker đang lỗi / bầu lại leader: bỏ qua mẫu này
            if ok:
                s0 = start_at.value
                if s0 > 0 and t >= s0:
                    lag_series[int(t - s0)] = total
                lag_state.update(last=total, t=time.time())
            stop_lag.wait(max(0.05, 1.0 - (time.time() - t)))

    threading.Thread(target=lag_loop, daemon=True).start()
    c.consume(1, 0.5)  # khởi động kết nối trước khi producer bắt đầu
    out_q.put({"type": "ready", "cid": cid})

    unpack = HEADER.unpack
    received = unique = dup = foreign = 0
    seen_bits: dict[int, bytearray] = {}
    seen_ids: set[str] = set()
    by_topic = Counter()
    sec_cnt = Counter()
    sec_lat: dict[int, Reservoir] = {}
    lat_all = Reservoir(100_000)
    first_msg = last_msg = None
    done_at = None
    timed_out = False

    while True:
        # consume() chờ ĐỦ num_messages hoặc hết timeout → timeout phải rất nhỏ, nếu không mỗi bản ghi bị cộng
        # thêm thời gian chờ gom lô của chính công cụ đo (với 0,2s, e2e ở tải thấp bị thổi lên ~100 ms).
        msgs = c.consume(5000, 0.005)
        now = time.time()
        if msgs:
            s0 = start_at.value
            sec = int(now - s0) if s0 else -1
            now_ms, warm_ms = now * 1000.0, (s0 + warmup) * 1000.0
            res = sec_lat.get(sec)
            if res is None:
                res = sec_lat[sec] = Reservoir(2_000)
            n_ok = 0
            for m in msgs:
                if m.error():
                    continue
                n_ok += 1
                ts = m.timestamp()[1]
                lat = now_ms - ts
                res.add(lat)
                if ts >= warm_ms:
                    lat_all.add(lat)
                if mode == "header":
                    h = m.headers()
                    if not h or h[0][0] != "tv4":
                        foreign += 1
                        continue
                    run, wid, seq = unpack(h[0][1])
                    if run != run_id:
                        foreign += 1
                        continue
                    ba = seen_bits.get(wid)
                    if ba is None:
                        ba = seen_bits[wid] = bytearray(1 << 20)
                    if seq >= len(ba):
                        ba.extend(bytes(max(seq + 1 - len(ba), len(ba))))
                    if ba[seq]:
                        dup += 1
                    else:
                        ba[seq] = 1
                        unique += 1
                else:
                    by_topic[m.topic()] += 1
                    try:
                        eid = json.loads(m.value()).get("event_id")
                    except (ValueError, AttributeError):
                        eid = None
                    if eid is None:
                        foreign += 1
                    elif eid in seen_ids:
                        dup += 1
                    else:
                        seen_ids.add(eid)
                        unique += 1
            received += n_ok
            sec_cnt[sec] += n_ok
            if n_ok:
                first_msg = first_msg or now
                last_msg = now
            for tp in c.position(tps):
                if tp.offset >= 0:
                    pos[(tp.topic, tp.partition)] = tp.offset

        if done_evt.is_set():
            if done_at is None:
                done_at = now
            # Kết thúc khi mẫu lag mới nhất (đo SAU khi producer xong) bằng 0 → đã đọc hết những gì broker có
            if lag_state["t"] > done_at + 0.5 and lag_state["last"] == 0:
                break
            if now - (last_msg or done_at) > idle_timeout or now - done_at > max_drain:
                timed_out = True
                break

    stop_lag.set()
    c.close()
    wm.close()

    sec_rows = {}
    for s, r in sec_lat.items():
        sec_rows[s] = [sec_cnt.get(s, 0), pct(r.data, 50), pct(r.data, 95), pct(r.data, 99)]
    out_q.put({
        "type": "done", "cid": cid, "received": received, "unique": unique, "duplicates": dup,
        "foreign": foreign, "by_topic": dict(by_topic), "first_msg": first_msg, "last_msg": last_msg,
        "done_at": done_at, "timed_out": timed_out, "sec_rows": sec_rows, "lag": lag_series,
        "lat_sample": lat_all.data, "lat_n": lat_all.n,
        "unique_by_worker": {w: ba.count(1) for w, ba in seen_bits.items()},
    })


class ConsumerGroup:
    """N tiến trình consumer kiểm chứng, mỗi tiến trình giữ một phần partition (chia vòng tròn)."""

    def __init__(self, cfg: dict, assignments: list, start_at, n_proc: int = 1):
        n_proc = max(1, min(n_proc, len(assignments)))
        parts = [assignments[i::n_proc] for i in range(n_proc)]
        self.q: mp.Queue = mp.Queue()
        self.done_evt = mp.Event()
        self.procs = [mp.Process(target=consumer_worker, args=(i, cfg, parts[i], self.done_evt, start_at, self.q),
                                 daemon=True) for i in range(n_proc)]

    def start(self, timeout: float = 60):
        for p in self.procs:
            p.start()
        ready = 0
        while ready < len(self.procs):
            msg = self.q.get(timeout=timeout)
            if msg["type"] == "error":
                raise RuntimeError(f"Consumer {msg['cid']} lỗi:\n{msg['error']}")
            ready += msg["type"] == "ready"

    def finish(self, timeout: float) -> list[dict]:
        self.done_evt.set()
        done, deadline = [], time.time() + timeout
        while len(done) < len(self.procs):
            try:
                msg = self.q.get(timeout=max(1.0, deadline - time.time()))
            except queue.Empty:
                raise TimeoutError("Consumer kiểm chứng không kết thúc đúng hạn")
            if msg["type"] == "error":
                raise RuntimeError(f"Consumer {msg['cid']} lỗi:\n{msg['error']}")
            if msg["type"] == "done":
                done.append(msg)
        for p in self.procs:
            p.join(10)
        return done
