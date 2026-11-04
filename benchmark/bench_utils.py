"""Tiện ích dùng chung cho bộ benchmark TV4: lấy mẫu độ trễ, tính phân vị, header kiểm chứng."""
import math
import os
import random
import struct

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9096")

# Mỗi message benchmark mang 1 header "tv4" = (run_id, worker_id, seq) để consumer đếm chính xác mất / trùng.
HEADER_KEY = "tv4"
HEADER = struct.Struct(">IHI")  # run_id uint32 | worker_id uint16 | seq uint32


class Reservoir:
    """Mẫu ngẫu nhiên đều (reservoir sampling) có kích thước cố định, dùng để tính phân vị độ trễ.

    Giữ tối đa `cap` giá trị đại diện cho toàn bộ `n` giá trị đã thấy, chi phí O(1) mỗi lần add.
    """

    __slots__ = ("cap", "n", "data", "_rnd")

    def __init__(self, cap: int = 50_000):
        self.cap = cap
        self.n = 0
        self.data: list[float] = []
        self._rnd = random.random

    def add(self, v: float) -> None:
        self.n += 1
        if len(self.data) < self.cap:
            self.data.append(v)
        else:
            j = int(self._rnd() * self.n)
            if j < self.cap:
                self.data[j] = v


def pct(values, p: float):
    """Phân vị theo phương pháp nearest-rank. Trả về None nếu không có dữ liệu."""
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, math.ceil(p / 100 * len(s)) - 1))
    return round(s[k], 2)


def weighted_pct(samples: list[tuple[list[float], int]], p: float):
    """Phân vị trên nhiều reservoir, mỗi reservoir (data, n) đại diện cho n giá trị gốc."""
    pts = []
    for data, n in samples:
        if data:
            w = n / len(data)
            pts.extend((v, w) for v in data)
    if not pts:
        return None
    pts.sort()
    total = sum(w for _, w in pts)
    target = p / 100 * total
    acc = 0.0
    for v, w in pts:
        acc += w
        if acc >= target:
            return round(v, 2)
    return round(pts[-1][0], 2)


def latency_summary(samples: list[tuple[list[float], int]]) -> dict:
    return {
        "p50": weighted_pct(samples, 50),
        "p95": weighted_pct(samples, 95),
        "p99": weighted_pct(samples, 99),
        "max": round(max((max(d) for d, _ in samples if d), default=0.0), 2) or None,
        "samples": sum(len(d) for d, _ in samples),
    }
