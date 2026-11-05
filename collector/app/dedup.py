"""Chống gửi trùng theo event_id bằng LRU cache trong bộ nhớ.

Vì sao cần: client (web/app/simulator) gửi lại cả lô khi mạng chập chờn hoặc Collector trả 503,
nên cùng một event có thể tới nhiều lần. Bản đầu tiên vào topic chính, các bản sau vào DLQ
với lý do duplicate_event_id.

Giới hạn (cần nói rõ khi bảo vệ):
- Chỉ nhớ DEDUP_CACHE_SIZE id gần nhất; bản trùng đến muộn hơn cửa sổ đó sẽ lọt qua.
- Bộ nhớ nằm trong một tiến trình: chạy nhiều worker/nhiều Collector thì mỗi cái có cache riêng,
  restart thì mất. Vì vậy uvicorn chạy 1 worker. Muốn chặt hơn thì dùng Redis, hoặc để tầng Storage lọc lại.
"""
from collections import OrderedDict


class DedupCache:
    """LRU: id mới hoặc vừa gặp lại được đưa xuống cuối; đầy thì bỏ id cũ nhất ở đầu.

    Không cần khoá: chỉ được gọi từ event loop của FastAPI (một luồng), và giữa lúc kiểm tra
    với lúc thêm không có await nào.
    """

    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError("capacity phải > 0")
        self.capacity = capacity
        self._ids: OrderedDict[str, None] = OrderedDict()

    def add(self, event_id: str) -> bool:
        """Ghi nhận event_id. Trả về True nếu là lần đầu, False nếu đã gặp (bản trùng)."""
        if event_id in self._ids:
            self._ids.move_to_end(event_id)
            return False
        self._ids[event_id] = None
        if len(self._ids) > self.capacity:
            self._ids.popitem(last=False)
        return True

    def discard(self, event_id: str) -> None:
        """Quên event_id: dùng khi gửi Kafka thất bại, để lần client gửi lại không bị coi là trùng."""
        self._ids.pop(event_id, None)

    def __len__(self) -> int:
        return len(self._ids)
