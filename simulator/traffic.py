"""Kịch bản lưu lượng: hệ số nhân tốc độ gửi theo thời gian chạy.

    constant  tốc độ đều = --rate
    peak      giờ cao điểm: 1/3 giữa thời gian chạy tăng lên --peak-mult lần (dạng bậc, dễ đo so sánh)
    daily     nén 1 ngày vào --day-seconds giây, tốc độ theo hourly_traffic trong catalog.json
              (cao điểm sáng 7–9h, trưa 12h, tối 18–20h); trung bình cả ngày ≈ --rate
    flash     flash sale: chạy đều, tới giữa thời gian tăng vọt --peak-mult lần rồi giảm dần
"""
import math

PROFILES = ("constant", "peak", "daily", "flash")


class Traffic:
    def __init__(self, profile: str, duration: float, peak_mult: float = 3.0, hourly: list[float] | None = None,
                 day_seconds: float | None = None, start_hour: float = 0.0):
        if profile not in PROFILES:
            raise ValueError(f"profile phải là một trong {PROFILES}")
        if profile == "daily" and (not hourly or len(hourly) != 24):
            raise ValueError("profile daily cần hourly_traffic gồm 24 giá trị")
        self.profile = profile
        self.duration = duration
        self.peak_mult = peak_mult
        self.day_seconds = day_seconds or duration
        self.start_hour = start_hour
        if hourly:
            mean = sum(hourly) / len(hourly)
            self.hourly = [h / mean for h in hourly]  # chuẩn hoá để trung bình = 1
        else:
            self.hourly = None

    def hour_at(self, elapsed: float) -> float | None:
        """Giờ ảo trong ngày (0–24) ở profile daily, None với các profile khác."""
        if self.profile != "daily":
            return None
        return (self.start_hour + elapsed / self.day_seconds * 24) % 24

    def multiplier(self, elapsed: float) -> float:
        if self.profile == "peak":
            third = self.duration / 3
            return self.peak_mult if third <= elapsed < 2 * third else 1.0
        if self.profile == "daily":
            h = self.hour_at(elapsed)
            i = int(h)
            frac = h - i
            # nội suy tuyến tính giữa hai mốc giờ để đường tốc độ mượt
            return self.hourly[i] * (1 - frac) + self.hourly[(i + 1) % 24] * frac
        if self.profile == "flash":
            t0 = self.duration / 2
            if elapsed < t0:
                return 1.0
            tau = max(1.0, self.duration * 0.05)  # sau ~3·tau giây gần như trở về bình thường
            return 1 + (self.peak_mult - 1) * math.exp(-(elapsed - t0) / tau)
        return 1.0
