import pytest

from generator import load_catalog
from traffic import Traffic

HOURLY = load_catalog()["hourly_traffic"]


def test_constant():
    t = Traffic("constant", 60)
    assert {t.multiplier(x) for x in range(60)} == {1.0}
    assert t.hour_at(10) is None


def test_peak_in_middle_third():
    t = Traffic("peak", 90, peak_mult=5)
    assert t.multiplier(10) == 1 and t.multiplier(45) == 5 and t.multiplier(80) == 1


def test_daily_average_equals_rate():
    t = Traffic("daily", 2400, hourly=HOURLY)
    mults = [t.multiplier(x) for x in range(2400)]
    assert sum(mults) / len(mults) == pytest.approx(1, abs=0.01)
    # 19h (cao điểm tối) đông hơn 3h sáng nhiều lần
    assert t.multiplier(19 * 100) > 10 * t.multiplier(3 * 100)


def test_daily_start_hour_and_day_length():
    t = Traffic("daily", 3600, hourly=HOURLY, day_seconds=1200, start_hour=6)
    assert t.hour_at(0) == 6
    assert t.hour_at(600) == pytest.approx(18)
    assert t.hour_at(1200) == pytest.approx(6)  # sang ngày ảo thứ hai


def test_flash_sale_spikes_then_decays():
    t = Traffic("flash", 600, peak_mult=8)
    assert t.multiplier(100) == 1
    assert t.multiplier(300) == pytest.approx(8)
    assert 1 < t.multiplier(400) < t.multiplier(320)
    assert t.multiplier(599) < 1.2


def test_daily_needs_24_hours():
    with pytest.raises(ValueError):
        Traffic("daily", 60, hourly=[1, 2, 3])
