"""Simulator produces realistic, schema-valid readings."""

import random
from datetime import datetime, timezone

from common.schemas import SensorReading
from services.simulator.catalog import BY_ID
from services.simulator.main import congestion_factor, generate_reading


def test_rush_hour_slower_than_night():
    rng = random.Random(0)
    spec = BY_ID["NYC-001"]
    rush = [generate_reading(spec, datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc), rng)["speed_mph"] for _ in range(20)]
    night = [generate_reading(spec, datetime(2026, 9, 2, 3, 0, tzinfo=timezone.utc), rng)["speed_mph"] for _ in range(20)]
    assert sum(rush) / len(rush) < sum(night) / len(night) - 5


def test_weekday_pm_peak_stronger_than_weekend():
    rng = random.Random(1)
    ts = datetime(2026, 9, 2, 17, 30, tzinfo=timezone.utc)   # Wednesday
    wknd = datetime(2026, 9, 6, 17, 30, tzinfo=timezone.utc)  # Sunday
    assert congestion_factor(ts, rng) > congestion_factor(wknd, rng)


def test_readings_validate_against_schema():
    rng = random.Random(2)
    for spec in list(BY_ID.values())[:5]:
        reading = SensorReading.model_validate(
            generate_reading(spec, datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc), rng))
        assert 5 <= reading.speed_mph <= 120
        assert reading.volume_vph >= 0
        assert 0 <= reading.occupancy_pct <= 100


def test_catalog_covers_multiple_states():
    cities = {s.city for s in BY_ID.values()}
    assert len(cities) >= 5
