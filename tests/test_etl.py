"""ETL transform: cleaning rules and hourly aggregation."""

import pandas as pd

from pipelines.etl.transform import aggregate_hourly, clean


def _raw() -> pd.DataFrame:
    return pd.DataFrame([
        {"sensor_id": "NYC-001", "ts": pd.Timestamp("2026-09-01 08:05", tz="UTC"),
         "speed_mph": 40.0, "volume_vph": 3000, "occupancy_pct": 25.0},
        # exact duplicate -> dropped
        {"sensor_id": "NYC-001", "ts": pd.Timestamp("2026-09-01 08:05", tz="UTC"),
         "speed_mph": 40.0, "volume_vph": 3000, "occupancy_pct": 25.0},
        # out-of-range values -> clipped, not dropped
        {"sensor_id": "NYC-001", "ts": pd.Timestamp("2026-09-01 08:20", tz="UTC"),
         "speed_mph": 999.0, "volume_vph": -50, "occupancy_pct": 150.0},
        # null -> dropped
        {"sensor_id": "NYC-001", "ts": pd.Timestamp("2026-09-01 08:35", tz="UTC"),
         "speed_mph": None, "volume_vph": 2000, "occupancy_pct": 20.0},
        {"sensor_id": "NYC-001", "ts": pd.Timestamp("2026-09-01 09:05", tz="UTC"),
         "speed_mph": 60.0, "volume_vph": 1500, "occupancy_pct": 10.0},
    ])


def test_clean_drops_nulls_and_dupes_and_clips():
    cleaned, stats = clean(_raw())
    assert stats["null_rows_dropped"] == 1
    assert stats["duplicate_rows_dropped"] == 1
    assert len(cleaned) == 3
    assert cleaned["speed_mph"].max() <= 120
    assert cleaned["occupancy_pct"].max() <= 100
    assert cleaned["volume_vph"].min() >= 0


def test_aggregate_hourly_groups_by_sensor_and_hour():
    cleaned, _ = clean(_raw())
    hourly = aggregate_hourly(cleaned)
    assert len(hourly) == 2  # 08:00 and 09:00 buckets
    eight = hourly[hourly["hour"] == pd.Timestamp("2026-09-01 08:00", tz="UTC")].iloc[0]
    assert eight["samples"] == 2
    assert eight["avg_speed"] == round((40.0 + 120.0) / 2, 2)
    assert eight["total_volume"] == 3000 + 0
