"""Feature engineering: lags and targets line up on the right hours."""

import pandas as pd
import pytest

from ml.features import FEATURE_COLS, build_dataset


def _hourly(hours: int = 72) -> pd.DataFrame:
    idx = pd.date_range("2026-09-01", periods=hours, freq="h", tz="UTC")
    return pd.DataFrame({
        "sensor_id": "NYC-001",
        "hour": idx,
        "avg_speed": [50.0 + (i % 24) * 0.1 for i in range(hours)],
        "total_volume": [2000 + i for i in range(hours)],
        "avg_occupancy": [20.0] * hours,
        "samples": [4] * hours,
    })


def test_lag_and_target_alignment():
    X, y, meta = build_dataset(_hourly(72))
    # 72 rows minus 24 warmup rows minus 1 trailing row = 47
    assert len(X) == 47
    assert list(X.columns) == FEATURE_COLS
    first = X.iloc[0]
    assert first["lag_1h"] == 50.0 + 23 * 0.1          # hour 23
    assert first["lag_24h"] == 50.0                     # hour 0
    assert y.iloc[0]["target_speed"] == pytest.approx(50.0 + 1 * 0.1)  # idx 25 -> 25 % 24 == 1
    # calendar describes the target hour (row hour 00:00 -> target 01:00)
    assert first["hour_of_day"] == 1


def test_no_nan_leaks_through():
    X, y, _ = build_dataset(_hourly(72))
    assert not X.isna().any().any()
    assert not y.isna().any().any()
