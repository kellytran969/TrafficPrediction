"""Feature engineering: hourly aggregates -> supervised learning rows.

For each (sensor_id, hour h) we predict the *next* hour's average speed and
total volume. Features are strictly things known at the end of hour h:

  calendar(h+1)      hour_of_day, day_of_week, month, is_weekend, is_rush
  lag_1h             avg_speed at h
  lag_24h            avg_speed at h - 23h  (same hour yesterday)
  roll_3h            mean avg_speed over h-2 .. h
  lag_vol_1h         total_volume at h
  occupancy_now      avg_occupancy at h
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd  # noqa: E402

FEATURE_COLS = [
    "hour_of_day",
    "day_of_week",
    "month",
    "is_weekend",
    "is_rush",
    "lag_1h",
    "lag_24h",
    "roll_3h",
    "lag_vol_1h",
    "occupancy_now",
]

TARGETS = ["target_speed", "target_volume"]


def add_calendar(df: pd.DataFrame, col: str = "hour") -> pd.DataFrame:
    df = df.copy()
    ts = df[col]
    df["hour_of_day"] = ts.dt.hour
    df["day_of_week"] = ts.dt.dayofweek
    df["month"] = ts.dt.month
    df["is_weekend"] = (ts.dt.dayofweek >= 5).astype(int)
    rush = ts.dt.hour.isin([7, 8, 16, 17, 18]) & (ts.dt.dayofweek < 5)
    df["is_rush"] = rush.astype(int)
    return df


def build_dataset(hourly: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return (X, y, meta). y has target_speed / target_volume columns."""
    df = hourly.sort_values(["sensor_id", "hour"]).copy()
    df["hour"] = pd.to_datetime(df["hour"], utc=True)

    by_sensor = df.groupby("sensor_id")
    df["lag_1h"] = by_sensor["avg_speed"].shift(1)
    df["lag_24h"] = by_sensor["avg_speed"].shift(24)
    df["roll_3h"] = by_sensor["avg_speed"].transform(
        lambda s: s.shift(1).rolling(window=3, min_periods=1).mean()
    )
    df["lag_vol_1h"] = by_sensor["total_volume"].shift(1)
    df["occupancy_now"] = df["avg_occupancy"]

    df["target_speed"] = by_sensor["avg_speed"].shift(-1)
    df["target_volume"] = by_sensor["total_volume"].shift(-1)

    # calendar features describe the *target* hour (known in advance)
    df["target_hour"] = df["hour"] + pd.Timedelta(hours=1)
    df = add_calendar(df, col="target_hour")

    clean = df.dropna(subset=FEATURE_COLS + TARGETS).reset_index(drop=True)
    X = clean[FEATURE_COLS].copy()
    y = clean[TARGETS].copy()
    meta = clean[["sensor_id", "hour", "target_hour"]].copy()
    return X, y, meta


def time_split(X: pd.DataFrame, y: pd.DataFrame, meta: pd.DataFrame,
               test_fraction: float = 0.2):
    """Chronological split on the target hour (no shuffling -> no leakage)."""
    cutoff = meta["target_hour"].quantile(1.0 - test_fraction)
    test_mask = meta["target_hour"] >= cutoff
    return (
        X.loc[~test_mask], X.loc[test_mask],
        y.loc[~test_mask], y.loc[test_mask],
        meta.loc[~test_mask], meta.loc[test_mask],
    )
