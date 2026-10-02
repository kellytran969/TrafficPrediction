"""Transform stage: clean raw readings and roll them up to hourly features.

Cleaning rules (validated by unit tests):
  - drop rows with any null
  - drop exact duplicates on (sensor_id, ts)
  - clip speed to [0, 120] mph, occupancy to [0, 100] %, volume to >= 0

Aggregation collapses raw 5-15 minute readings into one row per sensor per
hour: mean speed, summed volume, mean occupancy, sample count. This is the
step that shrinks storage (raw 15-min data -> 1 hourly row = 4x fewer rows
before any compression) while keeping everything the model needs.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd  # noqa: E402


def clean(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats = {"input_rows": len(df)}
    df = df.dropna()
    stats["null_rows_dropped"] = stats["input_rows"] - len(df)

    df = df.drop_duplicates(subset=["sensor_id", "ts"])
    stats["duplicate_rows_dropped"] = (
        stats["input_rows"] - stats["null_rows_dropped"] - len(df)
    )

    df = df.copy()
    df["speed_mph"] = df["speed_mph"].clip(0, 120)
    df["occupancy_pct"] = df["occupancy_pct"].clip(0, 100)
    df["volume_vph"] = df["volume_vph"].clip(lower=0)
    stats["output_rows"] = len(df)
    return df, stats


def aggregate_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """One row per (sensor_id, hour)."""
    df = df.copy()
    df["hour"] = df["ts"].dt.floor("h")
    grouped = df.groupby(["sensor_id", "hour"], as_index=False)
    out = grouped.agg(
        avg_speed=("speed_mph", "mean"),
        total_volume=("volume_vph", "sum"),
        avg_occupancy=("occupancy_pct", "mean"),
        samples=("speed_mph", "size"),
    )
    out["avg_speed"] = out["avg_speed"].round(2)
    out["avg_occupancy"] = out["avg_occupancy"].round(2)
    return out
