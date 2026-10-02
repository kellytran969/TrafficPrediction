"""Extract stage: pull raw readings from Postgres and CSV dumps from DOT-style files.

The pipeline deliberately merges several origins so it behaves like the
multi-source ingestion described for the system:
  1. the live `readings` table (written by the ingestion service)
  2. CSV "open data" dumps in data/historical/ (one file per city per month)
  3. the static sensor catalog (for enrichment / filtering unknown sensors)
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd  # noqa: E402
from sqlalchemy import text  # noqa: E402

from common.db import get_engine  # noqa: E402
from services.simulator.catalog import BY_ID  # noqa: E402

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "historical"

COLUMNS = ["sensor_id", "ts", "speed_mph", "volume_vph", "occupancy_pct"]


def extract_db(start: datetime, end: datetime) -> pd.DataFrame:
    """Raw readings from Postgres/SQLite for [start, end)."""
    engine = get_engine()
    query = text(
        "SELECT sensor_id, ts, speed_mph, volume_vph, occupancy_pct "
        "FROM readings WHERE ts >= :start AND ts < :end"
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"start": start, "end": end})
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df[COLUMNS]


def extract_csvs() -> pd.DataFrame:
    """All DOT-style CSV dumps under data/historical/."""
    frames: list[pd.DataFrame] = []
    if not DATA_DIR.exists():
        return pd.DataFrame(columns=COLUMNS)
    for path in sorted(DATA_DIR.glob("*.csv")):
        # DOT dumps are messy: coerce bad timestamps/numbers, the transform
        # stage drops whatever doesn't parse.
        df = pd.read_csv(path, usecols=COLUMNS)
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        for col in ("speed_mph", "volume_vph", "occupancy_pct"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=COLUMNS)
    return pd.concat(frames, ignore_index=True)[COLUMNS]


def extract_all(start: datetime, end: datetime) -> tuple[pd.DataFrame, dict]:
    """Merge every source; drop rows for sensors not in the catalog."""
    db_df = extract_db(start, end)
    csv_df = extract_csvs()
    if not csv_df.empty:
        in_window = (csv_df["ts"] >= start) & (csv_df["ts"] < end)
        csv_df = csv_df.loc[in_window]
    combined = pd.concat([db_df, csv_df], ignore_index=True)
    known = set(BY_ID)
    before = len(combined)
    combined = combined[combined["sensor_id"].isin(known)]
    stats = {
        "db_rows": len(db_df),
        "csv_rows": len(csv_df),
        "unknown_sensor_rows_dropped": before - len(combined),
        "merged_rows": len(combined),
    }
    return combined, stats


def default_window(days: int = 30) -> tuple[datetime, datetime]:
    end = datetime.now(timezone.utc)
    return end - pd.Timedelta(days=days).to_pytimedelta(), end
