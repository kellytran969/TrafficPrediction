"""Load stage: upsert hourly aggregates into the warehouse table."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd  # noqa: E402
from sqlalchemy.dialects.postgresql import insert as pg_insert  # noqa: E402
from sqlalchemy.dialects.sqlite import insert as sqlite_insert  # noqa: E402

from common.db import HourlyFeature, get_session_factory  # noqa: E402


def load_hourly(df: pd.DataFrame) -> int:
    """Upsert aggregated rows; returns number of rows written."""
    if df.empty:
        return 0
    session_factory = get_session_factory()
    rows = df.to_dict(orient="records")
    with session_factory() as session:
        engine_name = session.bind.dialect.name
        inserter = pg_insert if engine_name == "postgresql" else sqlite_insert
        stmt = inserter(HourlyFeature).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["sensor_id", "hour"],
            set_={
                "avg_speed": stmt.excluded.avg_speed,
                "total_volume": stmt.excluded.total_volume,
                "avg_occupancy": stmt.excluded.avg_occupancy,
                "samples": stmt.excluded.samples,
            },
        )
        result = session.execute(stmt)
        session.commit()
        return result.rowcount if result.rowcount is not None else len(rows)
