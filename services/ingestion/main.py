"""Stream ingestion service.

Consumes SensorReading messages from the stream bus, validates them, and:
  1. batch-inserts them into Postgres (`readings` table)
  2. refreshes the per-sensor "latest reading" entry in the cache

Run:
    python -m services.ingestion.main [--batch-size 500] [--max-batches N]

Duplicate (sensor_id, ts) rows are ignored via the unique constraint, so the
consumer is safe to replay.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pydantic import ValidationError  # noqa: E402
from sqlalchemy.dialects.postgresql import insert as pg_insert  # noqa: E402
from sqlalchemy.dialects.sqlite import insert as sqlite_insert  # noqa: E402

from common.cache import get_cache  # noqa: E402
from common.db import Reading, Sensor, get_session_factory, init_db  # noqa: E402
from common.schemas import SensorReading  # noqa: E402
from common.stream import get_bus  # noqa: E402
from services.simulator.catalog import BY_ID  # noqa: E402


def upsert_sensors(session_factory) -> int:
    """Seed the sensor catalog (idempotent). Returns number of sensors."""
    with session_factory() as session:
        existing = {s.sensor_id for s in session.query(Sensor.sensor_id).all()}
        added = 0
        for spec in BY_ID.values():
            if spec.sensor_id not in existing:
                session.add(Sensor(
                    sensor_id=spec.sensor_id, city=spec.city,
                    highway=spec.highway, direction=spec.direction,
                    latitude=spec.latitude, longitude=spec.longitude,
                    lanes=spec.lanes,
                ))
                added += 1
        session.commit()
        return len(BY_ID)


def insert_batch(session_factory, rows: list[dict]) -> int:
    """Insert validated readings; silently skip duplicates. Returns inserted count."""
    if not rows:
        return 0
    with session_factory() as session:
        engine_name = session.bind.dialect.name
        inserter = pg_insert if engine_name == "postgresql" else sqlite_insert
        stmt = inserter(Reading).values(rows)
        # ON CONFLICT DO NOTHING on (sensor_id, ts)
        stmt = stmt.on_conflict_do_nothing(index_elements=["sensor_id", "ts"])
        result = session.execute(stmt)
        session.commit()
        return result.rowcount if result.rowcount is not None else 0


def run(batch_size: int = 500, max_batches: int | None = None,
        poll_ms: int = 1000, bus=None, cache=None,
        session_factory=None) -> dict[str, int]:
    init_db()
    session_factory = session_factory or get_session_factory()
    n_sensors = upsert_sensors(session_factory)
    bus = bus or get_bus()
    cache = cache or get_cache()

    stats = {"received": 0, "inserted": 0, "invalid": 0, "batches": 0}
    print(f"ingestion started ({n_sensors} sensors known). Waiting for readings...")
    try:
        while max_batches is None or stats["batches"] < max_batches:
            messages = bus.read_batch(batch_size=batch_size, block_ms=poll_ms)
            if not messages and max_batches is not None:
                break  # drained (useful for tests/backfill)
            rows: list[dict] = []
            for msg in messages:
                stats["received"] += 1
                try:
                    reading = SensorReading.model_validate(msg)
                except ValidationError:
                    stats["invalid"] += 1
                    continue
                rows.append({
                    "sensor_id": reading.sensor_id,
                    "ts": reading.ts,
                    "speed_mph": reading.speed_mph,
                    "volume_vph": reading.volume_vph,
                    "occupancy_pct": reading.occupancy_pct,
                })
                # refresh the "latest" cache entry (only if newer)
                key = f"sensor:{reading.sensor_id}:latest"
                prev = cache.get(key)
                payload = reading.model_dump(mode="json")
                if prev is None or payload["ts"] >= prev["ts"]:
                    cache.set(key, payload, ttl_seconds=600)
            stats["inserted"] += insert_batch(session_factory, rows)
            stats["batches"] += 1
    except KeyboardInterrupt:
        pass
    print(f"ingestion stopped: {stats}")
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Traffic stream ingestion service")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-batches", type=int, default=None,
                        help="stop after N batches (default: run forever)")
    args = parser.parse_args()
    run(batch_size=args.batch_size, max_batches=args.max_batches)


if __name__ == "__main__":
    main()
