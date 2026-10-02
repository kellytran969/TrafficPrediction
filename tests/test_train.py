"""End-to-end training on synthetic data: ETL -> features -> XGBoost."""

import random
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from common.db import get_session_factory
from ml import train
from pipelines.etl.load import load_hourly
from pipelines.etl.transform import aggregate_hourly, clean
from services.simulator.catalog import BY_ID
from services.simulator.main import generate_reading


@pytest.fixture()
def hourly_in_db(test_db):
    rng = random.Random(7)
    specs = [BY_ID["NYC-001"], BY_ID["NYC-002"], BY_ID["LA-001"]]
    rows = []
    start = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(days=14)
    ts = start
    while ts <= datetime.now(timezone.utc):
        for spec in specs:
            r = generate_reading(spec, ts, rng)
            r["ts"] = pd.Timestamp(r["ts"])
            rows.append(r)
        ts += timedelta(hours=1)
    raw = pd.DataFrame(rows)
    cleaned, _ = clean(raw)
    hourly = aggregate_hourly(cleaned)

    # seed the sensor catalog rows the ETL would normally expect
    from common.db import Sensor
    session_factory = get_session_factory()
    with session_factory() as session:
        for spec in specs:
            session.merge(Sensor(sensor_id=spec.sensor_id, city=spec.city,
                                highway=spec.highway, direction=spec.direction,
                                latitude=spec.latitude, longitude=spec.longitude,
                                lanes=spec.lanes))
        session.commit()
    assert load_hourly(hourly) > 0
    return hourly


def test_train_beats_accuracy_bar(hourly_in_db, test_db, tmp_path, monkeypatch):
    monkeypatch.setattr(train, "ARTIFACT_DIR", tmp_path / "artifacts")
    report = train.run(days=30, test_fraction=0.2, n_estimators=60)
    speed = report["targets"]["target_speed"]
    assert speed["accuracy"] > 0.80, f"speed accuracy {speed['accuracy']} below 0.80 bar"
    assert speed["r2"] > 0.70
    assert (tmp_path / "artifacts" / "metrics.json").exists()
    assert (tmp_path / "artifacts" / speed["artifact"]).exists()
