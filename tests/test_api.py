"""API endpoints against a throwaway DB, memory cache and stub model."""

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from common.cache import MemoryCache
from common.db import Reading, Sensor, get_session_factory


class StubModels:
    def load(self):
        pass

    def predict(self, sensor_id, target_time=None):
        target = (target_time or datetime.now(timezone.utc)).replace(
            minute=0, second=0, microsecond=0)
        return {
            "sensor_id": sensor_id,
            "target_time": target,
            "predicted_speed_mph": 45.0,
            "predicted_volume_vph": 2000,
            "congestion_level": "moderate",
            "model_version": "test-v1",
        }


@pytest.fixture()
def client(test_db):
    from services.api.app import create_app
    session_factory = get_session_factory()
    with session_factory() as session:
        session.add(Sensor(sensor_id="NYC-001", city="New York", highway="I-95",
                           direction="NB", latitude=40.8, longitude=-73.9, lanes=4))
        session.add(Reading(sensor_id="NYC-001", ts=datetime.now(timezone.utc),
                            speed_mph=52.0, volume_vph=2100, occupancy_pct=18.0))
        session.commit()
    app = create_app(cache=MemoryCache(), model_service=StubModels())
    return TestClient(app)


def test_health(client):
    assert client.get("/health").json()["status"] == "ok"


def test_list_sensors_filters_by_city(client):
    sensors = client.get("/sensors?city=New%20York").json()
    assert {s["sensor_id"] for s in sensors} >= {"NYC-001"}
    assert all(s["city"] == "New York" for s in sensors)


def test_latest_reading_from_db_then_cache(client):
    first = client.get("/sensors/NYC-001/latest").json()
    assert first["source"] == "db"
    assert first["speed_mph"] == 52.0
    second = client.get("/sensors/NYC-001/latest").json()
    assert second["source"] == "cache"


def test_unknown_sensor_404(client):
    assert client.get("/sensors/NOPE-1/latest").status_code == 404
    assert client.post("/predict", json={"sensor_id": "NOPE-1"}).status_code == 404


def test_predict_and_cache(client):
    body = {"sensor_id": "NYC-001"}
    first = client.post("/predict", json=body).json()
    assert first["predicted_speed_mph"] == 45.0
    assert first["congestion_level"] == "moderate"
    # second call served from cache - prediction rows not duplicated
    client.post("/predict", json=body)
    session_factory = get_session_factory()
    with session_factory() as session:
        from common.db import Prediction
        assert session.query(Prediction).count() == 1


def test_history_endpoint(client):
    assert client.get("/sensors/NYC-001/history?hours=24").json() == []
