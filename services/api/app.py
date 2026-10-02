"""Prediction API.

Serves live sensor readings and ML traffic forecasts to transportation
partners. Hot paths (latest reading, recent predictions) are cached in Redis;
the model service rebuilds features from the warehouse on a cache miss.

Endpoints:
  GET  /health
  GET  /cities
  GET  /sensors?city=
  GET  /sensors/{sensor_id}/latest
  GET  /sensors/{sensor_id}/history?hours=24
  POST /predict                      {sensor_id, target_time?, horizon_hours?}

Run:
    uvicorn services.api.app:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from fastapi import FastAPI, HTTPException, Query  # noqa: E402
from sqlalchemy import desc, text  # noqa: E402

from common.cache import Cache, get_cache  # noqa: E402
from common.db import Prediction, get_session_factory, init_db  # noqa: E402
from common.schemas import PredictionRequest, PredictionResponse, SensorInfo  # noqa: E402
from ml.model_service import ModelService  # noqa: E402
from services.simulator.catalog import BY_ID, CITIES  # noqa: E402


def create_app(cache: Cache | None = None,
               model_service: ModelService | None = None) -> FastAPI:
    init_db()
    app = FastAPI(title="Traffic Prediction API", version="1.0.0")
    app.state.cache = cache or get_cache()
    app.state.models = model_service or ModelService()
    app.state.session_factory = get_session_factory()

    @app.get("/health")
    def health():
        try:
            app.state.models.load()
            model_ok = True
        except Exception:
            model_ok = False
        return {"status": "ok", "model_loaded": model_ok}

    @app.get("/cities")
    def cities():
        return {"cities": CITIES}

    @app.get("/sensors", response_model=list[SensorInfo])
    def list_sensors(city: str | None = None):
        specs = BY_ID.values()
        if city:
            specs = [s for s in specs if s.city.lower() == city.lower()]
        return [
            SensorInfo(sensor_id=s.sensor_id, city=s.city, highway=s.highway,
                       direction=s.direction, latitude=s.latitude,
                       longitude=s.longitude, lanes=s.lanes)
            for s in specs
        ]

    @app.get("/sensors/{sensor_id}/latest")
    def latest(sensor_id: str):
        if sensor_id not in BY_ID:
            raise HTTPException(404, f"unknown sensor {sensor_id!r}")
        cache: Cache = app.state.cache
        key = f"sensor:{sensor_id}:latest"
        cached = cache.get(key)
        if cached is not None:
            return {**cached, "source": "cache"}
        with app.state.session_factory() as session:
            row = session.execute(
                text("SELECT sensor_id, ts, speed_mph, volume_vph, occupancy_pct "
                     "FROM readings WHERE sensor_id = :sid ORDER BY ts DESC LIMIT 1"),
                {"sid": sensor_id},
            ).mappings().first()
        if row is None:
            raise HTTPException(404, "no readings yet for this sensor")
        payload = dict(row)
        ts = payload["ts"]
        payload["ts"] = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
        cache.set(key, payload, ttl_seconds=60)
        return {**payload, "source": "db"}

    @app.get("/sensors/{sensor_id}/history")
    def history(sensor_id: str, hours: int = Query(24, ge=1, le=168)):
        if sensor_id not in BY_ID:
            raise HTTPException(404, f"unknown sensor {sensor_id!r}")
        with app.state.session_factory() as session:
            rows = session.execute(
                text("SELECT hour, avg_speed, total_volume, avg_occupancy "
                     "FROM hourly_features WHERE sensor_id = :sid "
                     "ORDER BY hour DESC LIMIT :n"),
                {"sid": sensor_id, "n": hours},
            ).mappings().all()
        return [
            {"hour": r["hour"].isoformat() if hasattr(r["hour"], "isoformat") else str(r["hour"]),
             "avg_speed": r["avg_speed"], "total_volume": r["total_volume"],
             "avg_occupancy": r["avg_occupancy"]}
            for r in reversed(rows)
        ]

    @app.post("/predict", response_model=PredictionResponse)
    def predict(req: PredictionRequest):
        if req.sensor_id not in BY_ID:
            raise HTTPException(404, f"unknown sensor {req.sensor_id!r}")
        cache: Cache = app.state.cache
        target = req.target_time or datetime.now(timezone.utc)
        target_hour = target.replace(minute=0, second=0, microsecond=0)
        key = f"pred:{req.sensor_id}:{target_hour.isoformat()}"
        cached = cache.get(key)
        if cached is not None:
            return PredictionResponse(**cached)

        try:
            result = app.state.models.predict(req.sensor_id, target)
        except FileNotFoundError as e:
            raise HTTPException(503, str(e))
        except ValueError as e:
            raise HTTPException(422, str(e))

        with app.state.session_factory() as session:
            session.add(Prediction(
                sensor_id=result["sensor_id"],
                target_time=result["target_time"],
                predicted_speed_mph=result["predicted_speed_mph"],
                model_version=result["model_version"],
            ))
            session.commit()

        payload = {
            "sensor_id": result["sensor_id"],
            "target_time": result["target_time"].isoformat(),
            "predicted_speed_mph": result["predicted_speed_mph"],
            "predicted_volume_vph": result["predicted_volume_vph"],
            "congestion_level": result["congestion_level"],
            "model_version": result["model_version"],
        }
        cache.set(key, payload, ttl_seconds=300)
        return PredictionResponse(**payload)

    return app


app = create_app()
