"""Model serving: loads trained artifacts and forecasts one sensor-hour.

Used by the API. Features are rebuilt from the warehouse (`hourly_features`)
so serving uses exactly the same definitions as training.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import joblib  # noqa: E402
import pandas as pd  # noqa: E402
from sqlalchemy import text  # noqa: E402

from common.db import get_engine  # noqa: E402
from ml.features import FEATURE_COLS  # noqa: E402
from services.simulator.catalog import BY_ID  # noqa: E402


def congestion_level(predicted_speed: float, free_flow_mph: float) -> str:
    ratio = predicted_speed / max(free_flow_mph, 1)
    if ratio >= 0.85:
        return "free_flow"
    if ratio >= 0.65:
        return "moderate"
    if ratio >= 0.45:
        return "heavy"
    return "severe"


class ModelService:
    def __init__(self, artifact_dir: str | Path | None = None) -> None:
        self.artifact_dir = Path(
            artifact_dir or Path(__file__).resolve().parent / "artifacts")
        self.models: dict[str, object] = {}
        self.model_version = "xgb_speed_v1"
        self._loaded = False

    def load(self) -> None:
        if self._loaded:
            return
        metrics_path = self.artifact_dir / "metrics.json"
        if metrics_path.exists():
            metrics = json.loads(metrics_path.read_text())
            self.model_version = metrics.get("model_version", self.model_version)
            for target, info in metrics.get("targets", {}).items():
                short = target.split("_")[1]  # target_speed -> speed
                path = self.artifact_dir / info["artifact"]
                self.models[short] = joblib.load(path)
        else:  # fall back to conventional filenames (tests, fresh checkouts)
            for short in ("speed", "volume"):
                path = self.artifact_dir / f"{self.model_version}_{short}.joblib"
                if path.exists():
                    self.models[short] = joblib.load(path)
        if not self.models:
            raise FileNotFoundError(
                f"no model artifacts in {self.artifact_dir} - run ml/train.py first")
        self._loaded = True

    def _history(self, sensor_id: str, target_hour: datetime) -> pd.DataFrame:
        """Last 26 hourly rows strictly before the target hour."""
        engine = get_engine()
        start = target_hour - pd.Timedelta(hours=26).to_pytimedelta()
        with engine.connect() as conn:
            df = pd.read_sql(
                text("SELECT hour, avg_speed, total_volume, avg_occupancy "
                     "FROM hourly_features WHERE sensor_id = :sid "
                     "AND hour >= :start AND hour < :target ORDER BY hour"),
                conn,
                params={"sid": sensor_id, "start": start, "target": target_hour},
            )
        df["hour"] = pd.to_datetime(df["hour"], utc=True)
        return df

    def build_features(self, sensor_id: str, target_time: datetime) -> pd.DataFrame:
        target_hour = target_time.replace(minute=0, second=0, microsecond=0)
        if target_hour.tzinfo is None:
            target_hour = target_hour.replace(tzinfo=timezone.utc)
        hist = self._history(sensor_id, target_hour)
        if len(hist) < 24:
            raise ValueError(
                f"not enough history for {sensor_id} "
                f"({len(hist)} hours, need 24)")

        last = hist.iloc[-1]
        row = {
            "hour_of_day": target_hour.hour,
            "day_of_week": target_hour.weekday(),
            "month": target_hour.month,
            "is_weekend": int(target_hour.weekday() >= 5),
            "is_rush": int(target_hour.hour in (7, 8, 16, 17, 18)
                            and target_hour.weekday() < 5),
            "lag_1h": float(last["avg_speed"]),
            "lag_24h": float(hist.iloc[-24]["avg_speed"]),
            "roll_3h": float(hist["avg_speed"].tail(3).mean()),
            "lag_vol_1h": float(last["total_volume"]),
            "occupancy_now": float(last["avg_occupancy"]),
        }
        return pd.DataFrame([row])[FEATURE_COLS]

    def predict(self, sensor_id: str,
                target_time: datetime | None = None) -> dict:
        self.load()
        if sensor_id not in BY_ID:
            raise ValueError(f"unknown sensor {sensor_id!r}")
        if target_time is None:
            target_time = datetime.now(timezone.utc) + pd.Timedelta(hours=1).to_pytimedelta()
        X = self.build_features(sensor_id, target_time)
        speed = float(self.models["speed"].predict(X)[0])
        volume = int(self.models["volume"].predict(X)[0])
        spec = BY_ID[sensor_id]
        target_hour = target_time.replace(minute=0, second=0, microsecond=0)
        return {
            "sensor_id": sensor_id,
            "target_time": target_hour,
            "predicted_speed_mph": round(max(0.0, speed), 1),
            "predicted_volume_vph": max(0, volume),
            "congestion_level": congestion_level(speed, spec.free_flow_mph),
            "model_version": self.model_version,
        }
