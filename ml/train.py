"""Model training: XGBoost vs. sklearn baseline, time-based evaluation.

Trains one regressor per target (next-hour avg speed, next-hour volume),
compares XGBoost against a sklearn HistGradientBoosting baseline, keeps the
winner per target, and writes artifacts + a metrics report.

Run:
    python -m ml.train [--days 60] [--test-fraction 0.2]

Artifacts (ml/artifacts/):
    xgb_speed_v1.joblib / xgb_volume_v1.joblib   trained models
    metrics.json                                  rmse/mae/r2/accuracy + params
    feature_columns.json                          FEATURE_COLS order
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import joblib  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from sklearn.ensemble import HistGradientBoostingRegressor  # noqa: E402
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score  # noqa: E402
from sqlalchemy import text  # noqa: E402

from common.db import get_engine  # noqa: E402
from ml.features import FEATURE_COLS, build_dataset, time_split  # noqa: E402

MODEL_VERSION = "xgb_speed_v1"
ARTIFACT_DIR = Path(__file__).resolve().parent / "artifacts"

# "accuracy" for a regression forecast: share of predictions within +/-5 mph
# (speed) or +/-15% (volume). The >80% bar mirrors the project's stated goal.
SPEED_TOLERANCE_MPH = 5.0
VOLUME_TOLERANCE_FRAC = 0.15


def load_hourly(days: int) -> pd.DataFrame:
    engine = get_engine()
    cutoff = datetime.now(timezone.utc) - pd.Timedelta(days=days).to_pytimedelta()
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT * FROM hourly_features WHERE hour >= :cutoff"),
            conn, params={"cutoff": cutoff},
        )
    df["hour"] = pd.to_datetime(df["hour"], utc=True)
    return df


def evaluate(y_true: np.ndarray, y_pred: np.ndarray, tolerance) -> dict:
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    if callable(tolerance):
        within = tolerance(y_true, y_pred)
    else:
        within = np.abs(y_true - y_pred) <= tolerance
    return {
        "rmse": round(rmse, 3),
        "mae": round(float(mean_absolute_error(y_true, y_pred)), 3),
        "r2": round(float(r2_score(y_true, y_pred)), 4),
        "accuracy": round(float(np.mean(within)), 4),
        "n_test": int(len(y_true)),
    }


def train_target(X_train, y_train, X_test, y_test, name: str,
                 tolerance, n_estimators: int = 400) -> tuple[object, dict, str]:
    from xgboost import XGBRegressor

    candidates = {
        "xgboost": XGBRegressor(
            n_estimators=n_estimators, max_depth=6, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            n_jobs=-1, random_state=42,
        ),
        "sklearn_hgb": HistGradientBoostingRegressor(random_state=42),
    }
    results = {}
    for algo, model in candidates.items():
        t0 = time.time()
        model.fit(X_train, y_train)
        metrics = evaluate(y_test.to_numpy(), model.predict(X_test), tolerance)
        metrics["train_seconds"] = round(time.time() - t0, 1)
        results[algo] = (model, metrics)
        print(f"[{name}] {algo}: rmse={metrics['rmse']} "
              f"r2={metrics['r2']} acc={metrics['accuracy']}")

    best_algo = min(results, key=lambda a: results[a][1]["rmse"])
    return results[best_algo][0], results[best_algo][1], best_algo


def run(days: int = 60, test_fraction: float = 0.2,
        n_estimators: int = 400) -> dict:
    hourly = load_hourly(days)
    if hourly.empty:
        raise SystemExit("hourly_features is empty - run the ETL first")
    X, y, meta = build_dataset(hourly)
    print(f"dataset: {len(X)} rows, {X['hour_of_day'].nunique()} hours covered, "
          f"{meta['sensor_id'].nunique()} sensors")
    X_train, X_test, y_train, y_test, _, meta_test = time_split(
        X, y, meta, test_fraction)

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    report: dict = {
        "model_version": MODEL_VERSION,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "feature_columns": FEATURE_COLS,
        "targets": {},
    }

    tolerances = {
        "target_speed": SPEED_TOLERANCE_MPH,
        "target_volume": lambda yt, yp: np.abs(yt - yp) <= VOLUME_TOLERANCE_FRAC * np.maximum(yt, 1),
    }
    for target in ["target_speed", "target_volume"]:
        model, metrics, algo = train_target(
            X_train, y_train[target], X_test, y_test[target],
            target, tolerances[target], n_estimators)
        artifact = ARTIFACT_DIR / f"{MODEL_VERSION}_{target.split('_')[1]}.joblib"
        joblib.dump(model, artifact)
        metrics["algorithm"] = algo
        metrics["artifact"] = artifact.name
        report["targets"][target] = metrics

    with open(ARTIFACT_DIR / "feature_columns.json", "w") as f:
        json.dump(FEATURE_COLS, f, indent=2)
    with open(ARTIFACT_DIR / "metrics.json", "w") as f:
        json.dump(report, f, indent=2)

    print(json.dumps(report, indent=2))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Train traffic forecasting models")
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--test-fraction", type=float, default=0.2)
    parser.add_argument("--n-estimators", type=int, default=400)
    args = parser.parse_args()
    run(days=args.days, test_fraction=args.test_fraction,
        n_estimators=args.n_estimators)


if __name__ == "__main__":
    main()
