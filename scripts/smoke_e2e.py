"""End-to-end smoke test: backfill -> ingest -> ETL -> train -> predict.

Wires the simulator, ingestion, ETL, trainer and API together in one process
(sharing an in-memory stream bus) and asserts the whole pipeline works.

Run:
    DATABASE_URL=sqlite:///traffic.smoke.db python scripts/smoke_e2e.py
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.cache import MemoryCache  # noqa: E402
from common.stream import MemoryStreamBus  # noqa: E402
from pipelines.etl.main import run as run_etl  # noqa: E402
from pipelines.etl.sources import default_window  # noqa: E402
from ml.train import run as run_train  # noqa: E402
from services.ingestion.main import run as run_ingestion  # noqa: E402
from services.simulator.main import run as run_simulator  # noqa: E402


def main() -> None:
    t0 = time.time()
    bus = MemoryStreamBus()
    cache = MemoryCache()

    n = run_simulator("backfill", rate=0, duration=None, days=30,
                      step_minutes=60, city=None, seed=123, bus=bus)
    print(f"[1/5] simulator backfilled {n} readings")

    stats = run_ingestion(batch_size=2000, max_batches=50, poll_ms=200,
                          bus=bus, cache=cache)
    print(f"[2/5] ingestion: {stats}")
    assert stats["inserted"] > 15000, "expected most backfilled rows ingested"

    start, end = default_window(days=31)
    report = run_etl(start, end)
    print(f"[3/5] etl: {report['hourly_rows']} hourly rows")
    assert report["hourly_rows"] > 15000

    metrics = run_train(days=31, test_fraction=0.2)
    speed = metrics["targets"]["target_speed"]
    print(f"[4/5] train: speed rmse={speed['rmse']} r2={speed['r2']} "
          f"accuracy={speed['accuracy']}")

    # serve one prediction through the real API stack
    from fastapi.testclient import TestClient
    from ml.model_service import ModelService
    from services.api.app import create_app

    client = TestClient(create_app(cache=cache,
                                   model_service=ModelService()))
    health = client.get("/health").json()
    assert health["model_loaded"], "model artifacts not loadable by API"
    pred = client.post("/predict", json={"sensor_id": "NYC-001"}).json()
    assert 0 < pred["predicted_speed_mph"] <= 120, pred
    print(f"[5/5] predict: NYC-001 next hour = "
          f"{pred['predicted_speed_mph']} mph ({pred['congestion_level']})")

    print(f"SMOKE OK in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
