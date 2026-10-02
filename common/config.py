"""Central configuration, read from environment variables with sane local defaults."""

from __future__ import annotations

import os
from functools import lru_cache


class Settings:
    """All knobs for every service. Override via environment variables."""

    def __init__(self) -> None:
        # sqlite is the zero-setup local default; compose/k8s override with postgres
        self.database_url: str = os.getenv(
            "DATABASE_URL", "sqlite:///./traffic.db"
        )
        self.redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        # "redis" for real streaming, "memory" for zero-infra local runs/tests
        self.stream_backend: str = os.getenv("STREAM_BACKEND", "memory")
        self.cache_backend: str = os.getenv("CACHE_BACKEND", "memory")

        self.stream_name: str = os.getenv("STREAM_NAME", "traffic:readings")
        self.consumer_group: str = os.getenv("CONSUMER_GROUP", "ingestion")

        self.api_host: str = os.getenv("API_HOST", "0.0.0.0")
        self.api_port: int = int(os.getenv("API_PORT", "8000"))

        # model artifacts written by ml/train.py and read by the API
        self.model_dir: str = os.getenv("MODEL_DIR", "ml/artifacts")
        self.model_name: str = os.getenv("MODEL_NAME", "xgb_speed_v1.joblib")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
