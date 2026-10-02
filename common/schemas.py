"""Pydantic schemas shared by the simulator, ingestion and API."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class SensorReading(BaseModel):
    """One observation from one road sensor."""

    sensor_id: str = Field(..., min_length=1)
    ts: datetime
    speed_mph: float = Field(..., ge=0, le=120)
    volume_vph: int = Field(..., ge=0)  # vehicles per hour
    occupancy_pct: float = Field(..., ge=0, le=100)


class SensorInfo(BaseModel):
    sensor_id: str
    city: str
    highway: str
    direction: str
    latitude: float
    longitude: float
    lanes: int


class PredictionRequest(BaseModel):
    sensor_id: str = Field(..., min_length=1)
    # ISO-8601 hour we want the forecast for; defaults to "next hour"
    target_time: datetime | None = None
    horizon_hours: int = Field(1, ge=1, le=6)


class PredictionResponse(BaseModel):
    sensor_id: str
    target_time: datetime
    predicted_speed_mph: float
    predicted_volume_vph: int
    congestion_level: str  # free_flow | moderate | heavy | severe
    model_version: str
