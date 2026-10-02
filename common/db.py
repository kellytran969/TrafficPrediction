"""SQLAlchemy models and engine helpers.

Tables:
  sensors          - static catalog of road sensors
  readings         - raw high-frequency observations (ingestion writes)
  hourly_features  - cleaned, aggregated warehouse table (ETL writes)
  predictions      - log of served forecasts (API writes)
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from .config import get_settings


class Base(DeclarativeBase):
    pass


class Sensor(Base):
    __tablename__ = "sensors"

    sensor_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    city: Mapped[str] = mapped_column(String(64), index=True)
    highway: Mapped[str] = mapped_column(String(64))
    direction: Mapped[str] = mapped_column(String(16))
    latitude: Mapped[float] = mapped_column(Float)
    longitude: Mapped[float] = mapped_column(Float)
    lanes: Mapped[int] = mapped_column(Integer, default=3)


class Reading(Base):
    __tablename__ = "readings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sensor_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("sensors.sensor_id"), index=True
    )
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    speed_mph: Mapped[float] = mapped_column(Float)
    volume_vph: Mapped[int] = mapped_column(Integer)
    occupancy_pct: Mapped[float] = mapped_column(Float)

    __table_args__ = (
        UniqueConstraint("sensor_id", "ts", name="uq_reading_sensor_ts"),
    )


class HourlyFeature(Base):
    """One cleaned, aggregated row per sensor per hour - the ML training table."""

    __tablename__ = "hourly_features"

    sensor_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    hour: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    avg_speed: Mapped[float] = mapped_column(Float)
    total_volume: Mapped[int] = mapped_column(Integer)
    avg_occupancy: Mapped[float] = mapped_column(Float)
    samples: Mapped[int] = mapped_column(Integer)


class Prediction(Base):
    __tablename__ = "predictions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sensor_id: Mapped[str] = mapped_column(String(32), index=True)
    target_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    predicted_speed_mph: Mapped[float] = mapped_column(Float)
    model_version: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_engine(database_url: str | None = None):
    url = database_url or get_settings().database_url
    connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
    return create_engine(url, connect_args=connect_args, pool_pre_ping=True)


def init_db(database_url: str | None = None) -> None:
    """Create all tables (safe to call on every service start)."""
    Base.metadata.create_all(get_engine(database_url))


def get_session_factory(database_url: str | None = None):
    return sessionmaker(bind=get_engine(database_url), expire_on_commit=False)
