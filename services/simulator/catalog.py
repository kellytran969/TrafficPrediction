"""Static catalog of traffic sensors across major U.S. metros."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SensorSpec:
    sensor_id: str
    city: str
    highway: str
    direction: str
    latitude: float
    longitude: float
    lanes: int
    free_flow_mph: float  # speed with no congestion


SENSORS: list[SensorSpec] = [
    # ---- New York City metro ----
    SensorSpec("NYC-001", "New York", "I-95", "NB", 40.8681, -73.9052, 4, 65),
    SensorSpec("NYC-002", "New York", "I-95", "SB", 40.8456, -73.9158, 4, 65),
    SensorSpec("NYC-003", "New York", "I-278", "EB", 40.7260, -73.9480, 3, 55),
    SensorSpec("NYC-004", "New York", "I-278", "WB", 40.7128, -73.9650, 3, 55),
    SensorSpec("NYC-005", "New York", "I-495", "EB", 40.7450, -73.8800, 4, 60),
    SensorSpec("NYC-006", "New York", "I-495", "WB", 40.7520, -73.9100, 4, 60),
    SensorSpec("NYC-007", "New York", "FDR Dr", "NB", 40.7620, -73.9580, 3, 50),
    SensorSpec("NYC-008", "New York", "FDR Dr", "SB", 40.7480, -73.9650, 3, 50),
    SensorSpec("NYC-009", "New York", "I-87", "NB", 40.8850, -73.8750, 3, 60),
    SensorSpec("NYC-010", "New York", "I-87", "SB", 40.8700, -73.8800, 3, 60),
    # ---- Los Angeles ----
    SensorSpec("LA-001", "Los Angeles", "US-101", "NB", 34.0750, -118.2450, 4, 65),
    SensorSpec("LA-002", "Los Angeles", "US-101", "SB", 34.0600, -118.2550, 4, 65),
    SensorSpec("LA-003", "Los Angeles", "I-405", "NB", 33.9900, -118.4200, 5, 65),
    SensorSpec("LA-004", "Los Angeles", "I-405", "SB", 34.0200, -118.4100, 5, 65),
    SensorSpec("LA-005", "Los Angeles", "I-10", "EB", 34.0350, -118.3000, 4, 65),
    SensorSpec("LA-006", "Los Angeles", "I-10", "WB", 34.0400, -118.3300, 4, 65),
    # ---- Chicago ----
    SensorSpec("CHI-001", "Chicago", "I-90", "EB", 41.8780, -87.6400, 4, 60),
    SensorSpec("CHI-002", "Chicago", "I-90", "WB", 41.8850, -87.6200, 4, 60),
    SensorSpec("CHI-003", "Chicago", "I-94", "NB", 41.9000, -87.6300, 3, 60),
    SensorSpec("CHI-004", "Chicago", "I-94", "SB", 41.8900, -87.6400, 3, 60),
    SensorSpec("CHI-005", "Chicago", "I-290", "EB", 41.8800, -87.7000, 3, 55),
    # ---- Houston ----
    SensorSpec("HOU-001", "Houston", "I-45", "NB", 29.7800, -95.3700, 4, 65),
    SensorSpec("HOU-002", "Houston", "I-45", "SB", 29.7600, -95.3800, 4, 65),
    SensorSpec("HOU-003", "Houston", "I-10", "EB", 29.7700, -95.4000, 5, 65),
    SensorSpec("HOU-004", "Houston", "I-10", "WB", 29.7750, -95.4300, 5, 65),
    # ---- Atlanta ----
    SensorSpec("ATL-001", "Atlanta", "I-75", "NB", 33.7900, -84.3900, 4, 65),
    SensorSpec("ATL-002", "Atlanta", "I-75", "SB", 33.7700, -84.4000, 4, 65),
    SensorSpec("ATL-003", "Atlanta", "I-85", "NB", 33.8100, -84.3600, 4, 65),
    SensorSpec("ATL-004", "Atlanta", "I-285", "EB", 33.8800, -84.2900, 4, 60),
]

BY_ID: dict[str, SensorSpec] = {s.sensor_id: s for s in SENSORS}
CITIES: list[str] = sorted({s.city for s in SENSORS})
