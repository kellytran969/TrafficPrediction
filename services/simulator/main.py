"""Synthetic traffic sensor data generator.

Produces realistic readings (speed / volume / occupancy) with diurnal rush-hour
patterns, weekday-vs-weekend differences and random incidents, then publishes
them to the stream bus.

Two modes:
  live      - emit readings in real time (default)
  backfill  - emit historical readings as fast as possible (for demos/training)

Example:
    python -m services.simulator.main --mode backfill --days 30 --step-minutes 15
    python -m services.simulator.main --mode live --rate 20
"""

from __future__ import annotations

import argparse
import random
import time
from datetime import datetime, timedelta, timezone

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common.stream import get_bus  # noqa: E402
from services.simulator.catalog import BY_ID, SENSORS, SensorSpec  # noqa: E402


def congestion_factor(ts: datetime, rng: random.Random) -> float:
    """0 (empty road) .. ~0.6 (gridlock). Weekdays have sharp AM/PM peaks."""
    hour = ts.hour + ts.minute / 60.0
    weekday = ts.weekday() < 5

    def bump(center: float, width: float, height: float) -> float:
        return height * max(0.0, 1.0 - abs(hour - center) / width)

    if weekday:
        c = bump(8.25, 1.75, 0.50) + bump(17.5, 2.5, 0.60) + bump(12.5, 4.0, 0.18)
    else:
        c = bump(13.0, 4.0, 0.28) + bump(18.0, 2.5, 0.22)
    c += rng.uniform(-0.04, 0.04)
    return max(0.0, min(0.75, c))


def generate_reading(spec: SensorSpec, ts: datetime,
                     rng: random.Random) -> dict:
    """One synthetic observation for a sensor at a timestamp (pure function)."""
    c = congestion_factor(ts, rng)
    incident = rng.random() < 0.02  # ~2% of readings hit an incident slowdown

    speed = spec.free_flow_mph * (1.0 - 0.72 * c) + rng.uniform(-2.5, 2.5)
    if incident:
        speed *= 0.55
    speed = max(5.0, min(spec.free_flow_mph + 5.0, speed))

    # fundamental diagram: volume rises with demand, then falls in gridlock
    demand = 0.25 + 1.5 * c
    capacity_factor = 1.0 if c < 0.55 else 0.75
    volume = int(spec.lanes * 950 * demand * capacity_factor
                 + rng.uniform(-60, 60))
    volume = max(20, volume)

    occupancy = min(100.0, max(2.0,
        volume / (spec.lanes * 2200) * 100 * (1.0 + 0.8 * c)
        + rng.uniform(-1.5, 1.5)))

    return {
        "sensor_id": spec.sensor_id,
        "ts": ts.isoformat(),
        "speed_mph": round(speed, 1),
        "volume_vph": volume,
        "occupancy_pct": round(occupancy, 1),
    }


def run(mode: str, rate: float, duration: float | None, days: int,
        step_minutes: int, city: str | None, seed: int | None,
        bus=None) -> int:
    rng = random.Random(seed)
    specs = [s for s in SENSORS if city is None or s.city == city]
    if not specs:
        raise SystemExit(f"no sensors for city={city!r}")
    bus = bus or get_bus()

    count = 0
    if mode == "backfill":
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        start = now - timedelta(days=days)
        ts = start
        while ts <= now:
            for spec in specs:
                bus.publish(generate_reading(spec, ts, rng))
                count += 1
            ts += timedelta(minutes=step_minutes)
        print(f"backfilled {count} readings for {len(specs)} sensors "
              f"({days} days @ {step_minutes}-min steps)")
        return count

    # live mode
    interval = 1.0 / max(rate, 0.01)
    deadline = None if duration is None else time.time() + duration
    print(f"publishing live readings for {len(specs)} sensors at {rate}/s "
          f"(Ctrl-C to stop)")
    try:
        while deadline is None or time.time() < deadline:
            spec = rng.choice(specs)
            bus.publish(generate_reading(spec, datetime.now(timezone.utc), rng))
            count += 1
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    print(f"published {count} readings")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Traffic sensor data simulator")
    parser.add_argument("--mode", choices=["live", "backfill"], default="live")
    parser.add_argument("--rate", type=float, default=10.0,
                        help="readings per second (live mode)")
    parser.add_argument("--duration", type=float, default=None,
                        help="seconds to run (live mode; default: forever)")
    parser.add_argument("--days", type=int, default=30, help="backfill window")
    parser.add_argument("--step-minutes", type=int, default=15,
                        help="time step between backfilled readings")
    parser.add_argument("--city", default=None, help="only simulate one city")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()
    run(args.mode, args.rate, args.duration, args.days,
        args.step_minutes, args.city, args.seed)


if __name__ == "__main__":
    main()
