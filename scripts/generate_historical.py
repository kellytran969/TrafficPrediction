"""Generate DOT-style historical CSV dumps (one file per city per month).

These simulate the "many sources" the ETL merges: open-data exports from
different city DOTs, each with its own quirks (one file even contains a few
corrupt rows so the ETL's cleaning stage has something to do).

Run:
    python scripts/generate_historical.py [--months 3] [--out data/historical]
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.simulator.catalog import SENSORS  # noqa: E402
from services.simulator.main import generate_reading  # noqa: E402


def month_range(year: int, month: int):
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    end = datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=timezone.utc)
    return start, end


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", type=int, default=3)
    parser.add_argument("--out", default="data/historical")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(args.seed)

    now = datetime.now(timezone.utc)
    months = []
    y, m = now.year, now.month
    for _ in range(args.months):
        months.append((y, m))
        m -= 1
        if m == 0:
            m, y = 12, y - 1

    by_city: dict[str, list] = {}
    for spec in SENSORS:
        by_city.setdefault(spec.city, []).append(spec)

    total = 0
    for year, month in months:
        start, end = month_range(year, month)
        if start > now:
            continue
        for city, specs in by_city.items():
            path = out / f"{city.lower().replace(' ', '_')}_{year}-{month:02d}.csv"
            with open(path, "w", newline="") as f:
                writer = csv.DictWriter(
                    f, fieldnames=["sensor_id", "ts", "speed_mph",
                                   "volume_vph", "occupancy_pct"])
                writer.writeheader()
                ts = start
                while ts < min(end, now):
                    for spec in specs:
                        reading = generate_reading(spec, ts, rng)
                        writer.writerow(reading)
                        total += 1
                    ts += timedelta(hours=1)
            # sprinkle a few corrupt rows into one file per month
            if city == "New York":
                with open(path, "a", newline="") as f:
                    f.write("NYC-001,not-a-timestamp,fast,3000,25.0\n")
                    f.write("NYC-999,2026-01-01T00:00:00+00:00,55.0,2000,20.0\n")
            print(f"wrote {path}")
    print(f"{total} historical rows across {len(months)} months")


if __name__ == "__main__":
    main()
