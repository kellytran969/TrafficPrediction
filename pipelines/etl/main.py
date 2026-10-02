"""ETL entrypoint: extract -> clean -> aggregate hourly -> load.

Run:
    python -m pipelines.etl.main [--days 30]
    python -m pipelines.etl.main --start 2026-09-01 --end 2026-10-01
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common.db import init_db  # noqa: E402
from pipelines.etl.load import load_hourly  # noqa: E402
from pipelines.etl.sources import default_window, extract_all  # noqa: E402
from pipelines.etl.transform import aggregate_hourly, clean  # noqa: E402


def run(start: datetime, end: datetime) -> dict:
    init_db()
    raw, extract_stats = extract_all(start, end)
    cleaned, clean_stats = clean(raw)
    hourly = aggregate_hourly(cleaned)
    written = load_hourly(hourly)
    report = {
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "extract": extract_stats,
        "clean": clean_stats,
        "hourly_rows": len(hourly),
        "rows_written": written,
    }
    print(json.dumps(report, indent=2))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Traffic ETL pipeline")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--start", default=None, help="ISO date, e.g. 2026-09-01")
    parser.add_argument("--end", default=None, help="ISO date, e.g. 2026-10-01")
    args = parser.parse_args()
    if args.start and args.end:
        start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
        end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    else:
        start, end = default_window(args.days)
    run(start, end)


if __name__ == "__main__":
    main()
