#!/usr/bin/env python3
"""Quality checks for the Mini-FRED DuckDB warehouse."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import warehouse  # noqa: E402
from src.util import load_series_config, resolve_project_root  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run QC checks on Mini-FRED data.")
    parser.add_argument(
        "--config",
        default="config/series.yaml",
        help="Path to config/series.yaml (default: config/series.yaml).",
    )
    parser.add_argument(
        "--db",
        default="data/warehouse.duckdb",
        help="DuckDB warehouse to inspect (default: data/warehouse.duckdb).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = resolve_project_root()
    config_path = _resolve_path(project_root, args.config)
    config = load_series_config(config_path)
    db_path = _resolve_path(project_root, args.db)

    date_range = config.get("date_range", {})
    start = _parse_date(date_range.get("start"))
    end = _parse_date(date_range.get("end"))
    if not start or not end:
        raise ValueError("date_range.start and date_range.end must be set.")

    series_entries: List[dict] = config.get("series", [])
    required_series = [entry["id"] for entry in series_entries]

    con = warehouse.get_connection(db_path)
    failures: list[str] = []
    warnings: list[str] = []

    stored_series = _fetch_series_ids(con)
    missing = sorted(set(required_series) - stored_series)
    if missing:
        failures.append(f"Missing series metadata rows: {', '.join(missing)}")

    dup_rows = con.execute(
        """
        SELECT series_id, date, COUNT(*) AS c
        FROM observations
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
        LIMIT 10
        """
    ).fetchall()
    if dup_rows:
        failures.append("Observations contain duplicate primary keys.")

    counts = dict(
        con.execute("SELECT series_id, COUNT(*) FROM observations GROUP BY 1").fetchall()
    )

    for entry in series_entries:
        series_id = entry["id"]
        count = counts.get(series_id, 0)
        min_rows = _min_rows_threshold(entry.get("frequency", ""))
        if count < min_rows:
            failures.append(f"{series_id}: insufficient rows ({count} < {min_rows}).")

        min_date, max_date = con.execute(
            "SELECT MIN(date), MAX(date) FROM observations WHERE series_id = ?",
            [series_id],
        ).fetchone()

        if min_date is None or max_date is None:
            failures.append(f"{series_id}: no stored observations.")
            continue

        if min_date > end or max_date < start:
            failures.append(
                f"{series_id}: coverage {min_date} → {max_date} outside {start} → {end}."
            )

        total_rows, null_rows = con.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS nulls
            FROM observations
            WHERE series_id = ?
            """,
            [series_id],
        ).fetchone()

        if total_rows:
            ratio = (null_rows or 0) / total_rows
            if ratio > 0.05:
                warnings.append(
                    f"{series_id}: {ratio:.1%} NULL values (exceeds 5% threshold)."
                )

    for failure in failures:
        print(f"FAIL: {failure}")
    for warning in warnings:
        print(f"WARN: {warning}")

    if failures:
        print("QC checks failed.")
        sys.exit(1)

    print("QC checks passed." if not warnings else "QC checks passed with warnings.")


def _resolve_path(project_root: Path, raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def _parse_date(value: str | None):
    if not value:
        return None
    return datetime.fromisoformat(value).date()


def _fetch_series_ids(con) -> set[str]:
    rows = con.execute("SELECT series_id FROM series").fetchall()
    return {row[0] for row in rows}


def _min_rows_threshold(frequency_str: str) -> int:
    freq = (frequency_str or "").lower()
    if "daily" in freq:
        return 200
    if "monthly" in freq:
        return 200
    if "quarter" in freq:
        return 80
    return 50


if __name__ == "__main__":
    main()
