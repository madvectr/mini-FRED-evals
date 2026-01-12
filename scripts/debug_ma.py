#!/usr/bin/env python3
"""Debug helper to inspect moving-average windows."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb  # type: ignore[import]

from src import truth
from src.parse import parse_question
from src.util import resolve_project_root


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect the MA window used for a question.")
    parser.add_argument("question", help="Natural language question to parse.")
    parser.add_argument(
        "--db",
        default="data/warehouse.duckdb",
        help="Path to DuckDB warehouse (default: data/warehouse.duckdb).",
    )
    parser.add_argument(
        "--series",
        help="Override parsed series_id (optional).",
    )
    parser.add_argument(
        "--date",
        help="Override parsed date (YYYY-MM or YYYY-MM-DD).",
    )
    parser.add_argument(
        "--periods",
        type=int,
        help="Override parsed moving-average periods.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = resolve_project_root()
    db_path = (project_root / args.db).resolve()

    parsed = parse_question(args.question)
    if args.series:
        parsed.series_id = args.series.upper()
    if args.date:
        override_date = args.date.strip()
        # allow YYYY-MM shorthand
        parsed.date = override_date if len(override_date) == 10 else f"{override_date}-01"
    if args.periods:
        parsed.periods = args.periods

    if parsed.transform != "ma":
        parsed.transform = "ma"
        if parsed.periods is None:
            parsed.periods = args.periods or 3

    print("Question:", args.question)
    print("Parsed:")
    print(f"  series_id : {parsed.series_id}")
    print(f"  date      : {parsed.date}")
    print(f"  periods   : {parsed.periods}")
    if parsed.errors:
        print("  errors    :", "; ".join(parsed.errors))

    if not parsed.series_id or not parsed.date or not parsed.periods:
        print("Missing required fields to compute MA.")
        return

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        window = truth.select_trailing_window(
            con,
            parsed.series_id,
            parsed.date,
            parsed.periods,
            inclusive_end=True,
        )
        if not window:
            print("No rows found for the requested window.")
            return
        print("\nWindow rows (oldest -> newest):")
        for row_date, value in window:
            print(f"  {row_date}: {value}")
        ma_value = truth.get_ma(con, parsed.series_id, parsed.date, parsed.periods)
        print(f"\nComputed MA({parsed.periods}, {parsed.date}): {ma_value}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
