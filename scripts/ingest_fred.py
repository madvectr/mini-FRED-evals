#!/usr/bin/env python3
"""Placeholder CLI for FRED ingest."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure the repo root (parent of scripts/) is importable.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.util import load_series_config, resolve_project_root


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scaffold: document the intended ingest workflow."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to series.yaml (defaults to config/series.yaml).",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Optional override for DuckDB file (defaults to data/snapshots/mini_fred.duckdb).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = resolve_project_root()
    config_path = Path(args.config) if args.config else project_root / "config/series.yaml"
    config = load_series_config(config_path)

    db_path = (
        Path(args.db_path).expanduser().resolve()
        if args.db_path
        else project_root / "data" / "snapshots" / "mini_fred.duckdb"
    )

    series_ids = [entry["id"] for entry in config.get("series", [])]
    print("=== Mini-FRED ingest scaffold ===")
    print(f"Config: {config_path}")
    print(f"Planned DuckDB path: {db_path}")
    print(f"Series ({len(series_ids)} total): {', '.join(series_ids)}")
    print(f"Date window: {config.get('date_range')}")
    print("Truth policy:", config.get("truth_policy"))
    print("\nTODO (future implementation):")
    print("  1. Pull metadata for each series via src.fred_client.fetch_series_metadata.")
    print("  2. Pull observations for the locked date range.")
    print("  3. Persist raw JSON/CSV dumps under data/raw/ for auditing.")
    print("  4. Load data into DuckDB via src.warehouse.* helpers and snapshot the file.")
    print(
        "  5. Emit a manifest describing snapshot timestamp, source hash, and coverage."
    )


if __name__ == "__main__":
    main()
