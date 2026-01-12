#!/usr/bin/env python3
"""Placeholder CLI for Mini-FRED QC checks."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.util import load_series_config, resolve_project_root


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scaffold for running QC checks on the DuckDB snapshot."
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Optional path to config/series.yaml (defaults to repo config).",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        help="DuckDB snapshot to inspect (defaults to data/snapshots/mini_fred.duckdb).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = resolve_project_root()
    config_path = (
        Path(args.config).expanduser().resolve()
        if args.config
        else project_root / "config" / "series.yaml"
    )
    config = load_series_config(config_path)
    db_path = (
        Path(args.db_path).expanduser().resolve()
        if args.db_path
        else project_root / "data" / "snapshots" / "mini_fred.duckdb"
    )

    print("=== Mini-FRED QC scaffold ===")
    print(f"Config: {config_path}")
    print(f"DuckDB snapshot (expected): {db_path}")
    print(f"Series locked for QC: {[s['id'] for s in config.get('series', [])]}")
    print("\nTODO (future implementation):")
    print("  - schema_check: ensure series_metadata & series_observations tables exist.")
    print("  - coverage_check: verify date ranges cover 2000-01-01 -> 2025-12-31.")
    print("  - null_check: report missing values or placeholder '.' entries.")
    print("  - freshness_check: confirm snapshot timestamp matches manifest entry.")
    print("  - report: emit markdown/JSON summary for MVES verifiers.")


if __name__ == "__main__":
    main()
