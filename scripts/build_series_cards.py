#!/usr/bin/env python3
"""Placeholder CLI for generating Mini-FRED series cards."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.util import load_series_config, resolve_project_root, ensure_directory


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scaffold for generating Markdown series cards."
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Optional path to series.yaml (defaults to repo config).",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        help="DuckDB snapshot to read from.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Directory for emitted cards (defaults to corpus/series_cards).",
    )
    parser.add_argument(
        "--last-n",
        type=int,
        default=6,
        help="Number of recent observations to include per card (default: 6).",
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
    output_dir = (
        ensure_directory(args.output_dir)
        if args.output_dir
        else ensure_directory(project_root / "corpus" / "series_cards")
    )

    print("=== Mini-FRED series card scaffold ===")
    print(f"Config: {config_path}")
    print(f"Planned DuckDB snapshot: {db_path}")
    print(f"Output directory: {output_dir}")
    print(f"Recent observations per card: {args.last_n}")
    print("\nTODO (future implementation):")
    print("  1. Query DuckDB for metadata + last N observations per series.")
    print("  2. Use src.series_cards.render_series_card to create Markdown.")
    print("  3. Write one file per series (e.g., corpus/series_cards/CPIAUCSL.md).")
    print("  4. Commit outputs for deterministic RAG citations.")
    print("  5. Add manifest entries so downstream agents know which snapshot to cite.")
    print("\nFor now this script only validates configuration and outlines the workflow.")


if __name__ == "__main__":
    main()
