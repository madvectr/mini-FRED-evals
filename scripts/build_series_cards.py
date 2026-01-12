#!/usr/bin/env python3
"""Generate Markdown series cards from the Mini-FRED warehouse."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import series_cards, warehouse  # noqa: E402
from src.util import ensure_directory, load_series_config, resolve_project_root  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Markdown series cards.")
    parser.add_argument(
        "--config",
        default="config/series.yaml",
        help="Path to series.yaml (default: config/series.yaml).",
    )
    parser.add_argument(
        "--db",
        default="data/warehouse.duckdb",
        help="Path to DuckDB warehouse (default: data/warehouse.duckdb).",
    )
    parser.add_argument(
        "--out",
        default="corpus/series_cards",
        help="Output directory for Markdown cards (default: corpus/series_cards).",
    )
    parser.add_argument(
        "--last-n",
        type=int,
        default=12,
        help="Number of recent observations to include per card (default: 12).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = resolve_project_root()
    config_path = _resolve_path(project_root, args.config)
    db_path = _resolve_path(project_root, args.db)
    output_dir = ensure_directory(_resolve_path(project_root, args.out))

    config = load_series_config(config_path)
    series_entries: List[Dict[str, object]] = config.get("series", [])

    con = warehouse.get_connection(db_path)

    for entry in series_entries:
        series_id = entry["id"]
        metadata = _fetch_metadata(con, series_id)
        observations = _fetch_recent_observations(con, series_id, args.last_n)
        card = series_cards.render_series_card(metadata, observations, last_n=args.last_n)
        card_path = output_dir / f"series_{series_id}.md"
        with card_path.open("w", encoding="utf-8") as handle:
            handle.write(card)
        print(f"Wrote {card_path}")


def _fetch_metadata(con, series_id: str) -> Dict[str, object]:
    row = con.execute(
        """
        SELECT series_id, title, units, frequency, seasonal_adjustment, notes, last_updated
        FROM series
        WHERE series_id = ?
        """,
        [series_id],
    ).fetchone()
    if not row:
        raise ValueError(f"Series metadata not found in DuckDB for {series_id}.")
    return {
        "series_id": row[0],
        "title": row[1],
        "units": row[2],
        "frequency": row[3],
        "seasonal_adjustment": row[4],
        "notes": row[5],
        "last_updated": row[6],
    }


def _fetch_recent_observations(con, series_id: str, limit: int) -> List[Dict[str, object]]:
    rows = con.execute(
        """
        SELECT date, value
        FROM observations
        WHERE series_id = ?
        ORDER BY date DESC
        LIMIT ?
        """,
        [series_id, limit],
    ).fetchall()
    return [{"date": row[0], "value": row[1]} for row in rows]


def _resolve_path(project_root: Path, raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


if __name__ == "__main__":
    main()
