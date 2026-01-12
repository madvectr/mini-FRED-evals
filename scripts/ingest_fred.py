#!/usr/bin/env python3
"""Download Mini-FRED data, load DuckDB, and export reproducible snapshots."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

# Ensure the repo root (parent of scripts/) is importable.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import fred_client, warehouse
from src.util import ensure_directory, load_series_config, resolve_project_root


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Mini-FRED data into DuckDB.")
    parser.add_argument(
        "--config",
        default="config/series.yaml",
        help="Path to series.yaml (default: config/series.yaml).",
    )
    parser.add_argument(
        "--db",
        default="data/warehouse.duckdb",
        help="DuckDB file output path (default: data/warehouse.duckdb).",
    )
    parser.add_argument(
        "--cache-dir",
        default="data/raw",
        help="Directory for cached FRED responses (default: data/raw).",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Bypass cache files and re-download from FRED.",
    )
    parser.add_argument(
        "--export-snapshots",
        action="store_true",
        help="Export series/observations tables to CSV under data/snapshots/.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = resolve_project_root()
    config_path = _resolve_path(project_root, args.config)
    config = load_series_config(config_path)
    db_path = _resolve_path(project_root, args.db)
    cache_dir = _resolve_path(project_root, args.cache_dir)
    ensure_directory(cache_dir)

    date_range = config.get("date_range", {})
    start = date_range.get("start")
    end = date_range.get("end")
    if not start or not end:
        raise ValueError("config date_range.start and date_range.end are required.")

    con = warehouse.get_connection(db_path)
    warehouse.create_schema(con)

    series_stats: Dict[str, Dict[str, object]] = {}
    metadata_records: Dict[str, Dict[str, object]] = {}
    series_entries: List[Dict[str, object]] = config.get("series", [])

    for entry in series_entries:
        series_id = entry["id"]
        print(f"Ingesting {series_id}...")
        metadata = fred_client.fetch_series_metadata(
            series_id,
            cache_dir=cache_dir,
            refresh=args.refresh,
        )
        warehouse.upsert_series_metadata(con, metadata)
        metadata_records[series_id] = metadata

        observations_payload = fred_client.fetch_series_observations(
            series_id,
            start,
            end,
            cache_dir=cache_dir,
            refresh=args.refresh,
        )
        warehouse.upsert_observations(con, series_id, observations_payload)

        observations = observations_payload.get("observations", [])
        first_date = observations[0]["date"] if observations else None
        last_date = observations[-1]["date"] if observations else None
        series_stats[series_id] = {
            "rows": len(observations),
            "first_date": first_date,
            "last_date": last_date,
        }

    _write_manifest(
        project_root=project_root,
        metadata_records=metadata_records,
        series_stats=series_stats,
        date_range=date_range,
        truth_policy=config.get("truth_policy"),
        db_path=db_path,
        refresh_requested=args.refresh,
    )

    if args.export_snapshots:
        _export_snapshots(con, project_root)

    print("Ingest complete.")
    print(f"DuckDB warehouse: {db_path}")
    if args.export_snapshots:
        print("Snapshots written to data/snapshots/")


def _resolve_path(project_root: Path, raw_path: str) -> Path:
    """Resolve CLI paths relative to the project root."""
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def _write_manifest(
    *,
    project_root: Path,
    metadata_records: Dict[str, Dict[str, object]],
    series_stats: Dict[str, Dict[str, object]],
    date_range: Dict[str, str],
    truth_policy: Dict[str, object] | None,
    db_path: Path,
    refresh_requested: bool,
) -> None:
    """Persist a manifest describing the snapshot contents."""
    manifest_path = ensure_directory(project_root / "data") / "MANIFEST.json"
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "warehouse_path": str(db_path),
        "date_range": date_range,
        "truth_policy": truth_policy,
        "refresh_requested": refresh_requested,
        "series": [],
    }
    for series_id, stats in series_stats.items():
        manifest["series"].append(
            {
                "series_id": series_id,
                "title": metadata_records[series_id].get("title"),
                "rows": stats["rows"],
                "first_date": stats["first_date"],
                "last_date": stats["last_date"],
            }
        )

    with open(manifest_path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")


def _export_snapshots(con, project_root: Path) -> None:
    """Write CSV exports for series + observations tables."""
    snapshot_dir = ensure_directory(project_root / "data" / "snapshots")
    series_csv = snapshot_dir / "series.csv"
    observations_csv = snapshot_dir / "observations.csv"
    con.execute(
        f"COPY (SELECT * FROM series ORDER BY series_id) TO '{series_csv}' (HEADER, DELIMITER ',');"
    )
    con.execute(
        f"COPY (SELECT * FROM observations ORDER BY series_id, date) TO '{observations_csv}' (HEADER, DELIMITER ',');"
    )


if __name__ == "__main__":
    main()
