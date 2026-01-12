"""DuckDB warehouse helpers (schema management + upsert utilities)."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping, Sequence

import duckdb  # type: ignore[import]

from .util import ensure_directory


def get_connection(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    """Create or open the DuckDB database file at the requested location."""
    path = Path(db_path).expanduser().resolve()
    ensure_directory(path.parent)
    return duckdb.connect(str(path))


def create_schema(con) -> None:
    """Create the Mini-FRED schema in DuckDB if it does not already exist."""
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS series (
            series_id TEXT PRIMARY KEY,
            title TEXT,
            units TEXT,
            frequency TEXT,
            seasonal_adjustment TEXT,
            notes TEXT,
            last_updated TEXT
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS observations (
            series_id TEXT,
            date DATE,
            value DOUBLE,
            PRIMARY KEY (series_id, date)
        );
        """
    )


def upsert_series_metadata(con, metadata: Mapping[str, object]) -> None:
    """Upsert a single series metadata record."""

    record = {
        "series_id": metadata.get("id") or metadata.get("series_id"),
        "title": metadata.get("title"),
        "units": metadata.get("units"),
        "frequency": metadata.get("frequency"),
        "seasonal_adjustment": metadata.get("seasonal_adjustment"),
        "notes": metadata.get("notes"),
        "last_updated": metadata.get("last_updated"),
    }
    if not record["series_id"]:
        raise ValueError("Metadata payload must include 'id' or 'series_id'.")

    con.execute(
        """
        INSERT INTO series (
            series_id, title, units, frequency, seasonal_adjustment, notes, last_updated
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (series_id) DO UPDATE SET
            title = excluded.title,
            units = excluded.units,
            frequency = excluded.frequency,
            seasonal_adjustment = excluded.seasonal_adjustment,
            notes = excluded.notes,
            last_updated = excluded.last_updated;
        """,
        [
            record["series_id"],
            record["title"],
            record["units"],
            record["frequency"],
            record["seasonal_adjustment"],
            record["notes"],
            record["last_updated"],
        ],
    )


def upsert_observations(
    con,
    series_id: str,
    observations_payload: Mapping[str, Iterable[Mapping[str, object]]],
) -> None:
    """Upsert observation rows for a series."""

    rows: Sequence[Mapping[str, object]] = list(
        observations_payload.get("observations", [])
    )
    if not rows:
        return

    normalized_rows = [
        (
            series_id,
            row.get("date"),
            _coerce_value(row.get("value")),
        )
        for row in rows
    ]

    con.executemany(
        """
        INSERT INTO observations (series_id, date, value)
        VALUES (?, ?, ?)
        ON CONFLICT (series_id, date) DO UPDATE SET
            value = excluded.value;
        """,
        normalized_rows,
    )


def _coerce_value(value: object) -> float | None:
    """Return a float value or None for placeholder markers."""
    if value in (None, "", ".", "NA", "nan", "NaN"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
