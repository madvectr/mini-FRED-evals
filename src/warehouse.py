"""DuckDB warehouse helpers (schema + load stubs)."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping

import duckdb

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
        CREATE TABLE IF NOT EXISTS series_metadata (
            series_id TEXT PRIMARY KEY,
            title TEXT,
            units TEXT,
            frequency TEXT,
            seasonal_adjustment TEXT,
            notes TEXT,
            last_updated TIMESTAMP
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS series_observations (
            series_id TEXT,
            observation_date DATE,
            value DOUBLE,
            status TEXT,
            PRIMARY KEY (series_id, observation_date)
        );
        """
    )


def load_metadata(con, rows: Iterable[Mapping[str, object]]) -> None:
    """Insert normalized metadata rows into DuckDB (placeholder)."""
    raise NotImplementedError(
        "Metadata loading will be implemented once ingest wiring lands."
    )


def load_observations(con, rows: Iterable[Mapping[str, object]]) -> None:
    """Insert observation rows into DuckDB (placeholder)."""
    raise NotImplementedError(
        "Observation loading will be implemented once ingest wiring lands."
    )
