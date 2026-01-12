"""FRED API client scaffolding (networking intentionally unimplemented)."""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional


FRED_API_KEY_ENV = "FRED_API_KEY"
FRED_BASE_URL = "https://api.stlouisfed.org/fred"


def get_api_key() -> Optional[str]:
    """Return the FRED API key if it is set in the environment."""
    return os.getenv(FRED_API_KEY_ENV)


def fetch_series_metadata(series_id: str) -> Dict[str, Any]:
    """Retrieve metadata for a FRED series.

    Expected behavior once implemented:
    - Issue GET {FRED_BASE_URL}/series with parameters (series_id, api_key, file_type=json).
    - Apply exponential backoff (e.g., 1s, 2s, 4s) when HTTP 429 is returned to respect
      FRED's per-IP rate limit.
    - Parse the first element from the returned `seriess` list and normalize keys.

    Returns
    -------
    dict
        A dictionary containing keys like series_id, title, units, frequency, etc.

    Notes
    -----
    Networking is intentionally disabled in the scaffold; the function raises
    NotImplementedError to make that explicit.
    """

    raise NotImplementedError(
        "Networking disabled in scaffold. Implement FRED metadata fetch during ingest."
    )


def fetch_series_observations(
    series_id: str,
    start_date: str,
    end_date: str,
    frequency: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Retrieve observation rows for a FRED series.

    Parameters
    ----------
    series_id : str
        The FRED series identifier (e.g., CPIAUCSL).
    start_date : str
        ISO date string (YYYY-MM-DD) marking the inclusive lower bound.
    end_date : str
        ISO date string (YYYY-MM-DD) marking the inclusive upper bound.
    frequency : str, optional
        Optional override to let FRED aggregate daily data (e.g., FEDFUNDS) into
        monthly averages via the `frequency` and `aggregation_method` parameters.

    Planned behavior:
    - Use /series/observations endpoint with proper pagination.
    - Apply rate-limit friendly sleeps and retries on 429/5xx.
    - Return a list of dicts with normalized keys: observation_date, value, status.

    Returns
    -------
    list[dict]
        Placeholder until ingest implementation lands.
    """

    raise NotImplementedError(
        "Networking disabled in scaffold. Implement observation fetch during ingest."
    )
