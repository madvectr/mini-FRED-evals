"""FRED API client with lightweight caching + retry logic."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests  # type: ignore[import]
from dotenv import load_dotenv  # type: ignore[import]

from .util import ensure_directory, resolve_project_root


FRED_API_KEY_ENV = "FRED_API_KEY"
FRED_BASE_URL = "https://api.stlouisfed.org/fred"
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 5
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
BACKOFF_BASE_SECONDS = 1.5


def get_api_key() -> Optional[str]:
    """Return the FRED API key if it is set in the environment."""
    _ensure_env_loaded()
    return os.getenv(FRED_API_KEY_ENV)


def fetch_series_metadata(
    series_id: str,
    *,
    cache_dir: str | Path | None = None,
    refresh: bool = False,
) -> Dict[str, Any]:
    """Retrieve metadata for a FRED series, caching the JSON response to disk."""

    cache_file = _build_cache_path(
        cache_dir,
        f"series_{series_id}.json",
    )
    payload = _get_or_request_json(
        endpoint="series",
        params={"series_id": series_id},
        cache_file=cache_file,
        refresh=refresh,
    )
    series_list = payload.get("seriess", [])
    if not series_list:
        raise ValueError(f"No metadata returned for series '{series_id}'.")
    return series_list[0]


def fetch_series_observations(
    series_id: str,
    start_date: str,
    end_date: str,
    *,
    frequency: Optional[str] = None,
    cache_dir: str | Path | None = None,
    refresh: bool = False,
) -> Dict[str, Any]:
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

    Returns the raw JSON payload that includes the `observations` array. Results are
    cached in data/raw/ to enable offline replays.
    """

    cache_name = (
        f"observations_{series_id}_{start_date.replace('-', '')}_{end_date.replace('-', '')}.json"
    )
    cache_file = _build_cache_path(cache_dir, cache_name)
    params: Dict[str, Any] = {
        "series_id": series_id,
        "observation_start": start_date,
        "observation_end": end_date,
        "sort_order": "asc",
    }
    if frequency:
        params["frequency"] = frequency
    payload = _get_or_request_json(
        endpoint="series/observations",
        params=params,
        cache_file=cache_file,
        refresh=refresh,
    )
    return payload


def _build_cache_path(cache_dir: str | Path | None, filename: str) -> Path:
    """Resolve the cache directory and ensure it exists."""
    root = resolve_project_root()
    directory = Path(cache_dir).expanduser().resolve() if cache_dir else root / "data" / "raw"
    ensure_directory(directory)
    return directory / filename


def _get_or_request_json(
    *,
    endpoint: str,
    params: Dict[str, Any],
    cache_file: Path,
    refresh: bool,
) -> Dict[str, Any]:
    """Fetch JSON data, honoring cache files unless refresh=True."""

    if cache_file.exists() and not refresh:
        with cache_file.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    api_key = get_api_key()
    if not api_key:
        raise RuntimeError(
            f"Missing {FRED_API_KEY_ENV}. Export it before running the ingest workflow."
        )

    url = f"{FRED_BASE_URL}/{endpoint}"
    params_with_auth = {**params, "api_key": api_key, "file_type": "json"}

    last_error: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params_with_auth, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:  # pragma: no cover - network errors
            last_error = exc
            _sleep_with_backoff(attempt)
            continue

        if response.status_code == 200:
            data = response.json()
            with cache_file.open("w", encoding="utf-8") as handle:
                json.dump(data, handle, indent=2)
                handle.write("\n")
            return data

        if response.status_code in RETRY_STATUS_CODES:
            last_error = RuntimeError(
                f"FRED API rate limit or server error ({response.status_code})."
            )
            _sleep_with_backoff(attempt)
            continue

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            last_error = exc
        break

    raise RuntimeError(
        f"Failed to fetch '{endpoint}' after {MAX_RETRIES} attempts."
    ) from last_error


def _sleep_with_backoff(attempt: int) -> None:
    """Sleep with exponential backoff between retries."""
    delay = BACKOFF_BASE_SECONDS * (2**attempt)
    time.sleep(delay)


_ENV_LOADED = False


def _ensure_env_loaded() -> None:
    """Load .env once so FRED_API_KEY is available without manual exports."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    dotenv_path = resolve_project_root() / ".env"
    load_dotenv(dotenv_path=dotenv_path, override=False)
    _ENV_LOADED = True
