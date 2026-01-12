"""Deterministic truth computations over the Mini-FRED DuckDB warehouse."""

from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

_FREQUENCY_CACHE: Dict[str, str] = {}


def get_point(con, series_id: str, target_date: str) -> Optional[float]:
    """Return the value for a series on the specified date."""
    if not target_date:
        return None
    row = con.execute(
        """
        SELECT value
        FROM observations
        WHERE series_id = ? AND date = ?
        """,
        [series_id, target_date],
    ).fetchone()
    value = row[0] if row else None
    return float(value) if value is not None else None


def get_yoy(con, series_id: str, target_date: str) -> Optional[float]:
    """Return the year-over-year percent change."""
    if not target_date:
        return None
    current_value = get_point(con, series_id, target_date)
    if current_value is None:
        return None
    freq = _get_frequency(con, series_id)
    base_date = _shift_for_yoy(target_date, freq)
    prev_value = get_point(con, series_id, base_date)
    return _percent_change(current_value, prev_value)


def get_mom(con, series_id: str, target_date: str) -> Optional[float]:
    """Return the period-over-period percent change."""
    if not target_date:
        return None
    current_value = get_point(con, series_id, target_date)
    if current_value is None:
        return None
    freq = _get_frequency(con, series_id)
    base_date = _shift_for_mom(target_date, freq)
    prev_value = get_point(con, series_id, base_date)
    return _percent_change(current_value, prev_value)


def select_trailing_window(
    con,
    series_id: str,
    end_date: Optional[str],
    periods: int,
    inclusive_end: bool = True,
) -> List[Tuple[str, Optional[float]]]:
    """Return the trailing window (oldest -> newest) used for moving averages."""
    if not end_date or not periods or periods <= 0:
        return []
    comparator = "<=" if inclusive_end else "<"
    rows = con.execute(
        f"""
        SELECT date, value
        FROM observations
        WHERE series_id = ?
          AND date {comparator} ?
        ORDER BY date DESC
        LIMIT ?
        """,
        [series_id, end_date, periods],
    ).fetchall()
    return list(reversed(rows))


def get_ma(con, series_id: str, target_date: str, periods: int) -> Optional[float]:
    """Return MA(N, D): the average of the trailing N observations ending at D (inclusive)."""
    window = select_trailing_window(con, series_id, target_date, periods, inclusive_end=True)
    if len(window) < periods:
        return None
    values = [row[1] for row in window if row[1] is not None]
    if len(values) < periods:
        return None
    return float(sum(values) / periods)


def get_max(
    con,
    series_id: str,
    window_start: str,
    window_end: str,
) -> Tuple[Optional[str], Optional[float]]:
    """Return (date, value) for the maximum observation within the window."""
    window_start, window_end = _normalize_window(window_start, window_end)
    if not window_start or not window_end:
        return None, None
    row = con.execute(
        """
        SELECT date, value
        FROM observations
        WHERE series_id = ?
          AND date BETWEEN ? AND ?
          AND value IS NOT NULL
        ORDER BY value DESC
        LIMIT 1
        """,
        [series_id, window_start, window_end],
    ).fetchone()
    if not row:
        return None, None
    return row[0], float(row[1]) if row[1] is not None else None


def get_min(
    con,
    series_id: str,
    window_start: str,
    window_end: str,
) -> Tuple[Optional[str], Optional[float]]:
    """Return (date, value) for the minimum observation within the window."""
    window_start, window_end = _normalize_window(window_start, window_end)
    if not window_start or not window_end:
        return None, None
    row = con.execute(
        """
        SELECT date, value
        FROM observations
        WHERE series_id = ?
          AND date BETWEEN ? AND ?
          AND value IS NOT NULL
        ORDER BY value ASC
        LIMIT 1
        """,
        [series_id, window_start, window_end],
    ).fetchone()
    if not row:
        return None, None
    return row[0], float(row[1]) if row[1] is not None else None


def _get_frequency(con, series_id: str) -> str:
    cached = _FREQUENCY_CACHE.get(series_id)
    if cached is not None:
        return cached
    row = con.execute(
        "SELECT frequency FROM series WHERE series_id = ?", [series_id]
    ).fetchone()
    freq = (row[0] if row else "") or ""
    _FREQUENCY_CACHE[series_id] = freq
    return freq


def _shift_for_yoy(target_date: str, frequency: str) -> str:
    dt = _parse_date(target_date)
    if dt is None:
        return target_date
    if "day" in frequency.lower():
        return (dt - timedelta(days=365)).isoformat()
    shifted = _shift_months(dt, -12)
    return shifted.isoformat()


def _shift_for_mom(target_date: str, frequency: str) -> str:
    dt = _parse_date(target_date)
    if dt is None:
        return target_date
    freq = frequency.lower()
    if "quarter" in freq:
        shifted = _shift_months(dt, -3)
    elif "month" in freq:
        shifted = _shift_months(dt, -1)
    elif "day" in freq:
        shifted = dt - timedelta(days=1)
    else:
        shifted = _shift_months(dt, -1)
    return shifted.isoformat()


def _percent_change(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous in (None, 0):
        return None
    return (current - previous) / previous * 100.0


def _parse_date(value: str) -> Optional[date]:
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        return None


def _shift_months(dt: date, months: int) -> date:
    year = dt.year + (dt.month + months - 1) // 12
    month = (dt.month + months - 1) % 12 + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _normalize_window(start: Optional[str], end: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not start or not end:
        return None, None
    return (start, end) if start <= end else (end, start)
