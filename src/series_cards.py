"""Utilities for generating Markdown series cards."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Sequence


def render_series_card(
    series_meta: Mapping[str, Any],
    recent_observations: Sequence[Mapping[str, Any]],
    last_n: int = 12,
) -> str:
    """Return a Markdown series card with metadata, definition, and latest values."""

    title = series_meta.get("title") or "Unknown Series"
    series_id = series_meta.get("series_id") or series_meta.get("id") or "UNKNOWN"
    units = series_meta.get("units") or "Not specified"
    frequency = series_meta.get("frequency") or "Not specified"
    seasonal_adjustment = series_meta.get("seasonal_adjustment") or "Not specified"
    last_updated = series_meta.get("last_updated") or "unknown"

    definition = _build_definition(series_meta)
    trimmed = list(
        sorted(
            recent_observations,
            key=lambda row: row.get("date") or row.get("observation_date") or "",
            reverse=True,
        )
    )[:last_n]

    lines = [
        f"# {series_id}: {title}",
        "",
        f"- **Units:** {units}",
        f"- **Frequency:** {frequency}",
        f"- **Seasonal Adjustment:** {seasonal_adjustment}",
        f"- **Last Updated:** {last_updated}",
        "",
        "## Definition",
        definition,
        "",
        "## Recent observations",
    ]

    if not trimmed:
        lines.append("_No observations available._")
    else:
        lines.extend(_observations_table(trimmed))

    lines.extend(
        [
            "",
            f"Source: FRED series_id={series_id} (cached in mini-fred)",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def _observations_table(observations: Sequence[Mapping[str, Any]]) -> list[str]:
    rows = ["| Date | Value |", "| --- | --- |"]
    for obs in observations:
        date_str = _format_date(obs.get("date") or obs.get("observation_date"))
        value = _format_value(obs.get("value"))
        rows.append(f"| {date_str} | {value} |")
    return rows


def _build_definition(series_meta: Mapping[str, Any]) -> str:
    notes = (series_meta.get("notes") or "").strip()
    if notes:
        single_line = " ".join(line.strip() for line in notes.splitlines() if line.strip())
        return single_line or "This FRED series measures an economic indicator tracked by the St. Louis Fed."
    title = series_meta.get("title") or "this indicator"
    return f"This FRED series measures {title.lower()} as published by the St. Louis Fed."


def _format_date(value: Any) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, datetime):
        return value.date().isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:  # pragma: no cover
            pass
    return str(value)


def _format_value(value: Any) -> str:
    if value is None:
        return "N/A"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    if abs(numeric) >= 1000:
        return f"{numeric:,.2f}"
    if abs(numeric) >= 1:
        return f"{numeric:.2f}"
    return f"{numeric:.4f}"
