"""Utilities for generating Markdown series cards."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Sequence


def render_series_card(
    series_meta: Mapping[str, Any],
    recent_observations: Sequence[Mapping[str, Any]],
    last_n: int = 6,
) -> str:
    """Return a Markdown summary for a FRED series."""
    title = series_meta.get("title") or series_meta.get("series_id", "Unknown Series")
    series_id = series_meta.get("series_id", "UNKNOWN")
    units = series_meta.get("units", "Not specified")
    frequency = series_meta.get("frequency", "Not specified")
    notes = series_meta.get("notes") or "No notes available."

    sorted_obs = sorted(
        recent_observations,
        key=lambda row: row.get("observation_date") or "",
        reverse=True,
    )
    trimmed = sorted_obs[:last_n]

    lines = [
        f"# {title} (`{series_id}`)",
        "",
        f"- **Units:** {units}",
        f"- **Frequency:** {frequency}",
        f"- **Last Updated:** {series_meta.get('last_updated', 'unknown')}",
        "",
        "## Recent observations",
    ]

    if not trimmed:
        lines.append("_No observations loaded yet._")
    else:
        for obs in trimmed:
            date_str = obs.get("observation_date", "unknown")
            value = obs.get("value", "n/a")
            status = obs.get("status")
            formatted_date = _format_date(date_str)
            status_suffix = f" ({status})" if status else ""
            lines.append(f"- {formatted_date}: **{value}**{status_suffix}")

    lines.extend(["", "## Notes", notes])
    return "\n".join(lines).strip() + "\n"


def _format_date(date_value: Any) -> str:
    """Format an observation date into YYYY-MM-DD."""
    if isinstance(date_value, datetime):
        return date_value.date().isoformat()
    if hasattr(date_value, "isoformat"):
        try:
            return date_value.isoformat()
        except Exception:  # pragma: no cover - best effort formatting
            pass
    return str(date_value)
