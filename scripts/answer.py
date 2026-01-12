#!/usr/bin/env python3
"""Deterministic Mini-FRED answerer with DuckDB truth + series card context."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import warehouse  # noqa: E402
from src.parse import parse_question  # noqa: E402
from src.truth import (  # noqa: E402
    get_ma,
    get_max,
    get_min,
    get_mom,
    get_point,
    get_yoy,
)
from src.util import load_series_config, resolve_project_root  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Answer Mini-FRED questions deterministically.")
    parser.add_argument("question", help="Natural language question.")
    parser.add_argument(
        "--config",
        default="config/series.yaml",
        help="Path to series config (default: config/series.yaml).",
    )
    parser.add_argument(
        "--db",
        default="data/warehouse.duckdb",
        help="Path to DuckDB warehouse (default: data/warehouse.duckdb).",
    )
    parser.add_argument(
        "--cards-dir",
        default="corpus/series_cards",
        help="Directory containing generated series cards.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = resolve_project_root()
    config_path = _resolve_path(project_root, args.config)
    db_path = _resolve_path(project_root, args.db)
    cards_dir = _resolve_path(project_root, args.cards_dir)

    _ensure_cards_dir(cards_dir)
    load_series_config(config_path)  # Ensures config exists; no direct use yet.

    con = warehouse.get_connection(db_path)

    parsed = parse_question(args.question)
    response = _build_response(parsed, con, cards_dir)
    print(json.dumps(response, indent=2))


def _build_response(parsed, con, cards_dir: Path) -> Dict[str, object]:
    base = {
        "question": parsed.question,
        "series_id": parsed.series_id,
        "transform": parsed.transform,
        "date": parsed.date if parsed.transform in {"point", "yoy", "mom", "ma"} else None,
        "window": {
            "start": parsed.window_start if parsed.transform in {"max", "min"} else None,
            "end": parsed.window_end if parsed.transform in {"max", "min"} else None,
            "periods": parsed.periods if parsed.transform == "ma" else None,
        },
        "value": None,
        "unit": None,
        "answer": "",
        "citations": [],
        "confidence": 0.2,
        "errors": list(parsed.errors),
    }

    if parsed.errors:
        base["answer"] = " ".join(parsed.errors)
        return base

    metadata = _fetch_metadata(con, parsed.series_id)
    if not metadata:
        base["errors"].append(f"Series {parsed.series_id} not found in warehouse.")
        base["answer"] = base["errors"][0]
        return base
    base["unit"] = metadata.get("units")

    card_path = cards_dir / f"series_{parsed.series_id}.md"
    if not card_path.exists():
        base["errors"].append(
            f"Series card {card_path.name} not found. Run scripts/build_series_cards.py first."
        )
        base["answer"] = base["errors"][0]
        return base

    value, citation_dates, answer_text = _compute_answer(parsed, con, metadata)

    if value is None or not answer_text:
        if not base["errors"]:
            base["errors"].append("Unable to compute the requested value with available data.")
        base["answer"] = base["errors"][0]
        base["confidence"] = 0.25
        return base

    base["value"] = value
    base["answer"] = answer_text
    base["citations"] = [
        {
            "doc_id": f"series_{parsed.series_id}",
            "series_id": parsed.series_id,
            "dates": citation_dates,
            "source": "mini-fred",
        }
    ]
    base["confidence"] = 0.95
    base["errors"] = []
    return base


def _compute_answer(parsed, con, metadata: Dict[str, object]):
    series_id = parsed.series_id
    units = metadata.get("units")
    title = metadata.get("title") or series_id
    transform = parsed.transform
    citation_dates: List[str] = []
    value: Optional[float] = None
    answer_text: Optional[str] = None

    if transform == "point":
        value = get_point(con, series_id, parsed.date)
        citation_dates = [parsed.date] if parsed.date else []
        answer_text = _format_point_answer(title, value, parsed.date, units)
    elif transform == "yoy":
        value = get_yoy(con, series_id, parsed.date)
        citation_dates = [parsed.date] if parsed.date else []
        answer_text = _format_percent_answer(title, value, parsed.date, units, "year-over-year change")
    elif transform == "mom":
        value = get_mom(con, series_id, parsed.date)
        citation_dates = [parsed.date] if parsed.date else []
        answer_text = _format_percent_answer(title, value, parsed.date, units, "month-over-month change")
    elif transform == "ma":
        periods = parsed.periods or 3
        value = get_ma(con, series_id, parsed.date, periods)
        citation_dates = [parsed.date] if parsed.date else []
        answer_text = _format_ma_answer(title, value, parsed.date, units, periods)
    elif transform == "max":
        max_date, value = get_max(con, series_id, parsed.window_start, parsed.window_end)
        citation_dates = [max_date] if max_date else []
        answer_text = _format_extreme_answer(
            title,
            value,
            max_date,
            parsed.window_start,
            parsed.window_end,
            units,
            "maximum",
        )
    elif transform == "min":
        min_date, value = get_min(con, series_id, parsed.window_start, parsed.window_end)
        citation_dates = [min_date] if min_date else []
        answer_text = _format_extreme_answer(
            title,
            value,
            min_date,
            parsed.window_start,
            parsed.window_end,
            units,
            "minimum",
        )

    if value is not None:
        value = float(value)

    if value is None or not answer_text:
        return None, citation_dates, None

    citation_dates = [d for d in citation_dates if d]
    return value, citation_dates, answer_text


def _format_point_answer(title, value, date_str, units):
    if value is None or not date_str:
        return None
    value_text = _format_value(value, units, percent_hint=False)
    human_date = _humanize_date(date_str)
    unit_text = f" {units}" if units and "percent" not in units.lower() else ""
    return f"In {human_date}, {title} was {value_text}{unit_text}."


def _format_percent_answer(title, value, date_str, units, label):
    if value is None or not date_str:
        return None
    value_text = _format_value(value, units, percent_hint=True)
    human_date = _humanize_date(date_str)
    return f"In {human_date}, the {label} for {title} was {value_text}."


def _format_ma_answer(title, value, date_str, units, periods):
    if value is None or not date_str:
        return None
    value_text = _format_value(value, units, percent_hint=False)
    human_date = _humanize_date(date_str)
    unit_text = f" {units}" if units and "percent" not in units.lower() else ""
    return (
        f"The {periods}-period moving average of {title} on {human_date} was "
        f"{value_text}{unit_text}."
    )


def _format_extreme_answer(title, value, extreme_date, start, end, units, adjective):
    if value is None or not extreme_date or not start or not end:
        return None
    value_text = _format_value(value, units, percent_hint=False)
    human_date = _humanize_date(extreme_date)
    unit_text = f" {units}" if units and "percent" not in units.lower() else ""
    return (
        f"The {adjective} value of {title} between {start} and {end} was "
        f"{value_text}{unit_text} on {human_date}."
    )


def _format_value(value: float, units: Optional[str], percent_hint: bool) -> str:
    if percent_hint or (units and "percent" in units.lower()):
        return f"{value:.2f}%"
    if abs(value) >= 1000:
        return f"{value:,.2f}"
    if abs(value) >= 1:
        return f"{value:.2f}"
    return f"{value:.4f}"


def _humanize_date(date_str: str) -> str:
    try:
        dt = datetime.fromisoformat(date_str)
    except ValueError:
        return date_str
    if dt.day == 1:
        return dt.strftime("%B %Y")
    return dt.strftime("%B %d, %Y").replace(" 0", " ")


def _resolve_path(project_root: Path, raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def _ensure_cards_dir(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(
            f"Series card directory {path} not found. Run scripts/build_series_cards.py first."
        )


def _fetch_metadata(con, series_id: Optional[str]) -> Optional[Dict[str, object]]:
    if not series_id:
        return None
    row = con.execute(
        """
        SELECT series_id, title, units
        FROM series
        WHERE series_id = ?
        """,
        [series_id],
    ).fetchone()
    if not row:
        return None
    return {"series_id": row[0], "title": row[1], "units": row[2]}


if __name__ == "__main__":
    main()
