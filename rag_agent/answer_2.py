"""Improved Mini-FRED RAG agent with refined parsing heuristics."""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import warehouse  # noqa: E402
from src.parse import MISSING_SERIES_ERROR, parse_question  # noqa: E402
from src.retriever import TfidfRetriever  # noqa: E402
from src.truth import (  # noqa: E402
    get_ma,
    get_max,
    get_min,
    get_mom,
    get_point,
    get_yoy,
)
from src.util import load_series_config  # noqa: E402

MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def run(
    question: str,
    *,
    config_path: Path,
    db_path: Path,
    cards_dir: Path,
) -> Dict[str, object]:
    _ensure_cards_dir(cards_dir)
    load_series_config(config_path)

    retriever = TfidfRetriever(cards_dir)
    retriever.build()

    retrieved = retriever.retrieve(question, k=3)
    retrieved_summary = [
        {"doc_id": doc["doc_id"], "score": round(doc["score"], 4)} for doc in retrieved
    ]

    parsed = parse_question(question)
    parsed = _refine_parsed_result(parsed, question)
    parsed = _maybe_infer_series_from_retrieval(parsed, retrieved)

    con = warehouse.get_connection(db_path)

    if parsed.missing_series and (parsed.series_id is None):
        return _clarifying_response(parsed, retrieved_summary)

    return _build_response(parsed, con, cards_dir, retrieved_summary)


def _refine_parsed_result(parsed, question: str):
    normalized = question.lower()
    detected_transform = _detect_transform(normalized)
    if detected_transform:
        parsed.transform = detected_transform

    if parsed.transform in {"max", "min"}:
        window = _extract_window(question)
        if window[0]:
            parsed.window_start = window[0]
        if window[1]:
            parsed.window_end = window[1]

    if parsed.transform in {"point", "yoy", "mom", "ma"} and not parsed.date:
        parsed.date = _extract_single_date(question)

    if parsed.transform == "ma" and not parsed.periods:
        periods = _extract_periods(normalized)
        if periods:
            parsed.periods = periods

    return parsed


def _detect_transform(text: str) -> Optional[str]:
    if "between" in text or ("from" in text and "to" in text):
        if any(keyword in text for keyword in ["highest", "maximum", "max"]):
            return "max"
        if any(keyword in text for keyword in ["lowest", "minimum", "min"]):
            return "min"
    if any(phrase in text for phrase in ["year-over-year", "year over year", "yoy"]):
        return "yoy"
    if any(phrase in text for phrase in ["month-over-month", "month over month", "mom"]):
        return "mom"
    if "moving average" in text or re.search(r"\bma\b", text):
        return "ma"
    return None


def _extract_periods(text: str) -> Optional[int]:
    match = re.search(
        r"(\d+)\s*[- ]?\s*(?:periods?|months?)\s+(?:moving average|ma)",
        text,
    )
    if match:
        return int(match.group(1))
    return None


def _extract_window(question: str) -> Sequence[Optional[str]]:
    pattern = re.compile(
        r"(?:between|from)\s+(?P<start>[A-Za-z0-9\s\-/]+?)\s+(?:and|to)\s+(?P<end>[A-Za-z0-9\s\-/]+)",
        re.IGNORECASE,
    )
    match = pattern.search(question)
    if not match:
        return (None, None)
    start = _parse_date_token(match.group("start"))
    end = _parse_date_token(match.group("end"))
    return (start, end)


def _extract_single_date(question: str) -> Optional[str]:
    iso_date = re.search(r"\b\d{4}-\d{2}-\d{2}\b", question)
    if iso_date:
        return iso_date.group(0)
    iso_month = re.search(r"\b\d{4}-\d{2}\b", question)
    if iso_month:
        return f"{iso_month.group(0)}-01"
    month_year = re.search(r"\b([A-Za-z]+)\s+(\d{4})\b", question)
    if month_year:
        token = month_year.group(0)
        parsed = _parse_date_token(token)
        if parsed:
            return parsed
    return None


def _parse_date_token(token: str) -> Optional[str]:
    clean = token.strip().strip(".,;:?!")
    try:
        return datetime.fromisoformat(clean).date().isoformat()
    except ValueError:
        pass
    if re.fullmatch(r"\d{4}-\d{2}", clean):
        return f"{clean}-01"
    parts = clean.split()
    if len(parts) == 2:
        month = MONTHS.get(parts[0].lower())
        year = _safe_int(parts[1])
        if month and year:
            return f"{year:04d}-{month:02d}-01"
    return None


def _safe_int(value: str) -> Optional[int]:
    try:
        return int(value)
    except ValueError:
        return None


def _build_response(
    parsed,
    con,
    cards_dir: Path,
    retrieved_docs: List[Dict[str, object]],
) -> Dict[str, object]:
    base = {
        "question": parsed.question,
        "series_id": parsed.series_id,
        "transform": parsed.transform,
        "date": _date_to_str(parsed.date) if parsed.transform in {"point", "yoy", "mom", "ma"} else None,
        "window": {
            "start": _date_to_str(parsed.window_start) if parsed.transform in {"max", "min"} else None,
            "end": _date_to_str(parsed.window_end) if parsed.transform in {"max", "min"} else None,
            "periods": parsed.periods if parsed.transform == "ma" else None,
        },
        "value": None,
        "unit": None,
        "answer": "",
        "citations": [],
        "confidence": 0.2,
        "errors": list(parsed.errors),
        "retrieved_docs": retrieved_docs,
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

    snippet = _definition_snippet(card_path)
    if snippet:
        answer_text = f"{answer_text} {snippet}"

    primary_doc_id = f"series_{parsed.series_id}"
    _ensure_doc_in_retrieved(base["retrieved_docs"], primary_doc_id)

    base["value"] = value
    base["answer"] = answer_text
    citation_dates = [_date_to_str(d) for d in citation_dates]
    base["citations"] = [
        {
            "doc_id": primary_doc_id,
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
    if date_str is None:
        return "N/A"
    if isinstance(date_str, datetime):
        dt = date_str
    elif isinstance(date_str, date):
        dt = datetime.combine(date_str, datetime.min.time())
    else:
        try:
            dt = datetime.fromisoformat(str(date_str))
        except ValueError:
            return str(date_str)
    if dt.day == 1:
        return dt.strftime("%B %Y")
    return dt.strftime("%B %d, %Y").replace(" 0", " ")


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


def _maybe_infer_series_from_retrieval(parsed, retrieved_docs):
    if parsed.series_id:
        return parsed
    if not parsed.missing_series:
        return parsed
    other_errors = [err for err in parsed.errors if err != MISSING_SERIES_ERROR]
    if other_errors:
        return parsed
    if not retrieved_docs:
        return parsed
    top = retrieved_docs[0]
    if top["score"] >= 0.25:
        parsed.series_id = _series_id_from_doc(top["doc_id"])
        parsed.missing_series = False
        parsed.errors = other_errors
    return parsed


def _clarifying_response(parsed, retrieved_docs: List[Dict[str, object]]) -> Dict[str, object]:
    answer_text = " ".join(parsed.errors) if parsed.errors else MISSING_SERIES_ERROR
    return {
        "question": parsed.question,
        "series_id": None,
        "transform": parsed.transform,
        "date": _date_to_str(parsed.date) if parsed.transform in {"point", "yoy", "mom", "ma"} else None,
        "window": {
            "start": _date_to_str(parsed.window_start) if parsed.transform in {"max", "min"} else None,
            "end": _date_to_str(parsed.window_end) if parsed.transform in {"max", "min"} else None,
            "periods": parsed.periods if parsed.transform == "ma" else None,
        },
        "value": None,
        "unit": None,
        "answer": answer_text,
        "citations": [],
        "confidence": 0.2,
        "errors": list(parsed.errors),
        "retrieved_docs": retrieved_docs,
    }


def _definition_snippet(card_path: Path, max_chars: int = 200) -> Optional[str]:
    text = card_path.read_text(encoding="utf-8")
    marker = "## Definition"
    if marker not in text:
        return None
    after = text.split(marker, 1)[1]
    if "## " in after:
        after = after.split("## ", 1)[0]
    cleaned = " ".join(line.strip() for line in after.splitlines() if line.strip())
    cleaned = re.sub(r"https?://\S+", "", cleaned)
    if not cleaned:
        return None
    sentences = re.split(r"(?<=[.!?])\s+", cleaned)
    sentence = ""
    for candidate in sentences:
        candidate = candidate.strip()
        if not candidate:
            continue
        if len(candidate.split()) < 6:
            continue
        if re.search(r"\b\d{4}\s*-\s*\d{4}\b", candidate):
            continue
        sentence = candidate
        break
    if not sentence:
        return None
    if not sentence.endswith("."):
        sentence = f"{sentence}."
    if len(sentence) > max_chars:
        sentence = sentence[: max_chars].rstrip() + "…"
    return sentence


def _ensure_doc_in_retrieved(retrieved_docs: List[Dict[str, object]], doc_id: str) -> None:
    doc_ids = {doc["doc_id"] for doc in retrieved_docs}
    if doc_id not in doc_ids:
        retrieved_docs.insert(0, {"doc_id": doc_id, "score": 1.0})


def _series_id_from_doc(doc_id: str) -> str:
    if doc_id.startswith("series_"):
        return doc_id[len("series_") :].upper()
    return doc_id.upper()


def _date_to_str(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)
