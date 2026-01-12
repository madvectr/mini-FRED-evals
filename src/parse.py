"""Lightweight question parser for the Mini-FRED answerer."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Tuple

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

SERIES_KEYWORDS = [
    ("unemployment rate", "UNRATE"),
    ("unemployment", "UNRATE"),
    ("jobless", "UNRATE"),
    ("cpi", "CPIAUCSL"),
    ("inflation", "CPIAUCSL"),
    ("consumer price", "CPIAUCSL"),
    ("fed funds", "FEDFUNDS"),
    ("federal funds", "FEDFUNDS"),
    ("interest rate", "FEDFUNDS"),
    ("pce inflation", "PCEPI"),
    ("pcepi", "PCEPI"),
    ("personal consumption", "PCEPI"),
    ("real gdp", "GDPC1"),
    ("gdp", "GDPC1"),
]

WINDOW_PATTERN = re.compile(
    r"(?:between|from)\s+(?P<start>[^,.;]+?)\s+(?:and|to)\s+(?P<end>[^,.;]+)",
    re.IGNORECASE,
)

ISO_DATE_PATTERN = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
ISO_MONTH_PATTERN = re.compile(r"\b\d{4}-\d{2}\b")

TRANSFORM_KEYWORDS = {
    "yoy": "yoy",
    "year over year": "yoy",
    "year-on-year": "yoy",
    "mom": "mom",
    "month over month": "mom",
    "month-on-month": "mom",
    "moving average": "ma",
    "ma": "ma",
    "highest": "max",
    "maximum": "max",
    "max": "max",
    "lowest": "min",
    "minimum": "min",
    "min": "min",
}


MISSING_SERIES_ERROR = (
    "Please specify which series to use (unemployment, CPI, fed funds, PCE, or GDP)."
)
DATE_REQUIRED_ERROR = "Please specify a date (e.g., April 2020 or 2020-04)."
WINDOW_REQUIRED_ERROR = (
    "Please provide a date window (e.g., between 2006-01 and 2008-12)."
)


@dataclass
class ParseResult:
    question: str
    series_id: Optional[str] = None
    transform: str = "point"
    date: Optional[str] = None
    window_start: Optional[str] = None
    window_end: Optional[str] = None
    periods: Optional[int] = None
    errors: List[str] = field(default_factory=list)
    missing_series: bool = False


def parse_question(question: str) -> ParseResult:
    """Parse a natural-language question into structured directives."""
    text = question.strip()
    lowered = text.lower()

    result = ParseResult(question=text)
    result.series_id = _detect_series(lowered)
    result.transform = _detect_transform(lowered)

    if result.transform in {"max", "min"}:
        start, end = _extract_window(text)
        result.window_start = start
        result.window_end = end
        if not start or not end:
            result.errors.append(WINDOW_REQUIRED_ERROR)
    else:
        detected_date = _extract_single_date(text)
        result.date = detected_date
        if not detected_date:
            result.errors.append(DATE_REQUIRED_ERROR)

    if result.transform == "ma":
        result.periods = _detect_ma_periods(lowered)

    if not result.series_id:
        result.missing_series = True
        result.errors.append(MISSING_SERIES_ERROR)

    return result


def _detect_series(lowered_question: str) -> Optional[str]:
    for keyword, series_id in SERIES_KEYWORDS:
        if keyword in lowered_question:
            return series_id
    return None


def _detect_transform(lowered_question: str) -> str:
    for keyword, transform in TRANSFORM_KEYWORDS.items():
        if keyword in lowered_question:
            return transform
    return "point"


def _extract_window(text: str) -> Tuple[Optional[str], Optional[str]]:
    match = WINDOW_PATTERN.search(text)
    if not match:
        return None, None
    start_raw = match.group("start")
    end_raw = match.group("end")
    start = _parse_date_token(start_raw)
    end = _parse_date_token(end_raw)
    return start, end


def _extract_single_date(text: str) -> Optional[str]:
    iso_match = ISO_DATE_PATTERN.search(text)
    if iso_match:
        return iso_match.group(0)
    month_match = ISO_MONTH_PATTERN.search(text)
    if month_match:
        token = f"{month_match.group(0)}-01"
        return token
    # Look for Month YYYY patterns
    tokens = re.findall(r"[A-Za-z]+\s+\d{4}", text)
    for token in tokens:
        parsed = _parse_date_token(token)
        if parsed:
            return parsed
    return None


def _parse_date_token(token: str) -> Optional[str]:
    clean = token.strip().strip(".,;:?!").replace(",", "")
    try:
        return datetime.fromisoformat(clean).date().isoformat()
    except ValueError:
        pass
    iso_month_match = re.fullmatch(r"\d{4}-\d{2}", clean)
    if iso_month_match:
        try:
            dt = datetime.strptime(clean, "%Y-%m")
            return dt.replace(day=1).date().isoformat()
        except ValueError:
            return None
    parts = clean.split()
    if len(parts) == 2:
        month = MONTHS.get(parts[0].lower())
        year = _safe_int(parts[1])
        if month and year:
            return f"{year:04d}-{month:02d}-01"
    return None


def _detect_ma_periods(lowered_question: str) -> int:
    match = re.search(r"(\d+)\s*(?:period|month|point)\s+(?:moving average|ma)", lowered_question)
    if match:
        return int(match.group(1))
    return 3


def _safe_int(value: str) -> Optional[int]:
    try:
        return int(value)
    except ValueError:
        return None
