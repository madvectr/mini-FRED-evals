from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.parse import parse_question


def _assert_date(question: str, expected: str) -> None:
    result = parse_question(question)
    assert result.date == expected


def test_parse_month_abbreviation_with_period():
    _assert_date("What was CPI in Aug. 2018?", "2018-08-01")


def test_parse_month_with_comma():
    _assert_date("What was CPI in August, 2018?", "2018-08-01")


def test_parse_numeric_with_slash():
    _assert_date("What was CPI in 2018/08?", "2018-08-01")


def test_parse_month_with_hyphen():
    _assert_date("What was CPI in Aug-2018?", "2018-08-01")
