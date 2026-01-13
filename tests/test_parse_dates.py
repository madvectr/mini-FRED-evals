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


def test_parse_month_abbreviation_without_period():
    _assert_date("What was CPI in Jun 2014?", "2014-06-01")


def test_detect_yoy_synonyms():
    result = parse_question("What was the YoY change in CPI in 2014-06?")
    assert result.transform == "yoy"
    result = parse_question("Provide the annual change for CPI in June 2014.")
    assert result.transform == "yoy"
    result = parse_question("Give me the year-over-year change for CPI in June 2014.")
    assert result.transform == "yoy"


def test_detect_mom_synonyms():
    result = parse_question("What was the MoM change in CPI in June 2014?")
    assert result.transform == "mom"
    result = parse_question("Report the monthly percent change for CPI in June 2014.")
    assert result.transform == "mom"
    result = parse_question("Give me the monthly percentage change for CPI in June 2014.")
    assert result.transform == "mom"


def test_detect_max_min_synonyms():
    result = parse_question("How high did CPI get between January 2010 and March 2012?")
    assert result.transform == "max"
    result = parse_question("How low did CPI get between January 2010 and March 2012?")
    assert result.transform == "min"
