"""MVES verifiers for Mini-FRED deterministic answerer."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import duckdb  # type: ignore[import]
except ModuleNotFoundError as exc:  # pragma: no cover
    raise ImportError(
        "duckdb is required to run MVES verifiers. Install it via `pip install duckdb`."
    ) from exc
import sys
try:
    import yaml  # type: ignore[import]
except ModuleNotFoundError:  # pragma: no cover
    yaml = None  # type: ignore[assignment]

ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import truth  # noqa: E402

SPEC_PATH = Path("mves/spec.yaml")
MAP_PATH = Path("mves/verifier_map.yaml")


@dataclass
class Failure:
    verifier_id: str
    severity: str
    message: str


def load_spec() -> Dict[str, Any]:
    text = SPEC_PATH.read_text(encoding="utf-8")
    if yaml is not None:
        return yaml.safe_load(text)
    return json.loads(text)


def load_verifier_map() -> List[Dict[str, Any]]:
    text = MAP_PATH.read_text(encoding="utf-8")
    if yaml is not None:
        data = yaml.safe_load(text)
    else:
        data = json.loads(text)
    return data.get("verifiers", [])


def verify_case(
    case: Dict[str, Any], response: Dict[str, Any], db_path: Path
) -> List[Failure]:
    spec = load_spec()
    verifier_map = load_verifier_map()
    failures: List[Failure] = []

    registry = {
        "schema_valid": lambda: _schema_valid(response, spec),
        "no_hallucination_on_error": lambda: _no_hallucination_on_error(response),
        "citations_present_when_value": lambda: _citations_present_when_value(response),
        "citations_match_series_id": lambda: _citations_match_series_id(response),
        "citations_subset_of_retrieved": lambda: _citations_subset_of_retrieved(response),
        "window_rules": lambda: _window_rules(response),
        "date_rules": lambda: _date_rules(case, response),
        "confidence_rules": lambda: _confidence_rules(response),
        "no_urls_in_answer": lambda: _no_urls_in_answer(response),
        "value_display_in_answer": lambda: _value_display_in_answer(response),
        "expectation_transform": lambda: _expectation_transform(case, response),
        "expectation_value_presence": lambda: _expectation_value_presence(case, response),
        "truth_matches": lambda: _truth_matches(case, response, db_path),
    }

    for entry in verifier_map:
        if not entry.get("enabled", True):
            continue
        if not _entry_applicable(entry, case):
            continue
        verifier_id = entry["id"]
        severity = entry["severity"]
        fn = registry.get(verifier_id)
        if not fn:
            continue
        messages = fn()
        for message in messages:
            failures.append(Failure(verifier_id, severity, message))

    return failures


def _schema_valid(response: Dict[str, Any], spec: Dict[str, Any]) -> List[str]:
    messages: List[str] = []
    required = [
        "question",
        "series_id",
        "transform",
        "date",
        "window",
        "value",
        "unit",
        "answer",
        "citations",
        "confidence",
        "errors",
        "retrieved_docs",
    ]
    for key in required:
        if key not in response:
            messages.append(f"Missing required field '{key}'.")
    window = response.get("window")
    if not isinstance(window, dict):
        messages.append("window must be an object.")
    else:
        for key in ("start", "end", "periods"):
            if key not in window:
                messages.append(f"window missing '{key}'.")
    if response.get("transform") not in spec.get("transforms", []):
        messages.append(f"Unsupported transform '{response.get('transform')}'.")
    if not isinstance(response.get("citations"), list):
        messages.append("citations must be a list.")
    if not isinstance(response.get("retrieved_docs"), list):
        messages.append("retrieved_docs must be a list.")
    if not isinstance(response.get("errors"), list):
        messages.append("errors must be a list.")
    if response.get("value") is not None and response.get("value_display") in (None, ""):
        messages.append("value_display missing while value provided.")
    return messages


def _no_hallucination_on_error(response: Dict[str, Any]) -> List[str]:
    messages: List[str] = []
    errors = response.get("errors") or []
    if errors:
        if response.get("value") is not None:
            messages.append("Value present despite errors.")
        if response.get("citations"):
            messages.append("Citations present despite errors.")
        if response.get("confidence", 1) > 0.4:
            messages.append("Confidence too high when errors are present.")
    return messages


def _citations_present_when_value(response: Dict[str, Any]) -> List[str]:
    messages: List[str] = []
    if response.get("value") is not None:
        if response.get("series_id") in (None, ""):
            messages.append("series_id missing while value provided.")
        if not response.get("citations"):
            messages.append("Citations missing while value provided.")
        if response.get("confidence", 0) < 0.7:
            messages.append("Confidence too low for answered case.")
        if not response.get("value_display"):
            messages.append("value_display missing while value provided.")
    return messages


def _citations_match_series_id(response: Dict[str, Any]) -> List[str]:
    messages: List[str] = []
    series_id = response.get("series_id")
    citations = response.get("citations") or []
    if series_id:
        expected = f"series_{series_id}"
        for citation in citations:
            if citation.get("doc_id") != expected:
                messages.append(
                    f"Citation doc_id {citation.get('doc_id')} does not match {expected}."
                )
    return messages


def _citations_subset_of_retrieved(response: Dict[str, Any]) -> List[str]:
    messages: List[str] = []
    retrieved = {doc.get("doc_id") for doc in response.get("retrieved_docs", [])}
    for citation in response.get("citations", []):
        doc_id = citation.get("doc_id")
        if doc_id and doc_id not in retrieved:
            messages.append(f"Citation doc_id {doc_id} missing from retrieved_docs.")
    return messages


def _window_rules(response: Dict[str, Any]) -> List[str]:
    messages: List[str] = []
    transform = response.get("transform")
    if transform in {"max", "min"}:
        window = response.get("window") or {}
        if not window.get("start"):
            messages.append("window.start missing for max/min question.")
        if not window.get("end"):
            messages.append("window.end missing for max/min question.")
    return messages


def _date_rules(case: Dict[str, Any], response: Dict[str, Any]) -> List[str]:
    messages: List[str] = []
    expect = case.get("expect", {})
    if not expect.get("should_have_value", True):
        return messages
    transform = response.get("transform")
    if transform in {"point", "yoy", "mom", "ma"}:
        if not response.get("date"):
            messages.append("date missing for point/yoy/mom/ma transform.")
    return messages


def _confidence_rules(response: Dict[str, Any]) -> List[str]:
    messages: List[str] = []
    value = response.get("value")
    errors = response.get("errors") or []
    confidence = response.get("confidence", 0)
    if value is None and not errors and confidence > 0.8:
        messages.append("Confidence too high with no value and no errors.")
    return messages


def _no_urls_in_answer(response: Dict[str, Any]) -> List[str]:
    answer = (response.get("answer") or "").lower()
    if "http" in answer:
        return ["Answer contains HTTP/URL content."]
    return []


def _value_display_in_answer(response: Dict[str, Any]) -> List[str]:
    value = response.get("value")
    if value is None:
        return []
    value_display = response.get("value_display")
    answer = response.get("answer") or ""
    if not value_display:
        return ["value_display missing while value provided."]
    if value_display not in answer:
        return [f"value_display '{value_display}' not found in answer text."]
    return []


def _expectation_transform(case: Dict[str, Any], response: Dict[str, Any]) -> List[str]:
    expect = case.get("expect", {})
    messages: List[str] = []
    expected_series = expect.get("series_id")
    if expected_series and response.get("series_id") != expected_series:
        messages.append(
            f"Expected series {expected_series} but got {response.get('series_id')}."
        )
    expected_transform = expect.get("transform")
    if expected_transform and response.get("transform") != expected_transform:
        messages.append(
            f"Expected transform {expected_transform} but got {response.get('transform')}."
        )
    return messages


def _expectation_value_presence(
    case: Dict[str, Any], response: Dict[str, Any]
) -> List[str]:
    expect = case.get("expect", {})
    should_answer = expect.get("should_answer", True)
    should_have_value = expect.get("should_have_value", True)
    require_citation = expect.get("require_citation", False)
    require_retrieved = expect.get("require_retrieved_citation", False)

    messages: List[str] = []
    value = response.get("value")

    if should_have_value and value is None:
        messages.append("Expected a numeric value but got null.")
    if should_have_value and not case.get("truth_spec"):
        messages.append("truth_spec missing for a numeric expectation.")
    if not should_have_value and value is not None:
        messages.append("Value present though should_have_value=false.")

    if not should_answer and not response.get("errors"):
        messages.append("Expected refusal/clarification, but errors list is empty.")

    if require_citation and not response.get("citations"):
        messages.append("Expected citations but none were returned.")

    if require_retrieved and response.get("citations"):
        retrieved = {doc.get("doc_id") for doc in response.get("retrieved_docs", [])}
        for citation in response.get("citations", []):
            doc_id = citation.get("doc_id")
            if doc_id not in retrieved:
                messages.append(
                    f"Citation {doc_id} missing from retrieved docs for required match."
                )

    return messages


def _truth_matches(
    case: Dict[str, Any], response: Dict[str, Any], db_path: Path
) -> List[str]:
    messages: List[str] = []
    expect = case.get("expect", {})
    truth_spec = case.get("truth_spec")
    if not expect.get("should_have_value", True):
        return messages
    if not truth_spec:
        return messages

    expected_value = _compute_truth(db_path, truth_spec)
    if expected_value is None:
        messages.append("Unable to compute truth value for truth_spec.")
        return messages

    actual_value = response.get("value")
    if actual_value is None:
        messages.append("Response missing value despite truth_spec.")
        return messages

    tolerance = truth_spec.get("tolerance", 1e-6)
    if abs(float(actual_value) - expected_value) > tolerance:
        messages.append(
            f"Value {actual_value} differs from truth {expected_value} (tol={tolerance})."
        )
    return messages


def _compute_truth(db_path: Path, spec: Dict[str, Any]) -> Optional[float]:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        series_id = spec["series_id"]
        transform = spec["transform"]
        if transform == "point":
            return truth.get_point(conn, series_id, spec["date"])
        if transform == "yoy":
            return truth.get_yoy(conn, series_id, spec["date"])
        if transform == "mom":
            return truth.get_mom(conn, series_id, spec["date"])
        if transform == "ma":
            return truth.get_ma(conn, series_id, spec["date"], spec["periods"])
        if transform == "max":
            _, value = truth.get_max(conn, series_id, spec["window"]["start"], spec["window"]["end"])
            return value
        if transform == "min":
            _, value = truth.get_min(conn, series_id, spec["window"]["start"], spec["window"]["end"])
            return value
        return None
    finally:
        conn.close()


def _entry_applicable(entry: Dict[str, Any], case: Dict[str, Any]) -> bool:
    expect = case.get("expect", {})
    if entry.get("require_expect_should_answer") and not expect.get("should_answer", True):
        return False
    if entry.get("require_expect_should_have_value") and not expect.get("should_have_value", True):
        return False
    return True


def dump_failures_json(failures: List[Failure]) -> str:
    return json.dumps([failure.__dict__ for failure in failures], indent=2)
