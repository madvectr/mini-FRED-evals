#!/usr/bin/env python3
"""Promptfoo assertion for Mini-FRED extended evals."""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb  # type: ignore[import]

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import truth  # noqa: E402

DB_PATH = ROOT / "data" / "warehouse.duckdb"


def get_assert(output: Any, context: Any) -> Dict[str, Any]:
    """Entry point used by promptfoo's python assertion loader."""
    return _evaluate_assertion(output, context)


def main() -> None:
    """Allow running as a standalone script for local debugging."""
    if len(sys.argv) < 3:
        print(
            json.dumps(
                _build_result(False, "Assertion invoked without the expected promptfoo arguments.")
            )
        )
        return

    output = sys.argv[1]
    context = sys.argv[2]
    print(json.dumps(_evaluate_assertion(output, context)))


def _evaluate_assertion(raw_output: Any, raw_context: Any) -> Dict[str, Any]:
    response, error = _coerce_json_object(raw_output, "Agent output")
    if error:
        return _build_result(False, error)

    context, ctx_error = _coerce_json_object(raw_context, "Promptfoo context payload")
    if ctx_error:
        return _build_result(False, ctx_error)

    vars_payload = context.get("vars") or {}
    expect = vars_payload.get("expect")
    truth_spec = vars_payload.get("truth_spec")

    if not isinstance(expect, dict):
        return _build_result(False, "Test case vars.expect is missing or not an object.")

    failures: List[str] = []
    failures.extend(_validate_schema(response))

    should_have_value = bool(expect.get("should_have_value"))

    if should_have_value:
        failures.extend(_validate_answer_payload(response, expect))
        if isinstance(truth_spec, dict):
            truth_error = _validate_truth(response, truth_spec)
            if truth_error:
                failures.append(truth_error)
    else:
        failures.extend(_validate_refusal_payload(response))

    passed = not failures
    reason = "All checks passed." if passed else "; ".join(failures)
    return _build_result(passed, reason)


def _coerce_json_object(payload: Any, label: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    if isinstance(payload, dict):
        return payload, None
    if payload is None:
        return None, f"{label} was empty."
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8")
    if isinstance(payload, str):
        text = payload.strip()
        if not text:
            return None, f"{label} was empty."
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            return None, f"{label} was not valid JSON: {exc}"
        if not isinstance(parsed, dict):
            return None, f"{label} must decode to a JSON object."
        return parsed, None
    return None, f"{label} must be a JSON object or string."


def _validate_schema(response: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    required_fields = [
        "question",
        "series_id",
        "transform",
        "date",
        "window",
        "value",
        "value_display",
        "unit",
        "answer",
        "citations",
        "confidence",
        "errors",
        "retrieved_docs",
    ]
    for field in required_fields:
        if field not in response:
            errors.append(f"Missing required field '{field}'.")

    question = response.get("question")
    if not isinstance(question, str) or not question.strip():
        errors.append("Field 'question' must be a non-empty string.")

    answer = response.get("answer")
    if not isinstance(answer, str) or not answer.strip():
        errors.append("Field 'answer' must be a non-empty string.")

    transform = response.get("transform")
    if transform is not None and not isinstance(transform, str):
        errors.append("Field 'transform' must be a string when present.")

    series_id = response.get("series_id")
    if series_id is not None and not isinstance(series_id, str):
        errors.append("Field 'series_id' must be a string or null.")

    date = response.get("date")
    if date is not None and not isinstance(date, str):
        errors.append("Field 'date' must be a string or null.")

    window = response.get("window")
    if not isinstance(window, dict):
        errors.append("Field 'window' must be an object.")
    else:
        for key in ("start", "end"):
            value = window.get(key)
            if value is not None and not isinstance(value, str):
                errors.append(f"Window field '{key}' must be a string or null.")
        periods = window.get("periods")
        if periods is not None and not isinstance(periods, int):
            errors.append("Window field 'periods' must be an integer or null.")

    value = response.get("value")
    if value is not None and not _is_number(value):
        errors.append("Field 'value' must be numeric or null.")

    value_display = response.get("value_display")
    if value_display is not None and not isinstance(value_display, str):
        errors.append("Field 'value_display' must be a string or null.")

    unit = response.get("unit")
    if unit is not None and not isinstance(unit, str):
        errors.append("Field 'unit' must be a string or null.")

    citations = response.get("citations")
    if not isinstance(citations, list):
        errors.append("Field 'citations' must be a list.")
    else:
        for citation in citations:
            if not isinstance(citation, dict):
                errors.append("Each citation must be an object.")
                continue
            doc_id = citation.get("doc_id")
            if doc_id is not None and not isinstance(doc_id, str):
                errors.append("Citation doc_id must be a string.")
            dates = citation.get("dates")
            if dates is not None and not isinstance(dates, list):
                errors.append("Citation dates must be a list when present.")

    retrieved = response.get("retrieved_docs")
    if not isinstance(retrieved, list):
        errors.append("Field 'retrieved_docs' must be a list.")
    else:
        for doc in retrieved:
            if not isinstance(doc, dict):
                errors.append("Each retrieved doc must be an object.")
                continue
            doc_id = doc.get("doc_id")
            if doc_id is not None and not isinstance(doc_id, str):
                errors.append("Retrieved doc_id must be a string when present.")

    errors_field = response.get("errors")
    if not isinstance(errors_field, list):
        errors.append("Field 'errors' must be a list.")
    else:
        for item in errors_field:
            if not isinstance(item, str):
                errors.append("Each entry in 'errors' must be a string.")

    confidence = response.get("confidence")
    if confidence is None or not _is_number(confidence):
        errors.append("Field 'confidence' must be a numeric value.")
    else:
        if confidence < 0 or confidence > 1:
            errors.append("Confidence must be between 0 and 1.")

    return errors


def _validate_refusal_payload(response: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if response.get("value") is not None:
        errors.append("Refusal responses must not include a numeric value.")
    citations = response.get("citations") or []
    if citations:
        errors.append("Refusal responses must not include citations.")
    confidence = response.get("confidence")
    if confidence is None or confidence > 0.4:
        errors.append("Refusal confidence must be <= 0.4.")
    answer = response.get("answer") or ""
    if not answer.strip():
        errors.append("Refusal responses must include a clarifying answer.")
    elif not _looks_like_clarification(answer):
        errors.append("Refusal answer must request the missing information.")
    return errors


def _validate_answer_payload(response: Dict[str, Any], expect: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    expected_transform = expect.get("transform")
    expected_series = expect.get("series_id")
    transform = response.get("transform")
    series_id = response.get("series_id")

    if transform != expected_transform:
        errors.append(
            f"Transform mismatch: expected '{expected_transform}', got '{transform}'."
        )
    if series_id != expected_series:
        errors.append(
            f"Series mismatch: expected '{expected_series}', got '{series_id}'."
        )

    value = response.get("value")
    if value is None:
        errors.append("Answerable responses must include a numeric value.")
    elif not _is_number(value):
        errors.append("Value must be numeric.")

    if response.get("value_display") is None:
        errors.append("value_display must be populated when a value is returned.")
    else:
        display = str(response["value_display"])
        if display not in (response.get("answer") or ""):
            errors.append("value_display must appear verbatim in the answer text.")

    citations = response.get("citations") or []
    if not citations:
        errors.append("Answerable responses must include at least one citation.")
    else:
        expected_doc = f"series_{expected_series}"
        if not all(citation.get("doc_id") == expected_doc for citation in citations):
            errors.append(f"All citations must reference '{expected_doc}'.")
        doc_ids = {doc.get("doc_id") for doc in response.get("retrieved_docs", []) if doc.get("doc_id")}
        if expected_doc not in doc_ids:
            errors.append("Retrieved docs must include the cited series document.")

    confidence = response.get("confidence")
    if confidence is None or confidence < 0.7:
        errors.append("Answerable responses must have confidence >= 0.7.")

    if response.get("errors"):
        errors.append("Answerable responses must not include parse errors.")

    transform = transform or ""
    if transform in {"point", "yoy", "mom", "ma"} and not response.get("date"):
        errors.append(f"Transform '{transform}' requires a non-null date field.")
    if transform == "ma":
        window = response.get("window") or {}
        periods = window.get("periods")
        if not isinstance(periods, int):
            errors.append("Moving average responses must include window.periods.")
    if transform in {"max", "min"}:
        window = response.get("window") or {}
        if not window.get("start") or not window.get("end"):
            errors.append(f"Transform '{transform}' requires window start/end values.")

    return errors


def _validate_truth(response: Dict[str, Any], truth_spec: Dict[str, Any]) -> Optional[str]:
    series_id = truth_spec.get("series_id")
    transform = truth_spec.get("transform")
    tolerance = float(truth_spec.get("tolerance", 1e-6))

    if not series_id or not transform:
        return "truth_spec must include series_id and transform."

    if response.get("value") is None:
        return "Cannot run truth check without a numeric value in the response."

    try:
        response_value = float(response["value"])
    except (TypeError, ValueError):
        return "Response value could not be converted to float."

    try:
        with duckdb.connect(str(DB_PATH)) as con:
            truth_value = _compute_truth(con, series_id, transform, truth_spec)
    except Exception as exc:  # pragma: no cover - defensive guard
        return f"Truth evaluation failed: {exc}"

    if truth_value is None or (isinstance(truth_value, float) and math.isnan(truth_value)):
        return "Truth query returned no value."

    if abs(response_value - truth_value) > tolerance:
        return (
            f"Value {response_value} differs from truth {truth_value} beyond tolerance {tolerance}."
        )

    return None


def _compute_truth(con, series_id: str, transform: str, spec: Dict[str, Any]) -> Optional[float]:
    if transform == "point":
        return truth.get_point(con, series_id, spec.get("date"))
    if transform == "yoy":
        return truth.get_yoy(con, series_id, spec.get("date"))
    if transform == "mom":
        return truth.get_mom(con, series_id, spec.get("date"))
    if transform == "ma":
        periods = spec.get("periods")
        if periods is None:
            raise ValueError("truth_spec.periods is required for moving averages.")
        return truth.get_ma(con, series_id, spec.get("date"), periods)
    if transform == "max":
        window = spec.get("window") or {}
        return truth.get_max(con, series_id, window.get("start"), window.get("end"))[1]
    if transform == "min":
        window = spec.get("window") or {}
        return truth.get_min(con, series_id, window.get("start"), window.get("end"))[1]
    raise ValueError(f"Unsupported transform '{transform}'.")


def _looks_like_clarification(answer: str) -> bool:
    lower = answer.lower()
    keywords = [
        "provide",
        "specify",
        "missing",
        "need",
        "which series",
        "more detail",
        "clarify",
    ]
    return any(keyword in lower for keyword in keywords)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _build_result(passed: bool, reason: str) -> Dict[str, Any]:
    return {"pass": passed, "score": 1.0 if passed else 0.0, "reason": reason}


if __name__ == "__main__":
    main()
