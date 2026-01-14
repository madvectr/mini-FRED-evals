#!/usr/bin/env python3
"""Smoke test for the ext_v2 generation pipeline."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, Any, List

import duckdb  # type: ignore[import]

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import truth  # noqa: E402

GOLDEN_SCRIPT = ROOT / "evals" / "ext_v2" / "generate_golden_from_duckdb.py"
QUESTION_SCRIPT = ROOT / "evals" / "ext_v2" / "generate_questions.py"
BUILD_SCRIPT = ROOT / "evals" / "ext_v2" / "build_evalset.py"
REFUSALS_PATH = ROOT / "evals" / "ext_v2" / "refusals.jsonl"
DB_PATH = ROOT / "data" / "warehouse.duckdb"


def run_script(script: Path, *args: str) -> None:
    cmd = [sys.executable, str(script), *args]
    subprocess.run(cmd, check=True, cwd=ROOT)


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            items.append(json.loads(stripped))
    return items


def validate_truth_specs(cases: List[Dict[str, Any]]) -> None:
    answered = [case for case in cases if case.get("truth_spec")]
    if not answered:
        raise AssertionError("No answered cases produced.")
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        for case in answered[:3]:
            spec = case["truth_spec"]
            series_id = spec["series_id"]
            transform = spec["transform"]
            if transform == "point":
                value = truth.get_point(conn, series_id, spec["date"])
            elif transform == "yoy":
                value = truth.get_yoy(conn, series_id, spec["date"])
            elif transform == "mom":
                value = truth.get_mom(conn, series_id, spec["date"])
            elif transform == "ma":
                value = truth.get_ma(conn, series_id, spec["date"], spec["periods"])
            elif transform == "max":
                value = truth.get_max(conn, series_id, spec["window"]["start"], spec["window"]["end"])[1]
            elif transform == "min":
                value = truth.get_min(conn, series_id, spec["window"]["start"], spec["window"]["end"])[1]
            else:
                raise AssertionError(f"Unsupported transform {transform}")
            if value is None:
                raise AssertionError(f"Truth computation failed for case {case['id']}")
    finally:
        conn.close()


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        golden_path = tmp / "golden.jsonl"
        cases_path = tmp / "cases.jsonl"
        evalset_path = tmp / "evalset.jsonl"

        run_script(
            GOLDEN_SCRIPT,
            "--per-series",
            "2",
            "--seed",
            "777",
            "--out",
            str(golden_path),
        )
        run_script(
            QUESTION_SCRIPT,
            "--golden",
            str(golden_path),
            "--variants",
            "1",
            "--seed",
            "777",
            "--out",
            str(cases_path),
        )
        run_script(
            BUILD_SCRIPT,
            "--cases",
            str(cases_path),
            "--refusals",
            str(REFUSALS_PATH),
            "--out",
            str(evalset_path),
        )

        eval_cases = load_jsonl(evalset_path)
        if not eval_cases:
            raise AssertionError("evalset.jsonl is empty.")

        refusal_count = sum(
            1 for case in eval_cases if not case.get("expect", {}).get("should_have_value", True)
        )
        if refusal_count == 0:
            raise AssertionError("No refusal cases were included in evalset.")

        validate_truth_specs(eval_cases)

        print(
            f"[ext_v2] Smoke test passed (cases={len(eval_cases)}, refusals={refusal_count}).",
            flush=True,
        )


if __name__ == "__main__":
    main()
