#!/usr/bin/env python3
"""Run MVES verifiers against the Mini-FRED answer CLI."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mves import verifiers as mves_verifiers  # noqa: E402
DEFAULT_GOLDEN = Path("evals/mves/golden.jsonl")
REFUSAL_PATH = Path("evals/mves/refusal.jsonl")
REPORTS_DIR = Path("reports")
ANSWER_SCRIPT = Path("scripts/answer.py")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MVES evaluation suite.")
    parser.add_argument(
        "--golden",
        default=str(DEFAULT_GOLDEN),
        help="Path to golden JSONL file (default: evals/mves/golden.jsonl).",
    )
    parser.add_argument(
        "--reports-dir",
        default=str(REPORTS_DIR),
        help="Directory to write MVES reports (default: reports/).",
    )
    parser.add_argument(
        "--db",
        default="data/warehouse.duckdb",
        help="Path to DuckDB warehouse (default: data/warehouse.duckdb).",
    )
    parser.add_argument(
        "--agent",
        default="answer_1",
        help="rag_agent module to evaluate (default: answer_1).",
    )
    parser.add_argument(
        "--spec",
        default=str(mves_verifiers.SPEC_PATH),
        help="Path to MVES spec (default: mves/spec.yaml).",
    )
    parser.add_argument(
        "--verifiers",
        default=str(mves_verifiers.MAP_PATH),
        help="Path to verifier map (default: mves/verifier_map.yaml).",
    )
    parser.add_argument(
        "--refusals",
        default=str(REFUSAL_PATH),
        help="Optional refusal JSONL (set to empty string to skip).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of concurrent workers when running cases (default: 1).",
    )
    return parser.parse_args()


def load_golden(path: Path) -> List[Dict[str, Any]]:
    cases: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            cases.append(json.loads(stripped))
    return cases


def run_answer(question: str, agent: str) -> Dict[str, Any]:
    cmd = [sys.executable, str(ANSWER_SCRIPT), "--agent", agent, question]
    completed = subprocess.run(
        cmd, capture_output=True, text=True, check=False, cwd=Path(__file__).resolve().parents[1]
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"answer.py failed (exit {completed.returncode}): {completed.stderr.strip()}"
        )
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse answer.py output: {exc}\nOutput: {completed.stdout}")


def run_case(case: Dict[str, Any], db_path: Path, agent: str) -> Dict[str, Any]:
    question = case["question"]
    try:
        response = run_answer(question, agent)
    except Exception as exc:
        failure = mves_verifiers.Failure("runner_error", "critical", str(exc))
        return {
            "id": case["id"],
            "question": question,
            "status": "fail",
            "response": None,
            "failures": [asdict(failure)],
        }

    failures = [asdict(f) for f in mves_verifiers.verify_case(case, response, db_path)]
    status = "pass" if not failures else "fail"
    return {
        "id": case["id"],
        "question": question,
        "status": status,
        "response": response,
        "failures": failures,
    }


def summarize(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(results)
    failed = [r for r in results if r["status"] == "fail"]
    passed = total - len(failed)
    failure_counts: Dict[str, int] = {}
    critical_failures = 0
    for result in failed:
        for failure in result["failures"]:
            vid = failure["verifier_id"]
            failure_counts[vid] = failure_counts.get(vid, 0) + 1
            if failure["severity"] == "critical":
                critical_failures += 1
    pass_rate = passed / total if total else 0.0
    return {
        "total": total,
        "passed": passed,
        "failed": len(failed),
        "pass_rate": pass_rate,
        "critical_failures": critical_failures,
        "failure_breakdown": failure_counts,
    }


def write_reports(
    reports_dir: Path,
    summary: Dict[str, Any],
    results: List[Dict[str, Any]],
    agent: str,
) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    summary_with_agent = dict(summary)
    summary_with_agent["agent"] = agent
    report_json = {
        "summary": summary_with_agent,
        "cases": results,
    }
    suffix = agent.replace("/", "_")
    report_json = {
        "summary": summary_with_agent,
        "cases": results,
    }
    (reports_dir / f"mves_report_{suffix}.json").write_text(
        json.dumps(report_json, indent=2), encoding="utf-8"
    )

    lines = [
        "# MVES Report",
        "",
        f"- Agent: {agent}",
        f"- Total cases: {summary['total']}",
        f"- Passed: {summary['passed']}",
        f"- Failed: {summary['failed']}",
        f"- Pass rate: {summary['pass_rate']:.1%}",
        f"- Critical failures: {summary['critical_failures']}",
        "",
        "## Failure breakdown",
    ]
    if summary["failure_breakdown"]:
        for vid, count in summary["failure_breakdown"].items():
            lines.append(f"- {vid}: {count}")
    else:
        lines.append("- None")

    lines.append("")
    lines.append("## Example failed cases")
    failed_cases = [case for case in results if case["status"] == "fail"]
    for case in failed_cases[:5]:
        lines.append(f"- **{case['id']}**")
        lines.append(f"  - Question: {case['question']}")
        for failure in case["failures"]:
            lines.append(f"  - [{failure['severity']}] {failure['verifier_id']}: {failure['message']}")
        lines.append("")

    (reports_dir / f"mves_report_{suffix}.md").write_text(
        "\n".join(lines).strip() + "\n", encoding="utf-8"
    )


def _execute_cases(
    cases: List[Dict[str, Any]], db_path: Path, agent: str, workers: int
) -> List[Dict[str, Any]]:
    total = len(cases)
    if workers <= 1:
        results = []
        for idx, case in enumerate(cases, start=1):
            print(f"[MVES] ({idx}/{total}) running {case['id']}...", flush=True)
            start = time.perf_counter()
            result = run_case(case, db_path, agent)
            duration = time.perf_counter() - start
            results.append(result)
            print(
                f"[MVES] ({idx}/{total}) finished {case['id']} "
                f"| status={result['status']} | {duration:.1f}s",
                flush=True,
            )
        return results

    workers = max(1, workers)
    results: List[Optional[Dict[str, Any]]] = [None] * total

    def _task(index_case: Tuple[int, Dict[str, Any]]) -> Tuple[int, str, Dict[str, Any], float]:
        idx, case = index_case
        print(f"[MVES] ({idx + 1}/{total}) running {case['id']}...", flush=True)
        start = time.perf_counter()
        result = run_case(case, db_path, agent)
        duration = time.perf_counter() - start
        return idx, case["id"], result, duration

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_task, (idx, case)) for idx, case in enumerate(cases)]
        for future in concurrent.futures.as_completed(futures):
            idx, case_id, result, duration = future.result()
            results[idx] = result
            print(
                f"[MVES] ({idx + 1}/{total}) finished {case_id} "
                f"| status={result['status']} | {duration:.1f}s",
                flush=True,
            )

    # type: ignore return type, list fully populated
    return results  # type: ignore[return-value]


def main() -> None:
    args = parse_args()
    golden_path = Path(args.golden)
    reports_dir = Path(args.reports_dir)

    mves_verifiers.SPEC_PATH = Path(args.spec)
    mves_verifiers.MAP_PATH = Path(args.verifiers)

    cases = load_golden(golden_path)
    refusal_arg = args.refusals.strip()
    if refusal_arg:
        refusal_path = Path(refusal_arg)
        if refusal_path.exists():
            cases.extend(load_golden(refusal_path))

    db_path = Path(args.db)
    agent = args.agent

    results = _execute_cases(cases, db_path, agent, args.workers)
    summary = summarize(results)
    write_reports(reports_dir, summary, results, agent)

    pass_rate_ok = summary["pass_rate"] >= 0.9
    no_critical_failures = summary["critical_failures"] == 0

    print(
        f"[MVES] Completed {summary['total']} cases | "
        f"pass rate={summary['pass_rate']:.1%} | "
        f"critical_failures={summary['critical_failures']}",
        flush=True,
    )

    if not pass_rate_ok or not no_critical_failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
