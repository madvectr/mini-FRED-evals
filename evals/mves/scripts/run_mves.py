#!/usr/bin/env python3
"""Wrapper to run the canonical MVES golden + refusal suite."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
MVES_RUN = ROOT / "scripts" / "mves_run.py"

DEFAULT_GOLDEN = ROOT / "evals" / "mves" / "golden.jsonl"
DEFAULT_REFUSALS = ROOT / "evals" / "mves" / "refusal.jsonl"
DEFAULT_REPORTS = ROOT / "reports" / "mves"
DEFAULT_DB = ROOT / "data" / "warehouse.duckdb"
DEFAULT_SPEC = ROOT / "mves" / "spec.yaml"
DEFAULT_VERIFIERS = ROOT / "mves" / "verifier_map.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the MVES golden suite.")
    parser.add_argument("--agent", default="answer_1", help="Agent module to evaluate.")
    parser.add_argument("--golden", type=Path, default=DEFAULT_GOLDEN, help="Golden JSONL path.")
    parser.add_argument(
        "--refusals",
        default=str(DEFAULT_REFUSALS),
        help="Optional refusal JSONL (set to empty string to skip).",
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB path.")
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS, help="Reports dir.")
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC, help="MVES spec to use.")
    parser.add_argument(
        "--verifiers", type=Path, default=DEFAULT_VERIFIERS, help="Verifier map to use."
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cmd = [
        sys.executable,
        str(MVES_RUN),
        "--golden",
        str(args.golden),
        "--reports-dir",
        str(args.reports_dir),
        "--db",
        str(args.db),
        "--agent",
        args.agent,
        "--spec",
        str(args.spec),
        "--verifiers",
        str(args.verifiers),
        "--refusals",
        args.refusals,
    ]
    subprocess.run(cmd, check=True, cwd=ROOT)


if __name__ == "__main__":
    main()
