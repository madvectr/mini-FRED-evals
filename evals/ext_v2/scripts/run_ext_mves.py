#!/usr/bin/env python3
"""Helper to run MVES against the ext_v2 evalset with merged spec/verifiers."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]

LOAD_SPEC = ROOT / "evals" / "ext_v2" / "load_spec.py"
MVES_RUN = ROOT / "scripts" / "mves_run.py"

DEFAULT_EVAL = ROOT / "evals" / "ext_v2" / "evalset.jsonl"
DEFAULT_REPORTS = ROOT / "reports" / "ext_v2"
DEFAULT_SPEC_BASE = ROOT / "mves" / "spec.yaml"
DEFAULT_SPEC_OVERRIDE = ROOT / "evals" / "ext_v2" / "spec.override.json"
DEFAULT_VERIFIERS_BASE = ROOT / "mves" / "verifier_map.yaml"
DEFAULT_VERIFIERS_OVERRIDE = ROOT / "evals" / "ext_v2" / "verifiers.override.json"
DEFAULT_MERGED_SPEC = ROOT / "evals" / "ext_v2" / "spec.merged.json"
DEFAULT_MERGED_VERIFIERS = ROOT / "evals" / "ext_v2" / "verifiers.merged.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MVES against the ext_v2 evalset.")
    parser.add_argument("--eval", type=Path, default=DEFAULT_EVAL, help="Evalset JSONL path.")
    parser.add_argument("--agent", default="answer_4", help="Agent module to evaluate.")
    parser.add_argument("--db", type=Path, default=ROOT / "data" / "warehouse.duckdb", help="DuckDB path.")
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS, help="Reports output dir.")
    parser.add_argument("--spec-base", type=Path, default=DEFAULT_SPEC_BASE)
    parser.add_argument("--spec-override", type=Path, default=DEFAULT_SPEC_OVERRIDE)
    parser.add_argument("--spec-out", type=Path, default=DEFAULT_MERGED_SPEC)
    parser.add_argument("--verifiers-base", type=Path, default=DEFAULT_VERIFIERS_BASE)
    parser.add_argument("--verifiers-override", type=Path, default=DEFAULT_VERIFIERS_OVERRIDE)
    parser.add_argument("--verifiers-out", type=Path, default=DEFAULT_MERGED_VERIFIERS)
    return parser.parse_args()


def run_load_spec(base_flag: str, base_path: Path, override: Path, out_path: Path) -> None:
    cmd = [
        sys.executable,
        str(LOAD_SPEC),
        base_flag,
        str(base_path),
        "--override",
        str(override),
        "--out",
        str(out_path),
    ]
    subprocess.run(cmd, check=True, cwd=ROOT)


def main() -> None:
    args = parse_args()
    args.spec_out.parent.mkdir(parents=True, exist_ok=True)
    args.verifiers_out.parent.mkdir(parents=True, exist_ok=True)

    run_load_spec("--base-spec", args.spec_base, args.spec_override, args.spec_out)
    run_load_spec("--base-verifiers", args.verifiers_base, args.verifiers_override, args.verifiers_out)

    cmd = [
        sys.executable,
        str(MVES_RUN),
        "--golden",
        str(args.eval),
        "--reports-dir",
        str(args.reports_dir),
        "--db",
        str(args.db),
        "--agent",
        args.agent,
        "--spec",
        str(args.spec_out),
        "--verifiers",
        str(args.verifiers_out),
        "--refusals",
        "",
    ]
    subprocess.run(cmd, check=True, cwd=ROOT)


if __name__ == "__main__":
    main()
