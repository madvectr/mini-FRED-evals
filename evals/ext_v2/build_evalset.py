#!/usr/bin/env python3
"""Combine answered + refusal cases into a single evalset."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Dict, Any

DEFAULT_CASES = Path("evals/ext_v2/cases.jsonl")
DEFAULT_REFUSALS = Path("evals/ext_v2/refusals.jsonl")
DEFAULT_OUT = Path("evals/ext_v2/evalset.jsonl")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the merged ext_v2 evalset.")
    parser.add_argument("--cases", type=Path, default=DEFAULT_CASES, help="Answered cases JSONL.")
    parser.add_argument(
        "--refusals", type=Path, default=DEFAULT_REFUSALS, help="Refusal template JSONL."
    )
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output evalset JSONL path.")
    return parser.parse_args()


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            items.append(json.loads(stripped))
    return items


def main() -> None:
    args = parse_args()
    cases = load_jsonl(args.cases)
    refusals = load_jsonl(args.refusals)
    combined = cases + refusals

    seen = set()
    for case in combined:
        case_id = case.get("id")
        if not case_id:
            raise ValueError("Case missing 'id' field.")
        if case_id in seen:
            raise ValueError(f"Duplicate case id detected: {case_id}")
        seen.add(case_id)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as handle:
        for case in combined:
            handle.write(json.dumps(case) + "\n")

    print(
        f"[ext_v2] evalset built -> {args.out} (answered={len(cases)}, refusals={len(refusals)}, total={len(combined)})"
    )


if __name__ == "__main__":
    main()
