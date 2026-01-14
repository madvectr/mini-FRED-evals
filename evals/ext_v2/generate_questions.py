#!/usr/bin/env python3
"""Template-based question generator for MVES ext_v2 (noisier phrasing)."""

from __future__ import annotations

import argparse
import copy
import json
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import sys

try:
    import yaml  # type: ignore[import]
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit("pyyaml is required to run this script.") from exc

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_GOLDEN = ROOT / "evals" / "ext_v2" / "golden.jsonl"
DEFAULT_OUT = ROOT / "evals" / "ext_v2" / "cases.jsonl"
CONFIG_PATH = ROOT / "config" / "series.yaml"

POINT_TEMPLATES = [
    "What was {series} in {date}?",
    "Give me {series} for {date}.",
    "Report the value of {series} as of {date}.",
    "Can you recap {series} for {date}?",
]

CHANGE_TEMPLATES = [
    "What was the {label} for {series} in {date}?",
    "Provide the {label} of {series} for {date}.",
    "How large was the {label} in {date} for {series}?",
    "Quantify the {label} on {date} for {series}.",
]

MA_TEMPLATES = [
    "What was the {period}-period moving average of {series} in {date}?",
    "Report the {period}-month moving average for {series} as of {date}.",
    "Give me the {period}-period MA for {series} at {date}.",
    "How did the rolling {period}-month average of {series} look in {date}?",
]

EXTREME_TEMPLATES = [
    "What was the {label} value of {series} {window}?",
    "Tell me the {label} reading for {series} {window}.",
    "How {label_adj} did {series} get {window}?",
    "Pin down the {label} level of {series} {window}.",
]

YOY_LABELS = ["year-over-year change", "YoY change", "annual change", "annual swing"]
MOM_LABELS = ["month-over-month change", "MoM change", "monthly change", "month-to-month shift"]
MAX_LABELS = [
    ("highest", "high"),
    ("maximum", "high"),
    ("peak", "high"),
]
MIN_LABELS = [
    ("lowest", "low"),
    ("minimum", "low"),
    ("trough", "low"),
]

NOISE_PREFIXES = [
    "",
    "Quick check:",
    "For audit purposes,",
    "Before we proceed,",
    "Operationally speaking,",
]

NOISE_SUFFIXES = [
    "",
    "Keep it numeric.",
    "Answer precisely.",
    "Stick to the factual value.",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Expand golden truth specs into templated questions.")
    parser.add_argument("--golden", type=Path, default=DEFAULT_GOLDEN, help="Path to golden JSONL.")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output JSONL path.")
    parser.add_argument("--variants", type=int, default=3, help="Number of question variants per truth spec.")
    parser.add_argument("--seed", type=int, default=123, help="Deterministic RNG seed.")
    return parser.parse_args()


def load_series_titles() -> Dict[str, str]:
    data = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    titles = {}
    for entry in data.get("series", []):
        titles[entry["id"]] = entry.get("title") or entry["id"]
    return titles


def format_date_options(date_str: Optional[str]) -> List[str]:
    if not date_str:
        return []
    dt = datetime.fromisoformat(date_str).date()
    return [
        dt.strftime("%B %Y"),
        dt.strftime("%b %Y"),
        dt.strftime("%Y-%m"),
    ]


def format_window_options(window: Dict[str, str]) -> List[str]:
    start = window.get("start")
    end = window.get("end")
    if not start or not end:
        return []
    start_long = datetime.fromisoformat(start).strftime("%B %Y")
    end_long = datetime.fromisoformat(end).strftime("%B %Y")
    start_short = datetime.fromisoformat(start).strftime("%Y-%m")
    end_short = datetime.fromisoformat(end).strftime("%Y-%m")
    return [
        f"between {start_long} and {end_long}",
        f"between {start_short} and {end_short}",
        f"from {start_long} to {end_long}",
        f"from {start_short} to {end_short}",
    ]


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    titles = load_series_titles()
    args.out.parent.mkdir(parents=True, exist_ok=True)

    with args.golden.open("r", encoding="utf-8") as reader, args.out.open(
        "w", encoding="utf-8"
    ) as writer:
        total = 0
        for line in reader:
            stripped = line.strip()
            if not stripped:
                continue
            case = json.loads(stripped)
            expect = case.get("expect", {})
            truth_spec = case.get("truth_spec")
            if not truth_spec:
                continue
            series_id = expect.get("series_id") or truth_spec.get("series_id")
            transform = expect.get("transform") or truth_spec.get("transform")
            base_id = case["id"]
            for variant_idx in range(args.variants):
                question, template_name = build_question(
                    rng,
                    transform,
                    titles.get(series_id, series_id),
                    truth_spec,
                )
                variant = {
                    "id": f"{base_id}__v{variant_idx+1}",
                    "question": question,
                    "expect": copy.deepcopy(expect),
                    "truth_spec": copy.deepcopy(truth_spec),
                    "meta": {
                        **(case.get("meta") or {}),
                        "variant_index": variant_idx + 1,
                        "template": template_name,
                        "question_seed": args.seed,
                    },
                }
                writer.write(json.dumps(variant) + "\n")
                total += 1
        print(f"[ext_v2] Wrote {total} question variants to {args.out}")


def build_question(
    rng: random.Random, transform: str, series_title: str, truth_spec: Dict[str, str]
) -> Tuple[str, str]:
    if transform == "point":
        return _build_point_question(rng, series_title, truth_spec)
    if transform == "yoy":
        return _build_change_question(rng, series_title, truth_spec, YOY_LABELS)
    if transform == "mom":
        return _build_change_question(rng, series_title, truth_spec, MOM_LABELS)
    if transform == "ma":
        return _build_ma_question(rng, series_title, truth_spec)
    if transform == "max":
        return _build_extreme_question(rng, series_title, truth_spec, MAX_LABELS)
    if transform == "min":
        return _build_extreme_question(rng, series_title, truth_spec, MIN_LABELS)
    raise ValueError(f"Unsupported transform '{transform}'")


def _build_point_question(
    rng: random.Random, series_title: str, truth_spec: Dict[str, str]
) -> Tuple[str, str]:
    date_options = format_date_options(truth_spec.get("date"))
    template = rng.choice(POINT_TEMPLATES)
    date_text = rng.choice(date_options or [truth_spec.get("date") or "the requested period"])
    question = template.format(series=series_title, date=date_text)
    return decorate_question(rng, question), template


def _build_change_question(
    rng: random.Random, series_title: str, truth_spec: Dict[str, str], labels: List[str]
) -> Tuple[str, str]:
    template = rng.choice(CHANGE_TEMPLATES)
    date_options = format_date_options(truth_spec.get("date"))
    date_text = rng.choice(date_options or [truth_spec.get("date") or "the requested period"])
    label = rng.choice(labels)
    question = template.format(series=series_title, date=date_text, label=label)
    return decorate_question(rng, question), template


def _build_ma_question(
    rng: random.Random, series_title: str, truth_spec: Dict[str, str]
) -> Tuple[str, str]:
    template = rng.choice(MA_TEMPLATES)
    date_options = format_date_options(truth_spec.get("date"))
    date_text = rng.choice(date_options or [truth_spec.get("date") or "the requested period"])
    periods = truth_spec.get("periods", 3)
    question = template.format(series=series_title, date=date_text, period=periods)
    return decorate_question(rng, question), template


def _build_extreme_question(
    rng: random.Random,
    series_title: str,
    truth_spec: Dict[str, str],
    labels: List[tuple],
) -> Tuple[str, str]:
    template = rng.choice(EXTREME_TEMPLATES)
    window_options = format_window_options(truth_spec.get("window", {}))
    window_text = rng.choice(window_options or ["between the requested dates"])
    label, adj = rng.choice(labels)
    question = template.format(series=series_title, window=window_text, label=label, label_adj=adj)
    return decorate_question(rng, question), template


def decorate_question(rng: random.Random, text: str) -> str:
    prefix = rng.choice(NOISE_PREFIXES).strip()
    suffix = rng.choice(NOISE_SUFFIXES).strip()
    body = text.strip()
    if prefix:
        body = f"{prefix} {body}"
    if suffix:
        body = f"{body} {suffix}"
    return ensure_question_mark(body)


def ensure_question_mark(text: str) -> str:
    trimmed = text.strip()
    if trimmed.endswith("?"):
        return trimmed
    return f"{trimmed}?"


if __name__ == "__main__":
    main()
