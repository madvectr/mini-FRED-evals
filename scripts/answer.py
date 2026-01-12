#!/usr/bin/env python3
"""Wrapper for Mini-FRED RAG agents."""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Dict, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.util import resolve_project_root  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Answer Mini-FRED questions via versioned agents.")
    parser.add_argument("question", help="Natural language question.")
    parser.add_argument(
        "--config",
        default="config/series.yaml",
        help="Path to series config (default: config/series.yaml).",
    )
    parser.add_argument(
        "--db",
        default="data/warehouse.duckdb",
        help="Path to DuckDB warehouse (default: data/warehouse.duckdb).",
    )
    parser.add_argument(
        "--cards-dir",
        default="corpus/series_cards",
        help="Directory containing generated series cards.",
    )
    parser.add_argument(
        "--agent",
        default="answer_4",
        help="Agent module name inside rag_agent package (default: answer_4).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = resolve_project_root()
    config_path = _resolve_path(project_root, args.config)
    db_path = _resolve_path(project_root, args.db)
    cards_dir = _resolve_path(project_root, args.cards_dir)

    agent = _load_agent(args.agent)
    response = agent.run(
        args.question,
        config_path=config_path,
        db_path=db_path,
        cards_dir=cards_dir,
    )
    _ensure_value_display(response)
    print(json.dumps(response, indent=2))


def _load_agent(agent_name: str):
    try:
        module = importlib.import_module(f"rag_agent.{agent_name}")
    except ModuleNotFoundError as exc:
        raise SystemExit(f"Unknown agent '{agent_name}'.") from exc
    if not hasattr(module, "run"):
        raise SystemExit(f"Agent '{agent_name}' does not expose a run() function.")
    return module


def _ensure_value_display(payload: Dict[str, object]) -> None:
    value = payload.get("value")
    if value is None:
        payload.setdefault("value_display", None)
        return
    canonical = _format_value_display(
        payload.get("transform"),
        value,
        payload.get("unit"),
    )
    if not payload.get("value_display"):
        payload["value_display"] = canonical
    answer_text = payload.get("answer") or ""
    if canonical and canonical not in answer_text:
        payload["answer"] = (answer_text + f" {canonical}").strip()


def _format_value_display(transform: Optional[str], value: object, units: object) -> Optional[str]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    percent_hint = (transform in {"yoy", "mom"}) or (
        isinstance(units, str) and "percent" in units.lower()
    )
    if percent_hint:
        return f"{numeric:.2f}%"
    if abs(numeric) >= 1000:
        return f"{numeric:,.2f}"
    if abs(numeric) >= 1:
        return f"{numeric:.2f}"
    return f"{numeric:.4f}"


def _resolve_path(project_root: Path, raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


if __name__ == "__main__":
    main()
