#!/usr/bin/env python3
"""Generate Mini-FRED golden questions from the DuckDB warehouse."""

from __future__ import annotations

import argparse
import json
import random
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import duckdb  # type: ignore[import]

try:
    import yaml  # type: ignore[import]
except ModuleNotFoundError:  # pragma: no cover - fallback when PyYAML missing
    yaml = None  # type: ignore[assignment]

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import truth  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate evals/mves/golden.jsonl")
    parser.add_argument("--db", default="data/warehouse.duckdb", help="DuckDB path.")
    parser.add_argument("--config", default="config/series.yaml", help="Series config.")
    parser.add_argument("--out", default="evals/mves/golden.jsonl", help="Output JSONL.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument("--n-point", type=int, default=20)
    parser.add_argument("--n-window", type=int, default=10)
    parser.add_argument("--n-yoy", type=int, default=10)
    parser.add_argument("--n-mom", type=int, default=10)
    parser.add_argument("--n-ma", type=int, default=8)
    parser.add_argument("--ma-periods", type=str, default="3,6", help="Min,max periods.")
    return parser.parse_args()


def load_config(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if yaml is not None:
        return yaml.safe_load(text)
    # Minimal fallback parser for simple key/value lists
    series: List[Dict[str, Any]] = []
    current: Dict[str, Any] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("- id:"):
            if current:
                series.append(current)
            current = {"id": line.split(":", 1)[1].strip()}
        elif ":" in line and current:
            key, value = line.split(":", 1)
            current[key.strip()] = value.strip().strip('"')
    if current:
        series.append(current)
    return {"series": series}


def fmt_date(date_str: str) -> str:
    dt = datetime.fromisoformat(date_str)
    if dt.day == 1:
        return dt.strftime("%B %Y")
    return dt.strftime("%B %d, %Y")


class GoldenGenerator:
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        config: Dict[str, Any],
        seed: int,
        ma_period_range: Sequence[int],
    ):
        self.conn = conn
        self.config = config
        self.rng = random.Random(seed)
        self.series_meta = {entry["id"]: entry for entry in config.get("series", [])}
        self.ma_period_range = ma_period_range
        self.date_cache: Dict[str, List[str]] = {}

    def build(self, counts: Dict[str, int]) -> List[Dict[str, Any]]:
        cases: List[Dict[str, Any]] = []
        cases += self._build_point_cases(counts["point"])
        cases += self._build_yoy_cases(counts["yoy"])
        cases += self._build_mom_cases(counts["mom"])
        cases += self._build_ma_cases(counts["ma"])
        window_max = counts["window"] // 2
        window_min = counts["window"] - window_max
        cases += self._build_window_cases(window_max, "max")
        cases += self._build_window_cases(window_min, "min")
        self.rng.shuffle(cases)
        return cases

    def _build_point_cases(self, count: int) -> List[Dict[str, Any]]:
        candidates = []
        for series_id in self.series_meta:
            for date_str in self._series_dates(series_id):
                candidates.append((series_id, date_str))
        self.rng.shuffle(candidates)
        cases = []
        for idx, (series_id, date_str) in enumerate(candidates[:count]):
            title = self.series_meta[series_id]["title"]
            question = f"What was {title} in {fmt_date(date_str)}?"
            cases.append(
                self._make_case(
                    series_id=series_id,
                    transform="point",
                    date=date_str,
                    question=question,
                    identifier=f"{series_id.lower()}_point_{idx}",
                )
            )
        return cases

    def _build_yoy_cases(self, count: int) -> List[Dict[str, Any]]:
        options = []
        for series_id in self.series_meta:
            for date_str in self._series_dates(series_id):
                if truth.get_yoy(self.conn, series_id, date_str) is None:
                    continue
                options.append((series_id, date_str))
        self.rng.shuffle(options)
        cases = []
        for idx, (series_id, date_str) in enumerate(options[:count]):
            title = self.series_meta[series_id]["title"]
            question = f"What was the year-over-year change in {title} in {fmt_date(date_str)}?"
            cases.append(
                self._make_case(
                    series_id=series_id,
                    transform="yoy",
                    date=date_str,
                    question=question,
                    identifier=f"{series_id.lower()}_yoy_{idx}",
                )
            )
        return cases

    def _build_mom_cases(self, count: int) -> List[Dict[str, Any]]:
        options = []
        for series_id in self.series_meta:
            for date_str in self._series_dates(series_id):
                if truth.get_mom(self.conn, series_id, date_str) is None:
                    continue
                options.append((series_id, date_str))
        self.rng.shuffle(options)
        cases = []
        for idx, (series_id, date_str) in enumerate(options[:count]):
            title = self.series_meta[series_id]["title"]
            question = f"What was the month-over-month change in {title} in {fmt_date(date_str)}?"
            cases.append(
                self._make_case(
                    series_id=series_id,
                    transform="mom",
                    date=date_str,
                    question=question,
                    identifier=f"{series_id.lower()}_mom_{idx}",
                )
            )
        return cases

    def _build_ma_cases(self, count: int) -> List[Dict[str, Any]]:
        min_period, max_period = self.ma_period_range
        options = []
        for series_id in self.series_meta:
            for date_str in self._series_dates(series_id):
                for periods in range(min_period, max_period + 1):
                    if truth.get_ma(self.conn, series_id, date_str, periods) is None:
                        continue
                    options.append((series_id, date_str, periods))
        self.rng.shuffle(options)
        cases = []
        for idx, (series_id, date_str, periods) in enumerate(options[:count]):
            title = self.series_meta[series_id]["title"]
            question = f"What was the {periods}-period moving average of {title} in {fmt_date(date_str)}?"
            cases.append(
                self._make_case(
                    series_id=series_id,
                    transform="ma",
                    date=date_str,
                    periods=periods,
                    question=question,
                    identifier=f"{series_id.lower()}_ma_{idx}",
                )
            )
        return cases

    def _build_window_cases(self, count: int, transform: str) -> List[Dict[str, Any]]:
        assert transform in {"max", "min"}
        options = []
        for series_id in self.series_meta:
            dates = self._series_dates(series_id)
            if len(dates) < 3:
                continue
            indices = list(range(len(dates) - 2))
            self.rng.shuffle(indices)
            for idx in indices:
                start_date = dates[idx]
                end_candidates = dates[idx + 2 :]
                if not end_candidates:
                    continue
                end_date = self.rng.choice(end_candidates)
                if transform == "max":
                    _, value = truth.get_max(self.conn, series_id, start_date, end_date)
                else:
                    _, value = truth.get_min(self.conn, series_id, start_date, end_date)
                if value is None:
                    continue
                options.append((series_id, start_date, end_date))
                if len(options) >= count * 2:
                    break
        self.rng.shuffle(options)
        cases = []
        for idx, (series_id, start, end) in enumerate(options[:count]):
            title = self.series_meta[series_id]["title"]
            adjective = "highest" if transform == "max" else "lowest"
            question = f"What was the {adjective} {title} between {fmt_date(start)} and {fmt_date(end)}?"
            cases.append(
                self._make_case(
                    series_id=series_id,
                    transform=transform,
                    window={"start": start, "end": end},
                    question=question,
                    identifier=f"{series_id.lower()}_{transform}_{idx}",
                )
            )
        return cases

    def _make_case(
        self,
        *,
        series_id: str,
        transform: str,
        question: str,
        identifier: str,
        date: Optional[str] = None,
        periods: Optional[int] = None,
        window: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        truth_spec: Dict[str, Any] = {
            "series_id": series_id,
            "transform": transform,
            "tolerance": 1e-6,
        }
        if date:
            truth_spec["date"] = date
        if periods:
            truth_spec["periods"] = periods
        if window:
            truth_spec["window"] = window
        return {
            "id": identifier,
            "question": question,
            "expect": {
                "should_answer": True,
                "should_have_value": True,
                "transform": transform,
                "series_id": series_id,
            },
            "truth_spec": truth_spec,
        }

    def _series_dates(self, series_id: str) -> List[str]:
        if series_id not in self.date_cache:
            rows = self.conn.execute(
                "SELECT date::TEXT FROM observations WHERE series_id = ? ORDER BY date",
                [series_id],
            ).fetchall()
            self.date_cache[series_id] = [row[0] for row in rows]
        return self.date_cache[series_id]


def main() -> None:
    args = parse_args()
    config = load_config(Path(args.config))
    conn = duckdb.connect(args.db, read_only=True)
    min_period, max_period = (int(x) for x in args.ma_periods.split(","))
    generator = GoldenGenerator(
        conn=conn,
        config=config,
        seed=args.seed,
        ma_period_range=(min_period, max_period),
    )
    counts = {
        "point": args.n_point,
        "yoy": args.n_yoy,
        "mom": args.n_mom,
        "ma": args.n_ma,
        "window": args.n_window,
    }
    cases = generator.build(counts)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as handle:
        for case in cases:
            handle.write(f"{json_dump(case)}\n")
    conn.close()


def json_dump(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"))


if __name__ == "__main__":
    main()
