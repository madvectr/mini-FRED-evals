#!/usr/bin/env python3
"""Generate Mini-FRED golden questions from the DuckDB warehouse."""

from __future__ import annotations

import argparse
import calendar
import csv
import json
import random
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
try:
    import yaml  # type: ignore[import]
except ModuleNotFoundError:  # pragma: no cover - fallback when PyYAML missing
    yaml = None  # type: ignore[assignment]

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate eval/golden.jsonl")
    parser.add_argument("--db", default="data/warehouse.duckdb", help="DuckDB path.")
    parser.add_argument("--config", default="config/series.yaml", help="Series config.")
    parser.add_argument("--out", default="eval/golden.jsonl", help="Output JSONL.")
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
        observations: Dict[str, List[Dict[str, Any]]],
        config: Dict[str, Any],
        seed: int,
        ma_period_range: Sequence[int],
    ):
        self.observations = observations
        self.config = config
        self.rng = random.Random(seed)
        self.series_meta = {entry["id"]: entry for entry in config.get("series", [])}
        self.ma_period_range = ma_period_range
        self.lookup = {
            series_id: {row["date"]: row["value"] for row in rows}
            for series_id, rows in observations.items()
        }

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
        for series_id, rows in self.observations.items():
            for row in rows:
                candidates.append((series_id, row["date"]))
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
        for series_id, rows in self.observations.items():
            for row in rows:
                date_str = row["date"]
                if self._get_yoy(series_id, date_str) is None:
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
        for series_id, rows in self.observations.items():
            for row in rows:
                date_str = row["date"]
                if self._get_mom(series_id, date_str) is None:
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
        for series_id, rows in self.observations.items():
            for row in rows:
                date_str = row["date"]
                for periods in range(min_period, max_period + 1):
                    if self._get_ma(series_id, date_str, periods) is None:
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
        for series_id, rows in self.observations.items():
            dates = [row["date"] for row in rows]
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
                value = (
                    self._get_window_extreme(series_id, start_date, end_date, transform)
                )
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

    def _get_value(self, series_id: str, target_date: str) -> Optional[float]:
        return self.lookup.get(series_id, {}).get(target_date)

    def _shift_months(self, dt: date, months: int) -> date:
        year = dt.year + (dt.month + months - 1) // 12
        month = (dt.month + months - 1) % 12 + 1
        day = min(dt.day, calendar.monthrange(year, month)[1])
        return date(year, month, day)

    def _shift_years(self, dt: date, years: int) -> date:
        target_year = dt.year + years
        day = dt.day
        while day > 0:
            try:
                return date(target_year, dt.month, day)
            except ValueError:
                day -= 1
        return date(target_year, dt.month, 1)

    def _get_yoy(self, series_id: str, target_date: str) -> Optional[float]:
        dt = datetime.fromisoformat(target_date).date()
        prev_dt = self._shift_years(dt, -1)
        current = self._get_value(series_id, target_date)
        prev = self._get_value(series_id, prev_dt.isoformat())
        if current is None or prev in (None, 0):
            return None
        return (current - prev) / prev * 100.0

    def _get_mom(self, series_id: str, target_date: str) -> Optional[float]:
        dt = datetime.fromisoformat(target_date).date()
        prev_dt = self._shift_months(dt, -1)
        current = self._get_value(series_id, target_date)
        prev = self._get_value(series_id, prev_dt.isoformat())
        if current is None or prev in (None, 0):
            return None
        return (current - prev) / prev * 100.0

    def _get_ma(self, series_id: str, target_date: str, periods: int) -> Optional[float]:
        rows = self.observations.get(series_id, [])
        dates = [row["date"] for row in rows]
        if target_date not in dates:
            return None
        idx = dates.index(target_date)
        if idx + 1 < periods:
            return None
        values = [rows[i]["value"] for i in range(idx - periods + 1, idx + 1)]
        if any(v is None for v in values):
            return None
        return sum(values) / periods

    def _get_window_extreme(
        self, series_id: str, start: str, end: str, transform: str
    ) -> Optional[float]:
        rows = self.observations.get(series_id, [])
        values = [
            row["value"]
            for row in rows
            if start <= row["date"] <= end and row["value"] is not None
        ]
        if not values:
            return None
        return max(values) if transform == "max" else min(values)


def main() -> None:
    args = parse_args()
    config = load_config(Path(args.config))
    observations = load_observations()
    min_period, max_period = (int(x) for x in args.ma_periods.split(","))
    generator = GoldenGenerator(
        observations=observations,
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


def json_dump(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"))


def load_observations() -> Dict[str, List[Dict[str, Any]]]:
    obs_path = Path("data/snapshots/observations.csv")
    data: Dict[str, List[Dict[str, Any]]] = {}
    with obs_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            series_id = row["series_id"]
            date_str = row["date"]
            if not date_str:
                continue
            value_str = row["value"]
            value = float(value_str) if value_str not in ("", ".", None) else None
            data.setdefault(series_id, []).append({"date": date_str, "value": value})
    for series_id in data:
        data[series_id].sort(key=lambda r: r["date"])
    return data


if __name__ == "__main__":
    main()
