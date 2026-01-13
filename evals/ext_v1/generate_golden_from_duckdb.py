#!/usr/bin/env python3
"""Deterministic truth-spec sampler for MVES ext_v1."""

from __future__ import annotations

import argparse
import json
import logging
import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import duckdb  # type: ignore[import]
import sys

try:
    import yaml  # type: ignore[import]
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit("pyyaml is required to run this script.") from exc

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import truth  # noqa: E402

DEFAULT_DB = ROOT / "data" / "warehouse.duckdb"
DEFAULT_OUT = ROOT / "evals" / "ext_v1" / "golden.jsonl"
CONFIG_PATH = ROOT / "config" / "series.yaml"
TRANSFORMS = ("point", "yoy", "mom", "ma", "max", "min")
MA_PERIOD_CHOICES = (3, 5, 6, 12)
TOLERANCE = 1e-6


@dataclass
class SampleContext:
    rng: random.Random
    conn: duckdb.DuckDBPyConnection
    series_id: str
    dates: List[str]
    date_positions: Dict[str, int]
    id_counters: Dict[str, int]
    seed: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate MVES golden cases from DuckDB.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to DuckDB warehouse.")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output JSONL file.")
    parser.add_argument("--seed", type=int, default=123, help="Deterministic RNG seed.")
    parser.add_argument(
        "--per-series",
        type=int,
        default=20,
        help="Approximate number of truth specs to sample per series.",
    )
    parser.add_argument(
        "--series",
        nargs="+",
        help="Series IDs to include (default: load from config/series.yaml).",
    )
    parser.add_argument(
        "--date-min",
        type=str,
        help="Optional inclusive minimum date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--date-max",
        type=str,
        help="Optional inclusive maximum date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-sample debugging information.",
    )
    return parser.parse_args()


def load_series_ids(override: Optional[Sequence[str]]) -> List[str]:
    if override:
        return list(override)
    config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    return [entry["id"] for entry in config.get("series", [])]


def load_dates_for_series(
    conn: duckdb.DuckDBPyConnection, series_id: str, date_min: Optional[str], date_max: Optional[str]
) -> List[str]:
    clauses = ["series_id = ?"]
    params: List[object] = [series_id]
    if date_min:
        clauses.append("date >= ?")
        params.append(date_min)
    if date_max:
        clauses.append("date <= ?")
        params.append(date_max)
    where = " AND ".join(clauses)
    query = f"SELECT date FROM observations WHERE {where} ORDER BY date ASC"
    rows = conn.execute(query, params).fetchall()
    raw_dates = [row[0] for row in rows]
    if not raw_dates:
        return []
    return _promote_to_month_starts(raw_dates)


def _promote_to_month_starts(date_values: Iterable[str]) -> List[str]:
    """Reduce to one observation per month (first available entry)."""
    per_month: Dict[str, str] = {}
    for raw in date_values:
        dt = datetime.fromisoformat(str(raw)).date()
        month_key = dt.strftime("%Y-%m")
        if month_key not in per_month:
            per_month[month_key] = dt.isoformat()
    ordered = [per_month[key] for key in sorted(per_month.keys())]
    return ordered


def allocate_counts(per_series: int) -> Dict[str, int]:
    counts = {transform: 0 for transform in TRANSFORMS}
    if per_series <= 0:
        return counts
    base = per_series // len(TRANSFORMS)
    remainder = per_series % len(TRANSFORMS)
    for transform in TRANSFORMS:
        counts[transform] = base
    idx = 0
    while remainder > 0:
        counts[TRANSFORMS[idx % len(TRANSFORMS)]] += 1
        idx += 1
        remainder -= 1
    return counts


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    rng = random.Random(args.seed)
    conn = duckdb.connect(str(args.db), read_only=True)
    try:
        series_ids = load_series_ids(args.series)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with args.out.open("w", encoding="utf-8") as handle:
            summary = defaultdict(lambda: defaultdict(lambda: {"requested": 0, "produced": 0}))
            for series_id in series_ids:
                dates = load_dates_for_series(conn, series_id, args.date_min, args.date_max)
                ctx = SampleContext(
                    rng=rng,
                    conn=conn,
                    series_id=series_id,
                    dates=dates,
                    date_positions={d: idx for idx, d in enumerate(dates)},
                    id_counters=defaultdict(int),
                    seed=args.seed,
                )
                counts = allocate_counts(args.per_series)
                for transform, requested in counts.items():
                    summary[series_id][transform]["requested"] = requested
                    if requested == 0:
                        continue
                    produced = list(_sample_transform(ctx, transform, requested))
                    for record in produced:
                        handle.write(json.dumps(record) + "\n")
                    summary[series_id][transform]["produced"] = len(produced)
                    if len(produced) < requested:
                        logging.warning(
                            "[ext_v1] %s/%s %s cases for %s (requested=%s)",
                            len(produced),
                            requested,
                            transform,
                            series_id,
                            requested,
                        )
            _print_summary(summary)
    finally:
        conn.close()


def _sample_transform(ctx: SampleContext, transform: str, requested: int) -> Iterable[Dict[str, Any]]:
    if transform == "point":
        yield from _sample_point(ctx, requested)
    elif transform == "yoy":
        yield from _sample_change(ctx, requested, transform, truth.get_yoy)
    elif transform == "mom":
        yield from _sample_change(ctx, requested, transform, truth.get_mom)
    elif transform == "ma":
        yield from _sample_ma(ctx, requested)
    elif transform == "max":
        yield from _sample_extrema(ctx, requested, transform, truth.get_max)
    elif transform == "min":
        yield from _sample_extrema(ctx, requested, transform, truth.get_min)
    else:  # pragma: no cover
        return []


def _sample_point(ctx: SampleContext, requested: int) -> Iterable[Dict[str, Any]]:
    remaining = requested
    if remaining <= 0:
        return
    for date_value in _shuffle(ctx.rng, ctx.dates):
        truth_spec = {
            "series_id": ctx.series_id,
            "transform": "point",
            "date": date_value,
            "tolerance": TOLERANCE,
        }
        value = truth.get_point(ctx.conn, ctx.series_id, date_value)
        if value is None:
            continue
        yield _build_record(ctx, "point", truth_spec)
        remaining -= 1
        if remaining <= 0:
            break


def _sample_change(ctx: SampleContext, requested: int, transform: str, fn) -> Iterable[Dict[str, Any]]:
    remaining = requested
    if remaining <= 0:
        return
    for date_value in _shuffle(ctx.rng, ctx.dates):
        truth_spec = {
            "series_id": ctx.series_id,
            "transform": transform,
            "date": date_value,
            "tolerance": TOLERANCE,
        }
        value = fn(ctx.conn, ctx.series_id, date_value)
        if value is None:
            continue
        yield _build_record(ctx, transform, truth_spec)
        remaining -= 1
        if remaining <= 0:
            break


def _sample_ma(ctx: SampleContext, requested: int) -> Iterable[Dict[str, Any]]:
    remaining = requested
    if remaining <= 0:
        return
    for date_value in _shuffle(ctx.rng, ctx.dates):
        periods = ctx.rng.choice(MA_PERIOD_CHOICES)
        idx = ctx.date_positions.get(date_value, -1)
        if idx < periods - 1:
            continue
        truth_spec = {
            "series_id": ctx.series_id,
            "transform": "ma",
            "date": date_value,
            "periods": periods,
            "tolerance": TOLERANCE,
        }
        value = truth.get_ma(ctx.conn, ctx.series_id, date_value, periods)
        if value is None:
            continue
        yield _build_record(ctx, "ma", truth_spec, extra_meta={"periods": periods})
        remaining -= 1
        if remaining <= 0:
            break


def _sample_extrema(ctx: SampleContext, requested: int, transform: str, fn) -> Iterable[Dict[str, Any]]:
    if len(ctx.dates) < 2:
        return []
    attempts = 0
    max_attempts = requested * 20 + 20
    remaining = requested
    while remaining > 0 and attempts < max_attempts:
        attempts += 1
        start_idx = ctx.rng.randrange(0, len(ctx.dates) - 1)
        end_idx = ctx.rng.randrange(start_idx + 1, len(ctx.dates))
        if end_idx - start_idx < 2:
            continue
        window_start = ctx.dates[start_idx]
        window_end = ctx.dates[end_idx]
        truth_spec = {
            "series_id": ctx.series_id,
            "transform": transform,
            "window": {"start": window_start, "end": window_end},
            "tolerance": TOLERANCE,
        }
        _, value = fn(ctx.conn, ctx.series_id, window_start, window_end)
        if value is None:
            continue
        extra_meta = {"window_span": f"{window_start}:{window_end}"}
        yield _build_record(ctx, transform, truth_spec, extra_meta=extra_meta)
        remaining -= 1


def _build_record(
    ctx: SampleContext,
    transform: str,
    truth_spec: Dict[str, Any],
    extra_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    idx = ctx.id_counters[transform]
    ctx.id_counters[transform] += 1
    record_id = f"{ctx.series_id}_{transform}_{idx}"
    record = {
        "id": record_id,
        "question": None,
        "expect": {
            "should_answer": True,
            "should_have_value": True,
            "transform": transform,
            "series_id": ctx.series_id,
        },
        "truth_spec": truth_spec,
        "meta": {
            "seed": ctx.seed,
            "source": "duckdb",
            "series_id": ctx.series_id,
            "transform": transform,
        },
    }
    if extra_meta:
        record["meta"].update(extra_meta)
    return record


def _shuffle(rng: random.Random, values: Sequence[str]) -> List[str]:
    items = list(values)
    rng.shuffle(items)
    return items


def _print_summary(summary: Dict[str, Dict[str, Dict[str, int]]]) -> None:
    print("[ext_v1] Generation summary:")
    total = 0
    for series_id, per_transform in summary.items():
        print(f"  {series_id}:")
        for transform in TRANSFORMS:
            stats = per_transform.get(transform, {"requested": 0, "produced": 0})
            total += stats["produced"]
            print(
                f"    - {transform}: produced {stats['produced']} / requested {stats['requested']}"
            )
    print(f"[ext_v1] Total truth specs: {total}")


if __name__ == "__main__":
    main()
