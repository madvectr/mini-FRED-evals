#!/usr/bin/env python3
"""Deep-merge helper for MVES spec/verifier overrides."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

try:
    import yaml  # type: ignore[import]
except ModuleNotFoundError:  # pragma: no cover
    yaml = None  # type: ignore[assignment]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge MVES spec/verifier overrides.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--base-spec", type=Path, help="Path to the base spec file (YAML or JSON).")
    group.add_argument(
        "--base-verifiers",
        type=Path,
        help="Path to the base verifier map file (YAML or JSON).",
    )
    parser.add_argument(
        "--override",
        type=Path,
        required=True,
        help="Path to the override JSON file (spec or verifiers).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output path for the merged JSON.",
    )
    return parser.parse_args()


def load_data(path: Path) -> Any:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".yaml", ".yml"} and yaml is not None:
        return yaml.safe_load(text)
    return json.loads(text)


def deep_merge(base: Any, override: Any) -> Any:
    if isinstance(base, dict) and isinstance(override, dict):
        merged: Dict[str, Any] = dict(base)
        for key, value in override.items():
            if key in merged:
                merged[key] = deep_merge(merged[key], value)
            else:
                merged[key] = value
        return merged

    if isinstance(base, list) and isinstance(override, list):
        if _list_is_dicts_with_id(base) or _list_is_dicts_with_id(override):
            return _merge_list_by_id(base, override)
        return base + override

    return override


def _list_is_dicts_with_id(value: Any) -> bool:
    if not isinstance(value, list) or not value:
        return False
    return all(isinstance(item, dict) and "id" in item for item in value)


def _merge_list_by_id(base: List[Dict[str, Any]], override: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged = [dict(item) for item in base]
    index = {item.get("id"): idx for idx, item in enumerate(merged) if "id" in item}
    for entry in override:
        entry_id = entry.get("id")
        if entry_id in index:
            merged[index[entry_id]] = deep_merge(merged[index[entry_id]], entry)
        else:
            merged.append(entry)
    return merged


def main() -> None:
    args = parse_args()
    if args.base_spec:
        base = load_data(args.base_spec)
    else:
        base = load_data(args.base_verifiers)
    override = load_data(args.override)
    merged = deep_merge(base, override)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(merged, indent=2), encoding="utf-8")
    print(f"[load_spec] Wrote merged file to {args.out}", flush=True)


if __name__ == "__main__":
    main()
