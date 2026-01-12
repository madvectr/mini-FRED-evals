"""Utility helpers shared across Mini-FRED modules."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import yaml


def resolve_project_root(start: Path | None = None) -> Path:
    """Return the Mini-FRED project root (directory that contains pyproject.toml)."""
    current = start or Path(__file__).resolve().parent.parent
    sentinel = "pyproject.toml"
    for candidate in [current, *current.parents]:
        if (candidate / sentinel).exists():
            return candidate
    raise FileNotFoundError(
        f"Unable to locate {sentinel}; ensure you are inside the Mini-FRED repo."
    )


def load_series_config(config_path: str | Path) -> Mapping[str, Any]:
    """Load the YAML configuration that describes series, date windows, and policies."""
    cfg_path = Path(config_path).expanduser().resolve()
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")
    with cfg_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def ensure_directory(path: str | Path) -> Path:
    """Create the directory if it does not yet exist and return its Path."""
    directory = Path(path).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True)
    return directory
