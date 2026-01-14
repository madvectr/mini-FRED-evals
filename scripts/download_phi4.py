#!/usr/bin/env python3
"""Download Phi-4 Mini locally for the answer_5 LLM parser."""

from __future__ import annotations

import argparse
import sys
import os
from pathlib import Path

try:
    from huggingface_hub import snapshot_download  # type: ignore[import]
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit(
        "huggingface_hub is required for this script. Install via `pip install huggingface_hub`."
    ) from exc

DEFAULT_REPO = "microsoft/Phi-4-mini-instruct"
DEFAULT_DEST = Path("models") / "phi4-mini"
DEFAULT_PATTERNS = [
    "config.json",
    "generation_config.json",
    "tokenizer_config.json",
    "tokenizer.json",
    "tokenizer.model",
    "special_tokens_map.json",
    "pytorch_model.bin",
    "*.safetensors",
    "configuration_phi3.py",
    "modeling_phi3.py",
    "tokenization_phi3.py",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Phi-4 Mini weights for local inference.")
    parser.add_argument("--repo", default=DEFAULT_REPO, help="Hugging Face repo ID to download.")
    parser.add_argument(
        "--dest",
        type=Path,
        default=DEFAULT_DEST,
        help="Destination directory for the model files (default: models/phi4-mini).",
    )
    parser.add_argument(
        "--patterns",
        nargs="+",
        default=DEFAULT_PATTERNS,
        help="Allow patterns to pass to snapshot_download (default: common config/tokenizer + PyTorch weights).",
    )
    parser.add_argument(
        "--revision",
        default=None,
        help="Optional repo revision (tag/commit). Defaults to the repo's latest main branch.",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("HUGGINGFACE_HUB_TOKEN"),
        help="Hugging Face access token (or set HUGGINGFACE_HUB_TOKEN env var).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dest = args.dest.expanduser().resolve()
    dest.mkdir(parents=True, exist_ok=True)
    print(f"[phi4] Downloading {args.repo} -> {dest}", flush=True)
    snapshot_download(
        repo_id=args.repo,
        revision=args.revision,
        allow_patterns=args.patterns,
        local_dir=dest,
        local_dir_use_symlinks=False,
        token=args.token,
    )
    print("[phi4] Download complete.", flush=True)


if __name__ == "__main__":
    main()
