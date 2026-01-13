#!/usr/bin/env python3
"""Promptfoo exec wrapper that resolves the repo root at runtime."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

LOG_PATH = Path(__file__).with_name("exec_answer.log")


def main() -> int:
    repo_root = Path(__file__).resolve().parents[3]
    answer_script = repo_root / "scripts" / "answer.py"
    if not answer_script.exists():
        raise SystemExit(f"Expected answer.py at {answer_script}, but it was not found.")

    cmd = [sys.executable or "python3", str(answer_script), *sys.argv[1:]]
    agent_override = os.environ.get("PROMPTFOO_AGENT")
    if agent_override:
        cmd.extend(["--agent", agent_override])
    completed = subprocess.run(cmd, capture_output=True, text=True, check=False)

    _append_log(
        {
            "ts": datetime.utcnow().isoformat() + "Z",
            "cwd": str(Path.cwd()),
            "python": sys.executable,
            "cmd": cmd,
            "returncode": completed.returncode,
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
        }
    )

    if completed.stdout:
        sys.stdout.write(completed.stdout)
        sys.stdout.flush()
    if completed.stderr:
        sys.stderr.write(completed.stderr)
        sys.stderr.flush()
    if completed.returncode != 0 and not completed.stderr:
        sys.stderr.write(
            f"promptfoo exec wrapper: '{answer_script.name}' exited with code {completed.returncode}\n"
        )
        sys.stderr.flush()

    return completed.returncode


def _append_log(entry: Dict[str, Any]) -> None:
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry) + "\n")
    except Exception:
        # Logging is best effort—never let it break the wrapper.
        pass


if __name__ == "__main__":
    raise SystemExit(main())
