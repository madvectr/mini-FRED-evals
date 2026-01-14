"""Local Phi-4 Mini helper to extract structured slots from fuzzy questions."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, Optional

MODEL_REL_PATH = Path("models") / "phi4-mini"
DEFAULT_CACHE_PATH = Path(os.environ.get("MINI_FRED_PHI_CACHE", "")) or (
    Path.home() / ".cache" / "mini-fred" / "phi4_parser_cache.jsonl"
)
SYSTEM_PROMPT = (
    "You extract structured instructions for a Mini-FRED agent. "
    "Return STRICT JSON with keys: series_guess (string or null), transform (one of "
    '["point","yoy","mom","ma","max","min"]), date (YYYY-MM or YYYY-MM-DD), '
    "window_start, window_end, periods (integer), should_refuse (boolean), confidence (0-1). "
    "If the question lacks enough detail, set should_refuse=true and explain in a short 'reason'."
)
JSON_PATTERN = re.compile(r"\{.*\}", re.DOTALL)


@dataclass
class LLMHints:
    transform: Optional[str] = None
    date: Optional[str] = None
    window_start: Optional[str] = None
    window_end: Optional[str] = None
    periods: Optional[int] = None
    series_guess: Optional[str] = None
    should_refuse: bool = False
    confidence: float = 0.0
    reason: Optional[str] = None

    @property
    def is_empty(self) -> bool:
        return not any(
            [
                self.transform,
                self.date,
                self.window_start,
                self.window_end,
                self.periods,
                self.series_guess,
                self.should_refuse,
            ]
        )


class _Phi4MiniParser:
    def __init__(self) -> None:
        self.model_dir = Path(os.environ.get("MINI_FRED_PHI_MODEL_DIR", MODEL_REL_PATH)).expanduser()
        self.cache_path = DEFAULT_CACHE_PATH
        self._tokenizer = None
        self._model = None
        self._device = None
        self._load_error: Optional[str] = None
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._load_cache()

    def is_available(self) -> bool:
        if self._load_error:
            return False
        return self.model_dir.exists()

    def infer(self, question: str) -> Optional[LLMHints]:
        question = question.strip()
        if not question or not self.is_available():
            return None
        key = sha256(question.encode("utf-8")).hexdigest()
        if key in self._cache:
            return self._json_to_hints(self._cache[key])
        parsed = self._run_model(question)
        if not parsed:
            return None
        self._cache[key] = parsed
        self._append_cache_entry(key, parsed)
        return self._json_to_hints(parsed)

    def _run_model(self, question: str) -> Optional[Dict[str, Any]]:
        self._ensure_model_loaded()
        if self._load_error:
            return None
        import torch  # type: ignore[import]

        prompt = f"<|system|>\n{SYSTEM_PROMPT}\n<|end|>\n<|user|>\n{question}\n<|end|>\n<|assistant|>\n"
        inputs = self._tokenizer(prompt, return_tensors="pt")
        inputs = {k: v.to(self._device) for k, v in inputs.items()}
        with torch.no_grad():
            output = self._model.generate(
                **inputs,
                max_new_tokens=256,
                do_sample=False,
                temperature=0.0,
            )
        generated = output[:, inputs["input_ids"].shape[-1] :]
        text = self._tokenizer.decode(generated[0], skip_special_tokens=True).strip()
        match = JSON_PATTERN.search(text)
        if not match:
            return None
        try:
            data = json.loads(match.group(0))
        except json.JSONDecodeError:
            return None
        return data

    def _ensure_model_loaded(self) -> None:
        if self._tokenizer is not None and self._model is not None:
            return
        if not self.model_dir.exists():
            self._load_error = f"Phi-4 Mini weights not found in {self.model_dir}"
            return
        try:
            import torch  # type: ignore[import]
            from transformers import AutoModelForCausalLM, AutoTokenizer, utils as hf_utils  # type: ignore[import]
            if not hasattr(hf_utils, "LossKwargs"):
                try:
                    from typing import TypedDict
                except ImportError:  # pragma: no cover
                    TypedDict = dict  # type: ignore[assignment]

                class LossKwargs(TypedDict, total=False):  # type: ignore[misc]
                    pass

                hf_utils.LossKwargs = LossKwargs  # type: ignore[attr-defined]
        except ModuleNotFoundError as exc:  # pragma: no cover
            self._load_error = f"Missing dependency for Phi-4 Mini ({exc}). Install transformers + torch."
            return
        try:
            self._tokenizer = AutoTokenizer.from_pretrained(
                self.model_dir,
                local_files_only=True,
                trust_remote_code=True,
            )
            dtype = torch.float16 if torch.cuda.is_available() else torch.float32
            self._model = AutoModelForCausalLM.from_pretrained(
                self.model_dir,
                local_files_only=True,
                trust_remote_code=True,
                torch_dtype=dtype,
                low_cpu_mem_usage=True,
            )
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            self._model.to(device)
            self._model.eval()
            self._device = device
            if os.environ.get("MINI_FRED_PHI_DISABLE_CACHE"):
                self._cache.clear()
        except Exception as exc:  # pragma: no cover
            self._load_error = f"Failed to load Phi-4 Mini: {exc}"
            self._tokenizer = None
            self._model = None

    def _load_cache(self) -> None:
        if not self.cache_path.exists():
            return
        try:
            with self.cache_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        entry = json.loads(line.strip())
                        if "key" in entry and "data" in entry:
                            self._cache[entry["key"]] = entry["data"]
                    except json.JSONDecodeError:
                        continue
        except OSError:
            pass

    def _append_cache_entry(self, key: str, data: Dict[str, Any]) -> None:
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            with self.cache_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({"key": key, "data": data}) + "\n")
        except OSError:
            pass

    @staticmethod
    def _json_to_hints(payload: Dict[str, Any]) -> LLMHints:
        def _int(value: Any) -> Optional[int]:
            try:
                if value is None:
                    return None
                return int(value)
            except (TypeError, ValueError):
                return None

        return LLMHints(
            transform=payload.get("transform"),
            date=payload.get("date"),
            window_start=payload.get("window_start"),
            window_end=payload.get("window_end"),
            periods=_int(payload.get("periods")),
            series_guess=payload.get("series_guess"),
            should_refuse=bool(payload.get("should_refuse", False)),
            confidence=float(payload.get("confidence", 0.0) or 0.0),
            reason=payload.get("reason"),
        )


_PARSER: Optional[_Phi4MiniParser] = None


def _get_parser() -> _Phi4MiniParser:
    global _PARSER
    if _PARSER is None:
        _PARSER = _Phi4MiniParser()
    return _PARSER


def infer_slots(question: str) -> Optional[LLMHints]:
    if os.environ.get("MINI_FRED_DISABLE_LOCAL_LLM"):
        return None
    parser = _get_parser()
    return parser.infer(question)


def is_available() -> bool:
    return _get_parser().is_available()


if __name__ == "__main__":  # pragma: no cover
    import argparse

    cli = argparse.ArgumentParser(description="Quick Phi-4 Mini sanity check.")
    cli.add_argument("question")
    args = cli.parse_args()
    hints = infer_slots(args.question)
    if not hints:
        print("Phi-4 Mini not available or unable to parse input.")
    else:
        print(hints)
